//! Per-phase helpers for `DbtRunWorkflow::run`.
//!
//! Each function corresponds to one logical phase in a dbt run lifecycle.
//! `WorkflowContext` uses interior mutability via Rc — methods take `&self`,
//! so helpers take `&WorkflowContext<DbtRunWorkflow>` matching the pattern
//! in `crate::hooks::execute_hooks`. State that crosses phases (effective
//! env, hook errors, success flag) is threaded by explicit `&mut` rather
//! than bundled into a struct, since each phase only needs a small subset
//! and bundling would force borrow-checker gymnastics.

// WorkflowContext uses Rc internally — the run() method already silences
// this same lint for the same reason.
#![allow(clippy::future_not_send)]

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::hooks::execute_hooks;
use crate::types::{
    CommandMemo, DbtRunInput, DbtRunOutput, ExecutionPlan, HookError, HookEvent, HookPayload,
    HooksConfig, NodeExecutionResult, NodeStatus, ProjectHookPhase, ProjectHooksInput,
    ResolveConfigInput, ResolvedProjectConfig, StoreArtifactsInput, StoreArtifactsOutput,
};

use super::DbtRunWorkflow;
use super::helpers::{elapsed_secs, plural};

/// Build the workflow-detail line shown after `plan_project` returns:
/// "planned 17 nodes across 8 levels".
pub fn build_planned_details(node_count: usize, level_count: usize) -> String {
    format!(
        "planned {node_count} node{} across {level_count} level{}",
        plural(node_count),
        plural(level_count),
    )
}

/// Build the input for the `resolve_config` activity. Pure projection of
/// project name + caller-supplied hook overrides.
pub fn build_resolve_config_input(plan: &ExecutionPlan, input: &DbtRunInput) -> ResolveConfigInput {
    ResolveConfigInput {
        project: Some(plan.project.clone()),
        input_hooks: input.hooks.clone(),
    }
}

/// Build the `HookPayload` that pre_run hooks receive. Output is always None
/// at this point — the run hasn't produced one yet.
pub fn build_pre_run_payload(plan: &ExecutionPlan, input: &DbtRunInput) -> HookPayload {
    HookPayload {
        event: HookEvent::PreRun,
        invocation_id: plan.invocation_id.clone(),
        input: input.clone(),
        plan: Some(plan.clone()),
        output: None,
    }
}

/// Build the `HookPayload` for on_success / on_failure hooks. The `event`
/// determines which list of hooks runs; the output snapshot is what the run
/// finished with so far.
pub fn build_post_hook_payload(
    event: HookEvent,
    plan: &ExecutionPlan,
    input: &DbtRunInput,
    output_snapshot: &DbtRunOutput,
) -> HookPayload {
    HookPayload {
        event,
        invocation_id: plan.invocation_id.clone(),
        input: input.clone(),
        plan: Some(plan.clone()),
        output: Some(output_snapshot.clone()),
    }
}

/// Build the `DbtRunOutput` returned when pre_run hooks signal skip. Drains
/// `hook_errors` so the caller can return the output without leaving stale
/// errors in the parent's accumulator.
pub fn build_skip_output(
    plan: &ExecutionPlan,
    skip_reason: Option<String>,
    hook_errors: &mut Vec<HookError>,
    elapsed: f64,
) -> DbtRunOutput {
    DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: true,
        skipped: true,
        skip_reason,
        node_results: vec![],
        elapsed_time: elapsed,
        artifacts: None,
        log_path: None,
        hook_errors: std::mem::take(hook_errors),
    }
}

/// Build the input for `run_project_hooks` (on-run-start or on-run-end).
/// `node_results` is empty for on-run-start, populated for on-run-end.
pub fn build_project_hooks_input(
    phase: ProjectHookPhase,
    plan: &ExecutionPlan,
    input: &DbtRunInput,
    effective_env: &BTreeMap<String, String>,
    node_results: &[NodeExecutionResult],
) -> ProjectHooksInput {
    ProjectHooksInput {
        phase,
        project: plan.project.clone(),
        invocation_id: plan.invocation_id.clone(),
        env: effective_env.clone(),
        target: input.target.clone(),
        node_results: node_results.to_vec(),
    }
}

/// Build the input for `store_artifacts` from the plan + run accumulators.
pub fn build_store_artifacts_input(
    plan: &ExecutionPlan,
    all_results: &[NodeExecutionResult],
    log_lines: &[String],
) -> StoreArtifactsInput {
    StoreArtifactsInput {
        invocation_id: plan.invocation_id.clone(),
        node_results: all_results.to_vec(),
        manifest_json: plan.manifest_json.clone(),
        manifest_ref: plan.manifest_ref.clone(),
        run_log: Some(log_lines.join("\n")),
    }
}

/// Pick the post-run hook list and event from the run's success flag.
pub fn select_post_run_hooks(
    success: bool,
    hooks: &HooksConfig,
) -> (&[crate::types::HookConfig], HookEvent) {
    if success {
        (&hooks.on_success, HookEvent::OnSuccess)
    } else {
        (&hooks.on_failure, HookEvent::OnFailure)
    }
}

/// Merge a successful `HookExecutionOutcome` into the parent run state and
/// decide whether the workflow should short-circuit.
///
/// `hook_errors` and `effective_env` are mutated in place; the returned
/// `ControlFlow` is `Break(output)` when the hooks signalled skip (the
/// caller returns `output` immediately) or `Continue(())` to proceed with
/// the run.
pub fn apply_pre_run_outcome(
    plan: &ExecutionPlan,
    outcome: crate::types::HookExecutionOutcome,
    effective_env: &mut BTreeMap<String, String>,
    hook_errors: &mut Vec<HookError>,
    elapsed: f64,
) -> ControlFlow<DbtRunOutput, ()> {
    hook_errors.extend(outcome.errors);
    effective_env.extend(outcome.extra_env);
    if outcome.skip {
        return ControlFlow::Break(build_skip_output(
            plan,
            outcome.skip_reason,
            hook_errors,
            elapsed,
        ));
    }
    ControlFlow::Continue(())
}

/// Merge the result of `execute_hooks` for a post-run hook batch. Returns the
/// new `success` flag for the workflow:
///
/// - On Ok: drains the outcome's collected non-fatal errors into the parent's
///   accumulator and preserves the incoming `success_in` value.
/// - On Err: a post-hook failure marks the workflow as failed (matches
///   dbt-core: failed `on_success` hooks turn the run into a failure).
pub fn apply_post_run_outcome(
    outcome_result: Result<crate::types::HookExecutionOutcome, anyhow::Error>,
    hook_errors: &mut Vec<HookError>,
    success_in: bool,
) -> bool {
    match outcome_result {
        Ok(outcome) => {
            hook_errors.extend(outcome.errors);
            success_in
        }
        Err(_) => false,
    }
}

/// Convert search-attribute strings into the JSON-encoded payloads Temporal's
/// `upsert_search_attributes` API expects. Pure: just serializes each value
/// and wraps any failure into `WorkflowTermination::failed` with the
/// attribute key in the error message.
pub fn build_search_attribute_payloads(
    attributes: &BTreeMap<String, String>,
) -> Result<
    Vec<(String, temporalio_common::protos::temporal::api::common::v1::Payload)>,
    WorkflowTermination,
> {
    attributes
        .iter()
        .map(|(k, v)| {
            let payload = v.as_json_payload().map_err(|e| {
                WorkflowTermination::failed(anyhow::anyhow!(
                    "serializing search attribute '{k}' as JSON payload: {e:#}"
                ))
            })?;
            Ok((k.clone(), payload))
        })
        .collect()
}

pub fn write_command_memo(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
) -> Result<(), WorkflowTermination> {
    let memo = CommandMemo::from(input);
    ctx.upsert_memo([(
        "command".to_string(),
        memo.as_json_payload()
            .map_err(WorkflowTermination::failed)?,
    )]);
    Ok(())
}

/// Runs `plan_project`, upserts search attributes, sets the workflow's
/// current_details header. Returns the resolved `ExecutionPlan`.
pub async fn plan_and_announce(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
) -> Result<ExecutionPlan, WorkflowTermination> {
    let plan: ExecutionPlan = ctx
        .start_activity(
            DbtActivities::plan_project,
            input.clone(),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(300)),
        )
        .await
        .map_err(|e| {
            WorkflowTermination::failed(anyhow::anyhow!("plan_project activity failed: {e:#}"))
        })?;

    if !plan.search_attributes.is_empty() {
        let sa_payloads = build_search_attribute_payloads(&plan.search_attributes)?;
        ctx.upsert_search_attributes(sa_payloads);
    }

    ctx.set_current_details(build_planned_details(plan.nodes.len(), plan.levels.len()));

    Ok(plan)
}

pub async fn resolve_project_config(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
) -> Result<ResolvedProjectConfig, WorkflowTermination> {
    ctx.start_activity(
        DbtActivities::resolve_config,
        build_resolve_config_input(plan, input),
        ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
    )
    .await
    .map_err(|e| {
        WorkflowTermination::failed(anyhow::anyhow!("resolve_config activity failed: {e:#}"))
    })
}

/// `ControlFlow::Break(out)` means the pre_run hooks signalled `skip` — the
/// caller should return `out` immediately. `ControlFlow::Continue(())` means
/// hooks completed; `effective_env` and `hook_errors` are updated in place.
pub async fn run_pre_run_hooks(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    hooks: &HooksConfig,
    effective_env: &mut BTreeMap<String, String>,
    hook_errors: &mut Vec<HookError>,
    start: Option<SystemTime>,
) -> Result<ControlFlow<DbtRunOutput, ()>, WorkflowTermination> {
    if hooks.pre_run.is_empty() {
        return Ok(ControlFlow::Continue(()));
    }

    let payload = build_pre_run_payload(plan, input);
    match execute_hooks(ctx, &hooks.pre_run, &payload).await {
        Ok(outcome) => {
            let elapsed = elapsed_secs(start, ctx.workflow_time());
            Ok(apply_pre_run_outcome(plan, outcome, effective_env, hook_errors, elapsed))
        }
        Err(e) => Err(WorkflowTermination::failed(e)),
    }
}

pub async fn run_on_run_start(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    effective_env: &BTreeMap<String, String>,
) -> Result<(), WorkflowTermination> {
    if !plan.has_on_run_start {
        return Ok(());
    }
    ctx.set_current_details("running on-run-start hooks".to_string());
    ctx.start_activity(
        DbtActivities::run_project_hooks,
        build_project_hooks_input(ProjectHookPhase::OnRunStart, plan, input, effective_env, &[]),
        ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
            .heartbeat_timeout(Duration::from_secs(120))
            .build(),
    )
    .await
    .map_err(|e| WorkflowTermination::failed(anyhow::anyhow!("on-run-start hook failed: {e:#}")))
}

pub async fn store_run_artifacts(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: &ExecutionPlan,
    all_results: &[NodeExecutionResult],
    log_lines: &[String],
) -> Result<(Option<StoreArtifactsOutput>, Option<String>), WorkflowTermination> {
    if !plan.write_artifacts {
        return Ok((None, None));
    }
    let artifacts: StoreArtifactsOutput = ctx
        .start_activity(
            DbtActivities::store_artifacts,
            build_store_artifacts_input(plan, all_results, log_lines),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(120)),
        )
        .await
        .map_err(|e| {
            WorkflowTermination::failed(anyhow::anyhow!("store_artifacts activity failed: {e:#}"))
        })?;
    let log_path = artifacts.log_path.clone();
    Ok((Some(artifacts), log_path))
}

/// Failure here is non-fatal: appended to `hook_errors`, doesn't change the
/// run outcome. Always fires when configured, even when the run failed
/// (matches dbt-core behaviour).
pub async fn run_on_run_end(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    effective_env: &BTreeMap<String, String>,
    all_results: &[NodeExecutionResult],
    hook_errors: &mut Vec<HookError>,
) {
    if !plan.has_on_run_end {
        return;
    }
    ctx.set_current_details("running on-run-end hooks".to_string());
    let result = ctx
        .start_activity(
            DbtActivities::run_project_hooks,
            build_project_hooks_input(
                ProjectHookPhase::OnRunEnd,
                plan,
                input,
                effective_env,
                all_results,
            ),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
                .heartbeat_timeout(Duration::from_secs(120))
                .build(),
        )
        .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "on-run-end hook failed (non-fatal)");
        hook_errors.push(HookError {
            hook_workflow_type: "on_run_end".to_string(),
            event: "on_run_end".to_string(),
            error: e.to_string(),
        });
    }
}

/// Runs the on_success or on_failure hooks. Returns the post-hook `success`
/// flag — caller is responsible for writing it back into `output.success`
/// before deciding `Ok` vs `Err`.
pub async fn run_post_hooks(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    hooks: &HooksConfig,
    output_snapshot: &DbtRunOutput,
    success_in: bool,
    hook_errors: &mut Vec<HookError>,
) -> bool {
    let (post_hooks, event) = select_post_run_hooks(success_in, hooks);

    if post_hooks.is_empty() {
        return success_in;
    }

    let payload = build_post_hook_payload(event, plan, input, output_snapshot);
    let result = execute_hooks(ctx, post_hooks, &payload).await;
    if let Err(ref e) = result {
        tracing::error!(event = %event, error = %e, "post-hook failed, marking workflow as failed");
    }
    apply_post_run_outcome(result, hook_errors, success_in)
}

/// Build the workflow output for a `list` command: returns all planned nodes as
/// success entries without executing any SQL or touching the warehouse.
pub fn build_list_output(plan: &ExecutionPlan, elapsed: f64) -> DbtRunOutput {
    let node_results = plan
        .nodes
        .values()
        .map(|node| NodeExecutionResult {
            unique_id: node.unique_id.clone(),
            status: NodeStatus::Success,
            execution_time: 0.0,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
        })
        .collect();
    DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: true,
        skipped: false,
        skip_reason: None,
        node_results,
        elapsed_time: elapsed,
        artifacts: None,
        log_path: None,
        hook_errors: vec![],
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use crate::types::HookConfig;

    fn empty_plan() -> ExecutionPlan {
        ExecutionPlan {
            project: "shop".to_string(),
            levels: vec![],
            nodes: BTreeMap::new(),
            manifest_json: None,
            manifest_ref: None,
            invocation_id: "inv-42".to_string(),
            search_attributes: BTreeMap::new(),
            write_artifacts: false,
            has_on_run_start: false,
            has_on_run_end: false,
        }
    }

    fn empty_input() -> DbtRunInput {
        DbtRunInput {
            project: Some("shop".to_string()),
            command: "build".to_string(),
            select: None,
            exclude: None,
            vars: BTreeMap::new(),
            target: None,
            full_refresh: false,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::new(),
        }
    }

    // --- build_list_output ---

    #[test]
    fn build_list_output_returns_all_nodes_as_success_without_execution() {
        use crate::types::NodeInfo;

        let plan = ExecutionPlan {
            invocation_id: "inv-list".to_string(),
            nodes: BTreeMap::from([
                (
                    "model.shop.customers".to_string(),
                    NodeInfo {
                        unique_id: "model.shop.customers".to_string(),
                        name: "customers".to_string(),
                        resource_type: "model".to_string(),
                        materialization: Some("table".to_string()),
                        package_name: "shop".to_string(),
                        depends_on: vec![],
                    },
                ),
                (
                    "model.shop.orders".to_string(),
                    NodeInfo {
                        unique_id: "model.shop.orders".to_string(),
                        name: "orders".to_string(),
                        resource_type: "model".to_string(),
                        materialization: Some("table".to_string()),
                        package_name: "shop".to_string(),
                        depends_on: vec!["model.shop.customers".to_string()],
                    },
                ),
            ]),
            ..empty_plan()
        };

        let out = build_list_output(&plan, 0.05);

        assert!(out.success);
        assert!(!out.skipped);
        assert_eq!(out.invocation_id, "inv-list");
        assert_eq!(out.node_results.len(), 2, "one result per planned node");
        for r in &out.node_results {
            assert_eq!(r.status, NodeStatus::Success);
            assert!(
                (r.execution_time - 0.0).abs() < f64::EPSILON,
                "list doesn't execute — no execution time"
            );
            assert!(r.compiled_code.is_none());
            assert!(r.message.is_none());
        }
        assert!(out.artifacts.is_none());
        assert!(out.hook_errors.is_empty());
    }

    #[test]
    fn build_list_output_empty_plan_returns_empty_results() {
        let out = build_list_output(&empty_plan(), 0.0);
        assert!(out.success);
        assert!(out.node_results.is_empty());
    }

    // --- build_planned_details ---

    #[test]
    fn build_planned_details_pluralises_both_counts() {
        assert_eq!(build_planned_details(0, 0), "planned 0 nodes across 0 levels");
        assert_eq!(build_planned_details(1, 1), "planned 1 node across 1 level");
        assert_eq!(build_planned_details(17, 8), "planned 17 nodes across 8 levels");
    }

    // --- build_resolve_config_input ---

    #[test]
    fn build_resolve_config_input_carries_project_and_input_hooks() {
        let plan = ExecutionPlan {
            project: "shop".to_string(),
            ..empty_plan()
        };
        let input = DbtRunInput {
            hooks: Some(HooksConfig::default()),
            ..empty_input()
        };
        let resolved = build_resolve_config_input(&plan, &input);
        assert_eq!(resolved.project.as_deref(), Some("shop"));
        assert!(resolved.input_hooks.is_some());
    }

    #[test]
    fn build_resolve_config_input_passes_through_none_input_hooks() {
        let plan = empty_plan();
        let input = empty_input(); // hooks = None
        let resolved = build_resolve_config_input(&plan, &input);
        assert!(resolved.input_hooks.is_none());
    }

    // --- build_pre_run_payload / build_post_hook_payload ---

    #[test]
    fn build_pre_run_payload_pins_event_invocation_and_omits_output() {
        let plan = ExecutionPlan {
            invocation_id: "inv-pre".to_string(),
            ..empty_plan()
        };
        let payload = build_pre_run_payload(&plan, &empty_input());
        assert_eq!(payload.event, HookEvent::PreRun);
        assert_eq!(payload.invocation_id, "inv-pre");
        assert!(payload.plan.is_some());
        // pre_run hasn't produced output yet — explicitly None.
        assert!(payload.output.is_none());
    }

    #[test]
    fn build_post_hook_payload_includes_output_snapshot() {
        let plan = empty_plan();
        let input = empty_input();
        let snapshot = DbtRunOutput {
            invocation_id: plan.invocation_id.clone(),
            success: true,
            skipped: false,
            skip_reason: None,
            node_results: vec![],
            elapsed_time: 1.5,
            artifacts: None,
            log_path: None,
            hook_errors: vec![],
        };
        let payload = build_post_hook_payload(HookEvent::OnSuccess, &plan, &input, &snapshot);
        assert_eq!(payload.event, HookEvent::OnSuccess);
        let out = payload
            .output
            .expect("post-hook payload must carry the snapshot");
        assert!(out.success);
        assert!((out.elapsed_time - 1.5).abs() < f64::EPSILON);
    }

    // --- build_skip_output ---

    #[test]
    fn build_skip_output_drains_hook_errors_and_records_reason() {
        let plan = ExecutionPlan {
            invocation_id: "inv-skip".to_string(),
            ..empty_plan()
        };
        let mut hook_errors = vec![HookError {
            hook_workflow_type: "notify".to_string(),
            event: "pre_run".to_string(),
            error: "warn-1".to_string(),
        }];
        let out = build_skip_output(&plan, Some("no new data".to_string()), &mut hook_errors, 0.42);
        assert!(out.success); // skip is success — the run reached its end intentionally.
        assert!(out.skipped);
        assert_eq!(out.skip_reason.as_deref(), Some("no new data"));
        assert!(out.node_results.is_empty());
        assert!((out.elapsed_time - 0.42).abs() < f64::EPSILON);
        // Drained — caller's accumulator is reset.
        assert!(hook_errors.is_empty());
        assert_eq!(out.hook_errors.len(), 1);
    }

    #[test]
    fn build_skip_output_handles_no_skip_reason() {
        let plan = empty_plan();
        let mut hook_errors: Vec<HookError> = vec![];
        let out = build_skip_output(&plan, None, &mut hook_errors, 0.0);
        assert!(out.skipped);
        assert!(out.skip_reason.is_none());
    }

    // --- build_project_hooks_input ---

    #[test]
    fn build_project_hooks_input_on_run_start_has_no_node_results() {
        let plan = ExecutionPlan {
            project: "shop".to_string(),
            invocation_id: "inv-1".to_string(),
            ..empty_plan()
        };
        let env = BTreeMap::from([("DBT_TARGET".to_string(), "prod".to_string())]);
        let mut input = empty_input();
        input.target = Some("prod".to_string());

        let phs = build_project_hooks_input(
            ProjectHookPhase::OnRunStart,
            &plan,
            &input,
            &env,
            &[], // on-run-start has no results yet
        );
        assert_eq!(phs.phase, ProjectHookPhase::OnRunStart);
        assert_eq!(phs.project, "shop");
        assert_eq!(phs.invocation_id, "inv-1");
        assert_eq!(phs.target.as_deref(), Some("prod"));
        assert!(phs.node_results.is_empty());
        assert_eq!(phs.env.get("DBT_TARGET").map(String::as_str), Some("prod"));
    }

    #[test]
    fn build_project_hooks_input_on_run_end_carries_node_results() {
        use crate::types::{NodeExecutionResult, NodeStatus};
        let plan = empty_plan();
        let input = empty_input();
        let env = BTreeMap::new();
        let results = vec![NodeExecutionResult {
            unique_id: "model.shop.a".to_string(),
            status: NodeStatus::Success,
            execution_time: 0.1,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
        }];
        let phs =
            build_project_hooks_input(ProjectHookPhase::OnRunEnd, &plan, &input, &env, &results);
        assert_eq!(phs.phase, ProjectHookPhase::OnRunEnd);
        assert_eq!(phs.node_results.len(), 1);
    }

    // --- build_store_artifacts_input ---

    #[test]
    fn build_store_artifacts_input_joins_log_lines_and_carries_manifest() {
        let plan = ExecutionPlan {
            invocation_id: "inv-store".to_string(),
            manifest_json: Some("{\"k\":1}".to_string()),
            manifest_ref: None,
            ..empty_plan()
        };
        let log_lines = vec!["line one".to_string(), "line two".to_string()];
        let store_input = build_store_artifacts_input(&plan, &[], &log_lines);
        assert_eq!(store_input.invocation_id, "inv-store");
        assert_eq!(store_input.manifest_json.as_deref(), Some("{\"k\":1}"));
        assert!(store_input.manifest_ref.is_none());
        assert_eq!(store_input.run_log.as_deref(), Some("line one\nline two"));
    }

    #[test]
    fn build_store_artifacts_input_uses_manifest_ref_when_provided() {
        let plan = ExecutionPlan {
            invocation_id: "inv".to_string(),
            manifest_json: None,
            manifest_ref: Some("/path/to/manifest.json".to_string()),
            ..empty_plan()
        };
        let store_input = build_store_artifacts_input(&plan, &[], &[]);
        assert!(store_input.manifest_json.is_none());
        assert_eq!(store_input.manifest_ref.as_deref(), Some("/path/to/manifest.json"));
        // Empty log_lines join to empty string — still wrapped in Some.
        assert_eq!(store_input.run_log.as_deref(), Some(""));
    }

    // --- select_post_run_hooks ---

    fn dummy_hook(name: &str) -> HookConfig {
        HookConfig {
            workflow_type: name.to_string(),
            task_queue: "q".to_string(),
            timeout_secs: None,
            on_error: crate::types::HookErrorMode::default(),
            input: None,
            fire_and_forget: false,
        }
    }

    #[test]
    fn select_post_run_hooks_picks_on_success_when_run_succeeded() {
        let mut hooks = HooksConfig::default();
        hooks.on_success.push(dummy_hook("succ"));
        hooks.on_failure.push(dummy_hook("fail"));
        let (selected, event) = select_post_run_hooks(true, &hooks);
        assert_eq!(event, HookEvent::OnSuccess);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].workflow_type, "succ");
    }

    #[test]
    fn select_post_run_hooks_picks_on_failure_when_run_failed() {
        let mut hooks = HooksConfig::default();
        hooks.on_success.push(dummy_hook("succ"));
        hooks.on_failure.push(dummy_hook("fail"));
        let (selected, event) = select_post_run_hooks(false, &hooks);
        assert_eq!(event, HookEvent::OnFailure);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].workflow_type, "fail");
    }

    #[test]
    fn select_post_run_hooks_returns_empty_slice_when_corresponding_list_unset() {
        let hooks = HooksConfig::default();
        let (sel_succ, _) = select_post_run_hooks(true, &hooks);
        let (sel_fail, _) = select_post_run_hooks(false, &hooks);
        assert!(sel_succ.is_empty());
        assert!(sel_fail.is_empty());
    }

    // --- build_search_attribute_payloads ---

    // --- apply_post_run_outcome ---

    #[test]
    fn apply_post_run_outcome_ok_collects_errors_and_preserves_success() {
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(false, None, &[], 2);
        let success = apply_post_run_outcome(Ok(outcome), &mut hook_errors, true);
        assert!(success);
        assert_eq!(hook_errors.len(), 2);
    }

    #[test]
    fn apply_post_run_outcome_ok_preserves_failure_status() {
        // The post-hook ran cleanly but the run already failed → success
        // stays false.
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(false, None, &[], 0);
        let success = apply_post_run_outcome(Ok(outcome), &mut hook_errors, false);
        assert!(!success);
    }

    #[test]
    fn apply_post_run_outcome_err_marks_workflow_failed() {
        // post-hook itself errored — even on a previously-successful run, the
        // workflow is marked failed. (dbt-core: a failed on_success turns the
        // run red.)
        let mut hook_errors = Vec::new();
        let success = apply_post_run_outcome(
            Err(anyhow::anyhow!("post-hook blew up")),
            &mut hook_errors,
            true,
        );
        assert!(!success);
        // Note: the err arm doesn't push to hook_errors — that loss is
        // intentional, the caller logs the error before calling here.
        assert!(hook_errors.is_empty());
    }

    #[test]
    fn build_search_attribute_payloads_empty_input() {
        let attrs = BTreeMap::new();
        let result = build_search_attribute_payloads(&attrs).unwrap();
        assert!(result.is_empty());
    }

    // --- apply_pre_run_outcome ---

    fn make_outcome(
        skip: bool,
        reason: Option<&str>,
        extra_env: &[(&str, &str)],
        errors: usize,
    ) -> crate::types::HookExecutionOutcome {
        crate::types::HookExecutionOutcome {
            errors: (0..errors)
                .map(|i| HookError {
                    hook_workflow_type: format!("hook-{i}"),
                    event: "pre_run".to_string(),
                    error: format!("err-{i}"),
                })
                .collect(),
            skip,
            skip_reason: reason.map(String::from),
            extra_env: extra_env
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    #[test]
    fn apply_pre_run_outcome_continues_when_no_skip_signal() {
        let plan = empty_plan();
        let mut env = BTreeMap::new();
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(false, None, &[("X", "y")], 1);

        let action = apply_pre_run_outcome(&plan, outcome, &mut env, &mut hook_errors, 0.5);
        assert!(matches!(action, ControlFlow::Continue(())));
        // Side effects: env merged, errors collected.
        assert_eq!(env.get("X").map(String::as_str), Some("y"));
        assert_eq!(hook_errors.len(), 1);
    }

    #[test]
    fn apply_pre_run_outcome_breaks_with_output_on_skip() {
        let plan = ExecutionPlan {
            invocation_id: "inv-skip".to_string(),
            ..empty_plan()
        };
        let mut env = BTreeMap::new();
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(true, Some("not today"), &[("Y", "z")], 0);

        let action = apply_pre_run_outcome(&plan, outcome, &mut env, &mut hook_errors, 1.5);
        let ControlFlow::Break(out) = action else {
            panic!("skip outcome must Break");
        };
        assert!(out.success);
        assert!(out.skipped);
        assert_eq!(out.skip_reason.as_deref(), Some("not today"));
        // env still merged before the break (matches dbt-core: env state is
        // observable even on skip).
        assert_eq!(env.get("Y").map(String::as_str), Some("z"));
    }

    #[test]
    fn apply_pre_run_outcome_drains_hook_errors_on_skip() {
        // Skip path: any errors collected by the pre_run hooks are drained
        // into the returned DbtRunOutput.hook_errors so the caller can
        // surface them to the user.
        let plan = empty_plan();
        let mut env = BTreeMap::new();
        let mut hook_errors = vec![HookError {
            hook_workflow_type: "previous".to_string(),
            event: "pre_run".to_string(),
            error: "warn-1".to_string(),
        }];
        let outcome = make_outcome(true, None, &[], 1);

        let action = apply_pre_run_outcome(&plan, outcome, &mut env, &mut hook_errors, 0.0);
        let ControlFlow::Break(out) = action else {
            panic!("skip must Break");
        };
        // Outer accumulator drained.
        assert!(hook_errors.is_empty());
        // Output collected the previous warn + the new one from the outcome.
        assert_eq!(out.hook_errors.len(), 2);
    }

    #[test]
    fn build_search_attribute_payloads_serializes_each_string() {
        let mut attrs = BTreeMap::new();
        attrs.insert("DbtProject".to_string(), "shop".to_string());
        attrs.insert("DbtCommand".to_string(), "build".to_string());

        let result = build_search_attribute_payloads(&attrs).unwrap();
        assert_eq!(result.len(), 2);

        // Order is BTreeMap-deterministic (alphabetical by key).
        assert_eq!(result[0].0, "DbtCommand");
        assert_eq!(result[1].0, "DbtProject");

        // Each Payload contains the JSON-encoded string value.
        let payload_value: serde_json::Value = serde_json::from_slice(&result[1].1.data).unwrap();
        assert_eq!(payload_value, serde_json::json!("shop"));
    }
}
