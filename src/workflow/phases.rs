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
    HooksConfig, NodeExecutionResult, ProjectHookPhase, ProjectHooksInput, ResolveConfigInput,
    ResolvedProjectConfig, StoreArtifactsInput, StoreArtifactsOutput,
};

use super::DbtRunWorkflow;
use super::helpers::{elapsed_secs, plural};

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
        let sa_payloads: Vec<(String, _)> = plan
            .search_attributes
            .iter()
            .map(|(k, v)| {
                let payload = v.as_json_payload().map_err(|e| {
                    WorkflowTermination::failed(anyhow::anyhow!(
                        "serializing search attribute '{k}' as JSON payload: {e:#}"
                    ))
                })?;
                Ok((k.clone(), payload))
            })
            .collect::<Result<_, WorkflowTermination>>()?;
        ctx.upsert_search_attributes(sa_payloads);
    }

    ctx.set_current_details(format!(
        "planned {} node{} across {} level{}",
        plan.nodes.len(),
        plural(plan.nodes.len()),
        plan.levels.len(),
        plural(plan.levels.len()),
    ));

    Ok(plan)
}

pub async fn resolve_project_config(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
) -> Result<ResolvedProjectConfig, WorkflowTermination> {
    let resolve_input = ResolveConfigInput {
        project: Some(plan.project.clone()),
        input_hooks: input.hooks.clone(),
    };
    ctx.start_activity(
        DbtActivities::resolve_config,
        resolve_input,
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

    let payload = HookPayload {
        event: HookEvent::PreRun,
        invocation_id: plan.invocation_id.clone(),
        input: input.clone(),
        plan: Some(plan.clone()),
        output: None,
    };
    match execute_hooks(ctx, &hooks.pre_run, &payload).await {
        Ok(outcome) => {
            hook_errors.extend(outcome.errors);
            effective_env.extend(outcome.extra_env);
            if outcome.skip {
                return Ok(ControlFlow::Break(DbtRunOutput {
                    invocation_id: plan.invocation_id.clone(),
                    success: true,
                    skipped: true,
                    skip_reason: outcome.skip_reason,
                    node_results: vec![],
                    elapsed_time: elapsed_secs(start, ctx.workflow_time()),
                    artifacts: None,
                    log_path: None,
                    hook_errors: std::mem::take(hook_errors),
                }));
            }
            Ok(ControlFlow::Continue(()))
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
        ProjectHooksInput {
            phase: ProjectHookPhase::OnRunStart,
            project: plan.project.clone(),
            invocation_id: plan.invocation_id.clone(),
            env: effective_env.clone(),
            target: input.target.clone(),
            node_results: vec![],
        },
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
    let store_input = StoreArtifactsInput {
        invocation_id: plan.invocation_id.clone(),
        node_results: all_results.to_vec(),
        manifest_json: plan.manifest_json.clone(),
        manifest_ref: plan.manifest_ref.clone(),
        run_log: Some(log_lines.join("\n")),
    };

    let artifacts: StoreArtifactsOutput = ctx
        .start_activity(
            DbtActivities::store_artifacts,
            store_input,
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
            ProjectHooksInput {
                phase: ProjectHookPhase::OnRunEnd,
                project: plan.project.clone(),
                invocation_id: plan.invocation_id.clone(),
                env: effective_env.clone(),
                target: input.target.clone(),
                node_results: all_results.to_vec(),
            },
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
    let post_hooks = if success_in {
        &hooks.on_success
    } else {
        &hooks.on_failure
    };
    let event = if success_in {
        HookEvent::OnSuccess
    } else {
        HookEvent::OnFailure
    };

    if post_hooks.is_empty() {
        return success_in;
    }

    let payload = HookPayload {
        event,
        invocation_id: plan.invocation_id.clone(),
        input: input.clone(),
        plan: Some(plan.clone()),
        output: Some(output_snapshot.clone()),
    };
    match execute_hooks(ctx, post_hooks, &payload).await {
        Ok(outcome) => {
            hook_errors.extend(outcome.errors);
            success_in
        }
        Err(e) => {
            tracing::error!(event = %event, error = %e, "post-hook failed, marking workflow as failed");
            false
        }
    }
}
