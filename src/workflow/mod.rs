//! Temporal workflow definition — must be fully deterministic.
//!
//! **No environment variable reads, filesystem I/O, wall-clock time, or random numbers.**
//! All non-deterministic work (SQL rendering, env var reads, DB access) belongs
//! in activities. This constraint is enforced by a test in this module.

// The `#[workflow_methods]` macro generates types without Debug impls.
#![allow(missing_debug_implementations)]

mod helpers;

use std::collections::BTreeSet;
use std::time::Duration;

use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_common::protos::coresdk::workflow_commands::ActivityCancellationType;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};

use crate::activities::DbtActivities;
use crate::hooks::execute_hooks;
use crate::types::{
    CommandMemo, DbtRunInput, DbtRunOutput, ExecutionPlan, HookEvent, HookPayload,
    NodeExecutionInput, NodeExecutionResult, NodeStatus, ProjectHookPhase, ProjectHooksInput,
    ResolveConfigInput, ResolvedProjectConfig, StoreArtifactsInput, StoreArtifactsOutput,
};

use self::helpers::{
    build_effective_env, build_log_header, build_node_status_tree, build_retry_policy,
    build_summary_lines, cancelled_result, elapsed_secs, error_result,
    mark_remaining_level_as_cancelled, node_label, plural, set_node_status, short_activity_error,
    skipped_result, upsert_memo_state, upsert_node_status,
};

/// Cap memo writes for deep DAGs — upsert on this cadence on quiet levels.
/// Levels with failures or cancellation always upsert immediately.
const MEMO_UPSERT_EVERY_N_LEVELS: usize = 10;

/// The main dbt-temporal workflow: plan → execute levels → collect → store artifacts.
#[workflow]
pub struct DbtRunWorkflow {
    input: DbtRunInput,
}

#[workflow_methods]
impl DbtRunWorkflow {
    #[init]
    #[allow(clippy::missing_const_for_fn)] // #[init] macro requires non-const.
    fn new(_ctx: &WorkflowContextView, input: DbtRunInput) -> Self {
        Self { input }
    }

    #[run(name = "dbt_run")]
    #[allow(
        clippy::too_many_lines,
        clippy::format_push_string,
        clippy::needless_pass_by_ref_mut, // Required by the #[run] macro.
        clippy::future_not_send           // WorkflowContext uses Rc internally.
    )]
    // Orchestration function that is inherently sequential and builds a CLI-style run log via
    // push_str(&format!(...)); write!() would require unwrap/discard on the infallible Result.
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<DbtRunOutput> {
        // Deterministic workflow time — the workflow's docstring forbids
        // wall-clock sources because they break history replay.
        let start = ctx.workflow_time();
        let input = ctx.state(|s| s.input.clone());

        // Store command metadata in memo (write-once).
        let command_memo = CommandMemo::from(&input);
        ctx.upsert_memo([(
            "command".to_string(),
            command_memo
                .as_json_payload()
                .map_err(temporalio_sdk::WorkflowTermination::failed)?,
        )]);

        // --- Step 1: Plan ---
        let plan: ExecutionPlan = ctx
            .start_activity(
                DbtActivities::plan_project,
                input.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(300)),
            )
            .await
            .map_err(|e| {
                temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                    "plan_project activity failed: {e:#}"
                ))
            })?;

        // Upsert search attributes from the plan (static config + dynamic values).
        if !plan.search_attributes.is_empty() {
            let sa_payloads: Vec<(String, _)> = plan
                .search_attributes
                .iter()
                .map(|(k, v)| {
                    let payload = v.as_json_payload().map_err(|e| {
                        temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                            "serializing search attribute '{k}' as JSON payload: {e:#}"
                        ))
                    })?;
                    Ok((k.clone(), payload))
                })
                .collect::<Result<_, temporalio_sdk::WorkflowTermination>>()?;
            ctx.upsert_search_attributes(sa_payloads);
        }

        ctx.set_current_details(format!(
            "planned {} node{} across {} level{}",
            plan.nodes.len(),
            plural(plan.nodes.len()),
            plan.levels.len(),
            plural(plan.levels.len()),
        ));

        // --- Resolve project config (hooks + retry) ---
        let resolve_input = ResolveConfigInput {
            project: Some(plan.project.clone()),
            input_hooks: input.hooks.clone(),
        };
        let project_config: ResolvedProjectConfig = ctx
            .start_activity(
                DbtActivities::resolve_config,
                resolve_input,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await
            .map_err(|e| {
                temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                    "resolve_config activity failed: {e:#}"
                ))
            })?;
        let hooks = project_config.hooks;
        let retry_config = project_config.retry;

        let mut all_hook_errors = Vec::new();

        // Effective env for this run: workflow input env (with `_` set to the
        // serialised input), extended by pre_run hook extra_env. Used in
        // NodeExecutionInput so execute_node picks it up for env_var() rendering
        // and per-workflow adapter engine rebuilding (profiles.yml env_var() overrides).
        let mut effective_env = build_effective_env(&input);

        // --- Pre-run hooks ---
        if !hooks.pre_run.is_empty() {
            let payload = HookPayload {
                event: HookEvent::PreRun,
                invocation_id: plan.invocation_id.clone(),
                input: input.clone(),
                plan: Some(plan.clone()),
                output: None,
            };
            match execute_hooks(ctx, &hooks.pre_run, &payload).await {
                Ok(outcome) => {
                    all_hook_errors.extend(outcome.errors);
                    effective_env.extend(outcome.extra_env);
                    if outcome.skip {
                        return Ok(DbtRunOutput {
                            invocation_id: plan.invocation_id.clone(),
                            success: true,
                            skipped: true,
                            skip_reason: outcome.skip_reason,
                            node_results: vec![],
                            elapsed_time: elapsed_secs(start, ctx.workflow_time()),
                            artifacts: None,
                            log_path: None,
                            hook_errors: all_hook_errors,
                        });
                    }
                }
                Err(e) => {
                    // A pre_run hook with on_error: fail — abort the workflow.
                    return Err(temporalio_sdk::WorkflowTermination::failed(e));
                }
            }
        }

        // --- on-run-start hooks (from dbt_project.yml) ---
        // Failure aborts the workflow — no nodes execute if these fail.
        if plan.has_on_run_start {
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
            .map_err(|e| {
                temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                    "on-run-start hook failed: {e:#}"
                ))
            })?;
        }

        // Build the initial node status tree (all nodes pending) and store in memo.
        let mut node_status = build_node_status_tree(&plan);
        upsert_node_status(ctx, &node_status)?;

        // --- Build CLI-style run log (dbt-like output, stored as array in memo) ---
        let total_nodes: usize = plan.levels.iter().map(Vec::len).sum();
        let mut log_lines = build_log_header(&plan);

        let mut node_counter = 0usize;

        // --- Step 2: Execute nodes level by level ---
        let mut all_results: Vec<NodeExecutionResult> = Vec::new();
        let mut had_failure = false;
        let mut failed_nodes: BTreeSet<String> = BTreeSet::new();
        let mut was_cancelled = false;

        for (level_idx, level) in plan.levels.iter().enumerate() {
            if was_cancelled {
                // Workflow was cancelled — mark all remaining nodes.
                for unique_id in level {
                    node_counter += 1;
                    set_node_status(&mut node_status, unique_id, NodeStatus::Cancelled);
                    all_results.push(cancelled_result(unique_id));
                }
                upsert_memo_state(ctx, &node_status, &log_lines)?;
                continue;
            }

            if input.fail_fast && had_failure {
                // Skip remaining levels — mark all nodes as skipped.
                for unique_id in level {
                    node_counter += 1;
                    let label = node_label(&plan, unique_id);
                    log_lines.push(format!("{node_counter} of {total_nodes} SKIP {label}"));
                    set_node_status(&mut node_status, unique_id, NodeStatus::Skipped);
                    all_results.push(skipped_result(
                        unique_id,
                        "skipped due to upstream failure (fail_fast)",
                    ));
                }
                upsert_memo_state(ctx, &node_status, &log_lines)?;
                continue;
            }

            // First pass: determine skip/run for each node, update status and log.
            // Collect nodes to schedule (we need to upsert memo before creating futures
            // because start_activity borrows ctx).
            let mut nodes_to_schedule: Vec<(String, NodeExecutionInput, Option<String>)> =
                Vec::new();
            for unique_id in level {
                // Check if any upstream dependency has failed.
                if let Some(node_info) = plan.nodes.get(unique_id)
                    && node_info
                        .depends_on
                        .iter()
                        .any(|dep| failed_nodes.contains(dep))
                {
                    node_counter += 1;
                    let label = node_label(&plan, unique_id);
                    log_lines.push(format!("{node_counter} of {total_nodes} SKIP {label}"));
                    failed_nodes.insert(unique_id.clone());
                    had_failure = true;
                    set_node_status(&mut node_status, unique_id, NodeStatus::Skipped);
                    all_results
                        .push(skipped_result(unique_id, "skipped: upstream dependency failed"));
                    continue;
                }

                node_counter += 1;
                let node_num = node_counter;
                let label = node_label(&plan, unique_id);
                log_lines.push(format!("{node_num} of {total_nodes} START {label}"));

                set_node_status(&mut node_status, unique_id, NodeStatus::Running);

                let node_input = NodeExecutionInput {
                    unique_id: unique_id.clone(),
                    invocation_id: plan.invocation_id.clone(),
                    project: plan.project.clone(),
                    env: effective_env.clone(),
                    target: input.target.clone(),
                    command: input.command.clone(),
                };

                // Per-node labeling in Temporal UI: activity_id for event details,
                // summary for the Gantt chart display.
                let node_label = plan.nodes.get(unique_id).map(|info| {
                    let rt = info
                        .resource_type
                        .strip_prefix("NODE_TYPE_")
                        .unwrap_or(&info.resource_type)
                        .to_ascii_lowercase();
                    format!("{rt}:{}", info.name)
                });

                nodes_to_schedule.push((unique_id.clone(), node_input, node_label));
            }

            // Memo: mark level as running (skipped nodes already set above).
            // Must happen before start_activity calls which borrow ctx.
            upsert_memo_state(ctx, &node_status, &log_lines)?;

            // Update workflow details with the currently executing models (visible in Temporal UI).
            {
                let running: Vec<&str> = nodes_to_schedule
                    .iter()
                    .filter_map(|(_, _, label)| label.as_deref())
                    .collect();
                if !running.is_empty() {
                    ctx.set_current_details(format!(
                        "level {}/{}: {}",
                        level_idx + 1,
                        plan.levels.len(),
                        running.join(", ")
                    ));
                }
            }

            // Second pass: schedule activities (borrows ctx immutably via futures).
            let mut futures = Vec::new();
            for (unique_id, node_input, node_label) in nodes_to_schedule {
                let future = ctx.start_activity(
                    DbtActivities::execute_node,
                    node_input,
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(3600))
                        .heartbeat_timeout(Duration::from_secs(300))
                        .maybe_activity_id(node_label.clone())
                        .maybe_summary(node_label)
                        .cancellation_type(ActivityCancellationType::TryCancel)
                        .retry_policy(build_retry_policy(&retry_config))
                        .build(),
                );

                futures.push((unique_id, future));
            }

            // Collect results for this level.
            // Track which nodes in this level we've already processed, so we can
            // mark the rest as cancelled if the workflow is cancelled mid-level.
            let level_node_ids: Vec<String> = futures.iter().map(|(id, _)| id.clone()).collect();
            let mut processed_in_level = BTreeSet::new();

            for (unique_id, future) in futures {
                let label = node_label(&plan, &unique_id);

                match future.await {
                    Ok(result) => {
                        processed_in_level.insert(unique_id.clone());

                        let status = match result.status {
                            NodeStatus::Error => {
                                had_failure = true;
                                failed_nodes.insert(unique_id.clone());
                                NodeStatus::Error
                            }
                            NodeStatus::Skipped => NodeStatus::Skipped,
                            _ => NodeStatus::Success,
                        };
                        let tag = match status {
                            NodeStatus::Success => {
                                let detail = result.message.as_deref().unwrap_or("OK");
                                format!("{detail} in {:.2}s", result.execution_time)
                            }
                            NodeStatus::Error => {
                                let msg = result.message.as_deref().unwrap_or("error");
                                format!("ERROR {msg}")
                            }
                            NodeStatus::Skipped => "SKIP".to_string(),
                            _ => format!("{status:?}"),
                        };
                        log_lines.push(format!("{label}  [{tag}]"));
                        set_node_status(&mut node_status, &unique_id, status);
                        all_results.push(result);
                    }
                    Err(e) => {
                        processed_in_level.insert(unique_id.clone());

                        // Check if the activity was cancelled (workflow cancellation).
                        if matches!(&e, temporalio_sdk::ActivityExecutionError::Cancelled(_)) {
                            was_cancelled = true;
                            log_lines.push(format!("{label}  [CANCELLED]"));
                            set_node_status(&mut node_status, &unique_id, NodeStatus::Cancelled);
                            all_results.push(cancelled_result(&unique_id));

                            mark_remaining_level_as_cancelled(
                                &level_node_ids,
                                &processed_in_level,
                                &mut node_status,
                                &mut all_results,
                            );
                            break;
                        }

                        // Activity failed.
                        let short = short_activity_error(&e);
                        log_lines.push(format!("{label}  [ERROR {short}]"));
                        had_failure = true;
                        failed_nodes.insert(unique_id.clone());
                        set_node_status(&mut node_status, &unique_id, NodeStatus::Error);
                        all_results
                            .push(error_result(&unique_id, &format!("activity failed: {e:#}")));
                    }
                }
            }

            // Memo upsert policy: every level on a deep DAG would mean dozens
            // of writes per workflow and risks the ~32 KB memo size limit. We
            // upsert (a) on any level with a failure, since callers need
            // failure visibility quickly, and (b) periodically on quiet
            // levels. The last level falls through to the final upsert below.
            let level_had_failure = processed_in_level
                .iter()
                .any(|id| failed_nodes.contains(id));
            let is_last = level_idx == plan.levels.len() - 1;
            let periodic = (level_idx + 1) % MEMO_UPSERT_EVERY_N_LEVELS == 0;
            if !is_last && (level_had_failure || periodic) {
                upsert_memo_state(ctx, &node_status, &log_lines)?;
            }
        }

        // Final memo upsert with all nodes in terminal state.
        upsert_memo_state(ctx, &node_status, &log_lines)?;

        // If the workflow was cancelled, return early — skip artifact storage and hooks.
        if was_cancelled {
            return Err(temporalio_sdk::WorkflowTermination::Cancelled);
        }

        // --- Run log summary ---
        {
            let pass = all_results
                .iter()
                .filter(|r| r.status == NodeStatus::Success)
                .count();
            let error = all_results
                .iter()
                .filter(|r| r.status == NodeStatus::Error)
                .count();
            let skip = all_results
                .iter()
                .filter(|r| r.status == NodeStatus::Skipped)
                .count();
            let elapsed = elapsed_secs(start, ctx.workflow_time());
            log_lines.extend(build_summary_lines(total_nodes, elapsed, pass, error, skip));
        }

        // Final memo upsert with summary lines.
        upsert_memo_state(ctx, &node_status, &log_lines)?;

        // --- Step 3: Store artifacts (if enabled) ---
        let (artifacts, log_path) = if plan.write_artifacts {
            let run_log = log_lines.join("\n");
            let store_input = StoreArtifactsInput {
                invocation_id: plan.invocation_id.clone(),
                node_results: all_results.clone(),
                manifest_json: plan.manifest_json.clone(),
                manifest_ref: plan.manifest_ref.clone(),
                run_log: Some(run_log),
            };

            let artifacts: StoreArtifactsOutput = ctx
                .start_activity(
                    DbtActivities::store_artifacts,
                    store_input,
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(120)),
                )
                .await
                .map_err(|e| {
                    temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                        "store_artifacts activity failed: {e:#}"
                    ))
                })?;
            let log_path = artifacts.log_path.clone();
            (Some(artifacts), log_path)
        } else {
            (None, None)
        };

        // --- on-run-end hooks (from dbt_project.yml) ---
        // Always fires, even when the run failed (matches dbt-core behaviour).
        // Failure is non-fatal: appended to hook_errors, doesn't change run outcome.
        if plan.has_on_run_end {
            ctx.set_current_details("running on-run-end hooks".to_string());
            match ctx
                .start_activity(
                    DbtActivities::run_project_hooks,
                    ProjectHooksInput {
                        phase: ProjectHookPhase::OnRunEnd,
                        project: plan.project.clone(),
                        invocation_id: plan.invocation_id.clone(),
                        env: effective_env.clone(),
                        target: input.target.clone(),
                        node_results: all_results.clone(),
                    },
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
                        .heartbeat_timeout(Duration::from_secs(120))
                        .build(),
                )
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!(error = %e, "on-run-end hook failed (non-fatal)");
                    all_hook_errors.push(crate::types::HookError {
                        hook_workflow_type: "on_run_end".to_string(),
                        event: "on_run_end".to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }

        // --- Build output (before post-hooks, since hooks receive it) ---
        let mut success = !had_failure;
        let elapsed_time = elapsed_secs(start, ctx.workflow_time());

        let mut output = DbtRunOutput {
            invocation_id: plan.invocation_id.clone(),
            success,
            skipped: false,
            skip_reason: None,
            node_results: all_results,
            elapsed_time,
            log_path,
            artifacts,
            hook_errors: vec![],
        };

        // --- Post-run hooks ---
        let post_hooks = if success {
            &hooks.on_success
        } else {
            &hooks.on_failure
        };
        let event = if success {
            HookEvent::OnSuccess
        } else {
            HookEvent::OnFailure
        };

        if !post_hooks.is_empty() {
            let payload = HookPayload {
                event,
                invocation_id: plan.invocation_id.clone(),
                input: input.clone(),
                plan: Some(plan.clone()),
                output: Some(output.clone()),
            };
            match execute_hooks(ctx, post_hooks, &payload).await {
                Ok(outcome) => all_hook_errors.extend(outcome.errors),
                Err(e) => {
                    // A post-hook with on_error: fail — flip success to false.
                    tracing::error!(event = %event, error = %e, "post-hook failed, marking workflow as failed");
                    success = false;
                }
            }
        }

        output.success = success;
        output.hook_errors = all_hook_errors;

        if success {
            Ok(output)
        } else {
            // Fail the workflow so Temporal marks it FAILED (not COMPLETED).
            // Structured output is already persisted in memo for callers to inspect.
            let error_count = output
                .node_results
                .iter()
                .filter(|r| r.status == NodeStatus::Error)
                .count();
            Err(temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                "dbt run failed: {error_count} node(s) errored (see workflow memo for details)"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    /// Workflow code must be deterministic — env var reads break Temporal replay.
    ///
    /// This test scans the workflow module source files for `std::env::var` calls.
    /// If someone adds an env var read to workflow code, this test will fail and
    /// remind them to move it into an activity.
    #[test]
    #[allow(clippy::expect_used, clippy::unwrap_used)]
    fn workflow_code_does_not_read_env_vars() {
        let workflow_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/workflow");

        for entry in std::fs::read_dir(&workflow_dir).expect("read src/workflow") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "rs") {
                let contents = std::fs::read_to_string(&path).expect("read file");
                let filename = path.file_name().unwrap().to_string_lossy();

                // Skip #[cfg(test)] blocks — test code is not replayed.
                let non_test_content = strip_cfg_test_blocks(&contents);

                assert!(
                    !non_test_content.contains("std::env::var"),
                    "src/workflow/{filename} contains `std::env::var` outside #[cfg(test)]. \
                     Workflow code must be deterministic — move env var reads into an activity."
                );
                assert!(
                    !non_test_content.contains("std::env::set_var"),
                    "src/workflow/{filename} contains `std::env::set_var` outside #[cfg(test)]. \
                     Workflow code must never mutate process environment."
                );
            }
        }
    }

    /// Strip `#[cfg(test)] mod tests { ... }` blocks from source, leaving only
    /// production code for the determinism scan.
    fn strip_cfg_test_blocks(source: &str) -> String {
        // Simple heuristic: find `#[cfg(test)]` and skip until matching closing brace.
        let mut result = String::new();
        let mut chars = source.chars();
        let mut buf = String::new();

        while let Some(c) = chars.next() {
            buf.push(c);

            // Detect #[cfg(test)]
            if buf.ends_with("#[cfg(test)]") {
                // Remove the #[cfg(test)] from result
                let prefix_len = buf.len() - "#[cfg(test)]".len();
                result.push_str(&buf[..prefix_len]);
                buf.clear();

                // Skip until we find the opening brace, then skip the balanced block.
                let mut depth = 0u32;
                for c in chars.by_ref() {
                    if c == '{' {
                        depth += 1;
                    } else if c == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                continue;
            }

            // Flush buffer periodically to avoid unbounded growth.
            if buf.len() > 128 {
                let keep = "#[cfg(test)]".len();
                let mut flush = buf.len() - keep;
                // Ensure we split on a char boundary.
                while flush > 0 && !buf.is_char_boundary(flush) {
                    flush -= 1;
                }
                result.push_str(&buf[..flush]);
                buf = buf[flush..].to_string();
            }
        }
        result.push_str(&buf);
        result
    }

    #[test]
    fn strip_cfg_test_blocks_works() {
        let input = r#"
fn real_code() { std::env::var("X"); }
#[cfg(test)]
mod tests {
    fn test_code() { std::env::var("Y"); }
}
fn more_real() {}
"#;
        let stripped = strip_cfg_test_blocks(input);
        assert!(stripped.contains("std::env::var(\"X\")"));
        assert!(!stripped.contains("std::env::var(\"Y\")"));
        assert!(stripped.contains("more_real"));
    }
}
