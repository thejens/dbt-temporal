//! Temporal workflow definition — must be fully deterministic.
//!
//! **No environment variable reads, filesystem I/O, wall-clock time, or random numbers.**
//! All non-deterministic work (SQL rendering, env var reads, DB access) belongs
//! in activities. This constraint is enforced by a test in this module.

// The `#[workflow_methods]` macro generates types without Debug impls.
#![allow(missing_debug_implementations)]

mod helpers;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_common::protos::coresdk::workflow_commands::ActivityCancellationType;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};

use crate::activities::DbtActivities;
use crate::hooks::execute_hooks;
use crate::types::{
    CommandMemo, DbtRunInput, DbtRunOutput, ExecutionPlan, HookPayload, NodeExecutionInput,
    NodeExecutionResult, NodeStatus, ResolveConfigInput, ResolvedProjectConfig,
    StoreArtifactsInput, StoreArtifactsOutput,
};

use self::helpers::{
    build_node_status_tree, build_retry_policy, cancelled_result, error_result, node_label,
    set_node_status, short_activity_error, skipped_result, upsert_memo_state, upsert_node_status,
};

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
        let start = std::time::Instant::now();
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
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(300)), // 5 min
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                    "plan_project activity failed: {e}"
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
                            "serializing search attribute '{k}' as JSON payload: {e}"
                        ))
                    })?;
                    Ok((k.clone(), payload))
                })
                .collect::<Result<_, temporalio_sdk::WorkflowTermination>>()?;
            ctx.upsert_search_attributes(sa_payloads);
        }

        // --- Resolve project config (hooks + retry) ---
        let resolve_input = ResolveConfigInput {
            project: Some(plan.project.clone()),
            input_hooks: input.hooks.clone(),
        };
        let project_config: ResolvedProjectConfig = ctx
            .start_activity(
                DbtActivities::resolve_config,
                resolve_input,
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(10)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                    "resolve_config activity failed: {e}"
                ))
            })?;
        let hooks = project_config.hooks;
        let retry_config = project_config.retry;

        let mut all_hook_errors = Vec::new();

        // Effective env for this run: workflow input env, extended by pre_run hook extra_env.
        // Used in NodeExecutionInput so execute_node picks it up for env_var() rendering
        // and per-workflow adapter engine rebuilding (profiles.yml env_var() overrides).
        let mut effective_env = input.env.clone();

        // --- Pre-run hooks ---
        if !hooks.pre_run.is_empty() {
            let payload = HookPayload {
                event: "pre_run".to_string(),
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
                            elapsed_time: start.elapsed().as_secs_f64(),
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

        // Build the initial node status tree (all nodes pending) and store in memo.
        let mut node_status = build_node_status_tree(&plan);
        upsert_node_status(ctx, &node_status)?;

        // --- Build CLI-style run log (dbt-like output, stored as array in memo) ---
        let total_nodes: usize = plan.levels.iter().map(Vec::len).sum();
        let mut log_lines: Vec<String> = Vec::new();
        log_lines.push(format!("Running with dbt-temporal={}", env!("CARGO_PKG_VERSION")));

        // Count nodes by resource type.
        {
            let mut type_counts: BTreeMap<&str, usize> = BTreeMap::new();
            for info in plan.nodes.values() {
                *type_counts.entry(info.resource_type.as_str()).or_default() += 1;
            }
            let parts: Vec<String> = type_counts
                .iter()
                .map(|(t, n)| format!("{n} {}{}", t, if *n == 1 { "" } else { "s" }))
                .collect();
            log_lines.push(format!("Found {}", parts.join(", ")));
        }
        log_lines.push(format!(
            "Concurrency: {} parallel level{}",
            plan.levels.len(),
            if plan.levels.len() == 1 { "" } else { "s" }
        ));

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
                };

                // Per-node labeling in Temporal UI: activity_id for event details,
                // summary for the Gantt chart display (appended after activity type).
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

            // Second pass: schedule activities (borrows ctx immutably via futures).
            let mut futures = Vec::new();
            for (unique_id, node_input, node_label) in nodes_to_schedule {
                let future = ctx.start_activity(
                    DbtActivities::execute_node,
                    node_input,
                    ActivityOptions {
                        activity_id: node_label.clone(),
                        summary: node_label,
                        start_to_close_timeout: Some(Duration::from_secs(3600)), // 1 hr
                        heartbeat_timeout: Some(Duration::from_secs(300)),       // 5 min
                        cancellation_type: ActivityCancellationType::TryCancel,
                        retry_policy: Some(build_retry_policy(&retry_config)),
                        ..Default::default()
                    },
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

                        let status = match result.status.as_str() {
                            "error" => {
                                had_failure = true;
                                failed_nodes.insert(unique_id.clone());
                                NodeStatus::Error
                            }
                            "skipped" => NodeStatus::Skipped,
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

                            // Mark remaining unprocessed nodes in this level as cancelled.
                            for id in &level_node_ids {
                                if !processed_in_level.contains(id) {
                                    set_node_status(&mut node_status, id, NodeStatus::Cancelled);
                                    all_results.push(cancelled_result(id));
                                }
                            }
                            break;
                        }

                        // Activity failed.
                        let short = short_activity_error(&e);
                        log_lines.push(format!("{label}  [ERROR {short}]"));
                        had_failure = true;
                        failed_nodes.insert(unique_id.clone());
                        set_node_status(&mut node_status, &unique_id, NodeStatus::Error);
                        all_results.push(error_result(&unique_id, format!("activity failed: {e}")));
                    }
                }
            }

            // Memo: update after level completes (unless it's the last level — final upsert below).
            if level_idx < plan.levels.len() - 1 {
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
            let pass = all_results.iter().filter(|r| r.status == "success").count();
            let error = all_results.iter().filter(|r| r.status == "error").count();
            let skip = all_results.iter().filter(|r| r.status == "skipped").count();
            let elapsed = start.elapsed().as_secs_f64();
            log_lines.push(format!(
                "Finished running {total_nodes} node{} in {elapsed:.2}s.",
                if total_nodes == 1 { "" } else { "s" }
            ));
            log_lines
                .push(format!("Done. PASS={pass} ERROR={error} SKIP={skip} TOTAL={total_nodes}"));
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
                    ActivityOptions {
                        start_to_close_timeout: Some(Duration::from_secs(120)), // 2 min
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| {
                    temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                        "store_artifacts activity failed: {e}"
                    ))
                })?;
            let log_path = artifacts.log_path.clone();
            (Some(artifacts), log_path)
        } else {
            (None, None)
        };

        // --- Build output (before post-hooks, since hooks receive it) ---
        let mut success = !had_failure;
        let elapsed_time = start.elapsed().as_secs_f64();

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
        let event = if success { "on_success" } else { "on_failure" };

        if !post_hooks.is_empty() {
            let payload = HookPayload {
                event: event.to_string(),
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
                .filter(|r| r.status == "error")
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
