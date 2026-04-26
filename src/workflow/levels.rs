//! Per-level activity scheduling for the dbt run.
//!
//! Owns the workflow's level loop: fail-fast skipping, dependency-skip on
//! upstream failure, mid-level cancellation cleanup, and the periodic memo
//! upsert cadence. Splits naturally into a driver (`execute_levels`) and the
//! per-level helper (`execute_one_level`).

// WorkflowContext uses Rc internally — the run() method already silences
// this same lint for the same reason.
#![allow(clippy::future_not_send)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use temporalio_common::protos::coresdk::workflow_commands::ActivityCancellationType;
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{
    DbtRunInput, ExecutionPlan, NodeExecutionInput, NodeExecutionResult, NodeStatus,
    NodeStatusTree, RetryConfig,
};

use super::DbtRunWorkflow;
use super::helpers::{
    build_log_header, build_node_status_tree, build_retry_policy, cancelled_result,
    classify_result_status, error_result, format_activity_label, format_progress_line,
    format_result_tag, format_running_details, is_blocked_by_failed_upstream,
    mark_remaining_level_as_cancelled, node_label, set_node_status, short_activity_error,
    should_flush_memo, skipped_result, upsert_memo_state, upsert_node_status,
};

/// Cap memo writes for deep DAGs — upsert on this cadence on quiet levels.
/// Levels with failures or cancellation always upsert immediately.
const MEMO_UPSERT_EVERY_N_LEVELS: usize = 10;

/// Everything the level loop produces, returned to the orchestrator.
pub struct LevelExecutionOutcome {
    pub log_lines: Vec<String>,
    pub all_results: Vec<NodeExecutionResult>,
    pub node_status: NodeStatusTree,
    pub had_failure: bool,
    pub was_cancelled: bool,
    pub total_nodes: usize,
}

/// Mutable accumulators + read-only references for one level's processing.
///
/// All fields are independent borrows from `execute_levels`'s locals; bundling
/// them here keeps `execute_one_level`'s argument list manageable. The
/// borrow checker tracks each field independently, so internal helpers can
/// re-borrow `&mut state.log_lines` while another helper holds
/// `&state.plan` — same shape as iterating fields in a normal struct.
struct LevelState<'a> {
    plan: &'a ExecutionPlan,
    input: &'a DbtRunInput,
    retry_config: &'a RetryConfig,
    effective_env: &'a BTreeMap<String, String>,
    total_nodes: usize,
    log_lines: &'a mut Vec<String>,
    all_results: &'a mut Vec<NodeExecutionResult>,
    node_status: &'a mut NodeStatusTree,
    failed_nodes: &'a mut BTreeSet<String>,
    had_failure: &'a mut bool,
    was_cancelled: &'a mut bool,
    node_counter: &'a mut usize,
}

/// Drive the level loop. Returns the accumulated state regardless of whether
/// the workflow was cancelled — cancellation handling stays in the orchestrator
/// (which inspects `was_cancelled` to decide whether to skip artifact storage
/// and post-run hooks).
pub async fn execute_levels(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    retry_config: &RetryConfig,
    effective_env: &BTreeMap<String, String>,
) -> Result<LevelExecutionOutcome, WorkflowTermination> {
    let mut node_status = build_node_status_tree(plan);
    upsert_node_status(ctx, &node_status)?;

    let total_nodes: usize = plan.levels.iter().map(Vec::len).sum();
    let mut log_lines = build_log_header(plan);
    let mut all_results: Vec<NodeExecutionResult> = Vec::new();
    let mut failed_nodes: BTreeSet<String> = BTreeSet::new();
    let mut had_failure = false;
    let mut was_cancelled = false;
    let mut node_counter = 0usize;

    for (level_idx, level) in plan.levels.iter().enumerate() {
        let mut state = LevelState {
            plan,
            input,
            retry_config,
            effective_env,
            total_nodes,
            log_lines: &mut log_lines,
            all_results: &mut all_results,
            node_status: &mut node_status,
            failed_nodes: &mut failed_nodes,
            had_failure: &mut had_failure,
            was_cancelled: &mut was_cancelled,
            node_counter: &mut node_counter,
        };
        execute_one_level(ctx, &mut state, level_idx, level).await?;
    }

    Ok(LevelExecutionOutcome {
        log_lines,
        all_results,
        node_status,
        had_failure,
        was_cancelled,
        total_nodes,
    })
}

#[allow(clippy::too_many_lines)]
// Sequential phases (skip-checks → schedule → collect → memo) are clearer
// inline than as a tower of single-call helpers.
async fn execute_one_level(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    state: &mut LevelState<'_>,
    level_idx: usize,
    level: &[String],
) -> Result<(), WorkflowTermination> {
    if *state.was_cancelled {
        // Workflow was cancelled — mark all remaining nodes.
        for unique_id in level {
            *state.node_counter += 1;
            set_node_status(state.node_status, unique_id, NodeStatus::Cancelled);
            state.all_results.push(cancelled_result(unique_id));
        }
        upsert_memo_state(ctx, state.node_status, state.log_lines)?;
        return Ok(());
    }

    if state.input.fail_fast && *state.had_failure {
        // Skip remaining levels — mark all nodes as skipped.
        for unique_id in level {
            *state.node_counter += 1;
            let label = node_label(state.plan, unique_id);
            state.log_lines.push(format_progress_line(
                *state.node_counter,
                state.total_nodes,
                "SKIP",
                &label,
            ));
            set_node_status(state.node_status, unique_id, NodeStatus::Skipped);
            state
                .all_results
                .push(skipped_result(unique_id, "skipped due to upstream failure (fail_fast)"));
        }
        upsert_memo_state(ctx, state.node_status, state.log_lines)?;
        return Ok(());
    }

    // First pass: classify each node as skip-due-to-upstream-failure or schedule.
    // Must collect schedule list before any start_activity call (which borrows
    // ctx) so we can upsert memo first.
    let mut nodes_to_schedule: Vec<(String, NodeExecutionInput, Option<String>)> = Vec::new();
    for unique_id in level {
        if is_blocked_by_failed_upstream(state.plan, unique_id, state.failed_nodes) {
            *state.node_counter += 1;
            let label = node_label(state.plan, unique_id);
            state.log_lines.push(format_progress_line(
                *state.node_counter,
                state.total_nodes,
                "SKIP",
                &label,
            ));
            state.failed_nodes.insert(unique_id.clone());
            *state.had_failure = true;
            set_node_status(state.node_status, unique_id, NodeStatus::Skipped);
            state
                .all_results
                .push(skipped_result(unique_id, "skipped: upstream dependency failed"));
            continue;
        }

        *state.node_counter += 1;
        let node_num = *state.node_counter;
        let label = node_label(state.plan, unique_id);
        state
            .log_lines
            .push(format_progress_line(node_num, state.total_nodes, "START", &label));

        set_node_status(state.node_status, unique_id, NodeStatus::Running);

        let node_input = NodeExecutionInput {
            unique_id: unique_id.clone(),
            invocation_id: state.plan.invocation_id.clone(),
            project: state.plan.project.clone(),
            env: state.effective_env.clone(),
            target: state.input.target.clone(),
            command: state.input.command.clone(),
        };

        // Per-node labelling in Temporal UI: activity_id for event details,
        // summary for the Gantt chart display.
        let activity_label = format_activity_label(state.plan, unique_id);

        nodes_to_schedule.push((unique_id.clone(), node_input, activity_label));
    }

    // Memo: mark level as running (skipped nodes already set above).
    // Must happen before start_activity calls which borrow ctx.
    upsert_memo_state(ctx, state.node_status, state.log_lines)?;

    // Update workflow details with the currently executing models (visible in Temporal UI).
    {
        let running: Vec<&str> = nodes_to_schedule
            .iter()
            .filter_map(|(_, _, label)| label.as_deref())
            .collect();
        if !running.is_empty() {
            ctx.set_current_details(format_running_details(
                level_idx,
                state.plan.levels.len(),
                &running,
            ));
        }
    }

    // Second pass: schedule activities.
    let mut futures = Vec::new();
    for (unique_id, node_input, activity_label) in nodes_to_schedule {
        let future = ctx.start_activity(
            DbtActivities::execute_node,
            node_input,
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(3600))
                .heartbeat_timeout(Duration::from_secs(300))
                .maybe_activity_id(activity_label.clone())
                .maybe_summary(activity_label)
                .cancellation_type(ActivityCancellationType::TryCancel)
                .retry_policy(build_retry_policy(state.retry_config))
                .build(),
        );
        futures.push((unique_id, future));
    }

    // Collect results for this level. Track which nodes have been processed
    // so we can mark the rest as cancelled if the workflow is cancelled mid-level.
    let level_node_ids: Vec<String> = futures.iter().map(|(id, _)| id.clone()).collect();
    let mut processed_in_level = BTreeSet::new();

    for (unique_id, future) in futures {
        let label = node_label(state.plan, &unique_id);

        match future.await {
            Ok(result) => {
                processed_in_level.insert(unique_id.clone());

                let status = classify_result_status(result.status);
                if status == NodeStatus::Error {
                    *state.had_failure = true;
                    state.failed_nodes.insert(unique_id.clone());
                }
                let tag =
                    format_result_tag(status, result.message.as_deref(), result.execution_time);
                state.log_lines.push(format!("{label}  [{tag}]"));
                set_node_status(state.node_status, &unique_id, status);
                state.all_results.push(result);
            }
            Err(e) => {
                processed_in_level.insert(unique_id.clone());

                if matches!(&e, temporalio_sdk::ActivityExecutionError::Cancelled(_)) {
                    *state.was_cancelled = true;
                    state.log_lines.push(format!("{label}  [CANCELLED]"));
                    set_node_status(state.node_status, &unique_id, NodeStatus::Cancelled);
                    state.all_results.push(cancelled_result(&unique_id));

                    mark_remaining_level_as_cancelled(
                        &level_node_ids,
                        &processed_in_level,
                        state.node_status,
                        state.all_results,
                    );
                    break;
                }

                let short = short_activity_error(&e);
                state.log_lines.push(format!("{label}  [ERROR {short}]"));
                *state.had_failure = true;
                state.failed_nodes.insert(unique_id.clone());
                set_node_status(state.node_status, &unique_id, NodeStatus::Error);
                state
                    .all_results
                    .push(error_result(&unique_id, &format!("activity failed: {e:#}")));
            }
        }
    }

    // Memo upsert policy: every level on a deep DAG would mean dozens of writes
    // per workflow and risks the ~32 KB memo size limit. Upsert (a) on any
    // level with a failure since callers need failure visibility quickly, and
    // (b) periodically on quiet levels. The last level falls through to the
    // final upsert by the orchestrator.
    let level_had_failure = processed_in_level
        .iter()
        .any(|id| state.failed_nodes.contains(id));
    if should_flush_memo(
        level_idx,
        state.plan.levels.len(),
        level_had_failure,
        MEMO_UPSERT_EVERY_N_LEVELS,
    ) {
        upsert_memo_state(ctx, state.node_status, state.log_lines)?;
    }

    Ok(())
}
