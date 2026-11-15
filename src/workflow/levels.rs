//! Per-level activity scheduling for the dbt run.
//!
//! Owns the workflow's level loop: fail-fast skipping, dependency-skip on
//! upstream failure, mid-level cancellation cleanup, and the periodic memo
//! upsert cadence. Splits naturally into a driver (`execute_levels`) and the
//! per-level helper (`execute_one_level`).

// WorkflowContext uses Rc internally — the run() method already silences
// this same lint for the same reason.
#![allow(clippy::future_not_send)]

use std::collections::BTreeSet;

use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{
    DbtRunInput, ExecutionPlan, NodeActivityInput, NodeExecutionResult, NodeStatus, NodeStatusTree,
    RetryConfig, RunScope, TimeoutConfig,
};

use super::DbtRunWorkflow;
use super::helpers::{
    MEMO_CADENCE_PATCH, TRANSITIVE_SKIP_PATCH, UNIQUE_ACTIVITY_ID_PATCH, activity_error_message,
    build_log_header, build_node_status_tree, cancelled_result, classify_result_status,
    error_result, format_activity_id, format_activity_label, format_progress_line,
    format_result_tag, format_running_details, is_blocked_by_failed_upstream,
    mark_remaining_level_as_cancelled, node_label, set_node_status, short_activity_error,
    should_announce_level, should_flush_memo, skipped_result, upsert_memo_state,
    upsert_node_status,
};
use super::node_activity::{node_activity_options, node_execution_input};

/// Cap memo writes for deep DAGs — upsert on this cadence on quiet levels.
/// Levels with failures or cancellation always upsert immediately.
const MEMO_UPSERT_EVERY_N_LEVELS: usize = 10;

/// Everything the level loop produces, returned to the orchestrator.
pub struct LevelExecutionOutcome {
    /// This segment's log lines and results — earlier segments left theirs in
    /// their own checkpoints, named by `prior_segments`.
    pub log_lines: Vec<String>,
    pub all_results: Vec<NodeExecutionResult>,
    /// Checkpoints written by every earlier segment of this run, oldest first.
    pub prior_segments: Vec<String>,
    pub node_status: NodeStatusTree,
    pub had_failure: bool,
    pub was_cancelled: bool,
    pub total_nodes: usize,
    /// Nodes numbered so far, so progress lines stay continuous if the run
    /// continues as new.
    pub node_counter: usize,
    /// Set when the loop stopped early because Temporal signalled that the
    /// history is getting large. Holds the level the successor should start
    /// from; `None` means the run finished every level.
    pub continue_at_level: Option<usize>,
    /// Nodes that failed, carried across a continuation so downstream skipping
    /// keeps working in the next segment.
    pub failed_nodes: BTreeSet<String>,
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
    timeouts: &'a TimeoutConfig,
    run_scope: &'a RunScope,
    total_nodes: usize,
    log_lines: &'a mut Vec<String>,
    all_results: &'a mut Vec<NodeExecutionResult>,
    node_status: &'a mut NodeStatusTree,
    failed_nodes: &'a mut BTreeSet<String>,
    had_failure: &'a mut bool,
    was_cancelled: &'a mut bool,
    node_counter: &'a mut usize,
    /// Activity ids come from the node's `unique_id` ([`UNIQUE_ACTIVITY_ID_PATCH`]).
    unique_activity_ids: bool,
    /// A skip carries through a skipped dependency ([`TRANSITIVE_SKIP_PATCH`]).
    transitive_skips: bool,
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
    timeouts: &TimeoutConfig,
    run_scope: &RunScope,
    resume: ResumePoint,
) -> Result<LevelExecutionOutcome, WorkflowTermination> {
    let mut node_status = resume.node_status;
    upsert_node_status(ctx, &node_status)?;

    let total_nodes: usize = plan.levels.iter().map(Vec::len).sum();
    let mut log_lines = resume.log_lines;
    let mut all_results = resume.all_results;
    let mut failed_nodes = resume.failed_nodes;
    let mut had_failure = resume.had_failure;
    let mut was_cancelled = false;
    let mut node_counter = resume.node_counter;
    let mut continue_at_level = None;

    // Each patch is consulted once per run, so every level agrees and the
    // marker is recorded once.
    let unique_activity_ids = ctx.patched(UNIQUE_ACTIVITY_ID_PATCH);
    let transitive_skips = ctx.patched(TRANSITIVE_SKIP_PATCH);

    for (level_idx, level) in plan.levels.iter().enumerate().skip(resume.start_level) {
        // Stop *between* levels only: a level's activities are already
        // scheduled and awaited as a unit, and abandoning them mid-flight
        // would lose results the history has already recorded.
        if should_continue_as_new(
            level_idx - resume.start_level,
            resume.can_continue,
            ctx.continue_as_new_suggested(),
        ) {
            continue_at_level = Some(level_idx);
            break;
        }
        let mut state = LevelState {
            plan,
            input,
            retry_config,
            timeouts,
            run_scope,
            total_nodes,
            log_lines: &mut log_lines,
            all_results: &mut all_results,
            node_status: &mut node_status,
            failed_nodes: &mut failed_nodes,
            had_failure: &mut had_failure,
            was_cancelled: &mut was_cancelled,
            node_counter: &mut node_counter,
            unique_activity_ids,
            transitive_skips,
        };
        execute_one_level(ctx, &mut state, level_idx, level).await?;
    }

    Ok(LevelExecutionOutcome {
        log_lines,
        all_results,
        prior_segments: resume.prior_segments,
        node_status,
        had_failure,
        was_cancelled,
        total_nodes,
        node_counter,
        continue_at_level,
        failed_nodes,
    })
}

/// Where the level loop starts and what it starts with.
///
/// A fresh run passes `ResumePoint::fresh`; a continued one rebuilds this from
/// the state its predecessor spilled.
pub struct ResumePoint {
    pub start_level: usize,
    /// Empty on resume: the predecessor's log and results stay in its
    /// checkpoint rather than travelling into this segment's history.
    pub log_lines: Vec<String>,
    pub all_results: Vec<NodeExecutionResult>,
    pub prior_segments: Vec<String>,
    pub node_status: NodeStatusTree,
    pub failed_nodes: BTreeSet<String>,
    pub had_failure: bool,
    pub node_counter: usize,
    /// Whether continuing as new is possible at all. False when the worker has
    /// no artifact store to spill state to, in which case the run pushes on and
    /// accepts the history growth rather than failing.
    pub can_continue: bool,
}

impl ResumePoint {
    /// Starting state for a run that has not continued.
    pub fn fresh(plan: &ExecutionPlan, can_continue: bool) -> Self {
        Self {
            start_level: 0,
            log_lines: build_log_header(plan),
            all_results: Vec::new(),
            prior_segments: Vec::new(),
            node_status: build_node_status_tree(plan),
            failed_nodes: BTreeSet::new(),
            had_failure: false,
            node_counter: 0,
            can_continue,
        }
    }
}

/// Whether to hand the rest of the run to a fresh execution.
///
/// Driven by Temporal's own `continue_as_new_suggested`, which the server sets
/// as history approaches its limits — rather than a node count we would have to
/// keep in step with event-per-node cost.
///
/// Every segment must execute at least one level before handing off, so the
/// count is of levels done *in this segment* rather than the absolute level
/// index. A resumed segment starts at a non-zero index, and gating on that
/// index would let a run that is already over the threshold on arrival continue
/// forever without ever executing anything.
pub(super) const fn should_continue_as_new(
    levels_done_this_segment: usize,
    can_continue: bool,
    suggested: bool,
) -> bool {
    can_continue && levels_done_this_segment > 0 && suggested
}

/// How many node activities one level may have outstanding at a time.
///
/// Temporal Cloud caps a workflow at 2,000 incomplete activities and recommends
/// no more than 500 concurrent operations; a self-hosted server has its own
/// limits on incomplete activities and on the size of one workflow task's
/// command batch. A DAG level's width is a property of the project, not of any
/// of that, so it has to be bounded here.
pub(super) const MAX_ACTIVITIES_IN_FLIGHT: usize = 500;

/// Take the nodes that may start now, given how many are already outstanding.
///
/// Separated from the scheduling itself so the bound is testable: everything
/// around it needs a live workflow context, and "how many activities may be in
/// flight" is the part that has to be right.
fn take_startable<T>(pending: &mut std::collections::VecDeque<T>, in_flight: usize) -> Vec<T> {
    let room = MAX_ACTIVITIES_IN_FLIGHT.saturating_sub(in_flight);
    pending.drain(..room.min(pending.len())).collect()
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
    // One memo policy for the whole loop — start, periodic progress, failure,
    // cancellation, terminal flush — instead of an unconditional write per
    // level plus a cadence check that could only ever suppress the second one.
    // Gated so a run recorded before this keeps its original command sequence.
    let bounded_memo_cadence = ctx.patched(MEMO_CADENCE_PATCH);

    if *state.was_cancelled {
        // Workflow was cancelled — mark all remaining nodes.
        for unique_id in level {
            *state.node_counter += 1;
            set_node_status(state.node_status, unique_id, NodeStatus::Cancelled);
            state.all_results.push(cancelled_result(unique_id));
        }
        // No memo write: the run is ending, and the orchestrator's terminal
        // flush publishes every node's final state once. Writing here put a
        // full snapshot in history for each remaining level of a deep DAG.
        if !bounded_memo_cadence {
            upsert_memo_state(ctx, state.node_status, state.log_lines)?;
        }
        return Ok(());
    }

    // Effective fail-fast: workflow input unless overridden mid-run by the
    // set_fail_fast update handler. Read at the level boundary so a toggle
    // applies from the next level onward (deterministic on replay — updates
    // re-apply in history order).
    let fail_fast = ctx
        .state(|s| s.fail_fast_override)
        .unwrap_or(state.input.fail_fast);
    if fail_fast && *state.had_failure {
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
        // Same as the cancelled tail: the terminal flush covers it.
        if !bounded_memo_cadence {
            upsert_memo_state(ctx, state.node_status, state.log_lines)?;
        }
        return Ok(());
    }

    // First pass: classify each node as skip-due-to-upstream-failure or schedule.
    // Must collect schedule list before any execute_activity call (which borrows
    // ctx) so we can upsert memo first.
    let mut nodes_to_schedule: Vec<(String, NodeActivityInput, Option<String>)> = Vec::new();
    for unique_id in level {
        if is_blocked_by_failed_upstream(
            state.plan,
            unique_id,
            state.failed_nodes,
            state.node_status,
            state.transitive_skips,
        ) {
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

        let node_input = node_execution_input(state.plan, state.run_scope, unique_id);

        // Per-node labelling in Temporal UI: the summary is the Gantt chart
        // display; the activity id is chosen where the activity is started.
        let activity_label = format_activity_label(state.plan, unique_id);

        nodes_to_schedule.push((unique_id.clone(), node_input, activity_label));
    }

    // Memo: mark level as running (skipped nodes already set above).
    // Must happen before execute_activity calls which borrow ctx.
    if !bounded_memo_cadence || should_announce_level(level_idx, MEMO_UPSERT_EVERY_N_LEVELS) {
        upsert_memo_state(ctx, state.node_status, state.log_lines)?;
    }

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

    // Second pass: schedule activities, at most `MAX_ACTIVITIES_IN_FLIGHT` of
    // them outstanding at a time.
    //
    // Worker concurrency bounds how many nodes *execute*; it does nothing about
    // how many are scheduled. One command per node in a single workflow task
    // would let a project with a few thousand independent models exceed the
    // server's limits on incomplete activities and on transaction size before
    // the between-levels continuation check ever ran.
    //
    // Scheduling and awaiting both follow the level's own order, so replay sees
    // the same commands in the same sequence, and a level narrower than the
    // window is scheduled in one go.
    let level_node_ids: Vec<String> = nodes_to_schedule
        .iter()
        .map(|(unique_id, _, _)| unique_id.clone())
        .collect();
    let mut pending: std::collections::VecDeque<_> = nodes_to_schedule.into();
    let mut futures = std::collections::VecDeque::new();

    // Take the next node off the queue and start it, if the window has room.
    let fill_window =
        |pending: &mut std::collections::VecDeque<(String, NodeActivityInput, Option<String>)>,
         futures: &mut std::collections::VecDeque<_>| {
            for (unique_id, node_input, activity_label) in take_startable(pending, futures.len()) {
                let activity_id =
                    format_activity_id(state.plan, &unique_id, state.unique_activity_ids);
                let future = ctx.execute_activity(
                    DbtActivities::execute_node,
                    node_input,
                    node_activity_options(
                        state.plan,
                        state.timeouts,
                        state.retry_config,
                        &unique_id,
                        activity_id,
                        activity_label,
                    ),
                );
                futures.push_back((unique_id, future));
            }
        };
    fill_window(&mut pending, &mut futures);

    // Collect results for this level. Track which nodes have been processed
    // so we can mark the rest as cancelled if the workflow is cancelled mid-level.
    let mut processed_in_level = BTreeSet::new();

    while let Some((unique_id, future)) = futures.pop_front() {
        let label = node_label(state.plan, &unique_id);

        // One node leaving the window lets the next one in.
        fill_window(&mut pending, &mut futures);

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
                state.all_results.push(error_result(
                    &unique_id,
                    &format!("activity failed: {}", activity_error_message(&e)),
                ));
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

    // Refresh the run_status query snapshot — unlike the memo this is free
    // (no history events), so it updates on every level.
    ctx.state_mut(|s| {
        s.status.completed_levels = level_idx + 1;
        s.status.tally(state.node_status);
    });

    Ok(())
}

#[cfg(test)]
mod tests;
