//! Readiness-driven execution of a dbt run's nodes.
//!
//! The workflow-facing driver around [`Scheduler`]: it turns the scheduler's
//! decisions into activities, waits for whichever activity finishes first, and
//! feeds the outcome back. All scheduling decisions live in the scheduler; this
//! module owns the Temporal side — activity commands, the run log, the status
//! tree, memo writes and the continue-as-new hand-off.
//!
//! # Replay
//!
//! Everything here is a pure function of the plan and of the order in which
//! activities complete, which workflow history fixes. Ready nodes start in the
//! scheduler's fixed order, and when several activities have completed by the
//! time the workflow wakes, they are consumed in the order they were started.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{ActivityExecutionError, WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{
    DbtRunInput, ExecutionPlan, NodeExecutionResult, NodeStatus, NodeStatusTree, RetryConfig,
    TimeoutConfig,
};

use super::DbtRunWorkflow;
use super::helpers::{
    activity_error_message, cancelled_result, classify_result_status, error_result,
    format_activity_label, format_progress_line, format_ready_queue_details, format_result_tag,
    node_label, set_node_status, short_activity_error, skipped_result, upsert_memo_state,
};
use super::levels::{
    LevelExecutionOutcome, MAX_ACTIVITIES_IN_FLIGHT, ResumePoint, node_activity_options,
    node_execution_input, should_continue_as_new,
};
use super::scheduler::{Scheduler, SchedulerError, SkipReason, Step};

/// A quiet run is flushed to the memo about this many times in all, however
/// many nodes it has, so memo writes grow with the run's size only up to a
/// fixed bound.
const MEMO_QUIET_FLUSHES: usize = 20;

/// The fewest completions between two quiet memo flushes, so a small run is not
/// flushed after every node.
const MEMO_MIN_COMPLETIONS_PER_FLUSH: usize = 10;

/// A failure is flushed once this fraction (one over this number) of a quiet
/// interval has passed since the last flush: soon enough that operators see it,
/// far enough apart that a run full of failures cannot write the memo per node.
const MEMO_FAILURE_GAP_DIVISOR: usize = 5;

/// When to publish the memo while nodes complete.
///
/// A memo write is a workflow command carrying a full snapshot, so the cadence
/// has to be bounded by the run's size rather than by its node count. There is
/// no clock to lean on, so it counts completions: every `quiet_every` of them,
/// or sooner (after `failure_gap`) when a failure is waiting to be shown.
#[derive(Debug)]
struct MemoCadence {
    quiet_every: usize,
    failure_gap: usize,
    since_flush: usize,
    failure_pending: bool,
}

impl MemoCadence {
    fn new(total_nodes: usize) -> Self {
        let quiet_every = total_nodes
            .div_ceil(MEMO_QUIET_FLUSHES)
            .max(MEMO_MIN_COMPLETIONS_PER_FLUSH);
        Self {
            quiet_every,
            failure_gap: quiet_every.div_ceil(MEMO_FAILURE_GAP_DIVISOR).max(2),
            since_flush: 0,
            failure_pending: false,
        }
    }

    /// Note `events` nodes reaching a terminal status, `failed` if any of them
    /// was a failure. Returns whether the memo should be written now; if so the
    /// counters restart, on the assumption that the caller writes it.
    const fn record(&mut self, events: usize, failed: bool) -> bool {
        self.since_flush += events;
        self.failure_pending |= failed;
        let due = self.since_flush >= self.quiet_every
            || (self.failure_pending && self.since_flush >= self.failure_gap);
        if due {
            self.flushed();
        }
        due
    }

    /// The memo was written for another reason; start counting again.
    const fn flushed(&mut self) {
        self.since_flush = 0;
        self.failure_pending = false;
    }
}

/// An activity future that remembers which node it belongs to.
struct Tagged<F> {
    node: String,
    label: String,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for Tagged<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}

/// Wait for the first of `pending` to finish, and say which one it was.
///
/// Polls in slice order with the workflow's own waker and nothing else: no
/// executor of its own, no wakers of its own, nothing woken from inside the
/// poll. That is the same shape the SDK's `select!` has, and why it is safe in
/// workflow code — what a poll returns depends only on which futures the
/// runtime has resolved, and the runtime resolves them from history. When
/// several are resolved at once the lowest index wins, so the choice is fixed
/// by the order the caller keeps the slice in.
///
/// A finished future is not polled again; the caller removes it before the next
/// call.
async fn next_completed<T: Future + Unpin>(pending: &mut [T]) -> (usize, T::Output) {
    std::future::poll_fn(|cx| {
        for (index, future) in pending.iter_mut().enumerate() {
            if let Poll::Ready(output) = Pin::new(future).poll(cx) {
                return Poll::Ready((index, output));
            }
        }
        Poll::Pending
    })
    .await
}

/// Convert an unexpected scheduler error into a workflow failure.
fn scheduler_failure(e: SchedulerError) -> WorkflowTermination {
    WorkflowTermination::failed_application(ApplicationFailure::non_retryable(e))
}

/// One run segment's ready-queue execution: the scheduler plus everything the
/// segment accumulates while it runs.
pub struct ReadyQueueRun<'a> {
    plan: &'a ExecutionPlan,
    input: &'a DbtRunInput,
    retry_config: &'a RetryConfig,
    timeouts: &'a TimeoutConfig,
    effective_env: &'a BTreeMap<String, String>,
    scheduler: Scheduler,
    total_nodes: usize,
    log_lines: Vec<String>,
    all_results: Vec<NodeExecutionResult>,
    prior_segments: Vec<String>,
    node_status: NodeStatusTree,
    node_counter: usize,
    can_continue: bool,
}

impl<'a> ReadyQueueRun<'a> {
    /// Set up a segment from the same starting point the level loop takes.
    ///
    /// Fails if the plan's edges contain a cycle; the caller then falls back to
    /// the level loop, whose levels are valid by construction.
    pub fn new(
        plan: &'a ExecutionPlan,
        input: &'a DbtRunInput,
        retry_config: &'a RetryConfig,
        timeouts: &'a TimeoutConfig,
        effective_env: &'a BTreeMap<String, String>,
        resume: &ResumePoint,
    ) -> Result<Self, SchedulerError> {
        // A node that finished in an earlier segment is already terminal in the
        // carried status tree; `start_level` is not consulted, so a checkpoint
        // written by the level loop resumes here just the same.
        let scheduler =
            Scheduler::new(plan, &resume.node_status, &resume.failed_nodes, resume.had_failure)?;
        Ok(Self {
            plan,
            input,
            retry_config,
            timeouts,
            effective_env,
            scheduler,
            total_nodes: plan.levels.iter().map(Vec::len).sum(),
            log_lines: resume.log_lines.clone(),
            all_results: resume.all_results.clone(),
            prior_segments: resume.prior_segments.clone(),
            node_status: resume.node_status.clone(),
            node_counter: resume.node_counter,
            can_continue: resume.can_continue,
        })
    }

    /// Run the segment until every node is terminal, the workflow is cancelled,
    /// or history growth asks for a continuation.
    pub async fn execute(
        mut self,
        ctx: &WorkflowContext<DbtRunWorkflow>,
    ) -> Result<LevelExecutionOutcome, WorkflowTermination> {
        let mut in_flight = Vec::new();
        let mut cadence = MemoCadence::new(self.total_nodes);
        let mut memo_written = false;
        let mut completed_this_segment = 0;
        let mut draining = false;
        let mut was_cancelled = false;

        loop {
            // Once history asks for a continuation, nothing new starts: the
            // activities already outstanding finish and are recorded, so the
            // successor resumes from statuses that are all final.
            draining |= should_continue_as_new(
                completed_this_segment,
                self.can_continue,
                ctx.continue_as_new_suggested(),
            );

            // Read on every pass so a `set_fail_fast` update applies to the very
            // next scheduling decision (updates re-apply in history order, so
            // this is the same on replay).
            let fail_fast = ctx
                .state(|s| s.fail_fast_override)
                .unwrap_or(self.input.fail_fast);
            let steps = if draining {
                Vec::new()
            } else {
                self.scheduler
                    .advance(MAX_ACTIVITIES_IN_FLIGHT.saturating_sub(in_flight.len()), fail_fast)
            };

            let mut skips = 0;
            for step in steps {
                match step {
                    Step::Skip { node, reason } => {
                        self.record_skip(&node, reason);
                        skips += 1;
                    }
                    Step::Start(node) => in_flight.push(self.start_node(ctx, node)),
                }
            }

            // The first pass writes the memo once, with the opening nodes
            // already shown as running; later writes follow the cadence.
            let flush = cadence.record(skips, skips > 0);
            if !memo_written || flush {
                upsert_memo_state(ctx, &self.node_status, &self.log_lines)?;
                cadence.flushed();
                memo_written = true;
            }
            self.publish_progress(ctx, &in_flight);

            if self.scheduler.is_finished() {
                break;
            }
            if in_flight.is_empty() {
                if draining {
                    break;
                }
                // Unreachable for an acyclic graph; failing loudly beats a
                // workflow that waits forever on nothing.
                return Err(scheduler_failure(SchedulerError::Stalled));
            }

            let (index, result) = next_completed(in_flight.as_mut_slice()).await;
            let done = in_flight.remove(index);
            completed_this_segment += 1;

            let failed = match result {
                Ok(result) => {
                    let status = self.record_result(&done.node, result);
                    self.scheduler
                        .on_complete(&done.node, status)
                        .map_err(scheduler_failure)?;
                    status == NodeStatus::Error
                }
                Err(ActivityExecutionError::Cancelled(_)) => {
                    // The workflow was cancelled. Activities still outstanding
                    // were cancelled with it and are closed out here, along
                    // with everything that never started.
                    was_cancelled = true;
                    self.record_cancellation(&done.node);
                    self.scheduler
                        .on_complete(&done.node, NodeStatus::Cancelled)
                        .map_err(scheduler_failure)?;
                    self.cancel_remaining();
                    break;
                }
                Err(e) => {
                    self.record_activity_error(&done.node, &e);
                    self.scheduler
                        .on_complete(&done.node, NodeStatus::Error)
                        .map_err(scheduler_failure)?;
                    true
                }
            };
            if cadence.record(1, failed) {
                upsert_memo_state(ctx, &self.node_status, &self.log_lines)?;
            }
        }

        Ok(self.into_outcome(was_cancelled))
    }

    /// Start a node's activity and mark it running.
    fn start_node<'c>(
        &mut self,
        ctx: &'c WorkflowContext<DbtRunWorkflow>,
        node: String,
    ) -> Tagged<impl Future<Output = Result<NodeExecutionResult, ActivityExecutionError>> + use<'c>>
    {
        let label = format_activity_label(self.plan, &node);
        self.record_start(&node);
        let future = ctx.execute_activity(
            DbtActivities::execute_node,
            node_execution_input(self.plan, self.input, self.effective_env, &node),
            node_activity_options(
                self.plan,
                self.timeouts,
                self.retry_config,
                &node,
                label.clone(),
            ),
        );
        Tagged {
            label: label.unwrap_or_else(|| node.clone()),
            node,
            future: Box::pin(future),
        }
    }

    /// What the segment hands back to the orchestrator.
    ///
    /// A segment that stopped with nodes unfinished (and was not cancelled)
    /// stopped to continue as new, at the first level that still has work.
    fn into_outcome(self, was_cancelled: bool) -> LevelExecutionOutcome {
        let continue_at_level = (!was_cancelled && !self.scheduler.is_finished())
            .then(|| self.scheduler.completed_levels());
        LevelExecutionOutcome {
            log_lines: self.log_lines,
            all_results: self.all_results,
            prior_segments: self.prior_segments,
            node_status: self.node_status,
            had_failure: self.scheduler.had_failure(),
            was_cancelled,
            total_nodes: self.total_nodes,
            node_counter: self.node_counter,
            continue_at_level,
            failed_nodes: self.scheduler.failed_nodes(),
        }
    }

    /// Log that a node is starting and mark it running.
    fn record_start(&mut self, node: &str) {
        self.node_counter += 1;
        self.log_lines.push(format_progress_line(
            self.node_counter,
            self.total_nodes,
            "START",
            &node_label(self.plan, node),
        ));
        set_node_status(&mut self.node_status, node, NodeStatus::Running);
    }

    /// Record a node that was skipped without running.
    fn record_skip(&mut self, node: &str, reason: SkipReason) {
        self.node_counter += 1;
        self.log_lines.push(format_progress_line(
            self.node_counter,
            self.total_nodes,
            "SKIP",
            &node_label(self.plan, node),
        ));
        set_node_status(&mut self.node_status, node, NodeStatus::Skipped);
        let message = match reason {
            SkipReason::UpstreamFailed => "skipped: upstream dependency failed",
            SkipReason::FailFast => "skipped due to upstream failure (fail_fast)",
        };
        self.all_results.push(skipped_result(node, message));
    }

    /// Record a finished activity's result and return its classified status.
    fn record_result(&mut self, node: &str, result: NodeExecutionResult) -> NodeStatus {
        let status = classify_result_status(result.status);
        let tag = format_result_tag(status, result.message.as_deref(), result.execution_time);
        self.log_lines
            .push(format!("{}  [{tag}]", node_label(self.plan, node)));
        set_node_status(&mut self.node_status, node, status);
        self.all_results.push(result);
        status
    }

    /// Record an activity that failed outright (retries exhausted, timeout).
    fn record_activity_error(&mut self, node: &str, e: &ActivityExecutionError) {
        self.record_failure(node, &short_activity_error(e), &activity_error_message(e));
    }

    fn record_failure(&mut self, node: &str, short: &str, full: &str) {
        self.log_lines
            .push(format!("{}  [ERROR {short}]", node_label(self.plan, node)));
        set_node_status(&mut self.node_status, node, NodeStatus::Error);
        self.all_results
            .push(error_result(node, &format!("activity failed: {full}")));
    }

    /// Record the activity that reported the workflow's cancellation.
    fn record_cancellation(&mut self, node: &str) {
        self.log_lines
            .push(format!("{}  [CANCELLED]", node_label(self.plan, node)));
        set_node_status(&mut self.node_status, node, NodeStatus::Cancelled);
        self.all_results.push(cancelled_result(node));
    }

    /// Close out every node that has not finished as cancelled, so each planned
    /// node still ends with exactly one result.
    fn cancel_remaining(&mut self) {
        for node in self.scheduler.cancel_unfinished() {
            set_node_status(&mut self.node_status, &node, NodeStatus::Cancelled);
            self.all_results.push(cancelled_result(&node));
        }
    }

    /// Refresh the `run_status` query snapshot and the workflow details line.
    /// Neither costs a history event, so both follow every change.
    fn publish_progress<F>(&self, ctx: &WorkflowContext<DbtRunWorkflow>, in_flight: &[Tagged<F>]) {
        let counts = self.scheduler.counts();
        let completed_levels = self.scheduler.completed_levels();
        ctx.state_mut(|s| {
            s.status.completed_levels = completed_levels;
            s.status.succeeded = counts.success;
            s.status.warned = counts.warn;
            s.status.failed = counts.error;
            s.status.skipped = counts.skipped;
            s.status.running = counts.running;
        });
        let running: Vec<&str> = in_flight.iter().map(|t| t.label.as_str()).collect();
        let done = counts.success + counts.warn + counts.error + counts.skipped;
        ctx.set_current_details(format_ready_queue_details(done, self.total_nodes, &running));
    }
}

#[cfg(test)]
mod tests;
