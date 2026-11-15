//! How a `dbt_run` ends.
//!
//! Every exit from the run's phases passes through [`finish_run`], so the
//! terminal `DbtStatus`, the failure hooks and the reporting of cancellation
//! are decided in one place instead of before each early return.

use std::sync::Arc;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{WorkflowContext, WorkflowResult, WorkflowTermination};

use crate::error::truncate_at_char_boundary;
use crate::types::{
    DbtRunInput, DbtRunOutput, ExecutionPlan, HookError, HooksConfig, NodeExecutionResult,
    NodeStatusTree, RunOutcome,
};

use super::DbtRunWorkflow;
use super::helpers::{
    FAILURE_FINALIZATION_PATCH, NodeStatusSummary, elapsed_secs, format_final_details,
};
use super::phases::{run_post_hooks, upsert_terminal_status};

/// What the phases had established when one of them failed.
#[derive(Default)]
pub struct RunProgress {
    /// Set once the run is planned; before that there is no invocation to
    /// report on and no `DbtStatus` to correct.
    pub plan: Option<Arc<ExecutionPlan>>,
    /// Set once the project's configuration is resolved; the failure hooks
    /// come from it.
    pub hooks: Option<Arc<HooksConfig>>,
    /// Node results, when the failure came after the nodes ran.
    pub results: Vec<NodeExecutionResult>,
    pub results_partial: bool,
}

/// A run that reached its end by way of its phases rather than an exit.
pub enum RunEnd {
    /// `list`: the selected nodes, nothing executed.
    Listed(DbtRunOutput),
    /// A pre_run hook asked for the run to be skipped.
    Skipped(DbtRunOutput),
    /// The nodes ran and the artifacts are stored; only the post-run hooks and
    /// the verdict remain.
    Executed(Box<ExecutedRun>),
}

pub struct ExecutedRun {
    pub plan: Arc<ExecutionPlan>,
    pub hooks: Arc<HooksConfig>,
    /// The run as it stands before the post-run hooks, which can still turn a
    /// success into a failure.
    pub output: DbtRunOutput,
    pub node_status: NodeStatusTree,
    pub hook_errors: Vec<HookError>,
    pub had_failure: bool,
}

/// Longest failure message put on the workflow's details line.
const DETAILS_MAX_BYTES: usize = 200;

/// End the run: publish its terminal state and return its result.
///
/// `ended` is what the phases produced. A cancellation or failure that arrives
/// as an `Err` is finished here; continue-as-new and eviction are not endings
/// and pass through untouched.
pub async fn finish_run(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    start: Option<std::time::SystemTime>,
    progress: RunProgress,
    ended: Result<RunEnd, WorkflowTermination>,
) -> WorkflowResult<DbtRunOutput> {
    match ended {
        Ok(RunEnd::Listed(out)) => {
            upsert_terminal_status(ctx, progress.plan.as_deref(), RunOutcome::Passed)?;
            ctx.set_current_details("list".to_string());
            Ok(out)
        }
        Ok(RunEnd::Skipped(out)) => {
            upsert_terminal_status(ctx, progress.plan.as_deref(), RunOutcome::Skipped)?;
            ctx.set_current_details("skipped by pre_run hook".to_string());
            Ok(out)
        }
        Ok(RunEnd::Executed(run)) => finish_executed(ctx, input, *run).await,
        Err(cancelled @ WorkflowTermination::Cancelled { .. }) => {
            upsert_terminal_status(ctx, progress.plan.as_deref(), RunOutcome::Cancelled)?;
            ctx.set_current_details("cancelled".to_string());
            Err(cancelled)
        }
        Err(failure @ WorkflowTermination::Failed(_)) => {
            if let Some(plan) = progress.plan.as_deref()
                && ctx.patched(FAILURE_FINALIZATION_PATCH)
            {
                fail_run(ctx, input, start, plan, &progress, &failure).await?;
            }
            Err(failure)
        }
        Err(other) => Err(other),
    }
}

/// Run the post-run hooks over a completed run and settle its verdict.
async fn finish_executed(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    run: ExecutedRun,
) -> WorkflowResult<DbtRunOutput> {
    let ExecutedRun {
        plan,
        hooks,
        mut output,
        node_status,
        mut hook_errors,
        had_failure,
    } = run;

    let success =
        run_post_hooks(ctx, input, &plan, &hooks, &output, !had_failure, &mut hook_errors).await;

    output.success = success;
    output.hook_errors = hook_errors;

    ctx.set_current_details(format_final_details(&node_status, success, output.elapsed_time));
    upsert_terminal_status(
        ctx,
        Some(&plan),
        if success {
            RunOutcome::Passed
        } else {
            RunOutcome::Failed
        },
    )?;

    if success {
        Ok(output)
    } else {
        // Fail the workflow so Temporal marks it FAILED (not COMPLETED).
        // Structured output is already persisted in memo for callers to inspect.
        let error_count = NodeStatusSummary::tally(&node_status).error;
        Err(WorkflowTermination::failed_application(ApplicationFailure::non_retryable(
            anyhow::anyhow!(
                "dbt run failed: {error_count} node(s) errored (see workflow memo for details)"
            ),
        )))
    }
}

/// Publish the outcome of a run that failed outside the node loop: fire the
/// failure hooks, mark `DbtStatus` failed, and leave the reason on the details
/// line.
///
/// The failure hooks are best-effort: whatever they report is logged by
/// `run_post_hooks` and does not replace the failure being finished.
async fn fail_run(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    start: Option<std::time::SystemTime>,
    plan: &ExecutionPlan,
    progress: &RunProgress,
    failure: &WorkflowTermination,
) -> Result<(), WorkflowTermination> {
    if let Some(hooks) = progress.hooks.as_deref() {
        let snapshot = failure_snapshot(plan, progress, elapsed_secs(start, ctx.workflow_time()));
        run_post_hooks(ctx, input, plan, hooks, &snapshot, false, &mut Vec::new()).await;
    }
    upsert_terminal_status(ctx, Some(plan), RunOutcome::Failed)?;
    ctx.set_current_details(failure_details(failure));
    Ok(())
}

/// The output the failure hooks receive for a run that did not complete: what
/// is known of it, marked unsuccessful.
fn failure_snapshot(plan: &ExecutionPlan, progress: &RunProgress, elapsed: f64) -> DbtRunOutput {
    DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: false,
        skipped: false,
        skip_reason: None,
        node_results: progress.results.clone(),
        node_results_partial: progress.results_partial,
        elapsed_time: elapsed,
        artifacts: None,
        log_path: None,
        hook_errors: vec![],
    }
}

/// The details line for a run that failed outside the node loop.
fn failure_details(failure: &WorkflowTermination) -> String {
    let message = failure.to_string();
    format!("failed: {}", truncate_at_char_boundary(&message, DETAILS_MAX_BYTES))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            project: "shop".to_string(),
            levels: vec![],
            nodes: BTreeMap::new(),
            manifest_json: None,
            manifest_ref: None,
            invocation_id: "inv-7".to_string(),
            search_attributes: BTreeMap::new(),
            write_artifacts: false,
            has_on_run_start: false,
            has_on_run_end: false,
            priority_scheduling: false,
            has_project_checks: false,
        }
    }

    fn failure(message: &str) -> WorkflowTermination {
        WorkflowTermination::failed_application(ApplicationFailure::non_retryable(anyhow::anyhow!(
            "{message}"
        )))
    }

    #[test]
    fn the_failure_hooks_see_an_unsuccessful_run_with_the_results_so_far() {
        let progress = RunProgress {
            results: vec![NodeExecutionResult::not_executed(
                "model.shop.a".to_string(),
                crate::types::NodeStatus::Success,
                None,
            )],
            results_partial: true,
            ..RunProgress::default()
        };

        let snapshot = failure_snapshot(&plan(), &progress, 4.5);

        assert!(!snapshot.success);
        assert_eq!(snapshot.invocation_id, "inv-7");
        assert_eq!(snapshot.node_results.len(), 1);
        assert!(snapshot.node_results_partial);
        assert!((snapshot.elapsed_time - 4.5).abs() < f64::EPSILON);
    }

    #[test]
    fn a_failure_before_any_node_ran_reports_no_results() {
        let snapshot = failure_snapshot(&plan(), &RunProgress::default(), 0.0);
        assert_eq!(snapshot.node_results.len(), 0);
        assert!(!snapshot.node_results_partial);
    }

    #[test]
    fn the_details_line_carries_the_failure_reason() {
        let details = failure_details(&failure("store_artifacts activity failed: access denied"));
        assert!(details.starts_with("failed: "), "{details}");
        assert!(details.contains("access denied"), "{details}");
    }

    #[test]
    fn a_long_failure_reason_is_cut_on_a_character_boundary() {
        let details = failure_details(&failure(&"é".repeat(500)));
        assert!(details.len() <= "failed: ".len() + DETAILS_MAX_BYTES, "{}", details.len());
    }
}
