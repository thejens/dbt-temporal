//! How a `dbt_run` ends.
//!
//! Every exit from the run's phases passes through [`finish_run`], so the
//! terminal `DbtStatus`, the failure hooks and the reporting of cancellation
//! are decided in one place instead of before each early return.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{WorkflowContext, WorkflowResult, WorkflowTermination};

use crate::error::truncate_at_char_boundary;
use crate::types::{
    DbtRunInput, DbtRunOutput, ExecutionPlan, HookError, HooksConfig, NodeExecutionResult,
    NodeStatus, NodeStatusTree, RetryConfig, RunOutcome, TimeoutConfig,
};

use super::DbtRunWorkflow;
use super::helpers::{
    ENDED_RUN_ARTIFACTS_PATCH, FAILURE_FINALIZATION_PATCH, NodeStatusSummary, RunClock, RunFacts,
    elapsed_secs, format_final_details, skipped_result,
};
use super::levels::LevelExecutionOutcome;
use super::phases::{
    RunRecord, run_post_hooks, store_artifacts_of_ended_run, upsert_terminal_status,
};

/// What the phases had established when one of them failed.
#[derive(Default)]
pub struct RunProgress {
    /// Set once the run is planned; before that there is no invocation to
    /// report on and no `DbtStatus` to correct.
    pub plan: Option<Arc<ExecutionPlan>>,
    /// Set once the project's configuration is resolved; the failure hooks
    /// come from it.
    pub hooks: Option<Arc<HooksConfig>>,
    /// Set once the nodes have run. Everything a run that ends from here on,
    /// by failure or cancellation, still owes the artifacts: what the nodes
    /// produced, and what the store activity needs to write it.
    pub executed: Option<ExecutionRecord>,
}

/// What the nodes produced, and how the artifacts that describe it are written.
pub struct ExecutionRecord {
    pub levels: LevelExecutionOutcome,
    pub store: StorePolicy,
    /// The artifact step has been made, or has failed outright. A write that a
    /// cancellation interrupted is not settled: nothing was stored.
    pub artifacts_settled: bool,
}

/// How the run's artifacts are written: the settings the ordinary artifact step
/// uses, held so the finisher can use them too.
pub struct StorePolicy {
    pub timeouts: TimeoutConfig,
    pub retry: RetryConfig,
    pub env: BTreeMap<String, String>,
    pub run_started_at: Option<SystemTime>,
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
    /// success into a failure. `success` is false for a run whose nodes failed
    /// or whose `on-run-end` hook failed.
    pub output: DbtRunOutput,
    pub node_status: NodeStatusTree,
    pub hook_errors: Vec<HookError>,
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
    start: Option<SystemTime>,
    mut progress: RunProgress,
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
            store_ended_run_artifacts(ctx, input, &mut progress).await;
            upsert_terminal_status(ctx, progress.plan.as_deref(), RunOutcome::Cancelled)?;
            ctx.set_current_details("cancelled".to_string());
            Err(cancelled)
        }
        Err(failure @ WorkflowTermination::Failed(_)) => {
            store_ended_run_artifacts(ctx, input, &mut progress).await;
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

/// Store the artifacts of a run that ends after its nodes ran but before its
/// own artifact step did, so that what finished is not lost with the run.
///
/// Nodes the run never reached are recorded as skipped, as dbt records the
/// nodes it did not run after a failure. A run that was cancelled already holds
/// a result for every node, the unreached ones marked cancelled.
///
/// This is best effort: the run is already ending for another reason, and a
/// store that fails here must not replace it. The write is detached from the
/// workflow's cancellation, which would otherwise cancel it before it began.
async fn store_ended_run_artifacts(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    progress: &mut RunProgress,
) {
    let (Some(plan), Some(executed)) = (progress.plan.as_deref(), progress.executed.as_mut())
    else {
        return;
    };
    if !plan.write_artifacts
        || executed.artifacts_settled
        || !ctx.patched(ENDED_RUN_ARTIFACTS_PATCH)
    {
        return;
    }
    executed.artifacts_settled = true;

    let unreached = unreached_results(plan, &executed.levels);
    let all_results;
    let results = if unreached.is_empty() {
        executed.levels.all_results.as_slice()
    } else {
        all_results = [executed.levels.all_results.as_slice(), unreached.as_slice()].concat();
        all_results.as_slice()
    };
    let stored = store_artifacts_of_ended_run(
        ctx,
        plan,
        &input.command,
        RunRecord {
            results,
            log_lines: &executed.levels.log_lines,
            prior_segments: &executed.levels.prior_segments,
        },
        &executed.store.timeouts,
        &executed.store.retry,
        RunFacts {
            clock: RunClock {
                started_at: executed.store.run_started_at,
                elapsed_secs: elapsed_secs(executed.store.run_started_at, ctx.workflow_time()),
            },
            env: &executed.store.env,
            target: input.target.as_deref(),
        },
    )
    .await;
    if let Err(e) = stored {
        tracing::warn!(error = %e, "could not store the artifacts of a run that ended early");
    }
}

/// A skipped result for each planned node that has no outcome: it was neither
/// run in this segment nor settled in an earlier one.
fn unreached_results(
    plan: &ExecutionPlan,
    levels: &LevelExecutionOutcome,
) -> Vec<NodeExecutionResult> {
    plan.nodes
        .keys()
        .filter(|id| {
            matches!(
                levels.node_status.nodes.get(*id),
                None | Some(NodeStatus::Pending | NodeStatus::Running)
            )
        })
        .map(|id| skipped_result(id, "skipped: the run ended before this node finished"))
        .collect()
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
    } = run;

    // Artifacts are stored by now, so a cancellation that reaches a post-run
    // hook has nothing left to save: the run ends as cancelled, as it would
    // had the cancellation arrived a moment earlier.
    let success =
        match run_post_hooks(ctx, input, &plan, &hooks, &output, output.success, &mut hook_errors)
            .await
        {
            Ok(success) => success,
            Err(cancelled) => {
                upsert_terminal_status(ctx, Some(&plan), RunOutcome::Cancelled)?;
                ctx.set_current_details("cancelled".to_string());
                return Err(cancelled);
            }
        };

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
            anyhow::anyhow!("{}", run_failure_message(error_count, &output.hook_errors)),
        )))
    }
}

/// Why a completed run is reported as failed: the nodes that errored, or, when
/// none did, the hook whose failure failed the run.
fn run_failure_message(error_count: usize, hook_errors: &[HookError]) -> String {
    match hook_errors.first() {
        Some(hook) if error_count == 0 => {
            let error = truncate_at_char_boundary(&hook.error, DETAILS_MAX_BYTES);
            format!("dbt run failed: {} hook failed: {error}", hook.event)
        }
        _ => {
            format!("dbt run failed: {error_count} node(s) errored (see workflow memo for details)")
        }
    }
}

/// Publish the outcome of a run that failed outside the node loop: fire the
/// failure hooks, mark `DbtStatus` failed, and leave the reason on the details
/// line.
///
/// The failure hooks are best-effort: whatever they report is logged by
/// `run_post_hooks` and does not replace the failure being finished. A
/// cancellation that reaches them abandons the rest of them, and the run still
/// ends as the failure it already was.
async fn fail_run(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    start: Option<SystemTime>,
    plan: &ExecutionPlan,
    progress: &RunProgress,
    failure: &WorkflowTermination,
) -> Result<(), WorkflowTermination> {
    if let Some(hooks) = progress.hooks.as_deref() {
        let (results, partial) = progress
            .executed
            .as_ref()
            .map_or((&[][..], false), |executed| {
                (
                    executed.levels.all_results.as_slice(),
                    !executed.levels.prior_segments.is_empty(),
                )
            });
        let snapshot =
            failure_snapshot(plan, results, partial, elapsed_secs(start, ctx.workflow_time()));
        if run_post_hooks(ctx, input, plan, hooks, &snapshot, false, &mut Vec::new())
            .await
            .is_err()
        {
            tracing::warn!("cancelled while the failure hooks ran; the run ends as failed");
        }
    }
    upsert_terminal_status(ctx, Some(plan), RunOutcome::Failed)?;
    ctx.set_current_details(failure_details(failure));
    Ok(())
}

/// The output the failure hooks receive for a run that did not complete: what
/// is known of it, marked unsuccessful.
fn failure_snapshot(
    plan: &ExecutionPlan,
    results: &[NodeExecutionResult],
    results_partial: bool,
    elapsed: f64,
) -> DbtRunOutput {
    DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: false,
        skipped: false,
        skip_reason: None,
        node_results: results.to_vec(),
        node_results_partial: results_partial,
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

    fn hook_error(event: &str, error: &str) -> HookError {
        HookError {
            hook_workflow_type: event.to_string(),
            event: event.to_string(),
            error: error.to_string(),
        }
    }

    #[test]
    fn a_run_failed_by_a_hook_names_the_hook() {
        let message = run_failure_message(0, &[hook_error("on_run_end", "syntax error")]);
        assert_eq!(message, "dbt run failed: on_run_end hook failed: syntax error");
    }

    #[test]
    fn errored_nodes_are_what_a_failed_run_reports_when_there_are_any() {
        let message = run_failure_message(2, &[hook_error("on_run_end", "syntax error")]);
        assert_eq!(message, "dbt run failed: 2 node(s) errored (see workflow memo for details)");
    }

    #[test]
    fn the_failure_hooks_see_an_unsuccessful_run_with_the_results_so_far() {
        let results = [NodeExecutionResult::not_executed(
            "model.shop.a".to_string(),
            NodeStatus::Success,
            None,
        )];

        let snapshot = failure_snapshot(&plan(), &results, true, 4.5);

        assert!(!snapshot.success);
        assert_eq!(snapshot.invocation_id, "inv-7");
        assert_eq!(snapshot.node_results.len(), 1);
        assert!(snapshot.node_results_partial);
        assert!((snapshot.elapsed_time - 4.5).abs() < f64::EPSILON);
    }

    fn node_info(name: &str) -> crate::types::NodeInfo {
        crate::types::NodeInfo {
            unique_id: format!("model.shop.{name}"),
            name: name.to_string(),
            resource_type: dbt_schemas::schemas::telemetry::NodeType::Model,
            materialization: None,
            package_name: "shop".to_string(),
            depends_on: vec![],
            priority: None,
            on_error: None,
        }
    }

    fn levels_with(statuses: &[(&str, NodeStatus)]) -> LevelExecutionOutcome {
        LevelExecutionOutcome {
            log_lines: vec![],
            all_results: vec![],
            prior_segments: vec![],
            node_status: NodeStatusTree {
                nodes: statuses
                    .iter()
                    .map(|(name, status)| (format!("model.shop.{name}"), *status))
                    .collect(),
            },
            had_failure: false,
            was_cancelled: false,
            total_nodes: statuses.len(),
            node_counter: 0,
            continue_at_level: None,
            failed_nodes: std::collections::BTreeSet::new(),
        }
    }

    /// A run that stopped between segments has nodes with no outcome; the
    /// artifacts record them as skipped, which `retry_from` re-runs.
    #[test]
    fn nodes_without_an_outcome_are_recorded_as_skipped() {
        let mut plan = plan();
        plan.nodes = ["done", "failed", "never_started", "mid_flight", "unlisted"]
            .iter()
            .map(|name| (format!("model.shop.{name}"), node_info(name)))
            .collect();
        let levels = levels_with(&[
            ("done", NodeStatus::Success),
            ("failed", NodeStatus::Error),
            ("never_started", NodeStatus::Pending),
            ("mid_flight", NodeStatus::Running),
        ]);

        let unreached = unreached_results(&plan, &levels);

        let mut ids: Vec<&str> = unreached.iter().map(|r| r.unique_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            [
                "model.shop.mid_flight",
                "model.shop.never_started",
                "model.shop.unlisted"
            ]
        );
        assert!(unreached.iter().all(|r| r.status == NodeStatus::Skipped));
    }

    #[test]
    fn a_run_whose_nodes_all_have_an_outcome_has_nothing_unreached() {
        let mut plan = plan();
        plan.nodes = BTreeMap::from([("model.shop.a".to_string(), node_info("a"))]);
        let levels = levels_with(&[("a", NodeStatus::Cancelled)]);

        assert!(unreached_results(&plan, &levels).is_empty());
    }

    #[test]
    fn a_failure_before_any_node_ran_reports_no_results() {
        let snapshot = failure_snapshot(&plan(), &[], false, 0.0);
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
