//! Continue-as-new: restoring a predecessor segment's state, handing a run's
//! state to its successor, and the values a continuation carries over.

use std::collections::BTreeMap;

use temporalio_sdk::{ContinueAsNewOptions, WorkflowContext, WorkflowTermination};

use super::DbtRunWorkflow;
use super::levels::{self, ResumePoint};
use super::phases::{load_segment_state, save_segment_state};
use crate::types::{
    DbtRunInput, ExecutionPlan, RunResumeState, RunSegmentControl, RunSegmentState,
};

/// Load the previous segment's state when this execution is a continuation.
///
/// `None` for a normal run, which is every run that has not exceeded
/// Temporal's history budget.
pub(super) async fn load_resume_state(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
) -> Result<Option<RunSegmentControl>, WorkflowTermination> {
    let Some(resume) = input.resume_from.as_ref() else {
        return Ok(None);
    };
    let state = load_segment_state(ctx, resume).await?;
    tracing::info!(
        segment = resume.segment,
        next_level = resume.next_level,
        "resuming run after continue-as-new"
    );
    Ok(Some(state))
}

/// Build the level loop's starting point, fresh or resumed.
///
/// `can_continue` gates continuation on artifact storage: without a store there
/// is nowhere to spill state, so the run pushes on and accepts history growth
/// rather than failing outright.
pub(super) fn build_resume_point(
    plan: &ExecutionPlan,
    resumed: Option<RunSegmentControl>,
    can_continue: bool,
) -> ResumePoint {
    let Some(state) = resumed else {
        return ResumePoint::fresh(plan, can_continue);
    };
    ResumePoint {
        start_level: state.next_level,
        // The predecessor's log and results stay in its checkpoint; this
        // segment accumulates only its own, and the artifact activity puts the
        // run back together from `prior_segments`.
        log_lines: Vec::new(),
        all_results: Vec::new(),
        prior_segments: state.prior_segments,
        node_status: state.node_status,
        failed_nodes: state.failed_nodes.into_iter().collect(),
        had_failure: state.had_failure,
        node_counter: state.node_counter,
        can_continue,
    }
}

/// Spill the run's state and restart with a fresh history.
///
/// Returns the termination the caller propagates: `continue_as_new` when the
/// checkpoint was written, a failure when it could not be.
///
/// Takes the snapshot already built, so the two halves stay separate: what the
/// successor needs is decided by `build_segment_state`, and the checkpoint
/// itself names the segment and level this hands over at.
pub(super) async fn continue_run_as_new(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    state: RunSegmentState,
) -> WorkflowTermination {
    let segment = state.segment;
    let next_level = state.next_level;

    let state_ref = match save_segment_state(ctx, &plan.invocation_id, state).await {
        Ok(state_ref) => state_ref,
        Err(failure) => return failure,
    };

    let mut next_input = input.clone();
    next_input.resume_from = Some(RunResumeState {
        invocation_id: plan.invocation_id.clone(),
        state_ref,
        next_level,
        segment,
        // Read after the checkpoint activity returned, so an update accepted
        // while it was outstanding is still carried.
        fail_fast_override: ctx.state(|s| s.fail_fast_override),
    });

    ctx.set_current_details(format!(
        "continuing as new at level {next_level}/{} (segment {segment})",
        plan.levels.len()
    ));
    tracing::info!(
        segment,
        next_level,
        total_levels = plan.levels.len(),
        "workflow history approaching its limit — continuing as new"
    );

    ctx.continue_as_new(next_input, ContinueAsNewOptions::default())
        .expect_err("continue_as_new always returns Err")
}

/// The fail-fast override a continuation inherits from its predecessor.
///
/// `set_fail_fast` lives on workflow state, which a continue-as-new discards.
/// Without this the successor starts at `None` and silently reverts to the
/// original input's `fail_fast`, undoing an update the caller was told had
/// been applied.
pub(super) fn inherited_fail_fast_override(input: &DbtRunInput) -> Option<bool> {
    input
        .resume_from
        .as_ref()
        .and_then(|r| r.fail_fast_override)
}

/// Segment number for the continuation this run is about to start.
///
/// Segments are 1-based from the first continuation, so a run that never
/// continues has no segment number at all.
pub(super) fn next_segment_number(resume: Option<&RunResumeState>) -> u32 {
    resume.map_or(1, |r| r.segment + 1)
}

/// Snapshot everything the successor needs.
///
/// Split out from `continue_run_as_new` so the handover contents can be tested
/// without a live workflow context — a field forgotten here silently loses run
/// state across a continuation.
pub(super) fn build_segment_state(
    plan: &ExecutionPlan,
    levels: &levels::LevelExecutionOutcome,
    effective_env: &BTreeMap<String, String>,
    hook_errors: &[crate::types::HookError],
    next_level: usize,
    segment: u32,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
) -> RunSegmentState {
    RunSegmentState {
        started_at,
        // Stamped so the successor can tell this checkpoint apart from another
        // segment's, rather than trusting whatever its `state_ref` names.
        schema_version: crate::activities::segment_state::SEGMENT_STATE_SCHEMA_VERSION,
        invocation_id: plan.invocation_id.clone(),
        segment,
        plan: plan.clone(),
        prior_segments: levels.prior_segments.clone(),
        all_results: levels.all_results.clone(),
        log_lines: levels.log_lines.clone(),
        node_status: levels.node_status.clone(),
        failed_nodes: levels.failed_nodes.iter().cloned().collect(),
        had_failure: levels.had_failure,
        effective_env: effective_env.clone(),
        hook_errors: hook_errors.to_vec(),
        total_nodes: levels.total_nodes,
        node_counter: levels.node_counter,
        next_level,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::types::{NodeStatus, NodeStatusTree};
    use std::collections::BTreeSet;

    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            project: "p".to_string(),
            levels: vec![vec!["model.p.a".to_string()], vec!["model.p.b".to_string()]],
            nodes: BTreeMap::new(),
            manifest_json: None,
            manifest_ref: None,
            invocation_id: "inv".to_string(),
            search_attributes: BTreeMap::new(),
            write_artifacts: true,
            has_on_run_start: false,
            has_on_run_end: false,
            priority_scheduling: false,
            has_project_checks: false,
            warnings: vec![],
        }
    }

    fn outcome() -> levels::LevelExecutionOutcome {
        levels::LevelExecutionOutcome {
            prior_segments: Vec::new(),
            log_lines: vec!["1 of 2 START model.p.a".to_string()],
            all_results: vec![],
            node_status: NodeStatusTree {
                nodes: BTreeMap::from([("model.p.a".to_string(), NodeStatus::Success)]),
            },
            had_failure: true,
            was_cancelled: false,
            total_nodes: 2,
            node_counter: 1,
            continue_at_level: Some(1),
            failed_nodes: BTreeSet::from(["model.p.bad".to_string()]),
        }
    }

    #[test]
    fn segments_number_from_one_and_increment() {
        assert_eq!(next_segment_number(None), 1, "first continuation is segment 1");
        assert_eq!(
            next_segment_number(Some(&RunResumeState {
                invocation_id: "inv".to_string(),
                state_ref: "ref".to_string(),
                next_level: 4,
                segment: 3,
                fail_fast_override: None,
            })),
            4
        );
    }

    fn resume_state(fail_fast_override: Option<bool>) -> RunResumeState {
        RunResumeState {
            invocation_id: "inv".to_string(),
            state_ref: "ref".to_string(),
            next_level: 1,
            segment: 1,
            fail_fast_override,
        }
    }

    fn input_resuming_from(resume: Option<RunResumeState>) -> DbtRunInput {
        let mut input: DbtRunInput =
            serde_json::from_value(serde_json::json!({ "command": "run" })).unwrap();
        input.resume_from = resume;
        input
    }

    /// `set_fail_fast` lives on workflow state, which continue-as-new discards.
    /// The successor has to read the accepted value back out of its input, or
    /// an update the caller was told had been applied silently reverts.
    #[test]
    fn a_continuation_inherits_the_accepted_fail_fast_update() {
        assert_eq!(
            inherited_fail_fast_override(&input_resuming_from(Some(resume_state(Some(true))))),
            Some(true)
        );
        assert_eq!(
            inherited_fail_fast_override(&input_resuming_from(Some(resume_state(Some(false))))),
            Some(false),
            "disabling fail-fast mid-run must survive too"
        );
    }

    #[test]
    fn a_run_with_no_predecessor_has_no_inherited_override() {
        assert_eq!(inherited_fail_fast_override(&input_resuming_from(None)), None);
        assert_eq!(
            inherited_fail_fast_override(&input_resuming_from(Some(resume_state(None)))),
            None,
            "a predecessor that was never updated leaves the input's own setting alone"
        );
    }

    /// A field missed here silently loses run state across a continuation, so
    /// assert on each one the successor actually reads.
    #[test]
    fn segment_state_captures_everything_the_successor_needs() {
        let env = BTreeMap::from([("K".to_string(), "v".to_string())]);
        let hook_errors = vec![crate::types::HookError {
            hook_workflow_type: "notify".to_string(),
            event: "pre_run".to_string(),
            error: "flaky".to_string(),
        }];

        let state = build_segment_state(&plan(), &outcome(), &env, &hook_errors, 1, 1, None);

        assert_eq!(state.next_level, 1);
        assert_eq!(state.plan.levels.len(), 2, "plan carried, not re-planned");
        assert_eq!(state.log_lines.len(), 1);
        assert_eq!(state.node_status.nodes.len(), 1);
        assert!(state.had_failure, "failure state must not reset");
        assert_eq!(state.failed_nodes, vec!["model.p.bad".to_string()]);
        assert_eq!(state.effective_env.get("K").map(String::as_str), Some("v"));
        assert_eq!(state.hook_errors.len(), 1);
        assert_eq!(state.total_nodes, 2);
        assert_eq!(state.node_counter, 1, "progress numbering stays continuous");
    }

    #[test]
    fn a_fresh_run_starts_at_level_zero_with_empty_accumulators() {
        let point = build_resume_point(&plan(), None, true);
        assert_eq!(point.start_level, 0);
        assert!(point.all_results.is_empty());
        assert!(!point.had_failure);
        assert_eq!(point.node_counter, 0);
        assert!(point.can_continue);
    }

    /// The control view a successor is handed, as `load_segment_state` builds
    /// it: the checkpoint just read joins the chain of segments to collect.
    fn control_from(state: RunSegmentState, state_ref: &str) -> RunSegmentControl {
        let mut prior_segments = state.prior_segments;
        prior_segments.push(state_ref.to_string());
        RunSegmentControl {
            plan: state.plan,
            prior_segments,
            node_status: state.node_status,
            failed_nodes: state.failed_nodes,
            had_failure: state.had_failure,
            effective_env: state.effective_env,
            hook_errors: state.hook_errors,
            total_nodes: state.total_nodes,
            node_counter: state.node_counter,
            next_level: state.next_level,
            started_at: state.started_at,
        }
    }

    #[test]
    fn a_resumed_run_picks_up_where_its_predecessor_stopped() {
        let env = BTreeMap::from([("K".to_string(), "v".to_string())]);
        let state = build_segment_state(&plan(), &outcome(), &env, &[], 1, 1, None);

        let point = build_resume_point(&plan(), Some(control_from(state, "seg-0.json")), true);

        assert_eq!(point.start_level, 1, "skips the levels already executed");
        assert_eq!(point.node_counter, 1);
        assert!(point.had_failure, "carries the failure flag forward");
        assert!(
            point.failed_nodes.contains("model.p.bad"),
            "downstream skipping depends on this"
        );
    }

    /// The predecessor's log and results stay in its checkpoint. Carrying them
    /// into the successor is what made every continuation cost the whole run
    /// so far, twice — once as the activity argument that wrote it and once as
    /// the activity result that read it back.
    #[test]
    fn a_resumed_run_starts_with_empty_accumulators_and_names_its_predecessor() {
        let env = BTreeMap::from([("K".to_string(), "v".to_string())]);
        let state = build_segment_state(&plan(), &outcome(), &env, &[], 1, 1, None);
        assert_eq!(state.log_lines.len(), 1, "the predecessor wrote its log into the checkpoint");

        let point = build_resume_point(&plan(), Some(control_from(state, "seg-0.json")), true);

        assert!(point.log_lines.is_empty(), "{:?}", point.log_lines);
        assert!(point.all_results.is_empty());
        assert_eq!(point.prior_segments, vec!["seg-0.json".to_string()]);
    }

    /// Each segment adds itself, so the chain the artifact activity walks is
    /// every segment of the run in order.
    #[test]
    fn segments_accumulate_across_continuations() {
        let env = BTreeMap::new();
        let first = build_segment_state(&plan(), &outcome(), &env, &[], 1, 1, None);
        let point = build_resume_point(&plan(), Some(control_from(first, "seg-0.json")), true);

        let mut second_outcome = outcome();
        second_outcome.prior_segments = point.prior_segments;
        let second = build_segment_state(&plan(), &second_outcome, &env, &[], 2, 2, None);

        assert_eq!(second.prior_segments, vec!["seg-0.json".to_string()]);
        let point = build_resume_point(&plan(), Some(control_from(second, "seg-1.json")), true);
        assert_eq!(point.prior_segments, vec!["seg-0.json".to_string(), "seg-1.json".to_string()]);
    }

    /// Without an artifact store there is nowhere to spill, so a resumed point
    /// must not advertise that it can continue again.
    #[test]
    fn resume_point_propagates_the_can_continue_gate() {
        assert!(!build_resume_point(&plan(), None, false).can_continue);
    }
}
