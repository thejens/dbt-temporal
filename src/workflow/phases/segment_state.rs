//! Spilling and restoring run state across a continue-as-new handover.

use std::time::Duration;

use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{LoadSegmentStateInput, RunSegmentState, SaveSegmentStateInput};

use super::super::DbtRunWorkflow;
use super::activity_options::{activity_failure, bounded_activity_options, unresolved_retry};

/// Time one segment-state read or write may take; a checkpoint is a single
/// object-store round trip.
const SEGMENT_STATE_TIMEOUT: Duration = Duration::from_mins(5);

/// Spill run state for a continue-as-new handover, returning its store path.
pub async fn save_segment_state(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    invocation_id: &str,
    state: RunSegmentState,
) -> Result<String, WorkflowTermination> {
    ctx.execute_activity(
        DbtActivities::save_segment_state,
        SaveSegmentStateInput {
            invocation_id: invocation_id.to_string(),
            state,
        },
        bounded_activity_options(SEGMENT_STATE_TIMEOUT, &unresolved_retry()),
    )
    .await
    .map_err(|e| activity_failure(ctx, "save_segment_state activity failed", &e))
}

/// Restore the state the previous segment spilled.
///
/// The whole resume record travels, not just its reference: the activity
/// refuses a checkpoint whose identity disagrees with it, so a stale or
/// hand-written `state_ref` fails loudly instead of resuming the run from
/// another point in its DAG.
pub async fn load_segment_state(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    resume: &crate::types::RunResumeState,
) -> Result<crate::types::RunSegmentControl, WorkflowTermination> {
    ctx.execute_activity(
        DbtActivities::load_segment_state,
        LoadSegmentStateInput {
            state_ref: resume.state_ref.clone(),
            expected: Some(crate::types::SegmentIdentity {
                invocation_id: resume.invocation_id.clone(),
                segment: resume.segment,
                next_level: resume.next_level,
            }),
        },
        bounded_activity_options(SEGMENT_STATE_TIMEOUT, &unresolved_retry()),
    )
    .await
    .map_err(|e| activity_failure(ctx, "load_segment_state activity failed", &e))
}
