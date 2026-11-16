use temporalio_sdk::WorkflowContext;

use super::patches::HOOK_CANCELS_RUN_PATCH;
use crate::workflow::DbtRunWorkflow;

/// Whether a lifecycle hook's error is the run's own cancellation.
///
/// A hook is a child workflow, and the error a cancelled one produces is the
/// same shape as any failure of it (a cancelled child, or a start that was
/// cancelled), so the error says little. What identifies it is the workflow's
/// own cancellation request, which is part of the replayed history. Called
/// only on a hook's error path, so a run whose hooks passed records no patch
/// marker.
pub fn hook_failure_is_cancellation(ctx: &WorkflowContext<DbtRunWorkflow>) -> bool {
    ctx.cancellation_token().is_cancelled() && ctx.patched(HOOK_CANCELS_RUN_PATCH)
}
