//! Patch ids that gate workflow behaviour changes against recorded histories.

/// Patch id for writing the terminal memo once.
///
/// A finished run publishes its terminal memo after the summary line is
/// appended; a write straight after the last level would be superseded at
/// once. Memo upserts are workflow commands, so a history recorded with both
/// writes keeps both and replays cleanly.
pub const SINGLE_TERMINAL_MEMO_PATCH: &str = "single-terminal-memo-upsert";

/// Patch id for the bounded memo cadence.
///
/// Memo upserts are workflow commands, so changing which levels write one
/// changes the command sequence. A run whose history was recorded before this
/// keeps the old cadence and replays cleanly; runs started after it get the
/// bounded one.
pub const MEMO_CADENCE_PATCH: &str = "bounded-memo-cadence";

/// Patch id for readiness-based scheduling.
///
/// The level loop and the ready queue issue different command sequences, so a
/// run whose history was recorded by the level loop has to keep replaying on
/// it. Only executions that start after this patch is deployed record the
/// marker and take the ready queue.
pub const READY_QUEUE_PATCH: &str = "ready-queue-scheduling";

/// Patch id for node activity ids derived from the node's `unique_id`.
///
/// The SDK compares an activity's id and type against the recorded history on
/// replay, so a run recorded with the short `<type>:<name>` ids has to keep
/// scheduling them. Only executions that start after this patch is deployed
/// record the marker and use ids that cannot collide.
pub const UNIQUE_ACTIVITY_ID_PATCH: &str = "unique-node-activity-ids";

/// Patch id for carrying a skip through a node that is itself skipped.
///
/// It decides which nodes the level loop schedules, so a history recorded
/// before it can hold an activity for a node whose ancestor had failed (a
/// descendant of a skipped model with `on_error: continue`). That run keeps its
/// scheduling on replay; executions that start after this patch is deployed
/// record the marker and skip every descendant of a failure.
pub const TRANSITIVE_SKIP_PATCH: &str = "transitive-upstream-skip";

/// Patch id for ending a run that fails or is cancelled outside the node loop.
///
/// Ending such a run properly — failure hooks, the terminal `DbtStatus`, a
/// cancellation reported as one — issues workflow commands after the failing
/// phase. A history recorded without the marker ends in failure at that phase
/// with no further commands, and has to replay that way. The marker is checked
/// only where those commands would start, so a run that never reaches such an
/// exit records nothing.
pub const FAILURE_FINALIZATION_PATCH: &str = "finalize-runs-that-fail-outside-the-node-loop";

/// Patch id for counting a failed `on-run-end` hook against the run.
///
/// The verdict decides which post-run hooks fire (`on_success` or `on_failure`)
/// and whether the workflow completes or fails. A history recorded without the
/// marker noted the failure in `hook_errors` only and carries `on_success`
/// commands, which it has to replay. The marker is consulted only when the
/// `on-run-end` activity did not succeed, so a run whose hooks passed records
/// nothing.
pub const ON_RUN_END_FAILS_RUN_PATCH: &str = "failed-on-run-end-fails-the-run";

/// Patch id for storing the artifacts of a run that ends before its own
/// artifact step has run.
///
/// The store is an activity, and a history recorded without the marker ends
/// such a run without scheduling it. The marker is consulted only where the
/// activity would be scheduled, so a run that stored its artifacts in the
/// ordinary course, or never ran a node, records nothing.
pub const ENDED_RUN_ARTIFACTS_PATCH: &str = "store-artifacts-of-runs-that-end-early";

/// Patch id for ending a run as cancelled when the cancellation reaches a
/// lifecycle hook.
///
/// A hook is a child workflow, and a cancellation that arrives while one runs
/// used to surface as that hook's error: `on_error: fail` failed the run and
/// fired its failure hooks, `warn` and `ignore` carried on to the next command.
/// Ending as cancelled issues different commands from there, so a history
/// recorded before the marker keeps the old ending. The marker is consulted
/// only when a hook errored while the workflow's cancellation was already
/// requested, so a run whose hooks passed records nothing.
pub const HOOK_CANCELS_RUN_PATCH: &str = "cancelled-lifecycle-hook-cancels-the-run";
