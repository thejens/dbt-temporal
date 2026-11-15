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
