//! Pure helpers for the workflow: node status, memo writes, log lines, labels
//! and ids, the on-error blocking rule, patch ids and result constructors.
//!
//! Each concern lives in its own submodule; they are re-exported here so the
//! workflow code imports from one place.

mod activity_errors;
mod blocking;
mod clock;
mod labels;
mod log_lines;
mod memo;
mod patches;
mod results;
mod retry;
mod status;
#[cfg(test)]
mod test_support;

pub use self::activity_errors::{activity_error_message, short_activity_error};
pub use self::blocking::{
    dep_continues_on_error, failure_blocks_dependents, is_blocked_by_failed_upstream,
};
pub use self::clock::{RunClock, RunFacts, elapsed_secs};
pub use self::labels::{format_activity_id, format_activity_label, node_label};
pub use self::log_lines::{
    build_freshness_summary_line, build_log_header, build_summary_lines, format_final_details,
    format_progress_line, format_ready_queue_details, format_result_tag, format_running_details,
    plural,
};
pub use self::memo::{
    NodeStatusSummary, should_announce_level, should_flush_memo, upsert_memo_state,
    upsert_node_status,
};
pub use self::patches::{
    ENDED_RUN_ARTIFACTS_PATCH, FAILURE_FINALIZATION_PATCH, MEMO_CADENCE_PATCH,
    ON_RUN_END_FAILS_RUN_PATCH, READY_QUEUE_PATCH, SINGLE_TERMINAL_MEMO_PATCH,
    TRANSITIVE_SKIP_PATCH, UNIQUE_ACTIVITY_ID_PATCH,
};
pub use self::results::{
    cancelled_result, classify_result_status, error_result, mark_remaining_level_as_cancelled,
    skipped_result,
};
pub use self::retry::build_retry_policy;
pub use self::status::{build_node_status_tree, set_node_status};
