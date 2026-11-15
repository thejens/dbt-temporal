//! Building the Temporal activity call for one node: its input payload and its
//! options (timeouts, retry, ids, priority). Shared by both schedulers so a
//! node runs with the same inputs whichever order it was started in.

use std::time::Duration;

use temporalio_common::protos::coresdk::workflow_commands::ActivityCancellationType;
use temporalio_sdk::ActivityOptions;

use crate::types::{
    CompactNodeInput, ExecutionPlan, NodeActivityInput, RetryConfig, RunScope, TimeoutConfig,
};

use super::helpers::build_retry_policy;

/// The activity input for one node.
///
/// The run-level data travels once, inside
/// `run_scope`; the activity derives env, vars, target and the rest from it.
pub(super) fn node_execution_input(
    plan: &ExecutionPlan,
    run_scope: &RunScope,
    unique_id: &str,
) -> NodeActivityInput {
    NodeActivityInput::compact(CompactNodeInput::new(
        unique_id,
        &plan.invocation_id,
        &plan.project,
        run_scope,
    ))
}

/// Temporal options for one node's activity: timeouts, heartbeat, retry policy,
/// the id and summary shown in the UI, and priority/fairness keys.
pub(super) fn node_activity_options(
    plan: &ExecutionPlan,
    timeouts: &TimeoutConfig,
    retry_config: &RetryConfig,
    unique_id: &str,
    activity_id: Option<String>,
    summary: Option<String>,
) -> ActivityOptions {
    // Priority/fairness keys (server >= 1.31; older servers ignore them):
    // priority_key orders nodes within a run by critical-path depth,
    // fairness_key gives concurrent runs proportional dispatch on a
    // shared queue instead of FIFO starvation.
    let priority = if plan.priority_scheduling {
        Some(
            temporalio_common::Priority::builder()
                .maybe_priority_key(plan.nodes.get(unique_id).and_then(|n| n.priority))
                .fairness_key(truncate_fairness_key(&plan.invocation_id))
                .build(),
        )
    } else {
        None
    };
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(timeouts.node_secs))
        .heartbeat_timeout(Duration::from_secs(timeouts.node_heartbeat_secs))
        .maybe_activity_id(activity_id)
        .maybe_summary(summary)
        .cancellation_type(ActivityCancellationType::TryCancel)
        .retry_policy(build_retry_policy(retry_config))
        .maybe_priority(priority)
        .build()
}

/// Fairness keys are limited to 64 bytes server-side; truncate on a char
/// boundary rather than letting the server reject the task.
fn truncate_fairness_key(key: &str) -> String {
    crate::error::truncate_at_char_boundary(key, 64).to_string()
}

#[cfg(test)]
mod tests {
    use super::truncate_fairness_key;

    #[test]
    fn fairness_key_short_passes_through() {
        assert_eq!(truncate_fairness_key("run-abc123"), "run-abc123");
        assert_eq!(truncate_fairness_key(""), "");
    }

    #[test]
    fn fairness_key_truncates_to_64_bytes() {
        let long = "x".repeat(100);
        let out = truncate_fairness_key(&long);
        assert_eq!(out.len(), 64);
    }

    #[test]
    fn fairness_key_truncates_on_char_boundary() {
        // 'ä' is 2 bytes; with a 1-byte prefix the ä's sit at odd offsets, so
        // byte 64 falls mid-char and must back off to 63 rather than panic.
        let multibyte = format!("x{}", "ä".repeat(40));
        let out = truncate_fairness_key(&multibyte);
        assert_eq!(out.len(), 63);
        assert_eq!(out, format!("x{}", "ä".repeat(31)));
    }
}
