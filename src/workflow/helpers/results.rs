use crate::types::{NodeExecutionResult, NodeStatus, NodeStatusTree};

use super::status::set_node_status;

pub fn skipped_result(unique_id: &str, message: &str) -> NodeExecutionResult {
    NodeExecutionResult::not_executed(unique_id, NodeStatus::Skipped, Some(message.to_string()))
}

pub fn cancelled_result(unique_id: &str) -> NodeExecutionResult {
    NodeExecutionResult::not_executed(
        unique_id,
        NodeStatus::Cancelled,
        Some("cancelled".to_string()),
    )
}

pub fn error_result(unique_id: &str, message: &str) -> NodeExecutionResult {
    NodeExecutionResult::not_executed(unique_id, NodeStatus::Error, Some(message.to_string()))
}

pub fn mark_remaining_level_as_cancelled(
    level_node_ids: &[String],
    processed_in_level: &std::collections::BTreeSet<String>,
    node_status: &mut NodeStatusTree,
    all_results: &mut Vec<NodeExecutionResult>,
) {
    for id in level_node_ids {
        if !processed_in_level.contains(id) {
            set_node_status(node_status, id, NodeStatus::Cancelled);
            all_results.push(cancelled_result(id));
        }
    }
}

/// Classify the result status from an activity completion. The level scheduler
/// treats anything that isn't an explicit `Error` or `Skipped` as a success
/// for log/memo purposes — the activity may have produced a richer status
/// internally but we're only interested in the success/error/skip trichotomy.
pub const fn classify_result_status(status: NodeStatus) -> NodeStatus {
    match status {
        // Pending / Running: a finished activity cannot report itself as not
        // yet finished. Counting it as a failure surfaces the defect instead
        // of letting it pass.
        NodeStatus::Error | NodeStatus::Pending | NodeStatus::Running => NodeStatus::Error,
        NodeStatus::Skipped => NodeStatus::Skipped,
        // A warned node completed. It is not a failure and must not gate the
        // run — it is kept apart from `Success` only so the run can say it
        // found something.
        NodeStatus::Warn => NodeStatus::Warn,
        // A cancelled activity reaches the scheduler as an `Err`, never as a
        // result, so `Cancelled` is unreachable here in practice; it keeps the
        // success class rather than inventing a fourth one for the trichotomy.
        NodeStatus::Success | NodeStatus::Cancelled => NodeStatus::Success,
    }
}

#[cfg(test)]
// float_cmp: tests compare directly-assigned floats, not computed values.
// unwrap_used: unwrap is acceptable in tests for brevity.
#[allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap
)]
mod tests {
    use super::*;

    #[test]
    fn skipped_result_fields() {
        let r = skipped_result("model.waffle.a", "upstream failure");
        assert_eq!(r.unique_id, "model.waffle.a");
        assert_eq!(r.status, NodeStatus::Skipped);
        assert_eq!(r.execution_time, 0.0);
        assert_eq!(r.message.as_deref(), Some("upstream failure"));
    }

    #[test]
    fn cancelled_result_fields() {
        let r = cancelled_result("model.waffle.b");
        assert_eq!(r.unique_id, "model.waffle.b");
        assert_eq!(r.status, NodeStatus::Cancelled);
        assert_eq!(r.message.as_deref(), Some("cancelled"));
    }

    #[test]
    fn error_result_fields() {
        let r = error_result("model.waffle.c", "timeout");
        assert_eq!(r.unique_id, "model.waffle.c");
        assert_eq!(r.status, NodeStatus::Error);
        assert_eq!(r.message.as_deref(), Some("timeout"));
    }

    #[test]
    fn mark_remaining_fills_unprocessed_only_once() {
        let level: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let mut processed = std::collections::BTreeSet::new();
        processed.insert("a".to_string());
        processed.insert("c".to_string());

        let mut tree = NodeStatusTree {
            nodes: level
                .iter()
                .map(|id| (id.clone(), NodeStatus::Pending))
                .collect(),
        };
        let mut results: Vec<NodeExecutionResult> =
            vec![error_result("a", "boom"), cancelled_result("c")];

        mark_remaining_level_as_cancelled(&level, &processed, &mut tree, &mut results);

        // Invariant: every level id has exactly one result entry.
        assert_eq!(results.len(), level.len());
        let mut ids: Vec<&str> = results.iter().map(|r| r.unique_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["a", "b", "c", "d"]);

        // Already-processed nodes keep their original status; unprocessed
        // nodes are now cancelled.
        assert_eq!(tree.nodes["a"], NodeStatus::Pending); // not changed by helper
        assert_eq!(tree.nodes["b"], NodeStatus::Cancelled);
        assert_eq!(tree.nodes["c"], NodeStatus::Pending);
        assert_eq!(tree.nodes["d"], NodeStatus::Cancelled);
    }

    #[test]
    fn mark_remaining_is_idempotent_when_all_processed() {
        let level: Vec<String> = ["a", "b"].iter().map(|s| (*s).to_string()).collect();
        let processed: std::collections::BTreeSet<String> = level.iter().cloned().collect();

        let mut tree = NodeStatusTree {
            nodes: level
                .iter()
                .map(|id| (id.clone(), NodeStatus::Success))
                .collect(),
        };
        let mut results: Vec<NodeExecutionResult> = vec![];

        mark_remaining_level_as_cancelled(&level, &processed, &mut tree, &mut results);

        assert!(results.is_empty());
        assert_eq!(tree.nodes["a"], NodeStatus::Success);
        assert_eq!(tree.nodes["b"], NodeStatus::Success);
    }

    // --- classify_result_status ---

    #[test]
    fn classify_result_status_maps_every_status() {
        for (status, expected) in [
            (NodeStatus::Success, NodeStatus::Success),
            (NodeStatus::Warn, NodeStatus::Warn),
            (NodeStatus::Error, NodeStatus::Error),
            (NodeStatus::Skipped, NodeStatus::Skipped),
            (NodeStatus::Cancelled, NodeStatus::Success),
            (NodeStatus::Pending, NodeStatus::Error),
            (NodeStatus::Running, NodeStatus::Error),
        ] {
            assert_eq!(classify_result_status(status), expected, "{status}");
        }
    }
}
