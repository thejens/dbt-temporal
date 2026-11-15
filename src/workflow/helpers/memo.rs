use std::collections::BTreeMap;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{MemoValue, WorkflowTermination};

use crate::types::{NodeStatus, NodeStatusTree};

use crate::workflow::DbtRunWorkflow;

pub fn upsert_node_status(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    tree: &NodeStatusTree,
) -> Result<(), WorkflowTermination> {
    // Bounded like every other memo write. This one runs at the start of the
    // level loop, when every node is Pending — the shape a count-based cap
    // would let through whole.
    let kept = bounded_node_status(tree);
    let summary = NodeStatusSummary::of(tree, &kept);
    ctx.upsert_memo([
        ("node_status", Some(MemoValue::new(kept))),
        ("node_status_summary", Some(MemoValue::new(summary))),
    ])
    .map_err(|e| WorkflowTermination::failed_application(ApplicationFailure::non_retryable(e)))
}

/// Maximum number of log lines to store in the memo.
/// Keeps the tail so the most recent progress is always visible.
/// Full logs are persisted in the artifact store at workflow completion.
const MEMO_LOG_MAX_LINES: usize = 200;

/// Bytes the node-status memo entry may occupy.
///
/// A memo is carried on every workflow-visible description of the run, so its
/// cost is paid over and over. A count of entries does not bound it: node ids
/// are unique-id strings, and 2,001 ordinary ones run to roughly 78 KB of
/// compact JSON before a single log line — which is why the budget here is in
/// bytes.
const MEMO_NODE_STATUS_BUDGET: usize = 16 * 1024;

/// Bytes the log memo entry may occupy, on top of the line cap. One long
/// adapter error can be larger than a hundred ordinary progress lines.
const MEMO_LOG_BUDGET: usize = 12 * 1024;

/// Serialized overhead per node-status entry in compact JSON: two quotes and a
/// colon around the key, two quotes and a comma around the value.
const MEMO_ENTRY_OVERHEAD: usize = 6;

/// The `{"nodes":{}}` wrapper around the entries, with room to spare — the
/// budget is a ceiling, so the envelope has to come out of it rather than be
/// added on top.
const MEMO_ENVELOPE_OVERHEAD: usize = 64;

/// How many nodes are in each state, so a truncated tree still totals.
///
/// Written alongside `node_status` rather than into it: the tree's values are
/// statuses, and a summary encoded as sentinel *keys* would be read by every
/// existing consumer as if those were nodes.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct NodeStatusSummary {
    pub pending: usize,
    pub running: usize,
    pub success: usize,
    pub warn: usize,
    pub error: usize,
    pub skipped: usize,
    pub cancelled: usize,
    /// Nodes left out of the `node_status` entry to stay inside its budget.
    pub omitted: usize,
}

impl NodeStatusSummary {
    /// Count the whole tree, nothing omitted. The tree covers every planned
    /// node of every segment of a continued run, which a segment's own results
    /// do not.
    pub fn tally(tree: &NodeStatusTree) -> Self {
        Self::of(tree, tree)
    }

    /// `kept` is the trimmed tree written alongside, so `omitted` says how much
    /// of the run the `node_status` entry is not showing.
    fn of(tree: &NodeStatusTree, kept: &NodeStatusTree) -> Self {
        let mut summary = Self::default();
        for status in tree.nodes.values() {
            match status {
                NodeStatus::Pending => summary.pending += 1,
                NodeStatus::Running => summary.running += 1,
                NodeStatus::Success => summary.success += 1,
                NodeStatus::Warn => summary.warn += 1,
                NodeStatus::Error => summary.error += 1,
                NodeStatus::Skipped => summary.skipped += 1,
                NodeStatus::Cancelled => summary.cancelled += 1,
            }
        }
        summary.omitted = tree.nodes.len().saturating_sub(kept.nodes.len());
        summary
    }
}

/// Persist the run log and node status together in a single memo upsert.
///
/// Every payload is capped in bytes to stay within Temporal memo size limits.
/// The full run log is stored as an artifact at workflow completion.
pub fn upsert_memo_state(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    tree: &NodeStatusTree,
    log_lines: &[String],
) -> Result<(), WorkflowTermination> {
    let kept = bounded_node_status(tree);
    let summary = NodeStatusSummary::of(tree, &kept);
    ctx.upsert_memo([
        ("node_status", Some(MemoValue::new(kept))),
        ("node_status_summary", Some(MemoValue::new(summary))),
        ("log", Some(MemoValue::new(truncate_log_for_memo(log_lines)))),
    ])
    .map_err(|e| WorkflowTermination::failed_application(ApplicationFailure::non_retryable(e)))
}

/// Keep the log tail, within both the line cap and the byte budget, prepending
/// a truncation notice.
///
/// Returns owned lines: `MemoValue` defers serialization to the data converter
/// at completion time, so it can only hold `'static` values.
fn truncate_log_for_memo(log_lines: &[String]) -> Vec<String> {
    const NOTICE: &str = "... (log truncated, full log available in artifacts)";

    // One slot of the line cap belongs to the notice itself, so a truncated
    // log is exactly `MEMO_LOG_MAX_LINES` entries and not one more.
    let from_line_cap = log_lines.len().saturating_sub(MEMO_LOG_MAX_LINES - 1);

    // Walk back from the newest line, taking lines while they fit.
    let mut budget = MEMO_LOG_BUDGET.saturating_sub(MEMO_ENVELOPE_OVERHEAD + NOTICE.len());
    let mut first_kept = log_lines.len();
    for (index, line) in log_lines.iter().enumerate().rev() {
        if index < from_line_cap {
            break;
        }
        let cost = line.len() + 3; // quotes and separator
        if cost > budget {
            break;
        }
        budget -= cost;
        first_kept = index;
    }

    if first_kept == 0 {
        return log_lines.to_vec();
    }
    let mut out = Vec::with_capacity(log_lines.len() - first_kept + 1);
    out.push(NOTICE.to_string());
    out.extend_from_slice(&log_lines[first_kept..]);
    out
}

/// The node-status entry, trimmed to its byte budget.
///
/// Running nodes first, then pending: what an operator watching a run needs is
/// what is happening now and what is next. Terminal nodes are dropped first and
/// counted in [`NodeStatusSummary`] instead.
///
/// Sizes are estimated from key and value lengths rather than measured by
/// serializing repeatedly — the estimate is exact for compact JSON of this
/// shape, and cheap enough to run on every level boundary. Being an estimate is
/// safe either way: it is computed identically on replay, which is what
/// workflow code requires of it.
fn bounded_node_status(tree: &NodeStatusTree) -> NodeStatusTree {
    let mut budget = MEMO_NODE_STATUS_BUDGET.saturating_sub(MEMO_ENVELOPE_OVERHEAD);
    let mut nodes = BTreeMap::new();

    // Two passes so a run with more nodes than fit still shows the live ones,
    // whatever their ids sort like.
    for wanted in [
        &[NodeStatus::Running][..],
        &[NodeStatus::Pending][..],
        &[
            NodeStatus::Error,
            NodeStatus::Warn,
            NodeStatus::Cancelled,
            NodeStatus::Success,
            NodeStatus::Skipped,
        ][..],
    ] {
        for (id, status) in &tree.nodes {
            if !wanted.contains(status) {
                continue;
            }
            let cost = id.len() + status.as_str().len() + MEMO_ENTRY_OVERHEAD;
            if cost > budget {
                continue;
            }
            budget -= cost;
            nodes.insert(id.clone(), *status);
        }
    }

    NodeStatusTree { nodes }
}

/// Decide whether to publish the memo *before* scheduling a level.
///
/// The first level always, so a run shows something as soon as it starts, and
/// then on the same periodic cadence as the end-of-level flush. In between,
/// this write only moves one level's nodes from Pending to Running — the
/// level's actual results are published by the end-of-level flush, and on a
/// deep DAG the announcement was a full memo snapshot per level for that.
pub const fn should_announce_level(level_idx: usize, periodic_every_n: usize) -> bool {
    level_idx == 0 || level_idx.is_multiple_of(periodic_every_n)
}

/// Decide whether to flush the memo at the end of a level.
///
/// Always flushes on a level that just had a failure (operators want failure
/// visibility quickly) or on a periodic boundary. The last level is left to
/// the orchestrator's terminal flush so we don't double-write.
pub const fn should_flush_memo(
    level_idx: usize,
    total_levels: usize,
    level_had_failure: bool,
    periodic_every_n: usize,
) -> bool {
    let is_last = level_idx == total_levels.saturating_sub(1);
    if is_last {
        return false;
    }
    if level_had_failure {
        return true;
    }
    (level_idx + 1).is_multiple_of(periodic_every_n)
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
    fn truncate_log_short_passes_through() {
        let lines: Vec<String> = (0..10).map(|i| format!("line {i}")).collect();
        let result = truncate_log_for_memo(&lines);
        assert_eq!(result.len(), 10);
        assert_eq!(result[0], "line 0");
    }

    #[test]
    fn truncate_log_over_limit_keeps_tail() {
        let lines: Vec<String> = (0..300).map(|i| format!("line {i}")).collect();
        let result = truncate_log_for_memo(&lines);
        assert_eq!(result.len(), MEMO_LOG_MAX_LINES);
        assert!(result[0].contains("truncated"));
        assert_eq!(result[result.len() - 1], "line 299");
    }

    fn tree_of(entries: impl IntoIterator<Item = (String, NodeStatus)>) -> NodeStatusTree {
        NodeStatusTree {
            nodes: entries.into_iter().collect(),
        }
    }

    /// Serialized size of what would actually be written to the memo.
    fn memo_bytes(tree: &NodeStatusTree) -> usize {
        serde_json::to_vec(tree)
            .expect("a status tree always serializes")
            .len()
    }

    #[test]
    fn a_small_status_tree_passes_through_whole() {
        let tree = tree_of([
            ("a".to_string(), NodeStatus::Success),
            ("b".to_string(), NodeStatus::Running),
        ]);
        assert_eq!(bounded_node_status(&tree).nodes.len(), 2);
    }

    /// A cap on entry count writes an unbounded number of bytes below it and
    /// keeps *every* pending node above it — which at the start of a run is all
    /// of them. Ids are unique-id strings, so the bound has to be in bytes.
    #[test]
    fn a_tree_of_pending_nodes_stays_inside_its_byte_budget() {
        let tree = tree_of((0..5000).map(|i| {
            (
                format!("model.some_reasonably_named_project.staging.stg_table_{i:04}"),
                NodeStatus::Pending,
            )
        }));

        let kept = bounded_node_status(&tree);
        assert!(
            memo_bytes(&kept) <= MEMO_NODE_STATUS_BUDGET,
            "wrote {} bytes for {} entries",
            memo_bytes(&kept),
            kept.nodes.len()
        );
        assert!(!kept.nodes.is_empty(), "something has to be visible");

        let summary = NodeStatusSummary::of(&tree, &kept);
        assert_eq!(summary.pending, 5000, "the totals are still exact");
        assert_eq!(summary.omitted, 5000 - kept.nodes.len());
    }

    /// What an operator watching a run needs first is what is running now.
    #[test]
    fn running_nodes_survive_a_tree_too_large_to_fit() {
        let mut entries: Vec<(String, NodeStatus)> = (0..5000)
            .map(|i| (format!("model.project.done_{i:04}"), NodeStatus::Success))
            .collect();
        // Sorts after every Success id, so ordering alone would drop it.
        entries.push(("model.project.zzz_live".to_string(), NodeStatus::Running));
        let tree = tree_of(entries);

        let kept = bounded_node_status(&tree);
        assert_eq!(
            kept.nodes.get("model.project.zzz_live"),
            Some(&NodeStatus::Running),
            "the running node must not be crowded out by finished ones"
        );
        assert!(memo_bytes(&kept) <= MEMO_NODE_STATUS_BUDGET);
    }

    /// One enormous adapter error can outweigh a hundred progress lines, so the
    /// line cap alone does not bound the entry.
    #[test]
    fn a_few_very_long_log_lines_stay_inside_the_byte_budget() {
        let lines: Vec<String> = (0..20)
            .map(|i| format!("{i}: {}", "x".repeat(4096)))
            .collect();
        let result = truncate_log_for_memo(&lines);

        let bytes = serde_json::to_vec(&result).expect("lines serialize").len();
        assert!(bytes <= MEMO_LOG_BUDGET + 128, "wrote {bytes} bytes");
        assert!(result[0].contains("truncated"));
        assert_eq!(
            result[result.len() - 1],
            lines[lines.len() - 1],
            "the newest line is the one worth keeping"
        );
    }

    // --- should_announce_level ---

    /// A run has to show something as soon as it starts.
    #[test]
    fn the_first_level_always_announces_itself() {
        assert!(should_announce_level(0, 5));
    }

    /// On a deep DAG the announcement was a full memo snapshot per level, for a
    /// change that only moves one level's nodes from Pending to Running.
    #[test]
    fn intermediate_levels_announce_on_the_periodic_cadence_only() {
        assert!(!should_announce_level(1, 5));
        assert!(!should_announce_level(4, 5));
        assert!(should_announce_level(5, 5));
        assert!(!should_announce_level(6, 5));
        assert!(should_announce_level(10, 5));
    }

    /// The two writes around a level share one cadence, so a level that
    /// announces itself is not also flushed on the way out.
    #[test]
    fn the_two_cadences_do_not_double_write_the_same_level() {
        let every = 5;
        for level in 0..20_usize {
            let announced = should_announce_level(level, every);
            let flushed = should_flush_memo(level, 100, false, every);
            assert!(!(announced && flushed), "level {level} would write the memo twice");
        }
    }

    // --- should_flush_memo ---

    #[test]
    fn should_flush_memo_skips_last_level() {
        // Last level (idx == total - 1) is always handled by the orchestrator's
        // terminal flush; we must not double-write here even on failure.
        assert!(!should_flush_memo(2, 3, true, 10));
        assert!(!should_flush_memo(0, 1, true, 10));
    }

    #[test]
    fn should_flush_memo_flushes_on_failure_in_intermediate_levels() {
        assert!(should_flush_memo(0, 3, true, 10));
        assert!(should_flush_memo(1, 3, true, 10));
    }

    #[test]
    fn should_flush_memo_flushes_on_periodic_boundary() {
        // Every 5th level on a 20-level run, intermediate (not last).
        assert!(should_flush_memo(4, 20, false, 5));
        assert!(should_flush_memo(9, 20, false, 5));
        assert!(!should_flush_memo(0, 20, false, 5));
        assert!(!should_flush_memo(3, 20, false, 5));
    }

    #[test]
    fn should_flush_memo_handles_zero_level_count() {
        // Empty plan — nothing to flush. saturating_sub keeps the math safe.
        assert!(!should_flush_memo(0, 0, true, 10));
    }
}
