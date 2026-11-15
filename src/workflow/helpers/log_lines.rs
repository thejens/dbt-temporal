use std::collections::BTreeMap;

use crate::types::{
    ExecutionPlan, FreshnessGrade, NodeExecutionResult, NodeStatus, NodeStatusTree,
};

use super::labels::resource_type_label;
use super::memo::NodeStatusSummary;

/// English pluralisation suffix: empty for 1, "s" otherwise.
pub const fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// Build the dbt-style run-log header: version line, resource-type tally,
/// and concurrency line. Pure — no ctx side effects.
pub fn build_log_header(plan: &ExecutionPlan) -> Vec<String> {
    let mut log_lines = Vec::with_capacity(3);
    log_lines.push(format!("Running with dbt-temporal={}", env!("CARGO_PKG_VERSION")));

    let mut type_counts: BTreeMap<String, usize> = BTreeMap::new();
    for info in plan.nodes.values() {
        *type_counts
            .entry(resource_type_label(info.resource_type))
            .or_default() += 1;
    }
    let parts: Vec<String> = type_counts
        .iter()
        .map(|(t, n)| format!("{n} {t}{}", plural(*n)))
        .collect();
    log_lines.push(format!("Found {}", parts.join(", ")));

    log_lines.push(format!(
        "Concurrency: {} parallel level{}",
        plan.levels.len(),
        plural(plan.levels.len())
    ));
    log_lines
}

/// Build the terminal one-line `current_details` shown on the workflow once it
/// finishes. Doubles as the run's label in the Temporal UI list — a compact
/// pass/error/skip tally so operators can read the outcome without opening the
/// run. Cancelled nodes are folded into the skip count (matching
/// `RunStatusSnapshot::tally`).
///
/// Tallied from the status tree rather than from result lists: a continued run
/// keeps only its final segment's results in memory, while the tree describes
/// every planned node.
pub fn format_final_details(tree: &NodeStatusTree, success: bool, elapsed_secs: f64) -> String {
    let summary = NodeStatusSummary::tally(tree);
    let pass = summary.success;
    let error = summary.error;
    let skip = summary.skipped + summary.cancelled;
    let total = tree.nodes.len();
    let outcome = if success { "passed" } else { "FAILED" };
    format!("{outcome} — {pass} ok, {error} error, {skip} skip of {total} in {elapsed_secs:.2}s")
}

/// Build the dbt-style summary lines that close out a run.
pub fn build_summary_lines(
    total_nodes: usize,
    elapsed_secs: f64,
    pass: usize,
    warn: usize,
    error: usize,
    skip: usize,
) -> [String; 2] {
    [
        format!(
            "Finished running {total_nodes} node{} in {elapsed_secs:.2}s.",
            plural(total_nodes)
        ),
        format!("Done. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total_nodes}"),
    ]
}

/// The extra summary line a freshness run closes with.
///
/// The node-level tally counts a warned check once; this line reports the
/// grading dbt gives each *measurement* (`pass` / `warn` / `error`) from the
/// `FreshnessOutcome`, which is the number an SLA run exists to show.
pub fn build_freshness_summary_line(results: &[NodeExecutionResult]) -> String {
    let with_status = |status: FreshnessGrade| {
        results
            .iter()
            .filter(|r| r.freshness.as_ref().is_some_and(|f| f.status == status))
            .count()
    };
    let pass = with_status(FreshnessGrade::Pass);
    let warn = with_status(FreshnessGrade::Warn);
    // A node past error_after fails its activity and produces no outcome, so
    // the error count comes from the node status rather than from the outcomes.
    let error = results
        .iter()
        .filter(|r| r.status == NodeStatus::Error)
        .count();
    format!("Freshness. PASS={pass} WARN={warn} ERROR={error} TOTAL={}", results.len())
}

/// Format the right-hand side `[tag]` of a per-node log line — the bit after
/// `<package>.<name>  `. Pure; the caller wraps it in the brackets.
pub fn format_result_tag(status: NodeStatus, message: Option<&str>, execution_time: f64) -> String {
    match status {
        NodeStatus::Success => {
            let detail = message.unwrap_or("OK");
            format!("{detail} in {execution_time:.2}s")
        }
        NodeStatus::Warn => {
            let detail = message.unwrap_or("warning");
            format!("WARN {detail} in {execution_time:.2}s")
        }
        // Pending / Running are tagged like the error they are classified as
        // (see `classify_result_status`).
        NodeStatus::Error | NodeStatus::Pending | NodeStatus::Running => {
            let msg = message.unwrap_or("error");
            format!("ERROR {msg}")
        }
        NodeStatus::Skipped => "SKIP".to_string(),
        // Spelled as the scheduler's own line for a cancelled node.
        NodeStatus::Cancelled => "CANCELLED".to_string(),
    }
}

/// "1 of 17 START stg_customers" log lines. The verb is the only thing that
/// changes between START and SKIP — extract once so the formatting stays in
/// one place.
pub fn format_progress_line(num: usize, total: usize, verb: &str, label: &str) -> String {
    format!("{num} of {total} {verb} {label}")
}

/// Build the workflow-detail line shown in the Temporal UI while a level is
/// running, e.g. "level 3/8: model:stg_customers, model:stg_orders".
pub fn format_running_details(level_idx: usize, total_levels: usize, running: &[&str]) -> String {
    format!("level {}/{}: {}", level_idx + 1, total_levels, running.join(", "))
}

/// How many running node labels the workflow details line spells out before
/// summarising the rest, so the line stays short on a wide run.
const DETAILS_MAX_RUNNING_LABELS: usize = 10;

/// Build the workflow-detail line shown in the Temporal UI while the ready
/// queue runs, e.g. "4/17 done, 3 running: model:a, model:b, model:c".
pub fn format_ready_queue_details(done: usize, total: usize, running: &[&str]) -> String {
    let shown = running.len().min(DETAILS_MAX_RUNNING_LABELS);
    let mut parts: Vec<String> = running[..shown].iter().map(ToString::to_string).collect();
    if running.len() > shown {
        parts.push(format!("+{} more", running.len() - shown));
    }
    let head = format!("{done}/{total} done, {} running", running.len());
    if parts.is_empty() {
        head
    } else {
        format!("{head}: {}", parts.join(", "))
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
    use crate::workflow::helpers::test_support::test_plan;
    use crate::workflow::helpers::{error_result, skipped_result};

    #[test]
    fn ready_queue_details_list_running_labels() {
        assert_eq!(
            format_ready_queue_details(4, 17, &["model:a", "model:b"]),
            "4/17 done, 2 running: model:a, model:b"
        );
        assert_eq!(format_ready_queue_details(17, 17, &[]), "17/17 done, 0 running");
    }

    #[test]
    fn ready_queue_details_cap_the_labels_on_a_wide_run() {
        let labels: Vec<String> = (0..25).map(|i| format!("m{i}")).collect();
        let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        let line = format_ready_queue_details(0, 100, &refs);
        assert!(line.starts_with("0/100 done, 25 running: m0, m1,"), "{line}");
        assert!(line.contains("m9, +15 more"), "{line}");
        assert!(!line.contains("m10"), "{line}");
    }

    // --- plural ---

    #[test]
    fn plural_omits_s_for_one_only() {
        assert_eq!(plural(0), "s");
        assert_eq!(plural(1), "");
        assert_eq!(plural(2), "s");
        assert_eq!(plural(100), "s");
    }

    // --- build_log_header / build_summary_lines ---

    #[test]
    fn build_log_header_lists_resource_type_counts_in_alpha_order() {
        let plan = test_plan(
            vec![vec!["m1", "m2"], vec!["t1"]],
            &[
                ("m1", Some("table"), vec![]),
                ("m2", Some("view"), vec![]),
                ("t1", Some("test"), vec![]),
            ],
        );
        let header = build_log_header(&plan);
        assert_eq!(header.len(), 3);
        assert!(header[0].starts_with("Running with dbt-temporal="));
        // `test_plan` builds only models; the tally is pluralised through the helper.
        assert_eq!(header[1], "Found 3 models");
        assert_eq!(header[2], "Concurrency: 2 parallel levels");
    }

    #[test]
    fn build_log_header_singular_for_single_level_and_node_type() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", Some("table"), vec![])]);
        let header = build_log_header(&plan);
        assert_eq!(header[1], "Found 1 model");
        assert_eq!(header[2], "Concurrency: 1 parallel level");
    }

    fn result_with_status(uid: &str, status: NodeStatus) -> NodeExecutionResult {
        NodeExecutionResult::not_executed(uid, status, None)
    }

    fn tree_from_statuses(statuses: &[NodeStatus]) -> NodeStatusTree {
        NodeStatusTree {
            nodes: statuses
                .iter()
                .enumerate()
                .map(|(i, s)| (format!("model.p.n{i:03}"), *s))
                .collect(),
        }
    }

    #[test]
    fn format_final_details_passed_summarises_counts() {
        let tree = tree_from_statuses(&[
            NodeStatus::Success,
            NodeStatus::Success,
            NodeStatus::Skipped,
        ]);
        let details = format_final_details(&tree, true, 3.456);
        assert_eq!(details, "passed — 2 ok, 0 error, 1 skip of 3 in 3.46s");
    }

    #[test]
    fn format_final_details_all_passed() {
        let tree = tree_from_statuses(&[NodeStatus::Success, NodeStatus::Success]);
        assert_eq!(
            format_final_details(&tree, true, 0.5),
            "passed — 2 ok, 0 error, 0 skip of 2 in 0.50s"
        );
    }

    #[test]
    fn format_final_details_empty_run() {
        assert_eq!(
            format_final_details(&tree_from_statuses(&[]), true, 0.0),
            "passed — 0 ok, 0 error, 0 skip of 0 in 0.00s"
        );
    }

    #[test]
    fn format_final_details_failed_folds_cancelled_into_skip() {
        let tree = tree_from_statuses(&[
            NodeStatus::Success,
            NodeStatus::Error,
            NodeStatus::Skipped,
            NodeStatus::Cancelled,
        ]);
        let details = format_final_details(&tree, false, 12.0);
        // Cancelled nodes count toward skip, matching RunStatusSnapshot::tally.
        assert_eq!(details, "FAILED — 1 ok, 1 error, 2 skip of 4 in 12.00s");
    }

    /// A continued run holds only its last segment's results, but the tree
    /// still names every node: the tally and its total must come from the tree.
    #[test]
    fn tally_and_total_agree_when_the_tree_outgrows_the_result_list() {
        let mut statuses = vec![NodeStatus::Success; 5];
        statuses.extend([NodeStatus::Warn, NodeStatus::Error, NodeStatus::Skipped]);
        let tree = tree_from_statuses(&statuses);
        let final_segment_results = [result_with_status("model.p.n007", NodeStatus::Skipped)];
        assert!(tree.nodes.len() > final_segment_results.len());

        let summary = NodeStatusSummary::tally(&tree);
        assert_eq!((summary.success, summary.warn, summary.error, summary.skipped), (5, 1, 1, 1));
        assert_eq!(summary.omitted, 0);
        assert_eq!(
            format_final_details(&tree, false, 1.0),
            "FAILED — 5 ok, 1 error, 1 skip of 8 in 1.00s"
        );
    }

    #[test]
    fn build_summary_lines_pluralises_node_count() {
        let lines = build_summary_lines(5, 2.345_678, 3, 1, 0, 1);
        assert_eq!(lines[0], "Finished running 5 nodes in 2.35s.");
        assert_eq!(lines[1], "Done. PASS=3 WARN=1 ERROR=0 SKIP=1 TOTAL=5");
    }

    #[test]
    fn build_summary_lines_singular_for_one_node() {
        let lines = build_summary_lines(1, 0.5, 1, 0, 0, 0);
        assert_eq!(lines[0], "Finished running 1 node in 0.50s.");
    }

    #[test]
    fn freshness_summary_separates_warn_from_pass() {
        let measured = |unique_id: &str, status: FreshnessGrade| {
            let mut result = skipped_result(unique_id, "");
            result.status = NodeStatus::Success;
            result.freshness = Some(crate::types::FreshnessOutcome {
                max_loaded_at: "2026-06-12T10:00:00+00:00".into(),
                snapshotted_at: "2026-06-12T11:00:00+00:00".into(),
                max_loaded_at_time_ago_in_s: 3600.0,
                status,
                resource_type: "source".into(),
                criteria: dbt_schemas::schemas::common::FreshnessDefinition::default(),
            });
            result
        };
        let results = vec![
            measured("source.p.s.a", FreshnessGrade::Pass),
            measured("source.p.s.b", FreshnessGrade::Warn),
            // A stale node fails its activity and carries no outcome.
            error_result("model.p.c", "stale"),
        ];
        assert_eq!(
            build_freshness_summary_line(&results),
            "Freshness. PASS=1 WARN=1 ERROR=1 TOTAL=3"
        );
        assert_eq!(build_freshness_summary_line(&[]), "Freshness. PASS=0 WARN=0 ERROR=0 TOTAL=0");
    }

    // --- format_result_tag ---

    #[test]
    fn format_result_tag_success_uses_message_or_ok_default() {
        assert_eq!(
            format_result_tag(NodeStatus::Success, Some("CREATE TABLE (4 rows)"), 1.234),
            "CREATE TABLE (4 rows) in 1.23s"
        );
        assert_eq!(format_result_tag(NodeStatus::Success, None, 0.05), "OK in 0.05s");
    }

    #[test]
    fn format_result_tag_error_includes_message_or_default() {
        assert_eq!(
            format_result_tag(NodeStatus::Error, Some("permission denied"), 0.1),
            "ERROR permission denied"
        );
        assert_eq!(format_result_tag(NodeStatus::Error, None, 0.0), "ERROR error");
    }

    #[test]
    fn format_result_tag_skipped_is_constant() {
        assert_eq!(format_result_tag(NodeStatus::Skipped, Some("ignored"), 9.0), "SKIP");
    }

    #[test]
    fn format_result_tag_covers_every_status() {
        for (status, message, expected) in [
            (NodeStatus::Success, None, "OK in 2.50s"),
            (NodeStatus::Warn, None, "WARN warning in 2.50s"),
            (NodeStatus::Warn, Some("2 rows"), "WARN 2 rows in 2.50s"),
            (NodeStatus::Error, None, "ERROR error"),
            (NodeStatus::Skipped, Some("ignored"), "SKIP"),
            (NodeStatus::Cancelled, None, "CANCELLED"),
            // Tagged as the error `classify_result_status` makes of them.
            (NodeStatus::Pending, None, "ERROR error"),
            (NodeStatus::Running, Some("still going"), "ERROR still going"),
        ] {
            assert_eq!(format_result_tag(status, message, 2.5), expected, "{status}");
        }
    }

    // --- format_progress_line ---

    #[test]
    fn format_progress_line_renders_dbt_style() {
        assert_eq!(
            format_progress_line(3, 17, "START", "model waffle.stg_customers"),
            "3 of 17 START model waffle.stg_customers"
        );
        assert_eq!(format_progress_line(1, 1, "SKIP", "x"), "1 of 1 SKIP x");
    }

    // --- format_running_details ---

    #[test]
    fn format_running_details_one_indexes_level_and_joins_running() {
        // level_idx is zero-based but the UI shows 1-based.
        assert_eq!(
            format_running_details(2, 5, &["model:a", "model:b"]),
            "level 3/5: model:a, model:b"
        );
    }

    #[test]
    fn format_running_details_handles_empty_running() {
        assert_eq!(format_running_details(0, 1, &[]), "level 1/1: ");
    }
}
