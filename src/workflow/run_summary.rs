//! The CLI-style summary appended to a run's log.

use super::helpers::{NodeStatusSummary, build_freshness_summary_line, build_summary_lines};
use super::levels::LevelExecutionOutcome;
use crate::types::DbtCommand;

/// Append the CLI-style run summary (pass/error/skip tallies) to the run log.
///
/// A freshness run gets one line more: its three-way grading does not fit the
/// pass/error/skip tally.
///
/// Counted from the status tree, which covers every planned node of every
/// segment; `all_results` holds only the final segment of a continued run, so
/// counting it would pair per-segment tallies with a whole-run TOTAL.
pub(super) fn append_run_summary(levels: &mut LevelExecutionOutcome, elapsed: f64, command: &str) {
    let tally = NodeStatusSummary::tally(&levels.node_status);
    levels.log_lines.extend(build_summary_lines(
        levels.total_nodes,
        elapsed,
        tally.success,
        tally.warn,
        tally.error,
        tally.skipped,
    ));
    // A freshness plan is one level, and a run only continues between levels,
    // so its results never span segments.
    if DbtCommand::parse(command).is_some_and(DbtCommand::is_freshness) {
        levels
            .log_lines
            .push(build_freshness_summary_line(&levels.all_results));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::types::{NodeStatus, NodeStatusTree};

    /// The shape of a continued run: the final segment holds one result, while
    /// the tree names every node. The summary's counts and TOTAL come from the
    /// tree, so they agree.
    #[test]
    fn run_summary_counts_the_whole_tree_on_a_continued_run() {
        let mut level_outcome = LevelExecutionOutcome {
            prior_segments: Vec::new(),
            log_lines: Vec::new(),
            all_results: vec![],
            node_status: NodeStatusTree {
                nodes: [
                    NodeStatus::Success,
                    NodeStatus::Success,
                    NodeStatus::Warn,
                    NodeStatus::Error,
                    NodeStatus::Skipped,
                    NodeStatus::Skipped,
                ]
                .into_iter()
                .enumerate()
                .map(|(i, status)| (format!("model.p.n{i}"), status))
                .collect(),
            },
            had_failure: true,
            was_cancelled: false,
            total_nodes: 6,
            node_counter: 1,
            continue_at_level: None,
            failed_nodes: BTreeSet::new(),
        };

        append_run_summary(&mut level_outcome, 2.0, "run");

        assert_eq!(
            level_outcome.log_lines,
            vec![
                "Finished running 6 nodes in 2.00s.".to_string(),
                "Done. PASS=2 WARN=1 ERROR=1 SKIP=2 TOTAL=6".to_string(),
            ]
        );
    }
}
