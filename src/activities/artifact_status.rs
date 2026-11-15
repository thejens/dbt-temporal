//! The status words of `run_results.json`, in one place.
//!
//! Three vocabularies meet at the artifact: this worker's [`NodeStatus`], the
//! freshness grade, and dbt's own words (`pass`/`fail` for assertions, `reused`,
//! `partial success`, ...). The writer maps the first two into the third; the
//! `retry_from` reader maps the third back into a decision. Both go through
//! [`ArtifactStatus`] so they cannot drift apart.

use crate::types::{FreshnessGrade, NodeStatus};

/// A status as it appears in a `run_results.json` row.
///
/// Covers everything this worker writes plus every other word dbt writes, so
/// results produced by the dbt CLI parse too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStatus {
    Success,
    /// An assertion (data test, unit test) that held.
    Pass,
    Warn,
    Error,
    /// An assertion that did not hold.
    Fail,
    Skipped,
    /// dbt's word for a source freshness check that could not run.
    RuntimeError,
    /// A microbatch model where only some batches succeeded.
    PartialSuccess,
    /// A node dbt reused rather than rebuilt.
    Reused,
    NoOp,
    /// Written by this worker only: dbt has no cancellation status.
    Cancelled,
    /// Written by this worker only, for a node that never finished.
    Pending,
    Running,
}

impl ArtifactStatus {
    /// The word dbt writes for this status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fail => "fail",
            Self::Skipped => "skipped",
            Self::RuntimeError => "runtime error",
            Self::PartialSuccess => "partial success",
            Self::Reused => "reused",
            Self::NoOp => "no-op",
            Self::Cancelled => "cancelled",
            Self::Pending => "pending",
            Self::Running => "running",
        }
    }

    /// Parse a status word; `None` for a word this worker does not know.
    pub fn parse(word: &str) -> Option<Self> {
        Some(match word {
            "success" => Self::Success,
            "pass" => Self::Pass,
            "warn" => Self::Warn,
            "error" => Self::Error,
            "fail" => Self::Fail,
            "skipped" => Self::Skipped,
            "runtime error" => Self::RuntimeError,
            "partial success" => Self::PartialSuccess,
            "reused" => Self::Reused,
            "no-op" => Self::NoOp,
            "cancelled" => Self::Cancelled,
            "pending" => Self::Pending,
            "running" => Self::Running,
            _ => return None,
        })
    }

    /// The status dbt reports for a node this worker ran to `status`.
    ///
    /// Assertions (`test.`, `unit_test.`) pass or fail where a build
    /// succeeds or errors; everything else keeps its own word, `warn`
    /// included.
    pub fn for_node(unique_id: &str, status: NodeStatus) -> Self {
        let assertion = unique_id.starts_with("test.") || unique_id.starts_with("unit_test.");
        match status {
            NodeStatus::Success if assertion => Self::Pass,
            NodeStatus::Error if assertion => Self::Fail,
            NodeStatus::Success => Self::Success,
            NodeStatus::Error => Self::Error,
            NodeStatus::Warn => Self::Warn,
            NodeStatus::Skipped => Self::Skipped,
            NodeStatus::Cancelled => Self::Cancelled,
            NodeStatus::Pending => Self::Pending,
            NodeStatus::Running => Self::Running,
        }
    }

    /// A freshness check reports the grade it measured.
    pub const fn for_freshness(grade: FreshnessGrade) -> Self {
        match grade {
            FreshnessGrade::Pass => Self::Pass,
            FreshnessGrade::Warn => Self::Warn,
            FreshnessGrade::Error => Self::Error,
        }
    }

    /// Whether `retry_from` re-runs a node that ended in this status.
    ///
    /// dbt retries `error`, `fail` and `skipped` (its own `RETRYABLE_STATUSES`);
    /// `warn` only under `--warn-error`, which a retry here has no equivalent of.
    /// `runtime error` is a failure under another name. `cancelled`, `pending`
    /// and `running` mean the node never finished, so it still has to run.
    /// `success`, `pass`, `warn`, `partial success`, `reused` and `no-op` are
    /// outcomes dbt does not retry.
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Error
                | Self::Fail
                | Self::Skipped
                | Self::RuntimeError
                | Self::Cancelled
                | Self::Pending
                | Self::Running
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [ArtifactStatus; 13] = [
        ArtifactStatus::Success,
        ArtifactStatus::Pass,
        ArtifactStatus::Warn,
        ArtifactStatus::Error,
        ArtifactStatus::Fail,
        ArtifactStatus::Skipped,
        ArtifactStatus::RuntimeError,
        ArtifactStatus::PartialSuccess,
        ArtifactStatus::Reused,
        ArtifactStatus::NoOp,
        ArtifactStatus::Cancelled,
        ArtifactStatus::Pending,
        ArtifactStatus::Running,
    ];

    #[test]
    fn every_status_round_trips_through_its_word() {
        for status in ALL {
            assert_eq!(ArtifactStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(ArtifactStatus::parse("exploded"), None);
    }

    #[test]
    fn retryable_set_matches_dbt() {
        let retryable: Vec<&str> = ALL
            .iter()
            .filter(|s| s.is_retryable())
            .map(|s| s.as_str())
            .collect();
        assert_eq!(
            retryable,
            [
                "error",
                "fail",
                "skipped",
                "runtime error",
                "cancelled",
                "pending",
                "running"
            ]
        );
    }

    #[test]
    fn assertions_use_pass_and_fail() {
        for id in ["test.p.t", "unit_test.p.m.ut"] {
            assert_eq!(ArtifactStatus::for_node(id, NodeStatus::Success), ArtifactStatus::Pass);
            assert_eq!(ArtifactStatus::for_node(id, NodeStatus::Error), ArtifactStatus::Fail);
            assert_eq!(ArtifactStatus::for_node(id, NodeStatus::Warn), ArtifactStatus::Warn);
        }
        assert_eq!(
            ArtifactStatus::for_node("model.p.m", NodeStatus::Success),
            ArtifactStatus::Success
        );
        assert_eq!(ArtifactStatus::for_node("model.p.m", NodeStatus::Error), ArtifactStatus::Error);
    }

    #[test]
    fn every_node_status_has_an_artifact_word() {
        let expected = [
            (NodeStatus::Pending, "pending"),
            (NodeStatus::Running, "running"),
            (NodeStatus::Warn, "warn"),
            (NodeStatus::Skipped, "skipped"),
            (NodeStatus::Cancelled, "cancelled"),
        ];
        for (status, word) in expected {
            assert_eq!(ArtifactStatus::for_node("model.p.m", status).as_str(), word);
        }
    }

    #[test]
    fn freshness_grades_keep_their_words() {
        assert_eq!(ArtifactStatus::for_freshness(FreshnessGrade::Pass).as_str(), "pass");
        assert_eq!(ArtifactStatus::for_freshness(FreshnessGrade::Warn).as_str(), "warn");
        assert_eq!(ArtifactStatus::for_freshness(FreshnessGrade::Error).as_str(), "error");
    }
}
