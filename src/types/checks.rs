//! Payload types for the project-check gate.
//!
//! A project check is a SQL file under `checks/` that asserts something about
//! the project itself — every row it returns is a violation, zero rows is a
//! pass. dbt evaluates them before the task graph exists, so a failing check
//! stops the build before anything is compiled or materialized; the gate
//! activity reproduces that, and the workflow fails the run on its verdict.

use serde::{Deserialize, Serialize};

/// One check's verdict, in the vocabulary dbt's `run_results` uses for
/// assertions.
///
/// `Skipped` is not a pass: it means the selection left the check nothing it
/// could report on, so nothing was actually verified. `Error` means the check
/// could not be evaluated at all — never reported as a pass, because what it
/// would have found is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Pass,
    Fail,
    Warn,
    Error,
    Skipped,
}

impl CheckStatus {
    /// Whether this outcome must stop the run.
    ///
    /// `Warn` does not: a warn-severity check reports its rows and lets the
    /// build proceed. `Error` does, whatever the check's severity — a check
    /// that has quietly stopped working is exactly the one a run must not
    /// wave through.
    pub const fn is_fatal(self) -> bool {
        matches!(self, Self::Fail | Self::Error)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Skipped => "skipped",
        }
    }
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single check's result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub unique_id: String,
    pub name: String,
    pub status: CheckStatus,
    /// Violation preview rows, or the reason the check could not be evaluated.
    pub message: Option<String>,
    /// Rows that survived scoping. `None` when the check never ran.
    pub violations: Option<u64>,
}

/// Input to the `run_project_checks` activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectChecksInput {
    /// Resolved project name — always set by the workflow from the plan.
    pub project: String,
    pub invocation_id: String,
    /// Node ids a violation row must name to count, or `None` for the whole
    /// project.
    ///
    /// `None` when the run had no selector. Scoping against the *default*
    /// selection would be wrong on both counts: it is not a statement of
    /// intent, and it does not cover every node kind a check may legitimately
    /// report on — a check keyed on groups or macros would have every
    /// violation filtered away and report a vacuous pass.
    #[serde(default)]
    pub scope: Option<Vec<String>>,
}

/// What running the project's checks established.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectChecksOutput {
    /// One entry per check that ran, ordered by `unique_id`.
    pub results: Vec<CheckResult>,
    /// Checks whose outcome must stop the run. Non-zero means the workflow
    /// must not proceed to execution.
    pub failed: usize,
}

impl ProjectChecksOutput {
    /// Tally the verdicts. `failed` is derived here rather than accumulated by
    /// the caller so that "which outcomes stop a run" has exactly one answer,
    /// the one [`CheckStatus::is_fatal`] gives.
    pub fn new(results: Vec<CheckResult>) -> Self {
        let failed = results.iter().filter(|r| r.status.is_fatal()).count();
        Self { results, failed }
    }

    /// One `status  name (detail)` line per check, for the run log and memo.
    ///
    /// Passing checks are included: a gate that only speaks up when it fails
    /// leaves an operator unable to tell "every check passed" from "this
    /// project has no checks".
    pub fn summary_lines(&self) -> Vec<String> {
        self.results
            .iter()
            .map(|r| {
                let count = r
                    .violations
                    .filter(|v| *v > 0)
                    .map(|v| format!(" ({v} violation(s))"))
                    .unwrap_or_default();
                let detail = r
                    .message
                    .as_deref()
                    .map(|m| format!(": {m}"))
                    .unwrap_or_default();
                format!("{:<7} check  {}{count}{detail}", r.status.as_str().to_uppercase(), r.name)
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn only_fail_and_error_stop_the_run() {
        assert!(CheckStatus::Fail.is_fatal());
        assert!(CheckStatus::Error.is_fatal());
        assert!(!CheckStatus::Pass.is_fatal());
        assert!(!CheckStatus::Warn.is_fatal());
        assert!(!CheckStatus::Skipped.is_fatal());
    }

    /// The wire spelling is what a caller reading `run_results`-shaped output
    /// sees, so it is pinned rather than left to the derive.
    #[test]
    fn status_serializes_as_the_run_results_vocabulary() {
        for (status, spelled) in [
            (CheckStatus::Pass, "pass"),
            (CheckStatus::Fail, "fail"),
            (CheckStatus::Warn, "warn"),
            (CheckStatus::Error, "error"),
            (CheckStatus::Skipped, "skipped"),
        ] {
            assert_eq!(status.as_str(), spelled);
            assert_eq!(status.to_string(), spelled);
            assert_eq!(serde_json::to_string(&status).unwrap(), format!("\"{spelled}\""));
        }
    }

    /// A caller that never scoped its run omits `scope` entirely, so the field
    /// has to decode as "no selector" rather than fail.
    #[test]
    fn input_decodes_without_a_scope() {
        let input: ProjectChecksInput =
            serde_json::from_str(r#"{"project":"shop","invocation_id":"abc"}"#).unwrap();
        assert!(input.scope.is_none());
    }

    fn result(name: &str, status: CheckStatus, violations: Option<u64>) -> CheckResult {
        CheckResult {
            unique_id: format!("check.shop.{name}"),
            name: name.to_string(),
            status,
            message: None,
            violations,
        }
    }

    #[test]
    fn failed_counts_only_the_outcomes_that_stop_a_run() {
        let output = ProjectChecksOutput::new(vec![
            result("a", CheckStatus::Pass, Some(0)),
            result("b", CheckStatus::Warn, Some(3)),
            result("c", CheckStatus::Fail, Some(1)),
            result("d", CheckStatus::Error, None),
            result("e", CheckStatus::Skipped, None),
        ]);
        assert_eq!(output.failed, 2);
        assert_eq!(output.results.len(), 5);
    }

    /// An operator reading the log has to be able to tell a clean gate from a
    /// project with no gate at all, so passes are reported too.
    #[test]
    fn summary_reports_passing_checks_as_well_as_failing_ones() {
        let mut failing = result("no_orphans", CheckStatus::Fail, Some(2));
        failing.message = Some("unique_id=model.shop.a".to_string());
        let output = ProjectChecksOutput::new(vec![
            result("all_documented", CheckStatus::Pass, Some(0)),
            failing,
        ]);
        let lines = output.summary_lines();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("PASS"), "got: {}", lines[0]);
        assert!(lines[0].contains("all_documented"), "got: {}", lines[0]);
        assert!(lines[1].starts_with("FAIL"), "got: {}", lines[1]);
        assert!(lines[1].contains("2 violation(s)"), "got: {}", lines[1]);
        assert!(lines[1].contains("model.shop.a"), "got: {}", lines[1]);
    }

    /// A pass reports zero violations; printing "(0 violation(s))" on every
    /// clean line is noise the reader has to filter out.
    #[test]
    fn summary_omits_a_zero_violation_count() {
        let output = ProjectChecksOutput::new(vec![result("clean", CheckStatus::Pass, Some(0))]);
        assert!(!output.summary_lines()[0].contains("violation"));
    }
}
