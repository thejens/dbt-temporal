//! The project-check gate: run every check against the index, before anything
//! downstream is built.
//!
//! Placement is the whole design. The worker has already parsed the project and
//! built the index the checks query, and the workflow has planned the DAG but
//! scheduled none of it — so a failing check needs no graph edges to stop
//! anything, the workflow simply does not proceed. That is where dbt puts the
//! same gate, and for the same reason: gating individual nodes would let a node
//! with no analyze task materialize straight through a failure.
//!
//! A run's selection scopes each check's *rows*; it never decides whether a
//! check runs. A selector that matches nothing therefore yields `skipped`
//! rather than `pass`, because a green result must not stand in for a check
//! that examined nothing.

pub mod evaluate;
pub mod index;

use std::collections::BTreeSet;
use std::sync::Arc;

use dbt_index_core::ingest::metadata_to_parquet::index_is_current;
use dbt_schemas::schemas::DbtCheck;
use dbt_schemas::schemas::common::Severity;
use tracing::{info, warn};

use crate::activities::DbtActivities;
use crate::activities::node_telemetry::invocation_span;
use crate::types::{CheckResult, CheckStatus, ProjectChecksInput, ProjectChecksOutput};
use crate::worker::project_checks::ProjectChecks;

use self::evaluate::{evaluate_batch, selection_filter_for, zero_rows_is_vacuous};
use self::index::IndexReader;

/// Violation rows carried on a failing check's message. Matches dbt's own
/// preview budget — enough to recognise the problem, short enough that a
/// project whose checks all fail still fits in the workflow memo.
const MAX_PREVIEW_ROWS: usize = 5;

/// Run the project's checks and report a verdict per check.
///
/// Errors only when the project cannot be looked up. Every per-check outcome,
/// "could not be evaluated" included, is reported in the output rather than
/// raised: the workflow gates on `failed`, so an unevaluable check has to reach
/// it as data instead of as an activity failure that retries.
pub fn run_project_checks_inner(
    activities: &DbtActivities,
    input: &ProjectChecksInput,
) -> Result<ProjectChecksOutput, anyhow::Error> {
    // Reading the index runs dbt code, and dbt's data layer asserts that every
    // dbt span sits under an `Invocation` root. `dbt check` is what dbt calls
    // this command, so that is what the span reports.
    let _invocation = invocation_span(&input.invocation_id, "dbt check").entered();

    let state = activities.registry.get(Some(input.project.as_str()))?;
    let Some(checks) = state.project_checks.as_ref() else {
        return Ok(ProjectChecksOutput::default());
    };
    let scope: Option<BTreeSet<String>> = input
        .scope
        .as_ref()
        .map(|ids| ids.iter().cloned().collect());

    let output = evaluate_all(checks, scope.as_ref());
    info!(
        project = %input.project,
        invocation_id = %input.invocation_id,
        checks = output.results.len(),
        failed = output.failed,
        "project checks evaluated"
    );
    Ok(output)
}

/// Evaluate every check against the index, or report all of them unevaluable
/// when the index cannot be read.
fn evaluate_all(checks: &ProjectChecks, scope: Option<&BTreeSet<String>>) -> ProjectChecksOutput {
    // Checks read the index and never build it. If it does not reflect the
    // parse it was built from there is nothing trustworthy to query — and every
    // check must say so rather than return zero rows, which reads as a pass.
    let results = match open_reader(checks) {
        Ok(mut reader) => checks
            .checks
            .iter()
            .map(|check| evaluate_one(&mut reader, check, scope))
            .collect(),
        Err(reason) => {
            warn!(reason, "project-check index is unreadable; no check could be evaluated");
            checks
                .checks
                .iter()
                .map(|check| unevaluable(check, reason.clone()))
                .collect()
        }
    };
    ProjectChecksOutput::new(results)
}

/// Open the index, or say in user-facing terms why it cannot be queried.
fn open_reader(checks: &ProjectChecks) -> Result<IndexReader, String> {
    if !index_is_current(&checks.metadata_dir, &checks.index_dir) {
        return Err(format!(
            "the metadata index at {} does not reflect the parse it was built from",
            checks.index_dir.display()
        ));
    }
    IndexReader::open(&checks.index_dir).map_err(|e| format!("{e:#}"))
}

/// Run one check and turn the rows it reported into a verdict.
fn evaluate_one(
    reader: &mut IndexReader,
    check: &Arc<DbtCheck>,
    scope: Option<&BTreeSet<String>>,
) -> CheckResult {
    let filter = selection_filter_for(check.deprecated_config.selection_filter_on.as_ref());
    let Some(sql) = check.__check_attr__.compiled_sql.as_deref() else {
        return unevaluable(check, "check has no rendered SQL".to_string());
    };

    let eval = match reader
        .query(sql)
        .and_then(|batch| evaluate_batch(&batch, &filter, scope, MAX_PREVIEW_ROWS))
    {
        // An execution error is fatal whatever the severity: what the check
        // would have found is unknown, so reporting a pass would be a guess.
        Err(message) => return unevaluable(check, message),
        Ok(eval) => eval,
    };

    let (status, message, violations) = if eval.violations == 0 {
        if zero_rows_is_vacuous(scope, &filter, &eval.reported_kinds) {
            (
                CheckStatus::Skipped,
                Some("selection matched no nodes this check can report on".to_string()),
                None,
            )
        } else {
            (CheckStatus::Pass, None, Some(0))
        }
    } else {
        let hard = check
            .deprecated_config
            .severity
            .clone()
            .unwrap_or(Severity::Error)
            == Severity::Error;
        (
            if hard {
                CheckStatus::Fail
            } else {
                CheckStatus::Warn
            },
            (!eval.preview.is_empty()).then(|| eval.preview.join("\n  ")),
            Some(eval.violations),
        )
    };

    CheckResult {
        unique_id: check.__common_attr__.unique_id.clone(),
        name: check.__common_attr__.name.clone(),
        status,
        message,
        violations,
    }
}

/// A check that could not be evaluated at all.
fn unevaluable(check: &Arc<DbtCheck>, message: String) -> CheckResult {
    CheckResult {
        unique_id: check.__common_attr__.unique_id.clone(),
        name: check.__common_attr__.name.clone(),
        status: CheckStatus::Error,
        message: Some(message),
        violations: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::worker::project_checks::without_an_index;

    fn check(name: &str) -> Arc<DbtCheck> {
        let mut check = DbtCheck::default();
        check.__common_attr__.name = name.to_string();
        check.__common_attr__.unique_id = format!("check.spike.{name}");
        Arc::new(check)
    }

    /// The load-bearing property of the whole gate: an index that cannot be
    /// queried must make every check say so. Reporting zero rows instead would
    /// read as "nothing wrong", which is the one wrong answer here.
    #[test]
    fn an_unreadable_index_makes_every_check_unevaluable_rather_than_passing() {
        let checks = without_an_index(vec![check("no_orphan_models"), check("every_model_owned")]);

        let output = evaluate_all(&checks, None);

        assert_eq!(output.results.len(), 2);
        assert_eq!(
            output.failed, 2,
            "an unevaluable check must stop the run: {:?}",
            output.results
        );
        for result in &output.results {
            assert_eq!(result.status, CheckStatus::Error, "{result:?}");
            assert!(
                result
                    .message
                    .as_deref()
                    .unwrap_or_default()
                    .contains("index"),
                "the message must say what could not be read: {result:?}"
            );
            assert!(
                result.violations.is_none(),
                "a check that never ran has no violation count: {result:?}"
            );
        }
    }

    /// A project with checks but no readable index still reports one verdict
    /// per check, so the summary cannot be mistaken for "no checks declared".
    #[test]
    fn every_declared_check_gets_a_verdict_even_when_none_could_run() {
        let checks = without_an_index(vec![check("a"), check("b"), check("c")]);
        let output = evaluate_all(&checks, None);
        let names: Vec<&str> = output.results.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
