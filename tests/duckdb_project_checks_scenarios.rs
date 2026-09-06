//! The project-check gate, end to end against a real project.
//!
//! Everything a check depends on is built for real here — the parse epochs, the
//! index ingested from them, and the DuckDB views over that index — because the
//! failure mode this feature has to avoid is a check that reads nothing and
//! reports a confident pass. A mocked index would reproduce that bug rather
//! than catch it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::{Harness, PROJECT};
use dbt_temporal::types::{CheckStatus, ProjectChecksOutput};

/// A project declaring checks must set `info_schema.version`, which pins the
/// `dbt.*` vocabulary its check SQL was written against.
const PROJECT_YML: &str =
    "name: spike\nversion: \"1.0.0\"\nprofile: spike\ninfo_schema:\n  version: 1\n";

fn verdict<'a>(
    output: &'a ProjectChecksOutput,
    name: &str,
) -> &'a dbt_temporal::types::CheckResult {
    output
        .results
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("no result for check '{name}' in {:?}", output.results))
}

#[tokio::test]
async fn a_project_without_checks_has_no_index_and_no_verdicts() {
    let harness = Harness::build(&[("m", "select 1 as id")]).await;
    assert!(
        harness.state().project_checks.is_none(),
        "a project with no checks must not pay for an index"
    );
    let output = harness.project_checks(None);
    assert!(output.results.is_empty());
    assert_eq!(output.failed, 0);
}

/// The load-bearing case: a check that finds nothing wrong must have actually
/// read the index. `dbt.models` is a parse-safe view over the index parquet, so
/// a passing verdict here proves the whole pipeline — epochs, ingest, views —
/// produced something queryable.
#[tokio::test]
async fn a_satisfied_check_passes_against_a_real_index() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/m.sql", "select 1 as id"),
        (
            "checks/no_seeds.sql",
            "select unique_id from {{ info_schema('models') }} where 1 = 0",
        ),
    ])
    .await;
    let output = harness.project_checks(None);
    assert_eq!(output.failed, 0, "{:?}", output.results);
    let result = verdict(&output, "no_seeds");
    assert_eq!(result.status, CheckStatus::Pass);
    assert_eq!(result.violations, Some(0));
}

/// The index has to hold the project's actual nodes, not an empty shell — a
/// check whose rows come back names the models this project declared.
#[tokio::test]
async fn a_violated_check_fails_the_gate_and_names_the_offending_nodes() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/undocumented.sql", "select 1 as id"),
        (
            "checks/every_model_is_documented.sql",
            "select unique_id, name from {{ info_schema('models') }} \
             where description is null or description = ''",
        ),
    ])
    .await;
    let output = harness.project_checks(None);
    assert_eq!(output.failed, 1, "{:?}", output.results);
    let result = verdict(&output, "every_model_is_documented");
    assert_eq!(result.status, CheckStatus::Fail);
    assert_eq!(result.violations, Some(1));
    let message = result.message.as_deref().expect("a preview of the rows");
    assert!(
        message.contains(&format!("model.{PROJECT}.undocumented")),
        "the preview should name the model: {message}"
    );
}

/// `severity: warn` reports its violations and lets the build proceed. Without
/// this the config would be indistinguishable from `error` — both produce rows.
#[tokio::test]
async fn a_warn_severity_check_reports_violations_without_gating() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/m.sql", "select 1 as id"),
        (
            "checks/advisory.sql",
            "{{ config(severity='warn') }}\nselect unique_id from {{ info_schema('models') }}",
        ),
    ])
    .await;
    let output = harness.project_checks(None);
    assert_eq!(output.failed, 0, "warn must not stop the run: {:?}", output.results);
    let result = verdict(&output, "advisory");
    assert_eq!(result.status, CheckStatus::Warn);
    assert_eq!(result.violations, Some(1));
}

/// A check that cannot execute is an error whatever its severity: what it would
/// have found is unknown, so anything but `error` would be a guess — and the
/// run must stop on it.
#[tokio::test]
async fn a_check_that_cannot_execute_is_an_error_that_stops_the_run() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/m.sql", "select 1 as id"),
        (
            "checks/broken.sql",
            "{{ config(severity='warn') }}\nselect * from dbt.no_such_view",
        ),
    ])
    .await;
    let output = harness.project_checks(None);
    assert_eq!(output.failed, 1, "{:?}", output.results);
    let result = verdict(&output, "broken");
    assert_eq!(result.status, CheckStatus::Error);
    assert_eq!(result.violations, None, "nothing was counted, so nothing is reported");
}

/// `dbt_internal` holds the index's raw tables, whose columns stay empty until
/// compile. Reaching one has to fail to bind rather than return zero rows,
/// which a check would report as a pass.
#[tokio::test]
async fn the_raw_index_tables_are_not_reachable_from_a_check() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/m.sql", "select 1 as id"),
        ("checks/reaches_past_the_views.sql", "select * from dbt.nodes"),
    ])
    .await;
    let result = &harness.project_checks(None).results[0];
    assert_eq!(result.status, CheckStatus::Error, "`dbt.nodes` must not resolve: {result:?}");
}

/// A selector scopes a check's rows. A violation outside the selection is not
/// this run's problem, so the check passes for the subset that was asked for.
#[tokio::test]
async fn a_selector_scopes_violations_to_the_selected_nodes() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/kept.sql", "select 1 as id"),
        ("models/dropped.sql", "select 2 as id"),
        (
            "checks/every_model_is_documented.sql",
            "select unique_id from {{ info_schema('models') }} \
             where description is null or description = ''",
        ),
    ])
    .await;

    let unscoped = harness.project_checks(None);
    assert_eq!(unscoped.failed, 1);
    assert_eq!(verdict(&unscoped, "every_model_is_documented").violations, Some(2));

    let scoped = harness.project_checks(Some(&[&format!("model.{PROJECT}.kept")]));
    assert_eq!(verdict(&scoped, "every_model_is_documented").violations, Some(1));
}

/// The subtle one: a scope holding nothing the check reports on examined
/// nothing, so its zero rows are `skipped` rather than a pass. Reporting a pass
/// here would claim a validation that never ran.
#[tokio::test]
async fn a_scope_the_check_cannot_report_on_is_skipped_not_passed() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/m.sql", "select 1 as id"),
        ("seeds/s.csv", "id\n1\n"),
        (
            "checks/every_model_is_documented.sql",
            "select unique_id from {{ info_schema('models') }} \
             where description is null or description = ''",
        ),
    ])
    .await;
    let output = harness.project_checks(Some(&[&format!("seed.{PROJECT}.s")]));
    let result = verdict(&output, "every_model_is_documented");
    assert_eq!(result.status, CheckStatus::Skipped);
    assert_eq!(
        output.failed, 0,
        "a skip is not a failure — it says the gate had nothing to look at"
    );
}

/// `selection_filter_on: none` is what an aggregate check sets, and it has to
/// survive a selector that names none of the rows it reports.
#[tokio::test]
async fn a_check_opting_out_of_scoping_still_sees_the_whole_project() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/a.sql", "select 1 as id"),
        ("models/b.sql", "select 2 as id"),
        (
            "checks/not_too_many_models.sql",
            "{{ config(selection_filter_on='none') }}\n\
             select count(*) as total from {{ info_schema('models') }} having count(*) > 1",
        ),
    ])
    .await;
    let output = harness.project_checks(Some(&[&format!("model.{PROJECT}.a")]));
    let result = verdict(&output, "not_too_many_models");
    assert_eq!(
        result.status,
        CheckStatus::Fail,
        "an aggregate check is never narrowed by a selector: {result:?}"
    );
}

/// A disabled check is not evaluated at all — it must not appear as a verdict,
/// green or otherwise.
#[tokio::test]
async fn a_disabled_check_produces_no_verdict() {
    let harness = Harness::build_files(&[
        ("dbt_project.yml", PROJECT_YML),
        ("models/m.sql", "select 1 as id"),
        (
            "checks/off.sql",
            "{{ config(enabled=false) }}\nselect unique_id from {{ info_schema('models') }}",
        ),
    ])
    .await;
    assert!(
        harness.state().project_checks.is_none(),
        "a project whose only check is disabled declares no gate"
    );
    assert!(harness.project_checks(None).results.is_empty());
}
