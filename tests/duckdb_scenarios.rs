//! Error-scenario tests driven through the real `execute_node` path against an
//! embedded DuckDB warehouse (no Docker). See `tests/common/duckdb.rs`.
//!
//! These document dbt-temporal's error *classification* for each failure mode,
//! which drives retry behavior: `Compilation`/`ProjectNotFound` are
//! non-retryable, `Adapter` is a retryable candidate. Notably, warehouse
//! execution errors caught inside a materialization macro surface as
//! `Compilation` (wrapping the warehouse message), not `Adapter`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::{Harness, raw_engine, raw_query_ok};
use dbt_temporal::error::DbtTemporalError;

/// Isolates "does the ADBC driver load" from the dbt path — the minimal
/// feasibility check for the embedded engine.
#[test]
fn duckdb_driver_loads_and_runs_queries() {
    let engine = raw_engine();
    assert!(raw_query_ok(&engine, "select 1 as id"), "select 1 should run");
    assert!(
        !raw_query_ok(&engine, "select * from a_table_that_does_not_exist"),
        "missing table should fail"
    );
}

#[tokio::test]
async fn good_model_materializes_successfully() {
    let harness = Harness::build(&[("good", "select 1 as id")]).await;
    harness.run_ok("good").await;
}

#[tokio::test]
async fn missing_table_wraps_the_warehouse_error_as_compilation() {
    let harness = Harness::build(&[("bad", "select * from a_table_that_does_not_exist")]).await;
    let err = harness.run_err("bad").await;
    // Caught while the view-materialization macro runs → a Compilation error
    // carrying DuckDB's catalog error, not an Adapter error. Compilation is
    // non-retryable, so a genuinely transient warehouse failure on this path
    // would not be retried.
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("a_table_that_does_not_exist"),
        "should carry the warehouse catalog message, got: {rendered}"
    );
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation, got: {err:?}"
    );
}

#[tokio::test]
async fn sql_syntax_error_is_a_compilation_error() {
    let harness = Harness::build(&[("typo", "selct 1 as id")]).await;
    let err = harness.run_err("typo").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation (parser error), got: {err:?}"
    );
}

#[tokio::test]
async fn type_conversion_error_in_a_table_is_a_compilation_error() {
    // A view stores SQL unevaluated, so a bad cast only errors when the query
    // actually runs — i.e. under a `table` materialization.
    let harness = Harness::build(&[(
        "cast",
        "{{ config(materialized='table') }}\nselect 'not a number'::int as x",
    )])
    .await;
    let err = harness.run_err("cast").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation (conversion error), got: {err:?}"
    );
}

#[tokio::test]
async fn unresolved_ref_is_a_compilation_error() {
    let harness = Harness::build(&[("orphan", "select * from {{ ref('does_not_exist') }}")]).await;
    let err = harness.run_err("orphan").await;
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("unresolved compilation errors"),
        "should report unresolved compilation errors, got: {rendered}"
    );
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation, got: {err:?}"
    );
}

#[tokio::test]
async fn unresolved_source_is_a_compilation_error() {
    let harness =
        Harness::build(&[("from_src", "select * from {{ source('missing_src', 'missing_tbl') }}")])
            .await;
    let err = harness.run_err("from_src").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation, got: {err:?}"
    );
}

#[tokio::test]
async fn model_calling_an_undefined_macro_is_not_registered() {
    // A macro-compile failure drops the node from the resolver state entirely,
    // so executing it reports ProjectNotFound rather than a Compilation error.
    // Documents current behavior — the failure mode differs from a bad ref/source.
    let harness = Harness::build(&[("macrofail", "select {{ nonexistent_macro() }} as x")]).await;
    let err = harness.run_err("macrofail").await;
    assert!(
        matches!(err, DbtTemporalError::ProjectNotFound(_)),
        "expected ProjectNotFound (node dropped during resolve), got: {err:?}"
    );
}

#[tokio::test]
async fn failing_data_test_reports_a_test_failure() {
    // A singular test whose query returns rows = failing rows → TestFailure
    // (non-retryable: the data won't change on retry).
    let harness = Harness::build_files(&[
        ("models/m.sql", "select 1 as id union all select 2 as id"),
        ("tests/all_rows_fail.sql", "select * from {{ ref('m') }}"),
    ])
    .await;
    harness.run_ok("m").await;
    let err = harness.run_err_uid("test.spike.all_rows_fail").await;
    assert!(
        matches!(err, DbtTemporalError::TestFailure { failures, .. } if failures == 2),
        "expected TestFailure with 2 failing rows, got: {err:?}"
    );
}

#[tokio::test]
async fn passing_data_test_succeeds() {
    let harness = Harness::build_files(&[
        ("models/m.sql", "select 1 as id"),
        ("tests/no_negative_ids.sql", "select * from {{ ref('m') }} where id < 0"),
    ])
    .await;
    harness.run_ok("m").await;
    let result = harness
        .run_uid("test.spike.no_negative_ids")
        .await
        .expect("passing test should not error");
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{result:?}");
}

#[tokio::test]
async fn warn_severity_test_does_not_fail_the_run() {
    // A failing test configured `severity: warn` logs a warning but the node
    // still succeeds — the run isn't failed.
    let harness = Harness::build_files(&[
        ("models/m.sql", "select 1 as id"),
        (
            "tests/warn_only.sql",
            "{{ config(severity='warn') }}\nselect * from {{ ref('m') }}",
        ),
    ])
    .await;
    harness.run_ok("m").await;
    let result = harness
        .run_uid("test.spike.warn_only")
        .await
        .expect("warn-severity test should not error");
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{result:?}");
}

#[tokio::test]
async fn store_failures_test_persists_rows_and_still_fails() {
    // `store_failures` writes the failing rows to an audit schema (created on
    // demand) and the test still fails — exercises the store-failures path.
    let harness = Harness::build_files(&[
        ("models/m.sql", "select 1 as id union all select 2 as id"),
        (
            "tests/stored.sql",
            "{{ config(store_failures=true) }}\nselect * from {{ ref('m') }}",
        ),
    ])
    .await;
    harness.run_ok("m").await;
    let err = harness.run_err_uid("test.spike.stored").await;
    assert!(
        matches!(err, DbtTemporalError::TestFailure { .. }),
        "expected TestFailure, got: {err:?}"
    );
}

#[tokio::test]
async fn unreachable_database_is_a_retryable_adapter_error() {
    // The whole point of running dbt on Temporal: a briefly-unreachable
    // warehouse must be retried, not fail the run. An unopenable DuckDB stands
    // in for a dropped connection (a transient "IO Error"). Before the fix this
    // surfaced as a non-retryable Compilation error, defeating recovery.
    let harness = Harness::build_unreachable(&[("good", "select 1 as id")]).await;
    let err = harness.run_err("good").await;
    assert!(err.is_retryable(), "an unreachable database must be retryable, got: {err:?}");
    assert!(
        matches!(err, DbtTemporalError::Adapter(_)),
        "expected a retryable Adapter error, got: {err:?}"
    );
}
