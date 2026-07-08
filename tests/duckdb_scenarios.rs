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
