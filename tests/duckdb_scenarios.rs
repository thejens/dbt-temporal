//! Error-scenario tests driven through the real `execute_node` path against an
//! embedded DuckDB warehouse (no Docker). See `tests/common/duckdb.rs`.

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
async fn bad_sql_model_wraps_the_warehouse_error_as_compilation() {
    let harness = Harness::build(&[("bad", "select * from a_table_that_does_not_exist")]).await;
    let err = harness.run_err("bad").await;
    // The missing table is caught while the view-materialization macro runs, so
    // it surfaces as a Compilation error that carries DuckDB's catalog error —
    // not an Adapter error. (Classification matters: Compilation is non-retryable.)
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("a_table_that_does_not_exist"),
        "error should carry the warehouse catalog message, got: {rendered}"
    );
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected a Compilation error, got: {err:?}"
    );
}

#[tokio::test]
async fn model_with_unresolved_ref_is_a_compilation_error() {
    // `ref()` to a non-existent model can't compile — a distinct path from a
    // warehouse execution error.
    let harness = Harness::build(&[("orphan", "select * from {{ ref('does_not_exist') }}")]).await;
    let err = harness.run_err("orphan").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected a Compilation error, got: {err:?}"
    );
}
