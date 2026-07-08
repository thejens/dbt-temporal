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
use common::fault_engine::AdapterFault;
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

// --- injected warehouse faults (via the fault-injecting adapter) ---
//
// These drive the real execute_node path with deterministic, warehouse-shaped
// failures, verifying the retry classification without a live warehouse.

/// Each transient warehouse fault must classify as a retryable `Adapter` error.
#[tokio::test]
async fn injected_transient_warehouse_faults_all_retry() {
    for fault in [
        AdapterFault::connection_failure(),    // Postgres/Redshift 08006
        AdapterFault::deadlock(),              // 40001 serialization
        AdapterFault::snowflake_throttled(),   // HTTP 503, no SQLSTATE
        AdapterFault::bigquery_rate_limited(), // reason string
        AdapterFault::azure_service_busy(),    // vendor code 40501
    ] {
        let harness = Harness::build(&[("good", "select 1 as id")]).await;
        harness.faults().fail_connections(fault);
        let err = harness.run_err("good").await;
        assert!(
            err.is_retryable() && matches!(err, DbtTemporalError::Adapter(_)),
            "fault should be a retryable Adapter error, got: {err:?}"
        );
    }
}

/// A permanent warehouse error stays non-retryable even injected at the
/// connection boundary — the authoritative SQLSTATE class wins.
#[tokio::test]
async fn injected_permanent_error_does_not_retry() {
    let harness = Harness::build(&[("good", "select 1 as id")]).await;
    harness
        .faults()
        .fail_connections(AdapterFault::permanent_undefined_table());
    let err = harness.run_err("good").await;
    assert!(
        !err.is_retryable() && matches!(err, DbtTemporalError::Compilation(_)),
        "a permanent (42P01) error must not retry, got: {err:?}"
    );
}

/// A table model creates its target schema before materializing. A transient
/// failure on that first adapter call is logged and non-fatal — the
/// materialization proceeds and the node still succeeds (partial resilience
/// within one activity, independent of Temporal's activity-level retry).
#[tokio::test]
async fn create_schema_failure_is_non_fatal() {
    let harness =
        Harness::build(&[("t", "{{ config(materialized='table') }}\nselect 1 as id")]).await;
    harness
        .faults()
        .fail_next_executes(1, &AdapterFault::connection_failure());
    harness.run_ok("t").await;
}

/// The recovery guarantee: a model that fails against a briefly-unreachable
/// warehouse succeeds once the warehouse is reachable again — exactly what
/// Temporal's retry buys.
#[tokio::test]
async fn transient_failure_then_recovers() {
    let harness = Harness::build(&[("good", "select 1 as id")]).await;
    harness
        .faults()
        .fail_connections(AdapterFault::connection_failure());
    assert!(
        harness.run_err("good").await.is_retryable(),
        "first attempt should fail retryably"
    );
    harness.faults().clear();
    harness.run_ok("good").await;
}

// --- project hooks (on-run-start / on-run-end) ---
//
// Hooks are a separate activity (run_project_hooks_inner) from execute_node,
// with their own error-wrap sites. `run_query`/`statement` macro calls execute
// as a side effect during Jinja rendering (like a materialization macro), and
// raw (non-Jinja) hook text executes as direct SQL — each had its own
// classify-as-Compilation-or-Adapter wrap, tested independently below.

#[tokio::test]
async fn hook_with_run_query_succeeds() {
    // `{% do run_query(...) %}` discards the return value, leaving the
    // rendered hook output empty — `{{ run_query(...) }}` would instead print
    // the result and re-execute it as SQL, which is a dbt hook-authoring bug,
    // not a dbt-temporal one.
    let yml = "name: spike\nversion: \"1.0.0\"\nprofile: spike\non-run-start:\n  \
               - \"{{ log('starting', info=True) }}\"\n  - \"{% do run_query('select 1') %}\"\n";
    let harness = Harness::build_files(&[("dbt_project.yml", yml)]).await;
    harness
        .run_hooks("on_run_start")
        .await
        .expect("hooks should succeed");
}

#[tokio::test]
async fn hook_run_query_against_missing_table_is_compilation_error() {
    let yml = "name: spike\nversion: \"1.0.0\"\nprofile: spike\non-run-start:\n  \
               - \"{% do run_query('select * from nope') %}\"\n";
    let harness = Harness::build_files(&[("dbt_project.yml", yml)]).await;
    let err = harness.run_hooks_err("on_run_start").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation (permanent catalog error), got: {err:?}"
    );
}

#[tokio::test]
async fn hook_calling_undefined_macro_is_compilation_error() {
    let yml = "name: spike\nversion: \"1.0.0\"\nprofile: spike\non-run-start:\n  \
               - \"{{ nonexistent_macro() }}\"\n";
    let harness = Harness::build_files(&[("dbt_project.yml", yml)]).await;
    let err = harness.run_hooks_err("on_run_start").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation (Jinja render failure), got: {err:?}"
    );
}

#[tokio::test]
async fn hook_run_query_transient_fault_is_a_retryable_adapter_error() {
    // The same recovery guarantee as execute_node: a hook's run_query() call
    // hitting a briefly-unreachable warehouse must be retryable, not permanent.
    //
    // `render_str`'s error type is `FsResult<T> = Result<T, Box<FsError>>` —
    // some Jinja-exposed functions (run_query) convert the underlying
    // AdapterError into an FsError, discarding the AdapterError as a
    // downcastable source but keeping its derived `ErrorCode`
    // (classify_adapter_execution_error checks both).
    let yml = "name: spike\nversion: \"1.0.0\"\nprofile: spike\non-run-start:\n  \
               - \"{% do run_query('select 1') %}\"\n";
    let harness = Harness::build_files(&[("dbt_project.yml", yml)]).await;
    harness
        .faults()
        .fail_next_executes(1, &AdapterFault::connection_failure());
    let err = harness.run_hooks_err("on_run_start").await;
    assert!(
        err.is_retryable() && matches!(err, DbtTemporalError::Adapter(_)),
        "expected a retryable Adapter error, got: {err:?}"
    );
}

#[tokio::test]
async fn hook_raw_sql_against_missing_schema_is_compilation_error() {
    // A hook whose rendered text is non-empty SQL (no Jinja tags) executes
    // directly via `execute_without_state` — a separate wrap site from the
    // run_query-via-render_str path above.
    let yml = "name: spike\nversion: \"1.0.0\"\nprofile: spike\non-run-start:\n  \
               - \"create table nonexistent_schema_xyz.foo as select 1\"\n";
    let harness = Harness::build_files(&[("dbt_project.yml", yml)]).await;
    let err = harness.run_hooks_err("on_run_start").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation (permanent catalog error), got: {err:?}"
    );
}

#[tokio::test]
async fn hook_raw_sql_transient_fault_is_a_retryable_adapter_error() {
    let yml = "name: spike\nversion: \"1.0.0\"\nprofile: spike\non-run-start:\n  \
               - \"create table t_direct as select 1 as id\"\n";
    let harness = Harness::build_files(&[("dbt_project.yml", yml)]).await;
    harness
        .faults()
        .fail_next_executes(1, &AdapterFault::connection_failure());
    let err = harness.run_hooks_err("on_run_start").await;
    assert!(
        err.is_retryable() && matches!(err, DbtTemporalError::Adapter(_)),
        "expected a retryable Adapter error, got: {err:?}"
    );
}

// --- materialization variants ---

#[tokio::test]
async fn ephemeral_model_is_inlined_as_a_cte() {
    // Ephemeral models never materialize as their own relation — they're
    // inlined as a CTE into whatever references them (inject_ephemeral_ctes /
    // persist_ephemeral_chain in node_helpers.rs). Only the referencing
    // model's result should exist; querying the ephemeral's own name directly
    // must fail since it was never created as a table/view.
    let harness = Harness::build(&[
        ("base", "{{ config(materialized='ephemeral') }}\nselect 1 as id"),
        ("downstream", "select id * 2 as doubled from {{ ref('base') }}"),
    ])
    .await;
    let result = harness.run_ok("downstream").await;
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{result:?}");
}

#[tokio::test]
async fn transitive_ephemeral_chain_is_inlined() {
    // base (ephemeral) <- mid (ephemeral, refs base) <- downstream (table, refs mid).
    // Exercises persist_ephemeral_chain's recursive leaf-first persistence.
    let harness = Harness::build(&[
        ("base", "{{ config(materialized='ephemeral') }}\nselect 1 as id"),
        (
            "mid",
            "{{ config(materialized='ephemeral') }}\nselect id * 2 as doubled from {{ ref('base') }}",
        ),
        (
            "downstream",
            "{{ config(materialized='table') }}\nselect doubled + 1 as tripled_ish from {{ ref('mid') }}",
        ),
    ])
    .await;
    harness.run_ok("downstream").await;
}
