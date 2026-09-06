//! Routing a node to the engine its `+adapter` selection names, against a real
//! two-adapter target loaded by the DuckDB harness.
//!
//! DuckDB is the target's default and the only adapter that can actually be
//! reached; Postgres is declared with a host nothing listens on. That asymmetry
//! is what makes the tests discriminating: a node routed to Postgres cannot
//! quietly succeed on DuckDB, and a node that fell back to DuckDB by mistake
//! cannot look like a Postgres failure.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::{Harness, PROJECT};
use dbt_adapter::AdapterType;
use dbt_temporal::types::NodeStatus;

/// A target declaring both adapters, DuckDB marked default. The Postgres entry
/// points at a port nothing listens on: it is declared so the engine exists and
/// nodes may select it, never so a query succeeds.
const TWO_ADAPTER_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     - type: duckdb\n        default: true\n        path: \"{DB_PATH}\"\n        \
     schema: main\n        threads: 1\n      \
     - type: postgres\n        host: 127.0.0.1\n        port: 1\n        \
     user: nobody\n        password: nobody\n        dbname: nothing\n        \
     schema: public\n        threads: 1\n";

/// The adapter the parser resolved onto a model — the node's own `+adapter`
/// selection when it made one, the target's default otherwise.
fn node_adapter(harness: &Harness, model: &str) -> AdapterType {
    harness
        .state()
        .resolver_state
        .nodes
        .get_node(&format!("model.{PROJECT}.{model}"))
        .unwrap_or_else(|| panic!("model {model} was not resolved"))
        .base()
        .adapter
}

#[tokio::test]
async fn a_target_declaring_two_adapters_builds_an_engine_for_each() {
    let harness = Harness::build_files_with_profile(
        &[("models/plain.sql", "select 1 as id")],
        TWO_ADAPTER_PROFILE,
    )
    .await;

    let engines = &harness.state().adapter_engines;
    assert_eq!(engines.default_type(), AdapterType::DuckDB);
    assert_eq!(
        engines.declared().collect::<Vec<_>>(),
        vec![AdapterType::DuckDB, AdapterType::Postgres],
        "both declared adapters must have an engine, in declaration order"
    );
    assert_eq!(
        engines
            .get(AdapterType::Postgres, "test")
            .unwrap()
            .adapter_type(),
        AdapterType::Postgres,
        "the postgres lookup must not hand back the default engine"
    );
}

/// A node that selects no adapter runs on the target's default — the fallback
/// that keeps every existing single-adapter project working unchanged.
#[tokio::test]
async fn a_node_without_an_adapter_selection_runs_on_the_target_default() {
    let harness = Harness::build_files_with_profile(
        &[("models/plain.sql", "select 42 as answer")],
        TWO_ADAPTER_PROFILE,
    )
    .await;

    assert_eq!(
        node_adapter(&harness, "plain"),
        AdapterType::DuckDB,
        "an unannotated node takes the target's default adapter"
    );

    let result = harness.run("plain").await.expect("plain model should run");
    assert_eq!(result.status, NodeStatus::Success, "{result:?}");
    // Proof it reached the DuckDB warehouse rather than merely reporting success.
    assert_eq!(harness.query_scalar("select answer from main.plain"), "42");
}

/// A node that selects the non-default adapter is routed to *that* engine. The
/// Postgres engine cannot connect, so the run fails — and the failure is the
/// evidence: had the selection been ignored, the model would have materialized
/// on DuckDB like its unannotated neighbour.
#[tokio::test]
async fn a_node_selecting_the_non_default_adapter_is_routed_to_its_engine() {
    let harness = Harness::build_files_with_profile(
        &[("models/on_postgres.sql", "{{ config(adapter='postgres') }}\nselect 1 as id")],
        TWO_ADAPTER_PROFILE,
    )
    .await;

    assert_eq!(
        node_adapter(&harness, "on_postgres"),
        AdapterType::Postgres,
        "the `+adapter` selection must survive parse onto the node"
    );

    let err = harness.run_err("on_postgres").await;
    let msg = err.to_string();
    assert!(
        err.is_retryable(),
        "a warehouse that refuses a connection is a retryable adapter error: {msg}"
    );
    // The declared Postgres endpoint, which appears nowhere in the DuckDB
    // config — so the node demonstrably dialled the adapter it selected.
    assert!(msg.contains("127.0.0.1"), "{msg}");

    // And nothing was written to the DuckDB warehouse under that name.
    assert_eq!(
        harness
            .query_scalar("select count(*) from duckdb_tables() where table_name = 'on_postgres'"),
        "0",
        "a node routed to postgres must leave no relation on the default adapter"
    );
}

/// A node naming an adapter the target does not declare has no credentials to
/// run against, so it fails permanently rather than falling back to the default
/// and writing to the wrong warehouse.
#[tokio::test]
async fn a_node_selecting_an_undeclared_adapter_fails_without_falling_back() {
    let harness = Harness::build_files_with_profile(
        &[("models/on_snowflake.sql", "{{ config(adapter='snowflake') }}\nselect 1 as id")],
        TWO_ADAPTER_PROFILE,
    )
    .await;

    let err = harness.run_err("on_snowflake").await;
    let msg = err.to_string();
    assert!(
        !err.is_retryable(),
        "no number of attempts makes an undeclared adapter appear: {msg}"
    );
    assert!(msg.contains("snowflake"), "{msg}");
    assert!(
        msg.contains(&format!("model.{PROJECT}.on_snowflake")),
        "the message must name the node that asked: {msg}"
    );
}
