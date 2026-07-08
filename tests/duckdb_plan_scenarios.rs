//! `activities::plan::select_command_node_ids` — the pure node-selection filter
//! extracted from `plan_project_inner` (which otherwise needs a real
//! `ActivityContext::new(Arc<CoreWorker>, ...)`, unreachable from this harness).
//! Exercised directly against a real `WorkerState` built by the DuckDB harness.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::Harness;
use dbt_temporal::activities::plan::select_command_node_ids;
use dbt_temporal::types::DbtRunInput;

fn build_input(command: &str) -> DbtRunInput {
    serde_json::from_value(serde_json::json!({ "command": command })).unwrap()
}

#[tokio::test]
async fn ephemeral_models_are_excluded_from_selection() {
    let harness = Harness::build(&[
        ("base", "{{ config(materialized='ephemeral') }}\nselect 1 as id"),
        ("downstream", "select id from {{ ref('base') }}"),
    ])
    .await;
    let ids = select_command_node_ids(harness.state(), &build_input("build"))
        .expect("selection should succeed");
    assert!(
        ids.iter().any(|id| id.contains("downstream")),
        "downstream model should be selected, got: {ids:?}"
    );
    assert!(
        !ids.iter().any(|id| id.contains("model.spike.base")),
        "ephemeral model must be excluded, got: {ids:?}"
    );
}

#[tokio::test]
async fn build_with_only_an_ephemeral_model_finds_no_nodes() {
    // After excluding the (only) ephemeral model, nothing is left to plan.
    let harness =
        Harness::build(&[("base", "{{ config(materialized='ephemeral') }}\nselect 1 as id")]).await;
    let err = select_command_node_ids(harness.state(), &build_input("build"))
        .expect_err("no nodes should remain");
    assert!(err.to_string().contains("no nodes found for command"), "got: {err}");
}

#[tokio::test]
async fn generic_test_macro_definition_is_excluded_and_normal_test_is_not() {
    // A file under tests/ whose body is a generic-test macro DEFINITION
    // (`{% test ... %}...{% endtest %}`) is misregistered by the parser as a
    // runnable test node; select_command_node_ids filters it back out. An
    // ordinary singular test (applied SQL, not a macro def) is unaffected.
    let harness = Harness::build_files(&[
        ("models/m.sql", "select 1 as id"),
        (
            "tests/generic_def.sql",
            "{% test not_actually_a_real_test(model, column_name) %}\nselect 1\n{% endtest %}",
        ),
        ("tests/applied.sql", "select * from {{ ref('m') }} where id < 0"),
    ])
    .await;
    let ids = select_command_node_ids(harness.state(), &build_input("build"))
        .expect("selection should succeed");
    assert!(
        ids.iter().any(|id| id.contains("applied")),
        "the applied test should be selected, got: {ids:?}"
    );
    assert!(
        !ids.iter().any(|id| id.contains("generic_def")),
        "the generic-test macro definition must be excluded, got: {ids:?}"
    );
}

#[tokio::test]
async fn run_command_selects_models_only() {
    let harness = Harness::build_files(&[
        ("models/m.sql", "select 1 as id"),
        ("tests/t.sql", "select * from {{ ref('m') }} where id < 0"),
    ])
    .await;
    let ids = select_command_node_ids(harness.state(), &build_input("run"))
        .expect("selection should succeed");
    assert!(ids.iter().any(|id| id.starts_with("model.")), "got: {ids:?}");
    assert!(
        !ids.iter().any(|id| id.starts_with("test.")),
        "`run` should exclude tests, got: {ids:?}"
    );
}
