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

/// A project carrying every freshness shape the planner has to tell apart: a
/// source with criteria, a source without, a model with an SLA, a model whose
/// only `freshness` key is the `build_after` scheduling rule, and a plain model.
const FRESHNESS_SHAPES_YML: &str = "
version: 2

sources:
  - name: raw
    schema: main
    tables:
      - name: events
        config:
          loaded_at_field: loaded_at
          freshness:
            error_after: {count: 1, period: day}
      - name: plain_source

models:
  - name: sla
    config:
      freshness:
        loaded_at_field: updated_at
        warn_after: {count: 12, period: hour}
  - name: scheduled
    config:
      freshness:
        build_after: {count: 1, period: day}
  - name: plain_model
";

async fn freshness_shapes_harness() -> Harness {
    Harness::build_files(&[
        ("models/schema.yml", FRESHNESS_SHAPES_YML),
        ("models/sla.sql", "select 1 as id, current_timestamp as updated_at"),
        ("models/scheduled.sql", "select 1 as id"),
        ("models/plain_model.sql", "select 1 as id"),
        ("models/from_source.sql", "select * from {{ source('raw', 'events') }}"),
    ])
    .await
}

#[tokio::test]
async fn freshness_selects_sources_and_sla_models_only() {
    let harness = freshness_shapes_harness().await;
    let ids = select_command_node_ids(harness.state(), &build_input("freshness"))
        .expect("selection should succeed");

    assert!(ids.iter().any(|id| id.ends_with("raw.events")), "got: {ids:?}");
    assert!(ids.iter().any(|id| id.ends_with("spike.sla")), "got: {ids:?}");
    // A source with no criteria, a `build_after`-only model (a scheduling
    // rule, not an SLA) and a plain model have nothing to measure.
    for absent in ["plain_source", ".scheduled", ".plain_model", ".from_source"] {
        assert!(!ids.iter().any(|id| id.ends_with(absent)), "{absent} in {ids:?}");
    }
}

#[tokio::test]
async fn source_freshness_never_selects_models() {
    let harness = freshness_shapes_harness().await;
    let ids = select_command_node_ids(harness.state(), &build_input("source-freshness"))
        .expect("selection should succeed");
    assert_eq!(ids.len(), 1, "only the source with criteria: {ids:?}");
    assert!(ids[0].ends_with("raw.events"), "got: {ids:?}");
}

#[tokio::test]
async fn resource_type_narrows_a_freshness_plan_to_models() {
    let harness = freshness_shapes_harness().await;
    let mut input = build_input("freshness");
    input.resource_types = vec!["model".to_string()];
    let ids = select_command_node_ids(harness.state(), &input).expect("selection should succeed");
    assert_eq!(ids.len(), 1, "sources are ruled out: {ids:?}");
    assert!(ids[0].ends_with("spike.sla"), "got: {ids:?}");

    let mut excluded = build_input("freshness");
    excluded.exclude_resource_types = vec!["model".to_string()];
    let ids =
        select_command_node_ids(harness.state(), &excluded).expect("selection should succeed");
    assert_eq!(ids.len(), 1, "models are ruled out: {ids:?}");
    assert!(ids[0].ends_with("raw.events"), "got: {ids:?}");
}

#[tokio::test]
async fn an_unknown_resource_type_is_rejected() {
    let harness = freshness_shapes_harness().await;
    let mut input = build_input("build");
    input.resource_types = vec!["sources".to_string()];
    let err = select_command_node_ids(harness.state(), &input).expect_err("plural is not a type");
    assert!(format!("{err:#}").contains("resource type 'sources'"), "got: {err:#}");
}

#[tokio::test]
async fn a_half_filled_freshness_rule_aborts_the_plan() {
    // dbt only warns about `count` without `period` at parse time and re-raises
    // when the rule is consumed. Dropping the node instead would make an
    // unchecked SLA look exactly like one that always passes.
    let harness = Harness::build_files(&[
        (
            "models/schema.yml",
            "
version: 2

models:
  - name: sla
    config:
      freshness:
        loaded_at_field: updated_at
        error_after: {count: 12}
",
        ),
        ("models/sla.sql", "select 1 as id, current_timestamp as updated_at"),
    ])
    .await;
    let err = select_command_node_ids(harness.state(), &build_input("freshness"))
        .expect_err("a half-filled rule must abort");
    assert!(
        err.to_string()
            .contains("error_after needs both count and period"),
        "got: {err}"
    );
}

#[tokio::test]
async fn an_sla_with_no_loaded_at_is_skipped_rather_than_measured() {
    // dbt would fall back to a batch INFORMATION_SCHEMA metadata query for this
    // node; dbt-temporal does not drive the adapter's metadata interface, so
    // the node is left out of the plan entirely.
    //
    // Both models materialize as tables: dbt rejects at resolve time an SLA on
    // a *view* that names neither key, since a view's relation metadata records
    // when its definition changed rather than how recent its data is.
    let harness = Harness::build_files(&[
        (
            "models/schema.yml",
            "
version: 2

models:
  - name: sla
    config:
      materialized: table
      freshness:
        error_after: {count: 12, period: hour}
  - name: measurable
    config:
      materialized: table
      freshness:
        loaded_at_field: updated_at
        error_after: {count: 12, period: hour}
",
        ),
        ("models/sla.sql", "select 1 as id"),
        ("models/measurable.sql", "select 1 as id, current_timestamp as updated_at"),
    ])
    .await;
    let ids = select_command_node_ids(harness.state(), &build_input("freshness"))
        .expect("selection should succeed");
    assert_eq!(ids.len(), 1, "only the measurable model: {ids:?}");
    assert!(ids[0].ends_with(".measurable"), "got: {ids:?}");
}
