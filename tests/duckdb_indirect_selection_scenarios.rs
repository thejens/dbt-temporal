//! `--indirect-selection`: which tests a selector-scoped run pulls in.
//!
//! dbt runs a selected model's tests even though the selector never names them.
//! dbt-temporal previously did not, so `--select my_model` under `build` ran the
//! model and silently skipped every test on it — a green run over unverified
//! data. These drive the real planner against a real project.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::Harness;
use dbt_temporal::activities::indirect::{IndirectSelection, expand_indirect_selection};
use dbt_temporal::activities::plan::select_command_node_ids;
use dbt_temporal::activities::selectors::apply_selectors;
use dbt_temporal::types::DbtRunInput;

/// A model with a test, plus a second model the shared test also depends on.
///
/// `both` is tested by a relationships test that references `a` as well, which
/// is the case the modes actually disagree about.
async fn project() -> Harness {
    Harness::build_files(&[
        ("models/a.sql", "select 1 as id"),
        ("models/both.sql", "select 1 as id"),
        (
            "models/schema.yml",
            "version: 2\n\
             models:\n\
             \x20 - name: a\n\
             \x20   columns:\n\
             \x20     - name: id\n\
             \x20       tests: [not_null]\n\
             \x20 - name: both\n\
             \x20   columns:\n\
             \x20     - name: id\n\
             \x20       tests:\n\
             \x20         - relationships:\n\
             \x20             to: ref('a')\n\
             \x20             field: id\n",
        ),
    ])
    .await
}

/// Run selection the way the planner does, then expand for `mode`.
fn selected_with(harness: &Harness, select: &str, mode: IndirectSelection) -> Vec<String> {
    selected_for_command(harness, "build", select, mode)
}

/// Same, for an arbitrary command — the command bounds what may be added.
fn selected_for_command(
    harness: &Harness,
    command: &str,
    select: &str,
    mode: IndirectSelection,
) -> Vec<String> {
    let input: DbtRunInput =
        serde_json::from_value(serde_json::json!({ "command": command })).unwrap();
    let all = select_command_node_ids(harness.state(), &input).unwrap();
    let eligible: std::collections::BTreeSet<String> = all.iter().cloned().collect();
    let nodes = &harness.state().resolver_state.nodes;
    let direct = apply_selectors(all, nodes, Some(select), None, None).unwrap();
    expand_indirect_selection(direct, nodes, &eligible, mode)
}

fn has_test_on(ids: &[String], needle: &str) -> bool {
    ids.iter()
        .any(|id| id.starts_with("test.") && id.contains(needle))
}

/// The regression: selecting a model must bring its own test along.
#[tokio::test]
async fn eager_pulls_in_the_selected_models_test() {
    let harness = project().await;
    let ids = selected_with(&harness, "a", IndirectSelection::Eager);

    assert!(ids.iter().any(|id| id == "model.spike.a"), "the selected model itself: {ids:?}");
    assert!(has_test_on(&ids, "not_null_a_id"), "a's test must be included: {ids:?}");
    assert!(
        !ids.iter().any(|id| id == "model.spike.both"),
        "indirect selection must never add a model: {ids:?}"
    );
}

/// `empty` is the old behavior, now opt-in rather than the accident.
#[tokio::test]
async fn empty_selects_no_tests_at_all() {
    let harness = project().await;
    let ids = selected_with(&harness, "a", IndirectSelection::Empty);

    assert_eq!(ids, vec!["model.spike.a".to_string()], "only the model: {ids:?}");
}

/// The case the modes disagree on: a test with a parent outside the selection.
/// `eager` takes it, `cautious` does not.
#[tokio::test]
async fn eager_and_cautious_differ_on_a_test_with_an_unselected_parent() {
    let harness = project().await;

    let eager = selected_with(&harness, "both", IndirectSelection::Eager);
    assert!(
        has_test_on(&eager, "relationships"),
        "eager takes a test whose other parent is unselected: {eager:?}"
    );

    let cautious = selected_with(&harness, "both", IndirectSelection::Cautious);
    assert!(
        !has_test_on(&cautious, "relationships"),
        "cautious requires every parent selected: {cautious:?}"
    );
}

/// `buildable` sits between the two: the unselected parent is an ancestor of
/// nothing here, so it behaves like `cautious` for this shape.
#[tokio::test]
async fn buildable_requires_parents_to_be_selected_or_upstream() {
    let harness = project().await;

    let buildable = selected_with(&harness, "both", IndirectSelection::Buildable);
    assert!(
        !has_test_on(&buildable, "relationships"),
        "`a` is not upstream of `both`, so buildable must not take the test: {buildable:?}"
    );

    // Selecting both parents satisfies every mode.
    let all_selected = selected_with(&harness, "a both", IndirectSelection::Buildable);
    assert!(
        has_test_on(&all_selected, "relationships"),
        "with both parents selected the test is buildable: {all_selected:?}"
    );
}

/// An unfiltered `build` already contains every test, so expanding is a no-op
/// rather than a source of duplicates.
#[tokio::test]
async fn expanding_a_full_selection_adds_nothing() {
    let harness = project().await;
    let input: DbtRunInput =
        serde_json::from_value(serde_json::json!({ "command": "build" })).unwrap();
    let all = select_command_node_ids(harness.state(), &input).unwrap();

    let eligible: std::collections::BTreeSet<String> = all.iter().cloned().collect();
    let expanded = expand_indirect_selection(
        all.clone(),
        &harness.state().resolver_state.nodes,
        &eligible,
        IndirectSelection::Eager,
    );
    assert_eq!(expanded.len(), all.len(), "no duplicates: {expanded:?}");
}

/// Regression: indirect selection must respect the command's node-type filter.
/// `dbt run` executes models only, so `run --select a` must not gain `a`'s
/// tests just because they depend on it.
#[tokio::test]
async fn run_command_gains_no_tests_even_under_eager() {
    let harness = project().await;
    let ids = selected_for_command(&harness, "run", "a", IndirectSelection::Eager);

    assert_eq!(
        ids,
        vec!["model.spike.a".to_string()],
        "run is models-only, so eager must add nothing: {ids:?}"
    );
}
