//! Indirect test selection — pulling a selected node's tests into the run.
//!
//! `dbt build --select my_model` runs `my_model` *and its tests*. The tests are
//! never named by the selector; dbt adds them afterwards based on the
//! `--indirect-selection` mode. Without this step a selector-scoped run
//! silently skips every test, which looks like a clean run over untested data.
//!
//! The modes come straight from dbt and differ only in how cautious they are
//! about a test whose other parents were *not* selected:
//!
//! | mode | include a test when… |
//! |---|---|
//! | `eager` (default) | any parent is selected |
//! | `cautious` | every parent is selected |
//! | `buildable` | every parent is selected or is an ancestor of something selected |
//! | `empty` | never — tests must be named explicitly |
//!
//! `eager` can therefore run a test whose second parent is not in the run at
//! all; that is dbt's default and the reason `cautious` exists.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::nodes::InternalDbtNode;

use super::dag::build_dependency_map;
use super::selectors::reverse_dep_map;

/// How aggressively to pull in tests that hang off the selected nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndirectSelection {
    /// Any selected parent is enough. dbt's default.
    #[default]
    Eager,
    /// Every parent must be selected.
    Cautious,
    /// Every parent must be selected or reachable as an ancestor of the selection.
    Buildable,
    /// No indirect selection at all.
    Empty,
}

impl FromStr for IndirectSelection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "eager" => Ok(Self::Eager),
            "cautious" => Ok(Self::Cautious),
            "buildable" => Ok(Self::Buildable),
            "empty" => Ok(Self::Empty),
            other => Err(format!(
                "unknown indirect_selection '{other}' — expected eager, cautious, buildable or empty"
            )),
        }
    }
}

/// Add the tests that hang off `selected` according to `mode`.
///
/// Returns the expanded selection. Order is the caller's order followed by any
/// newly-added tests, so the planner's node ordering stays stable.
///
/// `eligible` is the set the *command* allows — indirect selection may only add
/// nodes already in it. Without that bound `dbt run --select my_model` would
/// pull in tests, which `run` does not execute at all.
///
/// Only tests and unit tests are ever added; indirect selection never pulls in
/// a model the user did not ask for.
pub fn expand_indirect_selection(
    selected: Vec<String>,
    nodes: &Nodes,
    eligible: &BTreeSet<String>,
    mode: IndirectSelection,
) -> Vec<String> {
    if mode == IndirectSelection::Empty || selected.is_empty() {
        return selected;
    }

    let selected_set: BTreeSet<String> = selected.iter().cloned().collect();

    // Dependencies over *every* node, not just the selection: the tests we are
    // looking for are by definition outside it.
    let all_ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    let deps = build_dependency_map(nodes, &all_ids);
    let reverse = reverse_dep_map(&deps);

    // `buildable` also accepts parents that the run could have built — anything
    // upstream of the selection — plus sources, which are never "built" but
    // always available.
    let reachable = if mode == IndirectSelection::Buildable {
        Some(selection_with_ancestors(&selected_set, &deps, nodes))
    } else {
        None
    };

    let mut added = Vec::new();
    for candidate in dependents_of(&selected_set, &reverse) {
        if selected_set.contains(&candidate) || !eligible.contains(&candidate) {
            continue;
        }
        let Some(parents) = test_parents(nodes, &candidate) else {
            continue; // not a test — indirect selection never adds models
        };
        let include = match mode {
            IndirectSelection::Eager => true,
            IndirectSelection::Cautious => parents.is_subset(&selected_set),
            IndirectSelection::Buildable => reachable
                .as_ref()
                .is_some_and(|reachable| parents.is_subset(reachable)),
            IndirectSelection::Empty => false,
        };
        if include {
            added.push(candidate);
        }
    }

    let mut out = selected;
    out.extend(added);
    out
}

/// Every node that directly depends on something in `selected`.
fn dependents_of(
    selected: &BTreeSet<String>,
    reverse: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    selected
        .iter()
        .filter_map(|id| reverse.get(id))
        .flat_map(|dependents| dependents.iter().cloned())
        .collect()
}

/// The parents indirect selection judges a test by.
///
/// `None` for anything that is not a test. A unit test is judged only by the
/// model it tests — its fixtures name other nodes it does not actually read at
/// run time, and counting those would make `cautious` reject every unit test.
fn test_parents(nodes: &Nodes, unique_id: &str) -> Option<BTreeSet<String>> {
    if let Some(unit_test) = nodes.unit_tests.get(unique_id) {
        return Some(
            unit_test
                .base()
                .depends_on
                .nodes_with_ref_location
                .first()
                .map(|(dep, _)| dep.clone())
                .into_iter()
                .collect(),
        );
    }
    let test = nodes.tests.get(unique_id)?;
    Some(
        test.base()
            .depends_on
            .nodes_with_ref_location
            .iter()
            .map(|(dep, _)| dep.clone())
            .collect(),
    )
}

/// `selected` plus everything upstream of it, plus all sources.
fn selection_with_ancestors(
    selected: &BTreeSet<String>,
    deps: &BTreeMap<String, BTreeSet<String>>,
    nodes: &Nodes,
) -> BTreeSet<String> {
    let mut reachable = selected.clone();
    let mut queue: Vec<String> = selected.iter().cloned().collect();
    while let Some(id) = queue.pop() {
        for parent in deps.get(&id).into_iter().flatten() {
            if reachable.insert(parent.clone()) {
                queue.push(parent.clone());
            }
        }
    }
    reachable.extend(nodes.sources.keys().cloned());
    reachable
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_dbt_mode_case_insensitively() {
        assert_eq!("eager".parse(), Ok(IndirectSelection::Eager));
        assert_eq!("Cautious".parse(), Ok(IndirectSelection::Cautious));
        assert_eq!(" BUILDABLE ".parse(), Ok(IndirectSelection::Buildable));
        assert_eq!("empty".parse(), Ok(IndirectSelection::Empty));
        assert_eq!(IndirectSelection::default(), IndirectSelection::Eager);
    }

    #[test]
    fn rejects_an_unknown_mode_by_name() {
        let err = "aggressive".parse::<IndirectSelection>().unwrap_err();
        assert!(err.contains("aggressive"), "should name the input: {err}");
        assert!(err.contains("eager"), "should list the valid modes: {err}");
    }

    #[test]
    fn empty_mode_and_empty_selection_are_no_ops() {
        let nodes = Nodes::default();
        let selected = vec!["model.p.a".to_string()];
        let eligible = BTreeSet::new();
        assert_eq!(
            expand_indirect_selection(
                selected.clone(),
                &nodes,
                &eligible,
                IndirectSelection::Empty
            ),
            selected
        );
        assert!(
            expand_indirect_selection(vec![], &nodes, &eligible, IndirectSelection::Eager)
                .is_empty()
        );
    }
}
