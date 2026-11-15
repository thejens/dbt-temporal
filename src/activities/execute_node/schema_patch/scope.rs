//! Which relations a node's statement can name, and so which ones need their
//! per-workflow location resolved.

use std::collections::BTreeSet;

/// What the scope walk needs to know about a node.
#[derive(Debug, Default, Clone)]
pub(super) struct Edges {
    pub depends_on: Vec<String>,
    pub ephemeral: bool,
    /// The model a unit test runs, whose SQL the test's own statement wraps.
    pub tested_model: Option<String>,
}

/// The node itself plus the relations its compiled SQL can mention.
///
/// A statement names a relation in exactly two ways: through a `ref()` /
/// `source()` of the node it belongs to, which resolve to that node's direct
/// dependencies; and through the CTEs an ephemeral dependency injects, whose
/// own `ref()`s are that ephemeral's dependencies — and, if one of those is
/// ephemeral too, its dependencies in turn. A non-ephemeral dependency is
/// opaque: its SQL ran in its own activity and the consumer sees only its
/// relation name, so nothing behind it can appear. A unit test is the one node
/// whose SQL belongs to another: it wraps the SQL of the model under test, so
/// that model is walked as if it were the test's own.
///
/// Ephemeral models are walked but not returned: they never exist as a
/// relation, only as a CTE name that no rewrite touches.
pub(super) fn rewrite_scope(
    unique_id: &str,
    edges: impl Fn(&str) -> Option<Edges>,
) -> BTreeSet<String> {
    let mut scope = BTreeSet::from([unique_id.to_owned()]);
    let mut inlined = BTreeSet::new();
    let mut owners = vec![unique_id.to_owned()];
    if let Some(tested) = edges(unique_id).and_then(|e| e.tested_model) {
        owners.push(tested);
    }

    // Each entry is a node whose SQL is part of the statement.
    while let Some(owner) = owners.pop() {
        let Some(owner_edges) = edges(&owner) else {
            continue;
        };
        for dep in owner_edges.depends_on {
            let Some(dep_edges) = edges(&dep) else {
                continue;
            };
            if dep_edges.ephemeral {
                if inlined.insert(dep.clone()) {
                    owners.push(dep);
                }
            } else {
                scope.insert(dep);
            }
        }
    }
    scope
}

/// [`Edges`] for every node of the project, looked up by unique id.
pub(super) fn project_edges(
    nodes: &dbt_schemas::schemas::Nodes,
) -> impl Fn(&str) -> Option<Edges> + '_ {
    move |id| {
        let node = nodes.get_node(id)?;
        let base = node.base();
        Some(Edges {
            depends_on: base.depends_on.nodes.clone(),
            ephemeral: base
                .materialized
                .to_string()
                .eq_ignore_ascii_case("ephemeral"),
            tested_model: nodes.unit_tests.get(id).and_then(|unit| {
                // dbt stores the tested model as a unit test's first
                // dependency when the resolver did not record it directly.
                unit.tested_node_unique_id
                    .clone()
                    .or_else(|| unit.__base_attr__.depends_on.nodes.first().cloned())
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn graph(spec: &[(&str, &[&str], bool)]) -> BTreeMap<String, Edges> {
        spec.iter()
            .map(|(id, deps, ephemeral)| {
                (
                    (*id).to_owned(),
                    Edges {
                        depends_on: deps.iter().map(|d| (*d).to_owned()).collect(),
                        ephemeral: *ephemeral,
                        tested_model: None,
                    },
                )
            })
            .collect()
    }

    fn scope_of(graph: &BTreeMap<String, Edges>, id: &str) -> Vec<String> {
        rewrite_scope(id, |n| graph.get(n).cloned())
            .into_iter()
            .collect()
    }

    #[test]
    fn a_deep_chain_is_bounded_to_the_direct_dependencies() {
        let g = graph(&[
            ("m5", &["m4"], false),
            ("m4", &["m3"], false),
            ("m3", &["m2"], false),
            ("m2", &["m1"], false),
            ("m1", &["src"], false),
            ("src", &[], false),
        ]);
        assert_eq!(scope_of(&g, "m5"), vec!["m4", "m5"]);
    }

    #[test]
    fn an_ephemeral_chain_exposes_its_dependencies_but_not_itself() {
        let g = graph(&[
            ("final", &["eph_outer"], false),
            ("eph_outer", &["eph_inner", "plain"], true),
            ("eph_inner", &["base"], true),
            ("plain", &["deep"], false),
            ("base", &["deeper"], false),
            ("deep", &[], false),
            ("deeper", &[], false),
        ]);
        assert_eq!(scope_of(&g, "final"), vec!["base", "final", "plain"]);
    }

    #[test]
    fn a_diamond_of_ephemerals_is_walked_once() {
        let g = graph(&[
            ("final", &["left", "right"], false),
            ("left", &["shared"], true),
            ("right", &["shared"], true),
            ("shared", &["base"], true),
            ("base", &[], false),
        ]);
        assert_eq!(scope_of(&g, "final"), vec!["base", "final"]);
    }

    #[test]
    fn a_unit_test_walks_the_model_it_runs() {
        let mut g = graph(&[
            ("unit", &["tested", "given"], false),
            ("tested", &["given", "unmocked"], false),
            ("given", &["upstream"], false),
            ("unmocked", &[], false),
            ("upstream", &[], false),
        ]);
        if let Some(unit) = g.get_mut("unit") {
            unit.tested_model = Some("tested".to_owned());
        }
        assert_eq!(scope_of(&g, "unit"), vec!["given", "tested", "unit", "unmocked"]);
    }

    #[test]
    fn an_unknown_node_is_only_itself() {
        let g = graph(&[]);
        assert_eq!(scope_of(&g, "ghost"), vec!["ghost"]);
    }
}
