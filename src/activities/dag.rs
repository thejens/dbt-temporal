use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::telemetry::NodeType;

/// Build a dependency map from Nodes, filtered to selected node IDs.
///
/// When a node depends on an ephemeral model (not in the selected set), walk through
/// the ephemeral model's dependencies transitively to find non-ephemeral ancestors.
pub fn build_dependency_map(
    nodes: &Nodes,
    selected_ids: &[String],
) -> BTreeMap<String, BTreeSet<String>> {
    let selected_set: BTreeSet<&str> = selected_ids.iter().map(String::as_str).collect();
    let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for id in selected_ids {
        let mut node_deps = BTreeSet::new();
        if let Some(node) = nodes.get_node(id) {
            for (dep_id, _) in &node.base().depends_on.nodes_with_ref_location {
                if selected_set.contains(dep_id.as_str()) {
                    node_deps.insert(dep_id.clone());
                } else {
                    // Dependency is not selected (likely ephemeral) — promote its
                    // transitive dependencies so the DAG ordering is correct.
                    promote_ephemeral_deps(nodes, dep_id, &selected_set, &mut node_deps);
                }
            }
        }
        deps.insert(id.clone(), node_deps);
    }

    deps
}

/// Walk through an unselected (ephemeral) node's dependencies transitively,
/// collecting any that ARE in the selected set.
fn promote_ephemeral_deps(
    nodes: &Nodes,
    unselected_id: &str,
    selected_set: &BTreeSet<&str>,
    result: &mut BTreeSet<String>,
) {
    let mut stack = vec![unselected_id.to_string()];
    let mut visited = BTreeSet::new();
    while let Some(current) = stack.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(node) = nodes.get_node(&current) {
            for (dep_id, _) in &node.base().depends_on.nodes_with_ref_location {
                if selected_set.contains(dep_id.as_str()) {
                    result.insert(dep_id.clone());
                } else if !visited.contains(dep_id.as_str()) {
                    stack.push(dep_id.clone());
                }
            }
        }
    }
}

/// Inject test-gate dependencies.
///
/// For each model M with tests T1..Tn, any non-test node N that depends on M should
/// also depend on T1..Tn. This ensures tests run before downstream models and gate
/// the pipeline — if a test fails, downstreams are skipped.
///
/// Skip the gate edge `N → T` whenever T already runs after N — i.e. T transitively
/// depends on N. Adding the edge would close a cycle:
///
/// * Direct: a single test refs two siblings in a dep chain. Gating the lower sibling
///   on the test it already refs is a 2-cycle.
/// * Transitive: T refs M_x; M_x depends (via any chain) on a model M_y; another
///   model N also depends on M_y but is not referenced by T. Gating N on T closes a
///   cycle through the M_x→…→M_y chain. (The `logo_activity_model` case.)
///
/// Either way, T is already scheduled after N by the existing forward edge(s), so the
/// gate would be redundant in addition to unsafe.
pub fn inject_test_gates(
    nodes: &Nodes,
    selected_ids: &[String],
    deps: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let selected_set: BTreeSet<&str> = selected_ids.iter().map(String::as_str).collect();

    // Build model->tests map: for each model, collect all selected tests that depend on it.
    let mut model_tests: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in selected_ids {
        if let Some(node) = nodes.get_node(id)
            && node.resource_type() == NodeType::Test
        {
            for (dep_id, _) in &node.base().depends_on.nodes_with_ref_location {
                if selected_set.contains(dep_id.as_str())
                    && nodes
                        .get_node(dep_id)
                        .is_some_and(|n| n.resource_type() == NodeType::Model)
                {
                    model_tests
                        .entry(dep_id.as_str())
                        .or_default()
                        .push(id.as_str());
                }
            }
        }
    }

    // Compute each test's transitive ancestor set in the raw (pre-injection) dep
    // graph — every node T depends on, directly or via chains of model→model edges.
    let test_ids: Vec<&str> = selected_ids
        .iter()
        .filter(|id| {
            nodes
                .get_node(id)
                .is_some_and(|n| n.resource_type() == NodeType::Test)
        })
        .map(String::as_str)
        .collect();
    let mut test_ancestors: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for test_id in &test_ids {
        let mut ancestors: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = deps
            .get(*test_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        while let Some(cur) = stack.pop() {
            if !ancestors.insert(cur.clone()) {
                continue;
            }
            if let Some(parent_deps) = deps.get(&cur) {
                for p in parent_deps {
                    if !ancestors.contains(p) {
                        stack.push(p.clone());
                    }
                }
            }
        }
        test_ancestors.insert(*test_id, ancestors);
    }

    // For each non-test node that depends on a model with tests, add the test IDs
    // as additional dependencies — except where doing so would close a cycle.
    for id in selected_ids {
        let is_test = nodes
            .get_node(id)
            .is_some_and(|n| n.resource_type() == NodeType::Test);
        if is_test {
            continue;
        }
        if let Some(node_deps) = deps.get(id).cloned() {
            let mut extra = BTreeSet::new();
            for dep_id in &node_deps {
                if let Some(tests) = model_tests.get(dep_id.as_str()) {
                    for test_id in tests {
                        if test_ancestors
                            .get(test_id)
                            .is_some_and(|a| a.contains(id.as_str()))
                        {
                            continue;
                        }
                        extra.insert((*test_id).to_string());
                    }
                }
            }
            if !extra.is_empty()
                && let Some(entry) = deps.get_mut(id)
            {
                entry.extend(extra);
            }
        }
    }
}

/// Compute topological levels from a dependency map using Kahn's algorithm.
/// Returns levels where each level's nodes have no deps on nodes in the same or later levels.
pub fn topological_levels(
    deps: &BTreeMap<String, BTreeSet<String>>,
) -> Result<Vec<Vec<String>>, anyhow::Error> {
    let mut in_degree: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reverse: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for (node, node_deps) in deps {
        let node_str = node.as_str();
        // Ensures source nodes (no deps) land in the map with in-degree 0
        // so they end up in the initial queue.
        *in_degree.entry(node_str).or_insert(0) += node_deps.len();
        for dep in node_deps {
            in_degree.entry(dep.as_str()).or_insert(0);
            reverse.entry(dep.as_str()).or_default().push(node_str);
        }
    }

    let mut levels = Vec::new();
    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&node, _)| node)
        .collect();
    queue.sort_unstable(); // deterministic ordering

    while !queue.is_empty() {
        levels.push(queue.iter().map(ToString::to_string).collect());
        let mut next_queue = Vec::new();
        for &node in &queue {
            if let Some(dependents) = reverse.get(node) {
                for &dep in dependents {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            next_queue.push(dep);
                        }
                    }
                }
            }
        }
        next_queue.sort_unstable();
        queue = next_queue;
    }

    // Verify all nodes were processed — if not, the graph contains a cycle.
    let processed: usize = levels.iter().map(Vec::len).sum();
    if processed != in_degree.len() {
        let stuck: Vec<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg > 0)
            .map(|(&node, _)| node)
            .collect();
        anyhow::bail!(
            "cyclic dependency detected: {} node(s) not reachable: {}",
            stuck.len(),
            stuck.join(", ")
        );
    }

    Ok(levels)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use dbt_common::CodeLocationWithFile;
    use dbt_schemas::schemas::common::NodeDependsOn;
    use dbt_schemas::schemas::{CommonAttributes, DbtModel, DbtTest, NodeBaseAttributes};

    use super::*;

    fn deps(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        entries
            .iter()
            .map(|(node, node_deps)| {
                (node.to_string(), node_deps.iter().map(ToString::to_string).collect())
            })
            .collect()
    }

    fn make_common(unique_id: &str) -> CommonAttributes {
        CommonAttributes {
            unique_id: unique_id.to_string(),
            name: unique_id
                .rsplit('.')
                .next()
                .unwrap_or(unique_id)
                .to_string(),
            ..Default::default()
        }
    }

    fn make_base(dep_ids: &[&str]) -> NodeBaseAttributes {
        NodeBaseAttributes {
            depends_on: NodeDependsOn {
                nodes_with_ref_location: dep_ids
                    .iter()
                    .map(|d| ((*d).to_string(), CodeLocationWithFile::default()))
                    .collect(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn add_model(nodes: &mut Nodes, unique_id: &str, dep_ids: &[&str]) {
        let model = DbtModel {
            __common_attr__: make_common(unique_id),
            __base_attr__: make_base(dep_ids),
            ..Default::default()
        };
        nodes.models.insert(unique_id.to_string(), Arc::new(model));
    }

    fn add_test(nodes: &mut Nodes, unique_id: &str, dep_ids: &[&str]) {
        let test = DbtTest {
            __common_attr__: make_common(unique_id),
            __base_attr__: make_base(dep_ids),
            ..Default::default()
        };
        nodes.tests.insert(unique_id.to_string(), Arc::new(test));
    }

    #[test]
    fn test_topological_levels_single_node() {
        let d = deps(&[("a", &[])]);
        let levels = topological_levels(&d).unwrap();
        assert_eq!(levels, vec![vec!["a"]]);
    }

    #[test]
    fn test_topological_levels_linear_chain() {
        // a -> b -> c
        let d = deps(&[("c", &["b"]), ("b", &["a"]), ("a", &[])]);
        let levels = topological_levels(&d).unwrap();
        assert_eq!(levels, vec![vec!["a"], vec!["b"], vec!["c"]]);
    }

    #[test]
    fn test_topological_levels_diamond() {
        //   a
        //  / \
        // b   c
        //  \ /
        //   d
        let d = deps(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["b", "c"])]);
        let levels = topological_levels(&d).unwrap();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec!["a"]);
        assert_eq!(levels[1], vec!["b", "c"]); // sorted
        assert_eq!(levels[2], vec!["d"]);
    }

    #[test]
    fn test_topological_levels_independent_nodes() {
        let d = deps(&[("a", &[]), ("b", &[]), ("c", &[])]);
        let levels = topological_levels(&d).unwrap();
        assert_eq!(levels, vec![vec!["a", "b", "c"]]);
    }

    #[test]
    fn test_topological_levels_wide_fan_out() {
        // a -> b, c, d
        let d = deps(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["a"])]);
        let levels = topological_levels(&d).unwrap();
        assert_eq!(levels, vec![vec!["a"], vec!["b", "c", "d"]]);
    }

    #[test]
    fn test_topological_levels_empty() {
        let d = deps(&[]);
        let levels = topological_levels(&d).unwrap();
        assert!(levels.is_empty());
    }

    #[test]
    fn test_topological_levels_cycle_detected() {
        // a -> b -> a (cycle)
        let d = deps(&[("a", &["b"]), ("b", &["a"])]);
        let err = topological_levels(&d).unwrap_err();
        assert!(err.to_string().contains("cyclic dependency"), "{err}");
    }

    /// Regression: panda-cascade marketing_model phantom cycle.
    ///
    /// `test_country_base` refs four sibling models in a dep chain via a Jinja
    /// loop — `marketing_spend_{daily,weekly,monthly,quarterly}`. Naively
    /// gating each model on every test of its upstream models adds an edge
    /// `weekly → test_country_base`, but `test_country_base` already depends
    /// on `weekly`, so a cycle appears and Kahn's algorithm flags
    /// `{weekly, monthly, quarterly, test_country_base}` as unreachable.
    #[test]
    fn inject_test_gates_skips_back_edges_to_self_gating_test() {
        let mut nodes = Nodes::default();
        add_model(&mut nodes, "model.m.daily", &[]);
        add_model(&mut nodes, "model.m.weekly", &["model.m.daily"]);
        add_model(&mut nodes, "model.m.monthly", &["model.m.daily"]);
        add_model(&mut nodes, "model.m.quarterly", &["model.m.monthly"]);
        add_test(
            &mut nodes,
            "test.m.country_base",
            &[
                "model.m.daily",
                "model.m.weekly",
                "model.m.monthly",
                "model.m.quarterly",
            ],
        );

        let selected: Vec<String> = [
            "model.m.daily",
            "model.m.weekly",
            "model.m.monthly",
            "model.m.quarterly",
            "test.m.country_base",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();

        let mut deps_map = build_dependency_map(&nodes, &selected);
        inject_test_gates(&nodes, &selected, &mut deps_map);

        // None of the spend models should have been gated on a test that already depends on them.
        for sibling in ["model.m.weekly", "model.m.monthly", "model.m.quarterly"] {
            assert!(
                !deps_map[sibling].contains("test.m.country_base"),
                "{sibling} was gated on test.m.country_base, which would create a cycle"
            );
        }

        // And the graph must still planarise.
        let levels = topological_levels(&deps_map).unwrap();
        let placed: BTreeSet<&str> = levels.iter().flatten().map(String::as_str).collect();
        for id in &selected {
            assert!(placed.contains(id.as_str()), "{id} missing from levels");
        }
        // Test runs strictly after every model it depends on.
        let level_of = |id: &str| {
            levels
                .iter()
                .position(|l| l.iter().any(|n| n == id))
                .unwrap()
        };
        let test_level = level_of("test.m.country_base");
        for model in [
            "model.m.daily",
            "model.m.weekly",
            "model.m.monthly",
            "model.m.quarterly",
        ] {
            assert!(level_of(model) < test_level, "{model} should run before test.m.country_base");
        }
    }

    /// The standard gate must still fire when a downstream model has no
    /// reverse path to the test — i.e. the test only depends on the upstream
    /// model, not on the downstream consumer.
    #[test]
    fn inject_test_gates_adds_normal_forward_gate() {
        let mut nodes = Nodes::default();
        add_model(&mut nodes, "model.m.upstream", &[]);
        add_model(&mut nodes, "model.m.downstream", &["model.m.upstream"]);
        add_test(&mut nodes, "test.m.upstream_check", &["model.m.upstream"]);

        let selected: Vec<String> = [
            "model.m.upstream",
            "model.m.downstream",
            "test.m.upstream_check",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();

        let mut deps_map = build_dependency_map(&nodes, &selected);
        inject_test_gates(&nodes, &selected, &mut deps_map);

        assert!(
            deps_map["model.m.downstream"].contains("test.m.upstream_check"),
            "downstream model should be gated on its upstream model's test"
        );

        let levels = topological_levels(&deps_map).unwrap();
        let level_of = |id: &str| {
            levels
                .iter()
                .position(|l| l.iter().any(|n| n == id))
                .unwrap()
        };
        assert!(level_of("model.m.upstream") < level_of("test.m.upstream_check"));
        assert!(level_of("test.m.upstream_check") < level_of("model.m.downstream"));
    }

    /// A test that depends on a model and its descendant should not gate
    /// either of those models on itself, but should still gate any *other*
    /// downstream consumer of the same upstream.
    #[test]
    fn inject_test_gates_partial_back_edge_still_gates_other_consumers() {
        let mut nodes = Nodes::default();
        add_model(&mut nodes, "model.m.parent", &[]);
        add_model(&mut nodes, "model.m.child_a", &["model.m.parent"]);
        add_model(&mut nodes, "model.m.child_b", &["model.m.parent"]);
        // Test refs parent + child_a → must not gate child_a, but child_b is fair game.
        add_test(&mut nodes, "test.m.shared", &["model.m.parent", "model.m.child_a"]);

        let selected: Vec<String> = [
            "model.m.parent",
            "model.m.child_a",
            "model.m.child_b",
            "test.m.shared",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();

        let mut deps_map = build_dependency_map(&nodes, &selected);
        inject_test_gates(&nodes, &selected, &mut deps_map);

        assert!(
            !deps_map["model.m.child_a"].contains("test.m.shared"),
            "child_a is referenced by the test — gating it would create a cycle"
        );
        assert!(
            deps_map["model.m.child_b"].contains("test.m.shared"),
            "child_b shares parent's test gate but is not referenced by the test"
        );

        topological_levels(&deps_map).unwrap();
    }

    /// Phantom test nodes (zero `depends_on` — issue #2's symptom) must not
    /// disrupt gate injection. They should appear at level 0 with no extra
    /// edges added.
    #[test]
    fn inject_test_gates_tolerates_phantom_test_with_no_deps() {
        let mut nodes = Nodes::default();
        add_model(&mut nodes, "model.m.real", &[]);
        add_test(&mut nodes, "test.m.phantom", &[]);

        let selected: Vec<String> = ["model.m.real", "test.m.phantom"]
            .iter()
            .map(ToString::to_string)
            .collect();

        let mut deps_map = build_dependency_map(&nodes, &selected);
        inject_test_gates(&nodes, &selected, &mut deps_map);

        assert!(deps_map["model.m.real"].is_empty());
        assert!(deps_map["test.m.phantom"].is_empty());

        let levels = topological_levels(&deps_map).unwrap();
        assert_eq!(levels.len(), 1);
        assert!(levels[0].contains(&"model.m.real".to_string()));
        assert!(levels[0].contains(&"test.m.phantom".to_string()));
    }

    /// Regression: panda-cascade `logo_activity_model` transitive cycle.
    ///
    /// Reduced topology from the real project. The trigger is a test that refs
    /// a *descendant* of a model M, while another node N depends on M without
    /// being referenced by the test. Naive gate injection adds `N → T`, but T
    /// transitively depends on N via the descendant chain, closing a cycle.
    ///
    /// Concrete trace:
    ///   activity_events ← event_configuration
    ///   activity_events ← daily_activity_events
    ///   daily_activity_events, event_configuration ← logo_metadata
    ///   daily_activity_events, event_configuration ← _daily_active_users
    ///   _daily_active_users, logo_metadata ← logo_cohort_metrics
    ///   logo_cohort_metrics ← logo_metrics
    ///   test_dau refs daily_activity_events, event_configuration, logo_metrics
    ///
    /// Naively, `logo_metadata → test_dau` is injected (logo_metadata depends
    /// on event_configuration which has test_dau). But test_dau transitively
    /// depends on logo_metadata via logo_metrics → logo_cohort_metrics →
    /// logo_metadata. The transitive ancestor check must catch this.
    #[test]
    fn inject_test_gates_skips_transitive_back_edge() {
        let mut nodes = Nodes::default();
        add_model(&mut nodes, "model.l.activity_events", &[]);
        add_model(&mut nodes, "model.l.event_configuration", &["model.l.activity_events"]);
        add_model(&mut nodes, "model.l.daily_activity_events", &["model.l.activity_events"]);
        add_model(
            &mut nodes,
            "model.l.logo_metadata",
            &[
                "model.l.daily_activity_events",
                "model.l.event_configuration",
            ],
        );
        add_model(
            &mut nodes,
            "model.l._daily_active_users",
            &[
                "model.l.daily_activity_events",
                "model.l.event_configuration",
            ],
        );
        add_model(
            &mut nodes,
            "model.l.logo_cohort_metrics",
            &["model.l._daily_active_users", "model.l.logo_metadata"],
        );
        add_model(&mut nodes, "model.l.logo_metrics", &["model.l.logo_cohort_metrics"]);
        add_test(
            &mut nodes,
            "test.l.test_dau",
            &[
                "model.l.daily_activity_events",
                "model.l.event_configuration",
                "model.l.logo_metrics",
            ],
        );

        let selected: Vec<String> = [
            "model.l.activity_events",
            "model.l.event_configuration",
            "model.l.daily_activity_events",
            "model.l.logo_metadata",
            "model.l._daily_active_users",
            "model.l.logo_cohort_metrics",
            "model.l.logo_metrics",
            "test.l.test_dau",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();

        let mut deps_map = build_dependency_map(&nodes, &selected);
        inject_test_gates(&nodes, &selected, &mut deps_map);

        // Every model on the path test_dau → … → logo_metadata is a transitive
        // ancestor of test_dau. Gating any of them on test_dau would cycle.
        for upstream in [
            "model.l.activity_events",
            "model.l.event_configuration",
            "model.l.daily_activity_events",
            "model.l.logo_metadata",
            "model.l._daily_active_users",
            "model.l.logo_cohort_metrics",
        ] {
            assert!(
                !deps_map[upstream].contains("test.l.test_dau"),
                "{upstream} is a transitive ancestor of test_dau — gating it would cycle"
            );
        }

        // The graph must planarise.
        let levels = topological_levels(&deps_map).unwrap();
        let level_of = |id: &str| {
            levels
                .iter()
                .position(|l| l.iter().any(|n| n == id))
                .unwrap()
        };
        // test_dau still runs after every model it depends on, transitively.
        let test_level = level_of("test.l.test_dau");
        for ancestor in [
            "model.l.activity_events",
            "model.l.event_configuration",
            "model.l.daily_activity_events",
            "model.l.logo_metadata",
            "model.l._daily_active_users",
            "model.l.logo_cohort_metrics",
            "model.l.logo_metrics",
        ] {
            assert!(
                level_of(ancestor) < test_level,
                "{ancestor} should run before test.l.test_dau"
            );
        }
    }
}
