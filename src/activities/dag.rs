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

    // For each non-test node that depends on a model with tests, add the test IDs
    // as additional dependencies.
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
        in_degree.entry(node.as_str()).or_insert(0);
        for dep in node_deps {
            in_degree.entry(dep.as_str()).or_insert(0); // ensure dep exists
            *in_degree.entry(node.as_str()).or_insert(0) += 1;
            reverse.entry(dep.as_str()).or_default().push(node.as_str());
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
    use super::*;

    fn deps(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        entries
            .iter()
            .map(|(node, node_deps)| {
                (node.to_string(), node_deps.iter().map(ToString::to_string).collect())
            })
            .collect()
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
}
