//! Dependency-graph walks behind the `+model` / `model+` / `@model` operators.

use std::collections::{BTreeMap, BTreeSet};

/// Reverse a dependency map: from "node -> deps" to "dep -> dependents".
pub fn reverse_dep_map(
    deps: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut reverse: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (node, node_deps) in deps {
        reverse.entry(node.clone()).or_default(); // ensure node exists
        for dep in node_deps {
            reverse.entry(dep.clone()).or_default().insert(node.clone());
        }
    }
    reverse
}

/// BFS walk of a graph (deps or reverse_deps) from a seed node up to max_depth levels.
pub fn walk_graph(
    seed: &str,
    graph: &BTreeMap<String, BTreeSet<String>>,
    max_depth: u32,
    result: &mut BTreeSet<String>,
) {
    let mut queue: Vec<&str> = vec![seed];
    let mut visited: BTreeSet<&str> = BTreeSet::new();
    visited.insert(seed);
    let mut depth = 0;

    while !queue.is_empty() && depth < max_depth {
        let mut next_queue = Vec::new();
        for node in &queue {
            if let Some(neighbors) = graph.get(*node) {
                for neighbor in neighbors {
                    if visited.insert(neighbor.as_str()) {
                        result.insert(neighbor.clone());
                        next_queue.push(neighbor.as_str());
                    }
                }
            }
        }
        queue = next_queue;
        depth += 1;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn deps(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        entries
            .iter()
            .map(|(node, node_deps)| {
                ((*node).to_string(), node_deps.iter().map(ToString::to_string).collect())
            })
            .collect()
    }

    #[test]
    fn walk_graph_upstream() {
        // a -> b -> c -> d
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        let mut result = BTreeSet::new();
        walk_graph("d", &dep_map, u32::MAX, &mut result);
        assert_eq!(result, BTreeSet::from(["c".to_string(), "b".to_string(), "a".to_string()]));
    }

    #[test]
    fn walk_graph_depth_limited() {
        // a -> b -> c -> d
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        let mut result = BTreeSet::new();
        walk_graph("d", &dep_map, 1, &mut result);
        assert_eq!(result, BTreeSet::from(["c".to_string()]));
    }

    #[test]
    fn walk_graph_downstream() {
        // a -> b -> c
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let reverse = reverse_dep_map(&dep_map);
        let mut result = BTreeSet::new();
        walk_graph("a", &reverse, u32::MAX, &mut result);
        assert_eq!(result, BTreeSet::from(["b".to_string(), "c".to_string()]));
    }

    #[test]
    fn reverse_dep_map_inverts_edges_and_keeps_leaves() -> anyhow::Result<()> {
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["a"])]);
        let reverse = reverse_dep_map(&dep_map);
        assert_eq!(
            reverse
                .get("a")
                .ok_or_else(|| anyhow::anyhow!("missing key 'a'"))?,
            &BTreeSet::from(["b".to_string(), "c".to_string()])
        );
        assert!(
            reverse
                .get("b")
                .ok_or_else(|| anyhow::anyhow!("missing key 'b'"))?
                .is_empty()
        );
        assert!(
            reverse
                .get("c")
                .ok_or_else(|| anyhow::anyhow!("missing key 'c'"))?
                .is_empty()
        );
        Ok(())
    }
}
