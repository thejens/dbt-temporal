use std::collections::{BTreeMap, BTreeSet};

use dbt_common::node_selector::{
    MethodName, SelectExpression, SelectionCriteria, parse_model_specifiers,
};
use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::nodes::InternalDbtNodeAttributes;

use super::dag::build_dependency_map;

/// Apply --select and --exclude filters to a list of node IDs.
pub fn apply_selectors(
    mut selected_ids: Vec<String>,
    nodes: &Nodes,
    select: Option<&str>,
    exclude: Option<&str>,
) -> Result<Vec<String>, anyhow::Error> {
    if select.is_none() && exclude.is_none() {
        return Ok(selected_ids);
    }

    // Build full dependency maps for graph operator traversal.
    let all_ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    let full_deps = build_dependency_map(nodes, &all_ids);
    let reverse_deps = reverse_dep_map(&full_deps);

    if let Some(sel) = select {
        let tokens: Vec<String> = sel.split_whitespace().map(String::from).collect();
        if !tokens.is_empty() {
            let expr = parse_model_specifiers(&tokens)
                .map_err(|e| anyhow::anyhow!("invalid --select: {e}"))?;
            let matched = resolve_expression(nodes, &full_deps, &reverse_deps, &expr);
            selected_ids.retain(|uid| matched.contains(uid.as_str()));
        }
    }

    if let Some(excl) = exclude {
        let tokens: Vec<String> = excl.split_whitespace().map(String::from).collect();
        if !tokens.is_empty() {
            let expr = parse_model_specifiers(&tokens)
                .map_err(|e| anyhow::anyhow!("invalid --exclude: {e}"))?;
            let matched = resolve_expression(nodes, &full_deps, &reverse_deps, &expr);
            selected_ids.retain(|uid| !matched.contains(uid.as_str()));
        }
    }

    Ok(selected_ids)
}

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

/// Resolve a selector expression to a set of matching node IDs, expanding graph operators.
fn resolve_expression(
    nodes: &Nodes,
    deps: &BTreeMap<String, BTreeSet<String>>,
    reverse_deps: &BTreeMap<String, BTreeSet<String>>,
    expr: &SelectExpression,
) -> BTreeSet<String> {
    match expr {
        SelectExpression::Atom(criteria) => resolve_criteria(nodes, deps, reverse_deps, criteria),
        SelectExpression::And(exprs) => {
            let mut result: Option<BTreeSet<String>> = None;
            for e in exprs {
                let matched = resolve_expression(nodes, deps, reverse_deps, e);
                result = Some(match result {
                    Some(acc) => acc.intersection(&matched).cloned().collect(),
                    None => matched,
                });
            }
            result.unwrap_or_default()
        }
        SelectExpression::Or(exprs) => {
            let mut result = BTreeSet::new();
            for e in exprs {
                result.extend(resolve_expression(nodes, deps, reverse_deps, e));
            }
            result
        }
        SelectExpression::Exclude(inner) => {
            // For exclude, we collect what matches so the caller can subtract.
            resolve_expression(nodes, deps, reverse_deps, inner)
        }
    }
}

/// Resolve a single selection criterion, including graph operator expansion.
fn resolve_criteria(
    nodes: &Nodes,
    deps: &BTreeMap<String, BTreeSet<String>>,
    reverse_deps: &BTreeMap<String, BTreeSet<String>>,
    criteria: &SelectionCriteria,
) -> BTreeSet<String> {
    // First find nodes matching the base criterion.
    let mut matched: BTreeSet<String> = nodes
        .iter()
        .filter(|(_, node)| matches_base_criteria(*node, criteria))
        .map(|(id, _)| id.clone())
        .collect();

    // Apply nested exclude.
    if let Some(excl) = &criteria.exclude {
        let excluded = resolve_expression(nodes, deps, reverse_deps, excl);
        matched.retain(|id| !excluded.contains(id));
    }

    // Expand with graph operators.
    let has_graph_ops = criteria.parents_depth.is_some()
        || criteria.children_depth.is_some()
        || criteria.childrens_parents;

    if has_graph_ops {
        let seeds: BTreeSet<String> = matched.clone();
        // +model: walk upstream (parents) through deps
        if let Some(depth) = criteria.parents_depth {
            for seed in &seeds {
                walk_graph(seed, deps, depth, &mut matched);
            }
        }
        // model+: walk downstream (children) through reverse_deps
        if let Some(depth) = criteria.children_depth {
            for seed in &seeds {
                walk_graph(seed, reverse_deps, depth, &mut matched);
            }
        }
        // @model: direct parents + children (depth 1 each direction)
        if criteria.childrens_parents {
            for seed in &seeds {
                walk_graph(seed, deps, 1, &mut matched);
                walk_graph(seed, reverse_deps, 1, &mut matched);
            }
        }
    }

    matched
}

/// BFS walk of a graph (deps or reverse_deps) from a seed node up to max_depth levels.
fn walk_graph(
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

/// Check if a node matches the base criterion (without graph operators).
fn matches_base_criteria(
    node: &dyn InternalDbtNodeAttributes,
    criteria: &SelectionCriteria,
) -> bool {
    match criteria.method {
        MethodName::Tag => node.common().tags.contains(&criteria.value),
        MethodName::Fqn => {
            let name = &node.common().name;
            let fqn = node.common().fqn.join(".");
            criteria.value == *name || fqn.contains(&criteria.value)
        }
        MethodName::Path => node
            .common()
            .original_file_path
            .to_string_lossy()
            .contains(&criteria.value),
        MethodName::ResourceType => node
            .resource_type()
            .as_str_name()
            .eq_ignore_ascii_case(&criteria.value),
        MethodName::Package => node.common().package_name == criteria.value,
        MethodName::Config => {
            if criteria.method_args.first().map(String::as_str) == Some("materialized") {
                node.base()
                    .materialized
                    .to_string()
                    .eq_ignore_ascii_case(&criteria.value)
            } else {
                false
            }
        }
        _ => false,
    }
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
    fn test_walk_graph_upstream() {
        // a -> b -> c -> d
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        // Walk upstream from d with unlimited depth
        let mut result = BTreeSet::new();
        walk_graph("d", &dep_map, u32::MAX, &mut result);
        assert_eq!(result, BTreeSet::from(["c".to_string(), "b".to_string(), "a".to_string()]));
    }

    #[test]
    fn test_walk_graph_depth_limited() {
        // a -> b -> c -> d
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        // Walk upstream from d with depth 1 — should only get c
        let mut result = BTreeSet::new();
        walk_graph("d", &dep_map, 1, &mut result);
        assert_eq!(result, BTreeSet::from(["c".to_string()]));
    }

    #[test]
    fn test_walk_graph_downstream() {
        // a -> b -> c
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let reverse = reverse_dep_map(&dep_map);
        // Walk downstream from a
        let mut result = BTreeSet::new();
        walk_graph("a", &reverse, u32::MAX, &mut result);
        assert_eq!(result, BTreeSet::from(["b".to_string(), "c".to_string()]));
    }

    #[test]
    fn test_reverse_dep_map() -> anyhow::Result<()> {
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
