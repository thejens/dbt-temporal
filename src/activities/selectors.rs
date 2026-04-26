use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
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
            let expr = parse_model_specifiers(&tokens).context("invalid --select")?;
            let matched = resolve_expression(nodes, &full_deps, &reverse_deps, &expr);
            selected_ids.retain(|uid| matched.contains(uid.as_str()));
        }
    }

    if let Some(excl) = exclude {
        let tokens: Vec<String> = excl.split_whitespace().map(String::from).collect();
        if !tokens.is_empty() {
            let expr = parse_model_specifiers(&tokens).context("invalid --exclude")?;
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
        MethodName::ResourceType => {
            // `as_str_name()` returns the proto enum constant ("NODE_TYPE_TEST");
            // dbt-style selectors pass the bare name ("test"). Normalize both ends.
            let raw = node.resource_type().as_str_name();
            let bare = raw.strip_prefix("NODE_TYPE_").unwrap_or(raw);
            bare.eq_ignore_ascii_case(&criteria.value)
        }
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;

    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::schemas::nodes::{
        CommonAttributes, DbtModel, DbtSeed, DbtTest, NodeBaseAttributes,
    };

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

    // --- apply_selectors ---

    /// Build a model node with the selector-relevant fields populated.
    fn make_model(
        unique_id: &str,
        name: &str,
        package: &str,
        path: &str,
        tags: &[&str],
        fqn_extra: &[&str],
    ) -> Arc<DbtModel> {
        let common = CommonAttributes {
            unique_id: unique_id.to_string(),
            name: name.to_string(),
            package_name: package.to_string(),
            original_file_path: PathBuf::from(path),
            tags: tags.iter().map(|s| (*s).to_string()).collect(),
            fqn: std::iter::once(package.to_string())
                .chain(fqn_extra.iter().map(|s| (*s).to_string()))
                .chain(std::iter::once(name.to_string()))
                .collect(),
            ..CommonAttributes::default()
        };

        Arc::new(DbtModel {
            __common_attr__: common,
            __base_attr__: NodeBaseAttributes::default(),
            ..DbtModel::default()
        })
    }

    fn build_three_model_project() -> (Vec<String>, Nodes) {
        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.stg_customers".to_string(),
            make_model(
                "model.shop.stg_customers",
                "stg_customers",
                "shop",
                "models/staging/stg_customers.sql",
                &["nightly"],
                &["staging"],
            ),
        );
        nodes.models.insert(
            "model.shop.stg_orders".to_string(),
            make_model(
                "model.shop.stg_orders",
                "stg_orders",
                "shop",
                "models/staging/stg_orders.sql",
                &["nightly", "hourly"],
                &["staging"],
            ),
        );
        nodes.models.insert(
            "model.shop.customers".to_string(),
            make_model(
                "model.shop.customers",
                "customers",
                "shop",
                "models/customers.sql",
                &["hourly"],
                &[],
            ),
        );

        let ids = vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string(),
            "model.shop.customers".to_string(),
        ];
        (ids, nodes)
    }

    #[test]
    fn apply_selectors_no_filters_returns_input() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids.clone(), &nodes, None, None).unwrap();
        assert_eq!(out, ids);
    }

    #[test]
    fn apply_selectors_filters_by_tag() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("tag:nightly"), None).unwrap();
        assert_eq!(
            out,
            vec![
                "model.shop.stg_customers".to_string(),
                "model.shop.stg_orders".to_string(),
            ]
        );
    }

    #[test]
    fn apply_selectors_filters_by_fqn_name_exact() {
        // The fqn matcher accepts an exact name OR a substring of the dotted fqn,
        // so a search like "customers" matches anything whose fqn contains it
        // (including "stg_customers"). Use a name that's only one node's exact match.
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("stg_orders"), None).unwrap();
        assert_eq!(out, vec!["model.shop.stg_orders".to_string()]);
    }

    #[test]
    fn apply_selectors_filters_by_path_prefix() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("path:models/staging"), None).unwrap();
        assert_eq!(
            out,
            vec![
                "model.shop.stg_customers".to_string(),
                "model.shop.stg_orders".to_string(),
            ]
        );
    }

    #[test]
    fn apply_selectors_filters_by_package() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("package:shop"), None).unwrap();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn apply_selectors_excludes_subset() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, None, Some("tag:nightly")).unwrap();
        assert_eq!(out, vec!["model.shop.customers".to_string()]);
    }

    #[test]
    fn apply_selectors_select_and_exclude_combined() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("tag:nightly"), Some("tag:hourly")).unwrap();
        assert_eq!(out, vec!["model.shop.stg_customers".to_string()]);
    }

    #[test]
    fn apply_selectors_intersection_via_comma() {
        let (ids, nodes) = build_three_model_project();
        // Comma is intersection: nodes that are nightly AND hourly.
        let out = apply_selectors(ids, &nodes, Some("tag:nightly,tag:hourly"), None).unwrap();
        assert_eq!(out, vec!["model.shop.stg_orders".to_string()]);
    }

    #[test]
    fn apply_selectors_union_via_space() {
        let (ids, nodes) = build_three_model_project();
        // Whitespace-separated tokens are union (each parsed independently).
        let out = apply_selectors(ids, &nodes, Some("tag:hourly tag:nightly"), None).unwrap();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn apply_selectors_unknown_config_field_returns_empty() {
        // Some malformed selectors are accepted by the parser but match nothing;
        // the contract for apply_selectors here is "either error or return empty".
        let (ids, nodes) = build_three_model_project();
        let result = apply_selectors(ids, &nodes, Some("config:not_a_field=foo"), None);
        if let Ok(out) = result {
            assert!(out.is_empty());
        }
    }

    /// Build a 4-node DAG: stg_customers, stg_orders -> orders -> ar_summary
    fn build_dag_project() -> (Vec<String>, Nodes) {
        use dbt_common::CodeLocationWithFile;
        use dbt_schemas::schemas::common::NodeDependsOn;

        let mut nodes = Nodes::default();

        let mk = |uid: &str, name: &str, depends_on_uids: &[&str]| {
            let common = CommonAttributes {
                unique_id: uid.to_string(),
                name: name.to_string(),
                package_name: "shop".to_string(),
                fqn: vec!["shop".to_string(), name.to_string()],
                ..CommonAttributes::default()
            };
            let depends_on = NodeDependsOn {
                nodes_with_ref_location: depends_on_uids
                    .iter()
                    .map(|d| ((*d).to_string(), CodeLocationWithFile::default()))
                    .collect(),
                ..NodeDependsOn::default()
            };
            Arc::new(DbtModel {
                __common_attr__: common,
                __base_attr__: NodeBaseAttributes {
                    depends_on,
                    ..NodeBaseAttributes::default()
                },
                ..DbtModel::default()
            })
        };

        nodes.models.insert(
            "model.shop.stg_customers".to_string(),
            mk("model.shop.stg_customers", "stg_customers", &[]),
        );
        nodes.models.insert(
            "model.shop.stg_orders".to_string(),
            mk("model.shop.stg_orders", "stg_orders", &[]),
        );
        nodes.models.insert(
            "model.shop.orders".to_string(),
            mk("model.shop.orders", "orders", &["model.shop.stg_orders"]),
        );
        nodes.models.insert(
            "model.shop.ar_summary".to_string(),
            mk(
                "model.shop.ar_summary",
                "ar_summary",
                &["model.shop.orders", "model.shop.stg_customers"],
            ),
        );

        let ids = vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string(),
            "model.shop.orders".to_string(),
            "model.shop.ar_summary".to_string(),
        ];
        (ids, nodes)
    }

    #[test]
    fn apply_selectors_parents_walks_upstream() {
        let (ids, nodes) = build_dag_project();
        // `+ar_summary` selects ar_summary plus all transitive parents.
        let out = apply_selectors(ids, &nodes, Some("+ar_summary"), None).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert!(set.contains("model.shop.ar_summary"));
        assert!(set.contains("model.shop.orders"));
        assert!(set.contains("model.shop.stg_orders"));
        assert!(set.contains("model.shop.stg_customers"));
    }

    #[test]
    fn apply_selectors_parents_depth_limited() {
        let (ids, nodes) = build_dag_project();
        // `1+ar_summary` selects ar_summary plus direct parents only.
        let out = apply_selectors(ids, &nodes, Some("1+ar_summary"), None).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert!(set.contains("model.shop.ar_summary"));
        assert!(set.contains("model.shop.orders"));
        assert!(set.contains("model.shop.stg_customers"));
        // stg_orders is two hops away → excluded.
        assert!(!set.contains("model.shop.stg_orders"));
    }

    #[test]
    fn apply_selectors_children_walks_downstream() {
        let (ids, nodes) = build_dag_project();
        // `stg_orders+` selects stg_orders plus everything downstream.
        let out = apply_selectors(ids, &nodes, Some("stg_orders+"), None).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert!(set.contains("model.shop.stg_orders"));
        assert!(set.contains("model.shop.orders"));
        assert!(set.contains("model.shop.ar_summary"));
        // stg_customers is unrelated → excluded.
        assert!(!set.contains("model.shop.stg_customers"));
    }

    #[test]
    fn apply_selectors_children_depth_limited() {
        let (ids, nodes) = build_dag_project();
        // `stg_orders+1` selects stg_orders + direct children only.
        let out = apply_selectors(ids, &nodes, Some("stg_orders+1"), None).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert!(set.contains("model.shop.stg_orders"));
        assert!(set.contains("model.shop.orders"));
        // ar_summary is two hops away → excluded.
        assert!(!set.contains("model.shop.ar_summary"));
    }

    #[test]
    fn apply_selectors_at_operator_includes_direct_parents_and_children() {
        let (ids, nodes) = build_dag_project();
        // `@orders` per the implementation: orders + direct parents + direct
        // children (one hop in each direction). Multi-hop transitive expansion
        // is intentionally not done here.
        let out = apply_selectors(ids, &nodes, Some("@orders"), None).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert!(set.contains("model.shop.orders"));
        assert!(set.contains("model.shop.stg_orders"), "direct parent");
        assert!(set.contains("model.shop.ar_summary"), "direct child");
        // stg_customers is two hops away (ar_summary's other parent) — not pulled in.
        assert!(!set.contains("model.shop.stg_customers"));
    }

    #[test]
    fn apply_selectors_resource_type_filter() {
        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.m".to_string(),
            make_model("model.shop.m", "m", "shop", "models/m.sql", &[], &[]),
        );
        nodes.tests.insert(
            "test.shop.t".to_string(),
            Arc::new({
                let mut t = DbtTest::default();
                t.__common_attr__.unique_id = "test.shop.t".to_string();
                t.__common_attr__.name = "t".to_string();
                t.__common_attr__.package_name = "shop".to_string();
                t.__common_attr__.fqn = vec!["shop".to_string(), "t".to_string()];
                t
            }),
        );
        nodes.seeds.insert(
            "seed.shop.s".to_string(),
            Arc::new({
                let mut s = DbtSeed::default();
                s.__common_attr__.unique_id = "seed.shop.s".to_string();
                s.__common_attr__.name = "s".to_string();
                s.__common_attr__.package_name = "shop".to_string();
                s.__common_attr__.fqn = vec!["shop".to_string(), "s".to_string()];
                s
            }),
        );

        let ids = vec![
            "model.shop.m".to_string(),
            "test.shop.t".to_string(),
            "seed.shop.s".to_string(),
        ];
        let out = apply_selectors(ids, &nodes, Some("resource_type:test"), None).unwrap();
        assert_eq!(out, vec!["test.shop.t".to_string()]);
    }
}
