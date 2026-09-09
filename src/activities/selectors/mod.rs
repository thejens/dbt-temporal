//! `--select` / `--exclude` evaluation.
//!
//! dbt-common parses the whole dbt selector grammar; this planner evaluates
//! every method decidable from the parsed manifest plus the state comparison in
//! [`state`]. The four it cannot decide need data no worker holds when it plans
//! a run, and each is rejected by name rather than left to match nothing.
//!
//! The split is by what a method asks about: [`names`] for the FQN and
//! `<package>.<name>` families, [`paths`] for file location, [`kinds`] for node
//! kind and version, [`config`] for the rendered config, [`state`] for the
//! previous-manifest comparison, [`glob`] for the pattern rules they all share,
//! and [`graph`] for the `+`/`@` walks. [`criteria`] turns a parsed criterion
//! into a decision about a node; this file drives the expression tree and the
//! graph expansion around it.

mod config;
mod criteria;
mod glob;
mod graph;
mod kinds;
mod names;
mod paths;
mod state;
#[cfg(test)]
mod test_support;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use dbt_common::node_selector::{SelectExpression, SelectionCriteria, parse_model_specifiers};
use dbt_schemas::schemas::Nodes;

use super::dag::build_dependency_map;
use criteria::{Criterion, MatchContext, criterion_support_error};

pub use graph::reverse_dep_map;
pub use state::StateSelector;

/// Apply --select and --exclude filters to a list of node IDs.
///
/// `state` backs the `state:` methods; selectors using them match nothing when
/// it is `None` (the planner validates this upfront).
pub fn apply_selectors(
    mut selected_ids: Vec<String>,
    nodes: &Nodes,
    select: Option<&str>,
    exclude: Option<&str>,
    state: Option<&StateSelector>,
    expand_indirect: &dyn Fn(Vec<String>) -> Vec<String>,
) -> Result<Vec<String>, anyhow::Error> {
    let select_expr = parse_selector(select).context("invalid --select")?;
    let exclude_expr = parse_selector(exclude).context("invalid --exclude")?;
    if select_expr.is_none() && exclude_expr.is_none() {
        return Ok(selected_ids);
    }

    // Reject methods this planner cannot evaluate before selection runs. An
    // unevaluable criterion matches nothing, which is invisible in the two
    // positions that matter: inside an `or` it silently under-selects, and in
    // `--exclude` it silently excludes nothing.
    if let Some(expr) = select_expr.as_ref() {
        validate_selector_support(expr).context("invalid --select")?;
    }
    if let Some(expr) = exclude_expr.as_ref() {
        validate_selector_support(expr).context("invalid --exclude")?;
    }

    let ctx = MatchContext {
        state,
        project_name: nodes.project_name.as_deref(),
    };

    // Graph operators (`+model`, `model+`, `@model`) walk the dependency maps;
    // plain selectors never touch them. Building the maps clones every node id
    // in the project twice, so skip it when nothing walks the graph.
    let needs_graph = select_expr.as_ref().is_some_and(uses_graph_operators)
        || exclude_expr.as_ref().is_some_and(uses_graph_operators);
    let (full_deps, reverse_deps) = if needs_graph {
        let all_ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        let full_deps = build_dependency_map(nodes, &all_ids);
        let reverse_deps = reverse_dep_map(&full_deps);
        (full_deps, reverse_deps)
    } else {
        (BTreeMap::new(), BTreeMap::new())
    };

    if let Some(expr) = select_expr {
        let matched = resolve_expression(nodes, &full_deps, &reverse_deps, ctx, &expr)?;
        selected_ids.retain(|uid| matched.contains(uid.as_str()));
    }

    // Indirect selection runs between the two, matching the order upstream's
    // scheduler applies inside one selection atom: base filter, graph
    // operators, indirect selection, then exclude.
    //
    // The order is what makes an explicit exclusion stick. Expanding after the
    // exclusion instead lets a test that was named in `--exclude` come back in
    // through the model it hangs off — undoing the one thing the user asked
    // for by name.
    selected_ids = expand_indirect(selected_ids);

    if let Some(expr) = exclude_expr {
        let matched = resolve_expression(nodes, &full_deps, &reverse_deps, ctx, &expr)?;
        // The exclusion expands too. Upstream evaluates an exclude expression
        // through the same per-atom pipeline as a select expression, indirect
        // selection included, so `--exclude my_model` drops the model *and* the
        // tests hanging off it. Subtracting only the literal matches left those
        // tests in the run, querying a model the same command had declined to
        // build.
        let excluded = expand_indirect(matched.into_iter().collect());
        let excluded: BTreeSet<&str> = excluded.iter().map(String::as_str).collect();
        selected_ids.retain(|uid| !excluded.contains(uid.as_str()));
    }

    Ok(selected_ids)
}

/// Parse a `--select`/`--exclude` string into an expression. `None` when the
/// input is absent or all-whitespace.
fn parse_selector(spec: Option<&str>) -> Result<Option<SelectExpression>, anyhow::Error> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let tokens: Vec<String> = spec.split_whitespace().map(String::from).collect();
    if tokens.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_model_specifiers(&tokens)?))
}

/// Reject every criterion in `expr` that [`Criterion::parse`] cannot read.
///
/// Walking the whole tree up front means one error names every problem in the
/// selector rather than the user fixing them one round-trip at a time.
fn validate_selector_support(expr: &SelectExpression) -> Result<(), anyhow::Error> {
    let mut errors = Vec::new();
    collect_unsupported(expr, &mut errors);
    if errors.is_empty() {
        return Ok(());
    }
    errors.dedup();
    Err(unsupported_methods_error(&errors))
}

/// The single phrasing for criteria this planner will not evaluate.
fn unsupported_methods_error(errors: &[String]) -> anyhow::Error {
    anyhow::anyhow!(
        "unsupported selector method(s): {}. Supported: access, config, exposure, file, \
         fqn, function, group, metric, package, path, resource_type, saved_query, \
         semantic_model, source, state, tag, test_name, test_type, unit_test, version",
        errors.join("; ")
    )
}

/// Walk an expression tree, appending a message for each unsupported criterion.
fn collect_unsupported(expr: &SelectExpression, errors: &mut Vec<String>) {
    match expr {
        SelectExpression::Atom(criteria) => {
            if let Some(reason) = criterion_support_error(criteria) {
                errors.push(reason);
            }
            if let Some(inner) = criteria.exclude.as_deref() {
                collect_unsupported(inner, errors);
            }
        }
        SelectExpression::And(exprs) | SelectExpression::Or(exprs) => {
            for e in exprs {
                collect_unsupported(e, errors);
            }
        }
        SelectExpression::Exclude(inner) => collect_unsupported(inner, errors),
    }
}

/// True if any criterion in the expression walks the dependency graph
/// (`+model`, `model+`, `@model`), including inside nested excludes.
fn uses_graph_operators(expr: &SelectExpression) -> bool {
    match expr {
        SelectExpression::Atom(c) => {
            c.childrens_parents
                || c.parents_depth.is_some()
                || c.children_depth.is_some()
                || c.exclude.as_deref().is_some_and(uses_graph_operators)
        }
        SelectExpression::And(exprs) | SelectExpression::Or(exprs) => {
            exprs.iter().any(uses_graph_operators)
        }
        SelectExpression::Exclude(inner) => uses_graph_operators(inner),
    }
}

/// Resolve a selector expression to a set of matching node IDs, expanding graph operators.
fn resolve_expression(
    nodes: &Nodes,
    deps: &BTreeMap<String, BTreeSet<String>>,
    reverse_deps: &BTreeMap<String, BTreeSet<String>>,
    ctx: MatchContext<'_>,
    expr: &SelectExpression,
) -> Result<BTreeSet<String>, anyhow::Error> {
    match expr {
        SelectExpression::Atom(criteria) => {
            resolve_criteria(nodes, deps, reverse_deps, ctx, criteria)
        }
        SelectExpression::And(exprs) => {
            let mut result: Option<BTreeSet<String>> = None;
            for e in exprs {
                let matched = resolve_expression(nodes, deps, reverse_deps, ctx, e)?;
                result = Some(match result {
                    Some(acc) => acc.intersection(&matched).cloned().collect(),
                    None => matched,
                });
            }
            Ok(result.unwrap_or_default())
        }
        SelectExpression::Or(exprs) => {
            let mut result = BTreeSet::new();
            for e in exprs {
                result.extend(resolve_expression(nodes, deps, reverse_deps, ctx, e)?);
            }
            Ok(result)
        }
        // For exclude, we collect what matches so the caller can subtract.
        SelectExpression::Exclude(inner) => {
            resolve_expression(nodes, deps, reverse_deps, ctx, inner)
        }
    }
}

/// Resolve a single selection criterion, including graph operator expansion.
fn resolve_criteria(
    nodes: &Nodes,
    deps: &BTreeMap<String, BTreeSet<String>>,
    reverse_deps: &BTreeMap<String, BTreeSet<String>>,
    ctx: MatchContext<'_>,
    criteria: &SelectionCriteria,
) -> Result<BTreeSet<String>, anyhow::Error> {
    // `validate_selector_support` walked this tree through the same parse, so a
    // criterion reaching here always reads. Propagating rather than falling
    // back to the empty set is what keeps that an assumption the code states
    // instead of one it silently relies on.
    let criterion = Criterion::parse(criteria)
        .map_err(|reason| unsupported_methods_error(std::slice::from_ref(&reason)))?;

    let mut matched: BTreeSet<String> = nodes
        .iter()
        .filter(|(id, node)| criterion.matches(id, *node, ctx))
        .map(|(id, _)| id.clone())
        .collect();

    // Apply nested exclude.
    if let Some(excl) = &criteria.exclude {
        let excluded = resolve_expression(nodes, deps, reverse_deps, ctx, excl)?;
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
                graph::walk_graph(seed, deps, depth, &mut matched);
            }
        }
        // model+: walk downstream (children) through reverse_deps
        if let Some(depth) = criteria.children_depth {
            for seed in &seeds {
                graph::walk_graph(seed, reverse_deps, depth, &mut matched);
            }
        }
        // @model: direct parents + children (depth 1 each direction)
        if criteria.childrens_parents {
            for seed in &seeds {
                graph::walk_graph(seed, deps, 1, &mut matched);
                graph::walk_graph(seed, reverse_deps, 1, &mut matched);
            }
        }
    }

    Ok(matched)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Selection tests drive `--select`/`--exclude` alone; the expansion hook
    /// is exercised by the indirect-selection tests and by `plan_project`.
    fn no_expansion(ids: Vec<String>) -> Vec<String> {
        ids
    }

    use std::sync::Arc;

    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::schemas::nodes::DbtModel;

    use test_support::{
        exposure_node, generic_test_node, metric_node, model_node, model_with_code,
        model_with_config, singular_test_node, source_node, versioned_model_node,
    };

    /// A model carrying the fields the three-model project varies between nodes.
    fn make_model(
        unique_id: &str,
        name: &str,
        package: &str,
        path: &str,
        tags: &[&str],
        fqn_extra: &[&str],
    ) -> Arc<DbtModel> {
        let mut model = (*model_node(unique_id, name)).clone();
        model.__common_attr__.package_name = package.to_string();
        model.__common_attr__.original_file_path = std::path::PathBuf::from(path).into();
        model.__common_attr__.tags = tags.iter().map(|tag| (*tag).to_string()).collect();
        model.__common_attr__.fqn = std::iter::once(package.to_string())
            .chain(fqn_extra.iter().map(|part| (*part).to_string()))
            .chain(std::iter::once(name.to_string()))
            .collect();
        Arc::new(model)
    }

    fn build_three_model_project() -> (Vec<String>, Nodes) {
        let mut nodes = Nodes {
            project_name: Some("shop".to_string()),
            ..Nodes::default()
        };
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
        let out = apply_selectors(ids.clone(), &nodes, None, None, None, &no_expansion).unwrap();
        assert_eq!(out, ids);
    }

    #[test]
    fn apply_selectors_filters_by_tag() {
        let (ids, nodes) = build_three_model_project();
        let out =
            apply_selectors(ids, &nodes, Some("tag:nightly"), None, None, &no_expansion).unwrap();
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
        let (ids, nodes) = build_three_model_project();
        let out =
            apply_selectors(ids, &nodes, Some("stg_orders"), None, None, &no_expansion).unwrap();
        assert_eq!(out, vec!["model.shop.stg_orders".to_string()]);
    }

    /// Regression: the matcher used to substring-match the dotted FQN, so
    /// `customers` also pulled in `stg_customers`.
    #[test]
    fn apply_selectors_fqn_name_does_not_substring_match() {
        let (ids, nodes) = build_three_model_project();
        let out =
            apply_selectors(ids, &nodes, Some("customers"), None, None, &no_expansion).unwrap();
        assert_eq!(out, vec!["model.shop.customers".to_string()]);
    }

    #[test]
    fn apply_selectors_fqn_matches_dotted_path_with_and_without_package() {
        let (ids, nodes) = build_three_model_project();
        let expected = vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string(),
        ];

        let with_package =
            apply_selectors(ids.clone(), &nodes, Some("shop.staging"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(with_package, expected);

        let without_package =
            apply_selectors(ids, &nodes, Some("staging"), None, None, &no_expansion).unwrap();
        assert_eq!(without_package, expected);
    }

    #[test]
    fn apply_selectors_fqn_supports_wildcards() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("stg_*"), None, None, &no_expansion).unwrap();
        assert_eq!(
            out,
            vec![
                "model.shop.stg_customers".to_string(),
                "model.shop.stg_orders".to_string()
            ]
        );
    }

    #[test]
    fn apply_selectors_filters_by_path_prefix() {
        let (ids, nodes) = build_three_model_project();
        let out =
            apply_selectors(ids, &nodes, Some("path:models/staging"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(
            out,
            vec![
                "model.shop.stg_customers".to_string(),
                "model.shop.stg_orders".to_string(),
            ]
        );
    }

    /// A value carrying a path separator parses as `path:` with no prefix.
    #[test]
    fn apply_selectors_bare_path_value_selects_by_path() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(ids, &nodes, Some("models/staging"), None, None, &no_expansion)
            .unwrap();
        assert_eq!(
            out,
            vec![
                "model.shop.stg_customers".to_string(),
                "model.shop.stg_orders".to_string(),
            ]
        );
    }

    /// And one ending in `.sql` parses as `file:`, which now selects a node
    /// rather than being turned away as unevaluable.
    #[test]
    fn apply_selectors_file_method_and_its_bare_sql_spelling() {
        let (ids, nodes) = build_three_model_project();
        let bare =
            apply_selectors(ids.clone(), &nodes, Some("customers.sql"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(bare, vec!["model.shop.customers".to_string()]);

        let prefixed = apply_selectors(
            ids.clone(),
            &nodes,
            Some("file:stg_orders.sql"),
            None,
            None,
            &no_expansion,
        )
        .unwrap();
        assert_eq!(prefixed, vec!["model.shop.stg_orders".to_string()]);

        let none = apply_selectors(ids, &nodes, Some("file:absent.sql"), None, None, &no_expansion)
            .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn apply_selectors_filters_by_package() {
        let (ids, nodes) = build_three_model_project();
        let out =
            apply_selectors(ids, &nodes, Some("package:shop"), None, None, &no_expansion).unwrap();
        assert_eq!(out.len(), 3);
    }

    /// `package:this` names the root project, which the parsed node set carries.
    #[test]
    fn apply_selectors_package_this_resolves_to_the_root_project() {
        let (ids, mut nodes) = build_three_model_project();
        let all =
            apply_selectors(ids.clone(), &nodes, Some("package:this"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(all.len(), 3);

        nodes.project_name = Some("marketing".to_string());
        let none =
            apply_selectors(ids, &nodes, Some("package:this"), None, None, &no_expansion).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn apply_selectors_excludes_subset() {
        let (ids, nodes) = build_three_model_project();
        let out =
            apply_selectors(ids, &nodes, None, Some("tag:nightly"), None, &no_expansion).unwrap();
        assert_eq!(out, vec!["model.shop.customers".to_string()]);
    }

    #[test]
    fn apply_selectors_select_and_exclude_combined() {
        let (ids, nodes) = build_three_model_project();
        let out = apply_selectors(
            ids,
            &nodes,
            Some("tag:nightly"),
            Some("tag:hourly"),
            None,
            &no_expansion,
        )
        .unwrap();
        assert_eq!(out, vec!["model.shop.stg_customers".to_string()]);
    }

    #[test]
    fn apply_selectors_intersection_via_comma() {
        let (ids, nodes) = build_three_model_project();
        // Comma is intersection: nodes that are nightly AND hourly.
        let out =
            apply_selectors(ids, &nodes, Some("tag:nightly,tag:hourly"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(out, vec!["model.shop.stg_orders".to_string()]);
    }

    #[test]
    fn apply_selectors_union_via_space() {
        let (ids, nodes) = build_three_model_project();
        // Whitespace-separated tokens are union (each parsed independently).
        let out =
            apply_selectors(ids, &nodes, Some("tag:hourly tag:nightly"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(out.len(), 3);
    }

    // --- config: any key, not just materialized ---

    /// A project whose three models differ only in the config keys under test.
    fn build_config_project() -> (Vec<String>, Nodes) {
        let mut nodes = Nodes {
            project_name: Some("shop".to_string()),
            ..Nodes::default()
        };
        nodes.models.insert(
            "model.shop.a".to_string(),
            model_with_config(
                "model.shop.a",
                "a",
                "materialized: view\nschema: audit\nmeta:\n  owner: finance\n",
            ),
        );
        nodes.models.insert(
            "model.shop.b".to_string(),
            model_with_config(
                "model.shop.b",
                "b",
                "materialized: incremental\nschema: staging\nunique_key:\n  - order_id\n",
            ),
        );
        let ids = nodes.iter().map(|(id, _)| id.clone()).collect();
        (ids, nodes)
    }

    #[test]
    fn apply_selectors_filters_by_config_materialized() {
        let (ids, nodes) = build_config_project();
        let out = apply_selectors(
            ids.clone(),
            &nodes,
            Some("config.materialized:view"),
            None,
            None,
            &no_expansion,
        )
        .unwrap();
        assert_eq!(out, vec!["model.shop.a".to_string()]);

        let none = apply_selectors(
            ids,
            &nodes,
            Some("config.materialized:table"),
            None,
            None,
            &no_expansion,
        )
        .unwrap();
        assert!(none.is_empty());
    }

    /// The generalisation: any config key, nested keys, and list-valued keys.
    #[test]
    fn apply_selectors_filters_by_arbitrary_config_keys() {
        let (ids, nodes) = build_config_project();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("config.schema:audit"), vec!["model.shop.a".to_string()]);
        assert!(select("config.schema:marts").is_empty());

        assert_eq!(select("config.meta.owner:finance"), vec!["model.shop.a".to_string()]);
        assert!(select("config.meta.owner:marketing").is_empty());

        assert_eq!(select("config.unique_key:order_id"), vec!["model.shop.b".to_string()]);
        assert!(select("config.unique_key:customer_id").is_empty());
    }

    /// A config key no node sets is a legitimately empty selection, not an
    /// unevaluable method — the distinction the rejection path exists to draw.
    #[test]
    fn apply_selectors_unknown_config_key_selects_nothing_without_erroring() {
        let (ids, nodes) = build_config_project();
        let out =
            apply_selectors(ids, &nodes, Some("config.not_a_key:foo"), None, None, &no_expansion)
                .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn apply_selectors_config_naming_no_key_is_rejected() {
        let (ids, nodes) = build_config_project();
        let err = apply_selectors(ids, &nodes, Some("config:not_a_key"), None, None, &no_expansion)
            .expect_err("a config criterion naming no key must be rejected");
        assert!(err.to_string().contains("--select"), "got: {err:#}");
    }

    // --- resource-specific methods, end to end ---

    /// One node of every kind a resource-specific method names, so both a match
    /// and a non-match are observable for each.
    fn build_mixed_resource_project() -> (Vec<String>, Nodes) {
        let mut nodes = Nodes {
            project_name: Some("shop".to_string()),
            ..Nodes::default()
        };
        nodes
            .models
            .insert("model.shop.orders".to_string(), model_node("model.shop.orders", "orders"));
        nodes.sources.insert(
            "source.shop.raw.orders".to_string(),
            source_node("source.shop.raw.orders", "orders", "raw"),
        );
        nodes.exposures.insert(
            "exposure.shop.weekly".to_string(),
            exposure_node("exposure.shop.weekly", "weekly"),
        );
        nodes.metrics.insert(
            "metric.shop.revenue".to_string(),
            metric_node("metric.shop.revenue", "revenue"),
        );
        nodes.tests.insert(
            "test.shop.nn_orders".to_string(),
            generic_test_node("test.shop.nn_orders", "nn_orders", "not_null"),
        );
        nodes.tests.insert(
            "test.shop.assert_totals".to_string(),
            singular_test_node("test.shop.assert_totals", "assert_totals"),
        );

        let ids = nodes.iter().map(|(id, _)| id.clone()).collect();
        (ids, nodes)
    }

    #[test]
    fn apply_selectors_source_method() {
        let (ids, nodes) = build_mixed_resource_project();
        let out = apply_selectors(
            ids.clone(),
            &nodes,
            Some("source:raw.orders"),
            None,
            None,
            &no_expansion,
        )
        .unwrap();
        assert_eq!(out, vec!["source.shop.raw.orders".to_string()]);

        let none =
            apply_selectors(ids, &nodes, Some("source:raw.customers"), None, None, &no_expansion)
                .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn apply_selectors_exposure_and_metric_methods() {
        let (ids, nodes) = build_mixed_resource_project();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("exposure:weekly"), vec!["exposure.shop.weekly".to_string()]);
        assert!(select("exposure:monthly").is_empty());
        assert_eq!(select("metric:revenue"), vec!["metric.shop.revenue".to_string()]);
        assert!(select("metric:cost").is_empty());
    }

    #[test]
    fn apply_selectors_test_type_and_test_name_methods() {
        let (ids, nodes) = build_mixed_resource_project();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("test_type:generic"), vec!["test.shop.nn_orders".to_string()]);
        assert_eq!(select("test_type:singular"), vec!["test.shop.assert_totals".to_string()]);
        assert!(select("test_type:unit").is_empty());

        // A generic test answers to the macro behind it, not its node name.
        assert_eq!(select("test_name:not_null"), vec!["test.shop.nn_orders".to_string()]);
        assert!(select("test_name:unique").is_empty());
    }

    #[test]
    fn apply_selectors_access_and_group_methods() {
        let (ids, mut nodes) = build_mixed_resource_project();
        let mut orders = (*nodes.models["model.shop.orders"]).clone();
        orders.__model_attr__.group = Some("finance".to_string());
        orders.__model_attr__.access = dbt_schemas::schemas::common::Access::Public;
        nodes
            .models
            .insert("model.shop.orders".to_string(), Arc::new(orders));

        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("group:finance"), vec!["model.shop.orders".to_string()]);
        assert!(select("group:marketing").is_empty());
        assert_eq!(select("access:public"), vec!["model.shop.orders".to_string()]);
        assert!(select("access:private").is_empty());
    }

    #[test]
    fn apply_selectors_resource_type_filter() {
        let (ids, nodes) = build_mixed_resource_project();
        let out = apply_selectors(
            ids.clone(),
            &nodes,
            Some("resource_type:source"),
            None,
            None,
            &no_expansion,
        )
        .unwrap();
        assert_eq!(out, vec!["source.shop.raw.orders".to_string()]);

        // `relation` is dbt's alias for everything that is not a test or check.
        let relations =
            apply_selectors(ids, &nodes, Some("resource_type:relation"), None, None, &no_expansion)
                .unwrap();
        assert!(relations.contains(&"model.shop.orders".to_string()));
        assert!(!relations.contains(&"test.shop.nn_orders".to_string()));
    }

    #[test]
    fn apply_selectors_version_method() {
        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.orders.v1".to_string(),
            versioned_model_node("model.shop.orders.v1", "orders", Some("1"), Some("2")),
        );
        nodes.models.insert(
            "model.shop.orders.v2".to_string(),
            versioned_model_node("model.shop.orders.v2", "orders", Some("2"), Some("2")),
        );
        nodes
            .models
            .insert("model.shop.plain".to_string(), model_node("model.shop.plain", "plain"));
        let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("version:latest"), vec!["model.shop.orders.v2".to_string()]);
        assert_eq!(select("version:old"), vec!["model.shop.orders.v1".to_string()]);
        assert_eq!(select("version:none"), vec!["model.shop.plain".to_string()]);
        assert!(select("version:prerelease").is_empty());
    }

    // --- unsupported method rejection ---

    /// The failure mode this guards: an unevaluable method contributes an empty
    /// set, so inside an `or` it silently drops nodes the user asked for.
    #[test]
    fn unsupported_method_in_union_is_rejected_not_ignored() {
        let (ids, nodes) = build_three_model_project();
        let err = apply_selectors(
            ids,
            &nodes,
            Some("tag:nightly result:success"),
            None,
            None,
            &no_expansion,
        )
        .expect_err("result: is not evaluable and must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("result"), "should name the method: {msg}");
        assert!(msg.contains("unsupported selector method"), "got: {msg}");
    }

    /// And in `--exclude` it silently excludes nothing, which over-selects.
    #[test]
    fn unsupported_method_in_exclude_is_rejected() {
        let (ids, nodes) = build_three_model_project();
        let err =
            apply_selectors(ids, &nodes, None, Some("source_status:fresher"), None, &no_expansion)
                .expect_err("source_status: is not evaluable and must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("--exclude"), "got: {msg}");
        assert!(msg.contains("source_status"), "should name the method: {msg}");
    }

    /// Every method left unsupported, each named in the error it produces.
    #[test]
    fn methods_needing_data_the_planner_lacks_are_all_rejected() {
        let (ids, nodes) = build_three_model_project();
        for (selector, method) in [
            ("result:success", "result"),
            ("source_status:fresher", "source_status"),
            ("column:model.shop.customers.id", "column"),
            ("selector:nightly", "selector"),
        ] {
            let err =
                apply_selectors(ids.clone(), &nodes, Some(selector), None, None, &no_expansion)
                    .expect_err("must be rejected");
            assert!(format!("{err:#}").contains(method), "{selector}: {err:#}");
        }
    }

    /// A value a resource-specific method cannot read is a rejection too — the
    /// selector names no resource at all, so matching nothing would hide it.
    #[test]
    fn a_malformed_resource_specific_value_is_rejected() {
        let (ids, nodes) = build_three_model_project();
        for selector in ["exposure:a.b.c", "source:a.b.c.d", "test_type:integration"] {
            let err =
                apply_selectors(ids.clone(), &nodes, Some(selector), None, None, &no_expansion)
                    .expect_err("must be rejected");
            assert!(
                format!("{err:#}").contains("unsupported selector method"),
                "{selector}: {err:#}"
            );
        }
    }

    // --- graph operators ---

    /// Build a 4-node DAG: stg_customers, stg_orders -> orders -> ar_summary
    fn build_dag_project() -> (Vec<String>, Nodes) {
        use dbt_common::CodeLocationWithFile;
        use dbt_schemas::schemas::common::NodeDependsOn;

        let mut nodes = Nodes::default();

        let mk = |uid: &str, name: &str, depends_on_uids: &[&str]| {
            let mut model = (*model_node(uid, name)).clone();
            model.__base_attr__.depends_on = NodeDependsOn {
                nodes_with_ref_location: depends_on_uids
                    .iter()
                    .map(|dep| ((*dep).to_string(), CodeLocationWithFile::default()))
                    .collect(),
                ..NodeDependsOn::default()
            };
            Arc::new(model)
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
        let out =
            apply_selectors(ids, &nodes, Some("+ar_summary"), None, None, &no_expansion).unwrap();
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
        let out =
            apply_selectors(ids, &nodes, Some("1+ar_summary"), None, None, &no_expansion).unwrap();
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
        let out =
            apply_selectors(ids, &nodes, Some("stg_orders+"), None, None, &no_expansion).unwrap();
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
        let out =
            apply_selectors(ids, &nodes, Some("stg_orders+1"), None, None, &no_expansion).unwrap();
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
        let out = apply_selectors(ids, &nodes, Some("@orders"), None, None, &no_expansion).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert!(set.contains("model.shop.orders"));
        assert!(set.contains("model.shop.stg_orders"), "direct parent");
        assert!(set.contains("model.shop.ar_summary"), "direct child");
        // stg_customers is two hops away (ar_summary's other parent) — not pulled in.
        assert!(!set.contains("model.shop.stg_customers"));
    }

    // --- state: selectors ---

    fn state_nodes() -> Nodes {
        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.unchanged".to_string(),
            model_with_code("model.shop.unchanged", "unchanged", "select 1"),
        );
        nodes.models.insert(
            "model.shop.edited".to_string(),
            model_with_code("model.shop.edited", "edited", "select 2 -- edited"),
        );
        nodes.models.insert(
            "model.shop.brand_new".to_string(),
            model_with_code("model.shop.brand_new", "brand_new", "select 3"),
        );
        nodes
    }

    fn previous_manifest() -> serde_json::Value {
        serde_json::json!({
            "nodes": {
                "model.shop.unchanged": {"raw_code": "select 1"},
                "model.shop.edited": {"raw_code": "select 2"},
                "model.shop.removed": {"raw_code": "select 0"},
            }
        })
    }

    #[test]
    fn state_selectors_filter_by_the_previous_manifest_comparison() {
        let nodes = state_nodes();
        let state = StateSelector::from_previous_manifest(&nodes, &previous_manifest());
        let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, Some(&state), &no_expansion)
                .unwrap()
        };

        assert_eq!(
            select("state:modified"),
            vec![
                "model.shop.brand_new".to_string(),
                "model.shop.edited".to_string()
            ]
        );
        assert_eq!(select("state:new"), vec!["model.shop.brand_new".to_string()]);
        // Every `modified.<sub>` coarsens to the full modified set.
        assert_eq!(select("state:modified.body").len(), 2);
        assert_eq!(select("state:unmodified"), vec!["model.shop.unchanged".to_string()]);
        assert_eq!(
            select("state:old"),
            vec![
                "model.shop.edited".to_string(),
                "model.shop.unchanged".to_string()
            ]
        );
    }

    #[test]
    fn supported_state_subselectors_are_accepted() {
        let nodes = state_nodes();
        let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        for sel in [
            "state:new",
            "state:modified",
            "state:modified.body",
            "state:old",
            "state:unmodified",
        ] {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion)
                .unwrap_or_else(|e| panic!("{sel} should be accepted: {e:#}"));
        }
    }

    #[test]
    fn unsupported_state_subselector_is_rejected() {
        let nodes = state_nodes();
        let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        for selector in ["state:sideways", "state:modified.bdoy"] {
            let err =
                apply_selectors(ids.clone(), &nodes, Some(selector), None, None, &no_expansion)
                    .expect_err("has no backing set");
            assert!(format!("{err:#}").contains(selector), "got: {err:#}");
        }
    }

    /// These name dimensions the comparison does not read. Answering them with
    /// the body comparison is a narrowing, not a coarsening: `modified.configs`
    /// would match only nodes whose *body* changed, so a config-only change —
    /// the thing it was asked about — selected nothing at all.
    #[test]
    fn a_modified_subselector_this_comparison_cannot_decide_is_rejected() {
        let nodes = state_nodes();
        let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        for sub in [
            "configs",
            "relation",
            "persisted_descriptions",
            "macros",
            "contract",
        ] {
            let selector = format!("state:modified.{sub}");
            let err =
                apply_selectors(ids.clone(), &nodes, Some(&selector), None, None, &no_expansion)
                    .expect_err("this comparison cannot decide it");
            let msg = format!("{err:#}");
            assert!(msg.contains(sub), "should name the dimension: {msg}");
            assert!(msg.contains("state:modified.body"), "should point at what it can do: {msg}");
        }
    }

    #[test]
    fn state_selector_without_sets_matches_nothing() {
        let nodes = state_nodes();
        let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        let out = apply_selectors(ids, &nodes, Some("state:modified"), None, None, &no_expansion)
            .unwrap();
        assert!(out.is_empty());
    }
}
