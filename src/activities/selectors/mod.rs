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

use std::collections::BTreeSet;

use anyhow::Context;
use dbt_common::node_selector::{
    SelectExpression, SelectionCriteria, parse_model_specifiers, selectors_require_manifest,
};
use dbt_schemas::schemas::Nodes;

use criteria::{Criterion, MatchContext, criterion_support_error};

pub use graph::{DependencyMaps, LazyDependencyMaps, reverse_dep_map};
pub use state::StateSelector;

/// Apply --select and --exclude filters to a list of node IDs.
///
/// `state` backs the `state:` methods; selectors using them match nothing when
/// it is `None` (the planner validates this upfront).
///
/// `expand_indirect` receives the project's dependency maps, which are built at
/// most once per call, on the first graph operator or expansion that reads them.
pub fn apply_selectors(
    mut selected_ids: Vec<String>,
    nodes: &Nodes,
    select: Option<&str>,
    exclude: Option<&str>,
    state: Option<&StateSelector>,
    expand_indirect: &dyn Fn(Vec<String>, &LazyDependencyMaps<'_>) -> Vec<String>,
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

    // Graph operators (`+model`, `model+`, `@model`) and indirect selection walk
    // the dependency maps; plain selectors never touch them, so they are built
    // on first use and shared by every walk in this call.
    let graph = LazyDependencyMaps::new(nodes);

    if let Some(expr) = select_expr {
        let matched = resolve_expression(nodes, &graph, ctx, &expr)?;
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
    selected_ids = expand_indirect(selected_ids, &graph);

    if let Some(expr) = exclude_expr {
        let matched = resolve_expression(nodes, &graph, ctx, &expr)?;
        // The exclusion expands too. Upstream evaluates an exclude expression
        // through the same per-atom pipeline as a select expression, indirect
        // selection included, so `--exclude my_model` drops the model *and* the
        // tests hanging off it. Subtracting only the literal matches left those
        // tests in the run, querying a model the same command had declined to
        // build.
        let excluded = expand_indirect(matched.into_iter().collect(), &graph);
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

/// Whether the parsed selectors contain a `state:` method, i.e. whether the
/// planner needs a previous manifest to evaluate them.
///
/// Decided from the parsed method rather than the text: `tag:my_state:thing` and
/// a path containing `state:` mention the word without using the method.
pub fn selectors_use_state(
    select: Option<&str>,
    exclude: Option<&str>,
) -> Result<bool, anyhow::Error> {
    let select_expr = parse_selector(select).context("invalid --select")?;
    let exclude_expr = parse_selector(exclude).context("invalid --exclude")?;
    Ok(selectors_require_manifest(select_expr.as_ref(), exclude_expr.as_ref()))
}

/// Reject every criterion in `expr` that [`Criterion::parse`] cannot read.
///
/// Walking the whole tree up front means one error names every problem in the
/// selector rather than the user fixing them one round-trip at a time.
fn validate_selector_support(expr: &SelectExpression) -> Result<(), anyhow::Error> {
    reject_expression_exclude(expr)?;
    let mut errors = Vec::new();
    collect_unsupported(expr, &mut errors);
    if errors.is_empty() {
        return Ok(());
    }
    errors.dedup();
    Err(unsupported_methods_error(&errors))
}

/// Reject a [`SelectExpression::Exclude`] node anywhere in the tree.
///
/// `parse_model_specifiers` builds only atoms joined by `And`/`Or`; `Exclude`
/// nodes come from `selectors.yml` definitions, which this planner does not
/// read. Failing loudly keeps an exclude from being evaluated as an include if
/// a tree ever reaches the planner some other way.
fn reject_expression_exclude(expr: &SelectExpression) -> Result<(), anyhow::Error> {
    match expr {
        SelectExpression::Atom(criteria) => criteria
            .exclude
            .as_deref()
            .map_or(Ok(()), reject_expression_exclude),
        SelectExpression::And(exprs) | SelectExpression::Or(exprs) => {
            exprs.iter().try_for_each(reject_expression_exclude)
        }
        SelectExpression::Exclude(_) => Err(expression_exclude_error()),
    }
}

fn expression_exclude_error() -> anyhow::Error {
    anyhow::anyhow!(
        "exclude expressions nested inside a selector are not supported; they exist only in \
         selectors.yml definitions, which are not read. Use --exclude"
    )
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
        // Rejected by `reject_expression_exclude` before this walk runs.
        SelectExpression::Exclude(_) => {}
    }
}

/// Resolve a selector expression to a set of matching node IDs, expanding graph operators.
fn resolve_expression(
    nodes: &Nodes,
    graph: &LazyDependencyMaps<'_>,
    ctx: MatchContext<'_>,
    expr: &SelectExpression,
) -> Result<BTreeSet<String>, anyhow::Error> {
    match expr {
        SelectExpression::Atom(criteria) => resolve_criteria(nodes, graph, ctx, criteria),
        SelectExpression::And(exprs) => {
            let mut result: Option<BTreeSet<String>> = None;
            for e in exprs {
                let matched = resolve_expression(nodes, graph, ctx, e)?;
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
                result.extend(resolve_expression(nodes, graph, ctx, e)?);
            }
            Ok(result)
        }
        // `validate_selector_support` rejects these before resolution starts.
        SelectExpression::Exclude(_) => Err(expression_exclude_error()),
    }
}

/// Resolve a single selection criterion, including graph operator expansion.
fn resolve_criteria(
    nodes: &Nodes,
    graph: &LazyDependencyMaps<'_>,
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

    // Graph operators expand every matched node at once. Each direction is one
    // multi-source walk, so `tag:x+` over hundreds of matches costs one pass.
    // When `+` depths are given they take precedence over `@`, as in dbt v2's
    // scheduler: `@+model` is `+model`.
    if criteria.parents_depth.is_some() || criteria.children_depth.is_some() {
        let seeds = matched.clone();
        if let Some(depth) = criteria.parents_depth {
            matched.extend(graph::expand_from_seeds(&seeds, &graph.get().deps, depth));
        }
        if let Some(depth) = criteria.children_depth {
            matched.extend(graph::expand_from_seeds(&seeds, &graph.get().reverse, depth));
        }
    } else if criteria.childrens_parents {
        matched = graph::childrens_parents(graph.get(), &matched);
    }

    // A nested exclude removes from the fully expanded selection, so the nodes
    // it names are dropped even when the graph operators pulled them in.
    if let Some(excl) = &criteria.exclude {
        let excluded = resolve_expression(nodes, graph, ctx, excl)?;
        matched.retain(|id| !excluded.contains(id));
    }

    Ok(matched)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Selection tests drive `--select`/`--exclude` alone; the expansion hook
    /// is exercised by the indirect-selection tests and by `plan_project`.
    fn no_expansion(ids: Vec<String>, _: &LazyDependencyMaps<'_>) -> Vec<String> {
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

    /// The matcher must not substring-match the dotted FQN, or `customers`
    /// would also pull in `stg_customers`.
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

    /// And one ending in `.sql` parses as `file:`, which selects a node
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
        assert!(none.is_empty(), "{none:?}");
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
        assert!(none.is_empty(), "{none:?}");
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
        assert!(none.is_empty(), "{none:?}");
    }

    /// The generalisation: any config key, nested keys, and list-valued keys.
    #[test]
    fn apply_selectors_filters_by_arbitrary_config_keys() {
        let (ids, nodes) = build_config_project();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("config.schema:audit"), vec!["model.shop.a".to_string()]);
        assert_eq!(select("config.schema:marts"), Vec::<String>::new());

        assert_eq!(select("config.meta.owner:finance"), vec!["model.shop.a".to_string()]);
        assert_eq!(select("config.meta.owner:marketing"), Vec::<String>::new());

        assert_eq!(select("config.unique_key:order_id"), vec!["model.shop.b".to_string()]);
        assert_eq!(select("config.unique_key:customer_id"), Vec::<String>::new());
    }

    /// A config key no node sets is a legitimately empty selection, not an
    /// unevaluable method — the distinction the rejection path exists to draw.
    #[test]
    fn apply_selectors_unknown_config_key_selects_nothing_without_erroring() {
        let (ids, nodes) = build_config_project();
        let out =
            apply_selectors(ids, &nodes, Some("config.not_a_key:foo"), None, None, &no_expansion)
                .unwrap();
        assert!(out.is_empty(), "{out:?}");
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
        assert!(none.is_empty(), "{none:?}");
    }

    #[test]
    fn apply_selectors_exposure_and_metric_methods() {
        let (ids, nodes) = build_mixed_resource_project();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("exposure:weekly"), vec!["exposure.shop.weekly".to_string()]);
        assert_eq!(select("exposure:monthly"), Vec::<String>::new());
        assert_eq!(select("metric:revenue"), vec!["metric.shop.revenue".to_string()]);
        assert_eq!(select("metric:cost"), Vec::<String>::new());
    }

    #[test]
    fn apply_selectors_test_type_and_test_name_methods() {
        let (ids, nodes) = build_mixed_resource_project();
        let select = |sel: &str| {
            apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
        };

        assert_eq!(select("test_type:generic"), vec!["test.shop.nn_orders".to_string()]);
        assert_eq!(select("test_type:singular"), vec!["test.shop.assert_totals".to_string()]);
        assert_eq!(select("test_type:unit"), Vec::<String>::new());

        // A generic test answers to the macro behind it, not its node name.
        assert_eq!(select("test_name:not_null"), vec!["test.shop.nn_orders".to_string()]);
        assert_eq!(select("test_name:unique"), Vec::<String>::new());
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
        assert_eq!(select("group:marketing"), Vec::<String>::new());
        assert_eq!(select("access:public"), vec!["model.shop.orders".to_string()]);
        assert_eq!(select("access:private"), Vec::<String>::new());
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
        assert_eq!(select("version:prerelease"), Vec::<String>::new());
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

    /// `@orders` is orders, everything downstream of it, and everything those
    /// descendants are built from — not just one hop in each direction.
    #[test]
    fn apply_selectors_at_operator_selects_descendants_and_their_ancestors() {
        let (ids, nodes) = build_dag_project();
        let out = apply_selectors(ids, &nodes, Some("@orders"), None, None, &no_expansion).unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert_eq!(
            set,
            BTreeSet::from([
                "model.shop.orders",
                "model.shop.ar_summary",
                "model.shop.stg_orders",
                // ar_summary's other parent, two hops from the seed.
                "model.shop.stg_customers",
            ])
        );
    }

    /// The seed's own ancestors that feed no descendant stay out: `@stg_customers`
    /// takes ar_summary and what ar_summary needs, which includes orders.
    #[test]
    fn apply_selectors_at_operator_follows_every_matched_seed() {
        let (ids, nodes) = build_dag_project();
        let out = apply_selectors(
            ids,
            &nodes,
            Some("@stg_customers"),
            Some("stg_orders"),
            None,
            &no_expansion,
        )
        .unwrap();
        let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
        assert_eq!(
            set,
            BTreeSet::from([
                "model.shop.stg_customers",
                "model.shop.ar_summary",
                "model.shop.orders",
            ])
        );
    }

    /// `@` composes with methods like any other graph operator.
    #[test]
    fn apply_selectors_at_operator_works_with_methods_and_intersection() {
        let (ids, mut nodes) = build_dag_project();
        let tagged = nodes
            .models
            .get_mut("model.shop.stg_orders")
            .map(|m| {
                let mut model = (**m).clone();
                model.__common_attr__.tags = vec!["nightly".to_string()];
                Arc::new(model)
            })
            .unwrap();
        nodes
            .models
            .insert("model.shop.stg_orders".to_string(), tagged);

        let by_tag =
            apply_selectors(ids.clone(), &nodes, Some("@tag:nightly"), None, None, &no_expansion)
                .unwrap();
        assert_eq!(
            by_tag.iter().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "model.shop.stg_orders",
                "model.shop.orders",
                "model.shop.ar_summary",
                "model.shop.stg_customers",
            ])
        );

        let intersected = apply_selectors(
            ids,
            &nodes,
            Some("@tag:nightly,stg_customers"),
            None,
            None,
            &no_expansion,
        )
        .unwrap();
        assert_eq!(intersected, vec!["model.shop.stg_customers".to_string()]);
    }

    /// With a `+` operator present the `@` is ignored, as dbt v2 does.
    #[test]
    fn apply_selectors_plus_operators_take_precedence_over_at() {
        let (ids, nodes) = build_dag_project();
        let out =
            apply_selectors(ids, &nodes, Some("@+orders"), None, None, &no_expansion).unwrap();
        assert_eq!(
            out.iter().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["model.shop.orders", "model.shop.stg_orders"])
        );
    }

    /// Several matched seeds, depth limits and both directions through the
    /// public entry point: `tag:`-style multi-seed selection agrees with the
    /// union of the single-seed selections.
    #[test]
    fn apply_selectors_multi_seed_depth_limits_match_the_union_of_single_seeds() {
        let (ids, nodes) = build_dag_project();
        for spec in ["{}+1", "1+{}", "{}+", "+{}", "2+{}"] {
            let single = |name: &str| {
                apply_selectors(
                    ids.clone(),
                    &nodes,
                    Some(&spec.replace("{}", name)),
                    None,
                    None,
                    &no_expansion,
                )
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>()
            };
            let union: BTreeSet<String> = ["stg_orders", "stg_customers"]
                .into_iter()
                .flat_map(single)
                .collect();
            let combined = apply_selectors(
                ids.clone(),
                &nodes,
                Some(&spec.replace("{}", "stg_*")),
                None,
                None,
                &no_expansion,
            )
            .unwrap()
            .into_iter()
            .collect::<BTreeSet<_>>();
            assert_eq!(combined, union, "{spec}");
        }
    }

    /// The nested exclude removes from the expanded selection.
    #[test]
    fn a_nested_exclude_applies_after_graph_expansion() {
        let (_, nodes) = build_dag_project();
        let parsed = parse_selector(Some("orders+")).unwrap().unwrap();
        let SelectExpression::Atom(mut criteria) = parsed else {
            panic!("single selector parses to an atom");
        };
        criteria.exclude = parse_selector(Some("ar_summary")).unwrap().map(Box::new);

        let graph = LazyDependencyMaps::new(&nodes);
        let out = resolve_criteria(&nodes, &graph, NO_STATE, &criteria).unwrap();
        assert_eq!(out, BTreeSet::from(["model.shop.orders".to_string()]));
    }

    const NO_STATE: MatchContext<'static> = MatchContext {
        state: None,
        project_name: None,
    };

    fn exclude_expression() -> SelectExpression {
        let inner = parse_selector(Some("orders")).unwrap().unwrap();
        SelectExpression::Exclude(Box::new(inner))
    }

    /// The text parser cannot build an `Exclude` node, so one reaching the
    /// planner is refused rather than read as an include.
    #[test]
    fn an_exclude_expression_is_rejected_wherever_it_sits() {
        let atom = parse_selector(Some("stg_orders")).unwrap().unwrap();
        for expr in [
            exclude_expression(),
            SelectExpression::And(vec![atom.clone(), exclude_expression()]),
            SelectExpression::Or(vec![atom, exclude_expression()]),
        ] {
            let err = validate_selector_support(&expr).unwrap_err().to_string();
            assert!(err.contains("not supported"), "{err}");

            let (_, nodes) = build_dag_project();
            let graph = LazyDependencyMaps::new(&nodes);
            if matches!(expr, SelectExpression::Exclude(_)) {
                let err = resolve_expression(&nodes, &graph, NO_STATE, &expr)
                    .unwrap_err()
                    .to_string();
                assert!(err.contains("not supported"), "{err}");
            }
        }
    }

    #[test]
    fn nested_exclude_inside_an_atom_is_checked_for_expression_excludes() {
        let parsed = parse_selector(Some("orders")).unwrap().unwrap();
        let SelectExpression::Atom(mut criteria) = parsed else {
            panic!("single selector parses to an atom");
        };
        criteria.exclude = Some(Box::new(exclude_expression()));
        assert!(validate_selector_support(&SelectExpression::Atom(criteria)).is_err());
    }

    /// The text parser's output never contains an `Exclude` node.
    #[test]
    fn parsed_selector_text_never_yields_an_exclude_expression() {
        let expr = parse_selector(Some("a,b c+ @d,e")).unwrap().unwrap();
        assert!(validate_selector_support(&expr).is_ok());
    }

    #[test]
    fn selectors_use_state_reads_the_parsed_method() {
        assert!(selectors_use_state(Some("state:modified+"), None).unwrap());
        assert!(selectors_use_state(None, Some("state:new")).unwrap());
        assert!(selectors_use_state(Some("tag:nightly state:new"), None).unwrap());
        assert!(selectors_use_state(Some("tag:a,state:modified"), None).unwrap());

        // Mentions of the word that are not the method.
        assert!(!selectors_use_state(Some("tag:my_state:thing"), None).unwrap());
        assert!(!selectors_use_state(Some("models/state:dir"), None).unwrap());
        assert!(!selectors_use_state(Some("tag:a_state:x"), Some("tag:state:y")).unwrap());
        assert!(!selectors_use_state(None, None).unwrap());
    }

    #[test]
    fn selectors_use_state_reports_which_flag_is_malformed() {
        let err = format!("{:#}", selectors_use_state(Some("@a+"), None).unwrap_err());
        assert!(err.contains("invalid --select"), "{err}");
        let err = format!("{:#}", selectors_use_state(None, Some("@a+")).unwrap_err());
        assert!(err.contains("invalid --exclude"), "{err}");
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
        assert!(out.is_empty(), "{out:?}");
    }
}
