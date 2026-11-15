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
mod tests;
