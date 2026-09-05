//! Freshness checks — the `source-freshness` and `freshness` commands.
//!
//! Mirrors `dbt source freshness` / `dbt freshness`: per node, run the bundled
//! `collect_freshness` macro (`select max(loaded_at_field), current_timestamp
//! from <relation>`) — or `collect_freshness_custom_sql` for `loaded_at_query`
//! nodes — then compare the age of the freshest row against the node's
//! `warn_after` / `error_after` rules.
//!
//! Sources and models are siblings here: `dbt freshness` measures every source
//! plus every model carrying a freshness *SLA*. A model's `build_after` /
//! `updates_on` config is deliberately not part of that — it is a scheduling
//! rule consumed by state-aware `build`/`run` to decide whether a model needs
//! rebuilding, not something the freshness command measures.

mod criteria;
#[cfg(test)]
mod test_util;
mod timestamps;

use std::collections::BTreeMap;

use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::freshness_node::FreshnessNodeRef;
use dbt_schemas::schemas::telemetry::NodeType;

use crate::error::DbtTemporalError;
use crate::types::FreshnessOutcome;

pub use criteria::{declares_unmeasurable_sla, node_has_freshness_check, validate_freshness_rules};

use criteria::{LoadedAt, evaluate, loaded_at};

/// Freshness verdict before it's folded into the activity result.
#[derive(Debug)]
pub enum FreshnessVerdict {
    /// Within thresholds (or only informational): carry the outcome.
    Fresh(FreshnessOutcome),
    /// warn_after exceeded but error_after not: succeed with a warning.
    Warning(FreshnessOutcome),
    /// error_after exceeded.
    Stale {
        max_loaded_at: String,
        age_secs: f64,
        max_allowed_secs: i64,
    },
}

/// The freshness-node view of a resolved node, or `None` for node types that
/// carry no freshness criteria at all.
pub fn as_freshness_node<'a>(
    nodes: &'a Nodes,
    unique_id: &str,
    rt: NodeType,
) -> Option<&'a dyn FreshnessNodeRef> {
    match rt {
        NodeType::Source => nodes
            .sources
            .get(unique_id)
            .map(|s| s.as_ref() as &dyn FreshnessNodeRef),
        NodeType::Model => nodes
            .models
            .get(unique_id)
            .map(|m| m.as_ref() as &dyn FreshnessNodeRef),
        _ => None,
    }
}

/// Execute the freshness query for `node` through the Jinja macro layer and
/// evaluate the result against its criteria.
pub fn run_freshness_check(
    node: &dyn FreshnessNodeRef,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> Result<FreshnessVerdict, DbtTemporalError> {
    let unique_id = &node.common().unique_id;
    let kind = node.kind_label().to_lowercase();
    let criteria = node.freshness_criteria();

    // The macros end in `return(load_result(...))` — the statement result is
    // consumed inside the macro, so re-reading it from the result store would
    // fail with "already loaded". Evaluate the macro as an expression and use
    // its return value instead.
    //
    // `this` rather than the resolved relation name: per-workflow env
    // overrides patch `this` in the node context, and the check must query the
    // schema this run actually targets.
    let mut ctx = node_context.clone();
    let expression = match loaded_at(node) {
        Some(LoadedAt::Field(field)) => {
            ctx.insert("__dbt_freshness_loaded_at__".to_owned(), minijinja::Value::from(field));
            ctx.insert(
                "__dbt_freshness_filter__".to_owned(),
                criteria
                    .filter
                    .as_deref()
                    .map_or_else(|| minijinja::Value::from(()), minijinja::Value::from),
            );
            "collect_freshness(this, __dbt_freshness_loaded_at__, __dbt_freshness_filter__)"
        }
        Some(LoadedAt::Query(query)) => {
            ctx.insert("__dbt_freshness_query__".to_owned(), minijinja::Value::from(query));
            "collect_freshness_custom_sql(this, __dbt_freshness_query__)"
        }
        None => {
            return Err(DbtTemporalError::Configuration(format!(
                "{kind} {unique_id} has freshness criteria but no loaded_at_field or \
                 loaded_at_query — metadata-based freshness is not supported"
            )));
        }
    };

    let result = jinja_env
        .compile_expression(expression)
        .map_err(|e| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "compiling freshness expression for {unique_id}: {e}"
            ))
        })?
        .eval(&ctx, &[])
        .map_err(|e| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "freshness query for {unique_id} failed: {e}"
            ))
        })?;

    let (max_loaded_at, snapshotted_at) = timestamps::extract_timestamps(&result).map_err(|e| {
        DbtTemporalError::Adapter(anyhow::anyhow!("reading freshness result for {unique_id}: {e}"))
    })?;

    Ok(evaluate(
        &criteria,
        node.resource_type().as_static_ref(),
        max_loaded_at,
        snapshotted_at,
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessPeriod};
    use dbt_schemas::schemas::nodes::{DbtModel, DbtSource};
    use dbt_schemas::schemas::properties::ModelFreshness;
    use test_util::{model_with, rules, source_with};

    fn empty_jinja_env() -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(minijinja::Environment::new())
    }

    #[test]
    fn as_freshness_node_only_resolves_sources_and_models() {
        let mut nodes = Nodes::default();
        let mut source = DbtSource::default();
        source.__common_attr__.unique_id = "source.pkg.raw.orders".to_string();
        nodes
            .sources
            .insert(source.__common_attr__.unique_id.clone(), std::sync::Arc::new(source));
        let mut model = DbtModel::default();
        model.__common_attr__.unique_id = "model.pkg.stg_orders".to_string();
        nodes
            .models
            .insert(model.__common_attr__.unique_id.clone(), std::sync::Arc::new(model));

        assert!(as_freshness_node(&nodes, "source.pkg.raw.orders", NodeType::Source).is_some());
        assert!(as_freshness_node(&nodes, "model.pkg.stg_orders", NodeType::Model).is_some());
        assert!(as_freshness_node(&nodes, "model.pkg.stg_orders", NodeType::Seed).is_none());
        assert!(as_freshness_node(&nodes, "model.pkg.missing", NodeType::Model).is_none());
    }

    #[test]
    fn run_freshness_check_requires_a_loaded_at() {
        let source = source_with(
            FreshnessDefinition {
                warn_after: Some(rules(1, FreshnessPeriod::hour)),
                ..Default::default()
            },
            None,
        );
        let err = run_freshness_check(&source, &empty_jinja_env(), &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("no loaded_at_field"), "{err}");
        assert!(!err.is_retryable(), "a misconfigured node must not retry");
    }

    #[test]
    fn run_freshness_check_surfaces_query_errors() {
        // An environment without the dbt macros: the collect_freshness call
        // fails at evaluation and must surface as a (retryable) adapter error.
        let source = source_with(
            FreshnessDefinition {
                error_after: Some(rules(1, FreshnessPeriod::day)),
                ..Default::default()
            },
            Some("loaded_at"),
        );
        let err = run_freshness_check(&source, &empty_jinja_env(), &BTreeMap::new()).unwrap_err();
        assert!(err.is_retryable(), "query failures must stay retryable: {err}");
    }

    #[test]
    fn run_freshness_check_prefers_loaded_at_query() {
        // Both keys set: the query wins, so the failure comes from evaluating
        // collect_freshness_custom_sql rather than collect_freshness.
        let model = model_with(ModelFreshness {
            error_after: Some(rules(1, FreshnessPeriod::day)),
            loaded_at_field: Some("updated_at".to_string()),
            loaded_at_query: Some("select 1".to_string()),
            ..Default::default()
        });
        let err = run_freshness_check(&model, &empty_jinja_env(), &BTreeMap::new()).unwrap_err();
        assert!(err.is_retryable(), "{err}");
        assert!(err.to_string().contains("freshness query for"), "{err}");
    }
}
