//! Recompute where each of a run's relations lives when a per-workflow env
//! override changed the profile schema or database.
//!
//! Two strategies, chosen by whether the project overrides dbt's naming macros:
//!
//! **Default macros**: reconstruct the schema from dbt's own
//! `<target_schema>[_<custom>]` pattern against the profile-rebuilt
//! `target.schema`, and take `target.database` for the database.
//!
//! **Custom macros**: re-execute `generate_schema_name` — and
//! `generate_database_name` where the project defines one — through the
//! already-cloned Jinja env, which has `env_var()` overridden with the workflow
//! env and `target` patched. That is what dbt itself does per run, so macro
//! logic of any shape works: env reads, custom suffixes, per-model
//! `config(schema=...)`, and branching on the node.
//!
//! Either way the macros are evaluated **per node**, against that node's own
//! attributes. A project is entitled to send two models that share a startup
//! schema to different ones, and a schema-to-schema map cannot express that; a
//! map from each startup relation to where it moved can.
//!
//! With custom macros, what a node resolves to is remembered for the rest of its
//! run (see `worker::relation_cache`): an ancestor shared by many nodes is
//! rendered once, not once per descendant.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::nodes::NodeBaseAttributes;
use dbt_schemas::schemas::telemetry::NodeType;

mod naming_macro;

use naming_macro::{custom_name, naming_macro_node, naming_template, render_naming_macro};

use super::sql_rewrite::{RelationMove, RelationRewrite};
use crate::activities::render_env::RenderOverrides;
use crate::error::DbtTemporalError;
use crate::worker::relation_cache::{NamingMacro, NodeRelation, RunRelations, RunScope};
use crate::worker_state::WorkerState;

/// Everything the executing node needs to name relations correctly: where its
/// own output goes, and where every relation its compiled SQL can mention has
/// moved to.
#[derive(Debug, Default)]
pub struct ResolvedRelations {
    /// `None` when the node's own relation did not move.
    pub own: Option<NodeRelation>,
    pub rewrite: RelationRewrite,
}

/// Resolve the relations the executing node can name, under this workflow's
/// env overrides.
///
/// The scope is the node itself plus everything it transitively depends on:
/// exactly the set a compiled statement can mention, through its own `ref()`s,
/// through the CTEs an ephemeral ancestor injects, and — for a unit test —
/// through the SQL of the model under test. Resolving the whole project would
/// mean a macro render per project node per activity to answer questions
/// nobody asked.
///
/// `invocation_id` and `overrides` identify the run whose earlier answers may be
/// reused; `jinja_env` must be the environment those overrides were applied to.
pub fn resolve_relations(
    state: &WorkerState,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    invocation_id: &str,
    overrides: &RenderOverrides<'_>,
    unique_id: &str,
    env_schema: Option<&str>,
    env_database: Option<&str>,
) -> Result<ResolvedRelations, DbtTemporalError> {
    let mut resolved = ResolvedRelations::default();
    if env_schema.is_none() && env_database.is_none() {
        return Ok(resolved);
    }

    // Only the macros' renders are worth remembering; the default pattern is
    // string arithmetic.
    let run =
        (state.has_custom_schema_name_macro || state.has_custom_database_name_macro).then(|| {
            state.naming.run(RunScope::new(
                invocation_id,
                overrides.target,
                overrides.env,
                overrides.vars,
                env_schema,
                env_database,
            ))
        });

    for id in rewrite_scope(&state.resolver_state.nodes, unique_id) {
        let Some(node) = state.resolver_state.nodes.get_node(&id) else {
            continue;
        };
        if !takes_part_in_the_rewrite(node) {
            continue;
        }
        let base = node.base();
        let resolve = || resolve_one(state, jinja_env, node, env_schema, env_database);
        let moved = match run.as_deref() {
            Some(run) => RunRelations::resolve(run, &id, resolve)?,
            None => resolve()?,
        };
        if moved.schema == base.schema && moved.database == base.database {
            continue;
        }
        if id == unique_id {
            resolved.own = Some(moved.clone());
        }
        resolved.rewrite.insert(
            &base.schema,
            &base.alias,
            RelationMove {
                old_database: base.database.clone(),
                new_database: moved.database,
                new_schema: moved.schema,
            },
        );
    }
    Ok(resolved)
}

/// Where one node's relation lands, by whichever strategy its project's macros
/// call for.
fn resolve_one(
    state: &WorkerState,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node: &dyn InternalDbtNodeAttributes,
    env_schema: Option<&str>,
    env_database: Option<&str>,
) -> Result<NodeRelation, DbtTemporalError> {
    let base = node.base();
    let unique_id = &node.common().unique_id;

    let default_relation = || {
        compute_patched_relation_inner(
            &state.default_schema,
            &state.default_database,
            &base.schema,
            &base.database,
            env_schema,
            env_database,
            unique_id,
        )
        .unwrap_or_else(|| NodeRelation {
            database: base.database.clone(),
            schema: base.schema.clone(),
        })
    };

    if !state.has_custom_schema_name_macro && !state.has_custom_database_name_macro {
        return Ok(default_relation());
    }

    // The node as it stood when dbt called the macro: carrying the profile's
    // target values under this run's overrides, not the macro's own answer.
    let node_value = naming_macro_node(
        node,
        env_schema.unwrap_or(&state.default_schema),
        env_database.unwrap_or(&state.default_database),
    );

    let schema = if state.has_custom_schema_name_macro {
        // An empty answer is a macro that did not name this node; dbt keeps the
        // resolved value rather than writing to a schema with no name.
        let rendered = render_naming_macro(
            jinja_env,
            naming_template(state, jinja_env, NamingMacro::Schema)?,
            "generate_schema_name",
            custom_name(node, "schema").as_deref(),
            &node_value,
        )
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "re-executing generate_schema_name for {unique_id}: {e:#}"
            ))
        })?;
        if rendered.is_empty() {
            base.schema.clone()
        } else {
            rendered
        }
    } else {
        default_relation().schema
    };

    let database = if state.has_custom_database_name_macro {
        let rendered = render_naming_macro(
            jinja_env,
            naming_template(state, jinja_env, NamingMacro::Database)?,
            "generate_database_name",
            custom_name(node, "database").as_deref(),
            &node_value,
        )
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "re-executing generate_database_name for {unique_id}: {e:#}"
            ))
        })?;
        if rendered.is_empty() {
            base.database.clone()
        } else {
            rendered
        }
    } else {
        default_relation().database
    };

    Ok(NodeRelation { database, schema })
}

/// Whether a node's relation takes part in the per-workflow rewrite.
///
/// Sources do not: their schema is declared in YAML and names a table dbt did
/// not create, so moving it to the workflow's schema points every `source()`
/// at something that was never there. A source sharing the models' schema —
/// the ordinary case in a dev project — is exactly when that bites.
fn takes_part_in_the_rewrite(node: &dyn InternalDbtNodeAttributes) -> bool {
    node.resource_type() != NodeType::Source
}

/// The node itself plus everything it transitively depends on.
fn rewrite_scope(nodes: &dbt_schemas::schemas::Nodes, unique_id: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut queue = vec![unique_id.to_owned()];
    while let Some(id) = queue.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(node) = nodes.get_node(&id) {
            queue.extend(node.base().depends_on.nodes.iter().cloned());
        }
    }
    seen
}

fn compute_patched_relation_inner(
    default_schema: &str,
    default_database: &str,
    base_schema: &str,
    base_database: &str,
    env_schema: Option<&str>,
    env_database: Option<&str>,
    unique_id: &str,
) -> Option<NodeRelation> {
    let wf_schema = env_schema?;

    let wf_database = env_database.unwrap_or(base_database);
    let default_prefix = format!("{default_schema}_");

    let new_schema = if base_schema == default_schema {
        wf_schema.to_string()
    } else if let Some(custom) = base_schema.strip_prefix(&default_prefix) {
        format!("{wf_schema}_{custom}")
    } else {
        // Schema doesn't follow default pattern — keep it unchanged.
        // (The caller skips this path entirely when has_custom_schema_name_macro.)
        tracing::debug!(
            node = %unique_id,
            base_schema,
            "schema does not follow default pattern — keeping baked schema unchanged"
        );
        base_schema.to_string()
    };

    let new_database = if base_database == default_database {
        wf_database.to_string()
    } else {
        base_database.to_string()
    };

    if new_schema == base_schema && new_database == base_database {
        return None;
    }

    Some(NodeRelation {
        database: new_database,
        schema: new_schema,
    })
}

/// Apply the node's own resolved relation to its Jinja context: `this` — the
/// relation materializations call methods on — plus the bare `schema` and
/// `database` globals.
///
/// A relation that cannot be built is an error rather than a skipped step. The
/// bare globals are patched either way, so swallowing it left `this` still
/// pointing at the startup relation while everything around it named the new
/// one — the materialization would write to the old schema and the run would
/// report the new.
pub fn apply_relation_to_context(
    base: &NodeBaseAttributes,
    resolved: &NodeRelation,
    node_context: &mut BTreeMap<String, minijinja::Value>,
) -> Result<(), anyhow::Error> {
    let relation = dbt_adapter::relation::do_create_relation(
        base.adapter,
        resolved.database.clone(),
        resolved.schema.clone(),
        Some(base.alias.clone()),
        None,
        base.quoting,
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "building the patched relation {}.{}.{}: {e}",
            resolved.database,
            resolved.schema,
            base.alias
        )
    })?;
    node_context.insert(
        "this".to_owned(),
        dbt_adapter::relation::RelationObject::new(Arc::from(relation)).into_value(),
    );
    if resolved.schema != base.schema {
        node_context.insert("schema".to_owned(), minijinja::Value::from(resolved.schema.as_str()));
    }
    if resolved.database != base.database {
        node_context
            .insert("database".to_owned(), minijinja::Value::from(resolved.database.as_str()));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- compute_patched_relation_inner ---

    #[test]
    fn no_patch_when_env_schema_absent() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw",
            "warehouse",
            None,
            None,
            "model.x.y",
        );
        assert!(result.is_none());
    }

    #[test]
    fn patches_schema_when_base_equals_default() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .expect("should produce a patch");
        assert_eq!(result.schema, "workflow_42");
        assert_eq!(result.database, "warehouse");
    }

    #[test]
    fn patches_schema_with_custom_suffix_using_default_pattern() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw_marts",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .expect("should produce a patch");
        assert_eq!(result.schema, "workflow_42_marts");
    }

    #[test]
    fn keeps_base_schema_when_nondefault_pattern() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "totally_custom",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        );
        assert!(result.is_none());
    }

    #[test]
    fn patches_database_when_base_equals_default_database() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw",
            "warehouse",
            Some("raw"),
            Some("override_db"),
            "model.x.y",
        )
        .expect("should produce a patch");
        assert_eq!(result.schema, "raw");
        assert_eq!(result.database, "override_db");
    }

    #[test]
    fn keeps_base_database_when_nondefault() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw",
            "other_db",
            Some("workflow_42"),
            Some("override_db"),
            "model.x.y",
        )
        .expect("should produce a patch");
        assert_eq!(result.schema, "workflow_42");
        assert_eq!(result.database, "other_db");
    }

    #[test]
    fn returns_none_when_workflow_values_match_base() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw",
            "warehouse",
            Some("raw"),
            Some("warehouse"),
            "model.x.y",
        );
        assert!(result.is_none());
    }

    #[test]
    fn env_database_defaults_to_base_when_unset() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            "raw",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .expect("schema patch alone should still produce a result");
        assert_eq!(result.database, "warehouse");
    }
}
