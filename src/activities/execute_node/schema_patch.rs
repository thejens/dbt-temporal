//! Recompute `this`, `schema`, `database` for nodes whose profile schema or
//! database was overridden per-workflow.
//!
//! `build_run_node_context` builds `this` from `node.base()` — values frozen
//! at worker startup. When a workflow passes env overrides that change the
//! profile's schema/database, the baked-in values are stale; this module
//! reproduces dbt's default `generate_schema_name` / `generate_database_name`
//! pattern to derive the per-workflow values, and refuses to run if the
//! project overrides those macros (we can't replay the macro from outside).

use std::sync::Arc;

use dbt_schemas::schemas::nodes::NodeBaseAttributes;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

/// Result of a schema/database recomputation.
///
/// `None` means no patching is needed (env overrides absent or both fields
/// match base). Otherwise the patched strings are returned alongside flags
/// indicating which one to apply (so callers don't redundantly mutate the
/// jinja context).
pub struct PatchedRelation {
    pub schema: String,
    pub database: String,
    pub patched_schema: bool,
    pub patched_database: bool,
}

/// Compute new schema/database when per-workflow env overrides are in play.
/// Returns an error when the project overrides `generate_schema_name` AND
/// the node's schema doesn't follow the default pattern — we can't safely
/// reproduce the macro without rerunning the resolver, so we refuse rather
/// than silently writing into the wrong schema.
pub fn compute_patched_relation(
    state: &WorkerState,
    base: &NodeBaseAttributes,
    env_schema: Option<&str>,
    env_database: Option<&str>,
    unique_id: &str,
) -> Result<Option<PatchedRelation>, DbtTemporalError> {
    let Some(wf_schema) = env_schema else {
        return Ok(None);
    };

    let wf_database = env_database.unwrap_or(&base.database);
    let default_prefix = format!("{}_", state.default_schema);

    let new_schema = if base.schema == state.default_schema {
        wf_schema.to_string()
    } else if let Some(custom) = base.schema.strip_prefix(&default_prefix) {
        format!("{wf_schema}_{custom}")
    } else if state.has_custom_schema_name_macro {
        // Project overrides generate_schema_name — we can't reproduce its
        // logic from outside, so any patched value risks writing into the
        // wrong schema. Reject the workflow rather than silently using a
        // stale schema; the user can rebuild the worker with the right
        // env or move the override to profiles.yml.
        return Err(DbtTemporalError::Configuration(format!(
            "node {unique_id} can't be safely retargeted: project overrides \
             generate_schema_name and node schema {:?} does not follow the \
             default `<target_schema>_<custom>` pattern, so per-workflow env \
             overrides for schema cannot be honoured. Move the override into \
             profiles.yml, or remove the custom generate_schema_name macro.",
            base.schema
        )));
    } else {
        // Schema doesn't follow default pattern but project uses the
        // default macro — keep base.schema unchanged.
        base.schema.clone()
    };

    let new_database = if base.database == state.default_database {
        wf_database.to_string()
    } else {
        base.database.clone()
    };

    let patched_schema = new_schema != base.schema;
    let patched_database = new_database != base.database;
    if !patched_schema && !patched_database {
        return Ok(None);
    }

    Ok(Some(PatchedRelation {
        schema: new_schema,
        database: new_database,
        patched_schema,
        patched_database,
    }))
}

/// Apply a patched relation to the node Jinja context: rebuilds `this`
/// (the relation that materializations call methods on) plus the bare
/// `schema` and `database` globals.
pub fn apply_patched_relation(
    state: &WorkerState,
    base: &NodeBaseAttributes,
    patch: &PatchedRelation,
    node_context: &mut std::collections::BTreeMap<String, minijinja::Value>,
) {
    let patched_schema = if patch.patched_schema {
        patch.schema.as_str()
    } else {
        &base.schema
    };
    let patched_database = if patch.patched_database {
        patch.database.as_str()
    } else {
        &base.database
    };

    if let Ok(relation) = dbt_adapter::relation::do_create_relation(
        state.resolver_state.adapter_type,
        patched_database.to_string(),
        patched_schema.to_string(),
        Some(base.alias.clone()),
        None,
        base.quoting,
    ) {
        let relation_value =
            dbt_adapter::relation::RelationObject::new(Arc::from(relation)).into_value();
        node_context.insert("this".to_owned(), relation_value);
    }
    if patch.patched_schema {
        node_context.insert("schema".to_owned(), minijinja::Value::from(patched_schema));
    }
    if patch.patched_database {
        node_context.insert("database".to_owned(), minijinja::Value::from(patched_database));
    }
}
