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
#[derive(Debug)]
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
    compute_patched_relation_inner(
        &state.default_schema,
        &state.default_database,
        state.has_custom_schema_name_macro,
        &base.schema,
        &base.database,
        env_schema,
        env_database,
        unique_id,
    )
}

#[allow(clippy::too_many_arguments)]
fn compute_patched_relation_inner(
    default_schema: &str,
    default_database: &str,
    has_custom_schema_name_macro: bool,
    base_schema: &str,
    base_database: &str,
    env_schema: Option<&str>,
    env_database: Option<&str>,
    unique_id: &str,
) -> Result<Option<PatchedRelation>, DbtTemporalError> {
    let Some(wf_schema) = env_schema else {
        return Ok(None);
    };

    let wf_database = env_database.unwrap_or(base_database);
    let default_prefix = format!("{default_schema}_");

    let new_schema = if base_schema == default_schema {
        wf_schema.to_string()
    } else if let Some(custom) = base_schema.strip_prefix(&default_prefix) {
        format!("{wf_schema}_{custom}")
    } else if has_custom_schema_name_macro {
        // Project overrides generate_schema_name — we can't reproduce its
        // logic from outside, so any patched value risks writing into the
        // wrong schema. Reject the workflow rather than silently using a
        // stale schema; the user can rebuild the worker with the right
        // env or move the override to profiles.yml.
        return Err(DbtTemporalError::Configuration(format!(
            "node {unique_id} can't be safely retargeted: project overrides \
             generate_schema_name and node schema {base_schema:?} does not follow the \
             default `<target_schema>_<custom>` pattern, so per-workflow env \
             overrides for schema cannot be honoured. Move the override into \
             profiles.yml, or remove the custom generate_schema_name macro."
        )));
    } else {
        // Schema doesn't follow default pattern but project uses the
        // default macro — keep base.schema unchanged.
        base_schema.to_string()
    };

    let new_database = if base_database == default_database {
        wf_database.to_string()
    } else {
        base_database.to_string()
    };

    let patched_schema = new_schema != base_schema;
    let patched_database = new_database != base_database;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn no_patch_when_env_schema_absent() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw",
            "warehouse",
            None,
            None,
            "model.x.y",
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn patches_schema_when_base_equals_default() {
        // Common case: node uses the default schema, workflow overrides it.
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .unwrap()
        .expect("should produce a patch");
        assert_eq!(result.schema, "workflow_42");
        assert_eq!(result.database, "warehouse");
        assert!(result.patched_schema);
        assert!(!result.patched_database);
    }

    #[test]
    fn patches_schema_with_custom_suffix_using_default_pattern() {
        // dbt's default generate_schema_name appends `_<custom>` for nodes
        // configured with a non-default schema. We mirror that pattern.
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw_marts",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .unwrap()
        .expect("should produce a patch");
        assert_eq!(result.schema, "workflow_42_marts");
    }

    #[test]
    fn refuses_to_patch_when_custom_macro_breaks_default_pattern() {
        // Project overrides generate_schema_name AND the node's schema
        // doesn't follow the default pattern → can't safely reproduce.
        let err = compute_patched_relation_inner(
            "raw",
            "warehouse",
            true,
            "totally_custom",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.problematic",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(matches!(err, DbtTemporalError::Configuration(_)));
        assert!(msg.contains("model.x.problematic"));
        assert!(msg.contains("generate_schema_name"));
    }

    #[test]
    fn keeps_base_schema_when_nondefault_pattern_but_no_custom_macro() {
        // Node has a non-default schema and project does NOT override
        // generate_schema_name → assume the user knows what they're doing
        // and keep the base schema unchanged.
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "totally_custom",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .unwrap();
        // Schema stays "totally_custom"; database stays "warehouse";
        // neither field changed → None.
        assert!(result.is_none());
    }

    #[test]
    fn patches_database_when_base_equals_default_database() {
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw",
            "warehouse",
            Some("raw"),
            Some("override_db"),
            "model.x.y",
        )
        .unwrap()
        .expect("should produce a patch");
        // Schema stays the same (env_schema == default), database flips.
        assert_eq!(result.schema, "raw");
        assert_eq!(result.database, "override_db");
        assert!(!result.patched_schema);
        assert!(result.patched_database);
    }

    #[test]
    fn keeps_base_database_when_nondefault() {
        // Node configured with a non-default database → workflow overrides
        // for the default database don't apply.
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw",
            "other_db",
            Some("workflow_42"),
            Some("override_db"),
            "model.x.y",
        )
        .unwrap()
        .expect("should produce a patch");
        assert_eq!(result.schema, "workflow_42");
        assert_eq!(result.database, "other_db"); // unchanged
        assert!(result.patched_schema);
        assert!(!result.patched_database);
    }

    #[test]
    fn returns_none_when_workflow_values_match_base() {
        // env_schema == default_schema → no transformation → no patch.
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw",
            "warehouse",
            Some("raw"),
            Some("warehouse"),
            "model.x.y",
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn env_database_defaults_to_base_when_unset() {
        // env_schema set but env_database not — wf_database falls back to
        // base.database, which means no database patch is applied.
        let result = compute_patched_relation_inner(
            "raw",
            "warehouse",
            false,
            "raw",
            "warehouse",
            Some("workflow_42"),
            None,
            "model.x.y",
        )
        .unwrap()
        .expect("schema patch alone should still produce a result");
        assert_eq!(result.database, "warehouse");
        assert!(!result.patched_database);
    }
}
