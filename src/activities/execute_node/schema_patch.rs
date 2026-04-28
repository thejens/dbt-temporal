//! Recompute `this`, `schema`, `database` for nodes whose profile schema or
//! database was overridden per-workflow.
//!
//! Two strategies are implemented, chosen based on whether the project overrides
//! `generate_schema_name` / `generate_database_name`:
//!
//! **Default macro** (`has_custom_schema_name_macro = false`): reconstruct the new
//! schema using dbt's default pattern (`<target_schema>_<custom>`) from the
//! profile-rebuilt `target.schema`.
//!
//! **Custom macro** (`has_custom_schema_name_macro = true`): re-execute the macro
//! itself via the already-cloned Jinja env — which has `env_var()` overridden with
//! the workflow env and `target` patched. This matches vanilla dbt's per-run
//! evaluation and correctly handles any macro logic, including direct `env_var()`
//! reads, custom suffixes, and per-model `config(schema=...)` overrides.
//!
//! SQL text patching handles both double-quoted identifiers (PostgreSQL, Snowflake,
//! Redshift) and backtick-quoted identifiers (BigQuery) so that cross-model `ref()`
//! compilations also resolve to the correct per-workflow schemas.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dbt_schemas::schemas::nodes::NodeBaseAttributes;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

/// Result of a schema/database recomputation via the default-pattern strategy.
#[derive(Debug)]
pub struct PatchedRelation {
    pub schema: String,
    pub database: String,
    pub patched_schema: bool,
    pub patched_database: bool,
}

/// Compute new schema/database when per-workflow env overrides are in play
/// and the project uses the **default** `generate_schema_name` macro.
///
/// Only called when `!state.has_custom_schema_name_macro`; the custom-macro
/// path goes through `build_schema_rewrite_map` instead.
pub fn compute_patched_relation(
    state: &WorkerState,
    base: &NodeBaseAttributes,
    env_schema: Option<&str>,
    env_database: Option<&str>,
    unique_id: &str,
) -> Option<PatchedRelation> {
    compute_patched_relation_inner(
        &state.default_schema,
        &state.default_database,
        &base.schema,
        &base.database,
        env_schema,
        env_database,
        unique_id,
    )
}

fn compute_patched_relation_inner(
    default_schema: &str,
    default_database: &str,
    base_schema: &str,
    base_database: &str,
    env_schema: Option<&str>,
    env_database: Option<&str>,
    unique_id: &str,
) -> Option<PatchedRelation> {
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

    let patched_schema = new_schema != base_schema;
    let patched_database = new_database != base_database;
    if !patched_schema && !patched_database {
        return None;
    }

    Some(PatchedRelation {
        schema: new_schema,
        database: new_database,
        patched_schema,
        patched_database,
    })
}

/// Build a schema rewrite map by re-executing `generate_schema_name` with the
/// per-workflow Jinja env for every distinct (resolved-schema, custom-schema-input)
/// combination in the project.
///
/// The Jinja env passed in must already have:
/// - `env_var()` overridden with the workflow's env
/// - `target` patched with the per-workflow schema/database (if profiles.yml uses env_var)
///
/// This matches vanilla dbt's per-run evaluation of `generate_schema_name`.
pub fn build_schema_rewrite_map(
    state: &WorkerState,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
) -> Result<BTreeMap<String, String>, DbtTemporalError> {
    // Collect unique (resolved_schema → custom_schema_input) pairs.
    // `unrendered_config["schema"]` is the `custom_schema_name` argument that dbt
    // passed to `generate_schema_name` at resolve time. None means the model did
    // not set an explicit schema override.
    let mut schema_to_input: BTreeMap<String, (Option<String>, String)> = BTreeMap::new();
    for (_, node) in state.resolver_state.nodes.iter() {
        let base_schema = node.base().schema.clone();
        let custom_schema_name = node
            .base()
            .unrendered_config
            .get("schema")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let name = node.common().name.clone();
        // One representative node per resolved schema is enough.
        schema_to_input
            .entry(base_schema)
            .or_insert((custom_schema_name, name));
    }

    let mut map = BTreeMap::new();
    for (old_schema, (custom_schema_name, node_name)) in &schema_to_input {
        let new_schema =
            render_schema_name_macro(jinja_env, custom_schema_name.as_deref(), node_name).map_err(
                |e| {
                    DbtTemporalError::Compilation(format!(
                        "re-executing generate_schema_name for schema {old_schema:?}: {e:#}"
                    ))
                },
            )?;
        if !new_schema.is_empty() && new_schema != *old_schema {
            map.insert(old_schema.clone(), new_schema);
        }
    }
    Ok(map)
}

/// Render `{{ generate_schema_name(custom_schema_name, node) }}` through the
/// already-configured Jinja env. The env must have env_var() overridden and target
/// patched; this function only builds the argument and fires the call.
fn render_schema_name_macro(
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    custom_schema_name: Option<&str>,
    node_name: &str,
) -> Result<String, anyhow::Error> {
    let arg = custom_schema_name.map_or_else(
        || "none".to_string(),
        |s| {
            // Escape single quotes so the Jinja literal is valid.
            let escaped = s.replace('\'', "\\'");
            format!("'{escaped}'")
        },
    );

    // Build a minimal node object. Most generate_schema_name implementations only
    // read `node.name`; the full node is available at run time so this covers
    // the common case. Macros that need more fields (config, fqn, …) should use
    // the profiles.yml approach where target.schema carries the per-tenant value.
    let mut node_map = BTreeMap::<String, minijinja::Value>::new();
    node_map.insert("name".to_owned(), minijinja::Value::from(node_name));
    let node_val = minijinja::Value::from(node_map);

    let template = format!("{{{{ generate_schema_name({arg}, node) }}}}");
    let mut ctx = BTreeMap::new();
    ctx.insert("node".to_owned(), node_val);

    jinja_env
        .render_str(&template, &ctx, &[])
        .map(|s| s.trim().to_string())
        .map_err(anyhow::Error::from)
}

/// Apply a schema rewrite map to the node Jinja context (`this`, `schema`).
/// Also patches `database` when the workflow supplies a new one.
///
/// Used in the custom-macro path after `build_schema_rewrite_map`.
pub fn apply_schema_map_to_context(
    state: &WorkerState,
    base: &NodeBaseAttributes,
    schema_map: &BTreeMap<String, String>,
    env_database: Option<&str>,
    node_context: &mut BTreeMap<String, minijinja::Value>,
) {
    let new_schema = schema_map.get(&base.schema).map(String::as_str);
    let new_database = env_database.filter(|_| base.database == state.default_database);

    let effective_schema = new_schema.unwrap_or(&base.schema);
    let effective_database = new_database.unwrap_or(&base.database);

    if (new_schema.is_some() || new_database.is_some())
        && let Ok(relation) = dbt_adapter::relation::do_create_relation(
            state.resolver_state.adapter_type,
            effective_database.to_string(),
            effective_schema.to_string(),
            Some(base.alias.clone()),
            None,
            base.quoting,
        )
    {
        let relation_value =
            dbt_adapter::relation::RelationObject::new(Arc::from(relation)).into_value();
        node_context.insert("this".to_owned(), relation_value);
    }
    if new_schema.is_some() {
        node_context.insert("schema".to_owned(), minijinja::Value::from(effective_schema));
    }
    if new_database.is_some() {
        node_context.insert("database".to_owned(), minijinja::Value::from(effective_database));
    }
}

/// Apply a patched relation to the node Jinja context: rebuilds `this`
/// (the relation that materializations call methods on) plus the bare
/// `schema` and `database` globals.
///
/// Used in the default-macro path after `compute_patched_relation`.
pub fn apply_patched_relation(
    state: &WorkerState,
    base: &NodeBaseAttributes,
    patch: &PatchedRelation,
    node_context: &mut BTreeMap<String, minijinja::Value>,
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

/// Patch schema references in compiled SQL using a rewrite map.
///
/// Replaces every quoted occurrence of each old schema with the new schema.
/// Handles both double-quoted identifiers (`"schema"`) used by PostgreSQL,
/// Snowflake, and Redshift, and backtick-quoted identifiers (`` `schema` ``)
/// used by BigQuery.
pub fn patch_sql_with_schema_map(compiled: String, map: &BTreeMap<String, String>) -> String {
    if map.is_empty() {
        return compiled;
    }
    let mut result = compiled;
    for (old, new) in map {
        result = result.replace(&format!("\"{old}\""), &format!("\"{new}\""));
        result = result.replace(&format!("`{old}`"), &format!("`{new}`"));
    }
    result
}

/// Build a database rewrite map for SQL text patching.
///
/// Only replaces the worker-startup default database — nodes configured with
/// non-default databases are left unchanged (matching the relation-level logic
/// in `compute_patched_relation` / `apply_schema_map_to_context`).
pub fn build_database_rewrite_map(
    state: &WorkerState,
    env_database: Option<&str>,
) -> BTreeMap<String, String> {
    let Some(new_db) = env_database else {
        return BTreeMap::new();
    };
    if new_db == state.default_database {
        return BTreeMap::new();
    }
    // Collect all distinct databases in the project; only remap the default.
    let default_databases: BTreeSet<String> = state
        .resolver_state
        .nodes
        .iter()
        .filter(|(_, n)| n.base().database == state.default_database)
        .map(|(_, n)| n.base().database.clone())
        .collect();

    default_databases
        .into_iter()
        .map(|old| (old, new_db.to_string()))
        .collect()
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
        assert!(result.patched_schema);
        assert!(!result.patched_database);
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
        assert!(!result.patched_schema);
        assert!(result.patched_database);
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
        assert!(result.patched_schema);
        assert!(!result.patched_database);
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
        assert!(!result.patched_database);
    }

    // --- patch_sql_with_schema_map ---

    #[test]
    fn patches_double_quoted_schema_in_sql() {
        let mut map = BTreeMap::new();
        map.insert("dbt_dev".to_string(), "dbt_tenant1".to_string());
        let sql = r#"SELECT * FROM "dbt_dev"."orders""#.to_string();
        let patched = patch_sql_with_schema_map(sql, &map);
        assert_eq!(patched, r#"SELECT * FROM "dbt_tenant1"."orders""#);
    }

    #[test]
    fn patches_backtick_quoted_schema_in_sql() {
        let mut map = BTreeMap::new();
        map.insert("dbt_dev".to_string(), "dbt_tenant1".to_string());
        let sql = "SELECT * FROM `my-project`.`dbt_dev`.`orders`".to_string();
        let patched = patch_sql_with_schema_map(sql, &map);
        assert_eq!(patched, "SELECT * FROM `my-project`.`dbt_tenant1`.`orders`");
    }

    #[test]
    fn patches_multiple_schemas() {
        let mut map = BTreeMap::new();
        map.insert("dbt_dev_analytics".to_string(), "dbt_t1_analytics".to_string());
        map.insert("dbt_dev_raw".to_string(), "dbt_t1_raw".to_string());
        let sql =
            r#"SELECT * FROM "dbt_dev_analytics"."a" JOIN "dbt_dev_raw"."b" ON TRUE"#.to_string();
        let patched = patch_sql_with_schema_map(sql, &map);
        assert_eq!(
            patched,
            r#"SELECT * FROM "dbt_t1_analytics"."a" JOIN "dbt_t1_raw"."b" ON TRUE"#
        );
    }

    #[test]
    fn no_op_on_empty_map() {
        let sql = r#"SELECT * FROM "dbt_dev"."orders""#.to_string();
        let patched = patch_sql_with_schema_map(sql.clone(), &BTreeMap::new());
        assert_eq!(patched, sql);
    }

    #[test]
    fn does_not_patch_unquoted_schema_names() {
        // Unquoted identifiers are not touched — only safe to replace quoted ones
        // where we can be sure of identifier boundaries.
        let mut map = BTreeMap::new();
        map.insert("raw".to_string(), "workflow_42".to_string());
        let sql = "SELECT raw FROM raw.orders".to_string();
        let patched = patch_sql_with_schema_map(sql.clone(), &map);
        assert_eq!(patched, sql); // unchanged
    }

    // --- render_schema_name_macro ---

    #[test]
    fn render_schema_name_macro_returns_target_schema_for_none_arg() {
        let mut env = minijinja::Environment::new();
        env.add_function(
            "generate_schema_name",
            |custom: minijinja::Value, _node: minijinja::Value| -> String {
                if custom.is_none() {
                    "default_schema".to_string()
                } else {
                    format!("custom_{custom}")
                }
            },
        );
        let jinja_env = dbt_jinja_utils::jinja_environment::JinjaEnv::new(env);
        let result = render_schema_name_macro(&jinja_env, None, "my_model").unwrap();
        assert_eq!(result, "default_schema");
    }

    #[test]
    fn render_schema_name_macro_passes_custom_schema_name() {
        let mut env = minijinja::Environment::new();
        env.add_function(
            "generate_schema_name",
            |custom: minijinja::Value, _node: minijinja::Value| -> String {
                if custom.is_none() {
                    "default".to_string()
                } else {
                    format!("prefix_{custom}")
                }
            },
        );
        let jinja_env = dbt_jinja_utils::jinja_environment::JinjaEnv::new(env);
        let result = render_schema_name_macro(&jinja_env, Some("marketing"), "my_model").unwrap();
        assert_eq!(result, "prefix_marketing");
    }
}
