//! The compile phase: build the node's Jinja context and render its SQL into
//! it, without touching the warehouse (apart from introspection macros).

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::schemas::telemetry::NodeType;

use crate::error::DbtTemporalError;

use super::super::node_helpers::{EphemeralDirs, ephemeral_context_builder};
use super::super::node_serialization::{build_agate_table, get_node_config_yml, get_sql_header};
use super::super::render_env::{RenderEnv, RenderOverrides};
use super::context::{CompiledNode, NodeRun};
use super::raw_sql::resolve_raw_sql;
use super::schema_patch::{apply_relation_to_context, resolve_relations};
use super::schema_patcher::has_env_var_in_config_schema_or_database;
use super::sql_render::SqlRendering;
use super::sql_rewrite::RelationRewrite;
use super::unit_test;
use super::workspace::ActivityWorkspace;
use super::yml_to_value::yml_value_to_minijinja_with_jinja;

/// Build the node's context and render its SQL into it.
pub(super) fn compile_node(
    run: &NodeRun<'_>,
    overrides: &RenderOverrides<'_>,
    render_env: &RenderEnv,
    defer_nodes: Option<&dbt_schemas::schemas::Nodes>,
) -> Result<CompiledNode, anyhow::Error> {
    let jinja_env = &render_env.jinja_env;
    let base_context = build_base_context(run.state, defer_nodes, namespace_keys(jinja_env));

    let (mut node_context, result_store, workspace) = build_node_context(run, &base_context)?;
    render_node_sql(run, overrides, render_env, &base_context, &workspace, &mut node_context)?;

    // `workspace` rides along: its temp dir must outlive the materialization.
    Ok(CompiledNode {
        node_context,
        result_store,
        _workspace: workspace,
    })
}

/// Apply the workflow's overrides to the node context and render the node's SQL
/// into it.
fn render_node_sql(
    run: &NodeRun<'_>,
    overrides: &RenderOverrides<'_>,
    render_env: &RenderEnv,
    base_context: &BTreeMap<String, minijinja::Value>,
    workspace: &ActivityWorkspace,
    node_context: &mut BTreeMap<String, minijinja::Value>,
) -> Result<(), anyhow::Error> {
    let jinja_env = &render_env.jinja_env;

    // Ephemeral models this node pulls in render with their own node context,
    // built from the same base context and scratch IO as this node's.
    let ephemeral_context =
        ephemeral_context_builder(run.state, base_context, run.base.adapter, &workspace.io_args);

    apply_microbatch_window(run, node_context)?;

    let relation_rewrite = apply_relation_overrides(run, overrides, render_env, node_context)?;

    // Resolve raw SQL: build_run_node_context does NOT populate the top-level
    // "sql" context variable — that's the caller's responsibility. The
    // materialization template uses {{ sql }} as the compiled model query
    // (e.g. in `get_create_view_as_sql(target_relation, sql)`). Unit tests
    // have no raw SQL of their own (their path is the defining YAML); their
    // input SQL is assembled below from the tested model + fixtures.
    let raw_sql_result = if run.resource_type == NodeType::UnitTest || run.measures_freshness {
        Ok(String::new())
    } else {
        resolve_raw_sql(run.state, run.common, run.resource_type)
    };

    inject_generic_test_kwargs(run, jinja_env, node_context)?;
    reject_env_var_schema_config(run, render_env, &raw_sql_result)?;

    let rendering = SqlRendering {
        jinja_env,
        dirs: EphemeralDirs {
            in_dir: &run.state.io_args.in_dir,
            ephemeral_dir: &workspace.ephemeral_dir,
            context_for: &ephemeral_context,
        },
        relation_rewrite: &relation_rewrite,
        io_args: &workspace.io_args,
    };
    if run.resource_type == NodeType::UnitTest {
        rendering.inject_unit_test_sql(run, node_context)?;
    }
    rendering.render_raw_sql(run, raw_sql_result, node_context)
}

/// Build the compile+run base context.
///
/// The `store_result` / `load_result` / `store_raw_result` closures this
/// context carries are placeholders: `build_run_node_context` overlays its own
/// `ResultStore` onto them and hands that store back, so the run path must read
/// results from the store it returns rather than binding one here.
fn build_base_context(
    state: &crate::worker_state::WorkerState,
    defer_nodes: Option<&dbt_schemas::schemas::Nodes>,
    namespace_keys: Vec<String>,
) -> BTreeMap<String, minijinja::Value> {
    dbt_jinja_utils::phases::build_operation_context_btreemap(
        Arc::clone(&state.resolver_state.node_resolver),
        &state.resolver_state.root_project_name,
        &state.resolver_state.nodes,
        defer_nodes,
        Arc::clone(&state.resolver_state.runtime_config),
        namespace_keys,
        None,
    )
}

/// The macro namespaces the Jinja environment knows, which the operation
/// context needs by name.
fn namespace_keys(jinja_env: &JinjaEnv) -> Vec<String> {
    jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default()
}

/// Build the full run-phase node context, together with the scratch workspace
/// its file paths point into and the store its result closures write to.
///
/// The adapter here shapes `this` and the column data types in the context,
/// so it must be the node's own — the project-wide default would hand a node
/// on another adapter a relation rendered in the wrong dialect.
fn build_node_context(
    run: &NodeRun<'_>,
    base_context: &BTreeMap<String, minijinja::Value>,
) -> Result<
    (
        BTreeMap<String, minijinja::Value>,
        dbt_adapter::load_store::ResultStore,
        ActivityWorkspace,
    ),
    anyhow::Error,
> {
    let state = run.state;
    let unique_id = run.unique_id;
    let rt = run.resource_type;

    // Serialize the node config for the deprecated_config parameter.
    let mut deprecated_config = get_node_config_yml(&state.resolver_state.nodes, unique_id, rt)?;

    // The `unit` materialization reads the expected fixture from
    // config.get('expected_rows') / config.get('expected_sql').
    if rt == NodeType::UnitTest
        && let Some(unit) = state.resolver_state.nodes.unit_tests.get(unique_id)
    {
        unit_test::inject_expected_config(&mut deprecated_config, unit, &state.io_args.in_dir)?;
    }

    // Build agate_table for seeds (loads CSV data — uses in_dir only).
    let agate_table =
        build_agate_table(&state.resolver_state.nodes, unique_id, rt, &state.io_args)?;

    // Extract sql_header from model config (only models have this field).
    let sql_header = get_sql_header(&state.resolver_state.nodes, unique_id, rt);

    let workspace = ActivityWorkspace::new(state, run.node, &run.input.invocation_id)?;

    // The returned store is the one the context's `store_result`/`load_result`
    // closures write to.
    let (node_context, result_store) = dbt_jinja_utils::phases::run::build_run_node_context(
        run.node,
        &deprecated_config,
        run.base.adapter,
        agate_table,
        base_context,
        &workspace.io_args,
        dbt_telemetry::ExecutionPhase::Run,
        sql_header,
        state.packages.clone(),
    );
    Ok((node_context, result_store, workspace))
}

/// Microbatch: replace ref() and source() with time-window-aware versions when
/// event_time_start/end are provided. The materialisation template uses these to
/// filter upstream models/sources to the batch window. Dependency validation is
/// skipped (new_unvalidated) because the node was already validated at planning.
fn apply_microbatch_window(
    run: &NodeRun<'_>,
    node_context: &mut BTreeMap<String, minijinja::Value>,
) -> Result<(), anyhow::Error> {
    let input = run.input;
    let state = run.state;
    let (Some(start_str), Some(end_str)) = (&input.event_time_start, &input.event_time_end) else {
        return Ok(());
    };
    let event_time_start: chrono::DateTime<chrono::Utc> = start_str
        .parse()
        .with_context(|| format!("parsing event_time_start: {start_str}"))?;
    let event_time_end: chrono::DateTime<chrono::Utc> = end_str
        .parse()
        .with_context(|| format!("parsing event_time_end: {end_str}"))?;

    // Built once at startup rather than rescanned per node — the project's
    // event-time columns cannot change while the worker lives.
    let microbatch_ctx = dbt_jinja_utils::phases::MicrobatchRefContext::new(
        event_time_start,
        event_time_end,
        Arc::clone(&state.event_time_columns),
    );
    let mb_ref = dbt_jinja_utils::phases::RefFunction::new_with_microbatch_context(
        Arc::clone(&state.resolver_state.node_resolver),
        run.common.package_name.clone(),
        Arc::clone(&state.resolver_state.runtime_config),
        dbt_jinja_utils::phases::compile::DependencyValidationConfig::new_unvalidated(),
        microbatch_ctx.clone(),
        run.unique_id.to_owned(),
    );
    let mb_source = dbt_jinja_utils::phases::SourceFunction::new_with_microbatch_context(
        Arc::clone(&state.resolver_state.node_resolver),
        run.common.package_name.clone(),
        Arc::clone(&state.resolver_state.runtime_config),
        microbatch_ctx,
    );
    node_context.insert("ref".to_string(), minijinja::Value::from_object(mb_ref));
    node_context.insert("source".to_string(), minijinja::Value::from_object(mb_source));
    node_context.insert(
        "__dbt_microbatch_event_time_start__".to_string(),
        minijinja::Value::from(event_time_start.to_rfc3339()),
    );
    node_context.insert(
        "__dbt_microbatch_event_time_end__".to_string(),
        minijinja::Value::from(event_time_end.to_rfc3339()),
    );
    Ok(())
}

/// Resolve where this node's relation — and every relation its SQL can
/// name — lives for this workflow, then patch `this`, `schema` and
/// `database` in its Jinja context.
///
/// The compiled text needs the same treatment separately: the startup
/// manifest baked the startup schemas into every upstream `ref()`, and a
/// materialization interpolates that text rather than re-deriving it from
/// `this`. The returned rewrite is what that text is passed through.
fn apply_relation_overrides(
    run: &NodeRun<'_>,
    overrides: &RenderOverrides<'_>,
    render_env: &RenderEnv,
    node_context: &mut BTreeMap<String, minijinja::Value>,
) -> Result<RelationRewrite, anyhow::Error> {
    let resolved = resolve_relations(
        run.state,
        &render_env.jinja_env,
        &run.input.invocation_id,
        overrides,
        run.unique_id,
        render_env.env_schema.as_deref(),
        render_env.env_database.as_deref(),
    )?;
    if let Some(ref own) = resolved.own {
        apply_relation_to_context(run.base, own, node_context)
            .map_err(|e| DbtTemporalError::Compilation(format!("{e:#}")))?;
    }
    Ok(resolved.rewrite)
}

/// For generic tests, inject _dbt_generic_test_kwargs from test metadata.
/// The primary path uses generated SQL with inlined kwargs (from test_sql_cache),
/// but the raw_code fallback path may reference **_dbt_generic_test_kwargs.
fn inject_generic_test_kwargs(
    run: &NodeRun<'_>,
    jinja_env: &JinjaEnv,
    node_context: &mut BTreeMap<String, minijinja::Value>,
) -> Result<(), anyhow::Error> {
    if run.resource_type == NodeType::Test
        && let Some(test) = run.state.resolver_state.nodes.tests.get(run.unique_id)
        && let Some(ref meta) = test.__test_attr__.test_metadata
    {
        let kwargs_map = build_test_kwargs_map(&meta.kwargs, jinja_env, node_context)
            .map_err(|e| DbtTemporalError::Compilation(format!("{e:#}")))?;
        node_context
            .insert("_dbt_generic_test_kwargs".to_owned(), minijinja::Value::from(kwargs_map));
    }
    Ok(())
}

/// Detect unsupported pattern: config(schema=env_var(...)) or config(database=env_var(...))
/// with per-workflow env overrides. The config env_var() is evaluated once at resolution
/// time — per-workflow overrides won't change it, leading to silent stale schemas.
/// Error early so users switch to the supported profiles.yml approach.
fn reject_env_var_schema_config(
    run: &NodeRun<'_>,
    render_env: &RenderEnv,
    raw_sql_result: &Result<String, (std::path::PathBuf, std::io::Error)>,
) -> Result<(), anyhow::Error> {
    if render_env.env_schema.is_some()
        && let Ok(raw_sql) = raw_sql_result
        && has_env_var_in_config_schema_or_database(raw_sql)
    {
        return Err(DbtTemporalError::Configuration(format!(
            "node {} uses env_var() inside config(schema=...) or config(database=...). \
             This is not supported with per-workflow env overrides because the config value is \
             evaluated once at worker startup. Use env_var() in profiles.yml to set the base \
             schema/database instead — that path is fully supported.",
            run.unique_id
        ))
        .into());
    }
    Ok(())
}

/// Materialise generic-test `_dbt_generic_test_kwargs` for the node context.
///
/// The kwargs map is on `TestMetadata`. Each YAML value is converted through
/// `yml_value_to_minijinja_with_jinja` so brace-quoted Jinja expressions
/// (e.g. `"{{ get_where_subquery(ref('m')) }}"`) are evaluated against the
/// current context — matching dbt-fusion's generic-test executor.
fn build_test_kwargs_map(
    meta_kwargs: &BTreeMap<String, dbt_yaml::Value>,
    jinja_env: &JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> Result<BTreeMap<String, minijinja::Value>, anyhow::Error> {
    meta_kwargs
        .iter()
        .map(|(k, v)| {
            let value = yml_value_to_minijinja_with_jinja(v, jinja_env, node_context)
                .with_context(|| format!("test kwarg {k}"))?;
            Ok((k.clone(), value))
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- empty_raw_sql_dispatch / raw_sql_read_error_dispatch ---

    fn empty_jinja_env() -> JinjaEnv {
        JinjaEnv::new(minijinja::Environment::new())
    }

    #[test]
    fn build_test_kwargs_map_passes_through_scalar_values() {
        let mut kwargs: BTreeMap<String, dbt_yaml::Value> = BTreeMap::new();
        kwargs.insert("threshold".to_string(), dbt_yaml::from_str("42").unwrap());
        kwargs.insert("name".to_string(), dbt_yaml::from_str("\"customer_id\"").unwrap());

        let env = empty_jinja_env();
        let ctx = BTreeMap::new();
        let result = build_test_kwargs_map(&kwargs, &env, &ctx).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("threshold").unwrap().to_string(), "42");
        assert_eq!(result.get("name").unwrap().as_str(), Some("customer_id"));
    }

    #[test]
    fn build_test_kwargs_map_evaluates_braced_jinja_expressions() {
        // dbt-fusion's generic-test executor expects `"{{ ... }}"` strings to
        // be evaluated against the node context. We mirror that here.
        let mut kwargs: BTreeMap<String, dbt_yaml::Value> = BTreeMap::new();
        kwargs.insert("value".to_string(), dbt_yaml::from_str("\"{{ x + 1 }}\"").unwrap());

        let env = empty_jinja_env();
        let mut ctx = BTreeMap::new();
        ctx.insert("x".to_string(), minijinja::Value::from(7));
        let result = build_test_kwargs_map(&kwargs, &env, &ctx).unwrap();

        assert_eq!(result.get("value").unwrap().to_string(), "8");
    }

    #[test]
    fn build_test_kwargs_map_returns_empty_for_empty_input() {
        let env = empty_jinja_env();
        let ctx = BTreeMap::new();
        let result = build_test_kwargs_map(&BTreeMap::new(), &env, &ctx).unwrap();
        assert!(result.is_empty());
    }
}
