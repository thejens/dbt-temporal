mod raw_sql;
mod schema_patch;
mod schema_patcher;
mod yml_to_value;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use dbt_adapter::load_store::ResultStore;
use dbt_schemas::schemas::telemetry::NodeType;
use raw_sql::resolve_raw_sql;
use schema_patch::{apply_patched_relation, compute_patched_relation};
use schema_patcher::has_env_var_in_config_schema_or_database;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::{info, warn};
use yml_to_value::yml_value_to_minijinja_with_jinja;

use crate::error::DbtTemporalError;
use crate::project_registry::ProjectRegistry;
use crate::types::{NodeExecutionInput, NodeExecutionResult, NodeStatus, TimingEntry};

use super::DbtActivities;
use super::heartbeat;
use super::node_helpers::{
    extract_adapter_response, extract_test_failures, inject_ephemeral_ctes, patch_target_global,
    render_materialization,
};
use super::node_serialization::{
    build_agate_table, get_node_config_yml, get_node_yml, get_sql_header,
};

/// Execute node activity — outer wrapper that handles errors, cancellation,
/// and periodic heartbeating.
///
/// Called from `DbtActivities::execute_node`.
pub async fn execute_node_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: NodeExecutionInput,
) -> Result<NodeExecutionResult, ActivityError> {
    let unique_id = input.unique_id.clone();
    let project = input.project.clone();
    tokio::select! {
        result = execute_node_inner(activities, input) => {
            match result {
                Ok(result) => Ok(result),
                Err(e) => {
                    tracing::error!(node = %unique_id, error = %e, "activity failed");
                    let dbt_err = downcast_or_wrap_as_adapter(e);
                    let patterns = registry_non_retryable_patterns(&activities.registry, &project);
                    Err(classify_for_temporal(&dbt_err, patterns.as_deref().unwrap_or(&[])))
                }
            }
        }
        () = ctx.cancelled() => {
            info!(node = %unique_id, "activity cancelled");
            Err(ActivityError::cancelled())
        }
        // Never resolves — keeps the UI's last-heartbeat fresh and lets the
        // server's heartbeat_timeout reschedule on a fresh worker if this one
        // dies. Loses the select! race to the two real branches above.
        never = heartbeat::heartbeat_loop(&ctx) => match never {},
    }
}

/// Whether to surface an error to Temporal as retryable or not.
#[derive(Debug, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry within the activity's retry policy.
    Retry,
    /// Skip the policy — the error is permanent.
    NoRetry,
}

/// Decide whether a `DbtTemporalError` should be surfaced to Temporal as retryable.
///
/// Adapter errors are retryable by default unless the user-supplied pattern
/// list matches the error's display string. All other variants — Compilation,
/// Configuration, ProjectNotFound, TestFailure — are permanent.
pub fn decide_retry(
    err: &DbtTemporalError,
    non_retryable_patterns: &[regex::Regex],
) -> RetryDecision {
    if !err.is_retryable() {
        return RetryDecision::NoRetry;
    }
    if non_retryable_patterns.is_empty() {
        return RetryDecision::Retry;
    }
    let msg = err.to_string();
    if crate::error::matches_error_patterns(&msg, non_retryable_patterns) {
        tracing::info!(
            error = %msg,
            "adapter error matched non-retryable pattern, suppressing retry"
        );
        RetryDecision::NoRetry
    } else {
        RetryDecision::Retry
    }
}

/// Wrap an `anyhow::Error` as an `ActivityError` whose retry classification
/// follows from `decide_retry`.
fn classify_for_temporal(
    dbt_err: &DbtTemporalError,
    non_retryable_patterns: &[regex::Regex],
) -> ActivityError {
    let source = anyhow::anyhow!("{dbt_err}").into();
    match decide_retry(dbt_err, non_retryable_patterns) {
        RetryDecision::Retry => ActivityError::Retryable {
            source,
            explicit_delay: None,
        },
        RetryDecision::NoRetry => ActivityError::NonRetryable(source),
    }
}

/// Try to recover the original `DbtTemporalError` from an `anyhow::Error`,
/// falling back to wrapping the error as `Adapter` (the retryable default).
fn downcast_or_wrap_as_adapter(err: anyhow::Error) -> DbtTemporalError {
    match err.downcast::<DbtTemporalError>() {
        Ok(d) => d,
        Err(other) => DbtTemporalError::Adapter(other),
    }
}

/// Look up the user-configured non-retryable patterns for a project. Returns
/// `None` when the project isn't registered (during tests or shutdown) so the
/// caller can fall back to "all adapter errors retry".
fn registry_non_retryable_patterns(
    registry: &Arc<ProjectRegistry>,
    project: &str,
) -> Option<Vec<regex::Regex>> {
    let state = registry.get(Some(project)).ok()?;
    Some(state.non_retryable_error_patterns.clone())
}

/// Patch refs in compiled SQL when a per-workflow env override changed the
/// profile schema. Replaces quoted occurrences of the worker-startup default
/// schema with the workflow's schema; otherwise returns the input unchanged.
///
/// `env_schema = None` (no override active) and `env_schema == default_schema`
/// (override matches startup) are both no-ops.
fn patch_compiled_schema(
    compiled: String,
    env_schema: Option<&str>,
    default_schema: &str,
) -> String {
    let Some(wf_schema) = env_schema else {
        return compiled;
    };
    if wf_schema == default_schema {
        return compiled;
    }
    compiled.replace(&format!("\"{default_schema}\""), &format!("\"{wf_schema}\""))
}

/// Pick the materialization name to dispatch on. Seeds are forced to "seed"
/// because dbt-fusion still reports their `base.materialized` as "table"
/// (issue #1345); without the override `materialization_table_default` would
/// be invoked with empty SQL and produce invalid CREATE statements.
fn select_materialization_name(rt: NodeType, base_materialized: &str) -> String {
    if rt == NodeType::Seed {
        "seed".to_string()
    } else {
        base_materialized.to_lowercase()
    }
}

/// True if the node is one we expect `create_schema(this)` to be called for
/// before materialization. Tests and operations don't get a schema-create
/// pass — they only read.
const fn is_create_schema_eligible(rt: NodeType) -> bool {
    matches!(rt, NodeType::Model | NodeType::Seed | NodeType::Snapshot)
}

/// True if a node of this resource_type + materialization should produce an
/// adapter response. Used as a no-op guard: an empty adapter response on a
/// node that *should* execute SQL is a sign that `statement('main')` never
/// ran (likely a buggy materialization template).
const fn expects_adapter_response(rt: NodeType, materialization: &str) -> bool {
    match rt {
        // Ephemeral models never execute against the warehouse — they're
        // inlined as CTEs in their downstream consumer's SQL.
        NodeType::Model => !matches!(materialization.as_bytes(), b"ephemeral"),
        NodeType::Seed | NodeType::Snapshot | NodeType::Test => true,
        _ => false,
    }
}

/// Pick the final `compiled_code` for the result. Prefer the SQL the context
/// captured during render (the canonical compile output); fall back to the
/// stripped rendered output only when the context didn't get a `sql` set.
fn finalize_compiled_code(compiled_sql: Option<String>, rendered: &str) -> Option<String> {
    compiled_sql.or_else(|| {
        let trimmed = rendered.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Build the compile + execute `TimingEntry` pair returned to the workflow.
fn build_timing_entries(
    compile_start: chrono::DateTime<chrono::Utc>,
    compile_end: chrono::DateTime<chrono::Utc>,
    execute_start: chrono::DateTime<chrono::Utc>,
    execute_end: chrono::DateTime<chrono::Utc>,
) -> Vec<TimingEntry> {
    vec![
        TimingEntry {
            name: "compile".to_string(),
            started_at: compile_start.to_rfc3339(),
            completed_at: compile_end.to_rfc3339(),
        },
        TimingEntry {
            name: "execute".to_string(),
            started_at: execute_start.to_rfc3339(),
            completed_at: execute_end.to_rfc3339(),
        },
    ]
}

/// Decide what to do when raw SQL came back empty. Models, snapshots, and
/// tests *must* have a non-empty body — empty SQL there is a real bug.
/// Hooks and operations are allowed to compile to nothing.
fn empty_raw_sql_dispatch(rt: NodeType, unique_id: &str) -> Result<(), DbtTemporalError> {
    match rt {
        NodeType::Model | NodeType::Snapshot | NodeType::Test => {
            Err(DbtTemporalError::Compilation(format!("raw SQL is empty for {unique_id}")))
        }
        _ => Ok(()),
    }
}

/// Decide what to do when reading raw SQL from disk failed. Same shape as
/// `empty_raw_sql_dispatch` — hard error for nodes that need SQL, soft for
/// hooks/operations.
fn raw_sql_read_error_dispatch(
    rt: NodeType,
    unique_id: &str,
    path: &std::path::Path,
    err: &std::io::Error,
) -> Result<(), DbtTemporalError> {
    match rt {
        NodeType::Model | NodeType::Snapshot | NodeType::Test => {
            Err(DbtTemporalError::Compilation(format!(
                "reading raw SQL for {unique_id} at {}: {err:#}",
                path.display()
            )))
        }
        _ => Ok(()),
    }
}

/// Write a cached SQL string to disk under `dest`, creating parent dirs as
/// needed. No-op when `cache` doesn't contain `node_path`.
///
/// Used at activity startup to seed the per-activity temp `out_dir` with the
/// SQL captured during the worker's startup resolve — so materialization
/// macros that read `target/compiled/<path>` see the right content even
/// though each activity gets a fresh temp dir.
fn write_cached_sql(
    cache: &BTreeMap<String, String>,
    node_path: &str,
    dest: &std::path::Path,
) -> std::io::Result<()> {
    let Some(sql) = cache.get(node_path) else {
        return Ok(());
    };
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(dest, sql)
}

/// Parse the invocation ID from the workflow input. Rejects malformed IDs
/// with an actionable error message — falling back to the worker-state
/// invocation_id would silently mis-tag artifacts and run-results under a
/// different run.
fn parse_invocation_id(raw: &str) -> Result<uuid::Uuid, anyhow::Error> {
    raw.parse()
        .map_err(|e| anyhow::anyhow!("invalid invocation_id {raw:?}: {e}"))
}

#[allow(clippy::too_many_lines, clippy::unused_async)]
// Sequential adapter interaction with setup, execution, and result extraction.
// Kept async so tokio::select! in execute_node_outer can poll it against
// ctx.cancelled() and the heartbeat ticker.
async fn execute_node_inner(
    activities: &DbtActivities,
    input: NodeExecutionInput,
) -> Result<NodeExecutionResult, anyhow::Error> {
    let state = activities.registry.get(Some(&input.project))?;

    let unique_id = &input.unique_id;

    // Look up the node in the resolver state.
    let node = state
        .resolver_state
        .nodes
        .get_node(unique_id)
        .ok_or_else(|| DbtTemporalError::ProjectNotFound(format!("node not found: {unique_id}")))?;

    // Fail early if this node had resolution errors (e.g. broken ref/source).
    if state
        .resolver_state
        .nodes_with_resolution_errors
        .contains(unique_id)
    {
        return Err(DbtTemporalError::Compilation(format!(
            "node {unique_id} has unresolved compilation errors"
        ))
        .into());
    }

    let common = node.common();
    let base = node.base();
    let rt = node.resource_type();
    info!(node = %unique_id, resource_type = rt.as_str_name(), "executing node");

    let start_instant = std::time::Instant::now();

    // --- COMPILE PHASE ---
    let compile_start = chrono::Utc::now();

    // Clone the Jinja environment for this execution and configure for run phase.
    let mut jinja_env = (*state.jinja_env).clone();

    // Override env_var() with per-workflow environment variables so parallel
    // workflows get isolated env — no shared process-level env vars.
    if !input.env.is_empty() {
        let env_overrides = Arc::new(input.env.clone());
        jinja_env.env.add_func_func("env_var", move |state, args| {
            // dbt_jinja_utils::LookupFn = dyn Fn(&str) -> Option<Value>, which is
            // implicitly 'static — the inner closure must own its captures, so we
            // bump the Arc refcount per call.
            let map = Arc::clone(&env_overrides);
            let lookup = move |key: &str| -> Option<minijinja::Value> {
                // Value::from(&str) uses minijinja's inline SmallStr where it fits,
                // avoiding the second alloc that Value::from(String) would do.
                map.get(key).map(|v| minijinja::Value::from(v.as_str()))
            };
            dbt_jinja_utils::env_var(false, Some(&lookup), state, args)
        });
    }

    // Use per-workflow adapter engine if env overrides require it (profiles.yml uses env_var),
    // otherwise use the shared adapter engine from worker startup.
    // When rebuilding, capture the per-workflow schema/database for relation patching.
    //
    // _rebuild_guard keeps the RebuildResult (and its CancellationTokenSource) alive
    // for the duration of the activity. The engine's cancellation token holds a Weak
    // ref to the source; without this guard, the source is dropped and every SQL
    // statement is immediately cancelled.
    #[allow(unused_assignments, clippy::collection_is_never_read)]
    // _rebuild_guard is a drop guard, not read
    let mut _rebuild_guard = None;
    let (adapter_engine, env_schema, env_database) =
        if !input.env.is_empty() && state.profile_uses_env_vars {
            let result = crate::worker::rebuild_adapter_engine_with_env(
                state,
                input.target.as_deref(),
                &input.env,
            )
            .map_err(|e| {
                DbtTemporalError::Configuration(format!("rebuilding adapter engine: {e:#}"))
            })?;
            let engine = Arc::clone(&result.engine);
            let schema = result.schema.clone();
            let database = result.database.clone();
            _rebuild_guard = Some(result);
            (engine, Some(schema), Some(database))
        } else {
            (Arc::clone(&state.adapter_engine), None, None)
        };

    let adapter_impl = dbt_adapter::AdapterImpl::new(adapter_engine, None);
    let adapter = Arc::new(dbt_adapter::Adapter::new(
        Arc::new(adapter_impl),
        None, // time_machine
        state.cancellation_source.token(),
    ));

    // Configure the Jinja environment for compile+run phase.
    // This sets execute=true context where adapter.* calls hit the real DB.
    dbt_jinja_utils::phases::configure_compile_and_run_jinja_environment(&mut jinja_env, adapter);

    // Patch `target` / `env` Jinja globals with per-workflow schema/database so all macros
    // (generate_schema_name, materializations, custom macros) see the correct values.
    if let (Some(wf_schema), Some(wf_database)) = (&env_schema, &env_database) {
        patch_target_global(&mut jinja_env, wf_schema, wf_database, input.target.as_deref());
    }

    // Get namespace keys from the Jinja macro namespace registry.
    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default();

    // Build the base context for compile+run phase.
    let mut base_context = dbt_jinja_utils::phases::build_compile_and_run_base_context(
        Arc::clone(&state.resolver_state.node_resolver),
        &state.resolver_state.root_project_name,
        &state.resolver_state.nodes,
        Arc::clone(&state.resolver_state.runtime_config),
        namespace_keys,
    );

    // Override store_result/load_result/store_raw_result with our own ResultStore so we can
    // extract adapter response metadata after rendering completes.
    let result_store = ResultStore::default();
    base_context.insert(
        "store_result".to_owned(),
        minijinja::Value::from_function(result_store.store_result()),
    );
    base_context.insert(
        "load_result".to_owned(),
        minijinja::Value::from_function(result_store.load_result()),
    );
    base_context.insert(
        "store_raw_result".to_owned(),
        minijinja::Value::from_function(result_store.store_raw_result()),
    );

    // Serialize the node to YmlValue for the model parameter.
    let model_yml = get_node_yml(&state.resolver_state.nodes, unique_id, rt)?;

    // Serialize the node config for the deprecated_config parameter.
    let deprecated_config = get_node_config_yml(&state.resolver_state.nodes, unique_id, rt);

    // Build agate_table for seeds (loads CSV data — uses in_dir only).
    let agate_table =
        build_agate_table(&state.resolver_state.nodes, unique_id, rt, &state.io_args)?;

    // Extract sql_header from model config (only models have this field).
    let sql_header = get_sql_header(&state.resolver_state.nodes, unique_id, rt);

    // Create an ephemeral output dir for this activity so concurrent workflows
    // and cross-worker dispatch don't share target/.
    let temp_dir = tempfile::tempdir().context("creating temp dir for activity")?;
    let temp_out = temp_dir.path().to_path_buf();

    // Per-activity ephemeral CTE persistence dir. dbt-fusion's
    // `inject_and_persist_ephemeral_models` writes per-ephemeral cumulative CTE
    // chains here; sharing the dir across activities would race on those files.
    let ephemeral_dir = temp_dir.path().join("ephemeral");
    std::fs::create_dir_all(&ephemeral_dir)
        .with_context(|| format!("creating ephemeral dir {}", ephemeral_dir.display()))?;

    let node_path = common.path.to_string_lossy().to_string();

    // Seed the temp out_dir with cached compiled / snapshot SQL — see
    // `write_cached_sql` for why this is per-activity rather than shared.
    write_cached_sql(
        &state.compiled_sql_cache,
        &node_path,
        &temp_out.join("compiled").join(&common.path),
    )?;
    if rt == NodeType::Snapshot {
        write_cached_sql(&state.snapshot_sql_cache, &node_path, &temp_out.join(&common.path))?;
    }

    let invocation_id = parse_invocation_id(&input.invocation_id)?;
    let io_args = dbt_common::io_args::IoArgs {
        in_dir: state.io_args.in_dir.clone(),
        out_dir: temp_out,
        invocation_id,
        ..Default::default()
    };

    // Build the full run-phase node context.
    let mut node_context = dbt_jinja_utils::phases::run::build_run_node_context(
        model_yml,
        common,
        base,
        &deprecated_config,
        state.resolver_state.adapter_type,
        agate_table,
        &base_context,
        &io_args,
        rt,
        sql_header,
        state.packages.clone(),
    );

    // build_run_node_context creates its own ResultStore (via extend_base_context_stateful_fn),
    // overwriting ours. Re-inject our activity-scoped store so adapter_response extraction
    // reads the same store that materialization macros write to.
    node_context.insert(
        "store_result".to_owned(),
        minijinja::Value::from_function(result_store.store_result()),
    );
    node_context.insert(
        "load_result".to_owned(),
        minijinja::Value::from_function(result_store.load_result()),
    );
    node_context.insert(
        "store_raw_result".to_owned(),
        minijinja::Value::from_function(result_store.store_raw_result()),
    );

    // Inject TARGET_PACKAGE_NAME — required by ConfiguredVar (the var() function) to resolve
    // project-scoped variables. The compile phase sets this but build_run_node_context doesn't.
    node_context.insert(
        "TARGET_PACKAGE_NAME".to_owned(),
        minijinja::Value::from(common.package_name.clone()),
    );

    // Patch `this`, `database`, `schema` when per-workflow env overrides
    // changed the profile schema/database. `build_run_node_context` built
    // `this` from `node.base()`, which reflects worker-startup values.
    if let Some(patch) = compute_patched_relation(
        state,
        base,
        env_schema.as_deref(),
        env_database.as_deref(),
        unique_id,
    )? {
        apply_patched_relation(state, base, &patch, &mut node_context);
    }

    // Resolve raw SQL: build_run_node_context does NOT populate the top-level
    // "sql" context variable — that's the caller's responsibility. The
    // materialization template uses {{ sql }} as the compiled model query
    // (e.g. in `get_create_view_as_sql(target_relation, sql)`).
    let raw_sql_result = resolve_raw_sql(state, common, rt, &node_path);

    // For generic tests, inject _dbt_generic_test_kwargs from test metadata.
    // The primary path uses generated SQL with inlined kwargs (from test_sql_cache),
    // but the raw_code fallback path may reference **_dbt_generic_test_kwargs.
    if rt == NodeType::Test
        && let Some(test) = state.resolver_state.nodes.tests.get(unique_id)
        && let Some(ref meta) = test.__test_attr__.test_metadata
    {
        let kwargs_map: BTreeMap<String, minijinja::Value> = meta
            .kwargs
            .iter()
            .map(|(k, v)| {
                (k.clone(), yml_value_to_minijinja_with_jinja(v, &jinja_env, &node_context))
            })
            .collect();
        node_context
            .insert("_dbt_generic_test_kwargs".to_owned(), minijinja::Value::from(kwargs_map));
    }

    // Detect unsupported pattern: config(schema=env_var(...)) or config(database=env_var(...))
    // with per-workflow env overrides. The config env_var() is evaluated once at resolution
    // time — per-workflow overrides won't change it, leading to silent stale schemas.
    // Error early so users switch to the supported profiles.yml approach.
    if env_schema.is_some()
        && let Ok(ref raw_sql) = raw_sql_result
        && has_env_var_in_config_schema_or_database(raw_sql)
    {
        return Err(DbtTemporalError::Configuration(format!(
            "node {unique_id} uses env_var() inside config(schema=...) or config(database=...). \
             This is not supported with per-workflow env overrides because the config value is \
             evaluated once at worker startup. Use env_var() in profiles.yml to set the base \
             schema/database instead — that path is fully supported."
        ))
        .into());
    }

    match raw_sql_result {
        Ok(raw_sql) if !raw_sql.trim().is_empty() => {
            // Use the model's original file path as the rendering filename so any
            // Jinja error references the source file rather than `<unknown>`.
            let render_filename = state.io_args.in_dir.join(&common.original_file_path);
            let compiled = dbt_jinja_utils::utils::render_sql_with_listeners(
                &raw_sql,
                &jinja_env,
                &node_context,
                &[],
                &render_filename,
            )
            .map_err(|e| {
                DbtTemporalError::Compilation(format!("compiling SQL for {unique_id}: {e:#}"))
            })?;
            // Inject ephemeral model CTEs (ref('ephemeral_model') → __dbt__cte__<name>).
            let compiled = inject_ephemeral_ctes(
                &compiled,
                &common.name,
                &state.resolver_state.nodes,
                &jinja_env,
                &node_context,
                &state.io_args.in_dir,
                &ephemeral_dir,
            )?;
            // Patch ref() schemas in compiled SQL: ref() resolves to startup default schema,
            // but per-workflow env overrides may change the schema. Replace quoted occurrences
            // of the startup schema with the per-workflow schema so downstream refs find tables
            // in the correct schema.
            let compiled =
                patch_compiled_schema(compiled, env_schema.as_deref(), &state.default_schema);

            // Set both `sql` and `compiled_code` in the context. View materializations
            // reference `sql`, while table/incremental materializations reference
            // `compiled_code` (passed to create_table_as / bq_create_table_as).
            node_context.insert("sql".to_owned(), minijinja::Value::from(compiled.clone()));
            node_context
                .insert("compiled_code".to_owned(), minijinja::Value::from(compiled.clone()));

            // Write compiled SQL to the temp dir so model.compiled_code / model.compiled_sql
            // resolve correctly when accessed by the materialization template.
            let dest = io_args.out_dir.join("compiled").join(&common.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    DbtTemporalError::Configuration(format!(
                        "creating compiled SQL directory {}: {e:#}",
                        parent.display()
                    ))
                })?;
            }
            std::fs::write(&dest, &compiled).map_err(|e| {
                DbtTemporalError::Configuration(format!(
                    "writing compiled SQL for {unique_id} to {}: {e:#}",
                    dest.display()
                ))
            })?;
        }
        Ok(_) => {
            empty_raw_sql_dispatch(rt, unique_id)?;
            info!(node = %unique_id, "raw SQL is empty");
        }
        Err((path, e)) => {
            raw_sql_read_error_dispatch(rt, unique_id, &path, &e)?;
            info!(
                node = %unique_id,
                path = %path.display(),
                error = %e,
                "failed to read raw SQL file"
            );
        }
    }

    // Keep temp_dir alive until after rendering completes (dropped at end of scope).

    let compile_end = chrono::Utc::now();

    // For `dbt compile`, stop here — render SQL but skip materialization and any
    // adapter execution. The caller gets the compiled SQL via `compiled_code`.
    if input.command == "compile" {
        let compiled_code = node_context
            .get("sql")
            .and_then(|v| v.as_str().map(ToString::to_string));
        let execution_time = start_instant.elapsed().as_secs_f64();
        let compile_iso = compile_start.to_rfc3339();
        let compile_end_iso = compile_end.to_rfc3339();
        info!(node = %unique_id, time_secs = execution_time, "node compiled (compile-only)");
        return Ok(NodeExecutionResult {
            unique_id: unique_id.clone(),
            status: NodeStatus::Success,
            execution_time,
            message: Some("compiled".to_string()),
            adapter_response: BTreeMap::new(),
            compiled_code,
            timing: vec![TimingEntry {
                name: "compile".to_string(),
                started_at: compile_iso,
                completed_at: compile_end_iso,
            }],
            failures: None,
        });
    }

    // --- EXECUTE PHASE ---
    let execute_start = chrono::Utc::now();

    // Ensure the target schema/dataset exists (dbt does this before materializations).
    // Dispatches to the adapter-specific create_schema macro (e.g. CREATE SCHEMA IF NOT EXISTS).
    // Uses `this` which is the target relation (database + schema + identifier).
    if is_create_schema_eligible(rt)
        && let Err(e) = jinja_env.render_str("{% do create_schema(this) %}", &node_context, &[])
    {
        tracing::warn!(node = %unique_id, error = %e, "create_schema failed (non-fatal)");
    }

    // Resolve the materialization template using dbt-fusion's MaterializationResolver.
    // Dispatches with adapter prefix inheritance (e.g. redshift→postgres→default) and
    // package precedence (Root > Imported > Core for builtins).
    let materialization = select_materialization_name(rt, &base.materialized.to_string());

    // Extract compiled SQL from the node context before rendering.
    let compiled_sql = node_context
        .get("sql")
        .and_then(|v| v.as_str().map(ToString::to_string));

    let fq_name = state
        .materialization_resolver
        .find_materialization_macro_by_name(&materialization)
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "no materialization found for node {unique_id} (materialization={materialization}): {e:#}"
            ))
        })?;

    // Render the materialization template (triggers SQL execution through BridgeAdapter).
    let rendered = render_materialization(&jinja_env, &fq_name, &node_context)?;

    // Prefer the compiled SQL from the context; fall back to rendered output if non-empty.
    let compiled_code = finalize_compiled_code(compiled_sql, &rendered);

    let execute_end = chrono::Utc::now();

    let execution_time = start_instant.elapsed().as_secs_f64();

    let timing = build_timing_entries(compile_start, compile_end, execute_start, execute_end);

    // Extract adapter response from the ResultStore.
    // Materialization macros call store_result('main', response) during rendering.
    let adapter_response = extract_adapter_response(&result_store);

    // Guard against silent no-op executions. For nodes that should execute SQL, an empty
    // adapter response indicates that statement('main') likely never ran.
    if expects_adapter_response(rt, &materialization) && adapter_response.is_empty() {
        return Err(DbtTemporalError::Adapter(anyhow::anyhow!(
            "node {unique_id} finished without adapter response (resource_type={}, materialization={materialization}); no query appears to have run",
            rt.as_str_name()
        ))
        .into());
    }

    // For test nodes, extract failure count from the result table (not rows_affected).
    // The test materialization wraps the query in get_test_sql() which returns a single row
    // with a `failures` column. rows_affected is always 1 (one row returned), not the count.
    let failures = if rt == NodeType::Test {
        extract_test_failures(&result_store)
    } else {
        None
    };

    // Build a human-readable message from the adapter response for the Temporal UI.
    // Falls back to materialization type when the adapter doesn't return metadata
    // (e.g. ephemeral models that never execute against the warehouse).
    let message = build_success_message(&adapter_response, &materialization);

    info!(
        node = %unique_id,
        time_secs = execution_time,
        message = message.as_deref().unwrap_or("-"),
        "node execution complete"
    );

    // For test nodes, failures > 0 means the test found failing rows.
    // Tests with severity: warn produce warnings but don't fail the activity.
    if let Some(n) = failures
        && n > 0
    {
        use dbt_schemas::schemas::common::Severity;
        let severity = state
            .resolver_state
            .nodes
            .tests
            .get(unique_id)
            .and_then(|t| t.deprecated_config.severity.as_ref())
            .cloned()
            .unwrap_or_default();
        if matches!(severity, Severity::Warn) {
            warn!(node = %unique_id, failures = n, "test warning (severity: warn)");
        } else {
            return Err(DbtTemporalError::TestFailure {
                unique_id: unique_id.clone(),
                failures: n,
            }
            .into());
        }
    }

    Ok(NodeExecutionResult {
        unique_id: unique_id.clone(),
        status: NodeStatus::Success,
        execution_time,
        message,
        adapter_response,
        compiled_code,
        timing,
        failures,
    })
}

/// Build a human-readable success message from the adapter response.
/// E.g. "CREATE TABLE (42 rows)", "CREATE VIEW", "ephemeral".
fn build_success_message(
    adapter_response: &BTreeMap<String, serde_json::Value>,
    materialization: &str,
) -> Option<String> {
    // Try adapter response first (has DDL/DML info + rows_affected).
    if let Some(msg) = adapter_response
        .get("message")
        .and_then(serde_json::Value::as_str)
    {
        let rows = adapter_response
            .get("rows_affected")
            .and_then(serde_json::Value::as_i64);
        return Some(rows.map_or_else(|| msg.to_string(), |n| format!("{msg} ({n} rows)")));
    }

    // Fallback: use the materialization type (e.g. "ephemeral", "view").
    if !materialization.is_empty() {
        return Some(materialization.to_string());
    }

    None
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use crate::error::compile_error_patterns;

    // --- decide_retry ---

    fn empty_patterns() -> Vec<regex::Regex> {
        Vec::new()
    }

    #[test]
    fn decide_retry_no_retry_for_compilation() {
        let err = DbtTemporalError::Compilation("bad ref".into());
        assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
    }

    #[test]
    fn decide_retry_no_retry_for_configuration() {
        let err = DbtTemporalError::Configuration("missing profile".into());
        assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
    }

    #[test]
    fn decide_retry_no_retry_for_project_not_found() {
        let err = DbtTemporalError::ProjectNotFound("nope".into());
        assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
    }

    #[test]
    fn decide_retry_no_retry_for_test_failure() {
        let err = DbtTemporalError::TestFailure {
            unique_id: "test.foo".into(),
            failures: 5,
        };
        assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
    }

    #[test]
    fn decide_retry_retries_adapter_with_no_patterns() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("connection timeout"));
        assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::Retry);
    }

    #[test]
    fn decide_retry_promotes_adapter_to_no_retry_when_pattern_matches() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("permission denied for table foo"));
        let patterns = compile_error_patterns(&["permission denied".to_string()]);
        assert_eq!(decide_retry(&err, &patterns), RetryDecision::NoRetry);
    }

    #[test]
    fn decide_retry_keeps_adapter_retryable_when_pattern_does_not_match() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("connection refused"));
        let patterns = compile_error_patterns(&["permission denied".to_string()]);
        assert_eq!(decide_retry(&err, &patterns), RetryDecision::Retry);
    }

    // --- downcast_or_wrap_as_adapter ---

    #[test]
    fn downcast_or_wrap_recovers_dbt_temporal_error_variant() {
        let original = DbtTemporalError::Compilation("bad ref".into());
        let any: anyhow::Error = anyhow::anyhow!(original);
        let recovered = downcast_or_wrap_as_adapter(any);
        // Compilation must survive the round-trip — without this, retry
        // classification would silently demote Compilation to Adapter (retryable).
        assert!(matches!(recovered, DbtTemporalError::Compilation(_)));
        assert!(!recovered.is_retryable());
    }

    #[test]
    fn downcast_or_wrap_recovers_test_failure_variant() {
        let original = DbtTemporalError::TestFailure {
            unique_id: "test.x".into(),
            failures: 1,
        };
        let any: anyhow::Error = anyhow::anyhow!(original);
        let recovered = downcast_or_wrap_as_adapter(any);
        assert!(matches!(recovered, DbtTemporalError::TestFailure { .. }));
    }

    #[test]
    fn downcast_or_wrap_falls_back_to_adapter_for_plain_anyhow() {
        let any: anyhow::Error = anyhow::anyhow!("a plain error not from us");
        let recovered = downcast_or_wrap_as_adapter(any);
        // Adapter is the retryable default — keeps us out of false positives
        // for transient warehouse issues that don't carry our typed variant.
        assert!(matches!(recovered, DbtTemporalError::Adapter(_)));
        assert!(recovered.is_retryable());
    }

    // --- build_success_message ---

    #[test]
    fn build_success_message_prefers_adapter_message_with_row_count() {
        let mut response = BTreeMap::new();
        response.insert("message".to_string(), serde_json::json!("CREATE TABLE"));
        response.insert("rows_affected".to_string(), serde_json::json!(42));
        let msg = build_success_message(&response, "table");
        assert_eq!(msg.as_deref(), Some("CREATE TABLE (42 rows)"));
    }

    #[test]
    fn build_success_message_uses_message_only_when_rows_unavailable() {
        let mut response = BTreeMap::new();
        response.insert("message".to_string(), serde_json::json!("CREATE VIEW"));
        let msg = build_success_message(&response, "view");
        assert_eq!(msg.as_deref(), Some("CREATE VIEW"));
    }

    #[test]
    fn build_success_message_falls_back_to_materialization_when_no_response() {
        let response = BTreeMap::new();
        let msg = build_success_message(&response, "ephemeral");
        assert_eq!(msg.as_deref(), Some("ephemeral"));
    }

    #[test]
    fn build_success_message_returns_none_when_neither_available() {
        let response = BTreeMap::new();
        let msg = build_success_message(&response, "");
        assert!(msg.is_none());
    }

    #[test]
    fn build_success_message_treats_non_string_message_as_absent() {
        // A numeric "message" field doesn't satisfy as_str() — the builder
        // must fall through to the materialization fallback rather than
        // producing a malformed message.
        let mut response = BTreeMap::new();
        response.insert("message".to_string(), serde_json::json!(42));
        let msg = build_success_message(&response, "table");
        assert_eq!(msg.as_deref(), Some("table"));
    }

    // --- patch_compiled_schema ---

    #[test]
    fn patch_compiled_schema_replaces_quoted_default_with_workflow_schema() {
        let sql = "select * from \"raw\".\"orders\" join \"raw\".\"customers\" using (id)";
        let out = patch_compiled_schema(sql.to_string(), Some("workflow_42"), "raw");
        assert_eq!(
            out,
            "select * from \"workflow_42\".\"orders\" join \"workflow_42\".\"customers\" using (id)"
        );
    }

    #[test]
    fn patch_compiled_schema_no_op_when_env_schema_absent() {
        let sql = "select 1 from \"raw\".\"x\"";
        let out = patch_compiled_schema(sql.to_string(), None, "raw");
        assert_eq!(out, sql);
    }

    #[test]
    fn patch_compiled_schema_no_op_when_workflow_matches_default() {
        let sql = "select 1 from \"raw\".\"x\"";
        let out = patch_compiled_schema(sql.to_string(), Some("raw"), "raw");
        assert_eq!(out, sql);
    }

    #[test]
    fn patch_compiled_schema_only_replaces_quoted_occurrences() {
        // An unquoted match is left alone: only `"raw"` (with quotes) matters.
        // Bare `raw.foo` is something else (e.g. a column reference).
        let sql = "with raw as (select 1) select \"raw\".\"x\" from raw";
        let out = patch_compiled_schema(sql.to_string(), Some("env_a"), "raw");
        assert_eq!(out, "with raw as (select 1) select \"env_a\".\"x\" from raw");
    }

    // --- select_materialization_name ---

    #[test]
    fn select_materialization_name_forces_seed_for_seed_nodes() {
        // base.materialized still says "table" for seeds in dbt-fusion (#1345).
        assert_eq!(select_materialization_name(NodeType::Seed, "table"), "seed");
        assert_eq!(select_materialization_name(NodeType::Seed, "view"), "seed");
    }

    #[test]
    fn select_materialization_name_lowercases_for_non_seed() {
        assert_eq!(select_materialization_name(NodeType::Model, "Table"), "table");
        assert_eq!(select_materialization_name(NodeType::Model, "VIEW"), "view");
        assert_eq!(select_materialization_name(NodeType::Snapshot, "snapshot"), "snapshot");
        assert_eq!(select_materialization_name(NodeType::Test, "test"), "test");
    }

    // --- is_create_schema_eligible ---

    #[test]
    fn is_create_schema_eligible_for_writers_only() {
        assert!(is_create_schema_eligible(NodeType::Model));
        assert!(is_create_schema_eligible(NodeType::Seed));
        assert!(is_create_schema_eligible(NodeType::Snapshot));
        // Tests and operations don't need a schema-create pass.
        assert!(!is_create_schema_eligible(NodeType::Test));
        assert!(!is_create_schema_eligible(NodeType::Operation));
    }

    // --- expects_adapter_response ---

    #[test]
    fn expects_adapter_response_for_writer_resource_types() {
        assert!(expects_adapter_response(NodeType::Model, "table"));
        assert!(expects_adapter_response(NodeType::Model, "view"));
        assert!(expects_adapter_response(NodeType::Seed, "seed"));
        assert!(expects_adapter_response(NodeType::Snapshot, "snapshot"));
        assert!(expects_adapter_response(NodeType::Test, "test"));
    }

    #[test]
    fn expects_adapter_response_skips_ephemeral_models() {
        // Ephemeral models compile to CTEs in their downstream consumer — they
        // never execute against the warehouse, so an empty adapter response is
        // expected and not a bug.
        assert!(!expects_adapter_response(NodeType::Model, "ephemeral"));
    }

    #[test]
    fn expects_adapter_response_false_for_other_node_types() {
        // Hooks, operations, sources etc. don't run as standalone activities;
        // even if they did, they don't need adapter response inspection.
        assert!(!expects_adapter_response(NodeType::Operation, "view"));
        assert!(!expects_adapter_response(NodeType::Source, "view"));
    }

    // --- finalize_compiled_code ---

    #[test]
    fn finalize_compiled_code_prefers_context_sql() {
        // When the context captured an explicit `sql`, the rendered output
        // (often just an empty string from materialization templates) is
        // ignored.
        let code = finalize_compiled_code(Some("SELECT 1".to_string()), "");
        assert_eq!(code.as_deref(), Some("SELECT 1"));
        let code = finalize_compiled_code(Some("SELECT 1".to_string()), "ignored");
        assert_eq!(code.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn finalize_compiled_code_falls_back_to_rendered_when_context_absent() {
        let code = finalize_compiled_code(None, "  CREATE VIEW foo AS SELECT 1  ");
        assert_eq!(code.as_deref(), Some("CREATE VIEW foo AS SELECT 1"));
    }

    #[test]
    fn finalize_compiled_code_returns_none_when_both_empty() {
        assert!(finalize_compiled_code(None, "").is_none());
        assert!(finalize_compiled_code(None, "   \n  ").is_none());
    }

    // --- build_timing_entries ---

    #[test]
    fn build_timing_entries_emits_compile_then_execute() {
        let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t1 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_001, 0).unwrap();
        let t2 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_002, 0).unwrap();
        let t3 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_003, 0).unwrap();
        let entries = build_timing_entries(t0, t1, t2, t3);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "compile");
        assert_eq!(entries[0].started_at, t0.to_rfc3339());
        assert_eq!(entries[0].completed_at, t1.to_rfc3339());
        assert_eq!(entries[1].name, "execute");
        assert_eq!(entries[1].started_at, t2.to_rfc3339());
        assert_eq!(entries[1].completed_at, t3.to_rfc3339());
    }

    // --- empty_raw_sql_dispatch / raw_sql_read_error_dispatch ---

    #[test]
    fn empty_raw_sql_dispatch_errors_for_writer_types() {
        for rt in [NodeType::Model, NodeType::Snapshot, NodeType::Test] {
            let err = empty_raw_sql_dispatch(rt, "model.x.foo").unwrap_err();
            assert!(matches!(err, DbtTemporalError::Compilation(_)));
            assert!(err.to_string().contains("model.x.foo"));
            assert!(err.to_string().contains("empty"));
        }
    }

    #[test]
    fn empty_raw_sql_dispatch_ok_for_other_types() {
        // Hooks and operations are allowed to compile to nothing.
        for rt in [NodeType::Operation, NodeType::Seed] {
            assert!(empty_raw_sql_dispatch(rt, "x").is_ok());
        }
    }

    #[test]
    fn raw_sql_read_error_dispatch_errors_for_writer_types() {
        let path = std::path::PathBuf::from("/missing/foo.sql");
        let io_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        for rt in [NodeType::Model, NodeType::Snapshot, NodeType::Test] {
            let err = raw_sql_read_error_dispatch(rt, "model.x.foo", &path, &io_err).unwrap_err();
            assert!(matches!(err, DbtTemporalError::Compilation(_)));
            let msg = err.to_string();
            assert!(msg.contains("model.x.foo"));
            assert!(msg.contains("/missing/foo.sql"));
        }
    }

    #[test]
    fn raw_sql_read_error_dispatch_ok_for_other_types() {
        let path = std::path::PathBuf::from("/missing/foo.sql");
        let io_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert!(raw_sql_read_error_dispatch(NodeType::Operation, "x", &path, &io_err).is_ok());
    }

    // --- write_cached_sql ---

    #[test]
    fn write_cached_sql_writes_when_cache_hit() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = BTreeMap::new();
        cache.insert("models/m.sql".to_string(), "SELECT 1".to_string());

        let dest = dir.path().join("compiled/models/m.sql");
        write_cached_sql(&cache, "models/m.sql", &dest).unwrap();
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "SELECT 1");
    }

    #[test]
    fn write_cached_sql_noop_when_cache_miss() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BTreeMap::new();
        let dest = dir.path().join("compiled/models/m.sql");
        write_cached_sql(&cache, "models/m.sql", &dest).unwrap();
        assert!(!dest.exists());
    }

    #[test]
    fn write_cached_sql_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = BTreeMap::new();
        cache.insert("a/b/c.sql".to_string(), "x".to_string());

        // dest has multiple non-existent parent levels; the helper should
        // create them all.
        let dest = dir.path().join("nested/dirs/a/b/c.sql");
        write_cached_sql(&cache, "a/b/c.sql", &dest).unwrap();
        assert!(dest.exists());
    }

    // --- parse_invocation_id ---

    #[test]
    fn parse_invocation_id_accepts_valid_uuid() {
        let raw = "00000000-0000-0000-0000-000000000001";
        let id = parse_invocation_id(raw).unwrap();
        assert_eq!(id.to_string(), raw);
    }

    #[test]
    fn parse_invocation_id_rejects_garbage_with_actionable_message() {
        let err = parse_invocation_id("not-a-uuid").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid invocation_id"));
        assert!(msg.contains("not-a-uuid"));
    }

    // --- classify_for_temporal ---

    #[test]
    fn classify_for_temporal_marks_retryable_adapter_error() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("connection timeout"));
        let activity_err = classify_for_temporal(&err, &empty_patterns());
        assert!(matches!(activity_err, ActivityError::Retryable { .. }));
    }

    #[test]
    fn classify_for_temporal_marks_compilation_as_non_retryable() {
        let err = DbtTemporalError::Compilation("bad ref".into());
        let activity_err = classify_for_temporal(&err, &empty_patterns());
        assert!(matches!(activity_err, ActivityError::NonRetryable(_)));
    }

    #[test]
    fn classify_for_temporal_promotes_pattern_match_to_non_retryable() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("permission denied for table foo"));
        let patterns = compile_error_patterns(&["permission denied".to_string()]);
        let activity_err = classify_for_temporal(&err, &patterns);
        assert!(matches!(activity_err, ActivityError::NonRetryable(_)));
    }

    // --- registry_non_retryable_patterns ---

    #[test]
    fn registry_non_retryable_patterns_returns_none_for_unknown_project() {
        // Empty registry → unknown project lookup returns None, signalling
        // the caller to fall back to "all adapter errors retry".
        use std::collections::BTreeMap;

        let registry = Arc::new(ProjectRegistry::new(BTreeMap::new()));
        assert!(registry_non_retryable_patterns(&registry, "missing").is_none());
    }
}
