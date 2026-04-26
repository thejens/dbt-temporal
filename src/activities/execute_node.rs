mod schema_patcher;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use dbt_adapter::load_store::ResultStore;
use dbt_schemas::schemas::telemetry::NodeType;
use schema_patcher::has_env_var_in_config_schema_or_database;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::{info, warn};

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
                    // Try to preserve the original DbtTemporalError variant for
                    // correct retry classification; fall back to Adapter (retryable).
                    let dbt_err = match e.downcast::<DbtTemporalError>() {
                        Ok(d) => d,
                        Err(other) => DbtTemporalError::Adapter(other),
                    };
                    if dbt_err.is_retryable() {
                        // Check user-configured non-retryable error patterns.
                        if matches_non_retryable_pattern(&activities.registry, &project, &dbt_err) {
                            Err(ActivityError::NonRetryable(
                                anyhow::anyhow!("{dbt_err}").into(),
                            ))
                        } else {
                            Err(ActivityError::Retryable {
                                source: anyhow::anyhow!("{dbt_err}").into(),
                                explicit_delay: None,
                            })
                        }
                    } else {
                        Err(ActivityError::NonRetryable(
                            anyhow::anyhow!("{dbt_err}").into(),
                        ))
                    }
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

/// Check if an adapter error message matches any user-configured non-retryable pattern.
fn matches_non_retryable_pattern(
    registry: &Arc<ProjectRegistry>,
    project: &str,
    err: &DbtTemporalError,
) -> bool {
    let Ok(state) = registry.get(Some(project)) else {
        return false;
    };

    let patterns = &state.non_retryable_error_patterns;
    if patterns.is_empty() {
        return false;
    }

    let msg = err.to_string();
    let matched = crate::error::matches_error_patterns(&msg, patterns);
    if matched {
        tracing::info!(
            error = %msg,
            "adapter error matched non-retryable pattern, suppressing retry"
        );
    }
    matched
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

    // Write compiled SQL for this node into the temp dir.
    if let Some(sql) = state.compiled_sql_cache.get(&node_path) {
        let dest = temp_out.join("compiled").join(&common.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, sql)?;
    }

    // Write snapshot raw SQL if this is a snapshot node.
    if rt == NodeType::Snapshot
        && let Some(sql) = state.snapshot_sql_cache.get(&node_path)
    {
        let dest = temp_out.join(&common.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, sql)?;
    }

    // Reject malformed IDs loud; falling back to the worker-state invocation_id
    // would silently mis-tag artifacts and run-results under a different run.
    let invocation_id = input
        .invocation_id
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid invocation_id {:?}: {e}", input.invocation_id))?;
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

    // Patch `this`, `database`, `schema` when per-workflow env overrides changed the profile
    // schema/database. build_run_node_context builds `this` from node.base() which was resolved
    // at startup — it doesn't reflect the per-workflow profile values.
    //
    // Strategy: recompute the schema/database using the same pattern as dbt's default
    // generate_schema_name / generate_database_name macros, substituting the per-workflow
    // target values. This handles both nodes with no custom schema (target.schema directly)
    // and nodes with static custom schemas (target.schema + "_" + custom).
    //
    // Note: the `target` Jinja global is already patched above, so macros invoked during
    // subsequent rendering (raw SQL compilation, materialization) will also see correct values.
    if let Some(ref wf_schema) = env_schema {
        let wf_database = env_database.as_deref().unwrap_or(&base.database);
        let default_prefix = format!("{}_", state.default_schema);

        // Recompute schema: default generate_schema_name returns target.schema when no custom
        // schema, or target.schema + "_" + custom_schema_name when custom schema is set.
        let new_schema = if base.schema == state.default_schema {
            wf_schema.clone()
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
            ))
            .into());
        } else {
            // Schema doesn't follow default pattern but project uses the
            // default macro — keep base.schema unchanged.
            base.schema.clone()
        };

        // Recompute database: default generate_database_name returns target.database when
        // no custom database, or the custom database directly (no prefix pattern).
        let new_database = if base.database == state.default_database {
            wf_database.to_string()
        } else {
            base.database.clone()
        };

        let needs_schema_patch = new_schema != base.schema;
        let needs_db_patch = new_database != base.database;

        if needs_schema_patch || needs_db_patch {
            let patched_schema = if needs_schema_patch {
                new_schema.as_str()
            } else {
                &base.schema
            };
            let patched_database = if needs_db_patch {
                new_database.as_str()
            } else {
                &base.database
            };

            // Rebuild the `this` Relation with the corrected schema/database.
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
            if needs_schema_patch {
                node_context.insert("schema".to_owned(), minijinja::Value::from(patched_schema));
            }
            if needs_db_patch {
                node_context
                    .insert("database".to_owned(), minijinja::Value::from(patched_database));
            }
        }
    }

    // Compile the node's raw SQL by rendering it through Jinja (resolves ref/source/config).
    // build_run_node_context does NOT populate the top-level "sql" context variable — that's
    // the caller's responsibility. The materialization template uses {{ sql }} as the compiled
    // model query (e.g. in `get_create_view_as_sql(target_relation, sql)`).
    //
    // For tests, prefer the generated SQL file in out_dir. dbt-fusion persists generic tests
    // with fully inlined kwargs there (e.g. accepted_values values=[...]), while `raw_code`
    // can be a reduced template that references `_dbt_generic_test_kwargs`.
    #[allow(clippy::option_if_let_else)] // if-let-else is clearer here
    let raw_sql_result = if rt == NodeType::Seed {
        // Seeds have no SQL body — data comes from agate_table (loaded above).
        // Reading original_file_path here would pick up the CSV file and embed it
        // verbatim in CREATE TABLE AS (csv_content...), producing invalid SQL.
        Ok(String::new())
    } else if rt == NodeType::Test {
        if let Some(sql) = state.test_sql_cache.get(&node_path) {
            Ok(sql.clone())
        } else {
            // Fallback: try reading from disk (out_dir then in_dir).
            let candidates: Vec<(std::path::PathBuf, &'static str)> = vec![
                (
                    state.io_args.out_dir.join(&common.original_file_path),
                    "out_dir/original_file_path",
                ),
                (state.io_args.out_dir.join(&common.path), "out_dir/path"),
                (
                    state.io_args.in_dir.join(&common.original_file_path),
                    "in_dir/original_file_path",
                ),
                (state.io_args.in_dir.join(&common.path), "in_dir/path"),
            ];
            if let Some((sql, _, _)) = read_first_non_empty_sql(&candidates) {
                Ok(sql)
            } else {
                // Last resort: use raw_code from the node (may lack inlined kwargs).
                common
                    .raw_code
                    .as_deref()
                    .filter(|s| !s.is_empty() && *s != "--placeholder--")
                    .map(ToString::to_string)
                    .map_or_else(
                        || {
                            Err((
                                state.io_args.out_dir.join(&common.original_file_path),
                                std::io::Error::from(std::io::ErrorKind::NotFound),
                            ))
                        },
                        Ok,
                    )
            }
        }
    } else {
        let raw_sql_from_node = common
            .raw_code
            .as_deref()
            .filter(|s| !s.is_empty() && *s != "--placeholder--")
            .map(ToString::to_string);

        raw_sql_from_node.map_or_else(
            || {
                let raw_sql_path = state.io_args.in_dir.join(&common.original_file_path);
                std::fs::read_to_string(&raw_sql_path).map_err(|e| (raw_sql_path, e))
            },
            Ok,
        )
    };

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
            let compiled = if let Some(ref wf_schema) = env_schema {
                if *wf_schema == state.default_schema {
                    compiled
                } else {
                    compiled.replace(
                        &format!("\"{}\"", state.default_schema),
                        &format!("\"{wf_schema}\""),
                    )
                }
            } else {
                compiled
            };

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
        Ok(_) => match rt {
            NodeType::Model | NodeType::Snapshot | NodeType::Test => {
                return Err(DbtTemporalError::Compilation(format!(
                    "raw SQL is empty for {unique_id}"
                ))
                .into());
            }
            _ => {
                info!(node = %unique_id, "raw SQL is empty");
            }
        },
        Err((path, e)) => match rt {
            NodeType::Model | NodeType::Snapshot | NodeType::Test => {
                return Err(DbtTemporalError::Compilation(format!(
                    "reading raw SQL for {unique_id} at {}: {e:#}",
                    path.display()
                ))
                .into());
            }
            _ => {
                info!(
                    node = %unique_id,
                    path = %path.display(),
                    error = %e,
                    "failed to read raw SQL file"
                );
            }
        },
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
    if matches!(rt, NodeType::Model | NodeType::Seed | NodeType::Snapshot)
        && let Err(e) = jinja_env.render_str("{% do create_schema(this) %}", &node_context, &[])
    {
        tracing::warn!(node = %unique_id, error = %e, "create_schema failed (non-fatal)");
    }

    // Resolve the materialization template using dbt-fusion's MaterializationResolver.
    // This applies proper adapter prefix inheritance (e.g. redshift→postgres→default)
    // and package precedence (Root > Imported > Core for builtins).
    // Seeds are forced to "seed" because base.materialized still returns "table" for them
    // (dbt-fusion#1345 — DbtMaterialization::Seed exists for comparisons but node data
    // hasn't been updated to use it). Without this, materialization_table_default is called
    // with an empty sql body, producing invalid SQL.
    let materialization = if rt == NodeType::Seed {
        "seed".to_string()
    } else {
        base.materialized.to_string().to_lowercase()
    };

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
    let compiled_code = compiled_sql.or_else(|| {
        let trimmed = rendered.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    let execute_end = chrono::Utc::now();

    let execution_time = start_instant.elapsed().as_secs_f64();

    let timing = vec![
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
    ];

    // Extract adapter response from the ResultStore.
    // Materialization macros call store_result('main', response) during rendering.
    let adapter_response = extract_adapter_response(&result_store);

    // Guard against silent no-op executions. For nodes that should execute SQL, an empty
    // adapter response indicates that statement('main') likely never ran.
    let expects_adapter_response = match rt {
        NodeType::Model => materialization != "ephemeral",
        NodeType::Seed | NodeType::Snapshot | NodeType::Test => true,
        _ => false,
    };
    if expects_adapter_response && adapter_response.is_empty() {
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

/// Convert a `dbt_yaml::Value` to a native `minijinja::Value`.
///
/// Unlike `minijinja::Value::from_serialize()`, this produces native minijinja
/// types (Vec for sequences, BTreeMap for mappings) that support iteration and
/// interpolation in Jinja templates. This is critical for generic test kwargs
/// like `values=["active",...]` (accepted_values) and `threshold=0` (greater_than).
#[allow(clippy::option_if_let_else)]
fn yml_value_to_minijinja(v: &dbt_yaml::Value) -> minijinja::Value {
    match v {
        dbt_yaml::Value::Null(_) => minijinja::Value::from(()),
        dbt_yaml::Value::Bool(b, _) => minijinja::Value::from(*b),
        dbt_yaml::Value::Number(n, _) => {
            if let Some(i) = n.as_i64() {
                minijinja::Value::from(i)
            } else if let Some(u) = n.as_u64() {
                minijinja::Value::from(u)
            } else if let Some(f) = n.as_f64() {
                minijinja::Value::from(f)
            } else {
                minijinja::Value::from(n.to_string())
            }
        }
        dbt_yaml::Value::String(s, _) => minijinja::Value::from(s.clone()),
        dbt_yaml::Value::Sequence(seq, _) => {
            let items: Vec<minijinja::Value> = seq.iter().map(yml_value_to_minijinja).collect();
            minijinja::Value::from(items)
        }
        dbt_yaml::Value::Mapping(map, _) => {
            let items: BTreeMap<String, minijinja::Value> = map
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        dbt_yaml::Value::String(s, _) => s.clone(),
                        other => format!("{other:?}"),
                    };
                    (key, yml_value_to_minijinja(v))
                })
                .collect();
            minijinja::Value::from(items)
        }
        dbt_yaml::Value::Tagged(tagged, _) => yml_value_to_minijinja(&tagged.value),
    }
}

/// Convert a `dbt_yaml::Value` to minijinja and evaluate wrapped Jinja expressions.
///
/// Generic test kwargs can include expression strings like
/// "{{ get_where_subquery(ref('my_model')) }}". Evaluate these into runtime objects so
/// `**_dbt_generic_test_kwargs` behaves like dbt-fusion's executor path.
fn yml_value_to_minijinja_with_jinja(
    v: &dbt_yaml::Value,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> minijinja::Value {
    let base = yml_value_to_minijinja(v);
    let Some(raw) = base.as_str() else {
        return base;
    };

    let Some(expr) = raw
        .trim()
        .strip_prefix("{{")
        .and_then(|s| s.strip_suffix("}}"))
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return base;
    };

    jinja_env
        .env
        .compile_expression(expr)
        .and_then(|compiled| compiled.eval(node_context, &[]))
        .unwrap_or(base)
}

fn read_first_non_empty_sql(
    candidates: &[(std::path::PathBuf, &'static str)],
) -> Option<(String, &'static str, std::path::PathBuf)> {
    for (path, source) in candidates {
        let Ok(sql) = std::fs::read_to_string(path) else {
            continue;
        };
        if !sql.trim().is_empty() {
            return Some((sql, *source, path.clone()));
        }
    }
    None
}
