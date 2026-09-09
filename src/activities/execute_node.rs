pub(super) mod freshness;
mod raw_sql;
mod schema_patch;
mod schema_patcher;
mod sql_rewrite;
mod unit_test;
mod yml_to_value;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use dbt_schemas::schemas::telemetry::NodeType;
use raw_sql::resolve_raw_sql;
use schema_patch::{apply_relation_to_context, resolve_relations};
use schema_patcher::has_env_var_in_config_schema_or_database;
use sql_rewrite::rewrite_relations;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use tracing::{info, warn};
use yml_to_value::yml_value_to_minijinja_with_jinja;

use crate::error::DbtTemporalError;

use crate::types::{DbtCommand, NodeExecutionInput, NodeExecutionResult, NodeStatus, TimingEntry};

use super::DbtActivities;
use super::heartbeat;
use super::node_helpers::{
    extract_adapter_response, extract_test_outcome, inject_ephemeral_ctes, render_materialization,
};
use super::node_serialization::{build_agate_table, get_node_config_yml, get_sql_header};
use super::render_env;
use super::retry;

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
    // dbt-native telemetry: Invocation root + NodeEvaluated span. The spans
    // must outlive the select! so the outcome can be recorded on them.
    let spans = super::node_telemetry::node_execution_spans(&activities.registry, &input);

    // This activity's own cancellation source, handed to the adapter through
    // the render environment. The worker-wide source was never cancelled by
    // anything, so a cancelled activity left its query running: the server's
    // heartbeat timeout could then start a second attempt while the first was
    // still writing.
    let cancel = dbt_common::cancellation::CancellationTokenSource::new();
    let token = cancel.token();

    let work = tracing::Instrument::instrument(
        execute_node_cancellable(activities, input, &token),
        spans.node.clone(),
    );
    tokio::pin!(work);

    let mut cancelled = false;
    let result = loop {
        tokio::select! {
            result = &mut work => break result,
            // `&mut work` above, so this branch does not drop the work: the
            // query is told to stop and then joined. Dropping the future would
            // leave the blocking thread running against the warehouse with
            // nothing waiting for it.
            () = ctx.cancelled(), if !cancelled => {
                cancelled = true;
                info!(node = %unique_id, "activity cancelled — stopping warehouse work");
                cancel.cancel();
            }
            // Never resolves — keeps the UI's last-heartbeat fresh and lets the
            // server's heartbeat_timeout reschedule on a fresh worker if this
            // one dies. The node's own work runs on a blocking thread, so this
            // task is free to keep heartbeating while a long query runs.
            never = heartbeat::heartbeat_loop(&ctx) => match never {},
        }
    };

    if cancelled {
        super::node_telemetry::record_outcome(&spans, NodeStatus::Cancelled);
        return Err(ActivityError::cancelled());
    }

    match result {
        Ok(result) => {
            super::node_telemetry::record_outcome(&spans, result.status);
            Ok(result)
        }
        Err(e) => {
            super::node_telemetry::record_outcome(&spans, NodeStatus::Error);
            // {:#} prints the whole context chain; the top context alone
            // routinely hides the actionable cause.
            tracing::error!(node = %unique_id, error = %format!("{e:#}"), "activity failed");
            let patterns = retry::registry_non_retryable_patterns(&activities.registry, &project);
            Err(retry::classify(
                e,
                patterns.as_deref().unwrap_or(&[]),
                retry::Unclassified::RetryAsAdapter,
            ))
        }
    }
}

/// Longest domain-failure message carried on a node result.
///
/// A unit test's message holds its row diff, which has no natural bound. The
/// result travels through Temporal, the run log and `run_results.json`, so the
/// message is capped where the diff stops being readable anyway.
const DOMAIN_FAILURE_MESSAGE_MAX: usize = 4096;

/// Render a terminal domain outcome as the node's result message.
///
/// A data test that found rows, a unit test whose output differed, a source
/// past its `error_after`: dbt reports each of these *on the node*, and none of
/// them gets better on a retry. They travel back as a populated
/// `NodeExecutionResult` with `NodeStatus::Error` rather than as an activity
/// failure — failing the activity discarded the timings, compiled SQL, adapter
/// metadata, failure count and freshness measurement already collected, and the
/// workflow rebuilt a bare error result in their place, so a failing test was
/// the node whose results said the least.
///
/// The `DbtTemporalError` variants stay the vocabulary for these outcomes; only
/// their journey changes, so the wording an operator sees is unchanged.
fn domain_failure_message(error: &DbtTemporalError) -> String {
    let full = error.to_string();
    if full.len() <= DOMAIN_FAILURE_MESSAGE_MAX {
        return full;
    }
    format!(
        "{}… (truncated)",
        crate::error::truncate_at_char_boundary(&full, DOMAIN_FAILURE_MESSAGE_MAX)
    )
}

/// Load the deferred node set from a previous run's manifest.
///
/// `--defer`'s purpose: unbuilt upstream `ref()`s resolve against the relations
/// that manifest describes instead of failing.
async fn load_defer_nodes(
    activities: &DbtActivities,
    state: &crate::worker_state::WorkerState,
    manifest_ref: Option<&str>,
) -> Result<Option<Arc<dbt_schemas::schemas::Nodes>>, anyhow::Error> {
    let Some(manifest_ref) = manifest_ref else {
        return Ok(None);
    };
    // A manifest at a given reference is the run that produced it — immutable,
    // and the same for every node of this run.
    if let Some(cached) = state.defer_manifests.get(manifest_ref) {
        return Ok(Some(cached));
    }
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "defer_manifest_ref requires artifact storage to be configured \
             (set ARTIFACT_STORE and WRITE_ARTIFACTS)"
        )
    })?;
    let bytes = store.retrieve(manifest_ref).await.map_err(|e| {
        DbtTemporalError::ArtifactStore(
            e.context(format!("loading defer manifest from {manifest_ref}")),
        )
    })?;
    // Manifests must parse through dbt's own serde path (JSON → YmlValue →
    // typed): node config structs serialize warehouse-specific keys flattened,
    // which the plain serde_json deserializer rejects ("missing field
    // __warehouse_specific_config__").
    let manifest_str = std::str::from_utf8(&bytes).context("defer manifest is not UTF-8")?;
    let manifest: dbt_schemas::schemas::manifest::DbtManifest =
        dbt_schemas::schemas::serde::typed_struct_from_json_str(manifest_str, None)
            .map_err(|e| anyhow::anyhow!("parsing defer manifest JSON: {e}"))?;
    // The project's own quoting, not a fixed guess. These relations are
    // rendered into this run's SQL by this run's adapter, so its rules are what
    // decide whether each part is quoted; the hard-coded values here were wrong
    // for any project whose quoting differs from them.
    let resolved = state.resolver_state.root_project_quoting;
    let quoting = dbt_schemas::schemas::common::DbtQuoting {
        database: Some(resolved.database),
        schema: Some(resolved.schema),
        identifier: Some(resolved.identifier),
        snowflake_ignore_case: None,
    };
    let nodes =
        Arc::new(dbt_schemas::schemas::manifest::nodes_from_dbt_manifest(manifest, quoting));
    state
        .defer_manifests
        .insert(manifest_ref, Arc::clone(&nodes));
    Ok(Some(nodes))
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

/// Where dbt-fusion looks for a node's compiled SQL.
///
/// `model.compiled_code` is not a value we can set — the `model` object in the
/// node context is a `LazyModelWrapper` that reads this file on every access.
/// Materializations that go through the attribute rather than the top-level
/// `compiled_code` variable (function DDL is one) therefore render an empty
/// body unless the file is exactly here. The layout is fusion's
/// (`out_dir/compiled/<package>/<original_file_path>`, with per-resource-type
/// quirks for snapshots and unit tests), so ask fusion rather than rebuild it.
fn compiled_sql_path(
    node: &dyn dbt_schemas::schemas::nodes::InternalDbtNode,
    in_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::path::PathBuf {
    node.get_node_path_abs(dbt_schemas::schemas::nodes::NodePathKind::Compiled, in_dir, out_dir)
}

/// Per-activity scratch space for one node execution.
///
/// Activities must never share `target/`: dbt-fusion writes compiled SQL and
/// per-ephemeral cumulative CTE chains there, and concurrent workflows on the
/// same worker would race on those files. Each activity therefore gets a fresh
/// temp dir seeded from the worker's in-memory SQL caches.
///
/// Holding the `TempDir` keeps the directory alive; dropping this struct
/// deletes it, so it must outlive every render that reads from `io_args`.
struct ActivityWorkspace {
    _temp_dir: tempfile::TempDir,
    ephemeral_dir: std::path::PathBuf,
    io_args: dbt_common::io_args::IoArgs,
}

impl ActivityWorkspace {
    fn new(
        state: &crate::worker_state::WorkerState,
        node: &dyn dbt_schemas::schemas::nodes::InternalDbtNode,
        invocation_id: &str,
    ) -> Result<Self, anyhow::Error> {
        let temp_dir = tempfile::tempdir().context("creating temp dir for activity")?;
        let out_dir = temp_dir.path().to_path_buf();

        let ephemeral_dir = temp_dir.path().join("ephemeral");
        std::fs::create_dir_all(&ephemeral_dir)
            .with_context(|| format!("creating ephemeral dir {}", ephemeral_dir.display()))?;

        let node_path = node.common().path.clone();
        let cache_key = node.common().unique_id.as_str();
        write_cached_sql(
            &state.compiled_sql_cache,
            cache_key,
            &compiled_sql_path(node, &state.io_args.in_dir, &out_dir),
        )?;
        if node.resource_type() == NodeType::Snapshot {
            write_cached_sql(&state.snapshot_sql_cache, cache_key, &out_dir.join(&node_path))?;
        }

        Ok(Self {
            _temp_dir: temp_dir,
            ephemeral_dir,
            io_args: dbt_common::io_args::IoArgs {
                in_dir: state.io_args.in_dir.clone(),
                out_dir,
                invocation_id: parse_invocation_id(invocation_id)?,
                ..Default::default()
            },
        })
    }
}

/// The result of a `dbt compile`: the rendered SQL, and nothing the warehouse
/// did — because nothing was sent to it.
fn compile_only_result(
    unique_id: &str,
    node_context: &BTreeMap<String, minijinja::Value>,
    execution_time: f64,
    compile_window: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
) -> NodeExecutionResult {
    let (compile_start, compile_end) = compile_window;
    NodeExecutionResult {
        unique_id: unique_id.to_owned(),
        status: NodeStatus::Success,
        execution_time,
        message: Some("compiled".to_string()),
        adapter_response: BTreeMap::new(),
        compiled_code: node_context
            .get("sql")
            .and_then(|v| v.as_str().map(ToString::to_string)),
        timing: vec![TimingEntry {
            name: "compile".to_string(),
            started_at: compile_start.to_rfc3339(),
            completed_at: compile_end.to_rfc3339(),
        }],
        failures: None,
        freshness: None,
        // Compile-only: nothing was written, so there is nothing to reference.
        compiled_code_ref: None,
        relation_name: None,
    }
}

/// Turn a freshness verdict into the status and message a node result reports.
///
/// Freshness grades three ways where the rest of the run grades two, and the
/// distinction is the whole point of the check: `warn_after` exceeded is not a
/// failure, and reporting it as success hid it from anyone reading results
/// rather than logs.
fn grade_freshness(
    unique_id: &str,
    rt: NodeType,
    verdict: freshness::FreshnessVerdict,
) -> (NodeStatus, String, crate::types::FreshnessOutcome) {
    match verdict {
        freshness::FreshnessVerdict::Stale {
            outcome,
            max_allowed_secs,
        } => {
            let message = domain_failure_message(&DbtTemporalError::StaleSource {
                unique_id: unique_id.to_owned(),
                node_kind: rt.as_static_ref(),
                max_loaded_at: outcome.max_loaded_at.clone(),
                age_secs: outcome.max_loaded_at_time_ago_in_s,
                max_allowed_secs,
            });
            warn!(node = %unique_id, message = %message, "freshness error (error_after exceeded)");
            (NodeStatus::Error, message, outcome)
        }
        freshness::FreshnessVerdict::Warning(outcome) => {
            warn!(
                node = %unique_id,
                age_secs = outcome.max_loaded_at_time_ago_in_s,
                "freshness warning (warn_after exceeded)"
            );
            let message = freshness_summary(&outcome);
            (NodeStatus::Warn, message, outcome)
        }
        freshness::FreshnessVerdict::Fresh(outcome) => {
            let message = freshness_summary(&outcome);
            (NodeStatus::Success, message, outcome)
        }
    }
}

/// The one-line summary a passing or warning freshness check reports.
fn freshness_summary(outcome: &crate::types::FreshnessOutcome) -> String {
    format!(
        "freshness {} (age {:.0}s, max_loaded_at {})",
        outcome.status.to_uppercase(),
        outcome.max_loaded_at_time_ago_in_s,
        outcome.max_loaded_at
    )
}

/// Pick the materialization name to dispatch on. Seeds are forced to "seed"
/// because dbt-fusion still reports their `base.materialized` as "table"
/// (issue #1345); without the override `materialization_table_default` would
/// be invoked with empty SQL and produce invalid CREATE statements. Unit
/// tests always dispatch to the bundled `unit` materialization.
fn select_materialization_name(rt: NodeType, base_materialized: &str) -> String {
    match rt {
        NodeType::Seed => "seed".to_string(),
        NodeType::UnitTest => "unit".to_string(),
        _ => base_materialized.to_lowercase(),
    }
}

/// Create the node's target schema, once per run rather than once per node.
///
/// A project with three hundred models in one schema used to issue three
/// hundred identical `CREATE SCHEMA IF NOT EXISTS` statements and the metadata
/// round trips behind them. The claim is keyed by run, not by worker: a schema
/// dropped between runs must be created again.
///
/// Failures are still not fatal — a run against an existing schema on a role
/// without CREATE is a legitimate setup, and the materialization gives a much
/// better error if the schema really is absent. But a *retryable* failure here
/// (a dropped connection, a throttle) is propagated: swallowing it meant the
/// node went on to fail with a confusing materialization error instead of
/// being retried.
fn ensure_target_schema(
    state: &crate::worker_state::WorkerState,
    invocation_id: &str,
    base: &dbt_schemas::schemas::nodes::NodeBaseAttributes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> Result<(), DbtTemporalError> {
    // The relation `this` resolves to, which is what `create_schema` reads —
    // per-workflow overrides have already patched it into the context.
    let (database, schema) = node_context
        .get("this")
        .and_then(|this| {
            let database = this.get_attr("database").ok()?.as_str()?.to_string();
            let schema = this.get_attr("schema").ok()?.as_str()?.to_string();
            Some((database, schema))
        })
        .unwrap_or_else(|| (base.database.clone(), base.schema.clone()));

    if !state
        .created_schemas
        .claim(invocation_id, base.adapter, &database, &schema)
    {
        return Ok(());
    }

    let Err(e) = jinja_env.render_str("{% do create_schema(this) %}", node_context, &[]) else {
        return Ok(());
    };

    // Not created after all — let the next node needing it try again.
    state
        .created_schemas
        .release(invocation_id, base.adapter, &database, &schema);

    let classified = crate::error::classify_adapter_execution_error(
        &*e,
        &format!("creating schema {database}.{schema}"),
    );
    if classified.is_retryable() {
        return Err(classified);
    }
    tracing::warn!(
        database = %database,
        schema = %schema,
        error = %classified,
        "create_schema failed (non-fatal)"
    );
    Ok(())
}

/// The relation the node wrote, as `database.schema.identifier`.
///
/// Read from the patched `this` rather than from startup metadata: a workflow
/// that overrides the target or the profile's env writes somewhere else
/// entirely, and the catalog and `run_results.json` have no other way to learn
/// where.
fn executed_relation_name(node_context: &BTreeMap<String, minijinja::Value>) -> Option<String> {
    let this = node_context.get("this")?;
    let part =
        |name: &str| -> Option<String> { Some(this.get_attr(name).ok()?.as_str()?.to_string()) };
    Some(format!("{}.{}.{}", part("database")?, part("schema")?, part("identifier")?))
}

/// True if the node is one we expect `create_schema(this)` to be called for
/// before materialization. Tests and operations don't get a schema-create
/// pass — they only read. Unit tests qualify because the `unit`
/// materialization creates a temp table in the target schema to probe column
/// types, and may run before anything else has created that schema. Functions
/// are created *in* a schema like any other relation.
const fn is_create_schema_eligible(rt: NodeType) -> bool {
    matches!(
        rt,
        NodeType::Model
            | NodeType::Seed
            | NodeType::Snapshot
            | NodeType::UnitTest
            | NodeType::Function
    )
}

/// True if this test node persists failing rows to the warehouse
/// (`store_failures` / `store_failures_as: table|view`). Such tests need
/// their audit schema created before the materialization runs.
fn test_stores_failures(nodes: &dbt_schemas::schemas::Nodes, unique_id: &str) -> bool {
    use dbt_schemas::schemas::common::StoreFailuresAs;

    nodes.tests.get(unique_id).is_some_and(|test| {
        let config = &test.deprecated_config;
        matches!(config.store_failures_as, Some(StoreFailuresAs::Table | StoreFailuresAs::View))
            || (config.store_failures_as.is_none() && config.store_failures == Some(true))
    })
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
        NodeType::Seed
        | NodeType::Snapshot
        | NodeType::Test
        | NodeType::UnitTest
        // A function issues CREATE OR REPLACE FUNCTION through statement('main').
        | NodeType::Function => true,
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

/// Materialise generic-test `_dbt_generic_test_kwargs` for the node context.
///
/// The kwargs map is on `TestMetadata`. Each YAML value is converted through
/// `yml_value_to_minijinja_with_jinja` so brace-quoted Jinja expressions
/// (e.g. `"{{ get_where_subquery(ref('m')) }}"`) are evaluated against the
/// current context — matching dbt-fusion's generic-test executor.
fn build_test_kwargs_map(
    meta_kwargs: &BTreeMap<String, dbt_yaml::Value>,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
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

/// Execute a single node against its project's `WorkerState`, without a Temporal
/// activity context — the compile → materialize → result-extract path.
///
/// `execute_node_outer` wraps this with cancellation, heartbeat, and error
/// classification. It is also the entry point for integration tests that drive
/// real nodes against an embedded engine (e.g. the DuckDB scenario harness),
/// where standing up a Temporal `ActivityContext` would add nothing.
#[allow(clippy::too_many_lines, clippy::unused_async)]
// Sequential adapter interaction with setup, execution, and result extraction.
// Kept async so tokio::select! in execute_node_outer can poll it against
// ctx.cancelled() and the heartbeat ticker.
pub async fn execute_node_inner(
    activities: &DbtActivities,
    input: NodeExecutionInput,
) -> Result<NodeExecutionResult, anyhow::Error> {
    let cancel = dbt_common::cancellation::CancellationTokenSource::new();
    execute_node_cancellable(activities, input, &cancel.token()).await
}

/// `execute_node_inner`, with the caller holding the cancellation source.
///
/// The activity wrapper keeps the source so it can cancel the warehouse work
/// from its own task while this one is blocked inside dbt.
pub async fn execute_node_cancellable(
    activities: &DbtActivities,
    input: NodeExecutionInput,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<NodeExecutionResult, anyhow::Error> {
    let state = Arc::clone(activities.registry.get(Some(&input.project))?);

    // The one thing here that needs the async runtime: the deferred manifest
    // comes from the artifact store. Everything after it is synchronous, and
    // runs on a blocking thread so this task stays free to heartbeat and to
    // notice cancellation.
    let defer_nodes =
        load_defer_nodes(activities, &state, input.defer_manifest_ref.as_deref()).await?;

    let token = cancellation.clone();
    // dbt's telemetry data layer asserts that every span it sees descends from
    // an `Invocation` root, and a blocking thread starts with no current span
    // — so the node span has to be re-entered on the far side of the handoff,
    // or the first dbt macro to open a span panics the thread.
    let span = tracing::Span::current();
    let invocation_id = input.invocation_id.clone();
    let mut result = tokio::task::spawn_blocking(move || {
        let _entered = span.enter();
        execute_node_body(&state, &input, defer_nodes.as_deref(), &token)
    })
    .await
    .map_err(|e| anyhow::anyhow!("node execution task failed: {e}"))??;

    spill_compiled_sql(activities, &invocation_id, &mut result).await?;
    Ok(result)
}

/// Move the node's compiled SQL to the artifact store, leaving a reference.
///
/// The workflow accumulates every result, copies them into the checkpoint at
/// each continuation and clones them again for the artifact activity — so a
/// model's SQL crossed Temporal many times over to reach the one place that
/// reads it. Written once here instead, and read back by `store_artifacts`
/// when it assembles `run_results.json`.
///
/// Without artifact storage the SQL stays inline: nothing writes artifacts for
/// that run, so nothing would ever read the reference.
async fn spill_compiled_sql(
    activities: &DbtActivities,
    invocation_id: &str,
    result: &mut NodeExecutionResult,
) -> Result<(), anyhow::Error> {
    if !activities.write_artifacts.0 {
        return Ok(());
    }
    let (Some(store), Some(sql)) =
        (activities.artifact_store.as_ref(), result.compiled_code.take())
    else {
        return Ok(());
    };

    match store
        .store(
            invocation_id,
            &compiled_sql_artifact_name(&result.unique_id),
            Bytes::copy_from_slice(sql.as_bytes()),
        )
        .await
    {
        Ok(path) => {
            result.compiled_code_ref = Some(path);
            Ok(())
        }
        Err(e) => {
            // Put it back rather than losing it: an artifact-store blip should
            // not silently empty `compiled_code` in run_results.json.
            result.compiled_code = Some(sql);
            Err(DbtTemporalError::ArtifactStore(
                e.context(format!("storing compiled SQL for {}", result.unique_id)),
            )
            .into())
        }
    }
}

/// Artifact name for one node's compiled SQL.
pub fn compiled_sql_artifact_name(unique_id: &str) -> String {
    format!("compiled/{unique_id}.sql")
}

/// The synchronous half of a node execution: compile, materialize, decode.
///
/// Runs on a blocking thread. Every Jinja value it builds is thread-affine and
/// never leaves this function, which is why the environment is constructed
/// here rather than handed in.
#[allow(clippy::too_many_lines)]
fn execute_node_body(
    state: &crate::worker_state::WorkerState,
    input: &NodeExecutionInput,
    defer_nodes: Option<&dbt_schemas::schemas::Nodes>,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<NodeExecutionResult, anyhow::Error> {
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

    // Measured, not built. Sources only ever appear in freshness plans; the
    // unified `freshness` command additionally measures models carrying an
    // SLA. Neither compiles model SQL nor materializes anything, so both skip
    // straight from context building to the freshness query.
    let measures_freshness = rt == NodeType::Source
        || (rt == NodeType::Model
            && DbtCommand::parse(&input.command) == Some(DbtCommand::Freshness));

    let start_instant = std::time::Instant::now();

    // --- COMPILE PHASE ---
    let compile_start = chrono::Utc::now();

    // Private Jinja env + adapter for this activity, with the workflow's env
    // vars, --vars and --full-refresh applied. `render_env` must stay alive for
    // the whole activity: it owns the rebuilt engine's cancellation source.
    // `base.adapter` is the node's `+adapter` selection when it made one and the
    // run's default otherwise, so routing on it needs no fallback of its own —
    // and an adapter the target does not declare fails here rather than running
    // against the wrong warehouse.
    let mut render_env = render_env::prepare_render_env(
        state,
        &render_env::RenderOverrides {
            cancellation,
            env: &input.env,
            target: input.target.as_deref(),
            vars: &input.vars,
            full_refresh: input.full_refresh,
        },
        base.adapter,
        unique_id,
    )?;
    let jinja_env = &mut render_env.jinja_env;
    let env_schema = render_env.env_schema.clone();
    let env_database = render_env.env_database.clone();

    // Get namespace keys from the Jinja macro namespace registry.
    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default();

    let base_context = build_base_context(state, defer_nodes, namespace_keys);

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

    let workspace = ActivityWorkspace::new(state, node, &input.invocation_id)?;
    // Destructure so the TempDir guard stays owned by this scope — dropping
    // `workspace` early would delete the directory `io_args` points at.
    let ActivityWorkspace {
        _temp_dir,
        ephemeral_dir,
        io_args,
    } = workspace;

    // Build the full run-phase node context. The returned store is the one the
    // context's `store_result`/`load_result` closures write to, so every later
    // result extraction (adapter_response, test failures, unit-test outcomes)
    // must read from this store and not one built alongside it.
    //
    // The adapter here shapes `this` and the column data types in the context,
    // so it must be the node's own — the project-wide default would hand a node
    // on another adapter a relation rendered in the wrong dialect.
    let (mut node_context, result_store) = dbt_jinja_utils::phases::run::build_run_node_context(
        node,
        &deprecated_config,
        base.adapter,
        agate_table,
        &base_context,
        &io_args,
        dbt_telemetry::ExecutionPhase::Run,
        sql_header,
        state.packages.clone(),
    );

    // Microbatch: replace ref() and source() with time-window-aware versions when
    // event_time_start/end are provided. The materialisation template uses these to
    // filter upstream models/sources to the batch window. Dependency validation is
    // skipped (new_unvalidated) because the node was already validated at planning.
    if let (Some(start_str), Some(end_str)) = (&input.event_time_start, &input.event_time_end) {
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
            common.package_name.clone(),
            Arc::clone(&state.resolver_state.runtime_config),
            dbt_jinja_utils::phases::compile::DependencyValidationConfig::new_unvalidated(),
            microbatch_ctx.clone(),
            unique_id.clone(),
        );
        let mb_source = dbt_jinja_utils::phases::SourceFunction::new_with_microbatch_context(
            Arc::clone(&state.resolver_state.node_resolver),
            common.package_name.clone(),
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
    }

    // Resolve where this node's relation — and every relation its SQL can
    // name — lives for this workflow, then patch `this`, `schema` and
    // `database` in its Jinja context.
    //
    // The compiled text needs the same treatment separately: the startup
    // manifest baked the startup schemas into every upstream `ref()`, and a
    // materialization interpolates that text rather than re-deriving it from
    // `this`.
    let resolved = resolve_relations(
        state,
        jinja_env,
        unique_id,
        env_schema.as_deref(),
        env_database.as_deref(),
    )?;
    if let Some(ref own) = resolved.own {
        apply_relation_to_context(base, own, &mut node_context)
            .map_err(|e| DbtTemporalError::Compilation(format!("{e:#}")))?;
    }
    let relation_rewrite = resolved.rewrite;

    // Resolve raw SQL: build_run_node_context does NOT populate the top-level
    // "sql" context variable — that's the caller's responsibility. The
    // materialization template uses {{ sql }} as the compiled model query
    // (e.g. in `get_create_view_as_sql(target_relation, sql)`). Unit tests
    // have no raw SQL of their own (their path is the defining YAML); their
    // input SQL is assembled below from the tested model + fixtures.
    let raw_sql_result = if rt == NodeType::UnitTest || measures_freshness {
        Ok(String::new())
    } else {
        resolve_raw_sql(state, common, rt)
    };

    // For generic tests, inject _dbt_generic_test_kwargs from test metadata.
    // The primary path uses generated SQL with inlined kwargs (from test_sql_cache),
    // but the raw_code fallback path may reference **_dbt_generic_test_kwargs.
    if rt == NodeType::Test
        && let Some(test) = state.resolver_state.nodes.tests.get(unique_id)
        && let Some(ref meta) = test.__test_attr__.test_metadata
    {
        let kwargs_map = build_test_kwargs_map(&meta.kwargs, jinja_env, &node_context)
            .map_err(|e| DbtTemporalError::Compilation(format!("{e:#}")))?;
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

    if rt == NodeType::UnitTest {
        let unit = state
            .resolver_state
            .nodes
            .unit_tests
            .get(unique_id)
            .ok_or_else(|| {
                DbtTemporalError::ProjectNotFound(format!("unit test not found: {unique_id}"))
            })?;
        let sql = unit_test::build_unit_test_sql(
            state,
            unit,
            &state.resolver_state.nodes,
            jinja_env,
            &node_context,
            &state.io_args.in_dir,
            &ephemeral_dir,
        )?;
        let sql = rewrite_relations(sql, &relation_rewrite);
        // minijinja Values share their string via Arc — clone the Value, not the SQL.
        let sql_value = minijinja::Value::from(sql);
        node_context.insert("sql".to_owned(), sql_value.clone());
        node_context.insert("compiled_code".to_owned(), sql_value);
    }

    match raw_sql_result {
        Ok(_) if rt == NodeType::UnitTest || measures_freshness => {
            // Unit test SQL is assembled above; freshness nodes have no
            // runnable SQL of their own (their queries come from macros).
        }
        Ok(raw_sql) if !raw_sql.trim().is_empty() => {
            // Use the model's original file path as the rendering filename so any
            // Jinja error references the source file rather than `<unknown>`.
            let render_filename = state.io_args.in_dir.join(&common.original_file_path);
            let compiled = dbt_jinja_utils::utils::render_sql_with_listeners(
                &raw_sql,
                jinja_env,
                &node_context,
                &[],
                &[],
                &render_filename,
            )
            .map_err(|e| {
                // Rendering is not only templating: a model body can call
                // `run_query`, `adapter.get_relation` or another introspection
                // macro, so a warehouse that is briefly unreachable fails
                // *here*. Treating every render failure as permanent put those
                // outside the retry contract entirely — the same distinction
                // the project-hook path already makes.
                //
                // `FsResult<T> = Result<T, Box<FsError>>`, so the deref keeps
                // the concrete type the classifier downcasts for.
                crate::error::classify_adapter_execution_error(
                    &*e,
                    &format!("compiling SQL for {unique_id}"),
                )
            })?;
            // Inject ephemeral model CTEs (ref('ephemeral_model') → __dbt__cte__<name>).
            let compiled = inject_ephemeral_ctes(
                &compiled,
                &common.name,
                &base.depends_on.nodes,
                &state.resolver_state.nodes,
                jinja_env,
                &node_context,
                super::node_helpers::EphemeralDirs {
                    in_dir: &state.io_args.in_dir,
                    ephemeral_dir: &ephemeral_dir,
                },
            )?;
            // Resolve the relations the startup manifest baked into this text —
            // the upstream `ref()`s that still name startup schemas.
            let compiled = rewrite_relations(compiled, &relation_rewrite);

            // Write compiled SQL to the temp dir so model.compiled_code / model.compiled_sql
            // resolve correctly when accessed by the materialization template.
            let dest = compiled_sql_path(node, &io_args.in_dir, &io_args.out_dir);
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

            // Set both `sql` and `compiled_code` in the context. View materializations
            // reference `sql`, while table/incremental materializations reference
            // `compiled_code` (passed to create_table_as / bq_create_table_as).
            // minijinja Values share their string via Arc — clone the Value, not the SQL.
            let compiled_value = minijinja::Value::from(compiled);
            node_context.insert("sql".to_owned(), compiled_value.clone());
            node_context.insert("compiled_code".to_owned(), compiled_value);
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
    if DbtCommand::parse(&input.command) == Some(DbtCommand::Compile) {
        let execution_time = start_instant.elapsed().as_secs_f64();
        info!(node = %unique_id, time_secs = execution_time, "node compiled (compile-only)");
        return Ok(compile_only_result(
            unique_id,
            &node_context,
            execution_time,
            (compile_start, compile_end),
        ));
    }

    // --- EXECUTE PHASE ---
    let execute_start = chrono::Utc::now();

    // Run the freshness check instead of a materialization and return early.
    // A stale node (error_after exceeded) reports `NodeStatus::Error` with its
    // measurement attached, rather than failing the activity — see
    // `domain_failure_message`. A warning succeeds, because `NodeStatus` has no
    // warning state; the "warn" survives on the outcome and in the node
    // message, which is what the run log and run_results.json show.
    if measures_freshness {
        let freshness_node = freshness::as_freshness_node(
            &state.resolver_state.nodes,
            unique_id,
            rt,
        )
        .ok_or_else(|| {
            DbtTemporalError::ProjectNotFound(format!("freshness node not found: {unique_id}"))
        })?;
        let verdict = freshness::run_freshness_check(freshness_node, jinja_env, &node_context)?;
        let execute_end = chrono::Utc::now();
        let execution_time = start_instant.elapsed().as_secs_f64();
        let (status, message, outcome) = grade_freshness(unique_id, rt, verdict);
        info!(node = %unique_id, message = %message, "freshness check complete");
        return Ok(NodeExecutionResult {
            unique_id: unique_id.clone(),
            status,
            execution_time,
            message: Some(message),
            adapter_response: extract_adapter_response(&result_store),
            compiled_code: None,
            timing: build_timing_entries(compile_start, compile_end, execute_start, execute_end),
            failures: None,
            freshness: Some(outcome),
            compiled_code_ref: None,
            relation_name: executed_relation_name(&node_context),
        });
    }

    // Ensure the target schema/dataset exists (dbt does this before materializations).
    // Dispatches to the adapter-specific create_schema macro (e.g. CREATE SCHEMA IF NOT EXISTS).
    // Uses `this` which is the target relation (database + schema + identifier).
    // Tests normally only read, but with store_failures they write failing
    // rows into the audit schema — which may not exist yet.
    let needs_schema = is_create_schema_eligible(rt)
        || (rt == NodeType::Test && test_stores_failures(&state.resolver_state.nodes, unique_id));
    if needs_schema {
        ensure_target_schema(state, &input.invocation_id, base, jinja_env, &node_context)?;
    }

    // Resolve the materialization template using dbt-fusion's MaterializationResolver.
    // Dispatches with adapter prefix inheritance (e.g. redshift→postgres→default) and
    // package precedence (Root > Imported > Core for builtins).
    let materialization = select_materialization_name(rt, &base.materialized.to_string());

    // Extract compiled SQL from the node context before rendering.
    let compiled_sql = node_context
        .get("sql")
        .and_then(|v| v.as_str().map(ToString::to_string));

    // Resolve against the adapter the node actually runs on: `base.adapter` is the
    // node's `+adapter` selection when it made one, and the run's default adapter
    // otherwise. The resolver holds no default of its own.
    let fq_name = state
        .materialization_resolver
        .find_materialization_macro_by_name(&materialization, base.adapter)
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "no materialization found for node {unique_id} (materialization={materialization}): {e:#}"
            ))
        })?;

    // Render the materialization template (triggers SQL execution through BridgeAdapter).
    let rendered = render_materialization(jinja_env, &fq_name, &node_context)?;

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

    // A terminal outcome dbt reports on the node rather than an execution
    // failure to retry — see `domain_failure_message`. Set by the unit-test and
    // data-test verdicts below; when present it becomes the node's message and
    // its status is `Error`.
    let mut domain_failure: Option<String> = None;
    // A node that completed and found something the operator should see — a
    // test over `warn_if` but under `error_if`. Reported apart from success:
    // folding the two together made a run that found something look like one
    // that found nothing.
    let mut warned = false;

    // Unit tests: compare the actual vs expected partitions of the executed
    // union query. A difference is the test's answer, not a fault to retry —
    // fixtures and model SQL do not change between attempts.
    let unit_outcome = if rt == NodeType::UnitTest {
        let outcome = unit_test::extract_unit_test_outcome(&result_store)?;
        if !outcome.passed {
            domain_failure = Some(domain_failure_message(&DbtTemporalError::UnitTestFailure {
                unique_id: unique_id.clone(),
                failures: outcome.failures,
                diff: outcome.diff.clone(),
            }));
        }
        Some(outcome)
    } else {
        None
    };

    // For test nodes, read the result table the test materialization stored
    // (not rows_affected, which is always 1 — one row returned, not the count).
    // It carries the failure count and the two verdicts the warehouse computed
    // from `warn_if` / `error_if`.
    let test_outcome = if rt == NodeType::Test {
        Some(extract_test_outcome(&result_store)?)
    } else {
        None
    };
    let failures = test_outcome.map(|o| o.failures);

    // The test's verdict, on dbt's terms: it fails only when its severity is
    // `error` *and* the `error_if` expression the warehouse evaluated came back
    // true, and otherwise `warn_if` decides whether it warns. The failure count
    // decides nothing by itself — a test configured `error_if: ">100"` is
    // passing at 100 failures, and only the SQL knows that.
    //
    // Mirrors upstream `reported_test_verdict_from_components`.
    if let Some(outcome) = test_outcome {
        use dbt_schemas::schemas::common::Severity;
        let severity = state
            .resolver_state
            .nodes
            .tests
            .get(unique_id)
            .and_then(|t| t.deprecated_config.severity.as_ref())
            .cloned()
            .unwrap_or_default();
        if matches!(severity, Severity::Error) && outcome.should_error {
            domain_failure = Some(domain_failure_message(&DbtTemporalError::TestFailure {
                unique_id: unique_id.clone(),
                failures: outcome.failures,
            }));
        }
        if outcome.should_warn {
            warn!(
                node = %unique_id,
                failures = outcome.failures,
                severity = ?severity,
                "test warning"
            );
            warned = true;
        }
    }

    // Build a human-readable message from the adapter response for the Temporal UI.
    // Falls back to materialization type when the adapter doesn't return metadata
    // (e.g. ephemeral models that never execute against the warehouse).
    let message = domain_failure.clone().or_else(|| {
        unit_outcome.map_or_else(
            || build_success_message(&adapter_response, &materialization),
            |o| Some(format!("unit test passed ({} row(s) compared)", o.actual_rows)),
        )
    });

    let status = match (domain_failure.is_some(), warned) {
        (true, _) => NodeStatus::Error,
        (false, true) => NodeStatus::Warn,
        (false, false) => NodeStatus::Success,
    };

    if let Some(reason) = domain_failure.as_deref() {
        warn!(node = %unique_id, time_secs = execution_time, reason, "node failed");
    } else {
        info!(
            node = %unique_id,
            time_secs = execution_time,
            message = message.as_deref().unwrap_or("-"),
            "node execution complete"
        );
    }

    Ok(NodeExecutionResult {
        unique_id: unique_id.clone(),
        status,
        execution_time,
        message,
        adapter_response,
        compiled_code,
        timing,
        failures,
        freshness: None,
        compiled_code_ref: None,
        relation_name: executed_relation_name(&node_context),
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

    use crate::activities::retry::{
        RetryDecision, Unclassified, decide_retry, downcast_or_default,
        registry_non_retryable_patterns, to_activity_error,
    };
    use crate::error::compile_error_patterns;
    use crate::project_registry::ProjectRegistry;

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
        let recovered = downcast_or_default(any, Unclassified::RetryAsAdapter);
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
        let recovered = downcast_or_default(any, Unclassified::RetryAsAdapter);
        assert!(matches!(recovered, DbtTemporalError::TestFailure { .. }));
    }

    #[test]
    fn downcast_or_wrap_falls_back_to_adapter_for_plain_anyhow() {
        let any: anyhow::Error = anyhow::anyhow!("a plain error not from us");
        let recovered = downcast_or_default(any, Unclassified::RetryAsAdapter);
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
        let activity_err = to_activity_error(&err, &empty_patterns());
        assert!(
            matches!(activity_err, ActivityError::Application(ref af) if !af.is_non_retryable())
        );
    }

    #[test]
    fn classify_for_temporal_marks_compilation_as_non_retryable() {
        let err = DbtTemporalError::Compilation("bad ref".into());
        let activity_err = to_activity_error(&err, &empty_patterns());
        assert!(
            matches!(activity_err, ActivityError::Application(ref af) if af.is_non_retryable())
        );
    }

    #[test]
    fn classify_for_temporal_promotes_pattern_match_to_non_retryable() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("permission denied for table foo"));
        let patterns = compile_error_patterns(&["permission denied".to_string()]);
        let activity_err = to_activity_error(&err, &patterns);
        assert!(
            matches!(activity_err, ActivityError::Application(ref af) if af.is_non_retryable())
        );
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

    // --- test_stores_failures ---

    fn nodes_with_test(
        store_failures: Option<bool>,
        store_failures_as: Option<dbt_schemas::schemas::common::StoreFailuresAs>,
    ) -> dbt_schemas::schemas::Nodes {
        use dbt_schemas::schemas::nodes::DbtTest;

        let mut test = DbtTest::default();
        test.deprecated_config.store_failures = store_failures;
        test.deprecated_config.store_failures_as = store_failures_as;
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes.tests.insert("test.p.t".to_string(), Arc::new(test));
        nodes
    }

    #[test]
    fn test_stores_failures_on_flag_or_persistent_as() {
        use dbt_schemas::schemas::common::StoreFailuresAs;

        assert!(test_stores_failures(&nodes_with_test(Some(true), None), "test.p.t"));
        assert!(test_stores_failures(
            &nodes_with_test(None, Some(StoreFailuresAs::Table)),
            "test.p.t"
        ));
        assert!(test_stores_failures(
            &nodes_with_test(Some(false), Some(StoreFailuresAs::View)),
            "test.p.t"
        ));
    }

    #[test]
    fn test_stores_failures_off_for_ephemeral_default_or_unknown() {
        use dbt_schemas::schemas::common::StoreFailuresAs;

        assert!(!test_stores_failures(&nodes_with_test(None, None), "test.p.t"));
        assert!(!test_stores_failures(&nodes_with_test(Some(false), None), "test.p.t"));
        // store_failures_as: ephemeral never persists, even with the flag on.
        assert!(!test_stores_failures(
            &nodes_with_test(Some(true), Some(StoreFailuresAs::Ephemeral)),
            "test.p.t"
        ));
        assert!(!test_stores_failures(&nodes_with_test(Some(true), None), "test.p.missing"));
    }

    // --- build_test_kwargs_map ---

    fn empty_jinja_env() -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(minijinja::Environment::new())
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
