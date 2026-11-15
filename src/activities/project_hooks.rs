//! Project-level hook execution: `on-run-start` and `on-run-end` from `dbt_project.yml`.
//!
//! Each hook is a Jinja template (e.g. `"{{ log_run_results() }}"`) stored in
//! `state.resolver_state.operations.on_run_start` / `.on_run_end` as `DbtOperation`
//! nodes. We render each one with the full compile+run Jinja context. Side-effect
//! macros (`run_query`, `statement`, `log`) execute against the warehouse during
//! rendering. If the rendered output is non-empty SQL, we execute it directly.
//!
//! For `on-run-end`, the `results`, `schemas` and `database_schemas` Jinja
//! variables are built from the workflow's `node_results` and match what dbt
//! hands the hook: result objects with dbt's status words and the executed
//! `node`, and the schemas of the relations the run built.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::info;

use crate::artifact_store::ArtifactStore;
use crate::types::{NodeExecutionResult, NodeStatus, ProjectHookPhase, ProjectHooksInput};

use crate::error::DbtTemporalError;

use super::DbtActivities;
use super::cancellable;
use super::node_helpers::json_to_minijinja;
use super::render_env;
use super::retry::{self, RetryDecision};
use super::store_artifacts::dbt_status;
use crate::worker_state::WorkerState;

/// Whether a failed hook should be handed back to Temporal as retryable.
///
/// Two gates, both of which must pass. `retry_on_error` is the project author's
/// opt-in for this phase — hook SQL is only safe to re-run if it is idempotent,
/// which this crate cannot determine. The second gate is the same
/// classification nodes use, so even an opted-in hook retries only on the
/// genuinely transient variants; bad SQL is permanent on the first attempt.
fn hook_retry_decision(
    err: &DbtTemporalError,
    retry_on_error: bool,
    non_retryable_patterns: &[regex::Regex],
) -> RetryDecision {
    if !retry_on_error {
        return RetryDecision::NoRetry;
    }
    retry::decide_retry(err, non_retryable_patterns)
}

/// Outer wrapper — handles the `Result` translation, cancellation, and heartbeating.
///
/// Hook errors are non-retryable unless the project opted this phase in via
/// `retry.project_hooks` — re-running a hook repeats its side effects, and only
/// the author knows whether that is safe. A cancelled activity signals its
/// cancellation token so the hook SQL in flight is interrupted, then waits for
/// the hook to stop before reporting cancellation.
pub async fn run_project_hooks_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: ProjectHooksInput,
) -> Result<(), ActivityError> {
    let phase = input.phase;
    let retry_on_error = input.retry_on_error;
    let project = input.project.clone();

    // This activity's own cancellation source: the render environment's adapter
    // engine takes its token, so cancelling the activity interrupts the hook SQL
    // in flight. The source outlives the work because the engine holds only a
    // `Weak` reference to it.
    let cancel = dbt_common::cancellation::CancellationTokenSource::new();
    let token = cancel.token();
    let (result, cancelled) = cancellable::run_in_activity(
        &ctx,
        &cancel,
        || info!(phase = %phase, "project hooks cancelled — stopping warehouse work"),
        run_project_hooks_cancellable(activities, input, &token),
    )
    .await;

    if cancelled {
        return Err(ActivityError::cancelled());
    }
    result.map_err(|e| {
        let patterns = retry::registry_non_retryable_patterns(&activities.registry, &project);
        // An untyped hook failure is a template or SQL problem, not a
        // blip, so it stays permanent even when retry is enabled.
        let dbt_err = retry::downcast_or_default(e, retry::Unclassified::Permanent);
        let decision =
            hook_retry_decision(&dbt_err, retry_on_error, patterns.as_deref().unwrap_or(&[]));
        if decision == RetryDecision::Retry {
            info!(phase = %phase, error = %dbt_err, "hook failed, retrying");
        }
        retry::failure_for(&dbt_err, decision)
    })
}

/// Render and execute one phase's `dbt_project.yml` hooks against a project's
/// `WorkerState`, without a Temporal activity context.
///
/// `run_project_hooks_outer` wraps this with cancellation and heartbeat. Also
/// the entry point for integration tests that drive hooks against an embedded
/// engine (e.g. the DuckDB scenario harness); nothing cancels the hooks here.
pub async fn run_project_hooks_inner(
    activities: &DbtActivities,
    input: ProjectHooksInput,
) -> Result<(), anyhow::Error> {
    let cancel = dbt_common::cancellation::CancellationTokenSource::new();
    run_project_hooks_cancellable(activities, input, &cancel.token()).await
}

/// `run_project_hooks_inner`, with the caller holding the cancellation source.
///
/// The activity wrapper keeps the source so it can interrupt the hook SQL from
/// its own task while this one is blocked inside dbt. The caller must keep the
/// source alive until this returns.
async fn run_project_hooks_cancellable(
    activities: &DbtActivities,
    input: ProjectHooksInput,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<(), anyhow::Error> {
    let state = Arc::clone(activities.registry.get(Some(&input.project))?);
    // Store I/O is async and the pool closure is synchronous, so the earlier
    // segments are read before handing off.
    let input = with_prior_segments(activities.artifact_store.as_deref(), input).await?;
    // Hook SQL runs on the dbt pool, so this task stays free to heartbeat and
    // to notice cancellation.
    let token = cancellation.clone();
    crate::dbt_pool::run(move || run_project_hooks_body(&state, &input, &token)).await?
}

/// Prepend every earlier segment's results to this one's, so `results` in an
/// `on-run-end` hook describes the whole run.
///
/// Oldest segment first, matching the order `run_results.json` is assembled in.
/// A missing store or an unreadable checkpoint is an error rather than a
/// shorter `results`: a hook that summarises or alerts on the run would
/// silently under-report. A store read failure is transient I/O and retryable
/// (subject to the phase's own retry opt-in); a missing store is configuration.
async fn with_prior_segments(
    store: Option<&dyn ArtifactStore>,
    mut input: ProjectHooksInput,
) -> Result<ProjectHooksInput, anyhow::Error> {
    if input.prior_segments.is_empty() {
        return Ok(input);
    }
    let store = store.ok_or_else(|| {
        DbtTemporalError::Configuration(
            "on-run-end for a continued run requires artifact storage to read earlier segments"
                .to_string(),
        )
    })?;

    let mut results = Vec::with_capacity(input.node_results.len());
    for state_ref in &input.prior_segments {
        let (segment_results, _log) = super::segment_state::read_segment_payload(store, state_ref)
            .await
            .map_err(|e| {
                DbtTemporalError::ArtifactStore(
                    e.context("collecting an earlier segment for on-run-end hooks"),
                )
            })?;
        results.extend(segment_results);
    }
    info!(
        segments = input.prior_segments.len(),
        results = results.len(),
        "collected earlier segments for on-run-end hooks"
    );

    results.append(&mut input.node_results);
    input.node_results = results;
    Ok(input)
}

/// The synchronous half of [`run_project_hooks_inner`]: render and run each
/// hook in order.
// Sequential render-then-execute over the phase's hooks.
fn run_project_hooks_body(
    state: &WorkerState,
    input: &ProjectHooksInput,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<(), anyhow::Error> {
    // Hooks render Jinja and run SQL, so they need an `Invocation` root span for
    // the same reason node execution does. Sharing the run's invocation id puts
    // them in the same trace as the nodes they bracket.
    let _invocation = super::node_telemetry::invocation_span(
        &input.invocation_id,
        &format!("dbt {}", input.phase),
    )
    .entered();

    info!(
        phase = %input.phase,
        project = %input.project,
        invocation_id = %input.invocation_id,
        "running project hooks"
    );

    // ── Jinja env setup ─────────────────────────────────────────────────
    // Shared with execute_node so hooks and nodes render against identical
    // globals; see `render_env` for what the per-workflow overrides cover.
    let phase = input.phase.to_string();
    // Project hooks run on the target's default adapter. They are declared in
    // `dbt_project.yml` at project level, not on a node, so there is no
    // `+adapter` selection to honour — the same reason dbt runs them against the
    // run's default connection.
    let mut render_env = render_env::prepare_render_env(
        state,
        &render_env::RenderOverrides {
            cancellation,
            env: &input.env,
            target: input.target.as_deref(),
            vars: &input.vars,
            full_refresh: input.full_refresh,
            invocation_id: &input.invocation_id,
            run_started_at: input.run_started_at,
        },
        state.adapter_engines.default_type(),
        &phase,
    )?;
    let jinja_env = &mut render_env.jinja_env;
    let adapter = Arc::clone(&render_env.adapter);

    // ── Build base context ───────────────────────────────────────────────
    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default();

    let mut context = dbt_jinja_utils::phases::build_operation_context_btreemap(
        Arc::clone(&state.resolver_state.node_resolver),
        &state.resolver_state.root_project_name,
        &state.resolver_state.nodes,
        None, // defer_nodes: not using deferred state
        Arc::clone(&state.resolver_state.runtime_config),
        namespace_keys,
        None,
    );

    // Inject `model` as an empty-attribute placeholder. Project-level hooks run before
    // any node executes (on-run-start) or after all finish (on-run-end), so `model` is
    // not in scope. dbt-core injects `model` with empty attributes, making expressions
    // like `{{ "schema=" + model.schema }}` render as "schema=" instead of erroring.
    // Without this, `model.schema` is undefined and the `+` string operator fails.
    context.insert("model".to_owned(), empty_model_context());

    // For on_run_end: inject `results`, `schemas` and `database_schemas` as dbt does.
    if matches!(input.phase, ProjectHookPhase::OnRunEnd) {
        let nodes = &state.resolver_state.nodes;
        context.insert("results".to_owned(), build_results_context(&input.node_results, nodes));
        let built = built_schemas(&input.node_results, nodes);
        context.insert("schemas".to_owned(), schemas_context(&built));
        context.insert("database_schemas".to_owned(), database_schemas_context(&built));
    }

    // ── Select hook list ─────────────────────────────────────────────────
    let operations = match input.phase {
        ProjectHookPhase::OnRunStart => &state.resolver_state.operations.on_run_start,
        ProjectHookPhase::OnRunEnd => &state.resolver_state.operations.on_run_end,
    };

    if operations.is_empty() {
        // Defensive: workflow should skip the activity call when no hooks exist,
        // but this path also handles direct activity invocations.
        return Ok(());
    }

    // ── Execute each hook sequentially ───────────────────────────────────
    for (idx, op) in operations.iter().enumerate() {
        // Spanned<DbtOperation> derefs to DbtOperation via Deref.
        let raw_code = op.__common_attr__.raw_code.as_deref().unwrap_or("").trim();
        if raw_code.is_empty() {
            continue;
        }

        info!(phase = %input.phase, idx, "rendering hook");

        // Render — `run_query`/`statement`/`log` macros execute as a side
        // effect, so a transient warehouse failure (dropped connection,
        // throttling) can surface here. Classify it the same way
        // `render_materialization` does, so the AdapterError signal isn't
        // lost behind a blanket permanent `Compilation` error.
        let rendered = jinja_env.render_str(raw_code, &context, &[]).map_err(|e| {
            // `render_str` returns `FsResult<T> = Result<T, Box<FsError>>` — pass
            // the dereferenced `FsError` so its concrete type is preserved for
            // the classifier's `downcast_ref`; passing `&e` directly would make
            // the trait object's concrete type `Box<FsError>` instead.
            crate::error::classify_adapter_execution_error(
                &*e,
                &format!("{} hook[{idx}]", input.phase),
            )
        })?;

        // If rendering produced non-empty SQL (e.g. raw `create table ...` strings),
        // execute it directly. Pure logging hooks render to empty.
        let sql = rendered.trim();
        if !sql.is_empty() {
            let ctx = dbt_adbc::QueryCtx::new(format!("{} hook[{idx}]", input.phase));
            adapter
                // No adapter options: hook SQL runs on the connection's own
                // session settings, same as before upstream added the parameter.
                .execute_without_state(Some(&ctx), sql, false, None)
                .map_err(|e| {
                    crate::error::classify_adapter_execution_error(
                        &e,
                        &format!("{} hook[{idx}] direct SQL failed", input.phase),
                    )
                })?;
        }

        info!(phase = %input.phase, idx, "hook complete");
    }

    Ok(())
}

/// Create a `model` placeholder for project-level hook rendering.
///
/// Returns an object where any attribute access returns an empty string, matching
/// dbt-core's behaviour: when `model` is not in scope, macros that read `model.schema`,
/// `model.name`, etc. get empty strings rather than undefined errors.
fn empty_model_context() -> minijinja::Value {
    use std::sync::Arc;

    #[derive(Debug)]
    struct EmptyModel;

    impl minijinja::value::Object for EmptyModel {
        fn get_value(self: &Arc<Self>, _key: &minijinja::Value) -> Option<minijinja::Value> {
            Some(minijinja::Value::from(""))
        }
    }

    minijinja::Value::from_object(EmptyModel)
}

/// Convert the workflow's `Vec<NodeExecutionResult>` into the `results` list an
/// `on-run-end` hook sees.
///
/// Each item has the attributes of dbt's run result:
/// - `status`, in the words `run_results.json` uses (`success`, `error`,
///   `skipped`, `pass`, `fail`, `warn`, ...)
/// - `unique_id`, `execution_time`, `message`, `failures`, `thread_id`,
///   `compiled_code`
/// - `timing`, a list of `name` / `started_at` / `completed_at`
/// - `adapter_response`, the warehouse-specific response fields
/// - `node`, the resolved node as dbt serializes it (`name`, `resource_type`,
///   `config`, ...)
///
/// The node is looked up in the worker's resolver state at render time rather
/// than carried through Temporal workflow history.
fn build_results_context(
    results: &[NodeExecutionResult],
    nodes: &dbt_schemas::schemas::Nodes,
) -> minijinja::Value {
    let items: Vec<minijinja::Value> = results
        .iter()
        .map(|r| {
            let mut map = BTreeMap::<String, minijinja::Value>::new();

            map.insert("unique_id".to_owned(), minijinja::Value::from(r.unique_id.clone()));
            map.insert("status".to_owned(), minijinja::Value::from(dbt_status(r).as_str()));
            map.insert("execution_time".to_owned(), minijinja::Value::from(r.execution_time));
            map.insert("message".to_owned(), optional(r.message.as_deref()));
            map.insert("failures".to_owned(), optional(r.failures));
            map.insert("compiled_code".to_owned(), optional(r.compiled_code.as_deref()));
            // The artifact writer names the thread the same way; see its comment.
            map.insert("thread_id".to_owned(), minijinja::Value::from("main"));
            map.insert(
                "timing".to_owned(),
                minijinja::Value::from(r.timing.iter().map(timing_value).collect::<Vec<_>>()),
            );

            // adapter_response as a flat map (rows_affected, message, code, query_id...).
            let resp: BTreeMap<String, minijinja::Value> = r
                .adapter_response
                .iter()
                .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
                .collect();
            map.insert("adapter_response".to_owned(), minijinja::Value::from(resp));

            map.insert("node".to_owned(), node_value(&r.unique_id, nodes));

            minijinja::Value::from(map)
        })
        .collect();
    minijinja::Value::from(items)
}

/// `none` for an absent value, as dbt's result objects hold.
fn optional<T: Into<minijinja::Value>>(value: Option<T>) -> minijinja::Value {
    value.map_or_else(|| minijinja::Value::from(()), Into::into)
}

/// One timing entry. The instants are naive datetimes, as dbt hands them, so
/// a hook can call `strftime` on them; an instant that does not parse is `none`.
fn timing_value(timing: &crate::types::TimingEntry) -> minijinja::Value {
    use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;

    let instant = |text: &str| {
        text.parse::<chrono::DateTime<chrono::Utc>>().map_or_else(
            |_| minijinja::Value::from(()),
            |at| minijinja::Value::from_object(PyDateTime::new_naive(at.naive_utc())),
        )
    };
    let mut entry = BTreeMap::<String, minijinja::Value>::new();
    entry.insert("name".to_owned(), minijinja::Value::from(timing.name.clone()));
    entry.insert("started_at".to_owned(), instant(&timing.started_at));
    entry.insert("completed_at".to_owned(), instant(&timing.completed_at));
    minijinja::Value::from(entry)
}

/// The `node` of a result: the resolved node as dbt serializes it, so
/// `node.config`, `node.tags` and the rest read as they do in dbt.
///
/// A result whose node is not in the resolver (a node of another project, or a
/// run planned by an older worker) carries only its `unique_id`.
fn node_value(unique_id: &str, nodes: &dbt_schemas::schemas::Nodes) -> minijinja::Value {
    let Some(node) = nodes.get_node(unique_id) else {
        let mut minimal = BTreeMap::<String, minijinja::Value>::new();
        minimal.insert("unique_id".to_owned(), minijinja::Value::from(unique_id.to_owned()));
        return minijinja::Value::from(minimal);
    };
    let mut serialized = node.serialize_keep_none();
    if let dbt_yaml::Value::Mapping(map, _) = &mut serialized {
        // dbt's serialization spells the type as the node's own variant name;
        // macros compare it with the lower-case words ("model", "seed", ...).
        let resource_type = node.resource_type().as_str_name().to_ascii_lowercase();
        let resource_type = resource_type
            .strip_prefix("node_type_")
            .unwrap_or(&resource_type)
            .to_string();
        map.insert("resource_type".into(), dbt_yaml::Value::string(resource_type));
    }
    minijinja::Value::from_serialize(serialized)
}

/// The `(database, schema)` pairs of the relations the run built, which is what
/// dbt hands `on-run-end` as `database_schemas` (and, without the database, as
/// `schemas`).
///
/// Models, seeds and snapshots that succeeded, as in dbt. The schema is the
/// one the node wrote to when it recorded it, because a run that overrides the
/// target or the profile's env builds elsewhere than the worker's startup
/// metadata says.
fn built_schemas(
    results: &[NodeExecutionResult],
    nodes: &dbt_schemas::schemas::Nodes,
) -> BTreeSet<(String, String)> {
    results
        .iter()
        .filter(|r| r.status == NodeStatus::Success)
        .filter(|r| {
            ["model.", "seed.", "snapshot."]
                .iter()
                .any(|p| r.unique_id.starts_with(p))
        })
        .filter_map(|r| {
            if let Some((database, schema, _)) = super::catalog::executed_relation(r) {
                return Some((database, schema));
            }
            let base = nodes.get_node(&r.unique_id)?.base();
            Some((base.database.clone(), base.schema.clone()))
        })
        .collect()
}

/// `schemas`: the distinct schema names, in order.
fn schemas_context(built: &BTreeSet<(String, String)>) -> minijinja::Value {
    let schemas: BTreeSet<&str> = built.iter().map(|(_, schema)| schema.as_str()).collect();
    minijinja::Value::from_iter(schemas)
}

/// `database_schemas`: `[database, schema]` pairs.
fn database_schemas_context(built: &BTreeSet<(String, String)>) -> minijinja::Value {
    minijinja::Value::from_serialize(built)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeStatus;

    /// `empty_model_context` returns an object where any attribute access yields `""`.
    /// This lets on-run-start/on-run-end macros use `model.schema`, `model.name`, etc.
    /// without erroring — matching dbt-core's behaviour where `model` is an empty context
    /// object rather than absent from the rendering context.
    #[test]
    fn empty_model_context_allows_model_attr_in_hook_templates() -> anyhow::Result<()> {
        let env = minijinja::Environment::new();
        let mut ctx: BTreeMap<String, minijinja::Value> = BTreeMap::new();
        ctx.insert("model".to_owned(), empty_model_context());

        // `model.schema` should return empty string, making `+` concatenation work.
        let rendered = env
            .template_from_str(r#"{{ "s=" + model.schema }}"#)?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "s=", "model.schema should render as empty string");

        // Any attribute access returns empty string.
        let rendered = env
            .template_from_str(r"{{ model.name }}")?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "", "model.name should render as empty string");

        // `model is defined` is true — the placeholder is a real value.
        let rendered = env
            .template_from_str(r"{% if model is defined %}yes{% else %}no{% endif %}")?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "yes", "model should be defined");

        Ok(())
    }

    fn mk_result(
        unique_id: &str,
        status: NodeStatus,
        failures: Option<i64>,
    ) -> NodeExecutionResult {
        NodeExecutionResult {
            unique_id: unique_id.to_string(),
            status,
            execution_time: 1.5,
            message: Some(format!("{status:?} message")),
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }
    }

    #[test]
    fn results_context_renders_via_jinja() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let results = vec![
            mk_result("model.pkg.customers", NodeStatus::Success, None),
            mk_result("test.pkg.unique_customers", NodeStatus::Error, Some(2)),
        ];

        let value = build_results_context(&results, &nodes);

        // Exercise the value through Jinja the way real macros do.
        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        // Length check — `results | length` is the canonical "node count" pattern.
        let len = env
            .template_from_str("{{ results | length }}")?
            .render(&ctx, &[])?;
        assert_eq!(len, "2");

        // Field access on the first item.
        let first_status = env
            .template_from_str("{{ results[0].status }}")?
            .render(&ctx, &[])?;
        assert_eq!(first_status, "success");

        let first_unique_id = env
            .template_from_str("{{ results[0].unique_id }}")?
            .render(&ctx, &[])?;
        assert_eq!(first_unique_id, "model.pkg.customers");

        // node.unique_id — matches the bigquery example macro pattern.
        let first_node_id = env
            .template_from_str("{{ results[0].node.unique_id }}")?
            .render(&ctx, &[])?;
        assert_eq!(first_node_id, "model.pkg.customers");

        // failures on the failing test.
        let second_failures = env
            .template_from_str("{{ results[1].failures }}")?
            .render(&ctx, &[])?;
        assert_eq!(second_failures, "2");

        // Iteration with status filtering — the actual log_run_results pattern.
        // An assertion that errors reports dbt's word for it, `fail`.
        let summary = env
            .template_from_str(
                r"{% set ns = namespace(pass=0, error=0) %}\
{% for r in results %}\
{% if r.status == 'success' %}{% set ns.pass = ns.pass + 1 %}{% endif %}\
{% if r.status == 'fail' %}{% set ns.error = ns.error + 1 %}{% endif %}\
{% endfor %}\
pass={{ ns.pass }} error={{ ns.error }}",
            )?
            .render(&ctx, &[])?;
        assert!(summary.contains("pass=1"), "summary should report 1 pass: {summary}");
        assert!(summary.contains("error=1"), "summary should report 1 error: {summary}");

        Ok(())
    }

    #[test]
    fn empty_results_renders_as_empty_list() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let value = build_results_context(&[], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        let len = env
            .template_from_str("{{ results | length }}")?
            .render(&ctx, &[])?;
        assert_eq!(len, "0");

        // `results is defined` should be true even for empty list.
        let defined = env
            .template_from_str("{% if results is defined %}yes{% else %}no{% endif %}")?
            .render(&ctx, &[])?;
        assert_eq!(defined, "yes");

        Ok(())
    }

    #[test]
    fn results_with_no_message_renders_none() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut r = mk_result("model.pkg.foo", NodeStatus::Skipped, None);
        r.message = None;
        let value = build_results_context(&[r], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        // None message renders as 'none' in minijinja's lenient mode.
        let result = env
            .template_from_str("{% if results[0].message %}has-msg{% else %}no-msg{% endif %}")?
            .render(&ctx, &[])?;
        assert_eq!(result, "no-msg");
        Ok(())
    }

    #[test]
    fn node_lookup_enriches_with_name_and_resource_type() -> anyhow::Result<()> {
        // When the node IS in the resolver, build_results_context fills in
        // name + package_name + resource_type (lower-cased, stripped of the
        // proto prefix).
        use std::sync::Arc;

        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        let common = CommonAttributes {
            unique_id: "model.shop.orders".to_string(),
            name: "orders".to_string(),
            package_name: "shop".to_string(),
            ..CommonAttributes::default()
        };
        nodes.models.insert(
            "model.shop.orders".to_string(),
            Arc::new(DbtModel {
                __common_attr__: common,
                ..DbtModel::default()
            }),
        );

        let result = mk_result("model.shop.orders", NodeStatus::Success, None);
        let value = build_results_context(&[result], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        let name = env
            .template_from_str("{{ results[0].node.name }}")?
            .render(&ctx, &[])?;
        assert_eq!(name, "orders");

        let pkg = env
            .template_from_str("{{ results[0].node.package_name }}")?
            .render(&ctx, &[])?;
        assert_eq!(pkg, "shop");

        let rt = env
            .template_from_str("{{ results[0].node.resource_type }}")?
            .render(&ctx, &[])?;
        assert_eq!(rt, "model");

        Ok(())
    }

    fn render(template: &str, results: minijinja::Value) -> Result<String, minijinja::Error> {
        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), results);
        env.template_from_str(template)?.render(&ctx, &[])
    }

    /// The words are `run_results.json`'s, not this worker's own.
    #[test]
    fn statuses_use_dbt_words() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let results = [
            mk_result("model.p.ok", NodeStatus::Success, None),
            mk_result("model.p.bad", NodeStatus::Error, None),
            mk_result("model.p.later", NodeStatus::Skipped, None),
            mk_result("test.p.holds", NodeStatus::Success, None),
            mk_result("test.p.breaks", NodeStatus::Error, Some(3)),
            mk_result("test.p.noisy", NodeStatus::Warn, Some(1)),
        ];

        let rendered = render(
            "{% for r in results %}{{ r.status }},{% endfor %}",
            build_results_context(&results, &nodes),
        )?;

        assert_eq!(rendered, "success,error,skipped,pass,fail,warn,");
        Ok(())
    }

    #[test]
    fn a_freshness_result_reports_its_grade() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut stale = mk_result("source.p.raw.orders", NodeStatus::Error, None);
        stale.freshness = Some(crate::types::FreshnessOutcome {
            max_loaded_at: "2026-06-12T10:00:00+00:00".into(),
            snapshotted_at: "2026-06-12T11:00:00+00:00".into(),
            max_loaded_at_time_ago_in_s: 3600.0,
            status: crate::types::FreshnessGrade::Warn,
            resource_type: "source".into(),
            criteria: dbt_schemas::schemas::common::FreshnessDefinition::default(),
        });

        let rendered = render("{{ results[0].status }}", build_results_context(&[stale], &nodes))?;

        assert_eq!(rendered, "warn");
        Ok(())
    }

    #[test]
    fn the_node_is_the_resolved_node_with_its_config() -> anyhow::Result<()> {
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes.models.insert(
            "model.shop.orders".to_string(),
            Arc::new(DbtModel {
                __common_attr__: CommonAttributes {
                    unique_id: "model.shop.orders".to_string(),
                    name: "orders".to_string(),
                    ..CommonAttributes::default()
                },
                ..DbtModel::default()
            }),
        );

        let rendered = render(
            "{{ results[0].node.name }}|{{ results[0].node.resource_type }}|\
             {{ results[0].node.config is defined }}",
            build_results_context(
                &[mk_result("model.shop.orders", NodeStatus::Success, None)],
                &nodes,
            ),
        )?;

        assert_eq!(rendered, "orders|model|True");
        Ok(())
    }

    #[test]
    fn timing_instants_support_strftime() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut r = mk_result("model.p.m", NodeStatus::Success, None);
        r.timing = vec![crate::types::TimingEntry {
            name: "execute".to_string(),
            started_at: "2026-03-04T05:06:07Z".to_string(),
            completed_at: "not a timestamp".to_string(),
        }];

        let rendered = render(
            "{% set t = results[0].timing[0] %}{{ t.name }}|\
             {{ t.started_at.strftime('%Y-%m-%d %H:%M') }}|{{ t.completed_at is none }}",
            build_results_context(&[r], &nodes),
        )?;

        assert_eq!(rendered, "execute|2026-03-04 05:06|True");
        Ok(())
    }

    fn built(unique_id: &str, status: NodeStatus, relation: Option<&str>) -> NodeExecutionResult {
        let mut r = mk_result(unique_id, status, None);
        r.relation_name = relation.map(ToString::to_string);
        r
    }

    #[test]
    fn schemas_are_those_of_the_relations_the_run_built() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let results = [
            built("model.p.a", NodeStatus::Success, Some("db.analytics.a")),
            built("seed.p.b", NodeStatus::Success, Some("db.analytics.b")),
            built("snapshot.p.c", NodeStatus::Success, Some("db.history.c")),
            // Not built: failed, an assertion, or a node with nowhere recorded.
            built("model.p.failed", NodeStatus::Error, Some("db.broken.failed")),
            built("test.p.t", NodeStatus::Success, Some("db.tests.t")),
            built("model.p.unrecorded", NodeStatus::Success, None),
        ];

        let set = built_schemas(&results, &nodes);

        assert_eq!(
            set.iter().cloned().collect::<Vec<_>>(),
            [
                ("db".to_string(), "analytics".to_string()),
                ("db".to_string(), "history".to_string()),
            ]
        );
    }

    #[test]
    fn schemas_and_database_schemas_render_as_dbt_hands_them() -> anyhow::Result<()> {
        let set: BTreeSet<(String, String)> = [("db", "a"), ("db", "b"), ("other", "a")]
            .into_iter()
            .map(|(d, s)| (d.to_string(), s.to_string()))
            .collect();
        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("schemas".to_string(), schemas_context(&set));
        ctx.insert("database_schemas".to_string(), database_schemas_context(&set));

        let rendered = env
            .template_from_str(
                "{% for s in schemas %}{{ s }},{% endfor %}|\
                 {% for d, s in database_schemas %}{{ d }}.{{ s }},{% endfor %}",
            )?
            .render(&ctx, &[])?;

        assert_eq!(rendered, "a,b,|db.a,db.b,other.a,");
        Ok(())
    }

    #[test]
    fn adapter_response_carried_through_as_jinja_map() -> anyhow::Result<()> {
        use std::collections::BTreeMap;

        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut r = mk_result("model.pkg.x", NodeStatus::Success, None);
        let mut resp = BTreeMap::new();
        resp.insert("rows_affected".to_string(), serde_json::json!(42));
        resp.insert("message".to_string(), serde_json::json!("CREATE TABLE"));
        r.adapter_response = resp;

        let value = build_results_context(&[r], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        let rendered = env
            .template_from_str(
                "{{ results[0].adapter_response.rows_affected }}|\
                 {{ results[0].adapter_response.message }}",
            )?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "42|CREATE TABLE");
        Ok(())
    }

    #[test]
    fn results_with_failures_renders_count() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let r = mk_result("test.pkg.t", NodeStatus::Error, Some(7));
        let value = build_results_context(&[r], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);
        let s = env
            .template_from_str("{{ results[0].failures }}")?
            .render(&ctx, &[])?;
        assert_eq!(s, "7");
        Ok(())
    }

    // --- hook retry policy ---

    use crate::activities::project_hooks::hook_retry_decision;
    use crate::activities::retry::RetryDecision;
    use crate::error::DbtTemporalError;
    use crate::types::{ProjectHookPhase, ProjectHookRetry};

    fn transient() -> DbtTemporalError {
        DbtTemporalError::Adapter(anyhow::anyhow!("connection reset by peer"))
    }

    /// The default. A hook that appends audit rows must not be re-run behind
    /// the author's back, so opting in is required even for a transient error.
    #[test]
    fn a_hook_that_did_not_opt_in_never_retries() {
        assert_eq!(hook_retry_decision(&transient(), false, &[]), RetryDecision::NoRetry);
    }

    #[test]
    fn an_opted_in_hook_retries_a_transient_warehouse_error() {
        assert_eq!(hook_retry_decision(&transient(), true, &[]), RetryDecision::Retry);
    }

    /// Opting in must not turn bad SQL into a retry loop — only the transient
    /// variants are eligible, exactly as for nodes.
    #[test]
    fn opting_in_does_not_make_permanent_failures_retry() {
        let permanent = DbtTemporalError::Compilation("syntax error at or near".to_string());
        assert_eq!(hook_retry_decision(&permanent, true, &[]), RetryDecision::NoRetry);
    }

    /// The project's own `non_retryable_errors` patterns still apply on top of
    /// the opt-in, so an author can enable retries broadly and carve out the
    /// messages they know are permanent.
    #[test]
    fn non_retryable_patterns_still_win_over_the_opt_in() {
        let patterns =
            crate::error::compile_error_patterns(&["connection (reset|refused)".to_string()]);
        assert_eq!(hook_retry_decision(&transient(), true, &patterns), RetryDecision::NoRetry);
    }

    /// Per-phase, because idempotency differs between setup and teardown SQL.
    #[test]
    fn each_phase_opts_in_independently() {
        let start_only = ProjectHookRetry {
            on_run_start: true,
            on_run_end: false,
        };
        assert!(start_only.allows(ProjectHookPhase::OnRunStart));
        assert!(!start_only.allows(ProjectHookPhase::OnRunEnd));

        let neither = ProjectHookRetry::default();
        assert!(!neither.allows(ProjectHookPhase::OnRunStart));
        assert!(!neither.allows(ProjectHookPhase::OnRunEnd));
    }

    #[allow(clippy::unwrap_used, clippy::expect_used)] // Test assertions.
    mod prior_segments {
        use std::collections::BTreeMap;

        use crate::activities::project_hooks::with_prior_segments;
        use crate::artifact_store::{ArtifactStore, LocalArtifactStore};
        use crate::error::DbtTemporalError;
        use crate::types::{
            ExecutionPlan, NodeExecutionResult, NodeStatus, NodeStatusTree, ProjectHookPhase,
            ProjectHooksInput, RunSegmentState,
        };

        fn result(unique_id: &str) -> NodeExecutionResult {
            NodeExecutionResult {
                unique_id: unique_id.to_string(),
                status: NodeStatus::Success,
                execution_time: 0.1,
                message: None,
                adapter_response: BTreeMap::new(),
                compiled_code: None,
                timing: vec![],
                failures: None,
                freshness: None,
                compiled_code_ref: None,
                relation_name: None,
            }
        }

        fn hooks_input(node: &str, prior_segments: Vec<String>) -> ProjectHooksInput {
            ProjectHooksInput {
                phase: ProjectHookPhase::OnRunEnd,
                project: "p".to_string(),
                invocation_id: "inv".to_string(),
                env: BTreeMap::new(),
                target: None,
                node_results: vec![result(node)],
                vars: BTreeMap::new(),
                full_refresh: false,
                retry_on_error: false,
                prior_segments,
                run_started_at: None,
            }
        }

        async fn spill(store: &dyn ArtifactStore, segment: u32, node: &str) -> String {
            let state = RunSegmentState {
                schema_version: 1,
                invocation_id: "inv".to_string(),
                segment,
                plan: ExecutionPlan {
                    project: "p".to_string(),
                    levels: vec![],
                    nodes: BTreeMap::new(),
                    manifest_json: None,
                    manifest_ref: None,
                    invocation_id: "inv".to_string(),
                    search_attributes: BTreeMap::new(),
                    write_artifacts: true,
                    has_on_run_start: false,
                    has_on_run_end: true,
                    priority_scheduling: false,
                    has_project_checks: false,
                },
                prior_segments: vec![],
                all_results: vec![result(node)],
                log_lines: vec![],
                node_status: NodeStatusTree {
                    nodes: BTreeMap::new(),
                },
                failed_nodes: vec![],
                had_failure: false,
                effective_env: BTreeMap::new(),
                hook_errors: vec![],
                total_nodes: 3,
                node_counter: 0,
                next_level: 1,
                started_at: None,
            };
            let json = serde_json::to_vec(&state).expect("serialize state");
            store
                .store("inv", &format!("run_segment_state_{segment}.json"), json.into())
                .await
                .expect("store state")
        }

        fn ids(input: &ProjectHooksInput) -> Vec<&str> {
            input
                .node_results
                .iter()
                .map(|r| r.unique_id.as_str())
                .collect()
        }

        /// Hooks see earlier segments oldest first, then this segment's results.
        #[tokio::test]
        async fn earlier_segments_precede_this_segments_results() {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = LocalArtifactStore::new(dir.path().to_path_buf());
            let first = spill(&store, 1, "model.first").await;
            let second = spill(&store, 2, "model.second").await;

            let merged =
                with_prior_segments(Some(&store), hooks_input("model.last", vec![first, second]))
                    .await
                    .expect("merge");

            assert_eq!(ids(&merged), vec!["model.first", "model.second", "model.last"]);
        }

        #[tokio::test]
        async fn a_run_without_earlier_segments_needs_no_store() {
            let merged = with_prior_segments(None, hooks_input("model.only", vec![]))
                .await
                .expect("no segments, nothing to read");
            assert_eq!(ids(&merged), vec!["model.only"]);
        }

        #[tokio::test]
        async fn earlier_segments_without_a_store_is_a_configuration_error() {
            let err = with_prior_segments(None, hooks_input("model.last", vec!["seg.json".into()]))
                .await
                .expect_err("cannot read segments without a store");
            let typed = err.downcast_ref::<DbtTemporalError>().expect("typed error");
            assert!(matches!(typed, DbtTemporalError::Configuration(_)), "{typed}");
            assert!(!typed.is_retryable());
        }

        /// A checkpoint that is not in the store will not appear on a retry, so
        /// the read fails for good, and never yields a silently shorter
        /// `results`.
        #[tokio::test]
        async fn a_missing_segment_is_a_permanent_store_error() {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = LocalArtifactStore::new(dir.path().to_path_buf());
            let missing = dir.path().join("inv/run_segment_state_1.json");

            let err = with_prior_segments(
                Some(&store),
                hooks_input("model.last", vec![missing.to_string_lossy().into_owned()]),
            )
            .await
            .expect_err("a missing checkpoint must be reported");
            let typed = err.downcast_ref::<DbtTemporalError>().expect("typed error");
            assert!(matches!(typed, DbtTemporalError::ArtifactStore(_)), "{typed}");
            assert!(!typed.is_retryable());
            assert_eq!(typed.type_name(), "DbtTemporalError::ArtifactStorePermanent");
            assert!(format!("{typed}").contains("earlier segment"), "{typed}");
        }

        /// Inputs recorded before the field existed deserialize with no prior
        /// segments.
        #[test]
        fn old_input_without_prior_segments_deserializes() {
            let input: ProjectHooksInput = serde_json::from_value(serde_json::json!({
                "phase": "on_run_end",
                "project": "p",
                "invocation_id": "inv",
            }))
            .expect("deserialize");
            assert_eq!(input.prior_segments, Vec::<String>::new());
        }
    }
}
