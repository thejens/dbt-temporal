//! Project-level hook execution: `on-run-start` and `on-run-end` from `dbt_project.yml`.
//!
//! Each hook is a Jinja template (e.g. `"{{ log_run_results() }}"`) stored in
//! `state.resolver_state.operations.on_run_start` / `.on_run_end` as `DbtOperation`
//! nodes. We render each one with the full compile+run Jinja context. Side-effect
//! macros (`run_query`, `statement`, `log`) execute against the warehouse during
//! rendering. If the rendered output is non-empty SQL, we execute it directly.
//!
//! For `on-run-end`, the `results` Jinja variable is built from the workflow's
//! `node_results` and matches dbt-core's shape (`status`, `unique_id`,
//! `execution_time`, `message`, `failures`, `adapter_response`, `node.*`).

use std::collections::BTreeMap;
use std::sync::Arc;

use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::info;

use crate::error::DbtTemporalError;
use crate::types::{NodeExecutionResult, ProjectHookPhase, ProjectHooksInput};

use super::DbtActivities;
use super::heartbeat;
use super::node_helpers::{json_to_minijinja, patch_target_global};

/// Outer wrapper — handles the `Result` translation, cancellation, and heartbeating.
///
/// All errors are mapped to `NonRetryable` because hook side effects (DDL, logging)
/// are not safe to retry on transient errors. Cancellation is honoured so a
/// workflow termination does not leave hook SQL running on a doomed worker.
pub async fn run_project_hooks_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: ProjectHooksInput,
) -> Result<(), ActivityError> {
    let phase = input.phase;
    tokio::select! {
        result = run_project_hooks_inner(activities, input) => {
            result.map_err(|e| ActivityError::NonRetryable(e.into()))
        }
        () = ctx.cancelled() => {
            info!(phase = %phase, "project hooks cancelled");
            Err(ActivityError::cancelled())
        }
        never = heartbeat::heartbeat_loop(&ctx) => match never {},
    }
}

#[allow(clippy::too_many_lines, clippy::unused_async)]
// async required by the activity signature; rendering itself is sync.
async fn run_project_hooks_inner(
    activities: &DbtActivities,
    input: ProjectHooksInput,
) -> Result<(), anyhow::Error> {
    let state = activities.registry.get(Some(&input.project))?;

    info!(
        phase = %input.phase,
        project = %input.project,
        invocation_id = %input.invocation_id,
        "running project hooks"
    );

    // ── Jinja env setup (mirrors execute_node_inner) ────────────────────
    let mut jinja_env = (*state.jinja_env).clone();

    // Per-workflow env_var() override.
    if !input.env.is_empty() {
        let env_overrides = Arc::new(input.env.clone());
        jinja_env.env.add_func_func("env_var", move |state, args| {
            let map = Arc::clone(&env_overrides);
            let lookup = move |key: &str| -> Option<minijinja::Value> {
                map.get(key).map(|v| minijinja::Value::from(v.clone()))
            };
            dbt_jinja_utils::env_var(false, Some(&lookup), state, args)
        });
    }

    // Use per-workflow adapter engine if env overrides require it.
    // _rebuild_guard keeps the RebuildResult alive for the duration of the activity.
    #[allow(unused_assignments, clippy::collection_is_never_read)]
    let mut _rebuild_guard = None;
    let adapter_engine = if !input.env.is_empty() && state.profile_uses_env_vars {
        let result = crate::worker::rebuild_adapter_engine_with_env(
            state,
            input.target.as_deref(),
            &input.env,
        )
        .map_err(|e| {
            DbtTemporalError::Configuration(format!(
                "rebuilding adapter engine for {}: {e:#}",
                input.phase
            ))
        })?;
        let engine = Arc::clone(&result.engine);
        // Patch target/env globals so hooks see the per-workflow schema/database.
        patch_target_global(
            &mut jinja_env,
            &result.schema,
            &result.database,
            input.target.as_deref(),
        );
        _rebuild_guard = Some(result);
        engine
    } else {
        Arc::clone(&state.adapter_engine)
    };

    let adapter_impl = dbt_adapter::AdapterImpl::new(adapter_engine, None);
    let adapter = Arc::new(dbt_adapter::Adapter::new(
        Arc::new(adapter_impl),
        None,
        state.cancellation_source.token(),
    ));

    // Configure the Jinja env: registers adapter/api/dialect as globals,
    // sets undefined behavior to lenient.
    dbt_jinja_utils::phases::configure_compile_and_run_jinja_environment(
        &mut jinja_env,
        Arc::clone(&adapter),
    );

    // Set execute=true as a Jinja GLOBAL so cross-template macros (e.g. run_query
    // dispatched from package macros) see it. dbt-fusion only sets it as a context
    // variable in build_compile_and_run_base_context, which doesn't propagate to
    // cross-template macro calls (upstream issue #1289 — closed as not-planned).
    jinja_env
        .env
        .add_global("execute", minijinja::Value::from(true));

    // ── Build base context ───────────────────────────────────────────────
    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default();

    let mut context = dbt_jinja_utils::phases::build_compile_and_run_base_context(
        Arc::clone(&state.resolver_state.node_resolver),
        &state.resolver_state.root_project_name,
        &state.resolver_state.nodes,
        Arc::clone(&state.resolver_state.runtime_config),
        namespace_keys,
    );

    // Ensure invocation_id is in the context — hook macros commonly use {{ invocation_id }}.
    context.insert("invocation_id".to_owned(), minijinja::Value::from(input.invocation_id.clone()));

    // For on_run_end: inject `results` matching dbt-core's shape.
    if matches!(input.phase, ProjectHookPhase::OnRunEnd) {
        let results_value = build_results_context(&input.node_results, &state.resolver_state.nodes);
        context.insert("results".to_owned(), results_value);
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

        // Render — `run_query`/`statement`/`log` macros execute as a side effect.
        let rendered = jinja_env.render_str(raw_code, &context, &[]).map_err(|e| {
            DbtTemporalError::Compilation(format!("{} hook[{idx}]: {e:#}", input.phase))
        })?;

        // If rendering produced non-empty SQL (e.g. raw `create table ...` strings),
        // execute it directly. Pure logging hooks render to empty.
        let sql = rendered.trim();
        if !sql.is_empty() {
            let ctx = dbt_xdbc::QueryCtx::new(format!("{} hook[{idx}]", input.phase));
            adapter
                .execute_without_state(Some(&ctx), sql, false)
                .map_err(|e| {
                    DbtTemporalError::Adapter(anyhow::anyhow!(
                        "{} hook[{idx}] direct SQL failed: {e:#}",
                        input.phase
                    ))
                })?;
        }

        info!(phase = %input.phase, idx, "hook complete");
    }

    Ok(())
}

/// Convert the workflow's `Vec<NodeExecutionResult>` into a Jinja value list
/// matching dbt-core's `results` context shape.
///
/// Each item exposes:
/// - `unique_id`, `status`, `execution_time`, `message`, `failures`, `thread_id`
/// - `adapter_response` (map of warehouse-specific response fields)
/// - `node` (sub-object with `unique_id`, `name`, `resource_type`, `package_name`)
///
/// The `node` sub-object is enriched at render time by looking up
/// `nodes.get_node(unique_id)` — we don't serialize node metadata through Temporal
/// workflow history.
#[allow(clippy::option_if_let_else)] // if-let is clearer than nested map_or_else here.
fn build_results_context(
    results: &[NodeExecutionResult],
    nodes: &dbt_schemas::schemas::Nodes,
) -> minijinja::Value {
    let items: Vec<minijinja::Value> = results
        .iter()
        .map(|r| {
            let mut map = BTreeMap::<String, minijinja::Value>::new();

            map.insert("unique_id".to_owned(), minijinja::Value::from(r.unique_id.clone()));
            map.insert("status".to_owned(), minijinja::Value::from(r.status.as_str()));
            map.insert("execution_time".to_owned(), minijinja::Value::from(r.execution_time));
            map.insert(
                "message".to_owned(),
                r.message
                    .as_deref()
                    .map_or_else(|| minijinja::Value::from(()), minijinja::Value::from),
            );
            map.insert(
                "failures".to_owned(),
                r.failures
                    .map_or_else(|| minijinja::Value::from(()), minijinja::Value::from),
            );
            map.insert("thread_id".to_owned(), minijinja::Value::from("main"));

            // adapter_response as a flat map (rows_affected, message, code, query_id...).
            let resp: BTreeMap<String, minijinja::Value> = r
                .adapter_response
                .iter()
                .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
                .collect();
            map.insert("adapter_response".to_owned(), minijinja::Value::from(resp));

            // `node` sub-object — enriched from resolver_state.nodes.
            let node_val = if let Some(node) = nodes.get_node(&r.unique_id) {
                let common = node.common();
                let rt = node.resource_type().as_str_name().to_ascii_lowercase();
                let rt = rt.strip_prefix("node_type_").unwrap_or(&rt).to_string();
                let mut n = BTreeMap::<String, minijinja::Value>::new();
                n.insert("unique_id".to_owned(), minijinja::Value::from(r.unique_id.clone()));
                n.insert("name".to_owned(), minijinja::Value::from(common.name.clone()));
                n.insert("resource_type".to_owned(), minijinja::Value::from(rt));
                n.insert(
                    "package_name".to_owned(),
                    minijinja::Value::from(common.package_name.clone()),
                );
                minijinja::Value::from(n)
            } else {
                // Node not in resolver (e.g. cancelled before plan): minimal object.
                let mut n = BTreeMap::<String, minijinja::Value>::new();
                n.insert("unique_id".to_owned(), minijinja::Value::from(r.unique_id.clone()));
                minijinja::Value::from(n)
            };
            map.insert("node".to_owned(), node_val);

            minijinja::Value::from(map)
        })
        .collect();
    minijinja::Value::from(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeStatus;

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
        let summary = env
            .template_from_str(
                r"{% set ns = namespace(pass=0, error=0) %}\
{% for r in results %}\
{% if r.status == 'success' %}{% set ns.pass = ns.pass + 1 %}{% endif %}\
{% if r.status == 'error' %}{% set ns.error = ns.error + 1 %}{% endif %}\
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
}
