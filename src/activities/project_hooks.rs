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
use temporalio_sdk::error::ApplicationFailure;
use tracing::info;

use crate::types::{NodeExecutionResult, ProjectHookPhase, ProjectHooksInput};

use crate::error::DbtTemporalError;

use super::DbtActivities;
use super::heartbeat;
use super::node_helpers::json_to_minijinja;
use super::render_env;
use super::retry::{self, RetryDecision};

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
/// the author knows whether that is safe. Cancellation is honoured so a
/// workflow termination does not leave hook SQL running on a doomed worker.
pub async fn run_project_hooks_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: ProjectHooksInput,
) -> Result<(), ActivityError> {
    let phase = input.phase;
    let retry_on_error = input.retry_on_error;
    let project = input.project.clone();
    tokio::select! {
        result = run_project_hooks_inner(activities, input) => {
            result.map_err(|e| {
                let patterns =
                    retry::registry_non_retryable_patterns(&activities.registry, &project);
                // An untyped hook failure is a template or SQL problem, not a
                // blip, so it stays permanent even when retry is enabled.
                let dbt_err = retry::downcast_or_default(e, retry::Unclassified::Permanent);
                match hook_retry_decision(
                    &dbt_err,
                    retry_on_error,
                    patterns.as_deref().unwrap_or(&[]),
                ) {
                    RetryDecision::Retry => {
                        info!(phase = %phase, error = %dbt_err, "hook failed, retrying");
                        ActivityError::application(ApplicationFailure::new(anyhow::anyhow!(
                            "{dbt_err}"
                        )))
                    }
                    RetryDecision::NoRetry => ActivityError::application(
                        ApplicationFailure::non_retryable(anyhow::anyhow!("{dbt_err}")),
                    ),
                }
            })
        }
        () = ctx.cancelled() => {
            info!(phase = %phase, "project hooks cancelled");
            Err(ActivityError::cancelled())
        }
        never = heartbeat::heartbeat_loop(&ctx) => match never {},
    }
}

/// Render and execute one phase's `dbt_project.yml` hooks against a project's
/// `WorkerState`, without a Temporal activity context.
///
/// `run_project_hooks_outer` wraps this with cancellation and heartbeat. Also
/// the entry point for integration tests that drive hooks against an embedded
/// engine (e.g. the DuckDB scenario harness).
#[allow(clippy::too_many_lines, clippy::unused_async)]
// async required by the activity signature; rendering itself is sync.
pub async fn run_project_hooks_inner(
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

    // ── Jinja env setup ─────────────────────────────────────────────────
    // Shared with execute_node so hooks and nodes render against identical
    // globals; see `render_env` for what the per-workflow overrides cover.
    let phase = input.phase.to_string();
    let mut render_env = render_env::prepare_render_env(
        state,
        &render_env::RenderOverrides {
            env: &input.env,
            target: input.target.as_deref(),
            vars: &input.vars,
            full_refresh: input.full_refresh,
        },
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

    // Ensure invocation_id is in the context — hook macros commonly use {{ invocation_id }}.
    context.insert("invocation_id".to_owned(), minijinja::Value::from(input.invocation_id.clone()));

    // Inject `model` as an empty-attribute placeholder. Project-level hooks run before
    // any node executes (on-run-start) or after all finish (on-run-end), so `model` is
    // not in scope. dbt-core injects `model` with empty attributes, making expressions
    // like `{{ "schema=" + model.schema }}` render as "schema=" instead of erroring.
    // Without this, `model.schema` is undefined and the `+` string operator fails.
    context.insert("model".to_owned(), empty_model_context());

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
}
