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

mod model_context;
mod prior_segments;
mod results_context;
mod schemas_context;
#[cfg(test)]
mod test_support;

use std::sync::Arc;

use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::info;

use crate::error::DbtTemporalError;
use crate::types::{ProjectHookPhase, ProjectHooksInput};
use crate::worker_state::WorkerState;

use self::model_context::empty_model_context;
use self::prior_segments::with_prior_segments;
use self::results_context::build_results_context;
use self::schemas_context::{built_schemas, database_schemas_context, schemas_context};
use super::DbtActivities;
use super::cancellable;
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
                // session settings.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProjectHookRetry;

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
