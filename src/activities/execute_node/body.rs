//! The synchronous half of a node execution: compile, materialize, decode.

use tracing::info;

use crate::types::{NodeExecutionInput, NodeExecutionResult};

use super::super::render_env::{self, RenderOverrides};
use super::compile::compile_node;
use super::context::{NodeRun, Timeline};
use super::materialize::materialize_node;
use super::outcome::{compile_only_result, measure_freshness};
use super::verdict::{judge_node, node_result, read_adapter_response};

/// The phases of executing one node, in order.
///
/// Runs on a blocking thread. Every Jinja value it builds is thread-affine and
/// never leaves this function, which is why the environment is constructed
/// here rather than handed in.
pub(super) fn execute_node_body(
    state: &crate::worker_state::WorkerState,
    input: &NodeExecutionInput,
    defer_nodes: Option<&dbt_schemas::schemas::Nodes>,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<NodeExecutionResult, anyhow::Error> {
    let run = NodeRun::locate(state, input)?;

    let started = std::time::Instant::now();

    // --- COMPILE PHASE ---
    let compile_start = chrono::Utc::now();

    // Private Jinja env + adapter for this activity, with the workflow's env
    // vars, --vars and --full-refresh applied. `render_env` must stay alive for
    // the whole activity: it owns the adapter bound to this activity's token.
    // `base.adapter` is the node's `+adapter` selection when it made one and the
    // run's default otherwise, so routing on it needs no fallback of its own —
    // and an adapter the target does not declare fails here rather than running
    // against the wrong warehouse.
    let overrides = RenderOverrides {
        cancellation,
        env: &input.env,
        target: input.target.as_deref(),
        vars: &input.vars,
        full_refresh: input.full_refresh,
        invocation_id: &input.invocation_id,
        run_started_at: input.run_started_at,
    };
    let render_env =
        render_env::prepare_render_env(state, &overrides, run.base.adapter, run.unique_id)?;

    // Its scratch directory stays alive until after the materialization.
    let mut compiled = compile_node(&run, &overrides, &render_env, defer_nodes)?;

    let timeline = Timeline {
        started,
        compile_start,
        compile_end: chrono::Utc::now(),
    };

    // For `dbt compile`, stop here — render SQL but skip materialization and any
    // adapter execution. The caller gets the compiled SQL via `compiled_code`.
    if run.is_compile_only() {
        let execution_time = timeline.elapsed_secs();
        info!(node = %run.unique_id, time_secs = execution_time, "node compiled (compile-only)");
        return Ok(compile_only_result(
            run.unique_id,
            &compiled.node_context,
            execution_time,
            timeline.compile_window(),
        ));
    }

    // --- EXECUTE PHASE ---
    let execute_start = chrono::Utc::now();

    if run.measures_freshness {
        return measure_freshness(&run, &render_env, &compiled, &timeline, execute_start);
    }

    let materialized =
        materialize_node(&run, &render_env, &mut compiled, &timeline, execute_start)?;
    let adapter_response = read_adapter_response(&run, &compiled, &materialized)?;
    let verdict = judge_node(&run, &compiled, &materialized, adapter_response)?;
    Ok(node_result(&run, &compiled, materialized, verdict))
}
