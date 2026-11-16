mod body;
mod compile;
mod context;
mod defer;
pub(super) mod freshness;
mod materialize;
mod outcome;
mod raw_sql;
#[cfg(test)]
mod retry_tests;
mod schema_patch;
mod schema_patcher;
mod spill;
mod sql_render;
mod sql_rewrite;
mod unit_test;
mod verdict;
mod workspace;
mod yml_to_value;

use std::sync::Arc;

use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::info;

use crate::types::{NodeExecutionInput, NodeExecutionResult, NodeStatus};

use body::execute_node_body;
use defer::load_defer_nodes;
use spill::spill_compiled_sql;

use super::DbtActivities;
use super::cancellable;
use super::retry;

pub use spill::compiled_sql_artifact_name;

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
    // the render environment. A worker-wide source is never cancelled by
    // anything, so a cancelled activity would leave its query running: the
    // server's heartbeat timeout could then start a second attempt while the
    // first was still writing.
    let cancel = dbt_common::cancellation::CancellationTokenSource::new();
    let token = cancel.token();

    let work = tracing::Instrument::instrument(
        execute_node_cancellable(activities, input, &token),
        spans.node.clone(),
    );

    // `work` is joined, never dropped, on cancellation: dropping the future
    // would leave the blocking thread running against the warehouse with
    // nothing waiting for it.
    let (result, cancelled) = cancellable::run_in_activity(
        &ctx,
        &cancel,
        || info!(node = %unique_id, "activity cancelled — stopping warehouse work"),
        work,
    )
    .await;

    let result = match cancellable::settle(result, cancelled) {
        cancellable::Settled::Finished(result) => result,
        cancellable::Settled::Cancelled => {
            super::node_telemetry::record_outcome(&spans, NodeStatus::Cancelled);
            return Err(ActivityError::cancelled());
        }
    };

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

/// Execute a single node against its project's `WorkerState`, without a Temporal
/// activity context — the compile → materialize → result-extract path.
///
/// `execute_node_outer` wraps this with cancellation, heartbeat, and error
/// classification. It is also the entry point for integration tests that drive
/// real nodes against an embedded engine (e.g. the DuckDB scenario harness),
/// where standing up a Temporal `ActivityContext` would add nothing.
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
    // runs on the dbt pool so this task stays free to heartbeat and to
    // notice cancellation.
    let defer_nodes =
        load_defer_nodes(activities, &state, input.defer_manifest_ref.as_deref()).await?;

    let token = cancellation.clone();
    let invocation_id = input.invocation_id.clone();
    let mut result = crate::dbt_pool::run(move || {
        execute_node_body(&state, &input, defer_nodes.as_deref(), &token)
    })
    .await??;

    spill_compiled_sql(activities, &invocation_id, &mut result).await;
    Ok(result)
}
