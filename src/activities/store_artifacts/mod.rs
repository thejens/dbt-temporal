//! The `store_artifacts` activity: writes the run's artifacts to the artifact
//! store once every node has finished.

mod catalog_artifact;
mod compiled_sql;
mod freshness;
mod metadata;
mod run_results;
mod segments;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use anyhow::Context;
use bytes::Bytes;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::{info, warn};

use crate::error::DbtTemporalError;
use crate::types::{DbtCommand, StoreArtifactsInput, StoreArtifactsOutput};

use self::catalog_artifact::generate_and_store_catalog;
use self::compiled_sql::load_compiled_sql;
use self::freshness::{build_freshness_json, is_source_result};
use self::run_results::build_run_results_json;
use self::segments::collect_prior_segments;
use super::DbtActivities;
use super::cancellable;
use super::retry;

pub(super) use self::metadata::DBT_VERSION;
pub(super) use self::run_results::dbt_status;

/// Outer wrapper — cancellation, heartbeating, and retry classification.
///
/// This activity is pure object-store I/O and runs *after* every node has
/// finished, so a transient 5xx here would otherwise throw away a completed
/// run's results. Store failures are tagged `ArtifactStore` and retry;
/// everything else (missing store config, serialization bugs) is permanent.
///
/// The catalog queries the warehouse on the dbt pool, where dropping the future
/// that awaits them would leave them running. So the activity holds its own
/// cancellation source, the catalog's adapter takes its token, and a
/// cancellation — which is also how the server's timeout reaches the worker, at
/// the next heartbeat — stops those queries and waits for them to end.
pub async fn store_artifacts_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: StoreArtifactsInput,
) -> Result<StoreArtifactsOutput, ActivityError> {
    let project = input.project.clone();
    let cancel = dbt_common::cancellation::CancellationTokenSource::new();
    let token = cancel.token();
    // Heartbeats double as the server's liveness check: uploading a large
    // manifest to object storage can outlast the heartbeat timeout on a slow
    // link, and a slow upload must not look like a dead worker.
    let (result, cancelled) = cancellable::run_in_activity(
        &ctx,
        &cancel,
        || info!("store_artifacts cancelled — stopping warehouse work"),
        store_artifacts_inner(activities, input, &token),
    )
    .await;
    let result = match cancellable::settle(result, cancelled) {
        cancellable::Settled::Finished(result) => result,
        cancellable::Settled::Cancelled => return Err(ActivityError::cancelled()),
    };
    result.map_err(|e| {
        warn!(error = %format!("{e:#}"), "store_artifacts failed");
        let patterns = project
            .as_deref()
            .and_then(|p| retry::registry_non_retryable_patterns(&activities.registry, p));
        retry::classify(e, patterns.as_deref().unwrap_or(&[]), retry::Unclassified::Permanent)
    })
}

/// Tag an artifact-store I/O failure as retryable, preserving the call context.
fn store_io_error(context: &'static str, e: anyhow::Error) -> anyhow::Error {
    DbtTemporalError::ArtifactStore(e.context(context)).into()
}

/// Store run_results.json and manifest.json to the configured artifact store.
///
/// `store_artifacts_outer` wraps this with cancellation, heartbeat and error
/// classification. Also the entry point for integration tests that drive
/// artifact writing without a Temporal activity context.
///
/// `cancellation` is the activity's own token: it is what the catalog's
/// warehouse queries stop on. The caller keeps its source alive until this
/// returns, because the adapter engine holds only a `Weak` reference to it.
pub async fn store_artifacts_inner(
    activities: &DbtActivities,
    input: StoreArtifactsInput,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<StoreArtifactsOutput, anyhow::Error> {
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        anyhow::anyhow!("ArtifactStore not configured but store_artifacts was called")
    })?;

    // A run that continued as new left each earlier segment's results and log
    // in that segment's own checkpoint rather than dragging them through every
    // history since. Collect them so the artifacts describe the whole run.
    let mut input = collect_prior_segments(store.as_ref(), input).await?;

    // The nodes wrote their compiled SQL to the store instead of carrying it
    // through the workflow; read it back for the one artifact that reports it.
    let compiled_sql = load_compiled_sql(store.as_ref(), &input.node_results).await;

    let run_results_json =
        build_run_results_json(&input, &compiled_sql).context("serializing run_results.json")?;

    let run_results_path = store
        .store(&input.invocation_id, "run_results.json", run_results_json.into_bytes().into())
        .await
        .map_err(|e| store_io_error("storing run_results.json", e))?;

    info!(path = %run_results_path, "stored run_results.json");

    // Store manifest (if inline) or note existing ref.
    let manifest_path = if let Some(manifest_json) = input.manifest_json.take() {
        store
            .store(&input.invocation_id, "manifest.json", Bytes::from(manifest_json))
            .await
            .map_err(|e| store_io_error("storing manifest.json", e))?
    } else if let Some(manifest_ref) = &input.manifest_ref {
        // Already stored during plan phase.
        manifest_ref.clone()
    } else {
        anyhow::bail!("neither manifest_json nor manifest_ref provided");
    };

    info!(path = %manifest_path, "manifest.json available");

    // Store run log if enabled and provided.
    let log_path = if activities.write_run_log.0
        && let Some(run_log) = input.run_log.take()
    {
        let path = store
            .store(&input.invocation_id, "log.txt", Bytes::from(run_log))
            .await
            .map_err(|e| store_io_error("storing log.txt", e))?;
        info!(path = %path, "stored log.txt");
        Some(path)
    } else {
        None
    };

    // Optionally generate catalog.json (column metadata for the run's
    // relations). Catalog problems are logged, never fatal — the run's real
    // artifacts are already stored at this point.
    let catalog_path = if activities.write_catalog.0 {
        match generate_and_store_catalog(activities, &input, store.as_ref(), cancellation).await {
            Ok(path) => {
                info!(path = %path, "stored catalog.json");
                Some(path)
            }
            // A catalog cut short by cancellation is incomplete work, not a
            // best-effort miss: failing lets the caller report the activity as
            // cancelled instead of as a finished run that lacks its catalog.
            Err(e) if cancellation.is_cancelled() => return Err(e),
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "catalog.json generation failed (non-fatal)");
                None
            }
        }
    } else {
        None
    };

    // Freshness runs additionally produce dbt's freshness artifacts. Only nodes
    // that completed the check carry an outcome; a stale node fails its
    // activity and appears in run_results with the error message instead.
    if let Some(command) = input.command.as_deref().and_then(DbtCommand::parse)
        && command.is_freshness()
    {
        let sources_only = command == DbtCommand::SourceFreshness;
        // `dbt source freshness` writes sources.json unconditionally, an empty
        // result set included. The unified spelling only rewrites it when the
        // run actually measured a source, so a model-only freshness run does
        // not clobber a good artifact with an empty one.
        if sources_only || input.node_results.iter().any(is_source_result) {
            let sources_json =
                build_freshness_json(&input, true).context("serializing sources.json")?;
            let path = store
                .store(&input.invocation_id, "sources.json", sources_json.into_bytes().into())
                .await
                .map_err(|e| store_io_error("storing sources.json", e))?;
            info!(path = %path, "stored sources.json");
        }
        if !sources_only {
            let freshness_json =
                build_freshness_json(&input, false).context("serializing freshness.json")?;
            let path = store
                .store(&input.invocation_id, "freshness.json", freshness_json.into_bytes().into())
                .await
                .map_err(|e| store_io_error("storing freshness.json", e))?;
            info!(path = %path, "stored freshness.json");
        }
    }

    Ok(StoreArtifactsOutput {
        run_results_path,
        manifest_path,
        log_path,
        catalog_path,
    })
}
