use anyhow::Context;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::{info, warn};

use crate::artifact_store::ArtifactStore;
use crate::error::DbtTemporalError;
use crate::types::{
    SOURCE_FRESHNESS_COMMAND, StoreArtifactsInput, StoreArtifactsOutput, is_freshness_command,
};

use super::DbtActivities;
use super::heartbeat;
use super::retry;

/// Outer wrapper — cancellation, heartbeating, and retry classification.
///
/// This activity is pure object-store I/O and runs *after* every node has
/// finished, so a transient 5xx here would otherwise throw away a completed
/// run's results. Store failures are tagged `ArtifactStore` and retry;
/// everything else (missing store config, serialization bugs) is permanent.
pub async fn store_artifacts_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: StoreArtifactsInput,
) -> Result<StoreArtifactsOutput, ActivityError> {
    let project = input.project.clone();
    tokio::select! {
        result = store_artifacts_inner(activities, input) => {
            result.map_err(|e| {
                warn!(error = %format!("{e:#}"), "store_artifacts failed");
                let patterns = project
                    .as_deref()
                    .and_then(|p| retry::registry_non_retryable_patterns(&activities.registry, p));
                retry::classify(
                    e,
                    patterns.as_deref().unwrap_or(&[]),
                    retry::Unclassified::Permanent,
                )
            })
        }
        () = ctx.cancelled() => {
            info!("store_artifacts cancelled");
            Err(ActivityError::cancelled())
        }
        // Uploading a large manifest to object storage can outlast the
        // heartbeat timeout on a slow link; keep the tick alive so the server
        // does not mistake a slow upload for a dead worker.
        never = heartbeat::heartbeat_loop(&ctx) => match never {},
    }
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
pub async fn store_artifacts_inner(
    activities: &DbtActivities,
    input: StoreArtifactsInput,
) -> Result<StoreArtifactsOutput, anyhow::Error> {
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        anyhow::anyhow!("ArtifactStore not configured but store_artifacts was called")
    })?;

    let run_results_json =
        build_run_results_json(&input).context("serializing run_results.json")?;

    let run_results_path = store
        .store(&input.invocation_id, "run_results.json", run_results_json.as_bytes())
        .await
        .map_err(|e| store_io_error("storing run_results.json", e))?;

    info!(path = %run_results_path, "stored run_results.json");

    // Store manifest (if inline) or note existing ref.
    let manifest_path = if let Some(manifest_json) = &input.manifest_json {
        store
            .store(&input.invocation_id, "manifest.json", manifest_json.as_bytes())
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
    let log_path = if let Some(run_log) = &input.run_log {
        if activities.write_run_log.0 {
            let path = store
                .store(&input.invocation_id, "log.txt", run_log.as_bytes())
                .await
                .map_err(|e| store_io_error("storing log.txt", e))?;
            info!(path = %path, "stored log.txt");
            Some(path)
        } else {
            None
        }
    } else {
        None
    };

    // Optionally generate catalog.json (column metadata for the run's
    // relations). Catalog problems are logged, never fatal — the run's real
    // artifacts are already stored at this point.
    let catalog_path = if activities.write_catalog.0 {
        match generate_and_store_catalog(activities, &input, store.as_ref()).await {
            Ok(path) => {
                info!(path = %path, "stored catalog.json");
                Some(path)
            }
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
    if let Some(command) = input.command.as_deref()
        && is_freshness_command(command)
    {
        let sources_only = command == SOURCE_FRESHNESS_COMMAND;
        // `dbt source freshness` writes sources.json unconditionally, an empty
        // result set included. The unified spelling only rewrites it when the
        // run actually measured a source, so a model-only freshness run does
        // not clobber a good artifact with an empty one.
        if sources_only || input.node_results.iter().any(is_source_result) {
            let sources_json =
                build_freshness_json(&input, true).context("serializing sources.json")?;
            let path = store
                .store(&input.invocation_id, "sources.json", sources_json.as_bytes())
                .await
                .map_err(|e| store_io_error("storing sources.json", e))?;
            info!(path = %path, "stored sources.json");
        }
        if !sources_only {
            let freshness_json =
                build_freshness_json(&input, false).context("serializing freshness.json")?;
            let path = store
                .store(&input.invocation_id, "freshness.json", freshness_json.as_bytes())
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

/// Build catalog.json for the run's project and store it.
async fn generate_and_store_catalog(
    activities: &DbtActivities,
    input: &StoreArtifactsInput,
    store: &dyn ArtifactStore,
) -> Result<String, anyhow::Error> {
    let state = activities
        .registry
        .get(input.project.as_deref())
        .context("resolving project for catalog generation")?;
    // Catalog generation queries the warehouse through the adapter, which emits
    // dbt telemetry spans; those need an `Invocation` root above them. Scoped to
    // the synchronous build so no span guard is held across the store `await`.
    let catalog_json =
        super::node_telemetry::invocation_span(&input.invocation_id, "dbt docs generate")
            .in_scope(|| {
                super::catalog::build_catalog_json(state, &input.node_results, &input.invocation_id)
            })?;
    store
        .store(&input.invocation_id, "catalog.json", catalog_json.as_bytes())
        .await
        .context("storing catalog.json")
}

/// Build the `run_results.json` content from the store artifacts input.
fn build_run_results_json(input: &StoreArtifactsInput) -> Result<String, anyhow::Error> {
    // Sum durations in nanoseconds (i64) so a long run with many nodes
    // doesn't lose low-bit precision the way an f64 fold would.
    let total: std::time::Duration = input
        .node_results
        .iter()
        .map(|r| std::time::Duration::from_secs_f64(r.execution_time.max(0.0)))
        .sum();
    let run_results = serde_json::json!({
        "metadata": {
            "invocation_id": input.invocation_id,
            "dbt_version": env!("CARGO_PKG_VERSION"),
            "generated_at": chrono::Utc::now().to_rfc3339(),
        },
        "results": input.node_results,
        "elapsed_time": total.as_secs_f64(),
    });
    serde_json::to_string_pretty(&run_results).map_err(Into::into)
}

/// Whether a node result belongs to a source.
///
/// Keyed on the unique_id prefix, which is how dbt itself separates sources
/// from models when it only has the id to go on — a resolved node is not in
/// reach here, only the results the workflow accumulated.
fn is_source_result(result: &crate::types::NodeExecutionResult) -> bool {
    result.unique_id.starts_with("source.")
}

/// Build the `sources.json` / `freshness.json` content from freshness-bearing
/// node results.
///
/// `sources_only` picks between the two: `sources.json` keeps the shape
/// `dbt source freshness` has always written (sources, no `resource_type`),
/// while `freshness.json` carries every measured node and tags each row with
/// its resource type.
fn build_freshness_json(
    input: &StoreArtifactsInput,
    sources_only: bool,
) -> Result<String, anyhow::Error> {
    let results: Vec<serde_json::Value> = input
        .node_results
        .iter()
        .filter_map(|r| {
            let f = r.freshness.as_ref()?;
            if sources_only && !is_source_result(r) {
                return None;
            }
            let mut row = serde_json::json!({
                "unique_id": r.unique_id,
                "max_loaded_at": f.max_loaded_at,
                "snapshotted_at": f.snapshotted_at,
                "max_loaded_at_time_ago_in_s": f.max_loaded_at_time_ago_in_s,
                "status": f.status,
                "criteria": f.criteria,
                "adapter_response": r.adapter_response,
                "timing": r.timing,
                "execution_time": r.execution_time,
            });
            // `resource_type` is what separates the two artifacts —
            // `sources.json`'s shape must not change.
            if !sources_only && let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "resource_type".to_owned(),
                    serde_json::Value::String(f.resource_type.clone()),
                );
            }
            Some(row)
        })
        .collect();
    let total: std::time::Duration = input
        .node_results
        .iter()
        .map(|r| std::time::Duration::from_secs_f64(r.execution_time.max(0.0)))
        .sum();
    let artifact = serde_json::json!({
        "metadata": {
            "invocation_id": input.invocation_id,
            "dbt_version": env!("CARGO_PKG_VERSION"),
            "generated_at": chrono::Utc::now().to_rfc3339(),
        },
        "results": results,
        "elapsed_time": total.as_secs_f64(),
    });
    serde_json::to_string_pretty(&artifact).map_err(Into::into)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::types::{NodeExecutionResult, NodeStatus};
    use std::collections::BTreeMap;

    fn sample_result(unique_id: &str, status: NodeStatus, time: f64) -> NodeExecutionResult {
        NodeExecutionResult {
            unique_id: unique_id.into(),
            status,
            execution_time: time,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
            freshness: None,
        }
    }

    fn with_freshness(unique_id: &str, resource_type: &str, status: &str) -> NodeExecutionResult {
        let mut result = sample_result(unique_id, NodeStatus::Success, 0.4);
        result.freshness = Some(crate::types::FreshnessOutcome {
            max_loaded_at: "2026-06-12T10:00:00+00:00".into(),
            snapshotted_at: "2026-06-12T11:00:00+00:00".into(),
            max_loaded_at_time_ago_in_s: 3600.0,
            status: status.into(),
            resource_type: resource_type.into(),
            criteria: dbt_schemas::schemas::common::FreshnessDefinition::default(),
        });
        result
    }

    fn freshness_input(
        command: &str,
        node_results: Vec<NodeExecutionResult>,
    ) -> StoreArtifactsInput {
        StoreArtifactsInput {
            invocation_id: "inv-9".into(),
            project: None,
            command: Some(command.into()),
            node_results,
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
        }
    }

    #[test]
    fn sources_json_includes_only_source_freshness_results() -> anyhow::Result<()> {
        let input = freshness_input(
            "freshness",
            vec![
                with_freshness("source.p.s.orders", "source", "pass"),
                with_freshness("model.p.stg_orders", "model", "warn"),
                sample_result("model.p.m", NodeStatus::Success, 1.0),
            ],
        );

        let parsed: serde_json::Value = serde_json::from_str(&build_freshness_json(&input, true)?)?;
        let results = parsed["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1, "models and plain results must be excluded");
        assert_eq!(results[0]["unique_id"], "source.p.s.orders");
        assert_eq!(results[0]["status"], "pass");
        assert!(
            (results[0]["max_loaded_at_time_ago_in_s"]
                .as_f64()
                .expect("age")
                - 3600.0)
                .abs()
                < f64::EPSILON
        );
        assert!(results[0]["criteria"].is_object());
        assert!(
            results[0].get("resource_type").is_none(),
            "sources.json's shape must not gain resource_type"
        );
        Ok(())
    }

    #[test]
    fn freshness_json_carries_models_and_resource_types() -> anyhow::Result<()> {
        let input = freshness_input(
            "freshness",
            vec![
                with_freshness("source.p.s.orders", "source", "pass"),
                with_freshness("model.p.stg_orders", "model", "warn"),
                sample_result("model.p.m", NodeStatus::Success, 1.0),
            ],
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&build_freshness_json(&input, false)?)?;
        let results = parsed["results"].as_array().expect("results array");
        assert_eq!(results.len(), 2, "both measured nodes belong in freshness.json");
        assert_eq!(results[0]["resource_type"], "source");
        assert_eq!(results[1]["resource_type"], "model");
        assert_eq!(results[1]["status"], "warn");
        Ok(())
    }

    #[test]
    fn build_run_results_json_structure() -> anyhow::Result<()> {
        let input = StoreArtifactsInput {
            invocation_id: "inv-123".into(),
            project: None,
            command: None,
            node_results: vec![
                sample_result("model.a", NodeStatus::Success, 1.5),
                sample_result("model.b", NodeStatus::Error, 0.3),
            ],
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
        };

        let json_str = build_run_results_json(&input)?;
        let parsed: serde_json::Value = serde_json::from_str(&json_str)?;

        assert_eq!(parsed["metadata"]["invocation_id"], "inv-123");
        assert!(parsed["metadata"]["dbt_version"].is_string());
        assert!(parsed["metadata"]["generated_at"].is_string());
        assert_eq!(
            parsed["results"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("results is array"))?
                .len(),
            2
        );
        assert!(
            (parsed["elapsed_time"]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("elapsed_time is f64"))?
                - 1.8)
                .abs()
                < f64::EPSILON
        );
        Ok(())
    }

    #[test]
    fn build_run_results_json_empty_results() -> anyhow::Result<()> {
        let input = StoreArtifactsInput {
            invocation_id: "inv-empty".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
        };

        let json_str = build_run_results_json(&input)?;
        let parsed: serde_json::Value = serde_json::from_str(&json_str)?;

        assert_eq!(
            parsed["results"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("results is array"))?
                .len(),
            0
        );
        assert!(
            (parsed["elapsed_time"]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("elapsed_time is f64"))?)
            .abs()
                < f64::EPSILON
        );
        Ok(())
    }

    // --- store_artifacts_inner: end-to-end against the local artifact store ---

    use std::sync::Arc;

    use crate::artifact_store::LocalArtifactStore;
    use crate::config::{
        RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts, WriteRunLog,
    };
    use crate::project_registry::ProjectRegistry;

    fn activities_with_local_store(
        base_dir: std::path::PathBuf,
        write_run_log: bool,
    ) -> DbtActivities {
        DbtActivities {
            registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
            artifact_store: Some(Arc::new(LocalArtifactStore::new(base_dir))),
            search_attr_config: SearchAttributeConfig(BTreeMap::new()),
            registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
            write_run_log: WriteRunLog(write_run_log),
            write_artifacts: WriteArtifacts(true),
            write_catalog: crate::config::WriteCatalog(false),
            priority_scheduling: crate::config::PriorityScheduling(false),
        }
    }

    #[tokio::test]
    async fn store_artifacts_inner_writes_run_results_and_inline_manifest() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            invocation_id: "inv-1".into(),
            project: None,
            command: None,
            node_results: vec![sample_result("model.a", NodeStatus::Success, 0.1)],
            manifest_json: Some("{\"manifest\":\"yes\"}".to_string()),
            manifest_ref: None,
            run_log: None,
        };

        let out = store_artifacts_inner(&activities, input).await?;
        assert!(out.run_results_path.contains("run_results.json"));
        assert!(out.manifest_path.contains("manifest.json"));
        assert!(out.log_path.is_none());

        let on_disk = std::fs::read_to_string(&out.run_results_path)?;
        assert!(on_disk.contains("model.a"));
        let manifest = std::fs::read_to_string(&out.manifest_path)?;
        assert!(manifest.contains("manifest"));
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_uses_existing_manifest_ref() -> anyhow::Result<()> {
        // When the plan phase already stored a large manifest at a known path,
        // the activity should pass that path through verbatim instead of
        // re-storing it.
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            invocation_id: "inv-2".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: None,
            manifest_ref: Some("/already/stored/manifest.json".to_string()),
            run_log: None,
        };

        let out = store_artifacts_inner(&activities, input).await?;
        assert_eq!(out.manifest_path, "/already/stored/manifest.json");
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_errors_when_neither_manifest_provided() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            invocation_id: "inv-3".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
        };

        let err = store_artifacts_inner(&activities, input)
            .await
            .expect_err("absent manifest must error");
        assert!(err.to_string().contains("manifest"), "got: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_writes_log_when_enabled() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), true);

        let input = StoreArtifactsInput {
            invocation_id: "inv-log".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: Some("line a\nline b".to_string()),
        };

        let out = store_artifacts_inner(&activities, input).await?;
        let log_path = out
            .log_path
            .expect("log_path should be set when run_log is on");
        assert!(log_path.ends_with("log.txt"));
        let on_disk = std::fs::read_to_string(&log_path)?;
        assert_eq!(on_disk, "line a\nline b");
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_skips_log_when_run_log_disabled() -> anyhow::Result<()> {
        // Run log was supplied but the writer is disabled — the file must not
        // be written and log_path stays None.
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            invocation_id: "inv-skiplog".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: Some("would-be-log".to_string()),
        };

        let out = store_artifacts_inner(&activities, input).await?;
        assert!(out.log_path.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_errors_when_no_artifact_store_configured() -> anyhow::Result<()>
    {
        // The activity guards against being called on an unconfigured worker.
        let activities = DbtActivities {
            registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
            artifact_store: None,
            search_attr_config: SearchAttributeConfig(BTreeMap::new()),
            registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
            write_run_log: WriteRunLog(false),
            write_artifacts: WriteArtifacts(true),
            write_catalog: crate::config::WriteCatalog(false),
            priority_scheduling: crate::config::PriorityScheduling(false),
        };

        let input = StoreArtifactsInput {
            invocation_id: "inv-noop".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: None,
        };

        let err = store_artifacts_inner(&activities, input)
            .await
            .expect_err("missing ArtifactStore must error");
        assert!(err.to_string().contains("ArtifactStore not configured"));
        // A misconfigured worker is permanent — retrying cannot fix it.
        assert!(
            !retry::downcast_or_default(err, retry::Unclassified::Permanent).is_retryable(),
            "missing store config must not retry"
        );
        Ok(())
    }

    /// The run is already finished by the time this activity runs, so losing it
    /// to a transient object-store 5xx would discard completed work.
    #[tokio::test]
    async fn store_io_failure_is_classified_retryable() {
        let err = store_io_error("storing run_results.json", anyhow::anyhow!("503 slow down"));

        let classified = retry::downcast_or_default(err, retry::Unclassified::Permanent);
        assert!(
            classified.is_retryable(),
            "artifact-store I/O must retry even under the activity's permanent default"
        );
        let msg = classified.to_string();
        assert!(msg.contains("storing run_results.json"), "context lost: {msg}");
        assert!(msg.contains("503 slow down"), "cause lost: {msg}");
    }
}
