use anyhow::Context;
use tracing::info;

use crate::types::{StoreArtifactsInput, StoreArtifactsOutput};

use super::DbtActivities;

/// Store run_results.json and manifest.json to the configured artifact store.
///
/// Called from `DbtActivities::store_artifacts`.
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
        .context("storing run_results.json")?;

    info!(path = %run_results_path, "stored run_results.json");

    // Store manifest (if inline) or note existing ref.
    let manifest_path = if let Some(manifest_json) = &input.manifest_json {
        store
            .store(&input.invocation_id, "manifest.json", manifest_json.as_bytes())
            .await
            .context("storing manifest.json")?
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
                .context("storing log.txt")?;
            info!(path = %path, "stored log.txt");
            Some(path)
        } else {
            None
        }
    } else {
        None
    };

    Ok(StoreArtifactsOutput {
        run_results_path,
        manifest_path,
        log_path,
    })
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
        }
    }

    #[test]
    fn build_run_results_json_structure() -> anyhow::Result<()> {
        let input = StoreArtifactsInput {
            invocation_id: "inv-123".into(),
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
        }
    }

    #[tokio::test]
    async fn store_artifacts_inner_writes_run_results_and_inline_manifest() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            invocation_id: "inv-1".into(),
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
        };

        let input = StoreArtifactsInput {
            invocation_id: "inv-noop".into(),
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: None,
        };

        let err = store_artifacts_inner(&activities, input)
            .await
            .expect_err("missing ArtifactStore must error");
        assert!(err.to_string().contains("ArtifactStore not configured"));
        Ok(())
    }
}
