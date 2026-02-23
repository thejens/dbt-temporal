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
    let run_results = serde_json::json!({
        "metadata": {
            "invocation_id": input.invocation_id,
            "dbt_version": env!("CARGO_PKG_VERSION"),
            "generated_at": chrono::Utc::now().to_rfc3339(),
        },
        "results": input.node_results,
        "elapsed_time": input.node_results.iter().map(|r| r.execution_time).sum::<f64>(),
    });
    serde_json::to_string_pretty(&run_results).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeExecutionResult;
    use std::collections::BTreeMap;

    fn sample_result(unique_id: &str, status: &str, time: f64) -> NodeExecutionResult {
        NodeExecutionResult {
            unique_id: unique_id.into(),
            status: status.into(),
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
                sample_result("model.a", "success", 1.5),
                sample_result("model.b", "error", 0.3),
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
}
