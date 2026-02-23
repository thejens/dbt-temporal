//! Artifact storage tests: run_results.json, manifest.json, and log.txt.

use std::path::Path;

use anyhow::{Context, Result};

use super::infra::*;

/// Test artifact storage: enable write_artifacts and write_run_log, run a workflow,
/// then verify that run_results.json, manifest.json, and log.txt exist on disk
/// with valid content.
#[tokio::test(flavor = "current_thread")]
async fn test_artifacts() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");

    let mut config = test_config(infra, &fixture_dir)?;
    config.write_artifacts = true;
    config.write_run_log = true;
    let task_queue = config.temporal_task_queue.clone();
    let artifact_dir = config.artifact_store.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);

    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            result = worker.run() => result,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;

            let run =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            let output = &run.output;

            tracing::info!("Artifact test results:");
            print_results(output);

            assert!(output.success, "dbt run should succeed");

            // --- Artifact output assertions ---
            let artifacts = output.artifacts.as_ref().ok_or_else(|| {
                anyhow::anyhow!("artifacts should be present when write_artifacts=true")
            })?;

            // Verify run_results.json exists and is valid JSON.
            assert!(
                Path::new(&artifacts.run_results_path).exists(),
                "run_results.json should exist at {}",
                artifacts.run_results_path
            );
            let run_results_bytes =
                std::fs::read(&artifacts.run_results_path).context("reading run_results.json")?;
            let run_results: serde_json::Value =
                serde_json::from_slice(&run_results_bytes).context("parsing run_results.json")?;
            let results_array = run_results["results"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("run_results.json results should be an array"))?;
            assert_eq!(results_array.len(), 5, "run_results.json should have 5 results");
            assert!(
                run_results["metadata"]["invocation_id"].is_string(),
                "run_results.json should have invocation_id"
            );

            // Verify manifest.json exists and is valid JSON.
            assert!(
                Path::new(&artifacts.manifest_path).exists(),
                "manifest.json should exist at {}",
                artifacts.manifest_path
            );
            let manifest_bytes =
                std::fs::read(&artifacts.manifest_path).context("reading manifest.json")?;
            let _manifest: serde_json::Value =
                serde_json::from_slice(&manifest_bytes).context("parsing manifest.json")?;

            // Verify log.txt exists and has meaningful content.
            let log_path = artifacts
                .log_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("log_path should be set when write_run_log=true"))?;
            assert!(Path::new(log_path).exists(), "log.txt should exist at {log_path}");
            let log_content = std::fs::read_to_string(log_path).context("reading log.txt")?;
            assert!(
                log_content.contains("PASS=5"),
                "log.txt should contain PASS=5 summary, got: {log_content}"
            );

            // Verify the output also has log_path set.
            assert!(
                output.log_path.is_some(),
                "DbtRunOutput.log_path should be set when write_run_log=true"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&artifact_dir).ok();

    test_result
}
