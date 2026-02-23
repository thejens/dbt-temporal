//! Basic BigQuery execution tests: happy path build and select filtering.

use anyhow::{Context, Result};

use super::infra::*;

/// Helper to find a node result by unique_id substring.
fn find_result<'a>(
    output: &'a dbt_temporal::types::DbtRunOutput,
    needle: &str,
) -> Option<&'a dbt_temporal::types::NodeExecutionResult> {
    output
        .node_results
        .iter()
        .find(|r| r.unique_id.contains(needle))
}

/// Happy path: `dbt build` all nodes end-to-end.
///
/// Uses `build` (the default workflow command) which runs seeds, models, tests,
/// and snapshots in DAG order. This catches issues that `dbt run` alone misses,
/// like models that depend on seeds.
#[tokio::test(flavor = "current_thread")]
async fn test_bigquery_dbt_build() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let _guard = test_guard(infra);
    let config = bigquery_test_config(&infra.temporal_addr, &infra.gcp_project, &infra.dataset)?;
    let task_queue = config.temporal_task_queue.clone();

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
                run_dbt_workflow(&client, &task_queue, make_input("build", None, None, true))
                    .await?;
            let output = &run.output;

            tracing::info!("BigQuery build results:");
            print_results(output);

            assert!(output.success, "dbt build should succeed");
            assert!(!output.invocation_id.is_empty(), "invocation_id should be set");
            assert!(output.elapsed_time > 0.0, "elapsed_time should be positive");

            // --- Seeds: should run before models that depend on them ---
            let seed_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("seed."))
                .collect();
            assert!(
                seed_results.len() >= 2,
                "should have at least 2 seed results (station_status_mapping, trip_subscriber_types), got {}",
                seed_results.len()
            );
            for r in &seed_results {
                assert_eq!(r.status, "success", "seed {} should succeed", r.unique_id);
            }

            // --- Models: 9 non-ephemeral models ---
            let model_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            assert_eq!(
                model_results.len(),
                9,
                "should have 9 non-ephemeral model results (12 total minus 3 ephemeral)"
            );
            for r in &model_results {
                assert_eq!(r.status, "success", "model {} should succeed", r.unique_id);
                assert!(
                    r.compiled_code.is_some(),
                    "model {} should have compiled_code",
                    r.unique_id
                );
                assert!(
                    r.execution_time > 0.0,
                    "model {} should have positive execution_time",
                    r.unique_id
                );
            }

            // Verify specific expected models ran (not just counts).
            let expected_models = [
                "stg_stations",
                "stg_stations_deduped",
                "stg_trips",
                "active_stations",
                "station_summary",
                "trip_summary",
                "subscriber_analysis",
                "daily_station_activity",
                "station_dashboard_summary",
            ];
            for name in &expected_models {
                assert!(
                    model_results.iter().any(|r| r.unique_id.contains(name)),
                    "expected model {name} to be in results"
                );
            }

            // --- Compiled SQL assertions: verify vars and macros resolved ---

            // stg_stations should contain the resolved var("station_active_status") = 'active'
            let stg_stations = find_result(output, "stg_stations")
                .ok_or_else(|| anyhow::anyhow!("stg_stations not in results"))?;
            let sql = stg_stations.compiled_code.as_deref()
                .ok_or_else(|| anyhow::anyhow!("stg_stations has no compiled_code"))?;
            assert!(
                sql.contains("'active'"),
                "stg_stations compiled SQL should contain resolved var 'active', got: {sql}"
            );
            // Should reference the public dataset source
            assert!(
                sql.contains("bigquery-public-data"),
                "stg_stations compiled SQL should reference bigquery-public-data source, got: {sql}"
            );

            // station_summary should contain the resolved var("shared_var") = 'hello_from_vars'
            let station_summary = find_result(output, "station_summary")
                .ok_or_else(|| anyhow::anyhow!("station_summary not in results"))?;
            let sql = station_summary.compiled_code.as_deref()
                .ok_or_else(|| anyhow::anyhow!("station_summary has no compiled_code"))?;
            assert!(
                sql.contains("hello_from_vars"),
                "station_summary compiled SQL should contain resolved var 'hello_from_vars', got: {sql}"
            );

            // active_stations depends on ephemeral_helper which refs the seed —
            // if seeds didn't run first, this model would fail.
            let active_stations = find_result(output, "active_stations")
                .ok_or_else(|| anyhow::anyhow!("active_stations not in results"))?;
            assert_eq!(
                active_stations.status, "success",
                "active_stations (depends on seed via ephemeral) should succeed"
            );

            // --- Snapshot ---
            let snapshot_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("snapshot."))
                .collect();
            assert_eq!(snapshot_results.len(), 1, "should have 1 snapshot result");
            assert_eq!(
                snapshot_results[0].status, "success",
                "snapshot should succeed"
            );

            // --- Tests: data tests from schema.yml should have run ---
            let test_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("test."))
                .collect();
            assert!(
                test_results.len() >= 10,
                "should have at least 10 data test results, got {}",
                test_results.len()
            );
            let test_failures: Vec<_> = test_results
                .iter()
                .filter(|r| r.status == "error")
                .collect();
            assert!(
                test_failures.is_empty(),
                "all data tests should pass, but {} failed: {:?}",
                test_failures.len(),
                test_failures.iter().map(|r| &r.unique_id).collect::<Vec<_>>()
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}

/// Select a single staging model and verify only it runs.
#[tokio::test(flavor = "current_thread")]
async fn test_bigquery_select() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let _guard = test_guard(infra);
    let config = bigquery_test_config(&infra.temporal_addr, &infra.gcp_project, &infra.dataset)?;
    let task_queue = config.temporal_task_queue.clone();

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

            tracing::info!("Select test: select=stg_stations");
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("run", Some("stg_stations"), None, false),
            )
            .await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success);
            assert_eq!(output.node_results.len(), 1, "should run exactly 1 model");
            assert!(
                output.node_results[0].unique_id.contains("stg_stations"),
                "should be stg_stations, got {}",
                output.node_results[0].unique_id
            );
            assert_eq!(output.node_results[0].status, "success");

            // Verify compiled SQL has resolved var and source reference.
            let sql = output.node_results[0]
                .compiled_code
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("stg_stations has no compiled_code"))?;
            assert!(sql.contains("'active'"), "compiled SQL should contain resolved var 'active'");

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}
