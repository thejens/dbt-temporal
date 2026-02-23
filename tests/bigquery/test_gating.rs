//! Test-gating: when a data test fails, downstream models are skipped.
//!
//! Injects a singular test on stg_stations that always fails, then verifies that
//! downstream models (stg_stations_deduped, station_summary, and their transitive
//! dependents) are skipped while independent models still succeed.

use anyhow::{Context, Result};

use dbt_temporal::types::NodeStatus;

use super::infra::*;

#[tokio::test(flavor = "current_thread")]
async fn test_gating_skips_downstreams_on_test_failure() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let _guard = test_guard(infra);

    // Copy the BigQuery fixture and inject an always-failing test on stg_stations.
    let fixture_dir = copy_bigquery_fixture()?;
    let failing_test = r"
-- Always-failing singular test on stg_stations.
-- A dbt data test fails when it returns any rows.
select 1 as bad_row
";
    std::fs::write(fixture_dir.join("tests/assert_stg_stations_always_fails.sql"), failing_test)
        .context("writing failing test")?;

    let config = bigquery_test_config_with_project_dir(
        &infra.temporal_addr,
        &infra.gcp_project,
        &infra.dataset,
        &fixture_dir,
    )?;
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

            let mut client = connect_client(&infra.temporal_addr).await?;

            // Run `dbt build` with fail_fast=false so independent branches keep running.
            tracing::info!("Test-gating: build with injected failing test, fail_fast=false");
            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("build", None, None, false),
            )
            .await?;

            tracing::info!("Node status tree:");
            print_node_status(&node_status);

            // --- The injected test should have errored ---
            let errored = nodes_with_status(&node_status, NodeStatus::Error);
            assert!(
                errored.iter().any(|id| id.contains("assert_stg_stations_always_fails")),
                "injected test should be in errored nodes, got: {errored:?}"
            );

            // --- stg_stations itself should succeed (it runs before its tests) ---
            let stg_stations_status = node_status
                .nodes
                .iter()
                .find(|(id, _)| id.contains("stg_stations") && !id.contains("deduped") && id.starts_with("model."))
                .map(|(_, s)| s);
            assert_eq!(
                stg_stations_status,
                Some(&NodeStatus::Success),
                "stg_stations model should succeed (tests run after it)"
            );

            // --- Downstream models that depend on stg_stations should be skipped ---
            // stg_stations_deduped refs stg_stations, so with the test gate it
            // must wait for all tests on stg_stations to pass. The injected test
            // fails, so stg_stations_deduped should be skipped.
            let skipped = nodes_with_status(&node_status, NodeStatus::Skipped);

            assert!(
                skipped.iter().any(|id| id.contains("stg_stations_deduped")),
                "stg_stations_deduped should be skipped due to failed test gate, skipped: {skipped:?}"
            );

            assert!(
                skipped.iter().any(|id| id.contains("station_summary")),
                "station_summary should be skipped due to failed test gate, skipped: {skipped:?}"
            );

            // --- Independent models (stg_trips, subscriber_analysis) should succeed ---
            // stg_trips doesn't depend on stg_stations, so it's unaffected.
            let stg_trips_status = node_status
                .nodes
                .iter()
                .find(|(id, _)| id.contains("stg_trips") && id.starts_with("model."))
                .map(|(_, s)| s);
            assert_eq!(
                stg_trips_status,
                Some(&NodeStatus::Success),
                "stg_trips (independent of stg_stations) should succeed"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}
