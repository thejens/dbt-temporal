//! Source freshness (`source-freshness` command): per-source freshness
//! queries with warn_after/error_after evaluation. Sources without freshness
//! criteria are excluded from the plan; stale sources fail the workflow.

use anyhow::{Context, Result};
use dbt_temporal::types::NodeStatus;

use super::infra::{
    connect_client, copy_fixture, init_tracing, make_input, run_dbt_workflow,
    run_dbt_workflow_expect_failure, shared_infra, test_config,
};

/// Sources with freshness criteria tuned so `orders` passes (raw data is from
/// 2018, threshold ~27 years) and `payments` warns via a custom
/// `loaded_at_query`. `customers` has no criteria and must not be planned.
const FRESH_SOURCES_YML: &str = "
version: 2

sources:
  - name: waffle_hut
    schema: raw
    tables:
      - name: customers
      - name: orders
        config:
          loaded_at_field: order_date
          freshness:
            error_after: {count: 10000, period: day}
      - name: payments
        config:
          loaded_at_query: \"select '2026-01-01'::timestamp\"
          freshness:
            warn_after: {count: 1, period: day}
            error_after: {count: 100000, period: day}
";

/// `orders` allows only 1 day of staleness — the 2018 data is hopelessly
/// stale, so the check must error.
const STALE_SOURCES_YML: &str = "
version: 2

sources:
  - name: waffle_hut
    schema: raw
    tables:
      - name: customers
      - name: orders
        config:
          loaded_at_field: order_date
          freshness:
            error_after: {count: 1, period: day}
      - name: payments
";

#[tokio::test(flavor = "current_thread")]
async fn test_source_freshness_pass_and_warn() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    std::fs::write(fixture_dir.join("models/sources.yml"), FRESH_SOURCES_YML)
        .context("writing sources.yml with freshness")?;

    let config = test_config(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);
    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            r = worker.run() => r,
            () = worker_abort_rx.notified() => Ok(()),
        }
    });

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("source-freshness", None, None, false),
            )
            .await?;
            assert!(run.output.success, "freshness run should succeed");

            // Only the two sources with criteria are planned — no models, no
            // criteria-less customers source.
            assert_eq!(
                run.output.node_results.len(),
                2,
                "exactly the freshness-checkable sources should run: {:?}",
                run.output
                    .node_results
                    .iter()
                    .map(|r| &r.unique_id)
                    .collect::<Vec<_>>()
            );
            for r in &run.output.node_results {
                assert!(r.unique_id.starts_with("source."), "unexpected node {}", r.unique_id);
                assert_eq!(r.status, NodeStatus::Success);
            }

            let orders = run
                .output
                .node_results
                .iter()
                .find(|r| r.unique_id.ends_with(".orders"))
                .context("orders source missing")?;
            let orders_freshness = orders
                .freshness
                .as_ref()
                .context("orders freshness outcome missing")?;
            assert_eq!(orders_freshness.status, "pass");
            assert!(orders_freshness.max_loaded_at.starts_with("2018-01-05"));
            // ~8 years old: sanity-check the age arithmetic.
            assert!(orders_freshness.max_loaded_at_time_ago_in_s > 8.0 * 365.0 * 86_400.0);

            let payments = run
                .output
                .node_results
                .iter()
                .find(|r| r.unique_id.ends_with(".payments"))
                .context("payments source missing")?;
            let payments_freshness = payments
                .freshness
                .as_ref()
                .context("payments freshness outcome missing")?;
            assert_eq!(payments_freshness.status, "warn", "warn_after=1d must warn");
            assert!(payments_freshness.max_loaded_at.starts_with("2026-01-01"));
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}

#[tokio::test(flavor = "current_thread")]
async fn test_source_freshness_stale_errors() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    std::fs::write(fixture_dir.join("models/sources.yml"), STALE_SOURCES_YML)
        .context("writing stale sources.yml")?;

    let config = test_config(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);
    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            r = worker.run() => r,
            () = worker_abort_rx.notified() => Ok(()),
        }
    });

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let mut client = connect_client(&infra.temporal_addr).await?;
            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("source-freshness", None, None, false),
            )
            .await?;

            let orders = node_status
                .nodes
                .iter()
                .find(|(k, _)| k.starts_with("source.") && k.ends_with(".orders"))
                .context("orders source missing from memo")?;
            assert_eq!(*orders.1, NodeStatus::Error, "stale source must error");
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}
