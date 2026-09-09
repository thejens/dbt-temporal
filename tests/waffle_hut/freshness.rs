//! Freshness commands: per-node freshness queries with warn_after/error_after
//! evaluation. `source-freshness` measures sources only; `freshness` also
//! measures models that declare an SLA. Nodes without freshness criteria are
//! excluded from the plan; a node past `error_after` fails the workflow.

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
                // `orders` is fresh and `payments` is past `warn_after`, so the
                // two statuses differ — neither is a failure.
                assert!(
                    matches!(r.status, NodeStatus::Success | NodeStatus::Warn),
                    "unexpected status for {}: {:?}",
                    r.unique_id,
                    r.status
                );
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
            assert_eq!(orders.status, NodeStatus::Success, "a fresh source is a plain pass");
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
            assert_eq!(payments.status, NodeStatus::Warn, "warn_after=1d must warn the node");
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

/// A model SLA lives under `config: freshness:` in schema.yml, the same place
/// a source declares one. `stg_orders` measures a column of the built relation
/// (2018 data, threshold ~27 years, so it passes); `stg_payments` measures a
/// custom query pinned to a past date so it warns. The remaining models carry
/// no SLA and must stay out of the plan.
const MODEL_FRESHNESS_SCHEMA_YML: &str = "
version: 2

models:
  - name: stg_customers
  - name: customers
  - name: orders
  - name: stg_orders
    config:
      freshness:
        loaded_at_field: order_date
        error_after: {count: 10000, period: day}
  - name: stg_payments
    config:
      freshness:
        loaded_at_query: \"select '2020-01-01'::timestamp\"
        warn_after: {count: 1, period: day}
        error_after: {count: 100000, period: day}
";

/// One source with criteria, so a `freshness` run covers both node kinds and
/// both artifacts. `customers`/`payments` have none and must not be planned.
const ONE_FRESH_SOURCE_YML: &str = "
version: 2

sources:
  - name: waffle_hut
    schema: raw
    tables:
      - name: customers
      - name: payments
      - name: orders
        config:
          loaded_at_field: order_date
          freshness:
            error_after: {count: 10000, period: day}
";

/// A model whose SLA it cannot meet: `stg_orders` reads 2018 data and allows
/// one day of staleness.
const STALE_MODEL_SCHEMA_YML: &str = "
version: 2

models:
  - name: stg_customers
  - name: customers
  - name: orders
  - name: stg_payments
  - name: stg_orders
    config:
      freshness:
        loaded_at_field: order_date
        error_after: {count: 1, period: day}
";

#[tokio::test(flavor = "current_thread")]
async fn test_model_freshness_measures_sources_and_sla_models() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    std::fs::write(fixture_dir.join("models/schema.yml"), MODEL_FRESHNESS_SCHEMA_YML)
        .context("writing schema.yml with model freshness")?;
    std::fs::write(fixture_dir.join("models/sources.yml"), ONE_FRESH_SOURCE_YML)
        .context("writing sources.yml")?;

    let mut config = test_config(infra, &fixture_dir)?;
    config.write_artifacts = true;
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

            // A model's freshness is measured against the relation it built,
            // so the models have to exist before the check runs.
            let built =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            assert!(built.output.success, "the seeding run should succeed");

            let run =
                run_dbt_workflow(&client, &task_queue, make_input("freshness", None, None, false))
                    .await?;
            assert!(run.output.success, "freshness run should succeed");

            let measured: Vec<&str> = run
                .output
                .node_results
                .iter()
                .map(|r| r.unique_id.as_str())
                .collect();
            assert_eq!(measured.len(), 3, "one source plus the two SLA models: {measured:?}");
            assert!(measured.iter().any(|id| id.starts_with("source.")), "{measured:?}");

            let outcome = |suffix: &str| {
                run.output
                    .node_results
                    .iter()
                    .find(|r| r.unique_id.ends_with(suffix))
                    .and_then(|r| r.freshness.as_ref())
                    .with_context(|| format!("no freshness outcome for {suffix}"))
            };

            let source_orders = outcome(".waffle_hut.orders")?;
            assert_eq!(source_orders.status, "pass");
            assert_eq!(source_orders.resource_type, "source");

            let stg_orders = outcome(".stg_orders")?;
            assert_eq!(stg_orders.status, "pass");
            assert_eq!(stg_orders.resource_type, "model");
            assert!(stg_orders.max_loaded_at.starts_with("2018-01-05"), "{stg_orders:?}");

            let stg_payments = outcome(".stg_payments")?;
            assert_eq!(stg_payments.status, "warn", "warn_after=1d must warn");
            assert_eq!(stg_payments.resource_type, "model");

            let artifacts = run
                .output
                .artifacts
                .as_ref()
                .context("artifacts missing with write_artifacts=true")?;
            let artifact_dir = std::path::Path::new(&artifacts.run_results_path)
                .parent()
                .context("run_results.json has no parent dir")?;

            let sources: serde_json::Value =
                serde_json::from_slice(&std::fs::read(artifact_dir.join("sources.json"))?)?;
            let source_rows = sources["results"]
                .as_array()
                .context("sources.json results")?;
            assert_eq!(source_rows.len(), 1, "sources.json holds sources only");
            assert!(
                source_rows[0].get("resource_type").is_none(),
                "sources.json keeps its historical shape"
            );

            let freshness: serde_json::Value =
                serde_json::from_slice(&std::fs::read(artifact_dir.join("freshness.json"))?)?;
            let rows = freshness["results"]
                .as_array()
                .context("freshness.json results")?;
            assert_eq!(rows.len(), 3, "freshness.json holds every measured node");
            assert!(rows.iter().any(|r| r["resource_type"] == "model"));
            assert!(rows.iter().any(|r| r["resource_type"] == "source"));
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}

#[tokio::test(flavor = "current_thread")]
async fn test_model_freshness_stale_errors() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    std::fs::write(fixture_dir.join("models/schema.yml"), STALE_MODEL_SCHEMA_YML)
        .context("writing schema.yml with a stale model SLA")?;
    std::fs::write(fixture_dir.join("models/sources.yml"), ONE_FRESH_SOURCE_YML)
        .context("writing sources.yml")?;

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
            let built =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            assert!(built.output.success, "the seeding run should succeed");

            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("freshness", None, None, false),
            )
            .await?;

            let stale = node_status
                .nodes
                .iter()
                .find(|(k, _)| k.ends_with(".stg_orders"))
                .context("stg_orders missing from memo")?;
            assert_eq!(*stale.1, NodeStatus::Error, "a stale model must error");

            // The measured source shares the level, so a stale model must not
            // take it down with it — freshness has no task graph.
            let source = node_status
                .nodes
                .iter()
                .find(|(k, _)| k.starts_with("source."))
                .context("source missing from memo")?;
            assert_ne!(
                *source.1,
                NodeStatus::Error,
                "the source is measured independently — whatever it graded, it did not fail"
            );
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}
