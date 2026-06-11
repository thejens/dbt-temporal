//! Failure handling tests: fail_fast behavior with broken models.

use anyhow::{Context, Result};

use dbt_temporal::types::NodeStatus;

use super::infra::*;

/// Test fail_fast behavior: a broken model causes downstream nodes to be skipped.
///
/// Uses a copy of the fixture with stg_customers.sql replaced by bad SQL.
///
/// DAG:
///   stg_customers (BROKEN) ──┐
///   stg_orders ──────────────┼──> customers
///   stg_payments ────────────┘
///   stg_orders ──────┬──> orders
///   stg_payments ────┘
///
/// With fail_fast=true:
///   Level 0: stg_customers=error, stg_orders=success, stg_payments=success
///   Level 1: entirely skipped (customers=skipped, orders=skipped)
///
/// With fail_fast=false:
///   Level 0: stg_customers=error, stg_orders=success, stg_payments=success
///   Level 1: customers=skipped (upstream dep failed), orders=success
#[tokio::test(flavor = "current_thread")]
async fn test_fail_fast() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    // Copy fixture and break stg_customers with a ref to a nonexistent model.
    // This causes a resolution error recorded in nodes_with_resolution_errors,
    // which execute_node detects and fails with a compilation error.
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    let broken_sql = "select * from {{ ref('totally_nonexistent_model') }}";
    let broken_path = fixture_dir.join("models/staging/stg_customers.sql");
    std::fs::write(&broken_path, broken_sql).context("writing broken model")?;

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
            result = worker.run() => result,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let mut client = connect_client(&infra.temporal_addr).await?;

            // The dbt workflow returns Err (Temporal Failure) when nodes error.
            // We use `run_dbt_workflow_expect_failure` to read the node status
            // from the workflow memo instead.

            // --- fail_fast=true: entire level 1 is skipped ---
            tracing::info!("Fail-fast test: fail_fast=true");
            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("run", None, None, true),
            )
            .await?;

            let nodes = &node_status.nodes;
            tracing::info!("fail_fast=true node statuses:");
            for (id, status) in nodes {
                tracing::info!("  {id} -> {status:?}");
            }

            assert_eq!(nodes.len(), 5, "all 5 models should have status entries");

            let errors: Vec<_> = nodes
                .iter()
                .filter(|(_, s)| **s == NodeStatus::Error)
                .map(|(k, _)| k.clone())
                .collect();
            let success_count = nodes
                .iter()
                .filter(|(_, s)| **s == NodeStatus::Success)
                .count();
            let skipped_count = nodes
                .iter()
                .filter(|(_, s)| **s == NodeStatus::Skipped)
                .count();

            assert_eq!(errors.len(), 1, "exactly 1 error (stg_customers)");
            assert!(errors[0].contains("stg_customers"));
            assert_eq!(success_count, 2, "2 successes (stg_orders, stg_payments)");
            assert_eq!(skipped_count, 2, "2 skipped (customers, orders) due to fail_fast");

            // --- fail_fast=false: only customers skipped, orders succeeds ---
            tracing::info!("Fail-fast test: fail_fast=false");
            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("run", None, None, false),
            )
            .await?;

            let nodes = &node_status.nodes;
            tracing::info!("fail_fast=false node statuses:");
            for (id, status) in nodes {
                tracing::info!("  {id} -> {status:?}");
            }

            assert_eq!(nodes.len(), 5, "all 5 models should have status entries");

            let errors: Vec<_> = nodes
                .iter()
                .filter(|(_, s)| **s == NodeStatus::Error)
                .map(|(k, _)| k.clone())
                .collect();
            let successes: Vec<_> = nodes
                .iter()
                .filter(|(_, s)| **s == NodeStatus::Success)
                .map(|(k, _)| k.clone())
                .collect();
            let skipped: Vec<_> = nodes
                .iter()
                .filter(|(_, s)| **s == NodeStatus::Skipped)
                .map(|(k, _)| k.clone())
                .collect();

            assert_eq!(errors.len(), 1, "exactly 1 error");
            assert!(errors[0].contains("stg_customers"));
            assert_eq!(successes.len(), 3, "3 successes: stg_orders, stg_payments, orders");
            assert!(successes.iter().any(|n| n.contains("orders")));
            assert_eq!(skipped.len(), 1, "1 skipped: customers");
            assert!(skipped[0].contains("customers"));

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}

/// A test node whose query returns failing rows: exercises both severity
/// branches in `execute_node`'s test-failure handler.
///
/// Adds two `accepted_values` tests on `stg_customers.first_name`:
///   - `severity: warn` → handler emits a warning, node still reports success.
///   - default (error) → handler returns DbtTemporalError::TestFailure, the
///     workflow surfaces the test as an error result.
#[tokio::test(flavor = "current_thread")]
async fn test_test_failures_severity_dispatch() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    // Replace schema.yml with one whose `accepted_values` lists exclude every
    // real value, so both tests fail.
    std::fs::write(
        fixture_dir.join("models/schema.yml"),
        r"version: 2

models:
  - name: stg_customers
    columns:
      - name: first_name
        tests:
          - accepted_values:
              name: warn_failing
              values: ['__never__']
              config:
                severity: warn
          - accepted_values:
              name: error_failing
              values: ['__also_never__']
",
    )
    .context("writing schema.yml with failing tests")?;

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
            // The error-severity test will fail → workflow returns Err. We pull
            // statuses from the memo via `run_dbt_workflow_expect_failure`.
            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("build", None, None, false),
            )
            .await?;

            let warn_status = node_status
                .nodes
                .iter()
                .find(|(k, _)| k.contains("warn_failing"));
            let error_status = node_status
                .nodes
                .iter()
                .find(|(k, _)| k.contains("error_failing"));
            assert!(warn_status.is_some(), "warn-severity test missing from memo");
            assert!(error_status.is_some(), "error-severity test missing from memo");
            assert_eq!(*warn_status.unwrap().1, NodeStatus::Success, "severity=warn must succeed");
            assert_eq!(*error_status.unwrap().1, NodeStatus::Error, "severity=error must error");
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}

/// Retry-from-failure: a second run with `retry_from` pointing at the failed
/// run's `run_results.json` plans only the nodes that did not succeed.
///
/// Run 1 (fail_fast=false): stg_customers=error, customers=skipped, the other
/// three models succeed. Run 2 with `retry_from` must re-plan exactly the
/// error+skipped pair and leave the succeeded models untouched.
#[tokio::test(flavor = "current_thread")]
async fn test_retry_from_failure() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    let broken_sql = "select * from {{ ref('totally_nonexistent_model') }}";
    std::fs::write(fixture_dir.join("models/staging/stg_customers.sql"), broken_sql)
        .context("writing broken model")?;

    let artifact_dir = tempfile::tempdir()?;
    let mut config = test_config(infra, &fixture_dir)?;
    config.write_artifacts = true;
    config.artifact_store = artifact_dir.path().to_string_lossy().into_owned();
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

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let mut client = connect_client(&infra.temporal_addr).await?;

            // Run 1: partial failure with artifacts written.
            let node_status = run_dbt_workflow_expect_failure(
                &mut client,
                &task_queue,
                make_input("run", None, None, false),
            )
            .await?;
            assert_eq!(node_status.nodes.len(), 5, "run 1 plans all 5 models");

            // Locate run 1's run_results.json (exactly one run so far).
            let run_results: Vec<_> = std::fs::read_dir(artifact_dir.path())?
                .filter_map(std::result::Result::ok)
                .map(|e| e.path().join("run_results.json"))
                .filter(|p| p.exists())
                .collect();
            assert_eq!(run_results.len(), 1, "run 1 should have written run_results.json");
            let run_results_path = run_results[0].to_string_lossy().into_owned();

            // Run 2: retry only what didn't succeed.
            let mut input = make_input("run", None, None, false);
            input.retry_from = Some(run_results_path);
            let node_status =
                run_dbt_workflow_expect_failure(&mut client, &task_queue, input).await?;

            tracing::info!("retry run node statuses:");
            for (id, status) in &node_status.nodes {
                tracing::info!("  {id} -> {status:?}");
            }

            assert_eq!(node_status.nodes.len(), 2, "retry plans only the error + skipped nodes");
            assert_eq!(
                node_status
                    .nodes
                    .iter()
                    .find(|(id, _)| id.ends_with(".stg_customers"))
                    .map(|(_, s)| *s),
                Some(NodeStatus::Error),
                "broken model fails again on retry"
            );
            assert_eq!(
                node_status
                    .nodes
                    .iter()
                    .find(|(id, _)| id.ends_with(".customers"))
                    .map(|(_, s)| *s),
                Some(NodeStatus::Skipped),
                "downstream of the broken model is skipped again"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}
