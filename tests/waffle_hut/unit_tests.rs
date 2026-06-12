//! dbt unit test execution: fixture-mocked model runs that gate the tested
//! model. A passing unit test lets the model build; a failing one errors the
//! unit test node and skips the model and everything downstream.

use anyhow::{Context, Result};
use dbt_temporal::types::NodeStatus;

use super::infra::{
    connect_client, copy_fixture, init_tracing, make_input, run_dbt_workflow,
    run_dbt_workflow_expect_failure, shared_infra, test_config,
};

/// YAML defining a unit test on stg_orders with a mocked source input.
/// `expect_status` lets the failing variant flip one expected value.
fn stg_orders_unit_test_yml(expected_status: &str) -> String {
    format!(
        "
unit_tests:
  - name: stg_orders_maps_columns
    model: stg_orders
    given:
      - input: source('waffle_hut', 'orders')
        rows:
          - {{id: 1, user_id: 7, order_date: \"2018-01-01\", status: completed}}
          - {{id: 2, user_id: 8, order_date: \"2018-01-02\", status: returned}}
    expect:
      rows:
        - {{order_id: 1, customer_id: 7, order_date: \"2018-01-01\", status: completed}}
        - {{order_id: 2, customer_id: 8, order_date: \"2018-01-02\", status: {expected_status}}}
"
    )
}

/// Happy path: the unit test node is planned by `build`, executes against
/// fixture rows only (the model itself hasn't been built yet when it runs),
/// and passes; the gated model then builds normally.
#[tokio::test(flavor = "current_thread")]
async fn test_unit_test_passes_in_build() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(
        fixture_dir.join("models/staging/stg_orders_unit.yml"),
        stg_orders_unit_test_yml("returned"),
    )
    .context("writing unit test yml")?;

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
            let run =
                run_dbt_workflow(&client, &task_queue, make_input("build", None, None, false))
                    .await?;
            assert!(run.output.success, "build with passing unit test should succeed");

            let unit_result = run
                .output
                .node_results
                .iter()
                .find(|r| {
                    r.unique_id.starts_with("unit_test.")
                        && r.unique_id.contains("stg_orders_maps_columns")
                })
                .context("unit test node missing from results")?;
            assert_eq!(
                unit_result.status,
                NodeStatus::Success,
                "unit test should pass: {:?}",
                unit_result.message
            );
            assert!(
                unit_result
                    .message
                    .as_deref()
                    .is_some_and(|m| m.contains("unit test passed")),
                "unexpected unit test message: {:?}",
                unit_result.message
            );

            let model_result = run
                .output
                .node_results
                .iter()
                .find(|r| r.unique_id == "model.waffle_hut.stg_orders")
                .context("stg_orders missing from results")?;
            assert_eq!(model_result.status, NodeStatus::Success);
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}

/// Failing unit test: the unit test node errors with a diff, the tested model
/// is skipped (the gate edge), and downstream marts are skipped transitively.
/// Independent staging models still build (fail_fast=false).
#[tokio::test(flavor = "current_thread")]
async fn test_unit_test_failure_skips_model_and_downstream() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(
        fixture_dir.join("models/staging/stg_orders_unit.yml"),
        stg_orders_unit_test_yml("definitely_wrong"),
    )
    .context("writing failing unit test yml")?;

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
                make_input("build", None, None, false),
            )
            .await?;

            let status_of = |needle: &str| {
                node_status
                    .nodes
                    .iter()
                    .find(|(k, _)| k.contains(needle))
                    .map(|(_, v)| *v)
            };

            assert_eq!(
                status_of("stg_orders_maps_columns"),
                Some(NodeStatus::Error),
                "failing unit test should error"
            );
            assert_eq!(
                status_of("model.waffle_hut.stg_orders"),
                Some(NodeStatus::Skipped),
                "tested model must be skipped when its unit test fails"
            );
            assert_eq!(
                status_of("model.waffle_hut.orders"),
                Some(NodeStatus::Skipped),
                "downstream mart must be skipped transitively"
            );
            assert_eq!(
                status_of("model.waffle_hut.customers"),
                Some(NodeStatus::Skipped),
                "downstream mart must be skipped transitively"
            );
            // Independent staging models are unaffected (fail_fast=false).
            assert_eq!(status_of("model.waffle_hut.stg_customers"), Some(NodeStatus::Success));
            assert_eq!(status_of("model.waffle_hut.stg_payments"), Some(NodeStatus::Success));
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}

/// Definition errors surface as activity failures, not panics: overrides are
/// rejected with a clear error, and a `given` input the model never
/// references is caught before any SQL runs. Both gate their models.
#[tokio::test(flavor = "current_thread")]
async fn test_unit_test_definition_errors() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(
        fixture_dir.join("models/staging/bad_unit_tests.yml"),
        "
unit_tests:
  - name: uses_overrides
    model: stg_orders
    overrides:
      macros:
        is_incremental: false
    given:
      - input: source('waffle_hut', 'orders')
        rows:
          - {id: 1, user_id: 7, order_date: \"2018-01-01\", status: completed}
    expect:
      rows:
        - {order_id: 1, customer_id: 7, order_date: \"2018-01-01\", status: completed}
  - name: mocks_unreferenced_input
    model: stg_customers
    given:
      - input: source('waffle_hut', 'payments')
        rows:
          - {id: 1, order_id: 1, payment_method: coupon, amount: 100}
    expect:
      rows:
        - {customer_id: 1, first_name: a, last_name: b}
",
    )
    .context("writing bad unit tests yml")?;

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
                make_input("build", None, None, false),
            )
            .await?;

            let status_of = |needle: &str| {
                node_status
                    .nodes
                    .iter()
                    .find(|(k, _)| k.contains(needle))
                    .map(|(_, v)| *v)
            };

            assert_eq!(status_of("uses_overrides"), Some(NodeStatus::Error));
            assert_eq!(status_of("mocks_unreferenced_input"), Some(NodeStatus::Error));
            assert_eq!(
                status_of("model.waffle_hut.stg_orders"),
                Some(NodeStatus::Skipped),
                "model must be gated by its failing unit test"
            );
            assert_eq!(status_of("model.waffle_hut.stg_customers"), Some(NodeStatus::Skipped));
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}
