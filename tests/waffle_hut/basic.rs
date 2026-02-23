//! Basic execution tests: happy path and select/exclude filtering.

use anyhow::{Context, Result};

use dbt_temporal::types::{CommandMemo, NodeStatus, NodeStatusTree};

use super::infra::*;

/// Happy path: run all 5 waffle_hut models end-to-end.
#[tokio::test(flavor = "current_thread")]
async fn test_waffle_hut_dbt_run() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");
    let mut config = test_config(infra, &fixture_dir)?;
    // Add a static search attribute; dynamic ones (DbtProject/DbtCommand) are always populated.
    config
        .search_attributes
        .insert("env".to_string(), "test".to_string());
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

            let run =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            let output = &run.output;

            tracing::info!("Happy path results:");
            print_results(output);

            assert!(output.success, "dbt run should succeed");

            let model_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            assert_eq!(model_results.len(), 5, "should have 5 model results");

            for r in &model_results {
                assert_eq!(r.status, "success", "model {} should succeed", r.unique_id);
            }

            // --- Output field assertions ---
            assert!(!output.invocation_id.is_empty(), "invocation_id should be set");
            assert!(output.elapsed_time > 0.0, "elapsed_time should be positive");

            // Each successful model should have compiled SQL and positive execution time.
            for r in &model_results {
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
                assert!(!r.timing.is_empty(), "model {} should have timing entries", r.unique_id);
            }

            // --- Search attribute assertions ---
            let desc = describe_workflow(&mut client, &run.workflow_id, &run.run_id).await?;
            let info = desc
                .workflow_execution_info
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("should have execution info"))?;
            let sa = info
                .search_attributes
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("should have search attributes"))?;

            let dbt_project = sa
                .indexed_fields
                .get("DbtProject")
                .and_then(payload_json_string)
                .ok_or_else(|| anyhow::anyhow!("DbtProject search attribute should be set"))?;
            assert_eq!(dbt_project, "waffle_hut", "DbtProject search attribute");

            let dbt_command = sa
                .indexed_fields
                .get("DbtCommand")
                .and_then(payload_json_string)
                .ok_or_else(|| anyhow::anyhow!("DbtCommand search attribute should be set"))?;
            assert_eq!(dbt_command, "run", "DbtCommand search attribute");

            // --- Memo assertions ---
            let memo = info
                .memo
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("should have memo"))?;

            // Command memo: verify it matches the input.
            let command_payload = memo
                .fields
                .get("command")
                .ok_or_else(|| anyhow::anyhow!("command memo should be set"))?;
            let command_memo: CommandMemo = serde_json::from_slice(&command_payload.data)
                .context("deserializing command memo")?;
            assert_eq!(command_memo.command, "run");
            assert!(command_memo.fail_fast, "fail_fast should be true");

            // Node status memo: all 5 nodes should be in success state.
            let status_payload = memo
                .fields
                .get("node_status")
                .ok_or_else(|| anyhow::anyhow!("node_status memo should be set"))?;
            let node_status: NodeStatusTree = serde_json::from_slice(&status_payload.data)
                .context("deserializing node_status memo")?;
            assert_eq!(node_status.nodes.len(), 5, "node_status should have 5 entries");
            for (uid, status) in &node_status.nodes {
                assert_eq!(
                    *status,
                    NodeStatus::Success,
                    "node_status memo: {uid} should be success"
                );
            }

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}

/// Test --select and --exclude node filtering.
///
/// Runs multiple workflows against the same worker with different selectors
/// and verifies the correct subset of models is executed each time.
#[tokio::test(flavor = "current_thread")]
async fn test_select_exclude() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");
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

            let client = connect_client(&infra.temporal_addr).await?;

            // --- select a single staging model ---
            tracing::info!("Select test: select=stg_customers");
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("run", Some("stg_customers"), None, false),
            )
            .await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success);
            assert_eq!(output.node_results.len(), 1, "should run exactly 1 model");
            assert!(output.node_results[0].unique_id.contains("stg_customers"));
            assert_eq!(output.node_results[0].status, "success");

            // --- select multiple staging models ---
            tracing::info!("Select test: select='stg_orders stg_payments'");
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("run", Some("stg_orders stg_payments"), None, false),
            )
            .await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success);
            assert_eq!(output.node_results.len(), 2, "should run exactly 2 models");
            let names: Vec<&str> = output
                .node_results
                .iter()
                .map(|r| r.unique_id.as_str())
                .collect();
            assert!(names.iter().any(|n| n.contains("stg_orders")));
            assert!(names.iter().any(|n| n.contains("stg_payments")));

            // --- exclude models matching "customers" ---
            // Fqn substring: "customers" matches both stg_customers and customers.
            // Remaining: stg_orders, stg_payments, orders (3 models, all independent).
            tracing::info!("Select test: exclude=customers");
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("run", None, Some("customers"), false),
            )
            .await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success);
            assert_eq!(output.node_results.len(), 3, "should run 3 models");
            for r in &output.node_results {
                assert_eq!(r.status, "success", "{} should succeed", r.unique_id);
                assert!(
                    !r.unique_id.contains("customers"),
                    "{} should not match 'customers'",
                    r.unique_id
                );
            }

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}
