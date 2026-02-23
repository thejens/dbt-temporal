//! Multi-project tests: loading and targeting multiple dbt projects on one worker.

use anyhow::{Context, Result};

use super::infra::*;

/// Test multi-project workflows: two projects loaded on the same worker,
/// each targeted by the `project` field in the workflow input.
#[tokio::test(flavor = "current_thread")]
async fn test_multi_project() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let waffle_dir = fixture_path("waffle_hut");
    let mini_dir = fixture_path("mini_project");

    let config = test_config_multi_project(
        infra,
        &[waffle_dir.as_path(), mini_dir.as_path()],
        &["waffle_hut", "mini_project"],
    )?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building multi-project worker")?;

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

            // --- Run waffle_hut by name ---
            tracing::info!("Multi-project test: running waffle_hut");
            let mut input = make_input("run", None, None, true);
            input.project = Some("waffle_hut".to_string());
            let run = run_dbt_workflow(&client, &task_queue, input).await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success, "waffle_hut run should succeed");
            let model_count = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .count();
            assert_eq!(model_count, 5, "waffle_hut has 5 models");

            // --- Run mini_project by name ---
            tracing::info!("Multi-project test: running mini_project");
            let mut input = make_input("run", None, None, true);
            input.project = Some("mini_project".to_string());
            let run = run_dbt_workflow(&client, &task_queue, input).await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success, "mini_project run should succeed");
            let model_count = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .count();
            assert_eq!(model_count, 1, "mini_project has 1 model");
            assert!(
                output.node_results[0].unique_id.contains("customer_count"),
                "should run customer_count model"
            );

            // --- Omitting project name with multi-project should fail ---
            tracing::info!("Multi-project test: no project name should fail");
            let input = make_input("run", None, None, true);
            let result = run_dbt_workflow(&client, &task_queue, input).await;
            assert!(
                result.is_err(),
                "workflow without project name should fail when multiple projects are loaded"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}
