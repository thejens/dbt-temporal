//! Robustness tests: target/ independence and read-only project directory.

use anyhow::{Context, Result};
use temporalio_client::{
    UntypedWorkflow, WorkflowGetResultOptions, WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;

use dbt_temporal::types::DbtRunOutput;

use super::infra::*;

/// Test that activities execute correctly without target/ by using workflow reset.
///
/// Proves that activity execution uses in-memory SQL caches (populated at startup)
/// rather than the filesystem target/ directory:
/// 1. Worker loads project (populates target/ and in-memory caches), runs workflow to completion.
/// 2. target/ is wiped (simulating loss of filesystem state).
/// 3. The completed workflow is reset to the first workflow task, forcing all activities to re-execute.
/// 4. The same worker picks up the reset workflow — activities succeed using in-memory caches.
#[tokio::test(flavor = "current_thread")]
async fn test_activities_independent_of_target() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

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

    let fd = fixture_dir.clone();
    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let mut client = connect_client(&infra.temporal_addr).await?;

            // Step 1: Run workflow to completion (all 5 models).
            tracing::info!("Reset test: running initial workflow");
            let workflow_id = format!("test-{}", uuid::Uuid::new_v4());
            let input = make_input("run", None, None, true);
            let handle = client
                .start_workflow(
                    UntypedWorkflow::new("dbt_run"),
                    RawValue::new(vec![input.as_json_payload()?]),
                    WorkflowStartOptions::new(&task_queue, &workflow_id).build(),
                )
                .await
                .context("starting initial workflow")?;
            let run_id = handle.run_id().unwrap_or_default().to_string();

            handle
                .get_result(WorkflowGetResultOptions::default())
                .await
                .context("waiting for initial workflow")?;

            tracing::info!("Reset test: initial workflow completed successfully");

            // Step 2: Wipe target/ to prove activities don't need it at execution time.
            tracing::info!("Reset test: wiping target/ directory");
            std::fs::remove_dir_all(fd.join("target")).ok();

            // Step 3: Find the first WorkflowTaskCompleted event -- resetting here
            // replays all activities from the beginning of the workflow.
            let history = handle
                .fetch_history(temporalio_client::WorkflowFetchHistoryOptions::builder().build())
                .await
                .context("fetching workflow history")?;

            use temporalio_common::protos::temporal::api::enums::v1::EventType;
            let reset_event_id = history
                .events()
                .iter()
                .find(|e| e.event_type() == EventType::WorkflowTaskCompleted)
                .map(|e| e.event_id)
                .context("should have a WorkflowTaskCompleted event")?;
            tracing::info!("Reset test: resetting workflow to event_id={reset_event_id}");

            // Step 4: Reset the workflow to replay all activities.
            use temporalio_client::tonic::IntoRequest;
            use temporalio_common::protos::temporal::api::{
                common::v1::WorkflowExecution as WfExec,
                workflowservice::v1::ResetWorkflowExecutionRequest,
            };

            client
                .reset_workflow_execution(
                    ResetWorkflowExecutionRequest {
                        namespace: "default".to_owned(),
                        workflow_execution: Some(WfExec {
                            workflow_id: workflow_id.clone(),
                            run_id: run_id.clone(),
                        }),
                        workflow_task_finish_event_id: reset_event_id,
                        request_id: format!("reset-{}", uuid::Uuid::new_v4()),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .context("resetting workflow")?;

            // Step 5: Wait for the reset workflow to complete.
            // After reset the run_id changes -- get a handle without a specific run_id.
            let reset_handle = client.get_workflow_handle::<UntypedWorkflow>(&workflow_id);
            let raw_value = reset_handle
                .get_result(WorkflowGetResultOptions::default())
                .await
                .context("waiting for reset workflow result")?;

            let payload = raw_value
                .payloads
                .first()
                .ok_or_else(|| anyhow::anyhow!("should have a payload"))?;
            let output: DbtRunOutput =
                serde_json::from_slice(&payload.data).context("deserializing output")?;

            tracing::info!("Reset test results (after target/ wipe):");
            print_results(&output);

            assert!(output.success, "reset workflow should succeed without target/");
            let model_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            assert_eq!(model_results.len(), 5, "should have 5 model results");
            for r in &model_results {
                assert_eq!(
                    r.status, "success",
                    "model {} should succeed after reset without target/",
                    r.unique_id
                );
            }

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}

/// Test that the project directory stays read-only during parallel workflow execution.
///
/// The resolve step writes to a temp directory (not the project dir), and activities
/// use ephemeral per-activity temp dirs. This test verifies that the project directory
/// itself receives NO writes — not even a `target/` subdirectory — by running two
/// concurrent workflows and checking the directory is untouched.
#[tokio::test(flavor = "current_thread")]
async fn test_project_dir_stays_readonly() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    let config = test_config(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    // After worker init, verify no target/ was created in the project directory.
    assert!(
        !fixture_dir.join("target").exists(),
        "target/ should not exist in the project directory after worker init"
    );

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);

    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            result = worker.run() => result,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let fd = fixture_dir.clone();
    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;

            // Launch two workflows concurrently running all models.
            tracing::info!("Read-only project dir: launching 2 concurrent workflows");
            let (result_a, result_b) = tokio::join!(
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)),
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)),
            );

            let run_a = result_a.context("workflow A failed")?;
            let run_b = result_b.context("workflow B failed")?;

            tracing::info!("Workflow A results:");
            print_results(&run_a.output);
            tracing::info!("Workflow B results:");
            print_results(&run_b.output);

            assert!(run_a.output.success, "workflow A should succeed");
            assert!(run_b.output.success, "workflow B should succeed");

            // Verify the project directory has no target/ — all writes went to /tmp.
            assert!(
                !fd.join("target").exists(),
                "target/ should not exist in the project directory after workflow execution"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}
