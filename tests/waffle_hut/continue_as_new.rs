//! Continue-as-new: a run that outgrows its history finishes in a successor.
//!
//! The unit tests cover the handover *contents*; this covers the handover
//! itself — that the successor picks up the spilled state, does not re-plan,
//! does not restart its progress numbering, and produces one complete run
//! across however many executions it took.
//!
//! The trigger is the server's own `continue_as_new_suggested`, lowered for
//! the whole suite via `SUGGEST_CONTINUE_AFTER_EVENTS` — there is no test-only
//! branch in the workflow, so what runs here is the production path.

use anyhow::{Context, Result};

use dbt_temporal::types::NodeStatus;
use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;

use super::infra::*;

#[tokio::test(flavor = "current_thread")]
async fn test_run_continues_as_new_and_still_completes() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");

    let mut config = test_config(infra, &fixture_dir)?;
    // Continuation needs somewhere to spill state; without a store the run
    // pushes on and accepts the history growth instead.
    config.write_artifacts = true;
    config.write_run_log = true;
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

            // `get_result` follows the continuation chain, so this is the
            // outcome of the whole logical run, not of its first execution.
            let run =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            let output = &run.output;
            print_results(output);

            // The execution that started is expected to have handed off rather
            // than finished. If it completed outright, the threshold no longer
            // falls inside this fixture and everything below proves nothing.
            let first = describe_workflow(&mut client, &run.workflow_id, &run.run_id).await?;
            let status = first
                .workflow_execution_info
                .as_ref()
                .map_or(0, |i| i.status);
            assert_eq!(
                status,
                WorkflowExecutionStatus::ContinuedAsNew as i32,
                "the first execution should have continued as new (status {status}); \
                 if this fixture no longer outgrows SUGGEST_CONTINUE_AFTER_EVENTS, \
                 raise the fixture or lower the threshold rather than deleting this"
            );

            // Everything below is the point: a continued run is still one run.
            assert!(output.success, "a continued run must still succeed");

            let model_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            assert_eq!(
                model_results.len(),
                5,
                "results from before the handover must survive it, got {:?}",
                output
                    .node_results
                    .iter()
                    .map(|r| &r.unique_id)
                    .collect::<Vec<_>>()
            );
            for r in &model_results {
                assert_eq!(r.status, NodeStatus::Success, "{} should pass", r.unique_id);
            }

            // One invocation id across the whole chain: the successor inherits
            // the plan rather than planning again, and artifacts land in one
            // place instead of one directory per segment.
            let artifacts = output
                .artifacts
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("artifacts should be present"))?;
            assert!(
                artifacts.run_results_path.contains(&output.invocation_id),
                "artifacts should be keyed by the run's original invocation id: {} vs {}",
                artifacts.run_results_path,
                output.invocation_id
            );

            // Progress numbering is continuous across the handover: every node
            // is counted once, out of the full total, in one log.
            let log_path = output
                .log_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("log_path should be set"))?;
            let log = std::fs::read_to_string(log_path).context("reading run log")?;
            for n in 1..=5 {
                assert!(
                    log.contains(&format!("{n} of 5")),
                    "'{n} of 5' missing from the run log — the successor \
                     restarted the counter or lost the log:\n{log}"
                );
            }

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}
