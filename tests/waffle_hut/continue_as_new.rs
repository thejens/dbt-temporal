//! Continue-as-new: a run that outgrows its history finishes in a successor.
//!
//! The unit tests cover the handover *contents*; this covers the handover
//! itself — that the successor picks up the spilled state, does not re-plan,
//! does not restart its progress numbering, and produces one complete run
//! across however many executions it took.
//!
//! The trigger is the server's own `continue_as_new_suggested`, so what runs
//! here is the production path with no test-only branch. Reaching it needs a
//! run big enough to matter, which is why this test brings both its own
//! project (waffle_hut plus a chain of filler models) and its own dev server
//! with a lowered threshold — see `SUGGEST_CONTINUE_AFTER_EVENTS`.

use anyhow::{Context, Result};

use dbt_temporal::types::NodeStatus;

use super::infra::*;

#[tokio::test(flavor = "current_thread")]
async fn test_run_continues_as_new_and_still_completes() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture_with_continuation_filler()?;
    let expected_models = 5 + CONTINUATION_FILLER_MODELS;

    let mut config = test_config(infra, &fixture_dir)?;
    // Its own Temporal: the suite's shared server keeps the stock 4096 default
    // so no other test hands over by accident. Postgres is still shared.
    config.temporal_address = format!("http://{}", continue_as_new_infra().temporal_addr);
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

            let mut client = connect_client(&continue_as_new_infra().temporal_addr).await?;

            // `get_result` follows the continuation chain, so this is the
            // outcome of the whole logical run, not of its first execution.
            let run =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            let output = &run.output;

            // The run is expected to have handed off rather than finished in
            // one execution. If it did not, everything below proves nothing.
            let segments = count_run_segments(&mut client, &run.workflow_id, &run.run_id).await?;
            let first = describe_workflow(&mut client, &run.workflow_id, &run.run_id).await?;
            let history_length = first
                .workflow_execution_info
                .as_ref()
                .map_or(0, |i| i.history_length);
            tracing::info!(
                segments,
                first_segment_events = history_length,
                models = expected_models,
                "continue-as-new run finished"
            );
            assert!(
                segments > 1,
                "the run should have handed off at least once but finished in {segments} \
                 execution(s); its first produced {history_length} events against a \
                 threshold of {SUGGEST_CONTINUE_AFTER_EVENTS}. Above the threshold means \
                 the server never applied the dynamic config; below means \
                 CONTINUATION_FILLER_MODELS no longer generates enough history"
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
                expected_models,
                "results from before each handover must survive it"
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

            // Progress numbering is continuous across every handover: each node
            // counted once, out of the full total, in a single log.
            let log_path = output
                .log_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("log_path should be set"))?;
            let log = std::fs::read_to_string(log_path).context("reading run log")?;
            for n in 1..=expected_models {
                assert!(
                    log.contains(&format!("{n} of {expected_models}")),
                    "'{n} of {expected_models}' missing from the run log — a successor \
                     restarted the counter or lost the log"
                );
            }

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    test_result
}

/// The filler models are generated, so a broken template would show up as a
/// confusing dbt parse failure inside a slow Docker-backed test. Check the
/// rendering here instead — no infrastructure needed.
#[test]
fn filler_models_form_a_chain_of_views() {
    let dir = copy_fixture_with_continuation_filler().expect("generating fixture");
    let filler = dir.join("models").join("filler");

    let first = std::fs::read_to_string(filler.join("filler_000.sql")).unwrap();
    assert_eq!(first, "{{ config(materialized='view') }}\nselect 1 as id\n");

    let second = std::fs::read_to_string(filler.join("filler_001.sql")).unwrap();
    assert_eq!(
        second,
        "{{ config(materialized='view') }}\nselect id from {{ ref('filler_000') }}\n"
    );

    // Every model but the first refs its predecessor, which is what makes the
    // chain one node per level — the shape the event budget was sized against.
    let last = CONTINUATION_FILLER_MODELS - 1;
    let last_sql = std::fs::read_to_string(filler.join(format!("filler_{last:03}.sql"))).unwrap();
    assert!(
        last_sql.contains(&format!("ref('filler_{:03}')", last - 1)),
        "last filler should ref its predecessor: {last_sql}"
    );
    assert_eq!(
        std::fs::read_dir(&filler).unwrap().count(),
        CONTINUATION_FILLER_MODELS,
        "one file per filler model"
    );

    std::fs::remove_dir_all(&dir).ok();
}
