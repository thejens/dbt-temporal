//! Lifecycle-hook integration tests: drive `execute_hooks` end-to-end.
//!
//! These tests start a second worker (`HookEchoWorkflow` on a separate task
//! queue), then submit a `dbt_run` workflow whose `hooks` config points
//! pre_run / on_success / on_failure at that hook worker. The hook
//! workflow's behaviour is controlled per-test by the `HookConfig.input`
//! payload (which propagates verbatim to the workflow's input).
//!
//! Together this exercises every branch of `execute_hooks`,
//! `process_pre_run_completion`, `record_hook_error`, and
//! `apply_post_run_outcome` against a real Temporal server.

use anyhow::{Context, Result};

use dbt_temporal::types::{HookConfig, HookErrorMode, HooksConfig};

use super::hook_echo::build_hook_worker;
use super::infra::*;

/// Build a HookConfig whose child workflow returns the supplied JSON value
/// (through `HookEchoWorkflow`).
fn echo_hook(
    task_queue: &str,
    on_error: HookErrorMode,
    return_value: serde_json::Value,
) -> HookConfig {
    HookConfig {
        workflow_type: "hook_echo".to_string(),
        task_queue: task_queue.to_string(),
        timeout_secs: Some(30),
        on_error,
        input: Some(return_value),
        fire_and_forget: false,
    }
}

/// pre_run hook returns `false` → workflow skips with no reason. Exercises
/// `process_pre_run_completion::Skip(None)` + `apply_pre_run_outcome::Break`.
#[tokio::test(flavor = "current_thread")]
async fn test_pre_run_skip_sentinel_aborts_run() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");
    let mut config = test_config(infra, &fixture_dir)?;
    let dbt_queue = config.temporal_task_queue.clone();
    let hook_queue = format!("hook-{}", uuid::Uuid::new_v4());

    let mut hooks = HooksConfig::default();
    hooks
        .pre_run
        .push(echo_hook(&hook_queue, HookErrorMode::Fail, serde_json::json!(false)));
    let _ = &mut config; // silence unused-mut warning if no later mutation

    let mut dbt_worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building dbt worker")?;
    let mut hook_worker = build_hook_worker(&infra.temporal_addr, &hook_queue)
        .await
        .context("building hook worker")?;

    let local = tokio::task::LocalSet::new();
    let abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let abort_dbt = std::sync::Arc::clone(&abort);
    let abort_hook = std::sync::Arc::clone(&abort);
    let _dbt_task = local.spawn_local(async move {
        tokio::select! {
            r = dbt_worker.run() => r,
            () = abort_dbt.notified() => Ok(()),
        }
    });
    let _hook_task = local.spawn_local(async move {
        tokio::select! {
            r = hook_worker.run() => r,
            () = abort_hook.notified() => Ok(()),
        }
    });

    let test: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;
            let mut input = make_input("run", None, None, false);
            input.hooks = Some(hooks);

            let run = run_dbt_workflow(&client, &dbt_queue, input).await?;
            assert!(run.output.skipped, "pre_run skip should set output.skipped");
            assert!(run.output.success, "skip is a successful outcome");
            assert!(run.output.node_results.is_empty(), "no nodes should run when pre_run skips");
            Ok(())
        })
        .await;

    abort.notify_waiters();
    test
}
