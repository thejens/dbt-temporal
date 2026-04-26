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
use temporalio_client::{
    UntypedWorkflow, WorkflowGetResultOptions, WorkflowStartOptions, errors::WorkflowGetResultError,
};
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;

use dbt_temporal::types::{DbtRunInput, DbtRunOutput, HookConfig, HookErrorMode, HooksConfig};

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

/// Outcome of running a dbt workflow under a paired hook worker.
#[allow(clippy::large_enum_variant)] // test-only; size of DbtRunOutput is fine here.
enum RunOutcome {
    Ok(DbtRunOutput),
    Failed,
}

/// Spawn a dbt worker + a hook worker, submit `input` against the dbt queue,
/// and return either the workflow output or its terminal error message.
///
/// Both workers are aborted on exit so each test cleans up after itself.
#[allow(clippy::future_not_send)] // LocalSet + Worker use Rc internally.
async fn run_with_hook_worker(input: DbtRunInput, hook_queue: &str) -> Result<RunOutcome> {
    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");
    let config = test_config(infra, &fixture_dir)?;
    let dbt_queue = config.temporal_task_queue.clone();

    let mut dbt_worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building dbt worker")?;
    let mut hook_worker = build_hook_worker(&infra.temporal_addr, hook_queue)
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

    let outcome: Result<RunOutcome> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;
            let workflow_id = format!("test-{}", uuid::Uuid::new_v4());
            let handle = client
                .start_workflow(
                    UntypedWorkflow::new("dbt_run"),
                    RawValue::new(vec![input.as_json_payload()?]),
                    WorkflowStartOptions::new(&dbt_queue, &workflow_id).build(),
                )
                .await
                .context("starting workflow")?;

            match handle.get_result(WorkflowGetResultOptions::default()).await {
                Ok(raw) => {
                    let payload = raw
                        .payloads
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("workflow should return a payload"))?;
                    let output: DbtRunOutput = serde_json::from_slice(&payload.data)
                        .context("deserializing DbtRunOutput")?;
                    Ok(RunOutcome::Ok(output))
                }
                Err(WorkflowGetResultError::Failed(_)) => Ok(RunOutcome::Failed),
                Err(other) => Err(anyhow::anyhow!("unexpected workflow error: {other:?}")),
            }
        })
        .await;

    abort.notify_waiters();
    outcome
}

fn hook_queue() -> String {
    format!("hook-{}", uuid::Uuid::new_v4())
}

/// pre_run hook returns `false` → workflow skips with no reason. Exercises
/// `process_pre_run_completion::Skip(None)` + `apply_pre_run_outcome::Break`.
#[tokio::test(flavor = "current_thread")]
async fn test_pre_run_skip_sentinel_aborts_run() -> Result<()> {
    init_tracing();
    let queue = hook_queue();

    let mut hooks = HooksConfig::default();
    hooks
        .pre_run
        .push(echo_hook(&queue, HookErrorMode::Fail, serde_json::json!(false)));

    let mut input = make_input("run", Some("+customers"), None, false);
    input.hooks = Some(hooks);

    let RunOutcome::Ok(output) = run_with_hook_worker(input, &queue).await? else {
        anyhow::bail!("expected workflow to succeed (skip is a success)");
    };

    assert!(output.skipped, "pre_run skip should set output.skipped");
    assert!(output.success, "skip is a successful outcome");
    assert!(output.node_results.is_empty(), "no nodes should run when pre_run skips");
    assert!(output.skip_reason.is_none(), "json-false sentinel has no reason");
    Ok(())
}

/// pre_run returns `{"skip": true, "reason": "..."}` → skip propagates the reason.
#[tokio::test(flavor = "current_thread")]
async fn test_pre_run_skip_with_reason() -> Result<()> {
    init_tracing();
    let queue = hook_queue();

    let mut hooks = HooksConfig::default();
    hooks.pre_run.push(echo_hook(
        &queue,
        HookErrorMode::Fail,
        serde_json::json!({"skip": true, "reason": "no new data"}),
    ));

    let mut input = make_input("run", Some("+customers"), None, false);
    input.hooks = Some(hooks);

    let RunOutcome::Ok(output) = run_with_hook_worker(input, &queue).await? else {
        anyhow::bail!("expected workflow to succeed");
    };

    assert!(output.skipped);
    assert!(output.success);
    assert_eq!(output.skip_reason.as_deref(), Some("no new data"));
    Ok(())
}

/// on_success hook with `on_error: warn` that fails → error collected in
/// `hook_errors`, run still reported successful. Exercises the Warn branch
/// of `record_hook_error` inside `execute_hooks`.
#[tokio::test(flavor = "current_thread")]
async fn test_on_success_warn_collects_into_hook_errors() -> Result<()> {
    init_tracing();
    let queue = hook_queue();

    let mut hooks = HooksConfig::default();
    hooks.on_success.push(echo_hook(
        &queue,
        HookErrorMode::Warn,
        serde_json::json!({"error": "boom"}),
    ));

    let mut input = make_input("run", Some("+customers"), None, false);
    input.hooks = Some(hooks);

    let RunOutcome::Ok(output) = run_with_hook_worker(input, &queue).await? else {
        anyhow::bail!("expected workflow to succeed despite warn-mode hook failure");
    };

    assert!(output.success, "warn-mode hook failure must not flip success");
    assert!(!output.skipped);
    assert!(!output.node_results.is_empty(), "model should still run");
    assert_eq!(output.hook_errors.len(), 1, "hook error should be collected");
    let err = &output.hook_errors[0];
    assert_eq!(err.event, "on_success");
    assert_eq!(err.hook_workflow_type, "hook_echo");
    // Temporal child-workflow failures surface a generic "Child Workflow execution
    // failed" wrapper; the hook's own message is not promoted into the parent's
    // cause chain. We only assert that *something* was recorded.
    assert!(!err.error.is_empty(), "hook error message should be non-empty");
    Ok(())
}

/// on_success hook with `on_error: fail` that fails → workflow itself fails.
#[tokio::test(flavor = "current_thread")]
async fn test_on_success_fail_aborts_workflow() -> Result<()> {
    init_tracing();
    let queue = hook_queue();

    let mut hooks = HooksConfig::default();
    hooks.on_success.push(echo_hook(
        &queue,
        HookErrorMode::Fail,
        serde_json::json!({"error": "fatal"}),
    ));

    let mut input = make_input("run", Some("+customers"), None, false);
    input.hooks = Some(hooks);

    match run_with_hook_worker(input, &queue).await? {
        RunOutcome::Failed => {}
        RunOutcome::Ok(_) => {
            anyhow::bail!("expected workflow to fail when on_error=fail hook errors")
        }
    }
    // The workflow's terminal failure message is generic ("dbt run failed: 0
    // node(s) errored") — the hook's anyhow error is consumed by
    // `apply_post_run_outcome` and only flips `success` to false. Asserting the
    // outcome variant is enough.
    Ok(())
}

/// on_success hook with `on_error: ignore` that fails → silent success, no
/// entries in `hook_errors`.
#[tokio::test(flavor = "current_thread")]
async fn test_on_success_ignore_silent() -> Result<()> {
    init_tracing();
    let queue = hook_queue();

    let mut hooks = HooksConfig::default();
    hooks.on_success.push(echo_hook(
        &queue,
        HookErrorMode::Ignore,
        serde_json::json!({"error": "whatever"}),
    ));

    let mut input = make_input("run", Some("+customers"), None, false);
    input.hooks = Some(hooks);

    let RunOutcome::Ok(output) = run_with_hook_worker(input, &queue).await? else {
        anyhow::bail!("expected workflow to succeed");
    };

    assert!(output.success);
    assert!(output.hook_errors.is_empty(), "ignore mode must not collect errors");
    Ok(())
}
