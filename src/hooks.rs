use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use temporalio_common::UntypedWorkflow;
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;

use temporalio_sdk::{ChildWorkflowCancellationType, ChildWorkflowOptions, ParentClosePolicy};
use tracing::{info, warn};

use crate::project_registry::ProjectRegistry;
use crate::types::{
    HookConfig, HookError, HookErrorMode, HookEvent, HookExecutionOutcome, HookPayload,
    HooksConfig, ResolveConfigInput, ResolvedProjectConfig, RetryConfig, TimeoutConfig,
};

use crate::workflow::DbtRunWorkflow;

const DEFAULT_HOOK_TIMEOUT_SECS: u64 = 300;

/// Load project config (hooks + retry + timeouts) from `dbt_temporal.yml` in
/// the given directory. Returns defaults if the file doesn't exist.
pub fn load_project_config(
    project_dir: &Path,
) -> anyhow::Result<(HooksConfig, RetryConfig, TimeoutConfig)> {
    #[derive(serde::Deserialize)]
    struct FileConfig {
        #[serde(default)]
        hooks: HooksConfig,
        #[serde(default)]
        retry: RetryConfig,
        #[serde(default)]
        timeouts: TimeoutConfig,
    }

    let path = project_dir.join("dbt_temporal.yml");
    if !path.exists() {
        return Ok((HooksConfig::default(), RetryConfig::default(), TimeoutConfig::default()));
    }

    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;

    let file_config: FileConfig =
        dbt_yaml::from_str(&contents).with_context(|| format!("parsing {}", path.display()))?;

    // Fail worker startup rather than let an unusable timeout surface mid-run.
    file_config
        .timeouts
        .validate()
        .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;

    Ok((file_config.hooks, file_config.retry, file_config.timeouts))
}

/// Activity inner logic for resolve_config — no Temporal context dependency.
///
/// Called from `DbtActivities::resolve_config`.
pub fn resolve_config_impl(
    registry: &ProjectRegistry,
    input: ResolveConfigInput,
) -> anyhow::Result<ResolvedProjectConfig> {
    let state = registry.get(input.project.as_deref())?;
    Ok(merge_resolved_config(
        input.input_hooks,
        &state.default_hooks,
        &state.default_retry,
        &state.default_timeouts,
    ))
}

/// Pure config-merge step extracted from `resolve_config_impl` so it can be
/// unit-tested without standing up a full `WorkerState`.
///
/// The caller's `input_hooks` is a full override (when present); retry and
/// timeout config always come from the worker-state defaults.
fn merge_resolved_config(
    input_hooks: Option<HooksConfig>,
    default_hooks: &HooksConfig,
    default_retry: &RetryConfig,
    default_timeouts: &TimeoutConfig,
) -> ResolvedProjectConfig {
    ResolvedProjectConfig {
        hooks: input_hooks.unwrap_or_else(|| default_hooks.clone()),
        retry: default_retry.clone(),
        timeouts: default_timeouts.clone(),
    }
}

/// What a successfully-completed pre_run hook tells the runner to do next.
#[derive(Debug, PartialEq, Eq)]
enum HookCompletionAction {
    /// Continue running remaining hooks.
    Continue,
    /// Stop here — pre_run signalled skip with the given (optional) reason.
    Skip(Option<String>),
}

/// Build the deterministic child-workflow ID used for a single hook invocation.
///
/// Embeds the parent workflow ID, the lifecycle event, the hook's index in the
/// configured list, and the hook workflow type — sufficient to make per-run
/// hook IDs unique across replays without baking timestamps into history.
fn build_hook_workflow_id(
    parent_id: &str,
    event: HookEvent,
    index: usize,
    workflow_type: &str,
) -> String {
    format!("{parent_id}-hook-{event}-{index}-{workflow_type}")
}

/// Build the per-hook child-workflow options. Fire-and-forget hooks must use
/// `Abandon` so the child survives the parent; awaited hooks use the default.
fn build_hook_options(
    hook: &HookConfig,
    workflow_id: String,
    event: HookEvent,
    index: usize,
) -> ChildWorkflowOptions {
    let timeout = hook.timeout_secs.unwrap_or(DEFAULT_HOOK_TIMEOUT_SECS);
    let parent_close_policy = if hook.fire_and_forget {
        ParentClosePolicy::Abandon
    } else {
        ParentClosePolicy::Unspecified
    };
    // Human-readable card for the child workflow in the Temporal UI — without
    // it, hook executions show up as bare workflow IDs.
    let fnf = if hook.fire_and_forget {
        ", fire-and-forget"
    } else {
        ""
    };
    let static_summary = format!(
        "{event} hook #{index}: {} (queue: {}{fnf})",
        hook.workflow_type, hook.task_queue
    );
    ChildWorkflowOptions::builder()
        .workflow_id(workflow_id)
        .task_queue(hook.task_queue.clone())
        .parent_close_policy(parent_close_policy)
        // A cancelled run must not block on its hooks acknowledging the
        // cancellation: a hook that ignores cancellation would hold the run
        // open until its own execution timeout. Set explicitly because the
        // SDK default waits for the child to finish cancelling.
        .cancel_type(ChildWorkflowCancellationType::Abandon)
        .execution_timeout(Duration::from_secs(timeout))
        .static_summary(static_summary)
        .build()
}

/// Process the completion payload of a pre_run hook: merge any `extra_env`
/// into `outcome` and detect a skip sentinel.
///
/// Always returns `Continue` for non-pre_run events — the only sentinels we
/// listen for are on the pre_run path; on_success / on_failure cannot affect
/// the run that already finished.
fn process_pre_run_completion(
    event: HookEvent,
    payload_bytes: Option<&[u8]>,
    outcome: &mut HookExecutionOutcome,
) -> HookCompletionAction {
    if event != HookEvent::PreRun {
        return HookCompletionAction::Continue;
    }
    let Some(value) = payload_bytes.and_then(|b| serde_json::from_slice(b).ok()) else {
        return HookCompletionAction::Continue;
    };
    if let Some(extra) = parse_extra_env(&value) {
        outcome.extra_env.extend(extra);
    }
    if let Some((true, reason)) = parse_skip_sentinel(&value) {
        outcome.skip = true;
        outcome.skip_reason.clone_from(&reason);
        return HookCompletionAction::Skip(reason);
    }
    HookCompletionAction::Continue
}

/// Build the JSON-encoded input payload that the hook child workflow
/// receives. When `hook.input` is set the user's override wins; otherwise the
/// canonical `HookPayload` (event + invocation id + plan + output) is used.
fn build_hook_input_payload(
    hook: &HookConfig,
    payload: &HookPayload,
) -> anyhow::Result<Vec<temporalio_common::protos::temporal::api::common::v1::Payload>> {
    if let Some(ref custom_input) = hook.input {
        Ok(vec![custom_input.as_json_payload()?])
    } else {
        Ok(vec![payload.as_json_payload()?])
    }
}

/// Apply the hook's `on_error` policy to a failure. Returns `Err` for `Fail`
/// mode (the runner aborts), pushes to `outcome.errors` for `Warn`, drops the
/// error silently for `Ignore`.
fn record_hook_error(
    hook: &HookConfig,
    payload: &HookPayload,
    error_msg: String,
    outcome: &mut HookExecutionOutcome,
) -> Result<(), anyhow::Error> {
    match hook.on_error {
        HookErrorMode::Fail => Err(anyhow::anyhow!(
            "hook '{}' ({}) failed: {}",
            hook.workflow_type,
            payload.event,
            error_msg
        )),
        HookErrorMode::Warn => {
            outcome.errors.push(HookError {
                hook_workflow_type: hook.workflow_type.clone(),
                event: payload.event.to_string(),
                error: error_msg,
            });
            Ok(())
        }
        HookErrorMode::Ignore => Ok(()),
    }
}

/// Execute a list of hooks sequentially as child workflows.
///
/// Returns a `HookExecutionOutcome` with collected non-fatal errors and skip state.
/// Returns `Err` if any hook with `on_error: fail` fails.
///
/// For pre_run hooks, a child workflow can signal "skip this run" by returning:
/// - JSON `false`
/// - JSON object with `"skip": true` (optionally with `"reason": "..."`)
///
/// When a skip sentinel is detected, remaining hooks are not executed.
#[allow(clippy::future_not_send)] // WorkflowContext uses Rc internally.
pub async fn execute_hooks(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    hooks: &[HookConfig],
    payload: &HookPayload,
) -> Result<HookExecutionOutcome, anyhow::Error> {
    let mut outcome = HookExecutionOutcome::default();
    let parent_workflow_id = ctx.workflow_id();

    for (i, hook) in hooks.iter().enumerate() {
        let workflow_id =
            build_hook_workflow_id(parent_workflow_id, payload.event, i, &hook.workflow_type);

        info!(
            workflow_type = %hook.workflow_type,
            task_queue = %hook.task_queue,
            event = %payload.event,
            "starting hook child workflow"
        );

        let input_payload = build_hook_input_payload(hook, payload)?;

        let opts = build_hook_options(hook, workflow_id, payload.event, i);
        let wf = UntypedWorkflow::new(&hook.workflow_type);
        let raw_input = RawValue::new(input_payload);

        if hook.fire_and_forget {
            // Start but don't await result — the child runs independently.
            match ctx.start_child_workflow(wf, raw_input, opts).await {
                Ok(_started) => {
                    info!(
                        workflow_type = %hook.workflow_type,
                        event = %payload.event,
                        "fire-and-forget hook started"
                    );
                }
                Err(e) => {
                    let error_msg =
                        format!("fire-and-forget child workflow failed to start: {e:#}");
                    if matches!(hook.on_error, HookErrorMode::Warn) {
                        warn!(
                            workflow_type = %hook.workflow_type,
                            event = %payload.event,
                            error = %error_msg,
                            "fire-and-forget hook failed to start (continuing)"
                        );
                    }
                    record_hook_error(hook, payload, error_msg, &mut outcome)?;
                }
            }
            continue;
        }

        let result = run_child_workflow(ctx, wf, raw_input, opts).await;

        match result {
            Ok(completion_payload) => {
                info!(
                    workflow_type = %hook.workflow_type,
                    event = %payload.event,
                    "hook completed successfully"
                );

                let action = process_pre_run_completion(
                    payload.event,
                    completion_payload.as_deref(),
                    &mut outcome,
                );
                if let HookCompletionAction::Skip(reason) = action {
                    info!(
                        workflow_type = %hook.workflow_type,
                        reason = ?reason,
                        "pre_run hook signalled skip"
                    );
                    break;
                }
            }
            Err(e) => {
                let error_msg = format!("{e:#}");
                if matches!(hook.on_error, HookErrorMode::Warn) {
                    warn!(
                        workflow_type = %hook.workflow_type,
                        event = %payload.event,
                        error = %error_msg,
                        "hook failed (continuing)"
                    );
                }
                record_hook_error(hook, payload, error_msg, &mut outcome)?;
            }
        }
    }

    Ok(outcome)
}

/// Parse a hook completion payload for extra env vars.
///
/// Returns `Some(map)` if the payload contains `{"extra_env": {"KEY": "value", ...}}`.
/// Values must be strings; non-string values are silently skipped.
fn parse_extra_env(
    value: &serde_json::Value,
) -> Option<std::collections::BTreeMap<String, String>> {
    let extra = value.as_object()?.get("extra_env")?.as_object()?;
    let map: std::collections::BTreeMap<String, String> = extra
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();
    if map.is_empty() { None } else { Some(map) }
}

/// Parse a hook completion payload for skip sentinel values.
///
/// Returns `Some((true, reason))` if the payload signals skip, `None` if unparseable.
/// Skip sentinels:
/// - JSON `false` → skip with no reason
/// - `{"skip": true}` → skip, optionally with `"reason"` field
fn parse_skip_sentinel(value: &serde_json::Value) -> Option<(bool, Option<String>)> {
    match value {
        serde_json::Value::Bool(false) => Some((true, None)),
        serde_json::Value::Object(obj) => {
            let skip = obj.get("skip")?.as_bool()?;
            if skip {
                let reason = obj.get("reason").and_then(|v| v.as_str()).map(String::from);
                Some((true, reason))
            } else {
                Some((false, None))
            }
        }
        _ => None,
    }
}

/// Start a child workflow and await its completion.
/// Returns the completion payload bytes (if any) on success.
#[allow(clippy::future_not_send)] // WorkflowContext uses Rc internally.
async fn run_child_workflow(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    wf: UntypedWorkflow,
    input: RawValue,
    opts: ChildWorkflowOptions,
) -> anyhow::Result<Option<Vec<u8>>> {
    let started = ctx
        .start_child_workflow(wf, input, opts)
        .await
        .context("child workflow failed to start")?;

    let result = started.result().await.context("child workflow failed")?;

    let bytes = result.payloads.into_iter().next().map(|p| p.data);
    Ok(bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
