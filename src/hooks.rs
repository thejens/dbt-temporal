use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_common::protos::coresdk::child_workflow::child_workflow_result;
use temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy;
use temporalio_sdk::ChildWorkflowOptions;
use tracing::{info, warn};

use crate::project_registry::ProjectRegistry;
use crate::types::{
    HookConfig, HookError, HookErrorMode, HookExecutionOutcome, HookPayload, HooksConfig,
    ResolveConfigInput, ResolvedProjectConfig, RetryConfig,
};

use crate::workflow::DbtRunWorkflow;

const DEFAULT_HOOK_TIMEOUT_SECS: u64 = 300;

/// Load project config (hooks + retry) from `dbt_temporal.yml` in the given directory.
/// Returns defaults if the file doesn't exist.
pub fn load_project_config(project_dir: &Path) -> anyhow::Result<(HooksConfig, RetryConfig)> {
    #[derive(serde::Deserialize)]
    struct FileConfig {
        #[serde(default)]
        hooks: HooksConfig,
        #[serde(default)]
        retry: RetryConfig,
    }

    let path = project_dir.join("dbt_temporal.yml");
    if !path.exists() {
        return Ok((HooksConfig::default(), RetryConfig::default()));
    }

    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;

    let file_config: FileConfig =
        dbt_yaml::from_str(&contents).with_context(|| format!("parsing {}", path.display()))?;

    Ok((file_config.hooks, file_config.retry))
}

/// Activity inner logic for resolve_config — no Temporal context dependency.
///
/// Called from `DbtActivities::resolve_config`.
pub fn resolve_config_impl(
    registry: &ProjectRegistry,
    input: ResolveConfigInput,
) -> anyhow::Result<ResolvedProjectConfig> {
    let state = registry.get(input.project.as_deref())?;

    // If the caller provided explicit hooks, use those (full override).
    // Retry config always comes from file defaults.
    let hooks = input
        .input_hooks
        .unwrap_or_else(|| state.default_hooks.clone());

    Ok(ResolvedProjectConfig {
        hooks,
        retry: state.default_retry.clone(),
    })
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
#[allow(clippy::too_many_lines, clippy::future_not_send)] // WorkflowContext uses Rc internally.
pub async fn execute_hooks(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    hooks: &[HookConfig],
    payload: &HookPayload,
) -> Result<HookExecutionOutcome, anyhow::Error> {
    let mut outcome = HookExecutionOutcome::default();
    let parent_workflow_id = ctx.workflow_initial_info().workflow_id.clone();

    for (i, hook) in hooks.iter().enumerate() {
        let timeout = hook.timeout_secs.unwrap_or(DEFAULT_HOOK_TIMEOUT_SECS);
        let workflow_id =
            format!("{}-hook-{}-{}-{}", parent_workflow_id, payload.event, i, hook.workflow_type);

        info!(
            workflow_type = %hook.workflow_type,
            task_queue = %hook.task_queue,
            event = %payload.event,
            "starting hook child workflow"
        );

        let input_payload = if let Some(ref custom_input) = hook.input {
            vec![custom_input.as_json_payload()?]
        } else {
            vec![payload.as_json_payload()?]
        };

        let parent_close_policy = if hook.fire_and_forget {
            ParentClosePolicy::Abandon
        } else {
            ParentClosePolicy::Unspecified
        };

        let opts = ChildWorkflowOptions {
            workflow_id,
            workflow_type: hook.workflow_type.clone(),
            task_queue: Some(hook.task_queue.clone()),
            input: input_payload,
            parent_close_policy,
            execution_timeout: Some(Duration::from_secs(timeout)),
            ..Default::default()
        };

        if hook.fire_and_forget {
            // Start but don't await — the child runs independently.
            let pending = ctx.child_workflow(opts).start().await;
            if pending.into_started().is_none() {
                let error_msg = "fire-and-forget child workflow failed to start".to_string();
                match hook.on_error {
                    HookErrorMode::Fail => {
                        return Err(anyhow::anyhow!(
                            "hook '{}' ({}) failed: {}",
                            hook.workflow_type,
                            payload.event,
                            error_msg
                        ));
                    }
                    HookErrorMode::Warn => {
                        warn!(
                            workflow_type = %hook.workflow_type,
                            event = %payload.event,
                            error = %error_msg,
                            "fire-and-forget hook failed to start (continuing)"
                        );
                        outcome.errors.push(HookError {
                            hook_workflow_type: hook.workflow_type.clone(),
                            event: payload.event.clone(),
                            error: error_msg,
                        });
                    }
                    HookErrorMode::Ignore => {}
                }
            } else {
                info!(
                    workflow_type = %hook.workflow_type,
                    event = %payload.event,
                    "fire-and-forget hook started"
                );
            }
            continue;
        }

        let result = run_child_workflow(ctx, opts).await;

        match result {
            Ok(completion_payload) => {
                info!(
                    workflow_type = %hook.workflow_type,
                    event = %payload.event,
                    "hook completed successfully"
                );

                // For pre_run hooks, check for skip sentinel in the result.
                if payload.event == "pre_run"
                    && let Some(ref bytes) = completion_payload
                    && let Some((skip, reason)) = parse_skip_sentinel(bytes)
                    && skip
                {
                    info!(
                        workflow_type = %hook.workflow_type,
                        reason = ?reason,
                        "pre_run hook signalled skip"
                    );
                    outcome.skip = true;
                    outcome.skip_reason = reason;
                    break;
                }
            }
            Err(e) => {
                let error_msg = format!("{e:#}");
                match hook.on_error {
                    HookErrorMode::Fail => {
                        return Err(anyhow::anyhow!(
                            "hook '{}' ({}) failed: {}",
                            hook.workflow_type,
                            payload.event,
                            error_msg
                        ));
                    }
                    HookErrorMode::Warn => {
                        warn!(
                            workflow_type = %hook.workflow_type,
                            event = %payload.event,
                            error = %error_msg,
                            "hook failed (continuing)"
                        );
                        outcome.errors.push(HookError {
                            hook_workflow_type: hook.workflow_type.clone(),
                            event: payload.event.clone(),
                            error: error_msg,
                        });
                    }
                    HookErrorMode::Ignore => {}
                }
            }
        }
    }

    Ok(outcome)
}

/// Parse a hook completion payload for skip sentinel values.
///
/// Returns `Some((true, reason))` if the payload signals skip, `None` if unparseable.
/// Skip sentinels:
/// - JSON `false` → skip with no reason
/// - `{"skip": true}` → skip, optionally with `"reason"` field
fn parse_skip_sentinel(bytes: &[u8]) -> Option<(bool, Option<String>)> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    match &value {
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
    opts: ChildWorkflowOptions,
) -> anyhow::Result<Option<Vec<u8>>> {
    let pending = ctx.child_workflow(opts).start().await;

    let started = pending
        .into_started()
        .ok_or_else(|| anyhow::anyhow!("child workflow failed to start"))?;

    let result = started.result().await;

    match result.status {
        Some(child_workflow_result::Status::Completed(success)) => {
            let bytes = success.result.map(|p| p.data);
            Ok(bytes)
        }
        Some(child_workflow_result::Status::Failed(f)) => {
            let msg = f
                .failure
                .map_or_else(|| "unknown failure".to_string(), |f| f.message);
            Err(anyhow::anyhow!("child workflow failed: {msg}"))
        }
        Some(child_workflow_result::Status::Cancelled(c)) => {
            let msg = c
                .failure
                .map_or_else(|| "cancelled".to_string(), |f| f.message);
            Err(anyhow::anyhow!("child workflow cancelled: {msg}"))
        }
        None => Err(anyhow::anyhow!("child workflow returned no status")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_project_config_missing_file_returns_defaults() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;

        let (hooks, retry) = load_project_config(&dir)?;
        assert!(hooks.pre_run.is_empty());
        assert!(hooks.on_success.is_empty());
        assert!(hooks.on_failure.is_empty());
        assert_eq!(retry.max_attempts, 3);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn load_project_config_parses_hooks_and_retry() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("dbt_temporal.yml"),
            r"
hooks:
  pre_run:
    - workflow_type: notify
      task_queue: hooks-queue
      timeout_secs: 60
      on_error: fail
  on_success:
    - workflow_type: publish
      task_queue: hooks-queue

retry:
  max_attempts: 5
  initial_interval_secs: 10
  non_retryable_errors:
    - 'permission denied'
    - 'relation .* does not exist'
",
        )?;

        let (hooks, retry) = load_project_config(&dir)?;
        assert_eq!(hooks.pre_run.len(), 1);
        assert_eq!(hooks.pre_run[0].workflow_type, "notify");
        assert_eq!(hooks.pre_run[0].task_queue, "hooks-queue");
        assert_eq!(hooks.pre_run[0].timeout_secs, Some(60));
        assert_eq!(hooks.pre_run[0].on_error, HookErrorMode::Fail);
        assert_eq!(hooks.on_success.len(), 1);
        assert_eq!(hooks.on_success[0].workflow_type, "publish");
        assert_eq!(hooks.on_success[0].on_error, HookErrorMode::Warn);
        assert!(hooks.on_failure.is_empty());

        assert_eq!(retry.max_attempts, 5);
        assert_eq!(retry.initial_interval_secs, 10);
        assert!((retry.backoff_coefficient - 2.0).abs() < f64::EPSILON); // default
        assert_eq!(retry.max_interval_secs, 60); // default
        assert_eq!(
            retry.non_retryable_errors,
            vec!["permission denied", "relation .* does not exist"]
        );

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn load_project_config_hooks_only_gets_default_retry() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("dbt_temporal.yml"),
            r"
hooks:
  on_failure:
    - workflow_type: alert
      task_queue: alerts
",
        )?;

        let (hooks, retry) = load_project_config(&dir)?;
        assert_eq!(hooks.on_failure.len(), 1);
        assert_eq!(retry.max_attempts, 3);
        assert!(retry.non_retryable_errors.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn load_project_config_empty_file() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("dbt_temporal.yml"), "")?;

        let result = load_project_config(&dir);
        if let Ok((hooks, retry)) = result {
            assert!(hooks.pre_run.is_empty());
            assert_eq!(retry.max_attempts, 3);
        } // Parsing empty file as error is acceptable.

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn load_project_config_with_custom_input() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("dbt_temporal.yml"),
            r"
hooks:
  pre_run:
    - workflow_type: dbt_run
      task_queue: dbt-tasks
      timeout_secs: 120
      on_error: fail
      input:
        command: run
        select: customers
        hooks: {}
",
        )?;

        let (hooks, _) = load_project_config(&dir)?;
        assert_eq!(hooks.pre_run.len(), 1);
        let hook = &hooks.pre_run[0];
        assert_eq!(hook.workflow_type, "dbt_run");
        let input = hook
            .input
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("input should be set"))?;
        assert_eq!(input["command"], "run");
        assert_eq!(input["select"], "customers");
        assert!(input["hooks"].is_object());

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn load_project_config_without_custom_input() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("dbt_temporal.yml"),
            r"
hooks:
  pre_run:
    - workflow_type: notify
      task_queue: hooks-queue
",
        )?;

        let (hooks, _) = load_project_config(&dir)?;
        assert!(hooks.pre_run[0].input.is_none());

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn skip_sentinel_json_false() {
        let bytes = b"false";
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, Some((true, None)));
    }

    #[test]
    fn skip_sentinel_object_skip_true() {
        let bytes = br#"{"skip": true}"#;
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, Some((true, None)));
    }

    #[test]
    fn skip_sentinel_object_skip_true_with_reason() {
        let bytes = br#"{"skip": true, "reason": "not ready"}"#;
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, Some((true, Some("not ready".to_string()))));
    }

    #[test]
    fn skip_sentinel_object_skip_false() {
        let bytes = br#"{"skip": false}"#;
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, Some((false, None)));
    }

    #[test]
    fn skip_sentinel_json_true_is_not_skip() {
        let bytes = b"true";
        let result = parse_skip_sentinel(bytes);
        // `true` doesn't match any sentinel pattern.
        assert_eq!(result, None);
    }

    #[test]
    fn skip_sentinel_string_is_not_skip() {
        let bytes = br#""hello""#;
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, None);
    }

    #[test]
    fn skip_sentinel_null_is_not_skip() {
        let bytes = b"null";
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, None);
    }

    #[test]
    fn skip_sentinel_invalid_json() {
        let bytes = b"not json";
        let result = parse_skip_sentinel(bytes);
        assert_eq!(result, None);
    }

    #[test]
    fn skip_sentinel_object_without_skip_field() {
        let bytes = br#"{"success": true}"#;
        let result = parse_skip_sentinel(bytes);
        // Object without "skip" field — doesn't match.
        assert_eq!(result, None);
    }

    #[test]
    fn load_project_config_fire_and_forget() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("dbt_temporal.yml"),
            r"
hooks:
  on_success:
    - workflow_type: notify
      task_queue: hooks-queue
      fire_and_forget: true
    - workflow_type: audit
      task_queue: hooks-queue
",
        )?;

        let (hooks, _) = load_project_config(&dir)?;
        assert_eq!(hooks.on_success.len(), 2);
        assert!(hooks.on_success[0].fire_and_forget);
        assert!(!hooks.on_success[1].fire_and_forget);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
