use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use temporalio_common::UntypedWorkflow;
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy;

use temporalio_sdk::ChildWorkflowOptions;
use tracing::{info, warn};

use crate::project_registry::ProjectRegistry;
use crate::types::{
    HookConfig, HookError, HookErrorMode, HookEvent, HookExecutionOutcome, HookPayload,
    HooksConfig, ResolveConfigInput, ResolvedProjectConfig, RetryConfig,
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
    Ok(merge_resolved_config(
        input.input_hooks,
        &state.default_hooks,
        &state.default_retry,
    ))
}

/// Pure config-merge step extracted from `resolve_config_impl` so it can be
/// unit-tested without standing up a full `WorkerState`.
///
/// The caller's `input_hooks` is a full override (when present); retry config
/// always comes from the worker-state defaults.
fn merge_resolved_config(
    input_hooks: Option<HooksConfig>,
    default_hooks: &HooksConfig,
    default_retry: &RetryConfig,
) -> ResolvedProjectConfig {
    ResolvedProjectConfig {
        hooks: input_hooks.unwrap_or_else(|| default_hooks.clone()),
        retry: default_retry.clone(),
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
fn build_hook_options(hook: &HookConfig, workflow_id: String) -> ChildWorkflowOptions {
    let timeout = hook.timeout_secs.unwrap_or(DEFAULT_HOOK_TIMEOUT_SECS);
    let parent_close_policy = if hook.fire_and_forget {
        ParentClosePolicy::Abandon
    } else {
        ParentClosePolicy::Unspecified
    };
    ChildWorkflowOptions {
        workflow_id,
        task_queue: Some(hook.task_queue.clone()),
        parent_close_policy,
        execution_timeout: Some(Duration::from_secs(timeout)),
        ..Default::default()
    }
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
    let Some(bytes) = payload_bytes else {
        return HookCompletionAction::Continue;
    };
    if let Some(extra) = parse_extra_env(bytes) {
        outcome.extra_env.extend(extra);
    }
    if let Some((true, reason)) = parse_skip_sentinel(bytes) {
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

        let opts = build_hook_options(hook, workflow_id);
        let wf = UntypedWorkflow::new(&hook.workflow_type);
        let raw_input = RawValue::new(input_payload);

        if hook.fire_and_forget {
            // Start but don't await result — the child runs independently.
            match ctx.child_workflow(wf, raw_input, opts).await {
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
fn parse_extra_env(bytes: &[u8]) -> Option<std::collections::BTreeMap<String, String>> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
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
    wf: UntypedWorkflow,
    input: RawValue,
    opts: ChildWorkflowOptions,
) -> anyhow::Result<Option<Vec<u8>>> {
    let started = ctx
        .child_workflow(wf, input, opts)
        .await
        .context("child workflow failed to start")?;

    let result = started.result().await.context("child workflow failed")?;

    let bytes = result.payloads.into_iter().next().map(|p| p.data);
    Ok(bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use crate::types::{HookConfig, HookErrorMode};

    #[test]
    fn merge_resolved_config_uses_input_hooks_when_present() {
        let mut input_hooks = HooksConfig::default();
        input_hooks.pre_run.push(HookConfig {
            workflow_type: "from_input".to_string(),
            task_queue: "q".to_string(),
            timeout_secs: None,
            on_error: HookErrorMode::default(),
            input: None,
            fire_and_forget: false,
        });

        let mut default_hooks = HooksConfig::default();
        default_hooks.pre_run.push(HookConfig {
            workflow_type: "from_default".to_string(),
            task_queue: "q".to_string(),
            timeout_secs: None,
            on_error: HookErrorMode::default(),
            input: None,
            fire_and_forget: false,
        });

        let default_retry = RetryConfig {
            max_attempts: 7,
            ..RetryConfig::default()
        };

        let resolved = merge_resolved_config(Some(input_hooks), &default_hooks, &default_retry);

        assert_eq!(resolved.hooks.pre_run.len(), 1);
        assert_eq!(resolved.hooks.pre_run[0].workflow_type, "from_input");
        // Retry comes from defaults regardless of input.
        assert_eq!(resolved.retry.max_attempts, 7);
    }

    // --- build_hook_workflow_id ---

    #[test]
    fn build_hook_workflow_id_includes_parent_event_index_and_type() {
        let id = build_hook_workflow_id("dbt-run-abc", HookEvent::PreRun, 2, "notify");
        assert_eq!(id, "dbt-run-abc-hook-pre_run-2-notify");
    }

    #[test]
    fn build_hook_workflow_id_distinguishes_each_event() {
        let pre = build_hook_workflow_id("p", HookEvent::PreRun, 0, "h");
        let succ = build_hook_workflow_id("p", HookEvent::OnSuccess, 0, "h");
        let fail = build_hook_workflow_id("p", HookEvent::OnFailure, 0, "h");
        assert_ne!(pre, succ);
        assert_ne!(pre, fail);
        assert_ne!(succ, fail);
    }

    // --- build_hook_options ---

    fn make_hook(
        workflow_type: &str,
        timeout_secs: Option<u64>,
        fire_and_forget: bool,
    ) -> HookConfig {
        HookConfig {
            workflow_type: workflow_type.to_string(),
            task_queue: "q".to_string(),
            timeout_secs,
            on_error: HookErrorMode::default(),
            input: None,
            fire_and_forget,
        }
    }

    #[test]
    fn build_hook_options_sets_workflow_id_and_task_queue() {
        let hook = make_hook("notify", Some(30), false);
        let opts = build_hook_options(&hook, "id-123".to_string());
        assert_eq!(opts.workflow_id, "id-123");
        assert_eq!(opts.task_queue.as_deref(), Some("q"));
        assert_eq!(opts.execution_timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn build_hook_options_uses_default_timeout_when_unset() {
        let hook = make_hook("notify", None, false);
        let opts = build_hook_options(&hook, "id".to_string());
        assert_eq!(opts.execution_timeout, Some(Duration::from_secs(DEFAULT_HOOK_TIMEOUT_SECS)));
    }

    #[test]
    fn build_hook_options_abandons_only_for_fire_and_forget() {
        let normal = build_hook_options(&make_hook("h", None, false), "id".to_string());
        let fire = build_hook_options(&make_hook("h", None, true), "id".to_string());
        assert_eq!(normal.parent_close_policy, ParentClosePolicy::Unspecified);
        assert_eq!(fire.parent_close_policy, ParentClosePolicy::Abandon);
    }

    // --- process_pre_run_completion ---

    fn empty_pre_run_payload() -> HookPayload {
        HookPayload {
            event: HookEvent::PreRun,
            invocation_id: "inv".to_string(),
            input: crate::types::DbtRunInput {
                project: None,
                command: "build".to_string(),
                select: None,
                exclude: None,
                vars: std::collections::BTreeMap::new(),
                target: None,
                full_refresh: false,
                fail_fast: false,
                hooks: None,
                env: std::collections::BTreeMap::new(),
            },
            plan: None,
            output: None,
        }
    }

    #[test]
    fn process_pre_run_completion_continue_for_non_pre_run_events() {
        let mut outcome = HookExecutionOutcome::default();
        // Even with a skip sentinel, a non-pre_run event must not flip skip.
        let bytes = br#"{"skip": true}"#;
        for event in [HookEvent::OnSuccess, HookEvent::OnFailure] {
            let action = process_pre_run_completion(event, Some(bytes), &mut outcome);
            assert_eq!(action, HookCompletionAction::Continue);
            assert!(!outcome.skip);
        }
    }

    #[test]
    fn process_pre_run_completion_continue_when_no_payload() {
        let mut outcome = HookExecutionOutcome::default();
        let action = process_pre_run_completion(HookEvent::PreRun, None, &mut outcome);
        assert_eq!(action, HookCompletionAction::Continue);
    }

    #[test]
    fn process_pre_run_completion_merges_extra_env() {
        let mut outcome = HookExecutionOutcome::default();
        let bytes = br#"{"extra_env": {"DBT_PARTITION": "2026-04-26", "TIER": "premium"}}"#;
        let action = process_pre_run_completion(HookEvent::PreRun, Some(bytes), &mut outcome);
        assert_eq!(action, HookCompletionAction::Continue);
        assert_eq!(outcome.extra_env.get("DBT_PARTITION").map(String::as_str), Some("2026-04-26"));
        assert_eq!(outcome.extra_env.get("TIER").map(String::as_str), Some("premium"));
        assert!(!outcome.skip);
    }

    #[test]
    fn process_pre_run_completion_returns_skip_with_reason() {
        let mut outcome = HookExecutionOutcome::default();
        let bytes = br#"{"skip": true, "reason": "no new data"}"#;
        let action = process_pre_run_completion(HookEvent::PreRun, Some(bytes), &mut outcome);
        assert_eq!(action, HookCompletionAction::Skip(Some("no new data".to_string())));
        assert!(outcome.skip);
        assert_eq!(outcome.skip_reason.as_deref(), Some("no new data"));
    }

    #[test]
    fn process_pre_run_completion_returns_skip_without_reason_for_json_false() {
        let mut outcome = HookExecutionOutcome::default();
        let action = process_pre_run_completion(HookEvent::PreRun, Some(b"false"), &mut outcome);
        assert_eq!(action, HookCompletionAction::Skip(None));
        assert!(outcome.skip);
    }

    #[test]
    fn process_pre_run_completion_merges_env_and_skip_in_one_payload() {
        let mut outcome = HookExecutionOutcome::default();
        let bytes = br#"{"skip": true, "reason": "soft", "extra_env": {"X": "y"}}"#;
        let action = process_pre_run_completion(HookEvent::PreRun, Some(bytes), &mut outcome);
        assert_eq!(action, HookCompletionAction::Skip(Some("soft".to_string())));
        assert_eq!(outcome.extra_env.get("X").map(String::as_str), Some("y"));
    }

    // --- record_hook_error ---

    #[test]
    fn record_hook_error_returns_err_for_fail_mode() {
        let mut hook = make_hook("h", None, false);
        hook.on_error = HookErrorMode::Fail;
        let payload = empty_pre_run_payload();
        let mut outcome = HookExecutionOutcome::default();
        let err = record_hook_error(&hook, &payload, "boom".to_string(), &mut outcome).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("hook 'h'"), "got: {msg}");
        assert!(msg.contains("boom"), "got: {msg}");
        // Outcome.errors stays empty — caller would never see Warn-style accumulation
        // when the hook is configured to abort the run.
        assert!(outcome.errors.is_empty());
    }

    #[test]
    fn record_hook_error_pushes_for_warn_mode() {
        let mut hook = make_hook("notify", None, false);
        hook.on_error = HookErrorMode::Warn;
        let payload = empty_pre_run_payload();
        let mut outcome = HookExecutionOutcome::default();
        record_hook_error(&hook, &payload, "warn-msg".to_string(), &mut outcome).unwrap();
        assert_eq!(outcome.errors.len(), 1);
        let entry = &outcome.errors[0];
        assert_eq!(entry.hook_workflow_type, "notify");
        assert_eq!(entry.event, "pre_run");
        assert_eq!(entry.error, "warn-msg");
    }

    // --- build_hook_input_payload ---

    #[test]
    fn build_hook_input_payload_uses_custom_input_when_present() -> anyhow::Result<()> {
        let mut hook = make_hook("notify", None, false);
        hook.input = Some(serde_json::json!({"target": "bigquery", "force": true}));
        let payload = empty_pre_run_payload();

        let payloads = build_hook_input_payload(&hook, &payload)?;
        assert_eq!(payloads.len(), 1);
        // The serialized payload bytes should reflect the custom input, not
        // the canonical HookPayload (which would include `event` etc).
        let bytes = &payloads[0].data;
        let json: serde_json::Value = serde_json::from_slice(bytes)?;
        assert_eq!(json["target"], "bigquery");
        assert_eq!(json["force"], true);
        // No event field — confirms it's the custom input, not the default.
        assert!(json.get("event").is_none());
        Ok(())
    }

    #[test]
    fn build_hook_input_payload_falls_back_to_default_payload() -> anyhow::Result<()> {
        let hook = make_hook("notify", None, false);
        // hook.input = None — fallback path
        let payload = empty_pre_run_payload();

        let payloads = build_hook_input_payload(&hook, &payload)?;
        assert_eq!(payloads.len(), 1);
        let bytes = &payloads[0].data;
        let json: serde_json::Value = serde_json::from_slice(bytes)?;
        // Default HookPayload includes `event`.
        assert_eq!(json["event"], "pre_run");
        assert_eq!(json["invocation_id"], "inv");
        Ok(())
    }

    #[test]
    fn record_hook_error_drops_silently_for_ignore_mode() {
        let mut hook = make_hook("h", None, false);
        hook.on_error = HookErrorMode::Ignore;
        let payload = empty_pre_run_payload();
        let mut outcome = HookExecutionOutcome::default();
        record_hook_error(&hook, &payload, "ignored".to_string(), &mut outcome).unwrap();
        assert!(outcome.errors.is_empty());
    }

    #[test]
    fn merge_resolved_config_falls_back_to_defaults_when_input_hooks_absent() {
        let mut default_hooks = HooksConfig::default();
        default_hooks.on_success.push(HookConfig {
            workflow_type: "publish".to_string(),
            task_queue: "q".to_string(),
            timeout_secs: Some(120),
            on_error: HookErrorMode::default(),
            input: None,
            fire_and_forget: false,
        });

        let default_retry = RetryConfig::default();

        let resolved = merge_resolved_config(None, &default_hooks, &default_retry);

        assert_eq!(resolved.hooks.on_success.len(), 1);
        assert_eq!(resolved.hooks.on_success[0].workflow_type, "publish");
        assert_eq!(resolved.retry.max_attempts, RetryConfig::default().max_attempts);
    }

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
    fn extra_env_basic() {
        let bytes = br#"{"extra_env": {"FOO": "bar", "NUM": "42"}}"#;
        let Some(result) = parse_extra_env(bytes) else {
            panic!("expected extra_env map");
        };
        assert_eq!(result.get("FOO").map(String::as_str), Some("bar"));
        assert_eq!(result.get("NUM").map(String::as_str), Some("42"));
    }

    #[test]
    fn extra_env_combined_with_skip() {
        // A hook can return both extra_env and a skip sentinel.
        let bytes = br#"{"skip": true, "reason": "not ready", "extra_env": {"X": "1"}}"#;
        let Some(extra) = parse_extra_env(bytes) else {
            panic!("expected extra_env map");
        };
        assert_eq!(extra.get("X").map(String::as_str), Some("1"));
        let skip = parse_skip_sentinel(bytes);
        assert_eq!(skip, Some((true, Some("not ready".to_string()))));
    }

    #[test]
    fn extra_env_non_string_values_skipped() {
        // Non-string values are silently dropped.
        let bytes = br#"{"extra_env": {"GOOD": "yes", "BAD": 123, "ALSO_BAD": null}}"#;
        let Some(result) = parse_extra_env(bytes) else {
            panic!("expected extra_env map");
        };
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("GOOD").map(String::as_str), Some("yes"));
    }

    #[test]
    fn extra_env_absent_returns_none() {
        assert!(parse_extra_env(b"{}").is_none());
        assert!(parse_extra_env(br#"{"skip": true}"#).is_none());
        assert!(parse_extra_env(b"false").is_none());
        assert!(parse_extra_env(b"null").is_none());
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
