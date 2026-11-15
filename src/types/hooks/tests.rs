use super::*;

#[test]
fn hook_error_mode_default_is_warn() {
    assert_eq!(HookErrorMode::default(), HookErrorMode::Warn);
}

#[test]
fn hook_error_mode_serializes_as_snake_case() -> anyhow::Result<()> {
    assert_eq!(serde_json::to_string(&HookErrorMode::Fail)?, "\"fail\"");
    assert_eq!(serde_json::to_string(&HookErrorMode::Warn)?, "\"warn\"");
    assert_eq!(serde_json::to_string(&HookErrorMode::Ignore)?, "\"ignore\"");
    Ok(())
}

#[test]
fn hooks_config_default_is_empty() {
    let hooks = HooksConfig::default();
    assert!(hooks.pre_run.is_empty());
    assert!(hooks.on_success.is_empty());
    assert!(hooks.on_failure.is_empty());
}

#[test]
fn retry_config_defaults() {
    let rc = RetryConfig::default();
    assert_eq!(rc.max_attempts, 3);
    assert_eq!(rc.initial_interval_secs, 5);
    assert!((rc.backoff_coefficient - 2.0).abs() < f64::EPSILON);
    assert_eq!(rc.max_interval_secs, 60);
    assert!(rc.non_retryable_errors.is_empty(), "{:?}", rc.non_retryable_errors);
}

#[test]
fn retry_config_deserializes_with_defaults() -> anyhow::Result<()> {
    let json = r"{}";
    let rc: RetryConfig = serde_json::from_str(json)?;
    assert_eq!(rc.max_attempts, 3);
    assert_eq!(rc.initial_interval_secs, 5);
    Ok(())
}

#[test]
fn retry_config_deserializes_partial_override() -> anyhow::Result<()> {
    let json = r#"{"max_attempts": 5, "non_retryable_errors": ["permission denied"]}"#;
    let rc: RetryConfig = serde_json::from_str(json)?;
    assert_eq!(rc.max_attempts, 5);
    assert_eq!(rc.initial_interval_secs, 5); // default
    assert_eq!(rc.non_retryable_errors, vec!["permission denied"]);
    Ok(())
}

#[test]
fn retry_config_round_trip() -> anyhow::Result<()> {
    let rc = RetryConfig {
        max_attempts: 1,
        initial_interval_secs: 10,
        backoff_coefficient: 1.5,
        max_interval_secs: 120,
        non_retryable_errors: vec!["access denied".into(), "relation .* does not exist".into()],
        project_hooks: ProjectHookRetry::default(),
    };
    let json = serde_json::to_string(&rc)?;
    let back: RetryConfig = serde_json::from_str(&json)?;
    assert_eq!(back.max_attempts, 1);
    assert_eq!(back.non_retryable_errors.len(), 2);
    Ok(())
}

#[test]
fn hook_config_with_custom_input_round_trip() -> anyhow::Result<()> {
    let json = r#"{
            "workflow_type": "dbt_run",
            "task_queue": "dbt-tasks",
            "timeout_secs": 120,
            "on_error": "fail",
            "input": {"command": "run", "select": "customers", "hooks": {}}
        }"#;
    let hook: HookConfig = serde_json::from_str(json)?;
    assert_eq!(hook.workflow_type, "dbt_run");
    let input = hook
        .input
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("input should be set"))?;
    assert_eq!(input["command"], "run");
    assert_eq!(input["select"], "customers");

    // Round-trip
    let json2 = serde_json::to_string(&hook)?;
    let back: HookConfig = serde_json::from_str(&json2)?;
    let back_input = back
        .input
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("input should be set"))?;
    assert_eq!(back_input["command"], "run");
    Ok(())
}

#[test]
fn hook_config_without_input_defaults_to_none() -> anyhow::Result<()> {
    let json = r#"{
            "workflow_type": "notify",
            "task_queue": "hooks-queue"
        }"#;
    let hook: HookConfig = serde_json::from_str(json)?;
    assert!(hook.input.is_none());
    assert!(!hook.fire_and_forget);
    Ok(())
}

#[test]
fn hook_config_fire_and_forget() -> anyhow::Result<()> {
    let json = r#"{
            "workflow_type": "notify",
            "task_queue": "hooks-queue",
            "fire_and_forget": true
        }"#;
    let hook: HookConfig = serde_json::from_str(json)?;
    assert!(hook.fire_and_forget);

    // Round-trip
    let json2 = serde_json::to_string(&hook)?;
    let back: HookConfig = serde_json::from_str(&json2)?;
    assert!(back.fire_and_forget);
    Ok(())
}

#[test]
fn hook_config_fire_and_forget_defaults_to_false() -> anyhow::Result<()> {
    let json = r#"{
            "workflow_type": "alert",
            "task_queue": "alerts"
        }"#;
    let hook: HookConfig = serde_json::from_str(json)?;
    assert!(!hook.fire_and_forget);
    Ok(())
}

#[test]
fn hook_event_as_str_for_each_variant() {
    assert_eq!(HookEvent::PreRun.as_str(), "pre_run");
    assert_eq!(HookEvent::OnSuccess.as_str(), "on_success");
    assert_eq!(HookEvent::OnFailure.as_str(), "on_failure");
}

#[test]
fn hook_event_display_matches_as_str() {
    assert_eq!(format!("{}", HookEvent::PreRun), "pre_run");
    assert_eq!(format!("{}", HookEvent::OnSuccess), "on_success");
    assert_eq!(format!("{}", HookEvent::OnFailure), "on_failure");
}

#[test]
fn hook_event_serde_round_trip() -> anyhow::Result<()> {
    let s = serde_json::to_string(&HookEvent::OnSuccess)?;
    assert_eq!(s, "\"on_success\"");
    let back: HookEvent = serde_json::from_str(&s)?;
    assert_eq!(back, HookEvent::OnSuccess);
    Ok(())
}

#[test]
fn hook_execution_outcome_default_is_clean_slate() {
    let outcome = HookExecutionOutcome::default();
    assert!(outcome.errors.is_empty());
    assert!(!outcome.skip);
    assert!(outcome.skip_reason.is_none());
    assert!(outcome.extra_env.is_empty());
}

#[test]
fn hook_error_round_trip() -> anyhow::Result<()> {
    let err = HookError {
        hook_workflow_type: "notify".into(),
        event: "on_failure".into(),
        error: "child failed".into(),
    };
    let json = serde_json::to_string(&err)?;
    let back: HookError = serde_json::from_str(&json)?;
    assert_eq!(back.hook_workflow_type, "notify");
    assert_eq!(back.event, "on_failure");
    assert_eq!(back.error, "child failed");
    Ok(())
}

// --- TimeoutConfig ---

#[test]
fn timeout_defaults_match_the_previously_hardcoded_values() {
    let t = TimeoutConfig::default();
    assert_eq!(t.node_secs, 3600);
    assert_eq!(t.node_heartbeat_secs, 300);
    assert_eq!(t.hook_secs, 300);
    assert_eq!(t.hook_heartbeat_secs, 120);
    assert_eq!(t.plan_secs, 300);
    assert_eq!(t.store_artifacts_secs, 120);
    t.validate().expect("defaults must be valid");
}

#[test]
fn timeout_config_fills_defaults_for_omitted_keys() {
    let t: TimeoutConfig = dbt_yaml::from_str("node_secs: 7200").unwrap();
    assert_eq!(t.node_secs, 7200);
    assert_eq!(t.hook_secs, 300, "unset keys keep their default");
    t.validate().expect("partial override must be valid");
}

#[test]
fn timeout_validation_rejects_zero_durations() {
    let t = TimeoutConfig {
        node_secs: 0,
        ..TimeoutConfig::default()
    };
    let err = t.validate().expect_err("zero must be rejected");
    assert!(err.contains("node_secs"), "got: {err}");
}

/// A heartbeat timeout below the 30s tick marks healthy workers dead.
#[test]
fn timeout_validation_rejects_heartbeats_shorter_than_the_tick() {
    let t = TimeoutConfig {
        node_heartbeat_secs: 15,
        ..TimeoutConfig::default()
    };
    let err = t
        .validate()
        .expect_err("sub-tick heartbeat must be rejected");
    assert!(err.contains("node_heartbeat_secs"), "got: {err}");
    assert!(err.contains("30s"), "should explain the tick: {err}");
}

/// A heartbeat timeout above start-to-close can never fire.
#[test]
fn timeout_validation_rejects_heartbeat_above_start_to_close() {
    let t = TimeoutConfig {
        hook_secs: 100,
        hook_heartbeat_secs: 200,
        ..TimeoutConfig::default()
    };
    let err = t
        .validate()
        .expect_err("unreachable heartbeat must be rejected");
    assert!(err.contains("hook_heartbeat_secs"), "got: {err}");
    assert!(err.contains("can never fire"), "got: {err}");
}
