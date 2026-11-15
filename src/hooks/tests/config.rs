use crate::hooks::*;
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

    let resolved = merge_resolved_config(
        Some(input_hooks),
        &default_hooks,
        &default_retry,
        &TimeoutConfig::default(),
    );

    assert_eq!(resolved.hooks.pre_run.len(), 1);
    assert_eq!(resolved.hooks.pre_run[0].workflow_type, "from_input");
    // Retry comes from defaults regardless of input.
    assert_eq!(resolved.retry.max_attempts, 7);
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

    let resolved =
        merge_resolved_config(None, &default_hooks, &default_retry, &TimeoutConfig::default());

    assert_eq!(resolved.hooks.on_success.len(), 1);
    assert_eq!(resolved.hooks.on_success[0].workflow_type, "publish");
    assert_eq!(resolved.retry.max_attempts, RetryConfig::default().max_attempts);
}

/// Hook retries are opt-in per phase, and absent config means off — a
/// project that never heard of the setting keeps today's behaviour.
#[test]
fn load_project_config_reads_per_phase_hook_retry() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        dir.join("dbt_temporal.yml"),
        "retry:\n  max_attempts: 5\n  project_hooks:\n    on_run_start: true\n",
    )?;

    let (_, retry, _) = load_project_config(&dir)?;
    assert_eq!(retry.max_attempts, 5);
    assert!(retry.project_hooks.on_run_start, "explicitly enabled");
    assert!(!retry.project_hooks.on_run_end, "unset means off, not inherited");

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn load_project_config_defaults_hook_retry_off() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("dbt_temporal.yml"), "retry:\n  max_attempts: 2\n")?;

    let (_, retry, _) = load_project_config(&dir)?;
    assert!(!retry.project_hooks.on_run_start);
    assert!(!retry.project_hooks.on_run_end);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn load_project_config_missing_file_returns_defaults() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let (hooks, retry, _timeouts) = load_project_config(&dir)?;
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

    let (hooks, retry, _timeouts) = load_project_config(&dir)?;
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

    let (hooks, retry, _timeouts) = load_project_config(&dir)?;
    assert_eq!(hooks.on_failure.len(), 1);
    assert_eq!(retry.max_attempts, 3);
    assert!(retry.non_retryable_errors.is_empty(), "{:?}", retry.non_retryable_errors);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn load_project_config_empty_file() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("dbtt-hooks-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("dbt_temporal.yml"), "")?;

    let result = load_project_config(&dir);
    if let Ok((hooks, retry, _timeouts)) = result {
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

    let (hooks, _, _) = load_project_config(&dir)?;
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

    let (hooks, _, _) = load_project_config(&dir)?;
    assert!(hooks.pre_run[0].input.is_none());

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
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

    let (hooks, _, _) = load_project_config(&dir)?;
    assert_eq!(hooks.on_success.len(), 2);
    assert!(hooks.on_success[0].fire_and_forget);
    assert!(!hooks.on_success[1].fire_and_forget);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
