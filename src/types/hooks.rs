use serde::{Deserialize, Serialize};

use super::workflow::{DbtRunInput, DbtRunOutput, ExecutionPlan};

// --- Retry configuration ---

/// Activity retry policy, configurable in `dbt_temporal.yml` under `retry:`.
///
/// Defaults match a sensible baseline: 3 attempts with exponential backoff.
/// The `non_retryable_errors` field lets users specify regex patterns that,
/// when matched against an adapter error message, suppress retries
/// (e.g. "permission denied", "relation .* does not exist").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of attempts (1 = no retries).
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Initial backoff interval in seconds.
    #[serde(default = "default_initial_interval_secs")]
    pub initial_interval_secs: u64,
    /// Multiplier for successive backoff intervals.
    #[serde(default = "default_backoff_coefficient")]
    pub backoff_coefficient: f64,
    /// Upper bound on backoff interval in seconds.
    #[serde(default = "default_max_interval_secs")]
    pub max_interval_secs: u64,
    /// Regex patterns matched against adapter error messages.
    /// A match promotes the error to non-retryable.
    #[serde(default)]
    pub non_retryable_errors: Vec<String>,
}

const fn default_max_attempts() -> u32 {
    3
}
const fn default_initial_interval_secs() -> u64 {
    5
}
const fn default_backoff_coefficient() -> f64 {
    2.0
}
const fn default_max_interval_secs() -> u64 {
    60
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_interval_secs: default_initial_interval_secs(),
            backoff_coefficient: default_backoff_coefficient(),
            max_interval_secs: default_max_interval_secs(),
            non_retryable_errors: Vec::new(),
        }
    }
}

// --- Lifecycle hooks ---

/// How to handle a hook workflow failure.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HookErrorMode {
    /// Abort the dbt run (pre_run) or mark success=false (on_success/on_failure).
    Fail,
    /// Log a warning and continue. Errors are collected in `DbtRunOutput.hook_errors`.
    #[default]
    Warn,
    /// Swallow the error silently.
    Ignore,
}

/// Configuration for a single lifecycle hook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfig {
    /// Temporal workflow type to start.
    pub workflow_type: String,
    /// Task queue the hook workflow's worker is listening on.
    pub task_queue: String,
    /// Execution timeout in seconds (default: 300).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// What to do if the hook workflow fails.
    #[serde(default)]
    pub on_error: HookErrorMode,
    /// Custom input payload. When set, replaces the default HookPayload.
    #[serde(default)]
    pub input: Option<serde_json::Value>,
    /// Fire-and-forget: start the child workflow but don't await its result.
    /// The child workflow will continue running even if the parent completes or fails.
    #[serde(default)]
    pub fire_and_forget: bool,
}

/// Hooks for each lifecycle event. Each event supports multiple hooks, run sequentially.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HooksConfig {
    #[serde(default)]
    pub pre_run: Vec<HookConfig>,
    #[serde(default)]
    pub on_success: Vec<HookConfig>,
    #[serde(default)]
    pub on_failure: Vec<HookConfig>,
}

/// Payload passed as input to every hook workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookPayload {
    /// Which event triggered this hook: "pre_run", "on_success", or "on_failure".
    pub event: String,
    pub invocation_id: String,
    pub input: DbtRunInput,
    /// The execution plan (available for all hooks).
    pub plan: Option<ExecutionPlan>,
    /// The final run output (only for on_success / on_failure).
    pub output: Option<DbtRunOutput>,
}

/// A non-fatal hook error collected during workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookError {
    pub hook_workflow_type: String,
    pub event: String,
    pub error: String,
}

/// Result of executing a batch of hooks, including skip support for pre_run hooks.
#[derive(Debug, Clone, Default)]
pub struct HookExecutionOutcome {
    pub errors: Vec<HookError>,
    pub skip: bool,
    pub skip_reason: Option<String>,
    /// Extra env vars injected by pre_run hooks via `{"extra_env": {"KEY": "value"}}`.
    /// Merged into NodeExecutionInput.env so they are available via env_var() in model
    /// SQL and also drive per-workflow adapter engine rebuilding for profiles.yml.
    pub extra_env: std::collections::BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
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
        assert!(rc.non_retryable_errors.is_empty());
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
}
