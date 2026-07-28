use serde::{Deserialize, Serialize};

use super::workflow::{DbtRunInput, DbtRunOutput, ExecutionPlan, ProjectHookPhase};

/// Lifecycle event for a user-defined hook in `dbt_temporal.yml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookEvent {
    PreRun,
    OnSuccess,
    OnFailure,
}

impl HookEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreRun => "pre_run",
            Self::OnSuccess => "on_success",
            Self::OnFailure => "on_failure",
        }
    }
}

impl std::fmt::Display for HookEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    /// Which `dbt_project.yml` hooks may retry. Off for both by default.
    #[serde(default)]
    pub project_hooks: ProjectHookRetry,
}

/// Whether `on-run-start` / `on-run-end` may be retried on a transient
/// warehouse error.
///
/// Off by default, and deliberately per-phase rather than one switch: a hook is
/// only safe to re-run if it is idempotent, and that is a property of the SQL
/// the project author wrote, which this crate cannot inspect. `on-run-start`
/// hooks are typically setup (`create schema if not exists`, grants) and safe;
/// `on-run-end` hooks typically append audit rows, where a retry after a
/// partial failure double-writes. Only the author knows which they have, so
/// they choose.
///
/// Enabling this never makes a *permanent* failure retry — bad SQL still fails
/// on the first attempt. It only lets the retryable error variants through, the
/// same classification nodes already use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectHookRetry {
    #[serde(default)]
    pub on_run_start: bool,
    #[serde(default)]
    pub on_run_end: bool,
}

impl ProjectHookRetry {
    /// Whether the given phase is allowed to retry.
    #[must_use]
    pub const fn allows(&self, phase: ProjectHookPhase) -> bool {
        match phase {
            ProjectHookPhase::OnRunStart => self.on_run_start,
            ProjectHookPhase::OnRunEnd => self.on_run_end,
        }
    }
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
            project_hooks: ProjectHookRetry::default(),
        }
    }
}

// --- Activity timeouts ---

/// Activity timeouts, configurable in `dbt_temporal.yml` under `timeouts:`.
///
/// Defaults reproduce the values these were hardcoded to. The one most likely
/// to need raising is `node_secs`: a model that legitimately runs longer than
/// an hour would otherwise be killed mid-statement and retried.
///
/// Heartbeat timeouts should stay well above the activity heartbeat tick
/// (`heartbeat::HEARTBEAT_INTERVAL`, 30s) — the server treats a missed
/// heartbeat as a dead worker and reschedules. `validate` enforces that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeoutConfig {
    /// `execute_node` start-to-close. Bounds a single node's compile + execute.
    #[serde(default = "default_node_secs")]
    pub node_secs: u64,
    /// `execute_node` heartbeat timeout.
    #[serde(default = "default_node_heartbeat_secs")]
    pub node_heartbeat_secs: u64,
    /// `run_project_hooks` start-to-close, for both on-run-start and on-run-end.
    #[serde(default = "default_hook_secs")]
    pub hook_secs: u64,
    /// `run_project_hooks` heartbeat timeout.
    #[serde(default = "default_hook_heartbeat_secs")]
    pub hook_heartbeat_secs: u64,
    /// `plan_project` start-to-close. Covers selector evaluation, DAG build,
    /// and any state/retry manifest fetched from the artifact store.
    #[serde(default = "default_plan_secs")]
    pub plan_secs: u64,
    /// `store_artifacts` start-to-close. Scales with manifest size and the
    /// artifact store's upload throughput.
    #[serde(default = "default_store_artifacts_secs")]
    pub store_artifacts_secs: u64,
}

const fn default_node_secs() -> u64 {
    3600
}
const fn default_node_heartbeat_secs() -> u64 {
    300
}
const fn default_hook_secs() -> u64 {
    300
}
const fn default_hook_heartbeat_secs() -> u64 {
    120
}
const fn default_plan_secs() -> u64 {
    300
}
const fn default_store_artifacts_secs() -> u64 {
    120
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            node_secs: default_node_secs(),
            node_heartbeat_secs: default_node_heartbeat_secs(),
            hook_secs: default_hook_secs(),
            hook_heartbeat_secs: default_hook_heartbeat_secs(),
            plan_secs: default_plan_secs(),
            store_artifacts_secs: default_store_artifacts_secs(),
        }
    }
}

/// Smallest heartbeat timeout that leaves room for a missed tick. The activity
/// heartbeats every 30s, so anything under this reports healthy workers dead.
const MIN_HEARTBEAT_SECS: u64 = 60;

impl TimeoutConfig {
    /// Reject values that would break the activity rather than tune it.
    ///
    /// Called at config load so a typo fails worker startup with a clear
    /// message, instead of surfacing later as activities timing out mid-run.
    pub fn validate(&self) -> Result<(), String> {
        let zero_checks = [
            ("node_secs", self.node_secs),
            ("hook_secs", self.hook_secs),
            ("plan_secs", self.plan_secs),
            ("store_artifacts_secs", self.store_artifacts_secs),
        ];
        for (name, value) in zero_checks {
            if value == 0 {
                return Err(format!("timeouts.{name} must be greater than zero"));
            }
        }

        let heartbeats = [
            ("node_heartbeat_secs", self.node_heartbeat_secs, self.node_secs, "node_secs"),
            ("hook_heartbeat_secs", self.hook_heartbeat_secs, self.hook_secs, "hook_secs"),
        ];
        for (name, value, ceiling, ceiling_name) in heartbeats {
            if value < MIN_HEARTBEAT_SECS {
                return Err(format!(
                    "timeouts.{name} must be at least {MIN_HEARTBEAT_SECS}s — the activity \
                     heartbeats every 30s, and a shorter timeout marks live workers dead"
                ));
            }
            if value > ceiling {
                return Err(format!(
                    "timeouts.{name} ({value}s) exceeds timeouts.{ceiling_name} ({ceiling}s), \
                     so it can never fire"
                ));
            }
        }
        Ok(())
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
    /// Which event triggered this hook.
    pub event: HookEvent,
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
}
