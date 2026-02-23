use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::hooks::HooksConfig;

fn default_command() -> String {
    "build".to_string()
}

/// Workflow input — what the user provides when starting the workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtRunInput {
    /// Project name or path. Optional if only one project is loaded.
    pub project: Option<String>,
    /// "run" or "build" (default: "build")
    #[serde(default = "default_command")]
    pub command: String,
    /// --select filter
    pub select: Option<String>,
    /// --exclude filter
    pub exclude: Option<String>,
    /// --vars overrides
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
    /// --target override
    pub target: Option<String>,
    /// --full-refresh
    #[serde(default)]
    pub full_refresh: bool,
    /// Stop on first failure
    #[serde(default)]
    pub fail_fast: bool,
    /// Lifecycle hooks — overrides file-based defaults from dbt_temporal.yml.
    #[serde(default)]
    pub hooks: Option<HooksConfig>,
    /// Per-workflow environment variable overrides.
    /// These override `env_var()` calls in model SQL, macros, and Jinja rendering.
    /// Each workflow gets its own isolated env — parallel workflows don't share state.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// Output of the plan_project activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Resolved project name — always set by the plan activity.
    pub project: String,
    /// Topological levels — each level is a vec of unique_ids that can run in parallel.
    pub levels: Vec<Vec<String>>,
    /// Metadata per node, keyed by unique_id.
    pub nodes: BTreeMap<String, NodeInfo>,
    /// Inline manifest JSON (if < 3MB).
    pub manifest_json: Option<String>,
    /// Artifact store reference for large manifests.
    pub manifest_ref: Option<String>,
    /// Workflow run identifier (= Temporal workflow ID).
    pub invocation_id: String,
    /// Search attributes to upsert on the workflow (static config + dynamic).
    #[serde(default)]
    pub search_attributes: BTreeMap<String, String>,
    /// Whether artifact writing is enabled for this run.
    #[serde(default)]
    pub write_artifacts: bool,
}

/// Metadata about a single dbt node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub unique_id: String,
    pub name: String,
    /// "model", "test", "seed", "snapshot"
    pub resource_type: String,
    /// "table", "view", "incremental", "ephemeral"
    pub materialization: Option<String>,
    pub package_name: String,
    pub depends_on: Vec<String>,
}

/// Input to the execute_node activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecutionInput {
    pub unique_id: String,
    pub invocation_id: String,
    /// Resolved project name — always set by the workflow from ExecutionPlan.
    pub project: String,
    /// Per-workflow environment variable overrides for `env_var()` in Jinja rendering.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Target override from workflow input — used for per-workflow adapter engine rebuilding.
    #[serde(default)]
    pub target: Option<String>,
}

/// Result of executing a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecutionResult {
    pub unique_id: String,
    /// "success", "error", "skipped"
    pub status: String,
    /// Wall-clock seconds.
    pub execution_time: f64,
    pub message: Option<String>,
    /// Adapter-specific metadata (rows_affected, bytes_processed, etc.).
    #[serde(default)]
    pub adapter_response: BTreeMap<String, serde_json::Value>,
    /// The compiled SQL that was sent to the warehouse.
    pub compiled_code: Option<String>,
    /// Compile + execute phase timings.
    #[serde(default)]
    pub timing: Vec<TimingEntry>,
    /// For test nodes: number of failures.
    pub failures: Option<i64>,
}

/// A single timing phase (compile or execute).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingEntry {
    pub name: String,
    pub started_at: String,
    pub completed_at: String,
}

/// Input to the store_artifacts activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreArtifactsInput {
    pub invocation_id: String,
    pub node_results: Vec<NodeExecutionResult>,
    /// Inline manifest JSON, or None if stored via manifest_ref.
    pub manifest_json: Option<String>,
    pub manifest_ref: Option<String>,
    /// CLI-style run log to store as `log.txt` (if run-log writing is enabled).
    #[serde(default)]
    pub run_log: Option<String>,
}

/// Output of the store_artifacts activity.
#[allow(clippy::struct_field_names)] // Fields are serialized to JSON; renaming would break the API contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreArtifactsOutput {
    /// Path/URI where run_results.json was stored.
    pub run_results_path: String,
    /// Path/URI where manifest.json was stored.
    pub manifest_path: String,
    /// Path/URI where log.txt was stored (if run-log writing was enabled).
    #[serde(default)]
    pub log_path: Option<String>,
}

/// Final workflow output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtRunOutput {
    pub invocation_id: String,
    pub success: bool,
    /// Whether the run was skipped by a pre_run hook returning a skip sentinel.
    #[serde(default)]
    pub skipped: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    pub node_results: Vec<NodeExecutionResult>,
    pub elapsed_time: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<StoreArtifactsOutput>,
    /// Path/URI where log.txt was stored (if run-log writing was enabled).
    #[serde(default)]
    pub log_path: Option<String>,
    /// Errors from lifecycle hooks that used `on_error: warn`.
    #[serde(default)]
    pub hook_errors: Vec<super::hooks::HookError>,
}

// --- Memo types (stored in Temporal workflow memo for observability) ---

/// Command metadata that triggered this workflow run. Stored once in memo key `"command"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMemo {
    pub command: String,
    pub project: Option<String>,
    pub select: Option<String>,
    pub exclude: Option<String>,
    pub target: Option<String>,
    pub full_refresh: bool,
    pub fail_fast: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub vars: BTreeMap<String, serde_json::Value>,
}

impl From<&DbtRunInput> for CommandMemo {
    fn from(input: &DbtRunInput) -> Self {
        Self {
            command: input.command.clone(),
            project: input.project.clone(),
            select: input.select.clone(),
            exclude: input.exclude.clone(),
            target: input.target.clone(),
            full_refresh: input.full_refresh,
            fail_fast: input.fail_fast,
            vars: input.vars.clone(),
        }
    }
}

/// Node status map. Stored in memo key `"node_status"` and updated after each level.
///
/// Keyed by unique_id, value is just the status — static node metadata (name, resource_type,
/// depends_on, level) is available in the ExecutionPlan and not duplicated here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatusTree {
    pub nodes: BTreeMap<String, NodeStatus>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Running,
    Success,
    Error,
    Skipped,
    Cancelled,
}

/// Input to the resolve_config activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveConfigInput {
    /// Project name to look up in the registry.
    pub project: Option<String>,
    /// Hooks from the workflow input (full override if present).
    pub input_hooks: Option<HooksConfig>,
}

/// Output of the resolve_config activity: hooks + retry policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedProjectConfig {
    pub hooks: HooksConfig,
    pub retry: super::hooks::RetryConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dbt_run_input_json_round_trip() -> anyhow::Result<()> {
        let input = DbtRunInput {
            project: Some("waffle".into()),
            command: "run".into(),
            select: Some("+stg_customers".into()),
            exclude: None,
            vars: BTreeMap::from([("key".into(), serde_json::json!("val"))]),
            target: Some("prod".into()),
            full_refresh: true,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::from([("PG_PORT".into(), "5433".into())]),
        };
        let json = serde_json::to_string(&input)?;
        let back: DbtRunInput = serde_json::from_str(&json)?;
        assert_eq!(back.project.as_deref(), Some("waffle"));
        assert_eq!(back.command, "run");
        assert_eq!(back.select.as_deref(), Some("+stg_customers"));
        assert!(back.full_refresh);
        assert!(!back.fail_fast);
        assert_eq!(
            back.env
                .get("PG_PORT")
                .ok_or_else(|| anyhow::anyhow!("PG_PORT missing"))?,
            "5433"
        );
        Ok(())
    }

    #[test]
    fn node_execution_result_json_round_trip() -> anyhow::Result<()> {
        let result = NodeExecutionResult {
            unique_id: "model.waffle.stg_customers".into(),
            status: "success".into(),
            execution_time: 1.23,
            message: Some("OK".into()),
            adapter_response: BTreeMap::from([("rows_affected".into(), serde_json::json!(42))]),
            compiled_code: Some("SELECT 1".into()),
            timing: vec![TimingEntry {
                name: "execute".into(),
                started_at: "2025-01-01T00:00:00Z".into(),
                completed_at: "2025-01-01T00:00:01Z".into(),
            }],
            failures: None,
        };
        let json = serde_json::to_string(&result)?;
        let back: NodeExecutionResult = serde_json::from_str(&json)?;
        assert_eq!(back.unique_id, "model.waffle.stg_customers");
        assert_eq!(back.status, "success");
        assert!((back.execution_time - 1.23).abs() < f64::EPSILON);
        assert_eq!(back.timing.len(), 1);
        Ok(())
    }

    #[test]
    fn node_status_serializes_as_snake_case() -> anyhow::Result<()> {
        assert_eq!(serde_json::to_string(&NodeStatus::Pending)?, "\"pending\"");
        assert_eq!(serde_json::to_string(&NodeStatus::Running)?, "\"running\"");
        assert_eq!(serde_json::to_string(&NodeStatus::Success)?, "\"success\"");
        assert_eq!(serde_json::to_string(&NodeStatus::Error)?, "\"error\"");
        assert_eq!(serde_json::to_string(&NodeStatus::Skipped)?, "\"skipped\"");
        assert_eq!(serde_json::to_string(&NodeStatus::Cancelled)?, "\"cancelled\"");
        Ok(())
    }

    #[test]
    fn command_memo_from_dbt_run_input() {
        let input = DbtRunInput {
            project: Some("proj".into()),
            command: "build".into(),
            select: Some("tag:nightly".into()),
            exclude: Some("test_*".into()),
            vars: BTreeMap::from([("v".into(), serde_json::json!(1))]),
            target: Some("dev".into()),
            full_refresh: false,
            fail_fast: true,
            hooks: None,
            env: BTreeMap::new(),
        };
        let memo = CommandMemo::from(&input);
        assert_eq!(memo.command, "build");
        assert_eq!(memo.project.as_deref(), Some("proj"));
        assert_eq!(memo.select.as_deref(), Some("tag:nightly"));
        assert_eq!(memo.exclude.as_deref(), Some("test_*"));
        assert_eq!(memo.target.as_deref(), Some("dev"));
        assert!(!memo.full_refresh);
        assert!(memo.fail_fast);
        assert_eq!(memo.vars.len(), 1);
    }

    #[test]
    fn command_memo_vars_skipped_when_empty() -> anyhow::Result<()> {
        let memo = CommandMemo {
            command: "run".into(),
            project: None,
            select: None,
            exclude: None,
            target: None,
            full_refresh: false,
            fail_fast: false,
            vars: BTreeMap::new(),
        };
        let json = serde_json::to_string(&memo)?;
        assert!(!json.contains("vars"));

        let with_vars = CommandMemo {
            vars: BTreeMap::from([("k".into(), serde_json::json!("v"))]),
            ..memo
        };
        let json = serde_json::to_string(&with_vars)?;
        assert!(json.contains("vars"));
        Ok(())
    }

    #[test]
    fn resolved_project_config_round_trip() -> anyhow::Result<()> {
        use super::super::hooks::RetryConfig;
        let config = ResolvedProjectConfig {
            hooks: HooksConfig::default(),
            retry: RetryConfig::default(),
        };
        let json = serde_json::to_string(&config)?;
        let back: ResolvedProjectConfig = serde_json::from_str(&json)?;
        assert!(back.hooks.pre_run.is_empty());
        assert_eq!(back.retry.max_attempts, 3);
        Ok(())
    }
}
