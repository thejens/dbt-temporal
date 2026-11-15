use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::NodeExecutionResult;

/// Phase of a `dbt_project.yml` lifecycle hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectHookPhase {
    OnRunStart,
    OnRunEnd,
}

impl ProjectHookPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OnRunStart => "on_run_start",
            Self::OnRunEnd => "on_run_end",
        }
    }
}

impl std::fmt::Display for ProjectHookPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Input to the run_project_hooks activity.
///
/// One activity invocation runs all hooks for a single phase
/// (`on_run_start` or `on_run_end`) sequentially.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectHooksInput {
    pub phase: ProjectHookPhase,
    pub project: String,
    pub invocation_id: String,
    /// Per-workflow environment variable overrides for `env_var()` rendering.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Target override — used for per-workflow adapter engine rebuilding.
    #[serde(default)]
    pub target: Option<String>,
    /// Node results for `on_run_end` (empty for `on_run_start`).
    #[serde(default)]
    pub node_results: Vec<NodeExecutionResult>,
    /// Mirrors `DbtRunInput.vars` — hooks read `var()` like any other template.
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
    /// Mirrors `DbtRunInput.full_refresh`, exposed to hooks as `flags.FULL_REFRESH`.
    #[serde(default)]
    pub full_refresh: bool,
    /// Whether this phase may retry on a transient warehouse error, from
    /// `retry.project_hooks` in `dbt_temporal.yml`. Resolved in the workflow so
    /// the activity does not have to re-read config on every attempt.
    #[serde(default)]
    pub retry_on_error: bool,
    /// Checkpoints of every earlier segment of a continued run, oldest first,
    /// for `on_run_end` only (empty for `on_run_start`). The activity reads
    /// their results back and puts them ahead of `node_results`, so the
    /// hook's `results` variable describes the whole run and not just its
    /// last leg.
    #[serde(default)]
    pub prior_segments: Vec<String>,
    /// When the run started, for the `run_started_at` Jinja global. Absent in
    /// an input recorded by an earlier release, in which case the render keeps
    /// the worker's parse-time value.
    #[serde(default)]
    pub run_started_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn project_hook_phase_round_trip() {
        assert_eq!(ProjectHookPhase::OnRunStart.as_str(), "on_run_start");
        assert_eq!(ProjectHookPhase::OnRunEnd.as_str(), "on_run_end");
        assert_eq!(format!("{}", ProjectHookPhase::OnRunStart), "on_run_start");
        assert_eq!(format!("{}", ProjectHookPhase::OnRunEnd), "on_run_end");

        let s = serde_json::to_string(&ProjectHookPhase::OnRunStart).unwrap();
        assert_eq!(s, "\"on_run_start\"");
        let back: ProjectHookPhase = serde_json::from_str(&s).unwrap();
        assert_eq!(back, ProjectHookPhase::OnRunStart);
    }
}
