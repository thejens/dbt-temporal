use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{DbtRunInput, NodeStatus};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defer_manifest_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_end: Option<String>,
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
            defer_manifest_ref: input.defer_manifest_ref.clone(),
            event_time_start: input.event_time_start.clone(),
            event_time_end: input.event_time_end.clone(),
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

/// Coarse stage of a run, as reported by the `run_status` query. Terminal
/// state is not a phase: it comes from the workflow result itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    #[default]
    Initializing,
    Planning,
    PreRunHooks,
    Executing,
    Finalizing,
}

/// Value of the `DbtStatus` search attribute, which lets the workflow list be
/// filtered by outcome. `Running` is set at plan time; the other four replace
/// it as the run ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    Running,
    Passed,
    Failed,
    Skipped,
    Cancelled,
}

impl RunOutcome {
    /// The keyword the search attribute holds. Workflow list filters and saved
    /// queries match on these strings, so they are spelled exactly as operators
    /// already query them.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Live run progress served by the `run_status` query.
///
/// Unlike the memo (truncated, written on a level cadence), this reflects
/// workflow state at the last level boundary and costs no history events to
/// read.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunStatusSnapshot {
    pub phase: RunPhase,
    pub total_nodes: usize,
    pub total_levels: usize,
    /// Index of the most recently completed level (1-based; 0 = none yet).
    pub completed_levels: usize,
    pub succeeded: usize,
    /// Nodes that reported a warning — a test over `warn_if`, a source past
    /// `warn_after`. Counted apart from `succeeded` so a run that found
    /// something is not reported as one that found nothing.
    #[serde(default)]
    pub warned: usize,
    pub failed: usize,
    pub skipped: usize,
    pub running: usize,
    /// Effective fail-fast: workflow input, or the live `set_fail_fast`
    /// update override.
    pub fail_fast: bool,
}

impl RunStatusSnapshot {
    /// Update the per-status counters from the node status tree.
    pub fn tally(&mut self, statuses: &NodeStatusTree) {
        self.succeeded = 0;
        self.failed = 0;
        self.skipped = 0;
        self.running = 0;
        for status in statuses.nodes.values() {
            match status {
                NodeStatus::Success => self.succeeded += 1,
                NodeStatus::Warn => self.warned += 1,
                NodeStatus::Error => self.failed += 1,
                NodeStatus::Skipped | NodeStatus::Cancelled => self.skipped += 1,
                NodeStatus::Running => self.running += 1,
                NodeStatus::Pending => {}
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn command_memo_from_dbt_run_input() {
        let input = DbtRunInput {
            project: Some("proj".into()),
            indirect_selection: None,
            resource_types: Vec::new(),
            exclude_resource_types: Vec::new(),
            resume_from: None,
            command: "build".into(),
            select: Some("tag:nightly".into()),
            exclude: Some("test_*".into()),
            vars: BTreeMap::from([("v".into(), serde_json::json!(1))]),
            target: Some("dev".into()),
            full_refresh: false,
            fail_fast: true,
            hooks: None,
            env: BTreeMap::new(),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
            retry_from: None,
            state_manifest_ref: None,
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
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
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
    fn run_phase_serializes_to_the_strings_the_query_reports() {
        for (phase, wire) in [
            (RunPhase::Initializing, "initializing"),
            (RunPhase::Planning, "planning"),
            (RunPhase::PreRunHooks, "pre_run_hooks"),
            (RunPhase::Executing, "executing"),
            (RunPhase::Finalizing, "finalizing"),
        ] {
            assert_eq!(serde_json::to_value(phase).unwrap(), serde_json::json!(wire));
            assert_eq!(serde_json::from_value::<RunPhase>(serde_json::json!(wire)).unwrap(), phase);
        }
    }

    #[test]
    fn run_status_snapshot_with_a_string_phase_deserializes() {
        let snapshot: RunStatusSnapshot = serde_json::from_value(serde_json::json!({
            "phase": "pre_run_hooks",
            "total_nodes": 3,
            "total_levels": 2,
            "completed_levels": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "running": 0,
            "fail_fast": false,
        }))
        .unwrap();
        assert_eq!(snapshot.phase, RunPhase::PreRunHooks);
        assert_eq!(serde_json::to_value(&snapshot).unwrap()["phase"], "pre_run_hooks");
        assert_eq!(RunStatusSnapshot::default().phase, RunPhase::Initializing);
    }

    #[test]
    fn run_outcome_spells_the_search_attribute_values() {
        for (outcome, wire) in [
            (RunOutcome::Running, "running"),
            (RunOutcome::Passed, "passed"),
            (RunOutcome::Failed, "failed"),
            (RunOutcome::Skipped, "skipped"),
            (RunOutcome::Cancelled, "cancelled"),
        ] {
            assert_eq!(outcome.as_str(), wire);
        }
    }

    #[test]
    fn run_status_snapshot_tally_counts_by_status() {
        let mut tree = NodeStatusTree {
            nodes: BTreeMap::new(),
        };
        for (uid, status) in [
            ("model.p.a", NodeStatus::Success),
            ("model.p.b", NodeStatus::Success),
            ("model.p.c", NodeStatus::Error),
            ("model.p.d", NodeStatus::Skipped),
            ("model.p.e", NodeStatus::Cancelled),
            ("model.p.f", NodeStatus::Running),
            ("model.p.g", NodeStatus::Pending),
        ] {
            tree.nodes.insert(uid.to_string(), status);
        }

        let mut snapshot = RunStatusSnapshot::default();
        snapshot.tally(&tree);
        assert_eq!(snapshot.succeeded, 2);
        assert_eq!(snapshot.failed, 1);
        assert_eq!(snapshot.skipped, 2, "skipped includes cancelled nodes");
        assert_eq!(snapshot.running, 1);

        // Re-tallying replaces, not accumulates.
        snapshot.tally(&tree);
        assert_eq!(snapshot.succeeded, 2);
    }
}
