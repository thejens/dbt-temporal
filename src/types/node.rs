use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Workflow command (`build`, `run`, `compile`). Mirrors `DbtRunInput.command`.
    /// `compile` skips the materialization/execute phase and returns the compiled SQL only.
    #[serde(default = "super::workflow::default_command")]
    pub command: String,
    /// Mirrors `DbtRunInput.defer_manifest_ref`.
    #[serde(default)]
    pub defer_manifest_ref: Option<String>,
    /// Mirrors `DbtRunInput.event_time_start`.
    #[serde(default)]
    pub event_time_start: Option<String>,
    /// Mirrors `DbtRunInput.event_time_end`.
    #[serde(default)]
    pub event_time_end: Option<String>,
    /// Mirrors `DbtRunInput.vars`. Applied to render-time `var()` lookups.
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
    /// Mirrors `DbtRunInput.full_refresh`. Surfaces as `flags.FULL_REFRESH`,
    /// which dbt's incremental materialization branches on.
    #[serde(default)]
    pub full_refresh: bool,
    /// When the run started, for the `run_started_at` Jinja global. Absent in
    /// an input recorded by an earlier release, in which case the render keeps
    /// the worker's parse-time value.
    #[serde(default)]
    pub run_started_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Result of executing a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecutionResult {
    pub unique_id: String,
    pub status: NodeStatus,
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
    /// For nodes measured by `source-freshness` / `freshness`: the freshness
    /// check detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessOutcome>,
    /// Where the compiled SQL was stored, when it was stored rather than
    /// carried.
    ///
    /// Compiled SQL is the largest thing a node produces and the workflow has
    /// no use for it: it accumulates every result, copies them into the
    /// checkpoint at each continuation, and clones them again for the artifact
    /// activity — so one model's SQL crossed Temporal many times to reach the
    /// one place that reads it, `run_results.json`. With artifact storage
    /// configured the activity writes it once and this names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_code_ref: Option<String>,
    /// The relation the node actually wrote, fully qualified.
    ///
    /// Recorded because startup metadata does not describe it: a workflow that
    /// overrides the target or the profile's env writes somewhere else
    /// entirely, and everything downstream — the catalog, `run_results.json` —
    /// was describing the warehouse the *worker* started against instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation_name: Option<String>,
}

impl NodeExecutionResult {
    /// The result for a node that never reached the warehouse: skipped,
    /// cancelled, rejected before it started, or merely listed.
    ///
    /// Nothing ran, so there is no timing, SQL, adapter response, failure
    /// count or freshness measurement, and nothing was written for
    /// `relation_name` or `compiled_code_ref` to name.
    pub fn not_executed(
        unique_id: impl Into<String>,
        status: NodeStatus,
        message: Option<String>,
    ) -> Self {
        Self {
            unique_id: unique_id.into(),
            status,
            execution_time: 0.0,
            message,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }
    }
}

/// The grade dbt gives a freshness measurement in `sources.json` /
/// `freshness.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessGrade {
    /// Within `warn_after`, or no threshold was exceeded.
    Pass,
    /// Past `warn_after` but not `error_after`.
    Warn,
    /// Past `error_after`.
    Error,
}

impl FreshnessGrade {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Detail of a completed freshness check, carried on the node result so
/// `store_artifacts` can assemble `sources.json` / `freshness.json` without
/// re-resolving the node's criteria.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessOutcome {
    /// RFC3339 timestamp of `max(loaded_at_field)` in the relation.
    pub max_loaded_at: String,
    /// RFC3339 timestamp taken by the warehouse at check time.
    pub snapshotted_at: String,
    /// Age of the freshest row in seconds (snapshotted_at - max_loaded_at).
    pub max_loaded_at_time_ago_in_s: f64,
    /// How the check graded. `NodeStatus` collapses a warn into success, so
    /// this field is where a warn survives — see also the node `message`.
    pub status: FreshnessGrade,
    /// `source` or `model` — dbt tags freshness.json rows with the resource
    /// type, and it is what separates the two artifacts.
    pub resource_type: String,
    /// The warn_after/error_after criteria the check was evaluated against.
    pub criteria: dbt_schemas::schemas::common::FreshnessDefinition,
}

/// A single timing phase (compile or execute).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingEntry {
    pub name: String,
    pub started_at: String,
    pub completed_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Running,
    Success,
    /// The node did what it was asked and reported something the operator
    /// should see — a data test over its `warn_if` threshold but under
    /// `error_if`, or a source past `warn_after` but not `error_after`. dbt
    /// reports these as `warn`; folding them into `success` made a run that
    /// found something indistinguishable from one that found nothing.
    Warn,
    Error,
    Skipped,
    Cancelled,
}

impl NodeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Success => "success",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn node_execution_result_json_round_trip() -> anyhow::Result<()> {
        let result = NodeExecutionResult {
            unique_id: "model.waffle.stg_customers".into(),
            status: NodeStatus::Success,
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
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        };
        let json = serde_json::to_string(&result)?;
        let back: NodeExecutionResult = serde_json::from_str(&json)?;
        assert_eq!(back.unique_id, "model.waffle.stg_customers");
        assert_eq!(back.status, NodeStatus::Success);
        assert!((back.execution_time - 1.23).abs() < f64::EPSILON);
        assert_eq!(back.timing.len(), 1);
        Ok(())
    }

    /// The wire form of a result that never ran is fixed: the workflow
    /// checkpoints these, so a field appearing or vanishing would change what
    /// a continuation reads back.
    #[test]
    fn not_executed_serializes_the_same_for_every_status() -> anyhow::Result<()> {
        let cases = [
            (NodeStatus::Skipped, Some("upstream failed"), "skipped"),
            (NodeStatus::Cancelled, Some("cancelled"), "cancelled"),
            (NodeStatus::Error, Some("boom"), "error"),
            (NodeStatus::Success, None, "success"),
        ];
        for (status, message, status_wire) in cases {
            let result = NodeExecutionResult::not_executed(
                "model.shop.a",
                status,
                message.map(str::to_string),
            );
            let expected = serde_json::json!({
                "unique_id": "model.shop.a",
                "status": status_wire,
                "execution_time": 0.0,
                "message": message,
                "adapter_response": {},
                "compiled_code": null,
                "timing": [],
                "failures": null,
            });
            assert_eq!(serde_json::to_value(&result)?, expected, "status {status_wire}");
        }
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
    fn node_status_display_matches_as_str() {
        for s in [
            NodeStatus::Pending,
            NodeStatus::Running,
            NodeStatus::Success,
            NodeStatus::Error,
            NodeStatus::Skipped,
            NodeStatus::Cancelled,
        ] {
            assert_eq!(format!("{s}"), s.as_str());
        }
    }

    #[test]
    fn freshness_grade_serializes_to_dbts_strings() {
        for (grade, wire) in [
            (FreshnessGrade::Pass, "pass"),
            (FreshnessGrade::Warn, "warn"),
            (FreshnessGrade::Error, "error"),
        ] {
            assert_eq!(grade.as_str(), wire);
            assert_eq!(serde_json::to_value(grade).unwrap(), serde_json::json!(wire));
            assert_eq!(
                serde_json::from_value::<FreshnessGrade>(serde_json::json!(wire)).unwrap(),
                grade
            );
        }
    }

    #[test]
    fn freshness_outcome_with_a_string_status_deserializes() {
        let outcome: FreshnessOutcome = serde_json::from_value(serde_json::json!({
            "max_loaded_at": "2026-06-12T10:00:00+00:00",
            "snapshotted_at": "2026-06-12T11:00:00+00:00",
            "max_loaded_at_time_ago_in_s": 3600.0,
            "status": "warn",
            "resource_type": "source",
            "criteria": {},
        }))
        .unwrap();
        assert_eq!(outcome.status, FreshnessGrade::Warn);
        assert_eq!(serde_json::to_value(&outcome).unwrap()["status"], "warn");
    }
}
