use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::NodeExecutionResult;

/// Input to the store_artifacts activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreArtifactsInput {
    pub invocation_id: String,
    /// Project the run executed against — needed for catalog generation.
    #[serde(default)]
    pub project: Option<String>,
    /// This execution's node results. A run that continued as new left the
    /// earlier segments' results in their own checkpoints; `prior_segments`
    /// names them.
    pub node_results: Vec<NodeExecutionResult>,
    /// Checkpoints of every earlier segment, oldest first. Read back here so
    /// `run_results.json` describes the whole run and not just its last leg.
    #[serde(default)]
    pub prior_segments: Vec<String>,
    /// The command the run executed, so freshness runs can write the artifacts
    /// dbt writes for them. `source-freshness` writes `sources.json` alone;
    /// `freshness` writes `freshness.json` too.
    #[serde(default)]
    pub command: Option<String>,
    /// Inline manifest JSON, carried only by runs that planned before the
    /// manifest moved to the artifact store; see `ExecutionPlan::manifest_json`.
    pub manifest_json: Option<String>,
    /// Artifact store reference for the manifest stored during planning.
    pub manifest_ref: Option<String>,
    /// CLI-style run log to store as `log.txt` (if run-log writing is enabled).
    #[serde(default)]
    pub run_log: Option<String>,
    /// The run's `env` overrides and `target`, so catalog generation queries
    /// the warehouse the run executed against rather than the one the worker
    /// started against.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub target: Option<String>,
    /// When the logical run began — the *first* segment's start, not this
    /// execution's, so a run that continued as new still reports one span.
    #[serde(default)]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Wall time of the logical run, measured by the workflow's deterministic
    /// clock. Not a sum of node durations: nodes run in parallel, so summing
    /// counts the same seconds many times and still misses the gaps between
    /// levels, hooks and checkpoints.
    #[serde(default)]
    pub elapsed_time: f64,
}

/// Output of the store_artifacts activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreArtifactsOutput {
    /// Path/URI where run_results.json was stored.
    pub run_results_path: String,
    /// Path/URI where manifest.json was stored.
    pub manifest_path: String,
    /// Path/URI where log.txt was stored (if run-log writing was enabled).
    #[serde(default)]
    pub log_path: Option<String>,
    /// Path/URI where catalog.json was stored (if catalog writing was enabled).
    #[serde(default)]
    pub catalog_path: Option<String>,
}
