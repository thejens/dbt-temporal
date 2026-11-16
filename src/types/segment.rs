use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::hooks::HookError;
use super::{ExecutionPlan, NodeExecutionResult, NodeStatusTree};

/// Where a continued run picks up.
///
/// Deliberately small: it travels in the continuation's workflow input, which
/// lands in the new run's history. The bulky part (plan, accumulated results)
/// lives in the artifact store behind `state_ref`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResumeState {
    /// The original run's invocation id, so artifacts and search attributes
    /// stay keyed to one logical run across every segment.
    pub invocation_id: String,
    /// Artifact-store path holding the previous segment's `RunSegmentState`.
    pub state_ref: String,
    /// Index of the first level the continued run should execute.
    pub next_level: usize,
    /// 1 for the first continuation, incrementing after that. Reported in the
    /// run log so a multi-segment run is legible in the Temporal UI.
    pub segment: u32,
    /// The value a `set_fail_fast` update left on the predecessor, if one ran.
    /// Carried in the successor's input rather than in the spilled segment
    /// state so it is read after the checkpoint activity returns: an update
    /// accepted while that activity was outstanding would otherwise be
    /// acknowledged and then dropped.
    #[serde(default)]
    pub fail_fast_override: Option<bool>,
}

/// What a successor needs in order to carry on, without the segment payloads.
///
/// `load_segment_state` returns this rather than the whole checkpoint: the
/// results and log of every segment so far would otherwise come back as an
/// activity result and land in the successor's history — the same payload the
/// continuation exists to avoid, paid on the way in instead of the way out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSegmentControl {
    pub plan: ExecutionPlan,
    /// Every checkpoint of this run so far, oldest first, including the one
    /// just read.
    pub prior_segments: Vec<String>,
    pub node_status: NodeStatusTree,
    pub failed_nodes: Vec<String>,
    pub had_failure: bool,
    pub effective_env: BTreeMap<String, String>,
    pub hook_errors: Vec<HookError>,
    pub total_nodes: usize,
    pub node_counter: usize,
    pub next_level: usize,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Input to `save_segment_state`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveSegmentStateInput {
    pub invocation_id: String,
    pub state: RunSegmentState,
}

/// Input to `load_segment_state`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadSegmentStateInput {
    pub state_ref: String,
    /// What the successor's own input says this checkpoint should be, verified
    /// against what the checkpoint says about itself before its contents are
    /// trusted. `None` when the caller has no identity to check against.
    #[serde(default)]
    pub expected: Option<SegmentIdentity>,
}

/// The identity a successor requires of the checkpoint it resumes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentIdentity {
    pub invocation_id: String,
    pub segment: u32,
    pub next_level: usize,
}

/// Everything a run segment hands to its successor.
///
/// The workflow passes this inline as the input of the `save_segment_state`
/// activity, which writes it to the artifact store as JSON; the continuation
/// then carries only a reference. That keeps the plan (which alone can reach
/// the manifest-inlining threshold) and the accumulated results, which grow
/// with every node executed, out of the new run's start payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSegmentState {
    /// Payload schema version. `0` identifies a checkpoint that carries no
    /// identity of its own, whose `invocation_id` and `segment` are therefore
    /// absent rather than wrong, so a run in flight across that boundary still
    /// resumes.
    #[serde(default)]
    pub schema_version: u32,
    /// The run this checkpoint belongs to.
    #[serde(default)]
    pub invocation_id: String,
    /// The continuation that wrote it. With the segment in the artifact name
    /// too, a delayed or retried write from an earlier segment lands on its own
    /// key instead of over the live one.
    #[serde(default)]
    pub segment: u32,
    /// The plan, carried forward so a continuation never re-plans. Re-planning
    /// could otherwise pick up a different node set mid-run.
    pub plan: ExecutionPlan,
    /// Checkpoints written by every earlier segment of this run, oldest first.
    ///
    /// Each segment's results and log stay in its own checkpoint; this is how
    /// the artifact activity finds them again at the end. Bounded by the number
    /// of continuations, which is a handful even for a very long run.
    #[serde(default)]
    pub prior_segments: Vec<String>,
    /// This segment's node results — not the run's. Carrying every segment's
    /// forward would make the checkpoint grow with the run rather than with
    /// the work in front of it.
    pub all_results: Vec<NodeExecutionResult>,
    /// This segment's log lines, on the same terms.
    pub log_lines: Vec<String>,
    pub node_status: NodeStatusTree,
    pub failed_nodes: Vec<String>,
    pub had_failure: bool,
    /// Resolved after pre_run hooks ran, so the continuation inherits any
    /// `extra_env` they injected without re-running them.
    pub effective_env: BTreeMap<String, String>,
    pub hook_errors: Vec<HookError>,
    /// Node count across the whole run, not just this segment — the progress
    /// lines stay continuous across a continuation.
    pub total_nodes: usize,
    /// Nodes already counted in the progress line numbering.
    pub node_counter: usize,
    /// First level the successor should execute.
    pub next_level: usize,
    /// When the logical run began, carried so the artifacts describe one run
    /// rather than restarting the clock at each continuation.
    #[serde(default)]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}
