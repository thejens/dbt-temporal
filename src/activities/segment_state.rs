//! Handover of run state across a continue-as-new boundary.
//!
//! A workflow that continues as new starts with an empty history, so anything
//! the run has accumulated has to travel with it. It cannot travel *inline*:
//! the continuation's input is itself recorded in the new history, and the plan
//! plus every node result would put us straight back into the size problem
//! continue-as-new exists to solve.
//!
//! So it goes through the artifact store, and only a short reference rides in
//! the input. That makes artifact storage a hard requirement for continuation —
//! the workflow checks for it before deciding to continue, and simply runs on
//! (accepting the history growth) when it is not configured.

use anyhow::Context;
use tracing::info;

use crate::error::DbtTemporalError;
use crate::types::{
    LoadSegmentStateInput, NodeExecutionResult, RunSegmentControl, RunSegmentState,
    SaveSegmentStateInput,
};

use super::DbtActivities;

/// Schema version stamped into every checkpoint this worker writes.
///
/// Bump it when a successor can no longer make sense of what an older worker
/// spilled. `0` means "written before checkpoints identified themselves".
pub const SEGMENT_STATE_SCHEMA_VERSION: u32 = 1;

/// Artifact name for one segment's handover, within the run's own directory.
///
/// One file per segment, never overwritten. A single reused name made the
/// checkpoint mutable: a delayed or retried `save_segment_state` from an
/// earlier segment would land on top of the live one, and the successor would
/// resume from a snapshot of the wrong point in the run — silently, because
/// the payload deserializes perfectly well.
fn segment_state_filename(segment: u32) -> String {
    format!("run_segment_state_{segment}.json")
}

/// Write the state a continuation needs, returning its artifact-store path.
pub async fn save_segment_state_inner(
    activities: &DbtActivities,
    input: SaveSegmentStateInput,
) -> Result<String, anyhow::Error> {
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        DbtTemporalError::Configuration(
            "continue-as-new requires artifact storage (set ARTIFACT_STORE and WRITE_ARTIFACTS)"
                .to_string(),
        )
    })?;

    let filename = segment_state_filename(input.state.segment);
    let json = serde_json::to_vec(&input.state).context("serializing run segment state")?;
    let size = json.len();
    let path = store
        .store(&input.invocation_id, &filename, json.into())
        .await
        .map_err(|e| DbtTemporalError::ArtifactStore(e.context("storing run segment state")))?;

    info!(
        path = %path,
        bytes = size,
        segment = input.state.segment,
        results = input.state.all_results.len(),
        "spilled run state for continue-as-new"
    );
    Ok(path)
}

/// Read back what the previous segment left for its successor.
///
/// Returns the control state only. The segment's own results and log stay in
/// the checkpoint until the artifact activity collects them: returning them
/// here would put every segment's payload into the successor's history as an
/// activity result, which is the cost the continuation exists to avoid.
pub async fn load_segment_state_inner(
    activities: &DbtActivities,
    input: LoadSegmentStateInput,
) -> Result<RunSegmentControl, anyhow::Error> {
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        DbtTemporalError::Configuration(
            "resuming a continued run requires artifact storage".to_string(),
        )
    })?;

    let bytes = store.retrieve(&input.state_ref).await.map_err(|e| {
        DbtTemporalError::ArtifactStore(
            e.context(format!("loading run segment state from {}", input.state_ref)),
        )
    })?;
    let state: RunSegmentState =
        serde_json::from_slice(&bytes).context("parsing run segment state")?;

    if let Some(expected) = input.expected.as_ref() {
        verify_identity(&state, expected)
            .with_context(|| format!("refusing run segment state from {}", input.state_ref))?;
    }

    info!(
        state_ref = %input.state_ref,
        segment = state.segment,
        results = state.all_results.len(),
        prior_segments = state.prior_segments.len(),
        "restored run state after continue-as-new"
    );

    // The checkpoint just read joins the chain: its own payload has to be
    // collected at the end too.
    let mut prior_segments = state.prior_segments;
    prior_segments.push(input.state_ref);

    Ok(RunSegmentControl {
        plan: state.plan,
        prior_segments,
        node_status: state.node_status,
        failed_nodes: state.failed_nodes,
        had_failure: state.had_failure,
        effective_env: state.effective_env,
        hook_errors: state.hook_errors,
        total_nodes: state.total_nodes,
        node_counter: state.node_counter,
        next_level: state.next_level,
        started_at: state.started_at,
    })
}

/// Read one segment's results and log back out of its checkpoint.
///
/// Used when the run finishes, to assemble artifacts that describe the whole
/// run rather than only its last segment.
pub async fn read_segment_payload(
    store: &dyn crate::artifact_store::ArtifactStore,
    state_ref: &str,
) -> Result<(Vec<NodeExecutionResult>, Vec<String>), anyhow::Error> {
    let bytes = store
        .retrieve(state_ref)
        .await
        .with_context(|| format!("loading run segment state from {state_ref}"))?;
    let state: RunSegmentState = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing run segment state from {state_ref}"))?;
    Ok((state.all_results, state.log_lines))
}

/// Refuse a checkpoint that is not the one this successor was continued from.
///
/// A resumable run reads its checkpoint reference from workflow input, so the
/// reference is not guaranteed to name the state this execution actually left
/// behind — a retry of an older execution, a reset, or a hand-written input can
/// all point somewhere else. The payload would deserialize either way, and the
/// run would carry on from another point in the DAG with no sign anything was
/// wrong.
///
/// `next_level` is checked even for a checkpoint written before checkpoints
/// carried identity, because that field always existed. The rest is checked
/// only when the checkpoint claims a schema version that has them.
fn verify_identity(
    state: &RunSegmentState,
    expected: &crate::types::SegmentIdentity,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        state.next_level == expected.next_level,
        "checkpoint resumes at level {} but this run continued at level {}",
        state.next_level,
        expected.next_level
    );

    if state.schema_version == 0 {
        return Ok(());
    }

    anyhow::ensure!(
        state.schema_version <= SEGMENT_STATE_SCHEMA_VERSION,
        "checkpoint schema version {} is newer than this worker understands ({})",
        state.schema_version,
        SEGMENT_STATE_SCHEMA_VERSION
    );
    anyhow::ensure!(
        state.invocation_id == expected.invocation_id,
        "checkpoint belongs to run {} but this run is {}",
        state.invocation_id,
        expected.invocation_id
    );
    anyhow::ensure!(
        state.segment == expected.segment,
        "checkpoint was written by segment {} but this run continued from segment {}",
        state.segment,
        expected.segment
    );
    Ok(())
}
