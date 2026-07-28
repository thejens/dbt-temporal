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
use crate::types::{LoadSegmentStateInput, RunSegmentState, SaveSegmentStateInput};

use super::DbtActivities;

/// Filename used for every segment handover of a run.
///
/// Keyed by invocation id in the store, and each segment overwrites the last —
/// only the most recent handover is ever read, and keeping one file per run
/// avoids unbounded litter for a run that continues many times.
const SEGMENT_STATE_FILENAME: &str = "run_segment_state.json";

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

    let json = serde_json::to_vec(&input.state).context("serializing run segment state")?;
    let size = json.len();
    let path = store
        .store(&input.invocation_id, SEGMENT_STATE_FILENAME, &json)
        .await
        .map_err(|e| DbtTemporalError::ArtifactStore(e.context("storing run segment state")))?;

    info!(
        path = %path,
        bytes = size,
        results = input.state.all_results.len(),
        "spilled run state for continue-as-new"
    );
    Ok(path)
}

/// Read back the state written by the previous segment.
pub async fn load_segment_state_inner(
    activities: &DbtActivities,
    input: LoadSegmentStateInput,
) -> Result<RunSegmentState, anyhow::Error> {
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

    info!(
        state_ref = %input.state_ref,
        results = state.all_results.len(),
        "restored run state after continue-as-new"
    );
    Ok(state)
}
