//! Collecting the results and log of the segments a continued run left behind.

use tracing::info;

use crate::artifact_store::ArtifactStore;
use crate::types::StoreArtifactsInput;

use super::store_io_error;

/// Prepend every earlier segment's results and log to this one's.
///
/// A missing or unreadable checkpoint is fatal: writing `run_results.json`
/// without the nodes an earlier segment ran would describe the run as smaller
/// than it was, and nothing downstream could tell.
pub(super) async fn collect_prior_segments(
    store: &dyn ArtifactStore,
    mut input: StoreArtifactsInput,
) -> Result<StoreArtifactsInput, anyhow::Error> {
    if input.prior_segments.is_empty() {
        return Ok(input);
    }

    let mut results = Vec::with_capacity(input.node_results.len());
    let mut log = Vec::new();
    for state_ref in &input.prior_segments {
        let (segment_results, segment_log) =
            crate::activities::segment_state::read_segment_payload(store, state_ref)
                .await
                .map_err(|e| store_io_error("collecting an earlier segment", e))?;
        results.extend(segment_results);
        log.extend(segment_log);
    }
    info!(
        segments = input.prior_segments.len(),
        results = results.len(),
        "collected earlier segments for artifact assembly"
    );

    results.append(&mut input.node_results);
    input.node_results = results;
    if let Some(tail) = input.run_log.take() {
        log.push(tail);
    }
    input.run_log = Some(log.join("\n"));
    Ok(input)
}
