//! Reading the earlier segments of a continued run back for `on-run-end`.

use tracing::info;

use crate::artifact_store::ArtifactStore;
use crate::error::DbtTemporalError;
use crate::types::ProjectHooksInput;

/// Prepend every earlier segment's results to this one's, so `results` in an
/// `on-run-end` hook describes the whole run.
///
/// Oldest segment first, matching the order `run_results.json` is assembled in.
/// A missing store or an unreadable checkpoint is an error rather than a
/// shorter `results`: a hook that summarises or alerts on the run would
/// silently under-report. A store read failure is transient I/O and retryable
/// (subject to the phase's own retry opt-in); a missing store is configuration.
pub(super) async fn with_prior_segments(
    store: Option<&dyn ArtifactStore>,
    mut input: ProjectHooksInput,
) -> Result<ProjectHooksInput, anyhow::Error> {
    if input.prior_segments.is_empty() {
        return Ok(input);
    }
    let store = store.ok_or_else(|| {
        DbtTemporalError::Configuration(
            "on-run-end for a continued run requires artifact storage to read earlier segments"
                .to_string(),
        )
    })?;

    let mut results = Vec::with_capacity(input.node_results.len());
    for state_ref in &input.prior_segments {
        let (segment_results, _log) =
            crate::activities::segment_state::read_segment_payload(store, state_ref)
                .await
                .map_err(|e| {
                    DbtTemporalError::ArtifactStore(
                        e.context("collecting an earlier segment for on-run-end hooks"),
                    )
                })?;
        results.extend(segment_results);
    }
    info!(
        segments = input.prior_segments.len(),
        results = results.len(),
        "collected earlier segments for on-run-end hooks"
    );

    results.append(&mut input.node_results);
    input.node_results = results;
    Ok(input)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::artifact_store::LocalArtifactStore;
    use crate::types::{
        ExecutionPlan, NodeExecutionResult, NodeStatus, NodeStatusTree, ProjectHookPhase,
        ProjectHooksInput, RunSegmentState,
    };

    fn result(unique_id: &str) -> NodeExecutionResult {
        NodeExecutionResult {
            unique_id: unique_id.to_string(),
            status: NodeStatus::Success,
            execution_time: 0.1,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }
    }

    fn hooks_input(node: &str, prior_segments: Vec<String>) -> ProjectHooksInput {
        ProjectHooksInput {
            phase: ProjectHookPhase::OnRunEnd,
            project: "p".to_string(),
            invocation_id: "inv".to_string(),
            env: BTreeMap::new(),
            target: None,
            node_results: vec![result(node)],
            vars: BTreeMap::new(),
            full_refresh: false,
            retry_on_error: false,
            prior_segments,
            run_started_at: None,
        }
    }

    async fn spill(store: &dyn ArtifactStore, segment: u32, node: &str) -> String {
        let state = RunSegmentState {
            schema_version: 1,
            invocation_id: "inv".to_string(),
            segment,
            plan: ExecutionPlan {
                project: "p".to_string(),
                levels: vec![],
                nodes: BTreeMap::new(),
                manifest_json: None,
                manifest_ref: None,
                invocation_id: "inv".to_string(),
                search_attributes: BTreeMap::new(),
                write_artifacts: true,
                has_on_run_start: false,
                has_on_run_end: true,
                priority_scheduling: false,
                has_project_checks: false,
            },
            prior_segments: vec![],
            all_results: vec![result(node)],
            log_lines: vec![],
            node_status: NodeStatusTree {
                nodes: BTreeMap::new(),
            },
            failed_nodes: vec![],
            had_failure: false,
            effective_env: BTreeMap::new(),
            hook_errors: vec![],
            total_nodes: 3,
            node_counter: 0,
            next_level: 1,
            started_at: None,
        };
        let json = serde_json::to_vec(&state).expect("serialize state");
        store
            .store("inv", &format!("run_segment_state_{segment}.json"), json.into())
            .await
            .expect("store state")
    }

    fn ids(input: &ProjectHooksInput) -> Vec<&str> {
        input
            .node_results
            .iter()
            .map(|r| r.unique_id.as_str())
            .collect()
    }

    /// Hooks see earlier segments oldest first, then this segment's results.
    #[tokio::test]
    async fn earlier_segments_precede_this_segments_results() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalArtifactStore::new(dir.path().to_path_buf());
        let first = spill(&store, 1, "model.first").await;
        let second = spill(&store, 2, "model.second").await;

        let merged =
            with_prior_segments(Some(&store), hooks_input("model.last", vec![first, second]))
                .await
                .expect("merge");

        assert_eq!(ids(&merged), vec!["model.first", "model.second", "model.last"]);
    }

    #[tokio::test]
    async fn a_run_without_earlier_segments_needs_no_store() {
        let merged = with_prior_segments(None, hooks_input("model.only", vec![]))
            .await
            .expect("no segments, nothing to read");
        assert_eq!(ids(&merged), vec!["model.only"]);
    }

    #[tokio::test]
    async fn earlier_segments_without_a_store_is_a_configuration_error() {
        let err = with_prior_segments(None, hooks_input("model.last", vec!["seg.json".into()]))
            .await
            .expect_err("cannot read segments without a store");
        let typed = err.downcast_ref::<DbtTemporalError>().expect("typed error");
        assert!(matches!(typed, DbtTemporalError::Configuration(_)), "{typed}");
        assert!(!typed.is_retryable());
    }

    /// A checkpoint that is not in the store will not appear on a retry, so
    /// the read fails for good, and never yields a silently shorter
    /// `results`.
    #[tokio::test]
    async fn a_missing_segment_is_a_permanent_store_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalArtifactStore::new(dir.path().to_path_buf());
        let missing = dir.path().join("inv/run_segment_state_1.json");

        let err = with_prior_segments(
            Some(&store),
            hooks_input("model.last", vec![missing.to_string_lossy().into_owned()]),
        )
        .await
        .expect_err("a missing checkpoint must be reported");
        let typed = err.downcast_ref::<DbtTemporalError>().expect("typed error");
        assert!(matches!(typed, DbtTemporalError::ArtifactStore(_)), "{typed}");
        assert!(!typed.is_retryable());
        assert_eq!(typed.type_name(), "DbtTemporalError::ArtifactStorePermanent");
        assert!(format!("{typed}").contains("earlier segment"), "{typed}");
    }

    /// An input that omits `prior_segments` deserializes with none.
    #[test]
    fn input_without_prior_segments_deserializes() {
        let input: ProjectHooksInput = serde_json::from_value(serde_json::json!({
            "phase": "on_run_end",
            "project": "p",
            "invocation_id": "inv",
        }))
        .expect("deserialize");
        assert_eq!(input.prior_segments, Vec::<String>::new());
    }
}
