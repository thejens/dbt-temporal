//! Fixtures shared by the artifact tests.

use std::collections::BTreeMap;

use crate::types::{
    FreshnessGrade, FreshnessOutcome, NodeExecutionResult, NodeStatus, StoreArtifactsInput,
};

/// A cancellation source nothing cancels, for tests that do not exercise
/// cancellation. Callers take its token within one statement, so the
/// source outlives the call as `store_artifacts_inner` requires.
pub(super) fn no_cancellation() -> dbt_common::cancellation::CancellationTokenSource {
    dbt_common::cancellation::CancellationTokenSource::new()
}

pub(super) fn sample_result(unique_id: &str, status: NodeStatus, time: f64) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: unique_id.into(),
        status,
        execution_time: time,
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

pub(super) fn with_freshness(
    unique_id: &str,
    resource_type: &str,
    status: FreshnessGrade,
) -> NodeExecutionResult {
    let mut result = sample_result(unique_id, NodeStatus::Success, 0.4);
    result.freshness = Some(FreshnessOutcome {
        max_loaded_at: "2026-06-12T10:00:00+00:00".into(),
        snapshotted_at: "2026-06-12T11:00:00+00:00".into(),
        max_loaded_at_time_ago_in_s: 3600.0,
        status,
        resource_type: resource_type.into(),
        criteria: dbt_schemas::schemas::common::FreshnessDefinition::default(),
    });
    result
}

pub(super) fn freshness_input(
    command: &str,
    node_results: Vec<NodeExecutionResult>,
) -> StoreArtifactsInput {
    StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-9".into(),
        project: None,
        command: Some(command.into()),
        node_results,
        manifest_json: None,
        manifest_ref: None,
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    }
}
