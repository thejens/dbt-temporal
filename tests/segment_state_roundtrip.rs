//! Continue-as-new state handover.
//!
//! Everything a run has accumulated has to survive the jump to a fresh
//! execution: the plan (so the successor never re-plans and picks up a
//! different node set), the results so far, the progress log, per-node status,
//! the failed set that drives downstream skipping, and the post-hook env.
//!
//! These drive the real activities against a real (local) artifact store, so a
//! field added to `RunSegmentState` without a serde path is caught here rather
//! than mid-run on a large project.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_temporal::activities::DbtActivities;
use dbt_temporal::activities::segment_state::{load_segment_state_inner, save_segment_state_inner};
use dbt_temporal::artifact_store::LocalArtifactStore;
use dbt_temporal::config::{
    PriorityScheduling, RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts,
    WriteCatalog, WriteRunLog,
};
use dbt_temporal::project_registry::ProjectRegistry;
use dbt_temporal::types::{
    ExecutionPlan, HookError, LoadSegmentStateInput, NodeExecutionResult, NodeStatus,
    NodeStatusTree, RunSegmentState, SaveSegmentStateInput,
};

fn activities(store_dir: &std::path::Path) -> DbtActivities {
    DbtActivities {
        registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
        artifact_store: Some(Arc::new(LocalArtifactStore::new(store_dir.to_path_buf()))),
        search_attr_config: SearchAttributeConfig(BTreeMap::new()),
        registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
        write_run_log: WriteRunLog(false),
        write_artifacts: WriteArtifacts(true),
        write_catalog: WriteCatalog(false),
        priority_scheduling: PriorityScheduling(false),
    }
}

fn identity(next_level: usize) -> dbt_temporal::types::SegmentIdentity {
    dbt_temporal::types::SegmentIdentity {
        invocation_id: "inv-continued".to_string(),
        segment: 1,
        next_level,
    }
}

async fn save(activities: &DbtActivities, state: RunSegmentState) -> String {
    save_segment_state_inner(
        activities,
        SaveSegmentStateInput {
            invocation_id: "inv-continued".to_string(),
            state,
        },
    )
    .await
    .expect("spilling state should succeed")
}

fn sample_state() -> RunSegmentState {
    let mut nodes = BTreeMap::new();
    nodes.insert("model.p.done".to_string(), NodeStatus::Success);
    nodes.insert("model.p.broke".to_string(), NodeStatus::Error);

    RunSegmentState {
        schema_version: 1,
        started_at: None,
        invocation_id: "inv-continued".to_string(),
        segment: 1,
        plan: ExecutionPlan {
            project: "p".to_string(),
            levels: vec![
                vec!["model.p.done".to_string()],
                vec!["model.p.broke".to_string()],
                vec!["model.p.pending".to_string()],
            ],
            nodes: BTreeMap::new(),
            manifest_json: None,
            manifest_ref: None,
            invocation_id: "inv-continued".to_string(),
            search_attributes: BTreeMap::new(),
            write_artifacts: true,
            has_on_run_start: true,
            has_on_run_end: true,
            priority_scheduling: false,
            has_project_checks: true,
        },
        all_results: vec![NodeExecutionResult {
            unique_id: "model.p.done".to_string(),
            status: NodeStatus::Success,
            execution_time: 1.5,
            message: Some("ok".to_string()),
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }],
        log_lines: vec!["1 of 3 START model.p.done".to_string()],
        node_status: NodeStatusTree { nodes },
        failed_nodes: vec!["model.p.broke".to_string()],
        had_failure: true,
        effective_env: BTreeMap::from([("EXTRA".to_string(), "from-pre-run-hook".to_string())]),
        hook_errors: vec![HookError {
            hook_workflow_type: "notify".to_string(),
            event: "pre_run".to_string(),
            error: "flaky".to_string(),
        }],
        total_nodes: 3,
        node_counter: 2,
        next_level: 2,
    }
}

#[tokio::test]
async fn segment_state_survives_the_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let activities = activities(dir.path());
    let original = sample_state();

    let state_ref = save_segment_state_inner(
        &activities,
        SaveSegmentStateInput {
            invocation_id: "inv-continued".to_string(),
            state: original.clone(),
        },
    )
    .await
    .expect("spilling state should succeed");

    let restored = load_segment_state_inner(
        &activities,
        LoadSegmentStateInput {
            state_ref,
            expected: Some(identity(2)),
        },
    )
    .await
    .expect("restoring state should succeed");

    // The successor resumes from here, so each of these changes what it does.
    assert_eq!(restored.next_level, 2, "resumes after the completed levels");
    assert_eq!(restored.plan.levels.len(), 3, "plan carried, not re-planned");
    assert_eq!(restored.plan.invocation_id, "inv-continued");
    assert_eq!(restored.all_results.len(), 1);
    assert_eq!(restored.all_results[0].unique_id, "model.p.done");
    assert_eq!(restored.log_lines, original.log_lines);
    assert_eq!(restored.node_status.nodes.len(), 2);
    assert!(restored.had_failure, "failure state must not reset");
    assert_eq!(
        restored.failed_nodes,
        vec!["model.p.broke".to_string()],
        "downstream skipping depends on this"
    );
    assert_eq!(
        restored.effective_env.get("EXTRA").map(String::as_str),
        Some("from-pre-run-hook"),
        "pre_run hooks do not re-run, so their extra_env has to survive"
    );
    assert_eq!(restored.hook_errors.len(), 1, "hook errors accumulate across segments");
    assert_eq!(restored.total_nodes, 3, "progress numbering stays continuous");
    assert_eq!(restored.node_counter, 2);
}

/// Continuation is impossible without somewhere to spill state, and the error
/// says so rather than surfacing as a serialization failure.
#[tokio::test]
async fn spilling_without_an_artifact_store_is_rejected_clearly() {
    let mut activities = activities(std::path::Path::new("/unused"));
    activities.artifact_store = None;

    let err = save_segment_state_inner(
        &activities,
        SaveSegmentStateInput {
            invocation_id: "inv".to_string(),
            state: sample_state(),
        },
    )
    .await
    .expect_err("no store means no continuation");

    let msg = format!("{err:#}");
    assert!(msg.contains("artifact storage"), "should name the cause: {msg}");
}

/// Each segment writes its own file. A single reused name made the checkpoint
/// mutable: a delayed or retried write from an earlier segment would land on
/// top of the live one, and the successor would resume from the wrong point in
/// the run with a payload that deserializes perfectly well.
#[tokio::test]
async fn each_segment_writes_its_own_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let activities = activities(dir.path());

    let mut first_state = sample_state();
    first_state.segment = 1;
    first_state.next_level = 1;
    let first = save(&activities, first_state).await;

    let mut second_state = sample_state();
    second_state.segment = 2;
    let second = save(&activities, second_state).await;

    assert_ne!(first, second, "a later segment must not overwrite an earlier one");

    // Re-spilling segment 1 late must not disturb what segment 2 wrote.
    let mut replay = sample_state();
    replay.segment = 1;
    replay.next_level = 1;
    assert_eq!(save(&activities, replay).await, first);

    let mut expected = identity(2);
    expected.segment = 2;
    let restored = load_segment_state_inner(
        &activities,
        LoadSegmentStateInput {
            state_ref: second,
            expected: Some(expected),
        },
    )
    .await
    .unwrap();
    assert_eq!(restored.next_level, 2, "segment 2's checkpoint is intact");
}

/// A resumable run reads its checkpoint reference from workflow input, so the
/// reference is not guaranteed to name the state this execution left behind. A
/// mismatched checkpoint deserializes fine and would silently resume the run
/// from another point in its DAG.
#[tokio::test]
async fn a_checkpoint_from_another_run_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let activities = activities(dir.path());
    let state_ref = save(&activities, sample_state()).await;

    let mut expected = identity(2);
    expected.invocation_id = "inv-somebody-else".to_string();

    let err = load_segment_state_inner(
        &activities,
        LoadSegmentStateInput {
            state_ref,
            expected: Some(expected),
        },
    )
    .await
    .expect_err("a checkpoint from another run must be refused");
    let msg = format!("{err:#}");
    assert!(msg.contains("belongs to run"), "should name the mismatch: {msg}");
}

#[tokio::test]
async fn a_checkpoint_from_another_segment_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let activities = activities(dir.path());
    let state_ref = save(&activities, sample_state()).await;

    let mut expected = identity(2);
    expected.segment = 7;

    let err = load_segment_state_inner(
        &activities,
        LoadSegmentStateInput {
            state_ref,
            expected: Some(expected),
        },
    )
    .await
    .expect_err("a checkpoint from another segment must be refused");
    let msg = format!("{err:#}");
    assert!(msg.contains("segment"), "should name the mismatch: {msg}");
}

/// The cursor check works on a checkpoint written before checkpoints carried
/// identity at all, because `next_level` always existed.
#[tokio::test]
async fn a_checkpoint_resuming_at_the_wrong_level_is_refused_even_without_identity() {
    let dir = tempfile::tempdir().unwrap();
    let activities = activities(dir.path());

    let mut legacy = sample_state();
    legacy.schema_version = 0;
    legacy.invocation_id = String::new();
    legacy.segment = 0;
    let state_ref = save(&activities, legacy).await;

    let mut expected = identity(9);
    expected.invocation_id = "anything".to_string();

    let err = load_segment_state_inner(
        &activities,
        LoadSegmentStateInput {
            state_ref: state_ref.clone(),
            expected: Some(expected),
        },
    )
    .await
    .expect_err("a checkpoint at the wrong level must be refused");
    assert!(format!("{err:#}").contains("level"), "{err:#}");

    // …and with the cursor agreeing, a run already in flight when identity
    // shipped still resumes, rather than failing on fields it never wrote.
    let mut expected = identity(2);
    expected.invocation_id = "anything".to_string();
    expected.segment = 42;
    let restored = load_segment_state_inner(
        &activities,
        LoadSegmentStateInput {
            state_ref,
            expected: Some(expected),
        },
    )
    .await
    .expect("a pre-identity checkpoint must still load");
    assert_eq!(restored.next_level, 2);
}
