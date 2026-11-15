//! `store_artifacts_inner` end to end against the local artifact store.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::artifact_store::LocalArtifactStore;
use crate::config::{
    RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts, WriteRunLog,
};
use crate::project_registry::ProjectRegistry;
use crate::types::NodeStatus;

use super::test_support::{no_cancellation, sample_result};

/// The plan a checkpoint carries; nothing in these tests reads it.
fn minimal_plan() -> crate::types::ExecutionPlan {
    crate::types::ExecutionPlan {
        project: "p".to_string(),
        nodes: BTreeMap::new(),
        levels: Vec::new(),
        manifest_json: None,
        manifest_ref: None,
        invocation_id: "inv-multi".to_string(),
        search_attributes: BTreeMap::new(),
        write_artifacts: true,
        has_on_run_start: false,
        has_on_run_end: false,
        priority_scheduling: false,
        has_project_checks: false,
    }
}

fn activities_with_local_store(base_dir: std::path::PathBuf, write_run_log: bool) -> DbtActivities {
    DbtActivities {
        registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
        artifact_store: Some(Arc::new(LocalArtifactStore::new(base_dir))),
        search_attr_config: SearchAttributeConfig(BTreeMap::new()),
        registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
        write_run_log: WriteRunLog(write_run_log),
        write_artifacts: WriteArtifacts(true),
        write_catalog: crate::config::WriteCatalog(false),
        priority_scheduling: crate::config::PriorityScheduling(false),
    }
}

/// A run that continued as new left each segment's results in that
/// segment's checkpoint. `run_results.json` has to describe the whole run,
/// so the activity walks the chain rather than reporting only the last leg.
#[tokio::test]
async fn store_artifacts_inner_collects_every_segment() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let activities = activities_with_local_store(dir.path().to_path_buf(), true);
    let store = Arc::clone(
        activities
            .artifact_store
            .as_ref()
            .expect("store configured"),
    );

    // Two earlier segments, each holding its own results and log.
    let mut refs = Vec::new();
    for (segment, node) in [(1u32, "model.first"), (2, "model.second")] {
        let state = crate::types::RunSegmentState {
            schema_version: 1,
            invocation_id: "inv-multi".into(),
            segment,
            plan: minimal_plan(),
            prior_segments: refs.clone(),
            all_results: vec![sample_result(node, NodeStatus::Success, 0.1)],
            log_lines: vec![format!("segment {segment} line")],
            node_status: crate::types::NodeStatusTree {
                nodes: BTreeMap::new(),
            },
            failed_nodes: Vec::new(),
            had_failure: false,
            effective_env: BTreeMap::new(),
            hook_errors: Vec::new(),
            total_nodes: 3,
            node_counter: usize::try_from(segment)?,
            next_level: usize::try_from(segment)?,
            started_at: None,
        };
        let json = serde_json::to_vec(&state)?;
        refs.push(
            store
                .store("inv-multi", &format!("run_segment_state_{segment}.json"), json.into())
                .await?,
        );
    }

    let input = StoreArtifactsInput {
        prior_segments: refs,
        invocation_id: "inv-multi".into(),
        project: None,
        command: None,
        node_results: vec![sample_result("model.last", NodeStatus::Success, 0.1)],
        manifest_json: Some("{}".to_string()),
        manifest_ref: None,
        run_log: Some("final line".to_string()),
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let out = store_artifacts_inner(&activities, input, &no_cancellation().token()).await?;
    let run_results = std::fs::read_to_string(&out.run_results_path)?;
    let parsed: serde_json::Value = serde_json::from_str(&run_results)?;
    let ids: Vec<&str> = parsed["results"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("results is array"))?
        .iter()
        .filter_map(|r| r["unique_id"].as_str())
        .collect();
    assert_eq!(ids, vec!["model.first", "model.second", "model.last"], "in run order");

    let log = std::fs::read_to_string(out.log_path.as_ref().expect("log written"))?;
    assert_eq!(log, "segment 1 line\nsegment 2 line\nfinal line");
    Ok(())
}

/// A checkpoint that cannot be read is fatal: `run_results.json` without an
/// earlier segment's nodes describes the run as smaller than it was, and
/// nothing downstream could tell.
#[tokio::test]
async fn store_artifacts_inner_refuses_a_missing_segment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let activities = activities_with_local_store(dir.path().to_path_buf(), false);

    let input = StoreArtifactsInput {
        prior_segments: vec![
            dir.path()
                .join("inv-multi/run_segment_state_1.json")
                .to_string_lossy()
                .into_owned(),
        ],
        invocation_id: "inv-multi".into(),
        project: None,
        command: None,
        node_results: vec![sample_result("model.last", NodeStatus::Success, 0.1)],
        manifest_json: Some("{}".to_string()),
        manifest_ref: None,
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let err = store_artifacts_inner(&activities, input, &no_cancellation().token())
        .await
        .expect_err("a missing checkpoint must be reported");
    assert!(
        format!("{err:#}").contains("collecting an earlier segment"),
        "unexpected error: {err:#}"
    );
}

/// A checkpoint that does not exist will not appear on a later attempt, so
/// the failure must reach Temporal as non-retryable under its own type.
#[tokio::test]
async fn a_missing_checkpoint_is_not_retried() {
    let dir = tempfile::tempdir().expect("tempdir");
    let activities = activities_with_local_store(dir.path().to_path_buf(), false);
    let input = StoreArtifactsInput {
        prior_segments: vec!["inv-multi/run_segment_state_1.json".to_string()],
        invocation_id: "inv-multi".into(),
        project: None,
        command: None,
        node_results: Vec::new(),
        manifest_json: Some("{}".to_string()),
        manifest_ref: None,
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let err = store_artifacts_inner(&activities, input, &no_cancellation().token())
        .await
        .expect_err("a missing checkpoint must be reported");

    let classified = retry::downcast_or_default(err, retry::Unclassified::Permanent);
    assert!(!classified.is_retryable(), "{classified}");
    assert_eq!(classified.type_name(), "DbtTemporalError::ArtifactStorePermanent");
}

#[tokio::test]
async fn store_artifacts_inner_writes_run_results_and_inline_manifest() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let activities = activities_with_local_store(dir.path().to_path_buf(), false);

    let input = StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-1".into(),
        project: None,
        command: None,
        node_results: vec![sample_result("model.a", NodeStatus::Success, 0.1)],
        manifest_json: Some("{\"manifest\":\"yes\"}".to_string()),
        manifest_ref: None,
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let out = store_artifacts_inner(&activities, input, &no_cancellation().token()).await?;
    assert!(out.run_results_path.contains("run_results.json"));
    assert!(out.manifest_path.contains("manifest.json"));
    assert!(out.log_path.is_none());

    let on_disk = std::fs::read_to_string(&out.run_results_path)?;
    assert!(on_disk.contains("model.a"));
    let manifest = std::fs::read_to_string(&out.manifest_path)?;
    assert!(manifest.contains("manifest"));
    Ok(())
}

#[tokio::test]
async fn store_artifacts_inner_uses_existing_manifest_ref() -> anyhow::Result<()> {
    // When the plan phase already stored a large manifest at a known path,
    // the activity should pass that path through verbatim instead of
    // re-storing it.
    let dir = tempfile::tempdir()?;
    let activities = activities_with_local_store(dir.path().to_path_buf(), false);

    let input = StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-2".into(),
        project: None,
        command: None,
        node_results: vec![],
        manifest_json: None,
        manifest_ref: Some("/already/stored/manifest.json".to_string()),
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let out = store_artifacts_inner(&activities, input, &no_cancellation().token()).await?;
    assert_eq!(out.manifest_path, "/already/stored/manifest.json");
    Ok(())
}

#[tokio::test]
async fn store_artifacts_inner_errors_when_neither_manifest_provided() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let activities = activities_with_local_store(dir.path().to_path_buf(), false);

    let input = StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-3".into(),
        project: None,
        command: None,
        node_results: vec![],
        manifest_json: None,
        manifest_ref: None,
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let err = store_artifacts_inner(&activities, input, &no_cancellation().token())
        .await
        .expect_err("absent manifest must error");
    assert!(err.to_string().contains("manifest"), "got: {err}");
    Ok(())
}

#[tokio::test]
async fn store_artifacts_inner_writes_log_when_enabled() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let activities = activities_with_local_store(dir.path().to_path_buf(), true);

    let input = StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-log".into(),
        project: None,
        command: None,
        node_results: vec![],
        manifest_json: Some("{}".to_string()),
        manifest_ref: None,
        started_at: None,
        elapsed_time: 0.0,
        env: BTreeMap::new(),
        target: None,
        run_log: Some("line a\nline b".to_string()),
    };

    let out = store_artifacts_inner(&activities, input, &no_cancellation().token()).await?;
    let log_path = out
        .log_path
        .expect("log_path should be set when run_log is on");
    assert!(log_path.ends_with("log.txt"));
    let on_disk = std::fs::read_to_string(&log_path)?;
    assert_eq!(on_disk, "line a\nline b");
    Ok(())
}

#[tokio::test]
async fn store_artifacts_inner_skips_log_when_run_log_disabled() -> anyhow::Result<()> {
    // Run log was supplied but the writer is disabled — the file must not
    // be written and log_path stays None.
    let dir = tempfile::tempdir()?;
    let activities = activities_with_local_store(dir.path().to_path_buf(), false);

    let input = StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-skiplog".into(),
        project: None,
        command: None,
        node_results: vec![],
        manifest_json: Some("{}".to_string()),
        manifest_ref: None,
        started_at: None,
        elapsed_time: 0.0,
        env: BTreeMap::new(),
        target: None,
        run_log: Some("would-be-log".to_string()),
    };

    let out = store_artifacts_inner(&activities, input, &no_cancellation().token()).await?;
    assert!(out.log_path.is_none());
    Ok(())
}

#[tokio::test]
async fn store_artifacts_inner_errors_when_no_artifact_store_configured() -> anyhow::Result<()> {
    // The activity guards against being called on an unconfigured worker.
    let activities = DbtActivities {
        registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
        artifact_store: None,
        search_attr_config: SearchAttributeConfig(BTreeMap::new()),
        registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
        write_run_log: WriteRunLog(false),
        write_artifacts: WriteArtifacts(true),
        write_catalog: crate::config::WriteCatalog(false),
        priority_scheduling: crate::config::PriorityScheduling(false),
    };

    let input = StoreArtifactsInput {
        prior_segments: Vec::new(),
        invocation_id: "inv-noop".into(),
        project: None,
        command: None,
        node_results: vec![],
        manifest_json: Some("{}".to_string()),
        manifest_ref: None,
        run_log: None,
        env: BTreeMap::new(),
        target: None,
        started_at: None,
        elapsed_time: 0.0,
    };

    let err = store_artifacts_inner(&activities, input, &no_cancellation().token())
        .await
        .expect_err("missing ArtifactStore must error");
    assert!(err.to_string().contains("ArtifactStore not configured"));
    // A misconfigured worker is permanent — retrying cannot fix it.
    assert!(
        !retry::downcast_or_default(err, retry::Unclassified::Permanent).is_retryable(),
        "missing store config must not retry"
    );
    Ok(())
}

/// The run is already finished by the time this activity runs, so losing it
/// to a transient object-store 5xx would discard completed work.
#[tokio::test]
async fn store_io_failure_is_classified_retryable() {
    let err = store_io_error("storing run_results.json", anyhow::anyhow!("503 slow down"));

    let classified = retry::downcast_or_default(err, retry::Unclassified::Permanent);
    assert!(
        classified.is_retryable(),
        "artifact-store I/O must retry even under the activity's permanent default"
    );
    let msg = classified.to_string();
    assert!(msg.contains("storing run_results.json"), "context lost: {msg}");
    assert!(msg.contains("503 slow down"), "cause lost: {msg}");
}
