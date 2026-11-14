//! The real `dbt_run` workflow against an embedded DuckDB warehouse and a
//! local Temporal dev server — no Docker.
//!
//! Covers what only a live server can: that nodes are scheduled by readiness
//! rather than by level, and that a run which outgrows its history hands over
//! to a successor mid-DAG and still finishes every node. The Postgres-backed
//! `waffle_hut` suite covers the same workflow against a heavier warehouse.
//!
//! The server binary comes from the SDK's cached download, as in the
//! `waffle_hut` suite; set `DBT_TEMPORAL_TEST_TEMPORAL_BIN` to use an installed
//! `temporal` CLI instead.

use std::collections::BTreeMap;
use std::future::Future;

use anyhow::{Context, Result};
use temporalio_client::errors::WorkflowGetResultError;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, UntypedWorkflow,
    WorkflowFetchHistoryOptions, WorkflowGetResultOptions, WorkflowHandle, WorkflowStartOptions,
};
use temporalio_client::{UntypedQuery, WorkflowCancelOptions, WorkflowQueryOptions};
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralServer, TemporalDevServerConfig, default_cached_download,
};

use dbt_temporal::config::{DbtTemporalConfig, TemporalMetricsConfig, WorkerTuningConfig};
use dbt_temporal::types::{DbtRunInput, DbtRunOutput, RunStatusSnapshot};

/// History length at which the dev server suggests continue-as-new. Low enough
/// that a run of a hundred-odd nodes crosses it.
const SUGGEST_CONTINUE_AFTER_EVENTS: u32 = 256;

const PROJECT: &str = "spike";

fn temporal_exe() -> EphemeralExe {
    std::env::var("DBT_TEMPORAL_TEST_TEMPORAL_BIN")
        .map_or_else(|_| default_cached_download(), EphemeralExe::ExistingPath)
}

async fn start_server() -> Result<EphemeralServer> {
    let mut args: Vec<String> = [
        "DbtProject",
        "DbtCommand",
        "DbtTarget",
        "DbtSelector",
        "DbtStatus",
        "env",
    ]
    .iter()
    .flat_map(|name| ["--search-attribute".to_string(), format!("{name}=Keyword")])
    .collect();
    args.push("--dynamic-config-value".to_string());
    args.push(format!(
        "limit.historyCount.suggestContinueAsNew={SUGGEST_CONTINUE_AFTER_EVENTS}"
    ));
    TemporalDevServerConfig::builder()
        .exe(temporal_exe())
        .namespace("default")
        .extra_args(args)
        .build()
        .start_server()
        .await
        .context("starting temporal dev server")
}

/// A DuckDB project whose models are `(name, sql)`, and the profile for it.
struct Project {
    dir: tempfile::TempDir,
    profiles: tempfile::TempDir,
    artifacts: tempfile::TempDir,
}

fn write_project(models: &[(String, String)]) -> Result<Project> {
    let dir = tempfile::TempDir::new()?;
    let profiles = tempfile::TempDir::new()?;
    let artifacts = tempfile::TempDir::new()?;
    std::fs::write(
        dir.path().join("dbt_project.yml"),
        format!("name: {PROJECT}\nversion: \"1.0.0\"\nprofile: {PROJECT}\n"),
    )?;
    std::fs::create_dir_all(dir.path().join("models"))?;
    for (name, sql) in models {
        std::fs::write(dir.path().join("models").join(format!("{name}.sql")), sql)?;
    }
    let db = artifacts.path().join("warehouse.duckdb");
    std::fs::write(
        profiles.path().join("profiles.yml"),
        format!(
            "{PROJECT}:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      \
             path: \"{}\"\n      schema: main\n      threads: 1\n",
            db.display()
        ),
    )?;
    Ok(Project {
        dir,
        profiles,
        artifacts,
    })
}

fn config(project: &Project, temporal_addr: &str, concurrency: u32) -> DbtTemporalConfig {
    DbtTemporalConfig {
        temporal_address: format!("http://{temporal_addr}"),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("duckdb-workflow-{}", uuid::Uuid::new_v4()),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: vec![project.dir.path().to_string_lossy().to_string()],
        dbt_profiles_dir: Some(project.profiles.path().to_string_lossy().to_string()),
        dbt_target: None,
        health_file: None,
        health_port: None,
        write_artifacts: true,
        write_catalog: false,
        artifact_store: project.artifacts.path().to_string_lossy().to_string(),
        search_attributes: BTreeMap::new(),
        write_run_log: true,
        worker_tuning: WorkerTuningConfig::Fixed {
            max_concurrent_workflow_tasks: 50,
            max_concurrent_activities: concurrency as usize,
            max_concurrent_local_activities: 4,
        },
        sticky_queue_timeout_secs: 10,
        nonsticky_to_sticky_poll_ratio: 0.2,
        max_worker_activities_per_second: None,
        max_task_queue_activities_per_second: None,
        graceful_shutdown_secs: None,
        max_cached_workflows: 1000,
        deployment_name: None,
        poller_autoscaling: None,
        temporal_metrics: TemporalMetricsConfig::None,
        priority_scheduling: false,
        nexus_enabled: false,
    }
}

fn run_input(fail_fast: bool) -> DbtRunInput {
    DbtRunInput {
        project: None,
        indirect_selection: None,
        resource_types: Vec::new(),
        exclude_resource_types: Vec::new(),
        resume_from: None,
        command: "run".to_string(),
        select: None,
        exclude: None,
        vars: BTreeMap::default(),
        target: None,
        full_refresh: false,
        fail_fast,
        hooks: None,
        env: BTreeMap::default(),
        defer_manifest_ref: None,
        event_time_start: None,
        event_time_end: None,
        retry_from: None,
        state_manifest_ref: None,
    }
}

/// Start a dev server and a worker for `project`, then run `body` against a
/// client; returns what `body` returns once everything is torn down.
///
/// Synchronous on purpose: the worker and its `LocalSet` are not `Send`, so the
/// whole scenario lives on one current-thread runtime owned by this function.
fn scenario<T, F, Fut>(project: &Project, concurrency: u32, body: F) -> Result<T>
where
    F: FnOnce(Client, String) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    dbt_temporal::tracing_setup::init_for_tests();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let server = start_server().await?;
        let config = config(project, &server.target, concurrency);
        let mut worker = dbt_temporal::worker::build_worker(&config)
            .await
            .context("building worker")?;
        let task_queue = config.temporal_task_queue.clone();

        let local = tokio::task::LocalSet::new();
        let abort = std::sync::Arc::new(tokio::sync::Notify::new());
        let abort_rx = std::sync::Arc::clone(&abort);
        let _worker_task = local.spawn_local(async move {
            tokio::select! {
                result = worker.run() => result,
                () = abort_rx.notified() => Ok(()),
            }
        });

        let result = local
            .run_until(async {
                let url = url::Url::parse(&format!("http://{}", server.target))?;
                let connection = Connection::connect(ConnectionOptions::new(url).build()).await?;
                let client = Client::new(connection, ClientOptions::new("default").build())
                    .map_err(|e| anyhow::anyhow!("creating client: {e}"))?;
                body(client, task_queue).await
            })
            .await;

        abort.notify_one();
        result
    })
}

async fn start(
    client: &Client,
    task_queue: &str,
    fail_fast: bool,
) -> Result<WorkflowHandle<Client, UntypedWorkflow>> {
    client
        .start_workflow(
            UntypedWorkflow::new("dbt_run"),
            RawValue::new(vec![run_input(fail_fast).as_json_payload()?]),
            WorkflowStartOptions::new(task_queue, format!("e2e-{}", uuid::Uuid::new_v4())).build(),
        )
        .await
        .context("starting workflow")
}

struct Finished {
    output: DbtRunOutput,
    /// `(event id, what happened)` for each activity scheduled or completed in
    /// the first execution's history, named by the activity's label.
    activity_events: Vec<(i64, String)>,
}

/// Run one workflow to completion and report its output and activity events.
fn run_workflow(project: &Project, concurrency: u32) -> Result<Finished> {
    scenario(project, concurrency, |client, task_queue| async move {
        let handle = start(&client, &task_queue, false).await?;
        let raw = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .context("waiting for the workflow")?;
        let payload = raw
            .payloads
            .first()
            .ok_or_else(|| anyhow::anyhow!("no result payload"))?;
        let output: DbtRunOutput = serde_json::from_slice(&payload.data)?;

        // The handle follows the run id it started, so this is the first
        // execution's history.
        let events = handle
            .fetch_history(WorkflowFetchHistoryOptions::default())
            .into_events()
            .await?;
        let mut scheduled: BTreeMap<i64, String> = BTreeMap::new();
        let mut activity_events = Vec::new();
        for event in events {
            match event.attributes {
                Some(Attributes::ActivityTaskScheduledEventAttributes(a)) => {
                    scheduled.insert(event.event_id, a.activity_id.clone());
                    activity_events.push((event.event_id, format!("scheduled {}", a.activity_id)));
                }
                Some(Attributes::ActivityTaskCompletedEventAttributes(a)) => {
                    let name = scheduled
                        .get(&a.scheduled_event_id)
                        .cloned()
                        .unwrap_or_default();
                    activity_events.push((event.event_id, format!("completed {name}")));
                }
                _ => {}
            }
        }
        Ok(Finished {
            output,
            activity_events,
        })
    })
}

fn event_id(events: &[(i64, String)], what: &str) -> Result<i64> {
    events
        .iter()
        .find(|(_, e)| e == what)
        .map(|(id, _)| *id)
        .ok_or_else(|| anyhow::anyhow!("no `{what}` event in {events:?}"))
}

/// `fast_grand` depends only on `fast` and `fast_child`, so it must start while
/// `slow` — which shares a level with `fast` — is still running. Scheduling by
/// level would hold it until `slow` finished.
#[test]
fn a_slow_node_does_not_hold_back_unrelated_dependents() -> Result<()> {
    // Heavy enough to outlast two quick models on any machine.
    let slow = "{{ config(materialized='table') }}\n\
                select sum(a.range * b.range) as total \
                from range(40000) as a, range(40000) as b";
    let models = vec![
        ("slow".to_string(), slow.to_string()),
        ("fast".to_string(), "select 1 as id".to_string()),
        ("fast_child".to_string(), "select id from {{ ref('fast') }}".to_string()),
        ("fast_grand".to_string(), "select id from {{ ref('fast_child') }}".to_string()),
        ("slow_child".to_string(), "select total from {{ ref('slow') }}".to_string()),
    ];
    let project = write_project(&models)?;

    let finished = run_workflow(&project, 4)?;

    assert!(finished.output.success);
    assert_eq!(finished.output.node_results.len(), 5);
    let events = &finished.activity_events;
    let grand_started = event_id(events, "scheduled model:fast_grand")?;
    let slow_done = event_id(events, "completed model:slow")?;
    assert!(
        grand_started < slow_done,
        "fast_grand (event {grand_started}) waited for slow (event {slow_done}): {events:?}"
    );
    let child_started = event_id(events, "scheduled model:slow_child")?;
    assert!(child_started > slow_done, "slow_child must wait for slow");
    Ok(())
}

/// A run large enough to cross the server's continue-as-new threshold hands
/// over mid-DAG: a wide level plus a long chain, so the successor resumes with
/// some nodes finished, some running to completion in the drain, and the chain
/// still ahead.
#[test]
fn a_run_continues_as_new_mid_dag_and_finishes_every_node() -> Result<()> {
    let wide = 90;
    let chain = 30;
    let mut models: Vec<(String, String)> = (0..wide)
        .map(|i| (format!("w{i:03}"), format!("select {i} as id")))
        .collect();
    models.push(("c000".to_string(), "select 0 as id".to_string()));
    for i in 1..chain {
        models.push((format!("c{i:03}"), format!("select id from {{{{ ref('c{:03}') }}}}", i - 1)));
    }
    let expected = wide + chain;
    let project = write_project(&models)?;

    let finished = run_workflow(&project, 8)?;
    let output = finished.output;

    assert!(output.success);
    assert!(output.node_results_partial, "the run should have handed over at least once");
    let artifacts = output
        .artifacts
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("artifacts should be present"))?;
    let run_results: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&artifacts.run_results_path)?)?;
    let results = run_results["results"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("results should be an array"))?;
    let mut ids: Vec<&str> = results
        .iter()
        .filter_map(|r| r["unique_id"].as_str())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), expected, "every node exactly once across segments");
    assert!(results.iter().all(|r| r["status"] == "success"));
    Ok(())
}

/// A failed node skips exactly its transitive dependents; unrelated work still
/// runs; and the failed run's `run_status` — served by replaying the history the
/// ready queue recorded — reports the same counts.
#[test]
fn a_failure_skips_only_its_dependents() -> Result<()> {
    let models = vec![
        ("bad".to_string(), "select from from".to_string()),
        ("bad_child".to_string(), "select * from {{ ref('bad') }}".to_string()),
        ("bad_grand".to_string(), "select * from {{ ref('bad_child') }}".to_string()),
        ("ok_a".to_string(), "select 1 as id".to_string()),
        ("ok_b".to_string(), "select id from {{ ref('ok_a') }}".to_string()),
    ];
    let project = write_project(&models)?;

    let status = scenario(&project, 4, |client, task_queue| async move {
        let handle = start(&client, &task_queue, false).await?;
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        assert!(
            matches!(result, Err(WorkflowGetResultError::Failed(_))),
            "the run should fail: {result:?}"
        );
        let raw = handle
            .query(
                UntypedQuery::new("run_status"),
                RawValue::new(vec![]),
                WorkflowQueryOptions::default(),
            )
            .await
            .context("querying run_status")?;
        let payload = raw
            .payloads
            .first()
            .ok_or_else(|| anyhow::anyhow!("no query payload"))?;
        serde_json::from_slice::<RunStatusSnapshot>(&payload.data).map_err(Into::into)
    })?;

    assert_eq!(
        (status.succeeded, status.failed, status.skipped, status.running),
        (2, 1, 2, 0),
        "{status:?}"
    );
    Ok(())
}

/// Cancelling the workflow while nodes run ends it as cancelled rather than
/// waiting for the nodes or hanging.
#[test]
fn cancelling_a_running_workflow_ends_it_as_cancelled() -> Result<()> {
    let slow = "{{ config(materialized='table') }}\n\
                select sum(a.range * b.range) as total \
                from range(120000) as a, range(120000) as b";
    let models = vec![
        ("slow_a".to_string(), slow.to_string()),
        ("after".to_string(), "select total from {{ ref('slow_a') }}".to_string()),
    ];
    let project = write_project(&models)?;

    scenario(&project, 4, |client, task_queue| async move {
        let handle = start(&client, &task_queue, false).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        handle.cancel(WorkflowCancelOptions::default()).await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_mins(1),
            handle.get_result(WorkflowGetResultOptions::default()),
        )
        .await
        .context("the cancelled workflow did not end")?;
        assert!(
            matches!(result, Err(WorkflowGetResultError::Cancelled { .. })),
            "expected cancellation: {result:?}"
        );
        Ok(())
    })
}
