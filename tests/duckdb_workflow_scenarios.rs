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

use dbt_temporal::config::{
    DbtTemporalConfig, ExternalPayloadStorageConfig, TemporalMetricsConfig, WorkerTuningConfig,
};
use dbt_temporal::types::{
    DbtRunInput, DbtRunOutput, HookConfig, HookErrorMode, HooksConfig, RunStatusSnapshot,
};

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
    /// Whether the worker writes `catalog.json` with the other artifacts.
    write_catalog: bool,
}

fn write_project(models: &[(String, String)]) -> Result<Project> {
    write_project_with("", models)
}

/// Like [`write_project`], with `extra_project_yml` appended to `dbt_project.yml`
/// (for settings such as `on-run-start`).
fn write_project_with(extra_project_yml: &str, models: &[(String, String)]) -> Result<Project> {
    let dir = tempfile::TempDir::new()?;
    let profiles = tempfile::TempDir::new()?;
    let artifacts = tempfile::TempDir::new()?;
    std::fs::write(
        dir.path().join("dbt_project.yml"),
        format!("name: {PROJECT}\nversion: \"1.0.0\"\nprofile: {PROJECT}\n{extra_project_yml}\n"),
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
        write_catalog: false,
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
        write_catalog: project.write_catalog,
        artifact_store: project.artifacts.path().to_string_lossy().to_string(),
        override_policy: dbt_temporal::config::OverridePolicy::default(),
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
        external_payload_storage: None,
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
    scenario_with(project, concurrency, None, body)
}

/// [`scenario`] with external payload storage configured on the worker. The
/// client gets the same data converter, as any starter reading results must.
fn scenario_with<T, F, Fut>(
    project: &Project,
    concurrency: u32,
    external_payload_storage: Option<ExternalPayloadStorageConfig>,
    body: F,
) -> Result<T>
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
        let mut config = config(project, &server.target, concurrency);
        config.external_payload_storage = external_payload_storage;
        let data_converter = dbt_temporal::worker::temporal::build_data_converter(&config).await?;
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
                let client = Client::new(
                    connection,
                    ClientOptions::new("default")
                        .data_converter(data_converter)
                        .build(),
                )
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
    start_with(client, task_queue, run_input(fail_fast)).await
}

async fn start_with(
    client: &Client,
    task_queue: &str,
    input: DbtRunInput,
) -> Result<WorkflowHandle<Client, UntypedWorkflow>> {
    client
        .start_workflow(
            UntypedWorkflow::new("dbt_run"),
            RawValue::new(vec![input.as_json_payload()?]),
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
    let grand_started = event_id(events, "scheduled model.spike.fast_grand")?;
    let slow_done = event_id(events, "completed model.spike.slow")?;
    assert!(
        grand_started < slow_done,
        "fast_grand (event {grand_started}) waited for slow (event {slow_done}): {events:?}"
    );
    let child_started = event_id(events, "scheduled model.spike.slow_child")?;
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

/// A failed node skips exactly its transitive dependents, even through a model
/// that tolerates its own errors with `on_error: continue`; unrelated work still
/// runs; and the failed run's `run_status` — served by replaying the history the
/// ready queue recorded — reports the same counts.
#[test]
fn a_failure_skips_only_its_dependents() -> Result<()> {
    let models = vec![
        ("bad".to_string(), "select from from".to_string()),
        (
            "bad_child".to_string(),
            "{{ config(on_error='continue') }}\nselect * from {{ ref('bad') }}".to_string(),
        ),
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

/// A fire-and-forget `on_failure` hook, which shows in the history as a child
/// workflow start without needing a worker for it.
fn failure_hook_input() -> DbtRunInput {
    let mut input = run_input(false);
    input.hooks = Some(HooksConfig {
        on_failure: vec![HookConfig {
            workflow_type: "notify_failure".to_string(),
            task_queue: "no-worker-listens-here".to_string(),
            timeout_secs: None,
            on_error: HookErrorMode::default(),
            input: None,
            fire_and_forget: true,
        }],
        ..HooksConfig::default()
    });
    input
}

async fn history_events(
    handle: &WorkflowHandle<Client, UntypedWorkflow>,
) -> Result<Vec<temporalio_common::protos::temporal::api::history::v1::HistoryEvent>> {
    Ok(handle
        .fetch_history(WorkflowFetchHistoryOptions::default())
        .into_events()
        .await?)
}

/// Every value `DbtStatus` took, in order, as the history recorded it.
fn status_values(
    events: &[temporalio_common::protos::temporal::api::history::v1::HistoryEvent],
) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event.attributes.as_ref()? {
            Attributes::UpsertWorkflowSearchAttributesEventAttributes(a) => {
                let payload = a
                    .search_attributes
                    .as_ref()?
                    .indexed_fields
                    .get("DbtStatus")?;
                serde_json::from_slice::<String>(&payload.data).ok()
            }
            _ => None,
        })
        .collect()
}

fn child_workflow_types(
    events: &[temporalio_common::protos::temporal::api::history::v1::HistoryEvent],
) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event.attributes.as_ref()? {
            Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(a) => {
                Some(a.workflow_type.as_ref()?.name.clone())
            }
            _ => None,
        })
        .collect()
}

const FAILING_ON_RUN_START: &str = "on-run-start:\n  - \"select from from\"";

/// A phase outside the node loop that fails ends the run as failed: the status
/// attribute leaves `running`, and the `on_failure` hooks fire.
#[test]
fn a_failure_outside_the_node_loop_ends_the_run_as_failed() -> Result<()> {
    let project = write_project_with(
        FAILING_ON_RUN_START,
        &[("ok".to_string(), "select 1 as id".to_string())],
    )?;

    let events = scenario(&project, 2, |client, task_queue| async move {
        let handle = start_with(&client, &task_queue, failure_hook_input()).await?;
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        assert!(
            matches!(result, Err(WorkflowGetResultError::Failed(_))),
            "the run should fail: {result:?}"
        );
        history_events(&handle).await
    })?;

    assert_eq!(status_values(&events), ["running", "failed"]);
    assert_eq!(child_workflow_types(&events), ["notify_failure"]);
    Ok(())
}

/// Cancelling while a phase outside the node loop runs ends the workflow as
/// cancelled, marks it so, and does not treat it as a failure to report.
#[test]
fn cancelling_outside_the_node_loop_ends_the_run_as_cancelled() -> Result<()> {
    // A DDL statement, so the warehouse has to finish the query rather than
    // hand back a stream nobody reads.
    let slow_hook = "on-run-start:\n  - \"create table slow_hook as select \
                     sum(a.range * b.range) as total \
                     from range(120000) as a, range(120000) as b\"";
    let project =
        write_project_with(slow_hook, &[("ok".to_string(), "select 1 as id".to_string())])?;

    let events = scenario(&project, 2, |client, task_queue| async move {
        let handle = start_with(&client, &task_queue, failure_hook_input()).await?;
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
        history_events(&handle).await
    })?;

    assert_eq!(status_values(&events), ["running", "cancelled"]);
    assert_eq!(child_workflow_types(&events), Vec::<String>::new());
    Ok(())
}

/// An awaited hook on a queue nobody serves: its child workflow starts and
/// then runs until the parent gives up on it.
fn blocking_hook(workflow_type: &str, on_error: HookErrorMode) -> HookConfig {
    HookConfig {
        workflow_type: workflow_type.to_string(),
        task_queue: "no-worker-listens-here".to_string(),
        timeout_secs: Some(600),
        on_error,
        input: None,
        fire_and_forget: false,
    }
}

/// Run `input` against a one-model project, cancel it `after` it starts, and
/// report the history of the cancelled workflow.
fn cancel_while_hook_runs(
    input: DbtRunInput,
    after: std::time::Duration,
) -> Result<Vec<temporalio_common::protos::temporal::api::history::v1::HistoryEvent>> {
    let project = write_project(&[("ok".to_string(), "select 1 as id".to_string())])?;
    scenario(&project, 2, |client, task_queue| async move {
        let handle = start_with(&client, &task_queue, input).await?;
        tokio::time::sleep(after).await;
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
        history_events(&handle).await
    })
}

/// A cancellation that reaches a `pre_run` hook ends the run as cancelled
/// instead of failing it for the hook's sake, and the failure hooks stay quiet.
#[test]
fn cancelling_during_a_pre_run_hook_ends_the_run_as_cancelled() -> Result<()> {
    let mut input = run_input(false);
    input.hooks = Some(HooksConfig {
        pre_run: vec![blocking_hook("slow_pre_run", HookErrorMode::Fail)],
        on_failure: vec![fire_and_forget("notify_failure")],
        ..HooksConfig::default()
    });

    let events = cancel_while_hook_runs(input, std::time::Duration::from_secs(3))?;

    assert_eq!(status_values(&events), ["running", "cancelled"]);
    assert_eq!(child_workflow_types(&events), ["slow_pre_run"]);
    Ok(())
}

/// `warn` lets a hook fail without consequence, but a cancellation is not the
/// hook failing: the run must not carry on and report a completed run.
#[test]
fn cancelling_during_a_warn_mode_post_run_hook_ends_the_run_as_cancelled() -> Result<()> {
    let mut input = run_input(false);
    input.hooks = Some(HooksConfig {
        on_success: vec![blocking_hook("slow_on_success", HookErrorMode::Warn)],
        on_failure: vec![fire_and_forget("notify_failure")],
        ..HooksConfig::default()
    });

    let events = cancel_while_hook_runs(input, std::time::Duration::from_secs(8))?;

    assert_eq!(status_values(&events).last().map(String::as_str), Some("cancelled"));
    assert_eq!(child_workflow_types(&events), ["slow_on_success"]);
    Ok(())
}

const FAILING_ON_RUN_END: &str = "on-run-end:\n  - \"select from from\"";

/// A fire-and-forget hook of `workflow_type`, which needs no worker to listen.
fn fire_and_forget(workflow_type: &str) -> HookConfig {
    HookConfig {
        workflow_type: workflow_type.to_string(),
        task_queue: "no-worker-listens-here".to_string(),
        timeout_secs: None,
        on_error: HookErrorMode::default(),
        input: None,
        fire_and_forget: true,
    }
}

/// The first file called `name` anywhere under `root`.
fn find_file(root: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_file(&path, name) {
                return Some(found);
            }
        } else if path.file_name().is_some_and(|n| n == name) {
            return Some(path);
        }
    }
    None
}

fn read_run_results(project: &Project) -> Result<serde_json::Value> {
    let path = find_file(project.artifacts.path(), "run_results.json")
        .context("no run_results.json was stored")?;
    Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
}

/// As in dbt, a failing `on-run-end` hook fails the run: the workflow fails,
/// `DbtStatus` ends `failed`, and the failure hooks fire in place of the success
/// hooks. The nodes' own results are still written.
#[test]
fn a_failing_on_run_end_hook_fails_the_run() -> Result<()> {
    let project = write_project_with(
        FAILING_ON_RUN_END,
        &[("ok".to_string(), "select 1 as id".to_string())],
    )?;
    let mut input = run_input(false);
    input.hooks = Some(HooksConfig {
        on_success: vec![fire_and_forget("notify_success")],
        on_failure: vec![fire_and_forget("notify_failure")],
        ..HooksConfig::default()
    });

    let events = scenario(&project, 2, |client, task_queue| async move {
        let handle = start_with(&client, &task_queue, input).await?;
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        assert!(
            matches!(result, Err(WorkflowGetResultError::Failed(_))),
            "the run should fail: {result:?}"
        );
        // The failure names what the hook did wrong, not just that the
        // activity task failed.
        assert!(format!("{result:?}").contains("syntax error"), "{result:?}");
        history_events(&handle).await
    })?;

    assert_eq!(status_values(&events), ["running", "failed"]);
    assert_eq!(child_workflow_types(&events), ["notify_failure"]);
    let results = read_run_results(&project)?;
    assert_eq!(results["results"][0]["unique_id"], "model.spike.ok");
    assert_eq!(results["results"][0]["status"], "success");
    Ok(())
}

/// As in dbt, a valid selection that matches nothing ends the run successfully
/// with a warning. dbt runs no hooks for it, so the project's failing
/// `on-run-start` and `on-run-end` hooks stay silent; the run still writes
/// `run_results.json` (with no results) and its log carries the warning.
#[test]
fn a_selection_that_matches_nothing_completes_successfully() -> Result<()> {
    let project = write_project_with(
        &format!("{FAILING_ON_RUN_START}\n{FAILING_ON_RUN_END}"),
        &[("ok".to_string(), "select 1 as id".to_string())],
    )?;
    let mut input = run_input(false);
    input.select = Some("no_such_model".to_string());
    input.hooks = Some(HooksConfig {
        on_success: vec![fire_and_forget("notify_success")],
        on_failure: vec![fire_and_forget("notify_failure")],
        ..HooksConfig::default()
    });

    let (output, events) = scenario(&project, 2, |client, task_queue| async move {
        let handle = start_with(&client, &task_queue, input).await?;
        let raw = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .context("an empty selection should complete")?;
        let payload = raw
            .payloads
            .first()
            .ok_or_else(|| anyhow::anyhow!("no result payload"))?;
        let output: DbtRunOutput = serde_json::from_slice(&payload.data)?;
        Ok((output, history_events(&handle).await?))
    })?;

    assert!(output.success, "{output:?}");
    assert!(output.node_results.is_empty(), "{output:?}");
    assert_eq!(status_values(&events), ["running", "passed"]);
    assert_eq!(child_workflow_types(&events), ["notify_success"]);

    let results = read_run_results(&project)?;
    assert_eq!(results["results"], serde_json::json!([]));
    let log_path = output.log_path.context("the run should store its log")?;
    let log = std::fs::read_to_string(&log_path)?;
    assert!(
        log.contains(
            "WARNING: The selection criterion 'no_such_model' does not match any enabled nodes"
        ),
        "{log}"
    );
    assert!(
        log.contains(
            "WARNING: Nothing to do. Try checking your model configs and model specification args"
        ),
        "{log}"
    );
    Ok(())
}

/// Run two models with `hook` as the project's `on-run-end`, and report
/// whether the run succeeded.
///
/// A hook cannot hand a value back, so the scenarios make it render invalid SQL
/// when what it sees is not what is expected. That fails the run, because a
/// failing `on-run-end` hook does.
fn on_run_end_passes(hook: &str) -> Result<bool> {
    let project = write_project_with(
        &format!("on-run-end:\n  - \"{hook}\""),
        &[
            ("a".to_string(), "select 1 as id".to_string()),
            ("b".to_string(), "select id from {{ ref('a') }}".to_string()),
        ],
    )?;
    scenario(&project, 2, |client, task_queue| async move {
        let handle = start(&client, &task_queue, false).await?;
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        Ok(result.is_ok())
    })
}

/// Fails the run (invalid SQL) unless `expected` holds.
fn fail_unless(expected: &str) -> String {
    format!("{{% if not ({expected}) %}}select from from{{% endif %}}")
}

/// `results` is dbt's list of result objects: each has the executed `node` and
/// a status in dbt's words.
#[test]
fn on_run_end_results_carry_the_node_and_dbt_statuses() -> Result<()> {
    let names_and_statuses = fail_unless(
        "results | map(attribute='node') | map(attribute='name') | sort | join(',') == 'a,b' \
         and results | map(attribute='status') | unique | list == ['success'] \
         and results | map(attribute='node') | map(attribute='resource_type') | unique | list \
             == ['model']",
    );
    assert!(on_run_end_passes(&names_and_statuses)?);

    // The same hook refuses what it should: the check is not vacuous.
    let wrong_name = fail_unless(
        "results | map(attribute='node') | map(attribute='name') \
                                  | join(',') == 'nobody'",
    );
    assert!(!on_run_end_passes(&wrong_name)?);
    Ok(())
}

/// `schemas` and `database_schemas` name where the run built its relations.
#[test]
fn on_run_end_sees_the_schemas_the_run_built_into() -> Result<()> {
    let built = fail_unless(
        "schemas | list == ['main'] and database_schemas | length == 1 \
         and database_schemas[0][1] == 'main'",
    );
    assert!(on_run_end_passes(&built)?);

    let wrong = fail_unless("schemas | list == ['elsewhere']");
    assert!(!on_run_end_passes(&wrong)?);
    Ok(())
}

/// `status` of each result in a project's stored `run_results.json`, by node.
fn stored_statuses(project: &Project) -> Result<BTreeMap<String, String>> {
    let results = read_run_results(project)?;
    results["results"]
        .as_array()
        .context("results should be an array")?
        .iter()
        .map(|r| {
            Ok((
                r["unique_id"].as_str().context("unique_id")?.to_string(),
                r["status"].as_str().context("status")?.to_string(),
            ))
        })
        .collect()
}

/// The nodes a `retry_from` run plans from `run_results`, found by planning
/// with `list` so that nothing executes.
fn nodes_retried_from(
    project: &Project,
    run_results: &std::path::Path,
) -> Result<std::collections::BTreeSet<String>> {
    let mut input = run_input(false);
    input.command = "list".to_string();
    input.retry_from = Some(run_results.to_string_lossy().into_owned());
    scenario(project, 2, |client, task_queue| async move {
        let handle = start_with(&client, &task_queue, input).await?;
        let raw = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .context("planning the retry")?;
        let payload = raw
            .payloads
            .first()
            .ok_or_else(|| anyhow::anyhow!("no result payload"))?;
        let output: DbtRunOutput = serde_json::from_slice(&payload.data)?;
        Ok(output
            .node_results
            .into_iter()
            .map(|r| r.unique_id)
            .collect())
    })
}

/// A run cancelled while a node executes still stores what it has: the node
/// that finished as it finished, the rest as cancelled, in a `run_results.json`
/// that `retry_from` plans from.
#[test]
fn a_cancelled_run_leaves_run_results_that_retry_from_reads() -> Result<()> {
    let slow = "{{ config(materialized='table') }}\n\
                select sum(a.range * b.range) as total \
                from range(120000) as a, range(120000) as b";
    let project = write_project(&[
        ("done".to_string(), "select 1 as id".to_string()),
        ("slow_a".to_string(), slow.to_string()),
        ("after".to_string(), "select total from {{ ref('slow_a') }}".to_string()),
    ])?;

    scenario(&project, 4, |client, task_queue| async move {
        let handle = start(&client, &task_queue, false).await?;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
    })?;

    let expected = BTreeMap::from([
        ("model.spike.after".to_string(), "cancelled".to_string()),
        ("model.spike.done".to_string(), "success".to_string()),
        ("model.spike.slow_a".to_string(), "cancelled".to_string()),
    ]);
    assert_eq!(stored_statuses(&project)?, expected);

    let run_results = find_file(project.artifacts.path(), "run_results.json")
        .context("no run_results.json was stored")?;
    let retried = nodes_retried_from(&project, &run_results)?;
    assert_eq!(
        retried,
        ["model.spike.after", "model.spike.slow_a"]
            .into_iter()
            .map(String::from)
            .collect()
    );
    Ok(())
}

/// A run that fails fast leaves the nodes it never started as skipped, and
/// `retry_from` plans the failure and everything the run did not do.
#[test]
fn a_failed_run_leaves_run_results_that_retry_from_reads() -> Result<()> {
    let project = write_project(&[
        ("bad".to_string(), "select from from".to_string()),
        ("child".to_string(), "select * from {{ ref('bad') }}".to_string()),
        ("fine".to_string(), "select 1 as id".to_string()),
    ])?;

    scenario(&project, 1, |client, task_queue| async move {
        let handle = start(&client, &task_queue, true).await?;
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        assert!(
            matches!(result, Err(WorkflowGetResultError::Failed(_))),
            "the run should fail: {result:?}"
        );
        Ok(())
    })?;

    let statuses = stored_statuses(&project)?;
    assert_eq!(statuses["model.spike.bad"], "error");
    assert_eq!(statuses["model.spike.child"], "skipped");
    let run_results = find_file(project.artifacts.path(), "run_results.json")
        .context("no run_results.json was stored")?;
    let retried = nodes_retried_from(&project, &run_results)?;
    assert!(retried.contains("model.spike.bad") && retried.contains("model.spike.child"));
    // Whether `fine` ran before the failure depends on scheduling; the retry
    // re-runs it exactly when it did not succeed.
    assert_eq!(retried.contains("model.spike.fine"), statuses["model.spike.fine"] != "success");
    Ok(())
}

/// The catalog is built under the artifact activity's own cancellation token;
/// a run with it enabled still stores `catalog.json` describing what it built.
#[test]
fn a_run_with_the_catalog_enabled_stores_it() -> Result<()> {
    let mut project = write_project(&[(
        "orders".to_string(),
        "{{ config(materialized='table') }}\nselect 1 as id, 'x' as label".to_string(),
    )])?;
    project.write_catalog = true;

    let finished = run_workflow(&project, 2)?;

    let artifacts = finished
        .output
        .artifacts
        .context("the run should have stored artifacts")?;
    let catalog_path = artifacts
        .catalog_path
        .context("catalog.json should have been stored")?;
    let catalog: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(catalog_path)?)?;
    let columns = &catalog["nodes"]["model.spike.orders"]["columns"];
    assert!(columns.get("id").is_some() && columns.get("label").is_some(), "{catalog}");
    Ok(())
}

/// Activities outside the node loop are scheduled with a bounded retry policy
/// that treats permanent failure types as final.
#[test]
fn activities_outside_the_node_loop_carry_a_bounded_retry_policy() -> Result<()> {
    let project = write_project(&[("ok".to_string(), "select 1 as id".to_string())])?;

    let events = scenario(&project, 2, |client, task_queue| async move {
        let handle = start(&client, &task_queue, false).await?;
        handle
            .get_result(WorkflowGetResultOptions::default())
            .await?;
        history_events(&handle).await
    })?;

    for activity in ["plan_project", "store_artifacts"] {
        let policy = events
            .iter()
            .find_map(|event| match event.attributes.as_ref()? {
                Attributes::ActivityTaskScheduledEventAttributes(a)
                    if a.activity_type.as_ref().is_some_and(|t| t.name == activity) =>
                {
                    Some(a.retry_policy.clone())
                }
                _ => None,
            })
            .with_context(|| format!("{activity} was not scheduled"))?
            .with_context(|| format!("{activity} was scheduled without a retry policy"))?;
        assert_eq!(policy.maximum_attempts, 3, "{activity}");
        assert!(
            policy
                .non_retryable_error_types
                .iter()
                .any(|name| name == "DbtTemporalError::ArtifactStorePermanent"),
            "{activity}: {:?}",
            policy.non_retryable_error_types
        );
    }
    Ok(())
}

/// With a threshold far below the plan's size, the run completes and workflow
/// history carries references where the large payloads would have been. The
/// payloads themselves sit in the artifact store, under their own directory.
#[test]
fn large_payloads_are_kept_out_of_history() -> Result<()> {
    const THRESHOLD: usize = 2048;
    let models: Vec<(String, String)> = (0..60)
        .map(|i| (format!("m{i:03}"), format!("select {i} as id")))
        .collect();
    let project = write_project(&models)?;

    let (output, inline, references) = scenario_with(
        &project,
        4,
        Some(ExternalPayloadStorageConfig {
            threshold_bytes: Some(THRESHOLD),
        }),
        |client, task_queue| async move {
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

            // Fetched history is returned as stored, references included.
            let events = handle
                .fetch_history(WorkflowFetchHistoryOptions::default())
                .into_events()
                .await?;
            let mut inline = Vec::new();
            let mut references = 0;
            for event in events {
                let payloads = match event.attributes {
                    Some(Attributes::ActivityTaskScheduledEventAttributes(a)) => {
                        a.input.map(|p| p.payloads)
                    }
                    Some(Attributes::ActivityTaskCompletedEventAttributes(a)) => {
                        a.result.map(|p| p.payloads)
                    }
                    _ => None,
                };
                for payload in payloads.unwrap_or_default() {
                    if payload.metadata.get("messageType").map(Vec::as_slice)
                        == Some(b"temporal.api.sdk.v1.ExternalStorageReference".as_slice())
                    {
                        references += 1;
                    } else {
                        inline.push(prost::Message::encoded_len(&payload));
                    }
                }
            }
            Ok((output, inline, references))
        },
    )?;

    assert!(output.success);
    assert_eq!(output.node_results.len(), 60);
    assert!(references > 0, "no payload was offloaded");
    assert!(
        inline.iter().all(|&size| size < THRESHOLD),
        "payloads at or above the threshold stayed in history: {inline:?}"
    );
    let stored = std::fs::read_dir(project.artifacts.path().join("_external_payloads"))?.count();
    assert!(stored > 0, "no payload object reached the artifact store");
    Ok(())
}
