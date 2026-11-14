//! Runs the workflow must refuse: a failing project check, and inputs the
//! planner cannot honour. Each stops before any node touches the warehouse,
//! and each has to say why — a rejected run that only reports "workflow
//! failed" sends the operator to the worker logs.

use anyhow::{Context, Result};
use temporalio_client::{
    Client, UntypedWorkflow, WorkflowGetResultOptions, WorkflowStartOptions,
    errors::WorkflowGetResultError,
};
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;

use dbt_temporal::types::DbtRunInput;

use super::infra::{
    connect_client, copy_fixture, init_tracing, make_input, psql_query, shared_infra, test_config,
    test_config_with_schema,
};

/// Run a workflow that must fail, returning the failure with its cause chain.
async fn failure_of(client: &Client, task_queue: &str, input: DbtRunInput) -> Result<String> {
    let workflow_id = format!("test-{}", uuid::Uuid::new_v4());
    let handle = client
        .start_workflow(
            UntypedWorkflow::new("dbt_run"),
            RawValue::new(vec![input.as_json_payload()?]),
            WorkflowStartOptions::new(task_queue, &workflow_id).build(),
        )
        .await
        .context("starting workflow")?;

    match handle.get_result(WorkflowGetResultOptions::default()).await {
        Err(WorkflowGetResultError::Failed(failure)) => Ok(format!("{failure:?}")),
        Ok(_) => anyhow::bail!("expected the workflow to fail, but it succeeded"),
        Err(other) => anyhow::bail!("expected a workflow failure, got: {other:?}"),
    }
}

/// A check that fails stops the run at the gate: the failure names the check,
/// and no model was materialized on the way to finding out.
#[tokio::test(flavor = "current_thread")]
async fn test_failing_project_check_stops_the_run_before_any_node() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    // Declaring checks requires pinning the `dbt.*` vocabulary they query.
    let project_yml = fixture_dir.join("dbt_project.yml");
    let mut contents = std::fs::read_to_string(&project_yml).context("reading dbt_project.yml")?;
    contents.push_str("\ninfo_schema:\n  version: 1\n");
    std::fs::write(&project_yml, contents).context("writing dbt_project.yml")?;

    // Every row a check returns is a violation, so selecting the project's
    // models at all fails it.
    std::fs::create_dir_all(fixture_dir.join("checks")).context("creating checks dir")?;
    std::fs::write(
        fixture_dir.join("checks/no_models_allowed.sql"),
        "select unique_id from {{ info_schema('models') }}",
    )
    .context("writing check")?;

    let (config, schema) = test_config_with_schema(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);
    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            r = worker.run() => r,
            () = worker_abort_rx.notified() => Ok(()),
        }
    });

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;
            let failure =
                failure_of(&client, &task_queue, make_input("build", None, None, false)).await?;

            assert!(failure.contains("project check(s) failed"), "got: {failure}");
            assert!(
                failure.contains("no_models_allowed"),
                "the failure should name the check: {failure}"
            );

            let tables = psql_query(
                infra,
                &format!(
                    "select count(*) from information_schema.tables \
                     where table_schema = '{schema}'"
                ),
            );
            assert_eq!(
                tables.trim(),
                "0",
                "a run stopped at the gate must not have built anything"
            );
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}

/// Inputs the planner cannot honour fail the run with the reason, instead of
/// planning something other than what was asked for.
#[tokio::test(flavor = "current_thread")]
async fn test_planner_rejects_inputs_it_cannot_honour() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    // `test_config` leaves artifact storage off, which is what the
    // state/retry cases below depend on.
    let config = test_config(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);
    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            r = worker.run() => r,
            () = worker_abort_rx.notified() => Ok(()),
        }
    });

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let client = connect_client(&infra.temporal_addr).await?;

            // A typo in the mode would otherwise change which tests run.
            let mut input = make_input("build", Some("stg_orders"), None, false);
            input.indirect_selection = Some("eagre".to_string());
            let failure = failure_of(&client, &task_queue, input).await?;
            assert!(failure.contains("eagre"), "should quote the bad mode: {failure}");

            // state: selectors compare against a manifest; without one they
            // would match nothing and look like an empty change set.
            let input = make_input("build", Some("state:modified"), None, false);
            let failure = failure_of(&client, &task_queue, input).await?;
            assert!(failure.contains("set state_manifest_ref"), "got: {failure}");

            let mut input = make_input("build", Some("state:modified"), None, false);
            input.state_manifest_ref = Some("previous/manifest.json".to_string());
            let failure = failure_of(&client, &task_queue, input).await?;
            assert!(
                failure.contains("state_manifest_ref requires artifact storage"),
                "got: {failure}"
            );

            let mut input = make_input("build", None, None, false);
            input.retry_from = Some("previous/run_results.json".to_string());
            let failure = failure_of(&client, &task_queue, input).await?;
            assert!(failure.contains("retry_from requires artifact storage"), "got: {failure}");

            // A selector that matches nothing is an error, not an empty success.
            let input = make_input("build", Some("no_such_model"), None, false);
            let failure = failure_of(&client, &task_queue, input).await?;
            assert!(failure.contains("no nodes matched"), "got: {failure}");

            // A microbatch window whose end is not a timestamp.
            let mut input = make_input("build", None, None, false);
            input.event_time_start = Some("2026-01-01T00:00:00Z".to_string());
            input.event_time_end = Some("tomorrow".to_string());
            let failure = failure_of(&client, &task_queue, input).await?;
            assert!(failure.contains("parsing event_time_end"), "got: {failure}");

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();
    result
}
