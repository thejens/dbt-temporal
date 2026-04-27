//! Tests for on-run-start / on-run-end project hook execution.
//!
//! Uses a copy of the waffle_hut fixture (so hook side-effects don't affect
//! other tests sharing the fixture). The copy gets a `macros/` directory and
//! `on-run-start` / `on-run-end` entries in `dbt_project.yml` that write to
//! a `hook_audit` table — the test then queries that table to verify both
//! hook phases fired with the right context.

use anyhow::{Context, Result};

use super::infra::*;

/// Set up a waffle_hut copy with on-run-start / on-run-end hooks that record
/// their execution in a `hook_audit` table.
fn setup_hooks_fixture() -> Result<std::path::PathBuf> {
    let dir = copy_fixture("waffle_hut")?;

    // Add macros that each render to a single SQL statement.
    let macros_dir = dir.join("macros");
    std::fs::create_dir_all(&macros_dir).context("creating macros dir")?;
    std::fs::write(
        macros_dir.join("on_run_hooks.sql"),
        r"{% macro on_run_start_create_audit() %}
  create table if not exists {{ target.schema }}.hook_audit (
    event     text    not null,
    run_id    text    not null,
    row_count integer
  )
{% endmacro %}

{% macro on_run_start_record() %}
  insert into {{ target.schema }}.hook_audit (event, run_id)
  values ('on_run_start', '{{ invocation_id }}')
{% endmacro %}

{% macro on_run_end_record() %}
  insert into {{ target.schema }}.hook_audit (event, run_id, row_count)
  values ('on_run_end', '{{ invocation_id }}',
          {%- if results is defined %} {{ results | length }}{%- else %} null{%- endif %})
{% endmacro %}
",
    )
    .context("writing macros")?;

    // Replace dbt_project.yml with one that wires the hooks.
    std::fs::write(
        dir.join("dbt_project.yml"),
        r#"name: waffle_hut
version: "1.0.0"
config-version: 2
profile: waffle_hut

on-run-start:
  - "{{ on_run_start_create_audit() }}"
  - "{{ on_run_start_record() }}"

on-run-end:
  - "{{ on_run_end_record() }}"

models:
  waffle_hut:
    +materialized: table

clean-targets:
  - target
  - dbt_packages
"#,
    )
    .context("writing dbt_project.yml")?;

    Ok(dir)
}

/// on-run-start fires before node execution and on-run-end fires after, with
/// the correct `results` context (right count, correct invocation_id).
#[tokio::test(flavor = "current_thread")]
async fn test_project_hooks_fire_with_correct_context() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = setup_hooks_fixture()?;
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
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;
            let run =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;

            assert!(run.output.success, "run should succeed");
            let invocation_id = run.output.invocation_id;

            // Both hooks should have written rows to hook_audit.
            let rows = psql_query(
                infra,
                &format!(
                    "SELECT event, run_id, row_count FROM \"{schema}\".hook_audit ORDER BY event"
                ),
            );

            // on-run-end: fired after nodes with results list of length 5
            assert!(
                rows.contains("on_run_end"),
                "on_run_end hook should have fired; got: {rows:?}"
            );
            assert!(
                rows.contains(&invocation_id),
                "hook should have seen the correct invocation_id; got: {rows:?}"
            );
            // waffle_hut runs 5 models — results should contain 5 entries.
            let on_run_end_row = rows
                .lines()
                .find(|l| l.contains("on_run_end"))
                .context("no on_run_end row")?;
            assert!(
                on_run_end_row.ends_with("|5"),
                "on_run_end results should have 5 entries; row: {on_run_end_row:?}"
            );

            // on-run-start fired before any nodes
            let on_run_start_row = rows
                .lines()
                .find(|l| l.contains("on_run_start"))
                .context("no on_run_start row")?;
            assert!(
                on_run_start_row.contains(&invocation_id),
                "on_run_start should have the correct invocation_id; row: {on_run_start_row:?}"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    result
}

/// Project hooks executing under per-workflow env overrides: the hook activity
/// rebuilds the adapter engine via `rebuild_adapter_engine_with_env` and patches
/// the Jinja `target` global. Exercises the env-override branch of
/// `activities/project_hooks::run_project_hooks_inner`.
#[tokio::test(flavor = "current_thread")]
async fn test_project_hooks_with_env_overrides() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = setup_hooks_fixture()?;
    let config = test_config_env_var_profile(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker with env_var profile")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);
    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            r = worker.run() => r,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;
            let env = pg_env(infra);
            let run = run_dbt_workflow(&client, &task_queue, make_input_with_env("run", None, env))
                .await?;
            assert!(run.output.success, "run-with-env-overrides + project hooks should succeed");
            Ok(())
        })
        .await;

    worker_abort.notify_one();
    result
}
