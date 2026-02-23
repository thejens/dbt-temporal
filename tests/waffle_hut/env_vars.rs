//! Environment variable override tests: per-workflow env isolation,
//! model-level env_var(), and parallel schema isolation.

use anyhow::{Context, Result};

use super::infra::*;

/// Test per-workflow env var overrides: profiles.yml uses `env_var()` for all
/// connection parameters. The workflow passes credentials via `DbtRunInput.env`,
/// proving that the adapter engine is rebuilt at runtime.
#[tokio::test(flavor = "current_thread")]
async fn test_env_var_override() -> Result<()> {
    init_tracing();

    let infra = shared_infra();
    let fixture_dir = fixture_path("waffle_hut");
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
            result = worker.run() => result,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;

            // --- Workflow with env overrides: should rebuild adapter engine ---
            tracing::info!("Env var test: workflow passes DB connection via env overrides");
            let env = pg_env(infra);

            let run = run_dbt_workflow(&client, &task_queue, make_input_with_env("run", None, env))
                .await?;
            let output = &run.output;
            print_results(output);

            assert!(output.success, "dbt run with env overrides should succeed");

            let model_results: Vec<_> = output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            assert_eq!(model_results.len(), 5, "should run all 5 models");
            for r in &model_results {
                assert_eq!(r.status, "success", "model {} should succeed", r.unique_id);
            }

            // --- Run a second workflow without env overrides ---
            // With no env overrides, the shared adapter engine (built at worker startup) is used.
            // This tests the fallback path through the same worker.
            tracing::info!("Env var test: workflow without env overrides (uses shared adapter)");
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("run", Some("stg_customers"), None, true),
            )
            .await?;
            print_results(&run.output);
            assert!(run.output.success, "workflow without env overrides should succeed");

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}

/// Test model-level `env_var()` override: a model SQL that uses `{{ env_var('MY_LABEL', ...) }}`
/// gets the overridden value from `DbtRunInput.env` at execution time.
#[tokio::test(flavor = "current_thread")]
async fn test_env_var_model_override() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    // Copy waffle_hut and inject env_var() into a model's SQL.
    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();
    let model_sql = r#"select
    id as customer_id,
    first_name,
    last_name,
    '{{ env_var("MY_LABEL", "default_label") }}' as label
from {{ source('waffle_hut', 'customers') }}
"#;
    std::fs::write(fixture_dir.join("models/staging/stg_customers.sql"), model_sql)
        .context("writing model with env_var")?;

    let config = test_config_env_var_profile(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);

    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            result = worker.run() => result,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;

            // --- Workflow A: env override with MY_LABEL=value_alpha ---
            tracing::info!("Model env_var test: workflow A with MY_LABEL=value_alpha");
            let mut env_a = pg_env(infra);
            env_a.insert("MY_LABEL".to_string(), "value_alpha".to_string());

            let mut input_a = make_input_with_env("run", None, env_a);
            input_a.select = Some("stg_customers".to_string());

            // --- Workflow B: env override with MY_LABEL=value_beta ---
            tracing::info!("Model env_var test: workflow B with MY_LABEL=value_beta");
            let mut env_b = pg_env(infra);
            env_b.insert("MY_LABEL".to_string(), "value_beta".to_string());

            let mut input_b = make_input_with_env("run", None, env_b);
            input_b.select = Some("stg_customers".to_string());

            // Run both concurrently — if env vars leaked between workflows, one might fail.
            let (result_a, result_b) = tokio::join!(
                run_dbt_workflow(&client, &task_queue, input_a),
                run_dbt_workflow(&client, &task_queue, input_b),
            );

            let run_a = result_a.context("workflow A failed")?;
            let run_b = result_b.context("workflow B failed")?;

            tracing::info!("Workflow A results:");
            print_results(&run_a.output);
            tracing::info!("Workflow B results:");
            print_results(&run_b.output);

            assert!(run_a.output.success, "workflow A with MY_LABEL=value_alpha should succeed");
            assert!(run_b.output.success, "workflow B with MY_LABEL=value_beta should succeed");
            assert_eq!(run_a.output.node_results.len(), 1);
            assert_eq!(run_b.output.node_results.len(), 1);
            assert_eq!(run_a.output.node_results[0].status, "success");
            assert_eq!(run_b.output.node_results[0].status, "success");

            // --- Workflow without env override: model should also succeed (uses default) ---
            tracing::info!("Model env_var test: no override (uses default)");
            let run = run_dbt_workflow(
                &client,
                &task_queue,
                make_input("run", Some("stg_customers"), None, true),
            )
            .await?;
            print_results(&run.output);
            assert!(
                run.output.success,
                "workflow without env override should succeed (uses default)"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}

/// Test parallel env var isolation: two workflows run simultaneously with DIFFERENT
/// `env` maps. Both workflows rebuild the adapter engine with their own overrides
/// and run concurrently without interference.
///
/// This test overrides `DB_SCHEMA` to place models in different schemas.
///
#[tokio::test(flavor = "current_thread")]
async fn test_parallel_env_var_isolation() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = fixture_path("waffle_hut");
    let config = test_config_env_var_profile(infra, &fixture_dir)?;
    let task_queue = config.temporal_task_queue.clone();

    let mut worker = dbt_temporal::worker::build_worker(&config)
        .await
        .context("building worker with schema env_var profile")?;

    let local = tokio::task::LocalSet::new();
    let worker_abort = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_abort_rx = std::sync::Arc::clone(&worker_abort);

    let _worker_task = local.spawn_local(async move {
        tokio::select! {
            result = worker.run() => result,
            _ = worker_abort_rx.notified() => Ok(()),
        }
    });

    let test_result: Result<()> = local
        .run_until(async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let client = connect_client(&infra.temporal_addr).await?;

            // Run two workflows concurrently, each targeting a different schema.
            tracing::info!("Parallel isolation: launching workflow A (schema_a) and B (schema_b)");

            let mut env_a = pg_env(infra);
            env_a.insert("DB_SCHEMA".to_string(), "schema_a".to_string());

            let mut env_b = pg_env(infra);
            env_b.insert("DB_SCHEMA".to_string(), "schema_b".to_string());

            let input_a = make_input_with_env("run", None, env_a);
            let input_b = make_input_with_env("run", None, env_b);

            let (result_a, result_b) = tokio::join!(
                run_dbt_workflow(&client, &task_queue, input_a),
                run_dbt_workflow(&client, &task_queue, input_b),
            );

            let run_a = result_a.context("workflow A failed")?;
            let run_b = result_b.context("workflow B failed")?;

            tracing::info!("Workflow A results:");
            print_results(&run_a.output);
            tracing::info!("Workflow B results:");
            print_results(&run_b.output);

            assert!(run_a.output.success, "workflow A (schema_a) should succeed");
            assert!(run_b.output.success, "workflow B (schema_b) should succeed");

            let models_a: Vec<_> = run_a
                .output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            let models_b: Vec<_> = run_b
                .output
                .node_results
                .iter()
                .filter(|r| r.unique_id.starts_with("model."))
                .collect();
            assert_eq!(models_a.len(), 5, "workflow A should run 5 models");
            assert_eq!(models_b.len(), 5, "workflow B should run 5 models");

            for r in &models_a {
                assert_eq!(r.status, "success", "workflow A: {} should succeed", r.unique_id);
            }
            for r in &models_b {
                assert_eq!(r.status, "success", "workflow B: {} should succeed", r.unique_id);
            }

            // Run a third workflow without any env override — uses default schema.
            tracing::info!("Parallel isolation: workflow C (no override, uses default schema)");
            let run_c =
                run_dbt_workflow(&client, &task_queue, make_input("run", None, None, true)).await?;
            print_results(&run_c.output);
            assert!(run_c.output.success, "workflow C (default schema) should succeed");

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    test_result
}
