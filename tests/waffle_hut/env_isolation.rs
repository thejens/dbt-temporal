//! Cross-workflow env isolation tests.
//!
//! These tests verify that per-workflow `DbtRunInput.env` overrides are correctly
//! isolated across all layers: SQL rendering, node metadata (schema/relation),
//! and compiled SQL caches. They serve as regression tests for the issues tracked
//! in TODO.md under "Cross-workflow env isolation".

use anyhow::{Context, Result};

use super::infra::*;

// ---------------------------------------------------------------------------
// Test 1: Schema routing via env_var
//
// Catches: "Node relation metadata uses startup env, not per-workflow env"
//
// Verifies materialization schema placement via information_schema queries.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_env_schema_override_materializes_in_correct_schema() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(fixture_dir.join("models/schema.yml"), "version: 2\nmodels: []\n")
        .context("clearing schema.yml")?;

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

            // Run workflow with DB_SCHEMA override.
            let target_schema = "env_override_target";
            let mut env = pg_env(infra);
            env.insert("DB_SCHEMA".to_string(), target_schema.to_string());

            let mut input = make_input_with_env("run", None, env);
            input.select = Some("stg_customers".to_string());

            let run = run_dbt_workflow(&client, &task_queue, input).await?;
            print_results(&run.output);
            assert!(run.output.success, "workflow with schema override should succeed");

            // Verify the table landed in the target schema via information_schema.
            let output = std::process::Command::new("psql")
                .arg("-h")
                .arg(&infra.pg_host)
                .arg("-p")
                .arg(infra.pg_port.to_string())
                .arg("-U")
                .arg(&infra.pg_user)
                .arg("-d")
                .arg(&infra.pg_database)
                .arg("-t")
                .arg("-A")
                .arg("-c")
                .arg(format!(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_schema = '{target_schema}' AND table_name = 'stg_customers'"
                ))
                .env("PGPASSWORD", &infra.pg_password)
                .output()
                .context("querying information_schema")?;
            let found = String::from_utf8_lossy(&output.stdout).trim().to_string();
            assert_eq!(
                found, "stg_customers",
                "stg_customers should exist in schema '{target_schema}'"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}

// ---------------------------------------------------------------------------
// Test 2: compiled_code reflects per-workflow env, not startup env
//
// Catches: "`compiled_sql_cache` reflects startup env"
//
// A model uses `env_var('MY_TAG', 'startup_default')` in its SQL. The startup
// resolver runs with the default. A workflow overrides MY_TAG. The compiled_code
// returned in NodeExecutionResult must contain the *overridden* value.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_compiled_code_reflects_workflow_env() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(fixture_dir.join("models/schema.yml"), "version: 2\nmodels: []\n")
        .context("clearing schema.yml")?;

    // Inject env_var() into model SQL with a default that differs from the override.
    let model_sql = r#"select
    id as customer_id,
    first_name,
    last_name,
    '{{ env_var("MY_TAG", "startup_default") }}' as tag
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

            // --- Run with MY_TAG override ---
            let mut env = pg_env(infra);
            env.insert("MY_TAG".to_string(), "workflow_override".to_string());

            let mut input = make_input_with_env("run", None, env);
            input.select = Some("stg_customers".to_string());

            let run = run_dbt_workflow(&client, &task_queue, input).await?;
            print_results(&run.output);
            assert!(run.output.success, "workflow should succeed");

            // Find the stg_customers result and check compiled_code.
            let result = run
                .output
                .node_results
                .iter()
                .find(|r| r.unique_id.contains("stg_customers"))
                .expect("should have stg_customers result");

            let compiled_code = result
                .compiled_code
                .as_deref()
                .expect("stg_customers should have compiled_code");

            assert!(
                compiled_code.contains("workflow_override"),
                "compiled_code should contain the per-workflow env value 'workflow_override', \
                 not the startup default. Got:\n{compiled_code}"
            );
            assert!(
                !compiled_code.contains("startup_default"),
                "compiled_code should NOT contain the startup default 'startup_default'. \
                 The compiled_sql_cache from worker init leaked into the result. Got:\n{compiled_code}"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}

// ---------------------------------------------------------------------------
// Test 3: Parallel workflows with different env_var values don't cross-contaminate
//
// Catches: any cross-workflow leakage in Jinja env, compiled SQL, or caches
//
// Two workflows run concurrently against the same worker. Each uses a different
// value for MY_TAG in model SQL. We verify each workflow's compiled_code contains
// only its own value and not the other's.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_parallel_compiled_code_no_cross_contamination() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(fixture_dir.join("models/schema.yml"), "version: 2\nmodels: []\n")
        .context("clearing schema.yml")?;

    // Inject env_var() into model SQL.
    let model_sql = r#"select
    id as customer_id,
    first_name,
    last_name,
    '{{ env_var("MY_TAG", "default_tag") }}' as tag
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

            // Build env maps for two workflows with distinct tags.
            let build_env = |tag: &str| -> std::collections::BTreeMap<String, String> {
                let mut env = pg_env(infra);
                env.insert("MY_TAG".to_string(), tag.to_string());
                env
            };

            let mut input_a = make_input_with_env("run", None, build_env("tenant_alpha"));
            input_a.select = Some("stg_customers".to_string());

            let mut input_b = make_input_with_env("run", None, build_env("tenant_beta"));
            input_b.select = Some("stg_customers".to_string());

            // Run both concurrently.
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

            assert!(run_a.output.success, "workflow A should succeed");
            assert!(run_b.output.success, "workflow B should succeed");

            // Extract compiled_code from each workflow's stg_customers result.
            let code_a = run_a
                .output
                .node_results
                .iter()
                .find(|r| r.unique_id.contains("stg_customers"))
                .and_then(|r| r.compiled_code.as_deref())
                .expect("workflow A should have stg_customers compiled_code");

            let code_b = run_b
                .output
                .node_results
                .iter()
                .find(|r| r.unique_id.contains("stg_customers"))
                .and_then(|r| r.compiled_code.as_deref())
                .expect("workflow B should have stg_customers compiled_code");

            // Workflow A should have tenant_alpha, NOT tenant_beta or default_tag.
            assert!(
                code_a.contains("tenant_alpha"),
                "workflow A compiled_code should contain 'tenant_alpha'. Got:\n{code_a}"
            );
            assert!(
                !code_a.contains("tenant_beta"),
                "workflow A compiled_code must NOT contain 'tenant_beta' (cross-contamination). Got:\n{code_a}"
            );
            assert!(
                !code_a.contains("default_tag"),
                "workflow A compiled_code must NOT contain 'default_tag' (startup cache leaked). Got:\n{code_a}"
            );

            // Workflow B should have tenant_beta, NOT tenant_alpha or default_tag.
            assert!(
                code_b.contains("tenant_beta"),
                "workflow B compiled_code should contain 'tenant_beta'. Got:\n{code_b}"
            );
            assert!(
                !code_b.contains("tenant_alpha"),
                "workflow B compiled_code must NOT contain 'tenant_alpha' (cross-contamination). Got:\n{code_b}"
            );
            assert!(
                !code_b.contains("default_tag"),
                "workflow B compiled_code must NOT contain 'default_tag' (startup cache leaked). Got:\n{code_b}"
            );

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}

// ---------------------------------------------------------------------------
// Test 4: Parallel schema isolation — models land in different schemas
//
// Catches: "Node relation metadata uses startup env" under parallel execution
//
// Verifies schema placement via information_schema queries.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_parallel_schema_isolation() -> Result<()> {
    init_tracing();

    let infra = shared_infra();

    let fixture_dir = copy_fixture("waffle_hut")?;
    std::fs::remove_dir_all(fixture_dir.join("target")).ok();

    std::fs::write(fixture_dir.join("models/schema.yml"), "version: 2\nmodels: []\n")
        .context("clearing schema.yml")?;

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

            // Launch two workflows concurrently, each targeting a different schema.
            let mut env_a = pg_env(infra);
            env_a.insert("DB_SCHEMA".to_string(), "iso_schema_a".to_string());
            let mut input_a = make_input_with_env("run", None, env_a);
            input_a.select = Some("stg_customers".to_string());

            let mut env_b = pg_env(infra);
            env_b.insert("DB_SCHEMA".to_string(), "iso_schema_b".to_string());
            let mut input_b = make_input_with_env("run", None, env_b);
            input_b.select = Some("stg_customers".to_string());

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

            assert!(run_a.output.success, "workflow A (iso_schema_a) should succeed");
            assert!(run_b.output.success, "workflow B (iso_schema_b) should succeed");

            // Verify each schema has the table via information_schema.
            for schema in ["iso_schema_a", "iso_schema_b"] {
                let output = std::process::Command::new("psql")
                    .arg("-h")
                    .arg(&infra.pg_host)
                    .arg("-p")
                    .arg(infra.pg_port.to_string())
                    .arg("-U")
                    .arg(&infra.pg_user)
                    .arg("-d")
                    .arg(&infra.pg_database)
                    .arg("-t")
                    .arg("-A")
                    .arg("-c")
                    .arg(format!(
                        "SELECT table_name FROM information_schema.tables \
                         WHERE table_schema = '{schema}' AND table_name = 'stg_customers'"
                    ))
                    .env("PGPASSWORD", &infra.pg_password)
                    .output()
                    .context("querying information_schema")?;
                let found = String::from_utf8_lossy(&output.stdout).trim().to_string();
                assert_eq!(
                    found, "stg_customers",
                    "stg_customers should exist in schema '{schema}'"
                );
            }

            Ok(())
        })
        .await;

    worker_abort.notify_one();
    std::fs::remove_dir_all(&fixture_dir).ok();

    test_result
}
