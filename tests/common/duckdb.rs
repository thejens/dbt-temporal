//! Shared harness for error-scenario tests against an embedded DuckDB warehouse.
//!
//! DuckDB boots in-process via its ADBC driver (no Docker), so these tests run
//! in milliseconds and drive the real `execute_node` compile → materialize →
//! result path. A deliberately broken model produces a genuine warehouse error
//! that flows through dbt-temporal's error handling — the SQLite approach of
//! testing against a real engine instead of a mock.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::large_futures
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dbt_adbc::QueryCtx;
use dbt_common::cancellation::CancellationTokenSource;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::profiles::{DbConfig, DuckDbConfig};
use dbt_temporal::activities::DbtActivities;
use dbt_temporal::activities::execute_node::execute_node_inner;
use dbt_temporal::activities::project_hooks::run_project_hooks_inner;
use dbt_temporal::config::{
    DbtTemporalConfig, PriorityScheduling, RegisteredSearchAttributes, SearchAttributeConfig,
    TemporalMetricsConfig, WorkerTuningConfig, WriteArtifacts, WriteCatalog, WriteRunLog,
};
use dbt_temporal::error::DbtTemporalError;
use dbt_temporal::project_registry::ProjectRegistry;
use dbt_temporal::types::{NodeExecutionResult, NodeStatus};
use dbt_temporal::worker::initialize_project;
use tempfile::TempDir;

use super::fault_engine::{FaultHandle, FaultInjectingEngine};

/// Project (and profile) name used by every harness fixture.
pub const PROJECT: &str = "spike";

/// Build a raw DuckDB `AdapterEngine` (no project/loader). Used by the driver
/// smoke test to isolate "does the ADBC driver load" from the dbt path.
pub fn raw_engine() -> Arc<dyn dbt_adapter::AdapterEngine> {
    let cfg = DuckDbConfig {
        path: Some(":memory:".to_string()),
        schema: Some("main".to_string()),
        ..Default::default()
    };
    let db_config = DbConfig::DuckDB(Box::new(cfg));
    dbt_temporal::worker::adapter::build_adapter_engine(
        &db_config,
        ResolvedQuoting::default(),
        None,
    )
    .expect("build duckdb adapter engine")
}

/// Run a single SQL statement against a fresh DuckDB connection, returning
/// whether it succeeded. Used only by the driver smoke test.
pub fn raw_query_ok(engine: &Arc<dyn dbt_adapter::AdapterEngine>, sql: &str) -> bool {
    let cts = CancellationTokenSource::new();
    let mut conn = engine
        .new_connection(None, None)
        .expect("open duckdb connection (driver dlopen)");
    let ctx = QueryCtx::new("smoke");
    engine
        .execute(None, conn.as_mut(), &ctx, sql, cts.token())
        .is_ok()
}

/// How `Harness::build_inner` should produce `profiles.yml`.
enum ProfileMode {
    /// The harness's own generated profile (`schema: main`, fixed target).
    Default { unreachable: bool },
    /// A caller-supplied template with a `{DB_PATH}` placeholder.
    Template(String),
}

/// A loaded DuckDB project ready to execute individual nodes against.
pub struct Harness {
    activities: DbtActivities,
    faults: FaultHandle,
    _project: TempDir,
    _profiles: TempDir,
    _db: TempDir,
}

impl Harness {
    /// Write a minimal project whose `models/` contains the given `(name, sql)`
    /// files, load it against an embedded DuckDB, and return a harness that can
    /// execute individual nodes.
    ///
    /// The DuckDB database is a temp file (not `:memory:`) so state persists
    /// across the per-node connections that `execute_node` opens.
    pub async fn build(models: &[(&str, &str)]) -> Self {
        Self::build_files_inner(&model_files(models), false).await
    }

    /// Like [`build`](Self::build) but points DuckDB at an unopenable path, so
    /// the first query fails with a transient IO/connection error — the
    /// "warehouse briefly unreachable" case that must classify as retryable.
    pub async fn build_unreachable(models: &[(&str, &str)]) -> Self {
        Self::build_files_inner(&model_files(models), true).await
    }

    /// Load a project from arbitrary `(relative_path, content)` files — e.g.
    /// `("models/m.sql", …)`, `("tests/t.sql", …)`, `("models/schema.yml", …)`
    /// — so tests, seeds, and schema definitions can be exercised, not just
    /// bare models.
    pub async fn build_files(files: &[(&str, &str)]) -> Self {
        let owned: Vec<(String, String)> = files
            .iter()
            .map(|(p, c)| ((*p).to_string(), (*c).to_string()))
            .collect();
        Self::build_files_inner(&owned, false).await
    }

    /// Like [`build_files`](Self::build_files), but with full control over
    /// `profiles.yml` — e.g. to exercise `env_var()`-driven profile fields
    /// (`rebuild_adapter_engine_with_env`). `profile_yml` must contain the
    /// literal placeholder `{DB_PATH}` wherever the DuckDB file path goes;
    /// the harness substitutes its own managed temp path.
    pub async fn build_files_with_profile(files: &[(&str, &str)], profile_yml: &str) -> Self {
        let owned: Vec<(String, String)> = files
            .iter()
            .map(|(p, c)| ((*p).to_string(), (*c).to_string()))
            .collect();
        Self::build_inner(&owned, ProfileMode::Template(profile_yml.to_string())).await
    }

    async fn build_files_inner(files: &[(String, String)], unreachable: bool) -> Self {
        Self::build_inner(files, ProfileMode::Default { unreachable }).await
    }

    async fn build_inner(files: &[(String, String)], profile_mode: ProfileMode) -> Self {
        let project = TempDir::new().unwrap();
        let profiles = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();

        std::fs::write(
            project.path().join("dbt_project.yml"),
            format!("name: {PROJECT}\nversion: \"1.0.0\"\nprofile: {PROJECT}\n"),
        )
        .unwrap();
        for (relpath, content) in files {
            let full = project.path().join(relpath);
            if let Some(parent) = full.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(full, content).unwrap();
        }

        let profile_yml = match profile_mode {
            ProfileMode::Default { unreachable } => {
                // An unreachable DB uses a path whose parent dir is absent, so
                // DuckDB fails to open it with a transient "IO Error: Cannot
                // open file".
                let db_path = if unreachable {
                    db.path().join("unreachable").join("spike.duckdb")
                } else {
                    db.path().join("spike.duckdb")
                };
                format!(
                    "{PROJECT}:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      \
                     path: \"{}\"\n      schema: main\n      threads: 1\n",
                    db_path.display()
                )
            }
            ProfileMode::Template(template) => {
                let db_path = db.path().join("spike.duckdb");
                template.replace("{DB_PATH}", &db_path.display().to_string())
            }
        };
        std::fs::write(profiles.path().join("profiles.yml"), profile_yml).unwrap();

        let config = build_config(project.path(), profiles.path());
        let mut state = initialize_project(project.path(), &config, None)
            .await
            .expect("initialize duckdb project");

        // Wrap the real DuckDB engine so tests can inject warehouse faults
        // (connection drops, throttling, deadlocks) at the adapter boundary.
        let faults = FaultHandle::default();
        let inner_engine = Arc::clone(&state.adapter_engine);
        state.adapter_engine = Arc::new(FaultInjectingEngine::new(inner_engine, faults.clone()));

        let registry =
            ProjectRegistry::new(BTreeMap::from([(PROJECT.to_string(), Arc::new(state))]));
        let activities = DbtActivities {
            registry: Arc::new(registry),
            artifact_store: None,
            search_attr_config: SearchAttributeConfig(BTreeMap::new()),
            registered_attrs: RegisteredSearchAttributes(BTreeSet::new()),
            write_run_log: WriteRunLog(false),
            write_artifacts: WriteArtifacts(false),
            write_catalog: WriteCatalog(false),
            priority_scheduling: PriorityScheduling(false),
        };

        Self {
            activities,
            faults,
            _project: project,
            _profiles: profiles,
            _db: db,
        }
    }

    /// Handle for injecting warehouse faults (connection drops, throttling,
    /// deadlocks) so error classification and retry can be tested with
    /// realistic, warehouse-shaped failures.
    pub const fn faults(&self) -> &FaultHandle {
        &self.faults
    }

    /// The project's real `WorkerState` — for tests that need to call
    /// worker-internal functions (e.g. `worker::profile::rebuild_adapter_engine_with_env`)
    /// directly rather than through `execute_node`/hooks.
    pub fn state(&self) -> &dbt_temporal::worker_state::WorkerState {
        self.activities
            .registry
            .get(Some(PROJECT))
            .expect("project registered")
    }

    /// Execute one model by name, returning the node result or a classified error.
    pub async fn run(&self, model: &str) -> Result<NodeExecutionResult, anyhow::Error> {
        self.run_uid(&format!("model.{PROJECT}.{model}")).await
    }

    /// Execute a node by its full unique id (e.g. `test.spike.my_test`) under
    /// the `build` command, so tests and unit tests run too.
    pub async fn run_uid(&self, unique_id: &str) -> Result<NodeExecutionResult, anyhow::Error> {
        self.run_uid_with_env(unique_id, &BTreeMap::new()).await
    }

    /// Like [`run_uid`](Self::run_uid), with per-workflow `env` overrides — for
    /// exercising the `rebuild_adapter_engine_with_env` integration inside
    /// `execute_node_inner` (as opposed to calling the rebuild fn directly).
    pub async fn run_uid_with_env(
        &self,
        unique_id: &str,
        env: &BTreeMap<String, String>,
    ) -> Result<NodeExecutionResult, anyhow::Error> {
        let input = serde_json::from_value(serde_json::json!({
            "unique_id": unique_id,
            "invocation_id": uuid::Uuid::new_v4().to_string(),
            "project": PROJECT,
            "command": "build",
            "env": env,
        }))
        .unwrap();
        execute_node_inner(&self.activities, input).await
    }

    /// Execute a node by full unique id, expecting failure, returning the
    /// classified error.
    pub async fn run_err_uid(&self, unique_id: &str) -> DbtTemporalError {
        let err = self
            .run_uid(unique_id)
            .await
            .expect_err(&format!("{unique_id} should error"));
        err.downcast::<DbtTemporalError>()
            .unwrap_or_else(|e| panic!("expected DbtTemporalError from {unique_id}, got: {e:#}"))
    }

    /// Like [`run_err_uid`](Self::run_err_uid), with per-workflow `env` overrides.
    pub async fn run_err_uid_with_env(
        &self,
        unique_id: &str,
        env: &BTreeMap<String, String>,
    ) -> DbtTemporalError {
        let err = self
            .run_uid_with_env(unique_id, env)
            .await
            .expect_err(&format!("{unique_id} should error"));
        err.downcast::<DbtTemporalError>()
            .unwrap_or_else(|e| panic!("expected DbtTemporalError from {unique_id}, got: {e:#}"))
    }

    /// Execute a model expected to succeed, asserting `Success` and returning the result.
    pub async fn run_ok(&self, model: &str) -> NodeExecutionResult {
        let result = self
            .run(model)
            .await
            .unwrap_or_else(|e| panic!("model {model} should succeed: {e:#}"));
        assert_eq!(result.status, NodeStatus::Success, "{result:?}");
        result
    }

    /// Run the `on-run-start` (or `on-run-end`, with empty `node_results`)
    /// project hooks for this project, returning the classified error on
    /// failure.
    pub async fn run_hooks(&self, phase: &str) -> Result<(), anyhow::Error> {
        let input = serde_json::from_value(serde_json::json!({
            "phase": phase,
            "project": PROJECT,
            "invocation_id": uuid::Uuid::new_v4().to_string(),
        }))
        .unwrap();
        run_project_hooks_inner(&self.activities, input).await
    }

    /// Run hooks for `phase`, expecting failure, returning the classified error.
    pub async fn run_hooks_err(&self, phase: &str) -> DbtTemporalError {
        let err = self
            .run_hooks(phase)
            .await
            .expect_err(&format!("hooks for {phase} should error"));
        err.downcast::<DbtTemporalError>()
            .unwrap_or_else(|e| panic!("expected DbtTemporalError from {phase} hooks, got: {e:#}"))
    }

    /// Execute a model expected to fail, returning the classified error.
    pub async fn run_err(&self, model: &str) -> DbtTemporalError {
        let err = self
            .run(model)
            .await
            .expect_err(&format!("model {model} should error"));
        err.downcast::<DbtTemporalError>()
            .unwrap_or_else(|e| panic!("expected DbtTemporalError from {model}, got: {e:#}"))
    }
}

/// Map `(name, sql)` model pairs to `(models/name.sql, sql)` project files.
fn model_files(models: &[(&str, &str)]) -> Vec<(String, String)> {
    models
        .iter()
        .map(|(name, sql)| (format!("models/{name}.sql"), (*sql).to_string()))
        .collect()
}

fn build_config(
    project_dir: &std::path::Path,
    profiles_dir: &std::path::Path,
) -> DbtTemporalConfig {
    DbtTemporalConfig {
        temporal_address: "http://127.0.0.1:7233".to_string(),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: "duckdb-scenarios".to_string(),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: vec![project_dir.to_string_lossy().to_string()],
        dbt_profiles_dir: Some(profiles_dir.to_string_lossy().to_string()),
        dbt_target: None,
        health_file: None,
        health_port: None,
        write_artifacts: false,
        write_catalog: false,
        artifact_store: std::env::temp_dir().to_string_lossy().to_string(),
        search_attributes: BTreeMap::new(),
        write_run_log: false,
        worker_tuning: WorkerTuningConfig::Fixed {
            max_concurrent_workflow_tasks: 1,
            max_concurrent_activities: 1,
            max_concurrent_local_activities: 1,
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
