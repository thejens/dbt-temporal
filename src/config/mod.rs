pub mod discovery;
pub mod tuning;

use std::collections::BTreeMap;

pub use self::discovery::is_remote_source;

/// Configuration for the dbt-temporal worker, read from environment variables.
#[derive(Debug, Clone)]
pub struct DbtTemporalConfig {
    // Temporal connection
    pub temporal_address: String,
    pub temporal_namespace: String,
    pub temporal_task_queue: String,
    pub temporal_api_key: Option<String>,
    pub temporal_tls_cert: Option<String>,
    pub temporal_tls_key: Option<String>,

    // dbt projects — local paths or remote URLs (git+https://, gs://, s3://)
    pub dbt_project_dirs: Vec<String>,
    pub dbt_profiles_dir: Option<String>,
    pub dbt_target: Option<String>,

    // Health check
    pub health_file: Option<String>,
    pub health_port: Option<u16>,

    // Artifact storage
    /// Whether to write artifacts (run_results.json, manifest.json, log.txt) after each run.
    /// Controlled by `WRITE_ARTIFACTS` (default: false).
    pub write_artifacts: bool,
    /// Artifact store location. Accepts a local path or a cloud URL (`gs://…`, `s3://…`).
    /// Controlled by `ARTIFACT_STORE` (default: `/tmp/dbt-artifacts`).
    pub artifact_store: String,

    // Search attributes: static key-value pairs upserted on every workflow run.
    pub search_attributes: BTreeMap<String, String>,

    // Logging
    /// Whether to write a CLI-style run log to the artifact store alongside other artifacts.
    pub write_run_log: bool,

    // Worker tuning
    pub worker_tuning: WorkerTuningConfig,

    // Worker polling & timing
    /// How long a workflow task can sit on the sticky queue before falling back
    /// to the normal queue. Controlled by `WORKER_STICKY_QUEUE_TIMEOUT_SECS` (default: 10).
    pub sticky_queue_timeout_secs: u64,
    /// Ratio of non-sticky to sticky queue pollers (0.0–1.0).
    /// Higher values make the worker poll the normal queue more aggressively.
    /// Controlled by `WORKER_NONSTICKY_TO_STICKY_POLL_RATIO` (default: 0.2).
    pub nonsticky_to_sticky_poll_ratio: f32,
    /// Per-worker rate limit on activities/second. `None` = unlimited.
    /// Controlled by `WORKER_MAX_ACTIVITIES_PER_SECOND`.
    pub max_worker_activities_per_second: Option<f64>,
    /// Server-side rate limit on activities/second for the whole task queue. `None` = unlimited.
    /// Controlled by `WORKER_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND`.
    pub max_task_queue_activities_per_second: Option<f64>,
    /// Grace period before canceling in-flight activities on shutdown. `None` = no grace.
    /// Controlled by `WORKER_GRACEFUL_SHUTDOWN_SECS`.
    pub graceful_shutdown_secs: Option<u64>,
    /// Number of workflows to keep cached on sticky queues (LRU eviction).
    /// Controlled by `WORKER_MAX_CACHED_WORKFLOWS` (default: 1000).
    pub max_cached_workflows: usize,
}

/// Controls how the worker limits concurrent task execution.
#[derive(Debug, Clone)]
pub enum WorkerTuningConfig {
    /// Fixed-size slot limits per task type.
    Fixed {
        max_concurrent_workflow_tasks: usize,
        max_concurrent_activities: usize,
        max_concurrent_local_activities: usize,
    },
    /// Resource-based: dynamically adjusts slots using PID controllers that track
    /// system memory and CPU usage. Backs off when the machine is under pressure.
    ResourceBased {
        target_mem_usage: f64,
        target_cpu_usage: f64,
        activity_min_slots: usize,
        activity_max_slots: usize,
    },
}

/// Newtype wrapper for search attribute config.
#[derive(Debug, Clone)]
pub struct SearchAttributeConfig(pub BTreeMap<String, String>);

/// Set of search attribute names that are registered on the Temporal namespace.
/// Used to filter out unregistered attributes before upserting (prevents workflow task failures).
#[derive(Debug, Clone)]
pub struct RegisteredSearchAttributes(pub std::collections::BTreeSet<String>);

/// Newtype wrapper for run-log config.
#[derive(Debug, Clone)]
pub struct WriteRunLog(pub bool);

/// Newtype wrapper for artifact-writing config.
#[derive(Debug, Clone)]
pub struct WriteArtifacts(pub bool);

impl DbtTemporalConfig {
    /// Read configuration from environment variables.
    ///
    /// Project discovery:
    /// 1. `DBT_PROJECT_DIRS` set → split on `,`, use each entry (local path or remote URL)
    /// 2. `DBT_PROJECTS_DIR` set → scan for subdirs containing `dbt_project.yml`
    /// 3. `DBT_PROJECT_DIR` set → single project directory
    /// 4. None set → use cwd (single project or scan subdirs)
    pub fn from_env() -> Result<Self, String> {
        let dbt_project_dirs = discovery::discover_project_dirs()?;

        Ok(Self {
            temporal_address: std::env::var("TEMPORAL_ADDRESS")
                .unwrap_or_else(|_| "http://localhost:7233".to_string()),
            temporal_namespace: std::env::var("TEMPORAL_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            temporal_task_queue: std::env::var("TEMPORAL_TASK_QUEUE")
                .unwrap_or_else(|_| "dbt-tasks".to_string()),
            temporal_api_key: std::env::var("TEMPORAL_API_KEY").ok(),
            temporal_tls_cert: std::env::var("TEMPORAL_TLS_CERT").ok(),
            temporal_tls_key: std::env::var("TEMPORAL_TLS_KEY").ok(),
            dbt_project_dirs,
            dbt_profiles_dir: std::env::var("DBT_PROFILES_DIR").ok(),
            dbt_target: std::env::var("DBT_TARGET").ok(),

            health_file: std::env::var("HEALTH_FILE").ok(),
            health_port: std::env::var("HEALTH_PORT")
                .ok()
                .map(|v| v.parse::<u16>())
                .transpose()
                .map_err(|e| format!("invalid HEALTH_PORT: {e}"))?,
            write_artifacts: std::env::var("WRITE_ARTIFACTS")
                .map(|v| v == "1" || v.to_lowercase() == "true")
                .unwrap_or(false),
            artifact_store: std::env::var("ARTIFACT_STORE")
                .unwrap_or_else(|_| "/tmp/dbt-artifacts".to_string()),
            search_attributes: tuning::parse_search_attributes()?,
            write_run_log: std::env::var("WRITE_RUN_LOG")
                .map(|v| v != "0" && v.to_lowercase() != "false")
                .unwrap_or(true),
            worker_tuning: tuning::parse_worker_tuning()?,
            sticky_queue_timeout_secs: tuning::parse_env_u64(
                "WORKER_STICKY_QUEUE_TIMEOUT_SECS",
                10,
            )?,
            nonsticky_to_sticky_poll_ratio: tuning::parse_env_f32(
                "WORKER_NONSTICKY_TO_STICKY_POLL_RATIO",
                0.2,
            )?,
            max_worker_activities_per_second: tuning::parse_optional_env_f64(
                "WORKER_MAX_ACTIVITIES_PER_SECOND",
            )?,
            max_task_queue_activities_per_second: tuning::parse_optional_env_f64(
                "WORKER_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND",
            )?,
            graceful_shutdown_secs: tuning::parse_optional_env_u64(
                "WORKER_GRACEFUL_SHUTDOWN_SECS",
            )?,
            max_cached_workflows: tuning::parse_env_usize("WORKER_MAX_CACHED_WORKFLOWS", 1000)?,
        })
    }
}
