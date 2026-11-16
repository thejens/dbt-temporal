pub mod discovery;
pub mod override_policy;
pub mod secret;
pub mod tuning;

#[cfg(test)]
mod test_util;

use std::collections::BTreeMap;

pub use self::discovery::is_remote_source;
pub use self::override_policy::OverridePolicy;
pub use self::secret::Secret;

/// Configuration for the dbt-temporal worker, read from environment variables.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // Env-derived flags: independent toggles, not a state machine.
pub struct DbtTemporalConfig {
    // Temporal connection
    pub temporal_address: String,
    pub temporal_namespace: String,
    pub temporal_task_queue: String,
    pub temporal_api_key: Option<Secret>,
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
    /// Whether to also generate a `catalog.json` artifact (warehouse column
    /// metadata for the run's relations) when storing artifacts.
    /// Controlled by `WRITE_CATALOG` (default: false). Requires `WRITE_ARTIFACTS`.
    pub write_catalog: bool,
    /// Artifact store location. Accepts a local path or a cloud URL (`gs://…`, `s3://…`).
    /// Controlled by `ARTIFACT_STORE` (default: `/tmp/dbt-artifacts`).
    pub artifact_store: String,

    // Per-workflow overrides
    /// Which `env` names and `target`s a workflow may override. Controlled by
    /// `ALLOWED_ENV_OVERRIDES` and `ALLOWED_TARGETS` (default: unrestricted).
    pub override_policy: OverridePolicy,

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
    /// Temporal worker deployment name for versioned routing (optional).
    ///
    /// When set, the worker registers under `{deployment_name}/{version}` so
    /// Temporal can route tasks to the correct worker version during rolling
    /// deploys. Set to e.g. `dbt-temporal-prod`. Controlled by `TEMPORAL_DEPLOYMENT_NAME`.
    pub deployment_name: Option<String>,
    /// Poller autoscaling: scale the number of open task-queue poll calls from
    /// server backlog feedback instead of the SDK's fixed default (5). Wide DAG
    /// levels schedule many activities at once; more pollers pick them up faster.
    /// `None` keeps the SDK default. Controlled by `WORKER_POLLER_AUTOSCALING`
    /// (+ `WORKER_POLLER_MIN`/`WORKER_POLLER_MAX`/`WORKER_POLLER_INITIAL`).
    pub poller_autoscaling: Option<PollerAutoscalingConfig>,
    /// Worker metrics export (slot usage, schedule-to-start latency, poll counts,
    /// sticky cache hit rate, …). Controlled by `TEMPORAL_METRICS_EXPORTER`.
    pub temporal_metrics: TemporalMetricsConfig,
    /// Priority-aware activity scheduling: per-node priority keys from
    /// critical-path depth plus a per-run fairness key, so blocking-path
    /// models run first under slot scarcity and concurrent runs share workers
    /// proportionally. Requires Temporal server >= 1.31 to take effect
    /// (older servers ignore the fields). Controlled by
    /// `TEMPORAL_PRIORITY_SCHEDULING` (default: off).
    pub priority_scheduling: bool,
    /// Reserved flag to serve `dbt_run` as a Nexus operation for cross-namespace
    /// invocation. Controlled by `NEXUS_ENABLED` (default: off).
    ///
    /// The Temporal Rust SDK does not yet expose a way to register/serve Nexus
    /// operation handlers (only the caller side, `NexusOperationOptions`, exists),
    /// so enabling this currently only logs a warning at startup. The flag is kept
    /// stable now so wiring the handler needs no config change once the SDK ships
    /// handler support.
    pub nexus_enabled: bool,
    /// Offload large Temporal payloads to the artifact store instead of
    /// carrying them in workflow history. `None` (default) keeps every payload
    /// inline. Controlled by `EXTERNAL_PAYLOAD_STORAGE`.
    pub external_payload_storage: Option<ExternalPayloadStorageConfig>,
}

/// Settings for offloading large Temporal payloads to the artifact store.
///
/// The SDK feature behind this is experimental, so it is opt-in. Payloads are
/// written to the store named by `ARTIFACT_STORE`, under a prefix of their own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalPayloadStorageConfig {
    /// Smallest encoded payload, in bytes, that is offloaded. `None` leaves the
    /// SDK's own default (256 KiB) in force. Controlled by
    /// `EXTERNAL_PAYLOAD_THRESHOLD_BYTES`; `0` offloads every payload.
    pub threshold_bytes: Option<usize>,
}

/// How the worker exports Temporal SDK metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalMetricsConfig {
    /// No metrics export (default).
    None,
    /// Serve a Prometheus scrape endpoint from the worker process.
    /// Controlled by `TEMPORAL_METRICS_PROMETHEUS_ADDR` (default `0.0.0.0:9464`).
    Prometheus { socket_addr: std::net::SocketAddr },
    /// Push metrics to an OpenTelemetry collector over OTLP.
    Otlp {
        /// Collector endpoint. From `TEMPORAL_METRICS_OTLP_URL`, falling back
        /// to the standard `OTEL_EXPORTER_OTLP_ENDPOINT`.
        url: String,
        /// `grpc` (default) or `http`. From `TEMPORAL_METRICS_OTLP_PROTOCOL`.
        use_http: bool,
        /// Extra headers (e.g. auth). From `TEMPORAL_METRICS_OTLP_HEADERS`
        /// as comma-separated `key=value` pairs.
        headers: BTreeMap<String, Secret>,
    },
}

/// Bounds for task-queue poller autoscaling (applies to both workflow and
/// activity task pollers).
#[derive(Debug, Clone)]
pub struct PollerAutoscalingConfig {
    /// Poll calls always kept open (≥ 1).
    pub minimum: usize,
    /// Upper bound on concurrently open poll calls.
    pub maximum: usize,
    /// Poll calls opened before scaling feedback kicks in.
    pub initial: usize,
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

impl WorkerTuningConfig {
    /// The most activities this worker runs at once.
    ///
    /// Every activity that touches the warehouse does so on the dbt pool, so
    /// this is also how many pool threads the worker can use: fewer, and an
    /// activity holding a slot would queue for a thread while its timeout runs.
    pub const fn max_activity_slots(&self) -> usize {
        match self {
            Self::Fixed {
                max_concurrent_activities,
                ..
            } => *max_concurrent_activities,
            Self::ResourceBased {
                activity_max_slots, ..
            } => *activity_max_slots,
        }
    }
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

/// Newtype: whether catalog.json generation is enabled (`WRITE_CATALOG`).
#[derive(Debug, Clone, Copy)]
pub struct WriteCatalog(pub bool);

/// Newtype wrapper for priority-scheduling config.
#[derive(Debug, Clone)]
pub struct PriorityScheduling(pub bool);

impl DbtTemporalConfig {
    /// Read configuration from environment variables.
    ///
    /// Project discovery:
    /// 1. `DBT_PROJECT_DIRS` set → split on `,`, use each entry (local path or remote URL)
    /// 2. `DBT_PROJECTS_DIR` set → scan for subdirs containing `dbt_project.yml`
    /// 3. `DBT_PROJECT_DIR` set → single project directory
    /// 4. None set → use cwd (single project or scan subdirs)
    pub fn from_env() -> anyhow::Result<Self> {
        use anyhow::Context;
        let dbt_project_dirs = discovery::discover_project_dirs()?;

        Ok(Self {
            temporal_address: std::env::var("TEMPORAL_ADDRESS")
                .unwrap_or_else(|_| "http://localhost:7233".to_string()),
            temporal_namespace: std::env::var("TEMPORAL_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            temporal_task_queue: std::env::var("TEMPORAL_TASK_QUEUE")
                .unwrap_or_else(|_| "dbt-tasks".to_string()),
            temporal_api_key: std::env::var("TEMPORAL_API_KEY").ok().map(Secret::new),
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
                .context("invalid HEALTH_PORT")?,
            write_artifacts: std::env::var("WRITE_ARTIFACTS")
                .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            write_catalog: std::env::var("WRITE_CATALOG")
                .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            artifact_store: std::env::var("ARTIFACT_STORE")
                .unwrap_or_else(|_| "/tmp/dbt-artifacts".to_string()),
            override_policy: OverridePolicy::from_env()?,
            search_attributes: tuning::parse_search_attributes()?,
            write_run_log: std::env::var("WRITE_RUN_LOG")
                .map_or(true, |v| v != "0" && !v.eq_ignore_ascii_case("false")),
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
            deployment_name: std::env::var("TEMPORAL_DEPLOYMENT_NAME").ok(),
            poller_autoscaling: tuning::parse_poller_autoscaling()?,
            temporal_metrics: tuning::parse_temporal_metrics()?,
            priority_scheduling: std::env::var("TEMPORAL_PRIORITY_SCHEDULING")
                .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            nexus_enabled: std::env::var("NEXUS_ENABLED")
                .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            external_payload_storage: tuning::parse_external_payload_storage()?,
        })
    }
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests;
