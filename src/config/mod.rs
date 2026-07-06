pub mod discovery;
pub mod tuning;

#[cfg(test)]
mod test_util;

use std::collections::BTreeMap;

pub use self::discovery::is_remote_source;

/// Configuration for the dbt-temporal worker, read from environment variables.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // Env-derived flags: independent toggles, not a state machine.
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
    /// Whether to also generate a `catalog.json` artifact (warehouse column
    /// metadata for the run's relations) when storing artifacts.
    /// Controlled by `WRITE_CATALOG` (default: false). Requires `WRITE_ARTIFACTS`.
    pub write_catalog: bool,
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
        headers: BTreeMap<String, String>,
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
                .context("invalid HEALTH_PORT")?,
            write_artifacts: std::env::var("WRITE_ARTIFACTS")
                .is_ok_and(|v| v == "1" || v.to_lowercase() == "true"),
            write_catalog: std::env::var("WRITE_CATALOG")
                .is_ok_and(|v| v == "1" || v.to_lowercase() == "true"),
            artifact_store: std::env::var("ARTIFACT_STORE")
                .unwrap_or_else(|_| "/tmp/dbt-artifacts".to_string()),
            search_attributes: tuning::parse_search_attributes()?,
            write_run_log: std::env::var("WRITE_RUN_LOG")
                .map_or(true, |v| v != "0" && v.to_lowercase() != "false"),
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
        })
    }
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use crate::config::test_util::with_env;

    /// Env vars touched by `from_env`. Tests blank-slate these between cases so
    /// host environment doesn't leak into assertions.
    const ALL_VARS: &[&str] = &[
        "TEMPORAL_ADDRESS",
        "TEMPORAL_NAMESPACE",
        "TEMPORAL_TASK_QUEUE",
        "TEMPORAL_API_KEY",
        "TEMPORAL_TLS_CERT",
        "TEMPORAL_TLS_KEY",
        "DBT_PROJECT_DIR",
        "DBT_PROJECT_DIRS",
        "DBT_PROJECTS_DIR",
        "DBT_PROFILES_DIR",
        "DBT_TARGET",
        "HEALTH_FILE",
        "HEALTH_PORT",
        "WRITE_ARTIFACTS",
        "WRITE_CATALOG",
        "ARTIFACT_STORE",
        "WRITE_RUN_LOG",
        "TEMPORAL_SEARCH_ATTRIBUTES",
        "WORKER_TUNING_MODE",
        "WORKER_STICKY_QUEUE_TIMEOUT_SECS",
        "WORKER_NONSTICKY_TO_STICKY_POLL_RATIO",
        "WORKER_MAX_ACTIVITIES_PER_SECOND",
        "WORKER_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND",
        "WORKER_GRACEFUL_SHUTDOWN_SECS",
        "WORKER_MAX_CACHED_WORKFLOWS",
        "TEMPORAL_DEPLOYMENT_NAME",
        "WORKER_POLLER_AUTOSCALING",
        "WORKER_POLLER_MIN",
        "WORKER_POLLER_MAX",
        "WORKER_POLLER_INITIAL",
        "TEMPORAL_METRICS_EXPORTER",
        "TEMPORAL_METRICS_PROMETHEUS_ADDR",
        "TEMPORAL_METRICS_OTLP_URL",
        "TEMPORAL_METRICS_OTLP_PROTOCOL",
        "TEMPORAL_METRICS_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "TEMPORAL_PRIORITY_SCHEDULING",
        "NEXUS_ENABLED",
    ];

    fn make_project_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("dbt_project.yml"), "name: t\nversion: 1\n").unwrap();
        dir
    }

    /// Combines a project dir + each `extra` override into the `with_env` form.
    fn env_with_project<'a>(
        project_dir: &'a str,
        extra: Vec<(&'a str, Option<&'a str>)>,
    ) -> Vec<(&'a str, Option<&'a str>)> {
        let mut vars: Vec<(&str, Option<&str>)> = ALL_VARS.iter().map(|k| (*k, None)).collect();
        // Override DBT_PROJECT_DIR last so the loop above clears any host value first.
        vars.push(("DBT_PROJECT_DIR", Some(project_dir)));
        vars.extend(extra);
        vars
    }

    #[test]
    fn from_env_uses_defaults_when_only_project_dir_set() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(&env_with_project(project_str, vec![]), || {
            let cfg = DbtTemporalConfig::from_env()?;
            assert_eq!(cfg.temporal_address, "http://localhost:7233");
            assert_eq!(cfg.temporal_namespace, "default");
            assert_eq!(cfg.temporal_task_queue, "dbt-tasks");
            assert!(cfg.temporal_api_key.is_none());
            assert!(cfg.temporal_tls_cert.is_none());
            assert!(cfg.temporal_tls_key.is_none());
            assert_eq!(cfg.dbt_project_dirs, vec![project_str.to_string()]);
            assert!(cfg.dbt_profiles_dir.is_none());
            assert!(cfg.dbt_target.is_none());
            assert!(cfg.health_file.is_none());
            assert!(cfg.health_port.is_none());
            assert!(!cfg.write_artifacts);
            assert_eq!(cfg.artifact_store, "/tmp/dbt-artifacts");
            assert!(cfg.write_run_log);
            assert!(cfg.search_attributes.is_empty());
            assert_eq!(cfg.sticky_queue_timeout_secs, 10);
            assert!((cfg.nonsticky_to_sticky_poll_ratio - 0.2).abs() < f32::EPSILON);
            assert!(cfg.max_worker_activities_per_second.is_none());
            assert!(cfg.max_task_queue_activities_per_second.is_none());
            assert!(cfg.graceful_shutdown_secs.is_none());
            assert_eq!(cfg.max_cached_workflows, 1000);
            assert!(matches!(cfg.worker_tuning, WorkerTuningConfig::Fixed { .. }));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn from_env_reads_temporal_overrides() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("TEMPORAL_ADDRESS", Some("http://temporal:7233")),
                    ("TEMPORAL_NAMESPACE", Some("ns")),
                    ("TEMPORAL_TASK_QUEUE", Some("q1")),
                    ("TEMPORAL_API_KEY", Some("secret")),
                    ("TEMPORAL_TLS_CERT", Some("/cert")),
                    ("TEMPORAL_TLS_KEY", Some("/key")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.temporal_address, "http://temporal:7233");
                assert_eq!(cfg.temporal_namespace, "ns");
                assert_eq!(cfg.temporal_task_queue, "q1");
                assert_eq!(cfg.temporal_api_key.as_deref(), Some("secret"));
                assert_eq!(cfg.temporal_tls_cert.as_deref(), Some("/cert"));
                assert_eq!(cfg.temporal_tls_key.as_deref(), Some("/key"));
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_parses_health_port() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("HEALTH_FILE", Some("/var/health")),
                    ("HEALTH_PORT", Some("8080")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.health_file.as_deref(), Some("/var/health"));
                assert_eq!(cfg.health_port, Some(8080));
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_rejects_invalid_health_port() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(project_str, vec![("HEALTH_PORT", Some("not_a_port"))]),
            || {
                assert!(DbtTemporalConfig::from_env().is_err());
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_parses_write_artifacts_truthy() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        for raw in ["1", "true", "TRUE", "True"] {
            with_env(&env_with_project(project_str, vec![("WRITE_ARTIFACTS", Some(raw))]), || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert!(cfg.write_artifacts, "write_artifacts should be true for {raw}");
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn from_env_treats_unrecognized_write_artifacts_as_false() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(&env_with_project(project_str, vec![("WRITE_ARTIFACTS", Some("yes"))]), || {
            let cfg = DbtTemporalConfig::from_env()?;
            assert!(!cfg.write_artifacts);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn from_env_write_run_log_defaults_true_unless_falsy() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        for (raw, expected) in [
            ("0", false),
            ("false", false),
            ("FALSE", false),
            ("1", true),
            ("anything-else", true),
        ] {
            with_env(&env_with_project(project_str, vec![("WRITE_RUN_LOG", Some(raw))]), || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.write_run_log, expected, "raw={raw}");
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn from_env_passes_through_artifact_store_path() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(project_str, vec![("ARTIFACT_STORE", Some("s3://bucket/key"))]),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.artifact_store, "s3://bucket/key");
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_reads_worker_tuning_overrides() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("WORKER_STICKY_QUEUE_TIMEOUT_SECS", Some("30")),
                    ("WORKER_NONSTICKY_TO_STICKY_POLL_RATIO", Some("0.5")),
                    ("WORKER_MAX_ACTIVITIES_PER_SECOND", Some("100.0")),
                    ("WORKER_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND", Some("250.0")),
                    ("WORKER_GRACEFUL_SHUTDOWN_SECS", Some("15")),
                    ("WORKER_MAX_CACHED_WORKFLOWS", Some("42")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.sticky_queue_timeout_secs, 30);
                assert!((cfg.nonsticky_to_sticky_poll_ratio - 0.5).abs() < f32::EPSILON);
                assert_eq!(cfg.max_worker_activities_per_second, Some(100.0));
                assert_eq!(cfg.max_task_queue_activities_per_second, Some(250.0));
                assert_eq!(cfg.graceful_shutdown_secs, Some(15));
                assert_eq!(cfg.max_cached_workflows, 42);
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_poller_autoscaling_disabled_by_default() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(&env_with_project(project_str, vec![]), || {
            let cfg = DbtTemporalConfig::from_env()?;
            assert!(cfg.poller_autoscaling.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn from_env_reads_poller_autoscaling() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("WORKER_POLLER_AUTOSCALING", Some("1")),
                    ("WORKER_POLLER_MIN", Some("2")),
                    ("WORKER_POLLER_MAX", Some("50")),
                    ("WORKER_POLLER_INITIAL", Some("8")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                let pa = cfg.poller_autoscaling.expect("autoscaling enabled");
                assert_eq!(pa.minimum, 2);
                assert_eq!(pa.maximum, 50);
                assert_eq!(pa.initial, 8);
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_rejects_inverted_poller_bounds() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("WORKER_POLLER_AUTOSCALING", Some("true")),
                    ("WORKER_POLLER_MIN", Some("10")),
                    ("WORKER_POLLER_MAX", Some("5")),
                ],
            ),
            || {
                assert!(DbtTemporalConfig::from_env().is_err());
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_metrics_default_none() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(&env_with_project(project_str, vec![]), || {
            let cfg = DbtTemporalConfig::from_env()?;
            assert_eq!(cfg.temporal_metrics, TemporalMetricsConfig::None);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn from_env_reads_prometheus_metrics() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("TEMPORAL_METRICS_EXPORTER", Some("prometheus")),
                    ("TEMPORAL_METRICS_PROMETHEUS_ADDR", Some("127.0.0.1:9999")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                let TemporalMetricsConfig::Prometheus { socket_addr } = cfg.temporal_metrics else {
                    anyhow::bail!("expected Prometheus metrics config");
                };
                assert_eq!(socket_addr.to_string(), "127.0.0.1:9999");
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_reads_otlp_metrics_with_fallback_endpoint() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("TEMPORAL_METRICS_EXPORTER", Some("otlp")),
                    ("OTEL_EXPORTER_OTLP_ENDPOINT", Some("http://collector:4317")),
                    ("TEMPORAL_METRICS_OTLP_HEADERS", Some("x-api-key=abc, x-team=data")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                let TemporalMetricsConfig::Otlp {
                    url,
                    use_http,
                    headers,
                } = cfg.temporal_metrics
                else {
                    anyhow::bail!("expected OTLP metrics config");
                };
                assert_eq!(url, "http://collector:4317");
                assert!(!use_http);
                assert_eq!(headers.get("x-api-key").map(String::as_str), Some("abc"));
                assert_eq!(headers.get("x-team").map(String::as_str), Some("data"));
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_otlp_metrics_without_endpoint_errors() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(project_str, vec![("TEMPORAL_METRICS_EXPORTER", Some("otlp"))]),
            || {
                assert!(DbtTemporalConfig::from_env().is_err());
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_reads_deployment_name() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![("TEMPORAL_DEPLOYMENT_NAME", Some("dbt-temporal-prod"))],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.deployment_name.as_deref(), Some("dbt-temporal-prod"));
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn from_env_nexus_enabled_defaults_false_and_parses_truthy() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(&env_with_project(project_str, vec![]), || {
            assert!(!DbtTemporalConfig::from_env()?.nexus_enabled);
            Ok(())
        })
        .unwrap();
        for raw in ["1", "true", "TRUE"] {
            with_env(&env_with_project(project_str, vec![("NEXUS_ENABLED", Some(raw))]), || {
                assert!(DbtTemporalConfig::from_env()?.nexus_enabled, "raw={raw}");
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn from_env_deployment_name_absent_is_none() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(&env_with_project(project_str, vec![]), || {
            let cfg = DbtTemporalConfig::from_env()?;
            assert!(cfg.deployment_name.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn from_env_propagates_dbt_target_and_profiles() {
        let project = make_project_dir();
        let project_str = project.path().to_str().unwrap();
        with_env(
            &env_with_project(
                project_str,
                vec![
                    ("DBT_PROFILES_DIR", Some("/profiles")),
                    ("DBT_TARGET", Some("prod")),
                ],
            ),
            || {
                let cfg = DbtTemporalConfig::from_env()?;
                assert_eq!(cfg.dbt_profiles_dir.as_deref(), Some("/profiles"));
                assert_eq!(cfg.dbt_target.as_deref(), Some("prod"));
                Ok(())
            },
        )
        .unwrap();
    }
}
