pub mod discovery;
pub mod tuning;

#[cfg(test)]
mod test_util;

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
