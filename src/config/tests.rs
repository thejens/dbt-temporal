use super::*;

use crate::config::test_util::with_env;

/// The pool is sized from this, so it has to follow whichever tuner is
/// active: fixed slots, or the resource tuner's ceiling.
#[test]
fn max_activity_slots_follows_the_active_tuner() {
    let fixed = WorkerTuningConfig::Fixed {
        max_concurrent_workflow_tasks: 200,
        max_concurrent_activities: 25,
        max_concurrent_local_activities: 10,
    };
    assert_eq!(fixed.max_activity_slots(), 25);

    let resource = WorkerTuningConfig::ResourceBased {
        target_mem_usage: 0.8,
        target_cpu_usage: 0.9,
        activity_min_slots: 1,
        activity_max_slots: 64,
    };
    assert_eq!(resource.max_activity_slots(), 64);
}

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
    "ALLOWED_ENV_OVERRIDES",
    "ALLOWED_TARGETS",
    "WORKER_TUNER",
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
        assert_eq!(cfg.override_policy, OverridePolicy::default());
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
            assert_eq!(cfg.temporal_api_key.as_ref().map(Secret::expose), Some("secret"));
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

/// A config is the natural thing to log with `{:?}` at start-up; none of
/// its credentials may come out.
#[test]
fn debug_output_holds_no_credentials() {
    let project = make_project_dir();
    let project_str = project.path().to_str().unwrap();
    with_env(
        &env_with_project(
            project_str,
            vec![
                ("TEMPORAL_API_KEY", Some("api-key-sentinel")),
                ("TEMPORAL_METRICS_EXPORTER", Some("otlp")),
                ("OTEL_EXPORTER_OTLP_ENDPOINT", Some("http://collector:4317")),
                ("TEMPORAL_METRICS_OTLP_HEADERS", Some("x-api-key=header-sentinel")),
            ],
        ),
        || {
            let cfg = DbtTemporalConfig::from_env()?;
            let shown = format!("{cfg:?} {cfg:#?}");
            for sentinel in ["api-key-sentinel", "header-sentinel"] {
                assert!(!shown.contains(sentinel), "{sentinel} leaked into Debug output");
            }
            assert!(shown.contains("x-api-key"), "header names stay visible");
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
            assert_eq!(headers.get("x-api-key").map(Secret::expose), Some("abc"));
            assert_eq!(headers.get("x-team").map(Secret::expose), Some("data"));
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

#[test]
fn from_env_reads_the_override_allowlists() {
    let project = make_project_dir();
    let project_str = project.path().to_str().unwrap();
    with_env(
        &env_with_project(
            project_str,
            vec![
                ("ALLOWED_ENV_OVERRIDES", Some("DB_HOST,DB_SCHEMA")),
                ("ALLOWED_TARGETS", Some("")),
            ],
        ),
        || {
            let cfg = DbtTemporalConfig::from_env()?;
            assert_eq!(
                cfg.override_policy,
                OverridePolicy::new(Some(&["DB_HOST", "DB_SCHEMA"]), Some(&[]))
            );
            Ok(())
        },
    )
    .unwrap();
}
