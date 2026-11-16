use super::*;
use crate::config::Secret;

#[test]
fn identity_names_the_host() {
    assert_eq!(format_identity(1, Some("dbt-7f5b6dc85c-dzwmx")), "1@dbt-7f5b6dc85c-dzwmx");
    assert_eq!(format_identity(42, Some("box\n")), "42@box");
    assert_eq!(format_identity(1, Some(" ")), "1@dbt-temporal");
    assert_eq!(format_identity(1, None), "1@dbt-temporal");
}

fn test_config() -> DbtTemporalConfig {
    DbtTemporalConfig {
        temporal_address: "localhost:7233".into(),
        temporal_namespace: "default".into(),
        temporal_task_queue: "test".into(),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: vec![],
        dbt_profiles_dir: None,
        dbt_target: None,
        health_file: None,
        health_port: None,
        write_artifacts: false,
        write_catalog: false,
        artifact_store: "/tmp/dbt-artifacts".into(),
        override_policy: crate::config::OverridePolicy::default(),
        search_attributes: std::collections::BTreeMap::new(),
        write_run_log: true,
        worker_tuning: WorkerTuningConfig::Fixed {
            max_concurrent_workflow_tasks: 200,
            max_concurrent_activities: 100,
            max_concurrent_local_activities: 100,
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

#[test]
fn build_worker_options_fixed() {
    let config = test_config();
    // Just verify it doesn't panic — WorkerOptions doesn't expose fields for inspection.
    let _opts = build_worker_options(&config);
}

#[test]
fn build_telemetry_options_none_has_no_metrics() -> Result<()> {
    let opts = build_telemetry_options(&test_config())?;
    assert!(opts.metrics.is_none());
    Ok(())
}

#[tokio::test]
async fn build_telemetry_options_prometheus_binds_and_sets_meter() -> Result<()> {
    let mut config = test_config();
    // Port 0 → OS-assigned port, so parallel test runs don't collide.
    config.temporal_metrics = TemporalMetricsConfig::Prometheus {
        socket_addr: "127.0.0.1:0".parse()?,
    };
    let opts = build_telemetry_options(&config)?;
    assert!(opts.metrics.is_some());
    Ok(())
}

#[test]
fn build_telemetry_options_otlp_rejects_invalid_url() {
    let mut config = test_config();
    config.temporal_metrics = TemporalMetricsConfig::Otlp {
        url: "not a url".into(),
        use_http: false,
        headers: std::collections::BTreeMap::new(),
    };
    assert!(build_telemetry_options(&config).is_err());
}

#[tokio::test]
async fn build_telemetry_options_otlp_grpc_sets_meter() -> Result<()> {
    let mut config = test_config();
    // Building the exporter doesn't connect — the collector doesn't need to exist.
    config.temporal_metrics = TemporalMetricsConfig::Otlp {
        url: "http://127.0.0.1:4317".into(),
        use_http: false,
        headers: std::collections::BTreeMap::from([("x-key".to_string(), Secret::new("v"))]),
    };
    let opts = build_telemetry_options(&config)?;
    assert!(opts.metrics.is_some());
    Ok(())
}

#[test]
fn build_worker_options_resource_based() {
    let mut config = test_config();
    config.worker_tuning = WorkerTuningConfig::ResourceBased {
        target_mem_usage: 0.7,
        target_cpu_usage: 0.85,
        activity_min_slots: 2,
        activity_max_slots: 300,
    };
    let _opts = build_worker_options(&config);
}

#[test]
fn normalize_address_adds_scheme_when_missing() -> Result<()> {
    let url = normalize_address("localhost:7233")?;
    assert_eq!(url.scheme(), "http");
    assert_eq!(url.host_str(), Some("localhost"));
    assert_eq!(url.port(), Some(7233));
    Ok(())
}

#[test]
fn normalize_address_preserves_existing_scheme() -> Result<()> {
    let url = normalize_address("https://example.com:7233")?;
    assert_eq!(url.scheme(), "https");
    assert_eq!(url.host_str(), Some("example.com"));
    Ok(())
}

#[test]
fn normalize_address_rejects_garbage() {
    assert!(normalize_address("").is_err());
}

#[test]
fn build_tls_options_returns_none_without_credentials() -> Result<()> {
    let config = test_config();
    let opts = build_tls_options(&config)?;
    assert!(opts.is_none());
    Ok(())
}

#[test]
fn build_tls_options_returns_some_with_api_key_only() -> Result<()> {
    // API-key auth still requires the encrypted transport, so we wrap it in
    // a TlsOptions even though no client cert is set.
    let mut config = test_config();
    config.temporal_api_key = Some(Secret::new("secret"));
    let opts = build_tls_options(&config)?;
    let opts = opts.expect("expected TlsOptions for api-key auth");
    assert!(opts.client_tls_options.is_none());
    Ok(())
}

#[test]
fn build_tls_options_reads_cert_and_key_files() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, b"cert-bytes")?;
    std::fs::write(&key_path, b"key-bytes")?;

    let mut config = test_config();
    config.temporal_tls_cert = Some(cert_path.to_string_lossy().into_owned());
    config.temporal_tls_key = Some(key_path.to_string_lossy().into_owned());

    let opts =
        build_tls_options(&config)?.expect("expected TlsOptions when both cert and key are set");
    let client_tls = opts
        .client_tls_options
        .expect("expected ClientTlsOptions for mTLS");
    assert_eq!(client_tls.client_cert, b"cert-bytes");
    assert_eq!(client_tls.client_private_key, b"key-bytes");
    Ok(())
}

#[test]
fn build_tls_options_rejects_only_one_of_cert_or_key() {
    let mut only_cert = test_config();
    only_cert.temporal_tls_cert = Some("/nope".into());
    let err = build_tls_options(&only_cert).unwrap_err();
    assert!(err.to_string().contains("both be set"), "got: {err}");

    let mut only_key = test_config();
    only_key.temporal_tls_key = Some("/nope".into());
    let err = build_tls_options(&only_key).unwrap_err();
    assert!(err.to_string().contains("both be set"), "got: {err}");
}

#[test]
fn build_tls_options_reports_missing_cert_file() {
    let mut config = test_config();
    config.temporal_tls_cert = Some("/dbtt/does-not-exist/cert.pem".into());
    config.temporal_tls_key = Some("/dbtt/does-not-exist/key.pem".into());
    let err = build_tls_options(&config).unwrap_err();
    assert!(err.to_string().contains("TEMPORAL_TLS_CERT"), "got: {err}");
}

#[test]
fn build_worker_options_with_deployment_name() {
    let mut config = test_config();
    config.deployment_name = Some("dbt-temporal-prod".into());
    let opts = build_worker_options(&config);

    let deployment = opts.deployment_options;
    assert!(deployment.use_worker_versioning);
    assert_eq!(deployment.version.deployment_name, "dbt-temporal-prod");
    // A versioned worker that reports Unspecified has its task completions
    // rejected by the server — the Rust SDK never sets a per-workflow
    // behavior, so this default is the only thing standing in for one.
    assert_eq!(deployment.default_versioning_behavior, Some(VersioningBehavior::Pinned));
}

#[test]
fn build_worker_options_without_deployment_name_is_unversioned() {
    // Core rejects a default_versioning_behavior when versioning is off, so
    // the unversioned path must leave it unset.
    let opts = build_worker_options(&test_config());
    let deployment = opts.deployment_options;
    assert!(!deployment.use_worker_versioning);
    assert_eq!(deployment.default_versioning_behavior, None);
}

#[test]
fn build_worker_options_applies_poller_autoscaling() {
    let mut config = test_config();
    config.poller_autoscaling = Some(crate::config::PollerAutoscalingConfig {
        minimum: 2,
        maximum: 64,
        initial: 8,
    });
    let opts = build_worker_options(&config);
    let expected = PollerBehavior::Autoscaling(
        AutoscalingOptions::builder()
            .minimum(2)
            .maximum(64)
            .initial(8)
            .build(),
    );
    assert_eq!(opts.workflow_task_poller_behavior, Some(expected));
    assert_eq!(opts.activity_task_poller_behavior, Some(expected));
}

/// Unset is not "no autoscaling" — it is what makes the worker eligible for
/// the server-driven poller autoscaling enrollment, so it must stay `None`
/// rather than be filled in with an explicit default.
#[test]
fn build_worker_options_leaves_poller_behavior_unconfigured_by_default() {
    let opts = build_worker_options(&test_config());
    assert_eq!(opts.workflow_task_poller_behavior, None);
    assert_eq!(opts.activity_task_poller_behavior, None);
}

#[test]
fn build_worker_options_applies_optional_rate_limits() {
    let mut config = test_config();
    config.max_worker_activities_per_second = Some(50.0);
    config.max_task_queue_activities_per_second = Some(200.0);
    config.graceful_shutdown_secs = Some(15);
    let opts = build_worker_options(&config);
    assert_eq!(opts.max_worker_activities_per_second, Some(50.0));
    assert_eq!(opts.max_task_queue_activities_per_second, Some(200.0));
    assert_eq!(opts.graceful_shutdown_period, Some(Duration::from_secs(15)));
}
