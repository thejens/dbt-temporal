use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use temporalio_client::Url;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_common::worker::{
    VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion,
};
use temporalio_sdk::WorkerOptions;
use temporalio_sdk::runtime::worker_tuner::{FixedSizeSlotSupplier, TunerHolder, WorkerTuner};
use temporalio_sdk::runtime::{AutoscalingOptions, PollerBehavior};
use tracing::info;

use crate::config::{DbtTemporalConfig, TemporalMetricsConfig, WorkerTuningConfig};

/// Build `TelemetryOptions` for the Temporal runtime, starting a metrics
/// exporter when configured.
///
/// The Prometheus scrape server is spawned onto the current Tokio runtime and
/// lives for the rest of the process — workers run until shutdown, so there is
/// nothing to reclaim earlier (dropping the returned abort handle does not stop
/// the task).
pub fn build_telemetry_options(config: &DbtTemporalConfig) -> Result<TelemetryOptions> {
    use temporalio_common::telemetry::metrics::CoreMeter;

    let builder = TelemetryOptions::builder();
    match &config.temporal_metrics {
        TemporalMetricsConfig::None => Ok(builder.build()),
        TemporalMetricsConfig::Prometheus { socket_addr } => {
            let server = temporalio_common::telemetry::start_prometheus_metric_exporter(
                temporalio_common::telemetry::PrometheusExporterOptions::builder()
                    .socket_addr(*socket_addr)
                    .build(),
            )
            .context("starting Prometheus metrics exporter")?;
            info!(addr = %server.bound_addr, "worker metrics: Prometheus scrape endpoint started");
            Ok(builder.metrics(server.meter as Arc<dyn CoreMeter>).build())
        }
        TemporalMetricsConfig::Otlp {
            url,
            use_http,
            headers,
        } => {
            let parsed: Url = url
                .parse()
                .with_context(|| format!("invalid OTLP metrics URL '{url}'"))?;
            let protocol = if *use_http {
                temporalio_common::telemetry::OtlpProtocol::Http
            } else {
                temporalio_common::telemetry::OtlpProtocol::Grpc
            };
            let meter = temporalio_common::telemetry::build_otlp_metric_exporter(
                temporalio_common::telemetry::OtelCollectorOptions::builder()
                    .url(parsed)
                    .headers(
                        headers
                            .iter()
                            .map(|(k, v)| (k.clone(), v.expose().to_string()))
                            .collect(),
                    )
                    .protocol(protocol)
                    .build(),
            )
            .context("building OTLP metrics exporter")?;
            info!(url = %url, http = use_http, "worker metrics: OTLP exporter configured");
            Ok(builder
                .metrics(Arc::new(meter) as Arc<dyn CoreMeter>)
                .build())
        }
    }
}

/// Parse a Temporal address, auto-prefixing `http://` for scheme-less inputs.
///
/// The Temporal CLI accepts the bare `host:port` form (`temporal --address
/// localhost:7233 ...`), so users naturally try the same in `TEMPORAL_ADDRESS`.
/// `Url::parse` rejects it with the cryptic "target URL has no host" error.
/// Adding the scheme transparently matches the CLI's behaviour.
pub fn normalize_address(address: &str) -> Result<Url> {
    let normalized = if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    };
    Url::parse(&normalized).with_context(|| format!("invalid Temporal address: {address:?}"))
}

/// The identity this worker reports to Temporal: `<pid>@<hostname>`, the
/// format Temporal's own SDKs use.
///
/// The UI shows it on every activity attempt and poller, so it has to tell
/// workers apart. A PID alone cannot: it is 1 in every container. The hostname
/// is the pod name on Kubernetes and the container id under Docker; both set
/// `HOSTNAME`. A systemd service gets no `HOSTNAME`, hence the kernel's.
pub fn worker_identity() -> String {
    let hostname = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/proc/sys/kernel/hostname").ok());
    format_identity(std::process::id(), hostname.as_deref())
}

fn format_identity(pid: u32, hostname: Option<&str>) -> String {
    let host = hostname
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .unwrap_or("dbt-temporal");
    format!("{pid}@{host}")
}

/// Build TLS options when connecting to Temporal Cloud or a TLS-enabled server.
///
/// Returns `Some(TlsOptions)` when any of `TEMPORAL_API_KEY`, `TEMPORAL_TLS_CERT`,
/// or `TEMPORAL_TLS_KEY` are set. For API-key-only auth, TLS is enabled without
/// client certificates (Temporal Cloud terminates TLS at the edge). For mTLS,
/// both cert and key must be provided.
pub fn build_tls_options(
    config: &DbtTemporalConfig,
) -> Result<Option<temporalio_client::TlsOptions>> {
    let has_tls_cert = config.temporal_tls_cert.is_some();
    let has_tls_key = config.temporal_tls_key.is_some();

    if config.temporal_api_key.is_none() && !has_tls_cert && !has_tls_key {
        return Ok(None);
    }

    let client_tls = match (&config.temporal_tls_cert, &config.temporal_tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert = std::fs::read(cert_path)
                .with_context(|| format!("reading TEMPORAL_TLS_CERT from {cert_path}"))?;
            let key = std::fs::read(key_path)
                .with_context(|| format!("reading TEMPORAL_TLS_KEY from {key_path}"))?;
            Some(
                temporalio_client::ClientTlsOptions::builder()
                    .client_cert(cert)
                    .client_private_key(key)
                    .build(),
            )
        }
        (None, None) => None,
        _ => anyhow::bail!("TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY must both be set for mTLS"),
    };

    Ok(Some(
        temporalio_client::TlsOptions::builder()
            .maybe_client_tls_options(client_tls)
            .build(),
    ))
}

/// Build a `WorkerOptions` with tuning settings from the config (fully built).
///
/// The caller registers the activities and workflow on the returned options
/// before constructing the `Worker`; the set of polled task types is derived
/// from those registrations.
pub fn build_worker_options(config: &DbtTemporalConfig) -> WorkerOptions {
    let build_id = format!("dbt-temporal-{}", env!("CARGO_PKG_VERSION"));
    // When a deployment name is configured, enable versioned task routing so Temporal
    // can steer tasks to the correct worker version during rolling deploys.
    //
    // `default_versioning_behavior` is mandatory here, not optional: the Rust SDK
    // has no per-workflow versioning attribute and always reports
    // `VersioningBehavior::Unspecified` on task completion, which a versioned
    // worker's server rejects. Core substitutes this default only when it is set.
    //
    // `Pinned` is the correct choice for `dbt_run`: a pinned execution replays
    // and finishes on the worker version that started it, so it never meets
    // workflow code that issues different commands than its history recorded.
    // The workflow's `patched()` markers (`workflow/helpers/patches.rs`) are a
    // second line of defence for runs that do cross versions (a worker started
    // without a deployment name, or an execution moved by an operator); they
    // do not replace pinning, because they cover only the changes someone
    // thought to gate. A dbt run is finite, so pinned executions drain on
    // their own.
    let deployment = if let Some(ref name) = config.deployment_name {
        WorkerDeploymentOptions::new(
            WorkerDeploymentVersion::builder()
                .deployment_name(name.clone())
                .build_id(build_id)
                .build(),
        )
        .use_worker_versioning(true)
        .default_versioning_behavior(VersioningBehavior::Pinned)
        .build()
    } else {
        WorkerDeploymentOptions::from_build_id(build_id)
    };

    let sticky_timeout = Duration::from_secs(config.sticky_queue_timeout_secs);

    let tuner = build_tuner(config);

    let mut opts = WorkerOptions::new(&config.temporal_task_queue)
        .max_cached_workflows(config.max_cached_workflows)
        .deployment_options(deployment)
        .sticky_queue_schedule_to_start_timeout(sticky_timeout)
        .nonsticky_to_sticky_poll_ratio(config.nonsticky_to_sticky_poll_ratio)
        .tuner(tuner)
        .build();

    // Set optional fields on the built struct (avoids bon builder type-state issues).
    if let Some(rate) = config.max_worker_activities_per_second {
        opts.max_worker_activities_per_second = Some(rate);
    }
    if let Some(rate) = config.max_task_queue_activities_per_second {
        opts.max_task_queue_activities_per_second = Some(rate);
    }
    if let Some(secs) = config.graceful_shutdown_secs {
        opts.graceful_shutdown_period = Some(Duration::from_secs(secs));
    }
    if let Some(ref pa) = config.poller_autoscaling {
        // dbt levels are bursty: a wide level schedules many activities at once,
        // then the queue goes quiet. Autoscaling opens more poll calls under
        // backlog so task pickup isn't throttled by poll round-trips.
        let behavior = PollerBehavior::Autoscaling(
            AutoscalingOptions::builder()
                .minimum(pa.minimum)
                .maximum(pa.maximum)
                .initial(pa.initial)
                .build(),
        );
        opts.workflow_task_poller_behavior = Some(behavior);
        opts.activity_task_poller_behavior = Some(behavior);
        info!(
            minimum = pa.minimum,
            maximum = pa.maximum,
            initial = pa.initial,
            "poller autoscaling enabled for workflow and activity task pollers"
        );
    }

    opts
}

/// Build a `WorkerTuner` based on the tuning configuration.
///
/// Nexus slots are sized alongside workflow-task slots: this worker serves no
/// Nexus operations, so the number is never reached, but the supplier is
/// required and a value tied to the configured concurrency ages better than a
/// literal.
fn build_tuner(config: &DbtTemporalConfig) -> WorkerTuner {
    match &config.worker_tuning {
        WorkerTuningConfig::Fixed {
            max_concurrent_workflow_tasks,
            max_concurrent_activities,
            max_concurrent_local_activities,
        } => {
            info!(
                max_workflow_tasks = max_concurrent_workflow_tasks,
                max_activities = max_concurrent_activities,
                max_local_activities = max_concurrent_local_activities,
                sticky_queue_timeout_secs = config.sticky_queue_timeout_secs,
                nonsticky_to_sticky_poll_ratio = config.nonsticky_to_sticky_poll_ratio,
                max_cached_workflows = config.max_cached_workflows,
                "worker tuning: fixed slot limits"
            );
            TunerHolder::builder()
                .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(
                    *max_concurrent_workflow_tasks,
                ))
                .activity_task_slot_supplier(FixedSizeSlotSupplier::new(*max_concurrent_activities))
                .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(
                    *max_concurrent_local_activities,
                ))
                .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(
                    *max_concurrent_workflow_tasks,
                ))
                .build()
                .into()
        }
        WorkerTuningConfig::ResourceBased {
            target_mem_usage,
            target_cpu_usage,
            activity_min_slots,
            activity_max_slots,
        } => {
            use temporalio_sdk::runtime::worker_tuner::{
                ResourceBasedSlotOptions, ResourceBasedTuner, ResourceBasedTunerOptions,
            };

            let tuner = ResourceBasedTuner::builder()
                .tuner_options(
                    ResourceBasedTunerOptions::builder()
                        .target_memory_usage(*target_mem_usage)
                        .target_cpu_usage(*target_cpu_usage)
                        .build(),
                )
                .activity_task_slot_options(
                    ResourceBasedSlotOptions::builder()
                        .minimum_slots(*activity_min_slots)
                        .maximum_slots(*activity_max_slots)
                        .ramp_throttle(Duration::from_millis(50))
                        .build(),
                )
                .build();
            info!(
                target_mem = target_mem_usage,
                target_cpu = target_cpu_usage,
                activity_min = activity_min_slots,
                activity_max = activity_max_slots,
                sticky_queue_timeout_secs = config.sticky_queue_timeout_secs,
                nonsticky_to_sticky_poll_ratio = config.nonsticky_to_sticky_poll_ratio,
                max_cached_workflows = config.max_cached_workflows,
                "worker tuning: resource-based slot management"
            );
            tuner.into()
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
