use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use temporalio_common::worker::{WorkerDeploymentOptions, WorkerTaskTypes};
use temporalio_sdk::WorkerOptions;
use temporalio_sdk_core::{FixedSizeSlotSupplier, TunerBuilder, WorkerTuner};
use tracing::info;

use crate::config::{DbtTemporalConfig, WorkerTuningConfig};

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
            Some(temporalio_client::ClientTlsOptions {
                client_cert: cert,
                client_private_key: key,
            })
        }
        (None, None) => None,
        _ => anyhow::bail!("TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY must both be set for mTLS"),
    };

    Ok(Some(temporalio_client::TlsOptions {
        server_root_ca_cert: None,
        domain: None,
        client_tls_options: client_tls,
    }))
}

/// Build a `WorkerOptions` with tuning settings from the config (fully built).
///
/// Activity/workflow registrations are added by the caller on the `Worker` after creation.
pub fn build_worker_options(config: &DbtTemporalConfig) -> WorkerOptions {
    let deployment = WorkerDeploymentOptions::from_build_id(format!(
        "dbt-temporal-{}",
        env!("CARGO_PKG_VERSION")
    ));

    let sticky_timeout = Duration::from_secs(config.sticky_queue_timeout_secs);

    let tuner = build_tuner(config);

    let mut opts = WorkerOptions::new(&config.temporal_task_queue)
        .task_types(WorkerTaskTypes::all())
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

    opts
}

/// Build a `WorkerTuner` based on the tuning configuration.
fn build_tuner(config: &DbtTemporalConfig) -> Arc<dyn WorkerTuner + Send + Sync> {
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
            let mut builder = TunerBuilder::default();
            builder.workflow_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(
                *max_concurrent_workflow_tasks,
            )));
            builder.activity_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(
                *max_concurrent_activities,
            )));
            builder.local_activity_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(
                *max_concurrent_local_activities,
            )));
            Arc::new(builder.build())
        }
        WorkerTuningConfig::ResourceBased {
            target_mem_usage,
            target_cpu_usage,
            activity_min_slots,
            activity_max_slots,
        } => {
            use temporalio_sdk_core::{ResourceBasedTuner, ResourceSlotOptions};

            let mut tuner = ResourceBasedTuner::new(*target_mem_usage, *target_cpu_usage);
            tuner.with_activity_slots_options(ResourceSlotOptions::new(
                *activity_min_slots,
                *activity_max_slots,
                Duration::from_millis(50),
            ));
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
            Arc::new(tuner)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            artifact_store: "/tmp/dbt-artifacts".into(),
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
        }
    }

    #[test]
    fn build_worker_options_fixed() {
        let config = test_config();
        // Just verify it doesn't panic — WorkerOptions doesn't expose fields for inspection.
        let _opts = build_worker_options(&config);
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
}
