use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow};

use super::{
    ExternalPayloadStorageConfig, PollerAutoscalingConfig, Secret, TemporalMetricsConfig,
    WorkerTuningConfig,
};

/// Parse `TEMPORAL_SEARCH_ATTRIBUTES` env var as a JSON object of string key-value pairs.
/// Example: `TEMPORAL_SEARCH_ATTRIBUTES={"env":"prod","team":"data-eng"}`
pub fn parse_search_attributes() -> Result<BTreeMap<String, String>> {
    let val = match std::env::var("TEMPORAL_SEARCH_ATTRIBUTES") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(BTreeMap::new()),
    };
    serde_json::from_str::<BTreeMap<String, String>>(&val)
        .context("invalid TEMPORAL_SEARCH_ATTRIBUTES JSON")
}

/// Parse worker tuning configuration from environment variables.
///
/// Mode selection via `WORKER_TUNER`:
///   - `"fixed"` (default): fixed-size concurrency limits
///   - `"resource-based"`: dynamic PID-controller-based slot management
///
/// Fixed mode env vars:
///   - `WORKER_MAX_CONCURRENT_WORKFLOW_TASKS` (default: 200)
///   - `WORKER_MAX_CONCURRENT_ACTIVITIES` (default: 100)
///   - `WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES` (default: 100)
///
/// Resource-based mode env vars:
///   - `WORKER_RESOURCE_TARGET_MEM` (default: 0.8) — target memory utilization [0.0–1.0]
///   - `WORKER_RESOURCE_TARGET_CPU` (default: 0.9) — target CPU utilization [0.0–1.0]
///   - `WORKER_RESOURCE_ACTIVITY_MIN_SLOTS` (default: 1)
///   - `WORKER_RESOURCE_ACTIVITY_MAX_SLOTS` (default: 500)
pub fn parse_worker_tuning() -> Result<WorkerTuningConfig> {
    let mode = std::env::var("WORKER_TUNER").unwrap_or_default();
    match mode.as_str() {
        "resource-based" | "resource_based" => Ok(WorkerTuningConfig::ResourceBased {
            target_mem_usage: parse_env_f64("WORKER_RESOURCE_TARGET_MEM", 0.8)?,
            target_cpu_usage: parse_env_f64("WORKER_RESOURCE_TARGET_CPU", 0.9)?,
            activity_min_slots: parse_env_usize("WORKER_RESOURCE_ACTIVITY_MIN_SLOTS", 1)?,
            activity_max_slots: parse_env_usize("WORKER_RESOURCE_ACTIVITY_MAX_SLOTS", 500)?,
        }),
        "" | "fixed" => Ok(WorkerTuningConfig::Fixed {
            max_concurrent_workflow_tasks: parse_env_usize(
                "WORKER_MAX_CONCURRENT_WORKFLOW_TASKS",
                200,
            )?,
            max_concurrent_activities: parse_env_usize("WORKER_MAX_CONCURRENT_ACTIVITIES", 100)?,
            max_concurrent_local_activities: parse_env_usize(
                "WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES",
                100,
            )?,
        }),
        other => Err(anyhow!(
            "unknown WORKER_TUNER mode '{other}' (expected 'fixed' or 'resource-based')"
        )),
    }
}

/// Parse poller autoscaling configuration from environment variables.
///
/// Enabled via `WORKER_POLLER_AUTOSCALING` (truthy: `1`/`true`). Bounds:
///   - `WORKER_POLLER_MIN` (default: 1)
///   - `WORKER_POLLER_MAX` (default: 100)
///   - `WORKER_POLLER_INITIAL` (default: 5)
///
/// Bounds are validated here rather than at worker start so a bad value fails
/// fast at boot with the env var name in the error.
pub fn parse_poller_autoscaling() -> Result<Option<PollerAutoscalingConfig>> {
    let enabled = std::env::var("WORKER_POLLER_AUTOSCALING")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if !enabled {
        return Ok(None);
    }
    let minimum = parse_env_usize("WORKER_POLLER_MIN", 1)?;
    let maximum = parse_env_usize("WORKER_POLLER_MAX", 100)?;
    let initial = parse_env_usize("WORKER_POLLER_INITIAL", 5)?;
    anyhow::ensure!(minimum >= 1, "WORKER_POLLER_MIN must be >= 1");
    anyhow::ensure!(
        maximum >= minimum,
        "WORKER_POLLER_MAX ({maximum}) must be >= WORKER_POLLER_MIN ({minimum})"
    );
    anyhow::ensure!(
        (minimum..=maximum).contains(&initial),
        "WORKER_POLLER_INITIAL ({initial}) must be between WORKER_POLLER_MIN and WORKER_POLLER_MAX"
    );
    Ok(Some(PollerAutoscalingConfig {
        minimum,
        maximum,
        initial,
    }))
}

/// Parse worker metrics export configuration from environment variables.
///
/// Mode selection via `TEMPORAL_METRICS_EXPORTER`:
///   - `"none"` (default): no metrics export
///   - `"prometheus"`: scrape endpoint at `TEMPORAL_METRICS_PROMETHEUS_ADDR`
///     (default `0.0.0.0:9464`)
///   - `"otlp"` (alias `"otel"`): push to `TEMPORAL_METRICS_OTLP_URL` (falls
///     back to `OTEL_EXPORTER_OTLP_ENDPOINT`), protocol
///     `TEMPORAL_METRICS_OTLP_PROTOCOL` (`grpc` default, or `http`), headers
///     `TEMPORAL_METRICS_OTLP_HEADERS` (`key=value,key2=value2`)
pub fn parse_temporal_metrics() -> Result<TemporalMetricsConfig> {
    let mode = std::env::var("TEMPORAL_METRICS_EXPORTER").unwrap_or_default();
    match mode.to_lowercase().as_str() {
        "" | "none" => Ok(TemporalMetricsConfig::None),
        "prometheus" => {
            let raw = std::env::var("TEMPORAL_METRICS_PROMETHEUS_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:9464".to_string());
            let socket_addr = raw
                .parse()
                .with_context(|| format!("invalid TEMPORAL_METRICS_PROMETHEUS_ADDR '{raw}'"))?;
            Ok(TemporalMetricsConfig::Prometheus { socket_addr })
        }
        "otlp" | "otel" => {
            let url = std::env::var("TEMPORAL_METRICS_OTLP_URL")
                .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
                .map_err(|_| {
                    anyhow!(
                        "TEMPORAL_METRICS_EXPORTER=otlp requires TEMPORAL_METRICS_OTLP_URL \
                         or OTEL_EXPORTER_OTLP_ENDPOINT"
                    )
                })?;
            let use_http = match std::env::var("TEMPORAL_METRICS_OTLP_PROTOCOL") {
                Ok(p) if p.eq_ignore_ascii_case("http") => true,
                Ok(p) if p.eq_ignore_ascii_case("grpc") => false,
                Ok(p) => anyhow::bail!(
                    "unknown TEMPORAL_METRICS_OTLP_PROTOCOL '{p}' (expected 'grpc' or 'http')"
                ),
                Err(_) => false,
            };
            let headers = parse_header_pairs(
                &std::env::var("TEMPORAL_METRICS_OTLP_HEADERS").unwrap_or_default(),
            )?;
            Ok(TemporalMetricsConfig::Otlp {
                url,
                use_http,
                headers,
            })
        }
        other => Err(anyhow!(
            "unknown TEMPORAL_METRICS_EXPORTER '{other}' (expected 'none', 'prometheus', or 'otlp')"
        )),
    }
}

/// Parse comma-separated `key=value` pairs (e.g. OTLP auth headers).
fn parse_header_pairs(raw: &str) -> Result<BTreeMap<String, Secret>> {
    let mut headers = BTreeMap::new();
    for pair in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid header pair '{pair}' (expected key=value)"))?;
        headers.insert(k.trim().to_string(), Secret::new(v.trim()));
    }
    Ok(headers)
}

/// Parse the external payload storage settings.
///
/// `EXTERNAL_PAYLOAD_STORAGE` turns it on, `EXTERNAL_PAYLOAD_THRESHOLD_BYTES`
/// optionally overrides the SDK's offload threshold. The threshold is ignored
/// while storage is off.
pub fn parse_external_payload_storage() -> Result<Option<ExternalPayloadStorageConfig>> {
    let enabled = std::env::var("EXTERNAL_PAYLOAD_STORAGE")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if !enabled {
        return Ok(None);
    }
    let threshold_bytes = parse_optional_env_u64("EXTERNAL_PAYLOAD_THRESHOLD_BYTES")?
        .map(|bytes| {
            usize::try_from(bytes)
                .map_err(|_| anyhow!("EXTERNAL_PAYLOAD_THRESHOLD_BYTES is too large: {bytes}"))
        })
        .transpose()?;
    Ok(Some(ExternalPayloadStorageConfig { threshold_bytes }))
}

pub fn parse_env_usize(name: &str, default: usize) -> Result<usize> {
    std::env::var(name).map_or(Ok(default), |v| {
        v.parse::<usize>()
            .with_context(|| format!("invalid {name}"))
    })
}

fn parse_env_f64(name: &str, default: f64) -> Result<f64> {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse::<f64>().with_context(|| format!("invalid {name}")))
}

pub fn parse_env_u64(name: &str, default: u64) -> Result<u64> {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse::<u64>().with_context(|| format!("invalid {name}")))
}

pub fn parse_env_f32(name: &str, default: f32) -> Result<f32> {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse::<f32>().with_context(|| format!("invalid {name}")))
}

pub fn parse_optional_env_f64(name: &str) -> Result<Option<f64>> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v
            .parse::<f64>()
            .map(Some)
            .with_context(|| format!("invalid {name}")),
        _ => Ok(None),
    }
}

pub fn parse_optional_env_u64(name: &str) -> Result<Option<u64>> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v
            .parse::<u64>()
            .map(Some)
            .with_context(|| format!("invalid {name}")),
        _ => Ok(None),
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests;
