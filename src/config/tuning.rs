use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow};

use super::{PollerAutoscalingConfig, TemporalMetricsConfig, WorkerTuningConfig};

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
fn parse_header_pairs(raw: &str) -> Result<BTreeMap<String, String>> {
    let mut headers = BTreeMap::new();
    for pair in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid header pair '{pair}' (expected key=value)"))?;
        headers.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(headers)
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
mod tests {
    use super::*;

    use crate::config::test_util::with_env;

    // --- parse_env_usize / parse_env_f64 ---

    #[test]
    fn parse_env_usize_valid() -> Result<()> {
        unsafe { std::env::set_var("TEST_CFG_USIZE_VALID", "42") };
        let val = parse_env_usize("TEST_CFG_USIZE_VALID", 0)?;
        assert_eq!(val, 42);
        unsafe { std::env::remove_var("TEST_CFG_USIZE_VALID") };
        Ok(())
    }

    #[test]
    fn parse_env_usize_invalid() {
        unsafe { std::env::set_var("TEST_CFG_USIZE_INVALID", "not_a_number") };
        assert!(parse_env_usize("TEST_CFG_USIZE_INVALID", 0).is_err());
        unsafe { std::env::remove_var("TEST_CFG_USIZE_INVALID") };
    }

    #[test]
    fn parse_env_usize_missing_uses_default() -> Result<()> {
        unsafe { std::env::remove_var("TEST_CFG_USIZE_MISSING") };
        let val = parse_env_usize("TEST_CFG_USIZE_MISSING", 99)?;
        assert_eq!(val, 99);
        Ok(())
    }

    #[test]
    fn parse_env_f64_valid() -> Result<()> {
        unsafe { std::env::set_var("TEST_CFG_F64_VALID", "0.75") };
        let val = parse_env_f64("TEST_CFG_F64_VALID", 0.0)?;
        assert!((val - 0.75).abs() < f64::EPSILON);
        unsafe { std::env::remove_var("TEST_CFG_F64_VALID") };
        Ok(())
    }

    #[test]
    fn parse_env_f64_invalid() {
        unsafe { std::env::set_var("TEST_CFG_F64_INVALID", "abc") };
        assert!(parse_env_f64("TEST_CFG_F64_INVALID", 0.0).is_err());
        unsafe { std::env::remove_var("TEST_CFG_F64_INVALID") };
    }

    #[test]
    fn parse_env_f64_missing_uses_default() -> Result<()> {
        unsafe { std::env::remove_var("TEST_CFG_F64_MISSING") };
        let val = parse_env_f64("TEST_CFG_F64_MISSING", 0.8)?;
        assert!((val - 0.8).abs() < f64::EPSILON);
        Ok(())
    }

    // --- parse_env_u64 / parse_env_f32 ---

    #[test]
    fn parse_env_u64_valid_invalid_and_default() -> Result<()> {
        with_env(&[("TEST_CFG_U64_VALID", Some("123"))], || {
            assert_eq!(parse_env_u64("TEST_CFG_U64_VALID", 0)?, 123);
            Ok(())
        })?;
        with_env(&[("TEST_CFG_U64_INVALID", Some("nope"))], || {
            assert!(parse_env_u64("TEST_CFG_U64_INVALID", 0).is_err());
            Ok(())
        })?;
        with_env(&[("TEST_CFG_U64_MISSING", None)], || {
            assert_eq!(parse_env_u64("TEST_CFG_U64_MISSING", 7)?, 7);
            Ok(())
        })
    }

    #[test]
    fn parse_env_f32_valid_invalid_and_default() -> Result<()> {
        with_env(&[("TEST_CFG_F32_VALID", Some("0.5"))], || {
            let val = parse_env_f32("TEST_CFG_F32_VALID", 0.0)?;
            assert!((val - 0.5).abs() < f32::EPSILON);
            Ok(())
        })?;
        with_env(&[("TEST_CFG_F32_INVALID", Some("xx"))], || {
            assert!(parse_env_f32("TEST_CFG_F32_INVALID", 0.0).is_err());
            Ok(())
        })?;
        with_env(&[("TEST_CFG_F32_MISSING", None)], || {
            let val = parse_env_f32("TEST_CFG_F32_MISSING", 0.3)?;
            assert!((val - 0.3).abs() < f32::EPSILON);
            Ok(())
        })
    }

    // --- parse_optional_env_f64 / parse_optional_env_u64 ---

    #[test]
    fn parse_optional_env_f64_returns_none_for_missing_or_empty() -> Result<()> {
        with_env(&[("TEST_CFG_OPT_F64", None)], || {
            assert!(parse_optional_env_f64("TEST_CFG_OPT_F64")?.is_none());
            Ok(())
        })?;
        with_env(&[("TEST_CFG_OPT_F64", Some(""))], || {
            assert!(parse_optional_env_f64("TEST_CFG_OPT_F64")?.is_none());
            Ok(())
        })
    }

    #[test]
    fn parse_optional_env_f64_parses_value_and_errors_on_garbage() -> Result<()> {
        with_env(&[("TEST_CFG_OPT_F64", Some("12.5"))], || {
            assert_eq!(parse_optional_env_f64("TEST_CFG_OPT_F64")?, Some(12.5));
            Ok(())
        })?;
        with_env(&[("TEST_CFG_OPT_F64", Some("nope"))], || {
            assert!(parse_optional_env_f64("TEST_CFG_OPT_F64").is_err());
            Ok(())
        })
    }

    #[test]
    fn parse_optional_env_u64_returns_none_for_missing_or_empty() -> Result<()> {
        with_env(&[("TEST_CFG_OPT_U64", None)], || {
            assert!(parse_optional_env_u64("TEST_CFG_OPT_U64")?.is_none());
            Ok(())
        })?;
        with_env(&[("TEST_CFG_OPT_U64", Some(""))], || {
            assert!(parse_optional_env_u64("TEST_CFG_OPT_U64")?.is_none());
            Ok(())
        })
    }

    #[test]
    fn parse_optional_env_u64_parses_value_and_errors_on_garbage() -> Result<()> {
        with_env(&[("TEST_CFG_OPT_U64", Some("9001"))], || {
            assert_eq!(parse_optional_env_u64("TEST_CFG_OPT_U64")?, Some(9001));
            Ok(())
        })?;
        with_env(&[("TEST_CFG_OPT_U64", Some("nope"))], || {
            assert!(parse_optional_env_u64("TEST_CFG_OPT_U64").is_err());
            Ok(())
        })
    }

    // --- parse_search_attributes ---

    #[test]
    fn parse_search_attributes_valid_json() -> Result<()> {
        with_env(
            &[("TEMPORAL_SEARCH_ATTRIBUTES", Some(r#"{"env":"prod","team":"data"}"#))],
            || {
                let attrs = parse_search_attributes()?;
                assert_eq!(
                    attrs
                        .get("env")
                        .ok_or_else(|| anyhow::anyhow!("missing 'env' key"))?,
                    "prod"
                );
                assert_eq!(
                    attrs
                        .get("team")
                        .ok_or_else(|| anyhow::anyhow!("missing 'team' key"))?,
                    "data"
                );
                Ok(())
            },
        )
    }

    #[test]
    fn parse_search_attributes_invalid_json() -> Result<()> {
        with_env(&[("TEMPORAL_SEARCH_ATTRIBUTES", Some("not json"))], || {
            assert!(parse_search_attributes().is_err());
            Ok(())
        })
    }

    #[test]
    fn parse_search_attributes_missing_returns_empty() -> Result<()> {
        with_env(&[("TEMPORAL_SEARCH_ATTRIBUTES", None)], || {
            let attrs = parse_search_attributes()?;
            assert!(attrs.is_empty());
            Ok(())
        })
    }

    #[test]
    fn parse_search_attributes_empty_string_returns_empty() -> Result<()> {
        with_env(&[("TEMPORAL_SEARCH_ATTRIBUTES", Some(""))], || {
            let attrs = parse_search_attributes()?;
            assert!(attrs.is_empty());
            Ok(())
        })
    }

    // --- parse_worker_tuning ---

    #[test]
    fn parse_worker_tuning_defaults_to_fixed() -> Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", None),
                ("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", None),
                ("WORKER_MAX_CONCURRENT_ACTIVITIES", None),
                ("WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES", None),
            ],
            || {
                let config = parse_worker_tuning()?;
                match config {
                    WorkerTuningConfig::Fixed {
                        max_concurrent_workflow_tasks,
                        max_concurrent_activities,
                        max_concurrent_local_activities,
                    } => {
                        assert_eq!(max_concurrent_workflow_tasks, 200);
                        assert_eq!(max_concurrent_activities, 100);
                        assert_eq!(max_concurrent_local_activities, 100);
                    }
                    WorkerTuningConfig::ResourceBased { .. } => panic!("expected Fixed"),
                }
                Ok(())
            },
        )
    }

    #[test]
    fn parse_worker_tuning_fixed_explicit() -> Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", Some("fixed")),
                ("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", Some("50")),
                ("WORKER_MAX_CONCURRENT_ACTIVITIES", Some("25")),
                ("WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES", Some("10")),
            ],
            || {
                let config = parse_worker_tuning()?;
                match config {
                    WorkerTuningConfig::Fixed {
                        max_concurrent_workflow_tasks,
                        max_concurrent_activities,
                        max_concurrent_local_activities,
                    } => {
                        assert_eq!(max_concurrent_workflow_tasks, 50);
                        assert_eq!(max_concurrent_activities, 25);
                        assert_eq!(max_concurrent_local_activities, 10);
                    }
                    WorkerTuningConfig::ResourceBased { .. } => panic!("expected Fixed"),
                }
                Ok(())
            },
        )
    }

    #[test]
    fn parse_worker_tuning_resource_based() -> Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", Some("resource-based")),
                ("WORKER_RESOURCE_TARGET_MEM", Some("0.7")),
                ("WORKER_RESOURCE_TARGET_CPU", Some("0.85")),
                ("WORKER_RESOURCE_ACTIVITY_MIN_SLOTS", Some("2")),
                ("WORKER_RESOURCE_ACTIVITY_MAX_SLOTS", Some("300")),
            ],
            || {
                let config = parse_worker_tuning()?;
                match config {
                    WorkerTuningConfig::ResourceBased {
                        target_mem_usage,
                        target_cpu_usage,
                        activity_min_slots,
                        activity_max_slots,
                    } => {
                        assert!((target_mem_usage - 0.7).abs() < f64::EPSILON);
                        assert!((target_cpu_usage - 0.85).abs() < f64::EPSILON);
                        assert_eq!(activity_min_slots, 2);
                        assert_eq!(activity_max_slots, 300);
                    }
                    WorkerTuningConfig::Fixed { .. } => panic!("expected ResourceBased"),
                }
                Ok(())
            },
        )
    }

    #[test]
    fn parse_worker_tuning_resource_based_underscore_variant() -> Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", Some("resource_based")),
                ("WORKER_RESOURCE_TARGET_MEM", None),
                ("WORKER_RESOURCE_TARGET_CPU", None),
                ("WORKER_RESOURCE_ACTIVITY_MIN_SLOTS", None),
                ("WORKER_RESOURCE_ACTIVITY_MAX_SLOTS", None),
            ],
            || {
                let config = parse_worker_tuning()?;
                assert!(matches!(config, WorkerTuningConfig::ResourceBased { .. }));
                Ok(())
            },
        )
    }

    #[test]
    fn parse_worker_tuning_invalid_mode() -> Result<()> {
        with_env(&[("WORKER_TUNER", Some("bogus"))], || {
            let Err(err) = parse_worker_tuning() else {
                anyhow::bail!("expected error for bogus tuner mode");
            };
            assert!(err.to_string().contains("bogus"));
            Ok(())
        })
    }

    const POLLER_VARS: &[&str] = &[
        "WORKER_POLLER_AUTOSCALING",
        "WORKER_POLLER_MIN",
        "WORKER_POLLER_MAX",
        "WORKER_POLLER_INITIAL",
    ];

    fn poller_env<'a>(overrides: &[(&'a str, Option<&'a str>)]) -> Vec<(&'a str, Option<&'a str>)> {
        let mut vars: Vec<(&str, Option<&str>)> = POLLER_VARS.iter().map(|k| (*k, None)).collect();
        vars.extend_from_slice(overrides);
        vars
    }

    #[test]
    fn parse_poller_autoscaling_disabled_by_default() -> Result<()> {
        with_env(&poller_env(&[]), || {
            assert!(parse_poller_autoscaling()?.is_none());
            Ok(())
        })
    }

    #[test]
    fn parse_poller_autoscaling_enabled_with_defaults() -> Result<()> {
        with_env(&poller_env(&[("WORKER_POLLER_AUTOSCALING", Some("1"))]), || {
            let pa = parse_poller_autoscaling()?.ok_or_else(|| anyhow!("expected Some"))?;
            assert_eq!(pa.minimum, 1);
            assert_eq!(pa.maximum, 100);
            assert_eq!(pa.initial, 5);
            Ok(())
        })
    }

    #[test]
    fn parse_poller_autoscaling_rejects_initial_out_of_bounds() -> Result<()> {
        with_env(
            &poller_env(&[
                ("WORKER_POLLER_AUTOSCALING", Some("true")),
                ("WORKER_POLLER_MIN", Some("2")),
                ("WORKER_POLLER_MAX", Some("4")),
                ("WORKER_POLLER_INITIAL", Some("9")),
            ]),
            || {
                let Err(err) = parse_poller_autoscaling() else {
                    anyhow::bail!("expected error for out-of-bounds initial");
                };
                assert!(err.to_string().contains("WORKER_POLLER_INITIAL"), "got: {err}");
                Ok(())
            },
        )
    }

    const METRICS_VARS: &[&str] = &[
        "TEMPORAL_METRICS_EXPORTER",
        "TEMPORAL_METRICS_PROMETHEUS_ADDR",
        "TEMPORAL_METRICS_OTLP_URL",
        "TEMPORAL_METRICS_OTLP_PROTOCOL",
        "TEMPORAL_METRICS_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ];

    fn metrics_env<'a>(
        overrides: &[(&'a str, Option<&'a str>)],
    ) -> Vec<(&'a str, Option<&'a str>)> {
        let mut vars: Vec<(&str, Option<&str>)> = METRICS_VARS.iter().map(|k| (*k, None)).collect();
        vars.extend_from_slice(overrides);
        vars
    }

    #[test]
    fn parse_temporal_metrics_defaults_to_none() -> Result<()> {
        with_env(&metrics_env(&[]), || {
            assert_eq!(parse_temporal_metrics()?, TemporalMetricsConfig::None);
            Ok(())
        })
    }

    #[test]
    fn parse_temporal_metrics_rejects_unknown_exporter() -> Result<()> {
        with_env(&metrics_env(&[("TEMPORAL_METRICS_EXPORTER", Some("statsd"))]), || {
            let Err(err) = parse_temporal_metrics() else {
                anyhow::bail!("expected error for unknown exporter");
            };
            assert!(err.to_string().contains("statsd"), "got: {err}");
            Ok(())
        })
    }

    #[test]
    fn parse_temporal_metrics_rejects_bad_prometheus_addr() -> Result<()> {
        with_env(
            &metrics_env(&[
                ("TEMPORAL_METRICS_EXPORTER", Some("prometheus")),
                ("TEMPORAL_METRICS_PROMETHEUS_ADDR", Some("not-an-addr")),
            ]),
            || {
                assert!(parse_temporal_metrics().is_err());
                Ok(())
            },
        )
    }

    #[test]
    fn parse_temporal_metrics_otlp_http_protocol() -> Result<()> {
        with_env(
            &metrics_env(&[
                ("TEMPORAL_METRICS_EXPORTER", Some("otel")),
                ("TEMPORAL_METRICS_OTLP_URL", Some("http://collector:4318/v1/metrics")),
                ("TEMPORAL_METRICS_OTLP_PROTOCOL", Some("http")),
            ]),
            || {
                let TemporalMetricsConfig::Otlp {
                    url,
                    use_http,
                    headers,
                } = parse_temporal_metrics()?
                else {
                    anyhow::bail!("expected Otlp config");
                };
                assert_eq!(url, "http://collector:4318/v1/metrics");
                assert!(use_http);
                assert!(headers.is_empty());
                Ok(())
            },
        )
    }

    #[test]
    fn parse_temporal_metrics_otlp_grpc_protocol_with_headers() -> Result<()> {
        with_env(
            &metrics_env(&[
                ("TEMPORAL_METRICS_EXPORTER", Some("otlp")),
                ("TEMPORAL_METRICS_OTLP_URL", Some("http://collector:4317")),
                ("TEMPORAL_METRICS_OTLP_PROTOCOL", Some("grpc")),
                ("TEMPORAL_METRICS_OTLP_HEADERS", Some("x-auth=secret,x-team=data")),
            ]),
            || {
                let TemporalMetricsConfig::Otlp {
                    url,
                    use_http,
                    headers,
                } = parse_temporal_metrics()?
                else {
                    anyhow::bail!("expected Otlp config");
                };
                assert_eq!(url, "http://collector:4317");
                assert!(!use_http);
                assert_eq!(headers.len(), 2);
                assert_eq!(headers.get("x-auth").map(String::as_str), Some("secret"));
                assert_eq!(headers.get("x-team").map(String::as_str), Some("data"));
                Ok(())
            },
        )
    }

    #[test]
    fn parse_temporal_metrics_rejects_unknown_otlp_protocol() -> Result<()> {
        with_env(
            &metrics_env(&[
                ("TEMPORAL_METRICS_EXPORTER", Some("otlp")),
                ("TEMPORAL_METRICS_OTLP_URL", Some("http://collector:4317")),
                ("TEMPORAL_METRICS_OTLP_PROTOCOL", Some("carrier-pigeon")),
            ]),
            || {
                let Err(err) = parse_temporal_metrics() else {
                    anyhow::bail!("expected error for unknown protocol");
                };
                assert!(err.to_string().contains("TEMPORAL_METRICS_OTLP_PROTOCOL"), "got: {err}");
                Ok(())
            },
        )
    }

    #[test]
    fn parse_header_pairs_rejects_missing_equals() {
        assert!(parse_header_pairs("just-a-key").is_err());
    }

    #[test]
    fn parse_poller_autoscaling_rejects_zero_minimum() -> Result<()> {
        with_env(
            &poller_env(&[
                ("WORKER_POLLER_AUTOSCALING", Some("1")),
                ("WORKER_POLLER_MIN", Some("0")),
            ]),
            || {
                let Err(err) = parse_poller_autoscaling() else {
                    anyhow::bail!("expected error for zero minimum");
                };
                assert!(err.to_string().contains("WORKER_POLLER_MIN"), "got: {err}");
                Ok(())
            },
        )
    }
}
