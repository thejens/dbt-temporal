use std::collections::BTreeMap;

use super::WorkerTuningConfig;

/// Parse `TEMPORAL_SEARCH_ATTRIBUTES` env var as a JSON object of string key-value pairs.
/// Example: `TEMPORAL_SEARCH_ATTRIBUTES={"env":"prod","team":"data-eng"}`
pub fn parse_search_attributes() -> Result<BTreeMap<String, String>, String> {
    let val = match std::env::var("TEMPORAL_SEARCH_ATTRIBUTES") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(BTreeMap::new()),
    };
    serde_json::from_str::<BTreeMap<String, String>>(&val)
        .map_err(|e| format!("invalid TEMPORAL_SEARCH_ATTRIBUTES JSON: {e}"))
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
pub fn parse_worker_tuning() -> Result<WorkerTuningConfig, String> {
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
        other => Err(format!(
            "unknown WORKER_TUNER mode '{other}' (expected 'fixed' or 'resource-based')"
        )),
    }
}

pub fn parse_env_usize(name: &str, default: usize) -> Result<usize, String> {
    std::env::var(name).map_or(Ok(default), |v| {
        v.parse::<usize>()
            .map_err(|e| format!("invalid {name}: {e}"))
    })
}

fn parse_env_f64(name: &str, default: f64) -> Result<f64, String> {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse::<f64>().map_err(|e| format!("invalid {name}: {e}")))
}

pub fn parse_env_u64(name: &str, default: u64) -> Result<u64, String> {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse::<u64>().map_err(|e| format!("invalid {name}: {e}")))
}

pub fn parse_env_f32(name: &str, default: f32) -> Result<f32, String> {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse::<f32>().map_err(|e| format!("invalid {name}: {e}")))
}

pub fn parse_optional_env_f64(name: &str) -> Result<Option<f64>, String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v
            .parse::<f64>()
            .map(Some)
            .map_err(|e| format!("invalid {name}: {e}")),
        _ => Ok(None),
    }
}

pub fn parse_optional_env_u64(name: &str) -> Result<Option<u64>, String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v
            .parse::<u64>()
            .map(Some)
            .map_err(|e| format!("invalid {name}: {e}")),
        _ => Ok(None),
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    static CFG_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce() -> anyhow::Result<()>>(
        vars: &[(&str, Option<&str>)],
        f: F,
    ) -> anyhow::Result<()> {
        let _guard = CFG_ENV_LOCK.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        let saved: Vec<(&str, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        let result = f();
        for (k, orig) in &saved {
            match orig {
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        result
    }

    // --- parse_env_usize / parse_env_f64 ---

    #[test]
    fn parse_env_usize_valid() -> anyhow::Result<()> {
        unsafe { std::env::set_var("TEST_CFG_USIZE_VALID", "42") };
        let val = parse_env_usize("TEST_CFG_USIZE_VALID", 0).map_err(|e| anyhow::anyhow!("{e}"))?;
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
    fn parse_env_usize_missing_uses_default() -> anyhow::Result<()> {
        unsafe { std::env::remove_var("TEST_CFG_USIZE_MISSING") };
        let val =
            parse_env_usize("TEST_CFG_USIZE_MISSING", 99).map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(val, 99);
        Ok(())
    }

    #[test]
    fn parse_env_f64_valid() -> anyhow::Result<()> {
        unsafe { std::env::set_var("TEST_CFG_F64_VALID", "0.75") };
        let val = parse_env_f64("TEST_CFG_F64_VALID", 0.0).map_err(|e| anyhow::anyhow!("{e}"))?;
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
    fn parse_env_f64_missing_uses_default() -> anyhow::Result<()> {
        unsafe { std::env::remove_var("TEST_CFG_F64_MISSING") };
        let val = parse_env_f64("TEST_CFG_F64_MISSING", 0.8).map_err(|e| anyhow::anyhow!("{e}"))?;
        assert!((val - 0.8).abs() < f64::EPSILON);
        Ok(())
    }

    // --- parse_search_attributes ---

    #[test]
    fn parse_search_attributes_valid_json() -> anyhow::Result<()> {
        with_env(
            &[("TEMPORAL_SEARCH_ATTRIBUTES", Some(r#"{"env":"prod","team":"data"}"#))],
            || {
                let attrs = parse_search_attributes().map_err(|e| anyhow::anyhow!("{e}"))?;
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
    fn parse_search_attributes_invalid_json() -> anyhow::Result<()> {
        with_env(&[("TEMPORAL_SEARCH_ATTRIBUTES", Some("not json"))], || {
            assert!(parse_search_attributes().is_err());
            Ok(())
        })
    }

    #[test]
    fn parse_search_attributes_missing_returns_empty() -> anyhow::Result<()> {
        with_env(&[("TEMPORAL_SEARCH_ATTRIBUTES", None)], || {
            let attrs = parse_search_attributes().map_err(|e| anyhow::anyhow!("{e}"))?;
            assert!(attrs.is_empty());
            Ok(())
        })
    }

    #[test]
    fn parse_search_attributes_empty_string_returns_empty() -> anyhow::Result<()> {
        with_env(&[("TEMPORAL_SEARCH_ATTRIBUTES", Some(""))], || {
            let attrs = parse_search_attributes().map_err(|e| anyhow::anyhow!("{e}"))?;
            assert!(attrs.is_empty());
            Ok(())
        })
    }

    // --- parse_worker_tuning ---

    #[test]
    fn parse_worker_tuning_defaults_to_fixed() -> anyhow::Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", None),
                ("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", None),
                ("WORKER_MAX_CONCURRENT_ACTIVITIES", None),
                ("WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES", None),
            ],
            || {
                let config = parse_worker_tuning().map_err(|e| anyhow::anyhow!("{e}"))?;
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
    fn parse_worker_tuning_fixed_explicit() -> anyhow::Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", Some("fixed")),
                ("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", Some("50")),
                ("WORKER_MAX_CONCURRENT_ACTIVITIES", Some("25")),
                ("WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES", Some("10")),
            ],
            || {
                let config = parse_worker_tuning().map_err(|e| anyhow::anyhow!("{e}"))?;
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
    fn parse_worker_tuning_resource_based() -> anyhow::Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", Some("resource-based")),
                ("WORKER_RESOURCE_TARGET_MEM", Some("0.7")),
                ("WORKER_RESOURCE_TARGET_CPU", Some("0.85")),
                ("WORKER_RESOURCE_ACTIVITY_MIN_SLOTS", Some("2")),
                ("WORKER_RESOURCE_ACTIVITY_MAX_SLOTS", Some("300")),
            ],
            || {
                let config = parse_worker_tuning().map_err(|e| anyhow::anyhow!("{e}"))?;
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
    fn parse_worker_tuning_resource_based_underscore_variant() -> anyhow::Result<()> {
        with_env(
            &[
                ("WORKER_TUNER", Some("resource_based")),
                ("WORKER_RESOURCE_TARGET_MEM", None),
                ("WORKER_RESOURCE_TARGET_CPU", None),
                ("WORKER_RESOURCE_ACTIVITY_MIN_SLOTS", None),
                ("WORKER_RESOURCE_ACTIVITY_MAX_SLOTS", None),
            ],
            || {
                let config = parse_worker_tuning().map_err(|e| anyhow::anyhow!("{e}"))?;
                assert!(matches!(config, WorkerTuningConfig::ResourceBased { .. }));
                Ok(())
            },
        )
    }

    #[test]
    fn parse_worker_tuning_invalid_mode() -> anyhow::Result<()> {
        with_env(&[("WORKER_TUNER", Some("bogus"))], || {
            let Err(err) = parse_worker_tuning() else {
                anyhow::bail!("expected error for bogus tuner mode");
            };
            assert!(err.contains("bogus"));
            Ok(())
        })
    }
}
