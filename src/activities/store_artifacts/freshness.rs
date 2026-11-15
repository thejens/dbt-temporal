//! Building `sources.json` and `freshness.json` for freshness runs.

use crate::types::{NodeExecutionResult, StoreArtifactsInput};

use super::metadata::{DBT_VERSION, artifact_env};

/// Schemas of the two freshness artifacts. They differ because the shapes do:
/// `freshness.json` adds `resource_type` and covers models, so it cannot claim
/// the `sources` schema `sources.json` has always followed.
const SOURCES_SCHEMA: &str = "https://schemas.getdbt.com/dbt/sources/v3.json";
const FRESHNESS_SCHEMA: &str = "https://schemas.getdbt.com/dbt/freshness/v0.json";

/// Whether a node result belongs to a source.
///
/// Keyed on the unique_id prefix, which is how dbt itself separates sources
/// from models when it only has the id to go on — a resolved node is not in
/// reach here, only the results the workflow accumulated.
pub(super) fn is_source_result(result: &NodeExecutionResult) -> bool {
    result.unique_id.starts_with("source.")
}

/// Build the `sources.json` / `freshness.json` content from freshness-bearing
/// node results.
///
/// `sources_only` picks between the two: `sources.json` keeps the shape
/// `dbt source freshness` has always written (sources, no `resource_type`),
/// while `freshness.json` carries every measured node and tags each row with
/// its resource type.
pub(super) fn build_freshness_json(
    input: &StoreArtifactsInput,
    sources_only: bool,
) -> Result<String, anyhow::Error> {
    let results: Vec<serde_json::Value> = input
        .node_results
        .iter()
        .filter_map(|r| {
            let f = r.freshness.as_ref()?;
            if sources_only && !is_source_result(r) {
                return None;
            }
            let mut row = serde_json::json!({
                "unique_id": r.unique_id,
                "max_loaded_at": f.max_loaded_at,
                "snapshotted_at": f.snapshotted_at,
                "max_loaded_at_time_ago_in_s": f.max_loaded_at_time_ago_in_s,
                "status": f.status,
                "criteria": f.criteria,
                "adapter_response": r.adapter_response,
                "timing": r.timing,
                "execution_time": r.execution_time,
            });
            // `resource_type` is what separates the two artifacts —
            // `sources.json`'s shape must not change.
            if !sources_only && let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "resource_type".to_owned(),
                    serde_json::Value::String(f.resource_type.clone()),
                );
            }
            Some(row)
        })
        .collect();
    let total: std::time::Duration = input
        .node_results
        .iter()
        .map(|r| std::time::Duration::from_secs_f64(r.execution_time.max(0.0)))
        .sum();
    let metadata = dbt_schemas::schemas::FreshnessResultsMetadata {
        dbt_schema_version: if sources_only {
            SOURCES_SCHEMA
        } else {
            FRESHNESS_SCHEMA
        }
        .to_string(),
        dbt_version: DBT_VERSION.to_string(),
        generated_at: chrono::Utc::now(),
        invocation_id: input.invocation_id.clone(),
        invocation_started_at: input.started_at,
        env: artifact_env(),
    };
    let artifact = serde_json::json!({
        "metadata": metadata,
        "results": results,
        "elapsed_time": total.as_secs_f64(),
    });
    serde_json::to_string_pretty(&artifact).map_err(Into::into)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use anyhow::Context;

    use super::*;
    use crate::types::{FreshnessGrade, NodeStatus};

    use super::super::test_support::{freshness_input, sample_result, with_freshness};

    #[test]
    fn sources_json_includes_only_source_freshness_results() -> anyhow::Result<()> {
        let input = freshness_input(
            "freshness",
            vec![
                with_freshness("source.p.s.orders", "source", FreshnessGrade::Pass),
                with_freshness("model.p.stg_orders", "model", FreshnessGrade::Warn),
                sample_result("model.p.m", NodeStatus::Success, 1.0),
            ],
        );

        let parsed: serde_json::Value = serde_json::from_str(&build_freshness_json(&input, true)?)?;
        assert_eq!(parsed["metadata"]["dbt_schema_version"], SOURCES_SCHEMA);
        assert_eq!(parsed["metadata"]["dbt_version"], DBT_VERSION);
        let results = parsed["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1, "models and plain results must be excluded");
        assert_eq!(results[0]["unique_id"], "source.p.s.orders");
        assert_eq!(results[0]["status"], "pass");
        assert!(
            (results[0]["max_loaded_at_time_ago_in_s"]
                .as_f64()
                .expect("age")
                - 3600.0)
                .abs()
                < f64::EPSILON
        );
        assert!(results[0]["criteria"].is_object());
        assert!(
            results[0].get("resource_type").is_none(),
            "sources.json's shape must not gain resource_type"
        );
        Ok(())
    }

    /// A stale source is the one measurement anybody reads a freshness artifact
    /// for. The filter here keeps only results that carry an outcome, so a
    /// stale verdict must carry one rather than the activity failing without
    /// returning a result.
    #[test]
    fn sources_json_includes_a_stale_source() -> anyhow::Result<()> {
        let mut stale = with_freshness("source.p.s.orders", "source", FreshnessGrade::Error);
        stale.status = NodeStatus::Error;
        let input = freshness_input("source-freshness", vec![stale]);

        let parsed: serde_json::Value = serde_json::from_str(&build_freshness_json(&input, true)?)?;
        let results = parsed["results"].as_array().context("results array")?;

        assert_eq!(results.len(), 1, "{parsed}");
        assert_eq!(results[0]["unique_id"], "source.p.s.orders");
        assert_eq!(results[0]["status"], "error");
        assert_eq!(results[0]["max_loaded_at_time_ago_in_s"], 3600.0);
        Ok(())
    }

    #[test]
    fn freshness_json_carries_models_and_resource_types() -> anyhow::Result<()> {
        let input = freshness_input(
            "freshness",
            vec![
                with_freshness("source.p.s.orders", "source", FreshnessGrade::Pass),
                with_freshness("model.p.stg_orders", "model", FreshnessGrade::Warn),
                sample_result("model.p.m", NodeStatus::Success, 1.0),
            ],
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&build_freshness_json(&input, false)?)?;
        assert_eq!(parsed["metadata"]["dbt_schema_version"], FRESHNESS_SCHEMA);
        let results = parsed["results"].as_array().expect("results array");
        assert_eq!(results.len(), 2, "both measured nodes belong in freshness.json");
        assert_eq!(results[0]["resource_type"], "source");
        assert_eq!(results[1]["resource_type"], "model");
        assert_eq!(results[1]["status"], "warn");
        Ok(())
    }
}
