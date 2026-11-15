//! Building `run_results.json`.

use std::collections::BTreeMap;

use crate::types::{NodeExecutionResult, StoreArtifactsInput};

use crate::activities::artifact_status::ArtifactStatus;

use super::metadata::{DBT_VERSION, artifact_env};

/// Schema the artifact claims to follow. dbt-fusion writes v6.
const RUN_RESULTS_SCHEMA: &str = "https://schemas.getdbt.com/dbt/run-results/v6.json";

/// Build the `run_results.json` content from the store artifacts input.
///
/// Serialized through upstream's own `RunResultsArtifact` rather than a
/// hand-written JSON object: the artifact is read by dbt's own tooling, and a
/// field this worker forgets is a field those consumers do not find.
pub(super) fn build_run_results_json(
    input: &StoreArtifactsInput,
    compiled_sql: &BTreeMap<String, String>,
) -> Result<String, anyhow::Error> {
    use dbt_schemas::schemas::{RunResultsArgs, RunResultsArtifact, RunResultsMetadata};
    use std::collections::BTreeMap;

    let command = input.command.as_deref().unwrap_or("run");
    let artifact = RunResultsArtifact {
        metadata: RunResultsMetadata {
            dbt_schema_version: RUN_RESULTS_SCHEMA.to_string(),
            dbt_version: DBT_VERSION.to_string(),
            generated_at: chrono::Utc::now(),
            invocation_id: input.invocation_id.clone(),
            invocation_started_at: input.started_at,
            env: artifact_env(),
        },
        results: input
            .node_results
            .iter()
            .map(|result| run_result_output(result, compiled_sql))
            .collect(),
        elapsed_time: input.elapsed_time,
        args: RunResultsArgs {
            command: command.to_string(),
            which: command.to_string(),
            __other__: BTreeMap::new(),
        },
    };
    serde_json::to_string_pretty(&artifact).map_err(Into::into)
}

/// Convert one node result into dbt's `run_results.json` row.
fn run_result_output(
    result: &NodeExecutionResult,
    compiled_sql: &BTreeMap<String, String>,
) -> dbt_schemas::schemas::RunResultOutput {
    use dbt_schemas::schemas::{RunResultOutput, TimingInfo};

    RunResultOutput {
        status: dbt_status(result).as_str().to_string(),
        timing: result
            .timing
            .iter()
            .map(|t| TimingInfo {
                name: t.name.clone(),
                started_at: t.started_at.parse().ok(),
                completed_at: t.completed_at.parse().ok(),
            })
            .collect(),
        // dbt names the OS thread that ran the node. A node here ran in its own
        // activity, on a worker that may not even be this one, so there is no
        // thread to name — upstream uses "main" for rows it synthesizes outside
        // the task graph, which is the same situation.
        thread_id: "main".to_string(),
        execution_time: result.execution_time,
        adapter_response: result
            .adapter_response
            .iter()
            .map(|(k, v)| (k.clone(), json_to_yml(v)))
            .collect(),
        message: result.message.clone(),
        failures: result.failures,
        unique_id: result.unique_id.clone(),
        compiled: Some(result.compiled_code.is_some() || result.compiled_code_ref.is_some()),
        // Read back from the store when the node spilled it there, otherwise
        // whatever it carried inline.
        compiled_code: compiled_sql
            .get(&result.unique_id)
            .cloned()
            .or_else(|| result.compiled_code.clone()),
        // The relation the node actually wrote, which is what a consumer needs
        // to find the table this row describes.
        relation_name: result.relation_name.clone(),
        batch_results: None,
        static_analysis_off_reason: None,
    }
}

/// dbt's status for one node: a freshness check reports the grade it
/// measured, an assertion passes or fails, anything else keeps its own word.
///
/// Shared with the `results` an `on-run-end` hook sees, which carry the same
/// words as `run_results.json`.
pub(in crate::activities) fn dbt_status(result: &NodeExecutionResult) -> ArtifactStatus {
    result.freshness.as_ref().map_or_else(
        || ArtifactStatus::for_node(&result.unique_id, result.status),
        |freshness| ArtifactStatus::for_freshness(freshness.status),
    )
}

/// Adapter responses arrive as JSON and leave as YAML values — the same data,
/// in the type upstream's artifact row holds.
fn json_to_yml(v: &serde_json::Value) -> dbt_yaml::Value {
    dbt_yaml::to_value(v).unwrap_or_else(|_| dbt_yaml::Value::null())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use anyhow::Context;

    use super::*;
    use crate::types::{FreshnessGrade, NodeStatus};

    use super::super::test_support::{freshness_input, sample_result, with_freshness};

    #[test]
    fn build_run_results_json_structure() -> anyhow::Result<()> {
        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-123".into(),
            project: None,
            command: None,
            node_results: vec![
                sample_result("model.a", NodeStatus::Success, 1.5),
                sample_result("model.b", NodeStatus::Error, 0.3),
            ],
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 12.5,
        };

        let json_str = build_run_results_json(&input, &BTreeMap::new())?;
        let parsed: serde_json::Value = serde_json::from_str(&json_str)?;

        assert_eq!(parsed["metadata"]["invocation_id"], "inv-123");
        assert_eq!(parsed["metadata"]["dbt_version"], DBT_VERSION);
        assert_eq!(parsed["metadata"]["dbt_schema_version"], RUN_RESULTS_SCHEMA);
        assert_eq!(
            parsed["metadata"]["env"]["DBT_TEMPORAL_VERSION"],
            env!("CARGO_PKG_VERSION"),
            "the worker's own version is kept, just not as dbt's"
        );
        assert!(parsed["metadata"]["generated_at"].is_string());
        assert_eq!(parsed["args"]["command"], "run");
        assert_eq!(
            parsed["results"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("results is array"))?
                .len(),
            2
        );
        // Wall time of the run, taken from the workflow clock — not the sum of
        // node durations, which double-counts every second two nodes shared.
        assert!(
            (parsed["elapsed_time"]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("elapsed_time is f64"))?
                - 12.5)
                .abs()
                < f64::EPSILON
        );
        Ok(())
    }

    /// dbt does not call a passing test "success". A consumer reading
    /// run_results.json for test outcomes looks for `pass` and `fail`.
    #[test]
    fn test_nodes_report_dbt_test_statuses() -> anyhow::Result<()> {
        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            node_results: vec![
                sample_result("test.p.not_null_id", NodeStatus::Success, 0.1),
                sample_result("test.p.unique_id", NodeStatus::Error, 0.1),
                sample_result("model.p.m", NodeStatus::Success, 0.1),
                with_freshness("source.p.s.orders", "source", FreshnessGrade::Warn),
            ],
            ..freshness_input("build", vec![])
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&build_run_results_json(&input, &BTreeMap::new())?)?;
        let results = parsed["results"].as_array().context("results array")?;

        assert_eq!(results[0]["status"], "pass", "test.p.not_null_id: {parsed}");
        assert_eq!(results[1]["status"], "fail", "test.p.unique_id: {parsed}");
        assert_eq!(results[2]["status"], "success", "a model still succeeds");
        assert_eq!(results[3]["status"], "warn", "freshness reports what it measured");
        Ok(())
    }

    /// The artifact this worker writes is the input `retry_from` reads, so the
    /// two must agree on every kind of result a run can contain.
    #[test]
    fn written_run_results_select_exactly_the_nodes_retry_reruns() -> anyhow::Result<()> {
        let input = freshness_input(
            "build",
            vec![
                sample_result("model.p.ok", NodeStatus::Success, 0.1),
                sample_result("model.p.bad", NodeStatus::Error, 0.1),
                sample_result("model.p.blocked", NodeStatus::Skipped, 0.0),
                sample_result("model.p.killed", NodeStatus::Cancelled, 0.0),
                sample_result("test.p.passes", NodeStatus::Success, 0.1),
                sample_result("test.p.fails", NodeStatus::Error, 0.1),
                sample_result("test.p.warns", NodeStatus::Warn, 0.1),
                sample_result("unit_test.p.m.fails", NodeStatus::Error, 0.1),
                with_freshness("source.p.s.fresh", "source", FreshnessGrade::Pass),
                with_freshness("source.p.s.aging", "source", FreshnessGrade::Warn),
                with_freshness("source.p.s.stale", "source", FreshnessGrade::Error),
            ],
        );

        let written = build_run_results_json(&input, &BTreeMap::new())?;
        let ids = crate::activities::plan::parse_retry_node_ids(written.as_bytes())?;

        assert_eq!(
            ids.into_iter().collect::<Vec<_>>(),
            [
                "model.p.bad",
                "model.p.blocked",
                "model.p.killed",
                "source.p.s.stale",
                "test.p.fails",
                "unit_test.p.m.fails",
            ]
        );
        Ok(())
    }

    #[test]
    fn build_run_results_json_empty_results() -> anyhow::Result<()> {
        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-empty".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        };

        let json_str = build_run_results_json(&input, &BTreeMap::new())?;
        let parsed: serde_json::Value = serde_json::from_str(&json_str)?;

        assert_eq!(
            parsed["results"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("results is array"))?
                .len(),
            0
        );
        assert!(
            (parsed["elapsed_time"]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("elapsed_time is f64"))?)
            .abs()
                < f64::EPSILON
        );
        Ok(())
    }
}
