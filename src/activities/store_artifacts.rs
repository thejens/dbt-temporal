use anyhow::Context;
use std::collections::BTreeMap;

use bytes::Bytes;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tracing::{info, warn};

use crate::artifact_store::ArtifactStore;
use crate::error::DbtTemporalError;
use crate::types::{DbtCommand, StoreArtifactsInput, StoreArtifactsOutput};

use super::DbtActivities;
use super::heartbeat;
use super::retry;

/// Outer wrapper — cancellation, heartbeating, and retry classification.
///
/// This activity is pure object-store I/O and runs *after* every node has
/// finished, so a transient 5xx here would otherwise throw away a completed
/// run's results. Store failures are tagged `ArtifactStore` and retry;
/// everything else (missing store config, serialization bugs) is permanent.
pub async fn store_artifacts_outer(
    activities: &DbtActivities,
    ctx: ActivityContext,
    input: StoreArtifactsInput,
) -> Result<StoreArtifactsOutput, ActivityError> {
    let project = input.project.clone();
    tokio::select! {
        result = store_artifacts_inner(activities, input) => {
            result.map_err(|e| {
                warn!(error = %format!("{e:#}"), "store_artifacts failed");
                let patterns = project
                    .as_deref()
                    .and_then(|p| retry::registry_non_retryable_patterns(&activities.registry, p));
                retry::classify(
                    e,
                    patterns.as_deref().unwrap_or(&[]),
                    retry::Unclassified::Permanent,
                )
            })
        }
        () = ctx.cancelled() => {
            info!("store_artifacts cancelled");
            Err(ActivityError::cancelled())
        }
        // Uploading a large manifest to object storage can outlast the
        // heartbeat timeout on a slow link; keep the tick alive so the server
        // does not mistake a slow upload for a dead worker.
        never = heartbeat::heartbeat_loop(&ctx) => match never {},
    }
}

/// Prepend every earlier segment's results and log to this one's.
///
/// A missing or unreadable checkpoint is fatal: writing `run_results.json`
/// without the nodes an earlier segment ran would describe the run as smaller
/// than it was, and nothing downstream could tell.
async fn collect_prior_segments(
    store: &dyn ArtifactStore,
    mut input: StoreArtifactsInput,
) -> Result<StoreArtifactsInput, anyhow::Error> {
    if input.prior_segments.is_empty() {
        return Ok(input);
    }

    let mut results = Vec::with_capacity(input.node_results.len());
    let mut log = Vec::new();
    for state_ref in &input.prior_segments {
        let (segment_results, segment_log) =
            crate::activities::segment_state::read_segment_payload(store, state_ref)
                .await
                .map_err(|e| store_io_error("collecting an earlier segment", e))?;
        results.extend(segment_results);
        log.extend(segment_log);
    }
    info!(
        segments = input.prior_segments.len(),
        results = results.len(),
        "collected earlier segments for artifact assembly"
    );

    results.append(&mut input.node_results);
    input.node_results = results;
    if let Some(tail) = input.run_log.take() {
        log.push(tail);
    }
    input.run_log = Some(log.join("\n"));
    Ok(input)
}

/// Tag an artifact-store I/O failure as retryable, preserving the call context.
fn store_io_error(context: &'static str, e: anyhow::Error) -> anyhow::Error {
    DbtTemporalError::ArtifactStore(e.context(context)).into()
}

/// Store run_results.json and manifest.json to the configured artifact store.
///
/// `store_artifacts_outer` wraps this with cancellation, heartbeat and error
/// classification. Also the entry point for integration tests that drive
/// artifact writing without a Temporal activity context.
pub async fn store_artifacts_inner(
    activities: &DbtActivities,
    input: StoreArtifactsInput,
) -> Result<StoreArtifactsOutput, anyhow::Error> {
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        anyhow::anyhow!("ArtifactStore not configured but store_artifacts was called")
    })?;

    // A run that continued as new left each earlier segment's results and log
    // in that segment's own checkpoint rather than dragging them through every
    // history since. Collect them so the artifacts describe the whole run.
    let input = collect_prior_segments(store.as_ref(), input).await?;

    // The nodes wrote their compiled SQL to the store instead of carrying it
    // through the workflow; read it back for the one artifact that reports it.
    let compiled_sql = load_compiled_sql(store.as_ref(), &input.node_results).await;

    let run_results_json =
        build_run_results_json(&input, &compiled_sql).context("serializing run_results.json")?;

    let run_results_path = store
        .store(&input.invocation_id, "run_results.json", run_results_json.into_bytes().into())
        .await
        .map_err(|e| store_io_error("storing run_results.json", e))?;

    info!(path = %run_results_path, "stored run_results.json");

    // Store manifest (if inline) or note existing ref.
    let manifest_path = if let Some(manifest_json) = &input.manifest_json {
        store
            .store(
                &input.invocation_id,
                "manifest.json",
                Bytes::copy_from_slice(manifest_json.as_bytes()),
            )
            .await
            .map_err(|e| store_io_error("storing manifest.json", e))?
    } else if let Some(manifest_ref) = &input.manifest_ref {
        // Already stored during plan phase.
        manifest_ref.clone()
    } else {
        anyhow::bail!("neither manifest_json nor manifest_ref provided");
    };

    info!(path = %manifest_path, "manifest.json available");

    // Store run log if enabled and provided.
    let log_path = if let Some(run_log) = &input.run_log {
        if activities.write_run_log.0 {
            let path = store
                .store(&input.invocation_id, "log.txt", Bytes::copy_from_slice(run_log.as_bytes()))
                .await
                .map_err(|e| store_io_error("storing log.txt", e))?;
            info!(path = %path, "stored log.txt");
            Some(path)
        } else {
            None
        }
    } else {
        None
    };

    // Optionally generate catalog.json (column metadata for the run's
    // relations). Catalog problems are logged, never fatal — the run's real
    // artifacts are already stored at this point.
    let catalog_path = if activities.write_catalog.0 {
        match generate_and_store_catalog(activities, &input, store.as_ref()).await {
            Ok(path) => {
                info!(path = %path, "stored catalog.json");
                Some(path)
            }
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "catalog.json generation failed (non-fatal)");
                None
            }
        }
    } else {
        None
    };

    // Freshness runs additionally produce dbt's freshness artifacts. Only nodes
    // that completed the check carry an outcome; a stale node fails its
    // activity and appears in run_results with the error message instead.
    if let Some(command) = input.command.as_deref().and_then(DbtCommand::parse)
        && command.is_freshness()
    {
        let sources_only = command == DbtCommand::SourceFreshness;
        // `dbt source freshness` writes sources.json unconditionally, an empty
        // result set included. The unified spelling only rewrites it when the
        // run actually measured a source, so a model-only freshness run does
        // not clobber a good artifact with an empty one.
        if sources_only || input.node_results.iter().any(is_source_result) {
            let sources_json =
                build_freshness_json(&input, true).context("serializing sources.json")?;
            let path = store
                .store(&input.invocation_id, "sources.json", sources_json.into_bytes().into())
                .await
                .map_err(|e| store_io_error("storing sources.json", e))?;
            info!(path = %path, "stored sources.json");
        }
        if !sources_only {
            let freshness_json =
                build_freshness_json(&input, false).context("serializing freshness.json")?;
            let path = store
                .store(&input.invocation_id, "freshness.json", freshness_json.into_bytes().into())
                .await
                .map_err(|e| store_io_error("storing freshness.json", e))?;
            info!(path = %path, "stored freshness.json");
        }
    }

    Ok(StoreArtifactsOutput {
        run_results_path,
        manifest_path,
        log_path,
        catalog_path,
    })
}

/// Build catalog.json for the run's project and store it.
async fn generate_and_store_catalog(
    activities: &DbtActivities,
    input: &StoreArtifactsInput,
    store: &dyn ArtifactStore,
) -> Result<String, anyhow::Error> {
    let state = activities
        .registry
        .get(input.project.as_deref())
        .context("resolving project for catalog generation")?;
    // Catalog generation queries the warehouse through the adapter, which emits
    // dbt telemetry spans; those need an `Invocation` root above them. Scoped to
    // the synchronous build so no span guard is held across the store `await`.
    let catalog_json =
        super::node_telemetry::invocation_span(&input.invocation_id, "dbt docs generate")
            .in_scope(|| {
                super::catalog::build_catalog_json(
                    state,
                    &input.node_results,
                    &input.invocation_id,
                    &crate::activities::render_env::RenderOverrides {
                        cancellation: &state.cancellation_source.token(),
                        env: &input.env,
                        target: input.target.as_deref(),
                        // Neither reaches a `get_columns_in_relation` call.
                        vars: &BTreeMap::new(),
                        full_refresh: false,
                    },
                )
            })?;
    store
        .store(&input.invocation_id, "catalog.json", catalog_json.into_bytes().into())
        .await
        .context("storing catalog.json")
}

/// Fetch each node's compiled SQL back from the store, keyed by unique id.
///
/// A node that failed to store its SQL, or one from a run without artifact
/// storage, simply has none here and keeps whatever it carried inline. A
/// failure to read one is logged rather than fatal: `run_results.json` without
/// one node's compiled SQL is worth far more than no artifact at all, and this
/// runs after every node has already finished.
async fn load_compiled_sql(
    store: &dyn ArtifactStore,
    results: &[crate::types::NodeExecutionResult],
) -> BTreeMap<String, String> {
    let mut compiled = BTreeMap::new();
    for result in results {
        let Some(reference) = result.compiled_code_ref.as_deref() else {
            continue;
        };
        match store.retrieve(reference).await {
            Ok(bytes) => match String::from_utf8(bytes.to_vec()) {
                Ok(sql) => {
                    compiled.insert(result.unique_id.clone(), sql);
                }
                Err(e) => tracing::warn!(
                    node = %result.unique_id,
                    error = %e,
                    "compiled SQL is not UTF-8; omitting it from run_results.json"
                ),
            },
            Err(e) => tracing::warn!(
                node = %result.unique_id,
                reference,
                error = %format!("{e:#}"),
                "could not read back compiled SQL; omitting it from run_results.json"
            ),
        }
    }
    compiled
}

/// dbt version stamped into artifact metadata.
///
/// The artifacts describe a dbt run, so consumers read this as dbt's version,
/// not the orchestrator's — `run_results.json` used to report dbt-temporal's,
/// which made every artifact claim a dbt that does not exist. None of the
/// pinned crates exports its version as a constant (each stamps its own
/// `CARGO_PKG_VERSION` where it needs one), so it is written here and moves
/// with the pin; `dbt_version_matches_the_pinned_crates` fails if it drifts.
const DBT_VERSION: &str = "2.0.0-rc.1";

/// Schema the artifact claims to follow. dbt-fusion writes v6.
const RUN_RESULTS_SCHEMA: &str = "https://schemas.getdbt.com/dbt/run-results/v6.json";

/// Build the `run_results.json` content from the store artifacts input.
///
/// Serialized through upstream's own `RunResultsArtifact` rather than a
/// hand-written JSON object: the artifact is read by dbt's own tooling, and a
/// field this worker forgets is a field those consumers do not find.
fn build_run_results_json(
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
            // The orchestrator's own version, kept where it does not pretend to
            // be dbt's.
            env: BTreeMap::from([(
                "DBT_TEMPORAL_VERSION".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            )]),
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
    result: &crate::types::NodeExecutionResult,
    compiled_sql: &BTreeMap<String, String>,
) -> dbt_schemas::schemas::RunResultOutput {
    use dbt_schemas::schemas::{RunResultOutput, TimingInfo};

    RunResultOutput {
        status: dbt_status(result),
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

/// dbt's status vocabulary for one node.
///
/// dbt does not use one set of words for everything: a data test passes or
/// fails, a freshness check reports the status it measured, and a model
/// succeeds or errors. Emitting `success` for a passing test made every test
/// row unreadable to a consumer expecting `pass`.
fn dbt_status(result: &crate::types::NodeExecutionResult) -> String {
    use crate::types::NodeStatus;

    if let Some(freshness) = result.freshness.as_ref() {
        return freshness.status.clone();
    }
    let is_test =
        result.unique_id.starts_with("test.") || result.unique_id.starts_with("unit_test.");
    match (is_test, result.status) {
        (true, NodeStatus::Success) => "pass".to_string(),
        (true, NodeStatus::Error) => "fail".to_string(),
        // `warn` is already dbt's word for it, for a test and a model alike.
        (_, status) => status.as_str().to_string(),
    }
}

/// Adapter responses arrive as JSON and leave as YAML values — the same data,
/// in the type upstream's artifact row holds.
fn json_to_yml(v: &serde_json::Value) -> dbt_yaml::Value {
    dbt_yaml::to_value(v).unwrap_or_else(|_| dbt_yaml::Value::null())
}

/// Whether a node result belongs to a source.
///
/// Keyed on the unique_id prefix, which is how dbt itself separates sources
/// from models when it only has the id to go on — a resolved node is not in
/// reach here, only the results the workflow accumulated.
fn is_source_result(result: &crate::types::NodeExecutionResult) -> bool {
    result.unique_id.starts_with("source.")
}

/// Build the `sources.json` / `freshness.json` content from freshness-bearing
/// node results.
///
/// `sources_only` picks between the two: `sources.json` keeps the shape
/// `dbt source freshness` has always written (sources, no `resource_type`),
/// while `freshness.json` carries every measured node and tags each row with
/// its resource type.
fn build_freshness_json(
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
    let artifact = serde_json::json!({
        "metadata": {
            "invocation_id": input.invocation_id,
            "dbt_version": env!("CARGO_PKG_VERSION"),
            "generated_at": chrono::Utc::now().to_rfc3339(),
        },
        "results": results,
        "elapsed_time": total.as_secs_f64(),
    });
    serde_json::to_string_pretty(&artifact).map_err(Into::into)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::artifact_store::LocalArtifactStore;
    use crate::types::{NodeExecutionResult, NodeStatus};
    use std::collections::BTreeMap;

    fn sample_result(unique_id: &str, status: NodeStatus, time: f64) -> NodeExecutionResult {
        NodeExecutionResult {
            unique_id: unique_id.into(),
            status,
            execution_time: time,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }
    }

    fn with_freshness(unique_id: &str, resource_type: &str, status: &str) -> NodeExecutionResult {
        let mut result = sample_result(unique_id, NodeStatus::Success, 0.4);
        result.freshness = Some(crate::types::FreshnessOutcome {
            max_loaded_at: "2026-06-12T10:00:00+00:00".into(),
            snapshotted_at: "2026-06-12T11:00:00+00:00".into(),
            max_loaded_at_time_ago_in_s: 3600.0,
            status: status.into(),
            resource_type: resource_type.into(),
            criteria: dbt_schemas::schemas::common::FreshnessDefinition::default(),
        });
        result
    }

    fn freshness_input(
        command: &str,
        node_results: Vec<NodeExecutionResult>,
    ) -> StoreArtifactsInput {
        StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-9".into(),
            project: None,
            command: Some(command.into()),
            node_results,
            manifest_json: None,
            manifest_ref: None,
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        }
    }

    #[test]
    fn sources_json_includes_only_source_freshness_results() -> anyhow::Result<()> {
        let input = freshness_input(
            "freshness",
            vec![
                with_freshness("source.p.s.orders", "source", "pass"),
                with_freshness("model.p.stg_orders", "model", "warn"),
                sample_result("model.p.m", NodeStatus::Success, 1.0),
            ],
        );

        let parsed: serde_json::Value = serde_json::from_str(&build_freshness_json(&input, true)?)?;
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

    /// Compiled SQL crosses Temporal once, to the store, and comes back only
    /// here — the one artifact that reports it. The workflow accumulator, the
    /// checkpoint and the hook payloads carry a reference instead.
    #[tokio::test]
    async fn run_results_reads_compiled_sql_back_from_the_store() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = LocalArtifactStore::new(dir.path().to_path_buf());
        let reference = store
            .store(
                "inv-9",
                &crate::activities::execute_node::compiled_sql_artifact_name("model.p.m"),
                Bytes::from_static(b"select 1 as id"),
            )
            .await?;

        let mut spilled = sample_result("model.p.m", NodeStatus::Success, 1.0);
        spilled.compiled_code = None;
        spilled.compiled_code_ref = Some(reference);

        let compiled = load_compiled_sql(&store, std::slice::from_ref(&spilled)).await;
        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            node_results: vec![spilled],
            ..freshness_input("build", vec![])
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&build_run_results_json(&input, &compiled)?)?;
        let row = &parsed["results"][0];
        assert_eq!(row["compiled_code"], "select 1 as id", "{parsed}");
        assert_eq!(row["compiled"], true, "{parsed}");
        Ok(())
    }

    /// A reference that cannot be read costs that node's SQL, not the whole
    /// artifact — every node has already finished by the time this runs.
    #[tokio::test]
    async fn an_unreadable_reference_does_not_lose_the_artifact() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = LocalArtifactStore::new(dir.path().to_path_buf());

        let mut spilled = sample_result("model.p.m", NodeStatus::Success, 1.0);
        spilled.compiled_code = None;
        spilled.compiled_code_ref = Some("inv-9/compiled/nothing-here.sql".to_string());

        let compiled = load_compiled_sql(&store, std::slice::from_ref(&spilled)).await;
        assert!(compiled.is_empty(), "nothing was read");

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            node_results: vec![spilled],
            ..freshness_input("build", vec![])
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&build_run_results_json(&input, &compiled)?)?;
        assert_eq!(parsed["results"][0]["unique_id"], "model.p.m", "the row survives: {parsed}");
        Ok(())
    }

    /// A stale source is the one measurement anybody reads a freshness artifact
    /// for. It used to be missing from both: the stale verdict carried no
    /// outcome, the activity failed instead of returning one, and the filter
    /// here keeps only results that have one.
    #[test]
    fn sources_json_includes_a_stale_source() -> anyhow::Result<()> {
        let mut stale = with_freshness("source.p.s.orders", "source", "error");
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
                with_freshness("source.p.s.orders", "source", "pass"),
                with_freshness("model.p.stg_orders", "model", "warn"),
                sample_result("model.p.m", NodeStatus::Success, 1.0),
            ],
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&build_freshness_json(&input, false)?)?;
        let results = parsed["results"].as_array().expect("results array");
        assert_eq!(results.len(), 2, "both measured nodes belong in freshness.json");
        assert_eq!(results[0]["resource_type"], "source");
        assert_eq!(results[1]["resource_type"], "model");
        assert_eq!(results[1]["status"], "warn");
        Ok(())
    }

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

    /// `dbt_version` is written by hand because nothing in the pinned crates
    /// exports it. Cargo.lock does record it, so a bump that forgets this
    /// constant fails here rather than shipping artifacts that name the wrong
    /// dbt.
    #[test]
    fn dbt_version_matches_the_pinned_crates() -> anyhow::Result<()> {
        let lock = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock"))
            .context("reading Cargo.lock")?;
        let pinned = lock
            .split("[[package]]")
            .find(|block| block.contains("name = \"dbt-schemas\""))
            .and_then(|block| {
                block
                    .lines()
                    .find_map(|line| line.trim().strip_prefix("version = "))
            })
            .map(|v| v.trim_matches('"').to_string())
            .context("dbt-schemas not found in Cargo.lock")?;

        assert_eq!(
            pinned, DBT_VERSION,
            "DBT_VERSION must move with the dbt-core pin in Cargo.toml"
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
                with_freshness("source.p.s.orders", "source", "warn"),
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

    // --- store_artifacts_inner: end-to-end against the local artifact store ---

    use std::sync::Arc;

    use crate::config::{
        RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts, WriteRunLog,
    };
    use crate::project_registry::ProjectRegistry;

    /// The plan a checkpoint carries; nothing in these tests reads it.
    fn minimal_plan() -> crate::types::ExecutionPlan {
        crate::types::ExecutionPlan {
            project: "p".to_string(),
            nodes: BTreeMap::new(),
            levels: Vec::new(),
            manifest_json: None,
            manifest_ref: None,
            invocation_id: "inv-multi".to_string(),
            search_attributes: BTreeMap::new(),
            write_artifacts: true,
            has_on_run_start: false,
            has_on_run_end: false,
            priority_scheduling: false,
            has_project_checks: false,
        }
    }

    fn activities_with_local_store(
        base_dir: std::path::PathBuf,
        write_run_log: bool,
    ) -> DbtActivities {
        DbtActivities {
            registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
            artifact_store: Some(Arc::new(LocalArtifactStore::new(base_dir))),
            search_attr_config: SearchAttributeConfig(BTreeMap::new()),
            registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
            write_run_log: WriteRunLog(write_run_log),
            write_artifacts: WriteArtifacts(true),
            write_catalog: crate::config::WriteCatalog(false),
            priority_scheduling: crate::config::PriorityScheduling(false),
        }
    }

    /// A run that continued as new left each segment's results in that
    /// segment's checkpoint. `run_results.json` has to describe the whole run,
    /// so the activity walks the chain rather than reporting only the last leg.
    #[tokio::test]
    async fn store_artifacts_inner_collects_every_segment() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), true);
        let store = Arc::clone(
            activities
                .artifact_store
                .as_ref()
                .expect("store configured"),
        );

        // Two earlier segments, each holding its own results and log.
        let mut refs = Vec::new();
        for (segment, node) in [(1u32, "model.first"), (2, "model.second")] {
            let state = crate::types::RunSegmentState {
                schema_version: 1,
                invocation_id: "inv-multi".into(),
                segment,
                plan: minimal_plan(),
                prior_segments: refs.clone(),
                all_results: vec![sample_result(node, NodeStatus::Success, 0.1)],
                log_lines: vec![format!("segment {segment} line")],
                node_status: crate::types::NodeStatusTree {
                    nodes: BTreeMap::new(),
                },
                failed_nodes: Vec::new(),
                had_failure: false,
                effective_env: BTreeMap::new(),
                hook_errors: Vec::new(),
                total_nodes: 3,
                node_counter: usize::try_from(segment)?,
                next_level: usize::try_from(segment)?,
                started_at: None,
            };
            let json = serde_json::to_vec(&state)?;
            refs.push(
                store
                    .store("inv-multi", &format!("run_segment_state_{segment}.json"), json.into())
                    .await?,
            );
        }

        let input = StoreArtifactsInput {
            prior_segments: refs,
            invocation_id: "inv-multi".into(),
            project: None,
            command: None,
            node_results: vec![sample_result("model.last", NodeStatus::Success, 0.1)],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: Some("final line".to_string()),
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        };

        let out = store_artifacts_inner(&activities, input).await?;
        let run_results = std::fs::read_to_string(&out.run_results_path)?;
        let parsed: serde_json::Value = serde_json::from_str(&run_results)?;
        let ids: Vec<&str> = parsed["results"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("results is array"))?
            .iter()
            .filter_map(|r| r["unique_id"].as_str())
            .collect();
        assert_eq!(ids, vec!["model.first", "model.second", "model.last"], "in run order");

        let log = std::fs::read_to_string(out.log_path.as_ref().expect("log written"))?;
        assert_eq!(log, "segment 1 line\nsegment 2 line\nfinal line");
        Ok(())
    }

    /// A checkpoint that cannot be read is fatal: `run_results.json` without an
    /// earlier segment's nodes describes the run as smaller than it was, and
    /// nothing downstream could tell.
    #[tokio::test]
    async fn store_artifacts_inner_refuses_a_missing_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            prior_segments: vec![
                dir.path()
                    .join("inv-multi/run_segment_state_1.json")
                    .to_string_lossy()
                    .into_owned(),
            ],
            invocation_id: "inv-multi".into(),
            project: None,
            command: None,
            node_results: vec![sample_result("model.last", NodeStatus::Success, 0.1)],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        };

        let err = store_artifacts_inner(&activities, input)
            .await
            .expect_err("a missing checkpoint must be reported");
        assert!(
            format!("{err:#}").contains("collecting an earlier segment"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn store_artifacts_inner_writes_run_results_and_inline_manifest() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-1".into(),
            project: None,
            command: None,
            node_results: vec![sample_result("model.a", NodeStatus::Success, 0.1)],
            manifest_json: Some("{\"manifest\":\"yes\"}".to_string()),
            manifest_ref: None,
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        };

        let out = store_artifacts_inner(&activities, input).await?;
        assert!(out.run_results_path.contains("run_results.json"));
        assert!(out.manifest_path.contains("manifest.json"));
        assert!(out.log_path.is_none());

        let on_disk = std::fs::read_to_string(&out.run_results_path)?;
        assert!(on_disk.contains("model.a"));
        let manifest = std::fs::read_to_string(&out.manifest_path)?;
        assert!(manifest.contains("manifest"));
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_uses_existing_manifest_ref() -> anyhow::Result<()> {
        // When the plan phase already stored a large manifest at a known path,
        // the activity should pass that path through verbatim instead of
        // re-storing it.
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-2".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: None,
            manifest_ref: Some("/already/stored/manifest.json".to_string()),
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        };

        let out = store_artifacts_inner(&activities, input).await?;
        assert_eq!(out.manifest_path, "/already/stored/manifest.json");
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_errors_when_neither_manifest_provided() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-3".into(),
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

        let err = store_artifacts_inner(&activities, input)
            .await
            .expect_err("absent manifest must error");
        assert!(err.to_string().contains("manifest"), "got: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_writes_log_when_enabled() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), true);

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-log".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            started_at: None,
            elapsed_time: 0.0,
            env: BTreeMap::new(),
            target: None,
            run_log: Some("line a\nline b".to_string()),
        };

        let out = store_artifacts_inner(&activities, input).await?;
        let log_path = out
            .log_path
            .expect("log_path should be set when run_log is on");
        assert!(log_path.ends_with("log.txt"));
        let on_disk = std::fs::read_to_string(&log_path)?;
        assert_eq!(on_disk, "line a\nline b");
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_skips_log_when_run_log_disabled() -> anyhow::Result<()> {
        // Run log was supplied but the writer is disabled — the file must not
        // be written and log_path stays None.
        let dir = tempfile::tempdir()?;
        let activities = activities_with_local_store(dir.path().to_path_buf(), false);

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-skiplog".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            started_at: None,
            elapsed_time: 0.0,
            env: BTreeMap::new(),
            target: None,
            run_log: Some("would-be-log".to_string()),
        };

        let out = store_artifacts_inner(&activities, input).await?;
        assert!(out.log_path.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn store_artifacts_inner_errors_when_no_artifact_store_configured() -> anyhow::Result<()>
    {
        // The activity guards against being called on an unconfigured worker.
        let activities = DbtActivities {
            registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
            artifact_store: None,
            search_attr_config: SearchAttributeConfig(BTreeMap::new()),
            registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
            write_run_log: WriteRunLog(false),
            write_artifacts: WriteArtifacts(true),
            write_catalog: crate::config::WriteCatalog(false),
            priority_scheduling: crate::config::PriorityScheduling(false),
        };

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            invocation_id: "inv-noop".into(),
            project: None,
            command: None,
            node_results: vec![],
            manifest_json: Some("{}".to_string()),
            manifest_ref: None,
            run_log: None,
            env: BTreeMap::new(),
            target: None,
            started_at: None,
            elapsed_time: 0.0,
        };

        let err = store_artifacts_inner(&activities, input)
            .await
            .expect_err("missing ArtifactStore must error");
        assert!(err.to_string().contains("ArtifactStore not configured"));
        // A misconfigured worker is permanent — retrying cannot fix it.
        assert!(
            !retry::downcast_or_default(err, retry::Unclassified::Permanent).is_retryable(),
            "missing store config must not retry"
        );
        Ok(())
    }

    /// The run is already finished by the time this activity runs, so losing it
    /// to a transient object-store 5xx would discard completed work.
    #[tokio::test]
    async fn store_io_failure_is_classified_retryable() {
        let err = store_io_error("storing run_results.json", anyhow::anyhow!("503 slow down"));

        let classified = retry::downcast_or_default(err, retry::Unclassified::Permanent);
        assert!(
            classified.is_retryable(),
            "artifact-store I/O must retry even under the activity's permanent default"
        );
        let msg = classified.to_string();
        assert!(msg.contains("storing run_results.json"), "context lost: {msg}");
        assert!(msg.contains("503 slow down"), "cause lost: {msg}");
    }
}
