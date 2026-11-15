//! Turning what a materialization stored into the node's status, message and
//! result.

use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;
use tracing::{info, warn};

use crate::error::DbtTemporalError;
use crate::types::{NodeExecutionResult, NodeStatus};

use super::super::node_helpers::{extract_adapter_response, extract_test_outcome};
use super::context::{CompiledNode, NodeRun};
use super::materialize::Materialized;
use super::outcome::{build_success_message, domain_failure_message, executed_relation_name};
use super::unit_test;

/// What the node's stored results say about how it went.
pub(super) struct Verdict {
    pub(super) status: NodeStatus,
    pub(super) message: Option<String>,
    pub(super) failures: Option<i64>,
    pub(super) adapter_response: BTreeMap<String, serde_json::Value>,
}

/// Read the adapter response the materialization stored, and fail a node whose
/// built-in materialization stored none.
pub(super) fn read_adapter_response(
    run: &NodeRun<'_>,
    compiled: &CompiledNode,
    materialized: &Materialized,
) -> Result<BTreeMap<String, serde_json::Value>, DbtTemporalError> {
    // Extract adapter response from the ResultStore.
    // Materialization macros call store_result('main', response) during rendering.
    let adapter_response = extract_adapter_response(&compiled.result_store);

    require_adapter_response(
        run.unique_id,
        run.resource_type,
        &materialized.materialization,
        &materialized.template,
        &run.state.project_name,
        &adapter_response,
    )?;
    Ok(adapter_response)
}

/// Decide the node's status and message from its unit-test or data-test
/// outcome, and log how it ended.
pub(super) fn judge_node(
    run: &NodeRun<'_>,
    compiled: &CompiledNode,
    materialized: &Materialized,
    adapter_response: BTreeMap<String, serde_json::Value>,
) -> Result<Verdict, anyhow::Error> {
    let unique_id = run.unique_id;
    let rt = run.resource_type;
    let result_store = &compiled.result_store;

    // A terminal outcome dbt reports on the node rather than an execution
    // failure to retry — see `domain_failure_message`. Set by the unit-test and
    // data-test verdicts below; when present it becomes the node's message and
    // its status is `Error`.
    let mut domain_failure: Option<String> = None;
    // A node that completed and found something the operator should see — a
    // test over `warn_if` but under `error_if`. Reported apart from success:
    // folding the two together made a run that found something look like one
    // that found nothing.
    let mut warned = false;

    // Unit tests: compare the actual vs expected partitions of the executed
    // union query. A difference is the test's answer, not a fault to retry —
    // fixtures and model SQL do not change between attempts.
    let unit_outcome = if rt == NodeType::UnitTest {
        let outcome = unit_test::extract_unit_test_outcome(result_store)?;
        if !outcome.passed {
            domain_failure = Some(domain_failure_message(&DbtTemporalError::UnitTestFailure {
                unique_id: unique_id.to_owned(),
                failures: outcome.failures,
                diff: outcome.diff.clone(),
            }));
        }
        Some(outcome)
    } else {
        None
    };

    // For test nodes, read the result table the test materialization stored
    // (not rows_affected, which is always 1 — one row returned, not the count).
    // It carries the failure count and the two verdicts the warehouse computed
    // from `warn_if` / `error_if`.
    let test_outcome = if rt == NodeType::Test {
        Some(extract_test_outcome(result_store)?)
    } else {
        None
    };
    let failures = test_outcome.map(|o| o.failures);

    // The test's verdict, on dbt's terms: it fails only when its severity is
    // `error` *and* the `error_if` expression the warehouse evaluated came back
    // true, and otherwise `warn_if` decides whether it warns. The failure count
    // decides nothing by itself — a test configured `error_if: ">100"` is
    // passing at 100 failures, and only the SQL knows that.
    //
    // Mirrors upstream `reported_test_verdict_from_components`.
    if let Some(outcome) = test_outcome {
        use dbt_schemas::schemas::common::Severity;
        let severity = run
            .state
            .resolver_state
            .nodes
            .tests
            .get(unique_id)
            .and_then(|t| t.deprecated_config.severity.as_ref())
            .cloned()
            .unwrap_or_default();
        if matches!(severity, Severity::Error) && outcome.should_error {
            domain_failure = Some(domain_failure_message(&DbtTemporalError::TestFailure {
                unique_id: unique_id.to_owned(),
                failures: outcome.failures,
            }));
        }
        if outcome.should_warn {
            warn!(
                node = %unique_id,
                failures = outcome.failures,
                severity = ?severity,
                "test warning"
            );
            warned = true;
        }
    }

    // Build a human-readable message from the adapter response for the Temporal UI.
    // Falls back to materialization type when the adapter doesn't return metadata
    // (e.g. ephemeral models that never execute against the warehouse).
    let message = domain_failure.clone().or_else(|| {
        unit_outcome.map_or_else(
            || build_success_message(&adapter_response, &materialized.materialization),
            |o| Some(format!("unit test passed ({} row(s) compared)", o.actual_rows)),
        )
    });

    let status = match (domain_failure.is_some(), warned) {
        (true, _) => NodeStatus::Error,
        (false, true) => NodeStatus::Warn,
        (false, false) => NodeStatus::Success,
    };

    if let Some(reason) = domain_failure.as_deref() {
        warn!(node = %unique_id, time_secs = materialized.execution_time, reason, "node failed");
    } else {
        info!(
            node = %unique_id,
            time_secs = materialized.execution_time,
            message = message.as_deref().unwrap_or("-"),
            "node execution complete"
        );
    }

    Ok(Verdict {
        status,
        message,
        failures,
        adapter_response,
    })
}

/// The result of a node that was materialized.
pub(super) fn node_result(
    run: &NodeRun<'_>,
    compiled: &CompiledNode,
    materialized: Materialized,
    verdict: Verdict,
) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: run.unique_id.to_owned(),
        status: verdict.status,
        execution_time: materialized.execution_time,
        message: verdict.message,
        adapter_response: verdict.adapter_response,
        compiled_code: materialized.compiled_code,
        timing: materialized.timing,
        failures: verdict.failures,
        freshness: None,
        compiled_code_ref: None,
        relation_name: executed_relation_name(&compiled.node_context),
    }
}

/// Fail a node whose dbt-provided materialization finished without storing a
/// result.
///
/// dbt's own templates for a writing node always run `statement('main')`, so an
/// empty adapter response from one means that statement never ran. The same
/// node renders the same template on every attempt, so the failure is
/// non-retryable.
///
/// A materialization the project defines itself is not held to this: dbt
/// accepts one that stores no result (a no-op, a hook-only or an externally
/// managed relation) and reports the node as successful with an empty
/// response. `template` is the resolved `<package>.<macro>`; a template of the
/// root project is the project's own whatever it is called, and a name outside
/// dbt's built-in set is a custom materialization. Ephemeral models never
/// execute against the warehouse, being inlined as CTEs in their consumer.
fn require_adapter_response(
    unique_id: &str,
    rt: NodeType,
    materialization: &str,
    template: &str,
    project_name: &str,
    adapter_response: &BTreeMap<String, serde_json::Value>,
) -> Result<(), DbtTemporalError> {
    let dbt_provided = template
        .split_once('.')
        .is_none_or(|(package, _)| package != project_name);
    if dbt_provided && expects_adapter_response(rt, materialization) && adapter_response.is_empty()
    {
        return Err(DbtTemporalError::MissingAdapterResponse(format!(
            "node {unique_id} finished without adapter response (resource_type={}, materialization={materialization}); no query appears to have run",
            rt.as_str_name()
        )));
    }
    Ok(())
}

/// Whether dbt's built-in materialization for this resource type and name
/// stores a `main` result.
const fn expects_adapter_response(rt: NodeType, materialization: &str) -> bool {
    match rt {
        NodeType::Model => matches!(
            materialization.as_bytes(),
            b"view" | b"table" | b"incremental" | b"materialized_view" | b"clone"
        ),
        NodeType::Seed
        | NodeType::Snapshot
        | NodeType::Test
        | NodeType::UnitTest
        // A function issues CREATE OR REPLACE FUNCTION through statement('main').
        | NodeType::Function => true,
        _ => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- expects_adapter_response ---

    #[test]
    fn expects_adapter_response_for_writer_resource_types() {
        assert!(expects_adapter_response(NodeType::Model, "table"));
        assert!(expects_adapter_response(NodeType::Model, "view"));
        assert!(expects_adapter_response(NodeType::Seed, "seed"));
        assert!(expects_adapter_response(NodeType::Snapshot, "snapshot"));
        assert!(expects_adapter_response(NodeType::Test, "test"));
    }

    #[test]
    fn expects_adapter_response_skips_custom_model_materializations() {
        assert!(!expects_adapter_response(NodeType::Model, "my_noop"));
    }

    fn require(
        template: &str,
        materialization: &str,
        response: &BTreeMap<String, serde_json::Value>,
    ) -> Result<(), DbtTemporalError> {
        require_adapter_response(
            "model.proj.m",
            NodeType::Model,
            materialization,
            template,
            "proj",
            response,
        )
    }

    #[test]
    fn a_dbt_provided_materialization_without_a_response_fails_permanently() {
        let err = require("dbt_duckdb.materialization_table_duckdb", "table", &BTreeMap::new())
            .expect_err("a built-in template that stored nothing is a fault");
        assert!(!err.is_retryable());
        assert_eq!(err.type_name(), "DbtTemporalError::MissingAdapterResponse");
        assert!(err.to_string().contains("model.proj.m"), "names the node: {err}");
    }

    #[test]
    fn a_dbt_provided_materialization_with_a_response_passes() {
        let response = BTreeMap::from([("code".to_string(), serde_json::json!("SUCCESS"))]);
        assert!(require("dbt_duckdb.materialization_table_duckdb", "table", &response).is_ok());
    }

    #[test]
    fn a_custom_materialization_may_store_no_response() {
        assert!(
            require("some_pkg.materialization_my_noop_default", "my_noop", &BTreeMap::new())
                .is_ok()
        );
    }

    #[test]
    fn a_root_project_materialization_may_store_no_response() {
        // The project's own override of a built-in name is still the project's.
        assert!(require("proj.materialization_table_default", "table", &BTreeMap::new()).is_ok());
    }

    #[test]
    fn expects_adapter_response_skips_ephemeral_models() {
        // Ephemeral models compile to CTEs in their downstream consumer — they
        // never execute against the warehouse, so an empty adapter response is
        // expected and not a bug.
        assert!(!expects_adapter_response(NodeType::Model, "ephemeral"));
    }

    #[test]
    fn expects_adapter_response_false_for_other_node_types() {
        // Hooks, operations, sources etc. don't run as standalone activities;
        // even if they did, they don't need adapter response inspection.
        assert!(!expects_adapter_response(NodeType::Operation, "view"));
        assert!(!expects_adapter_response(NodeType::Source, "view"));
    }
}
