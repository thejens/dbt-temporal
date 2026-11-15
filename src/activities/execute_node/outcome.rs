//! How a finished node is reported: the result message, status, timings and
//! compiled SQL that travel back to the workflow.

use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;
use tracing::{info, warn};

use crate::error::DbtTemporalError;
use crate::types::{NodeExecutionResult, NodeStatus, TimingEntry};

use super::super::node_helpers::extract_adapter_response;
use super::super::render_env::RenderEnv;
use super::context::{CompiledNode, NodeRun, Timeline};
use super::freshness;

/// Longest domain-failure message carried on a node result.
///
/// A unit test's message holds its row diff, which has no natural bound. The
/// result travels through Temporal, the run log and `run_results.json`, so the
/// message is capped where the diff stops being readable anyway.
const DOMAIN_FAILURE_MESSAGE_MAX: usize = 4096;

/// Render a terminal domain outcome as the node's result message.
///
/// A data test that found rows, a unit test whose output differed, a source
/// past its `error_after`: dbt reports each of these *on the node*, and none of
/// them gets better on a retry. They travel back as a populated
/// `NodeExecutionResult` with `NodeStatus::Error` rather than as an activity
/// failure — failing the activity would discard the timings, compiled SQL,
/// adapter metadata, failure count and freshness measurement already collected,
/// and the workflow would rebuild a bare error result in their place, so a
/// failing test would be the node whose results said the least.
///
/// The `DbtTemporalError` variants are the vocabulary for these outcomes, so
/// the wording an operator sees is the same as for any other failure.
pub(super) fn domain_failure_message(error: &DbtTemporalError) -> String {
    let full = error.to_string();
    if full.len() <= DOMAIN_FAILURE_MESSAGE_MAX {
        return full;
    }
    format!(
        "{}… (truncated)",
        crate::error::truncate_at_char_boundary(&full, DOMAIN_FAILURE_MESSAGE_MAX)
    )
}

/// The result of a `dbt compile`: the rendered SQL, and nothing the warehouse
/// did — because nothing was sent to it.
pub(super) fn compile_only_result(
    unique_id: &str,
    node_context: &BTreeMap<String, minijinja::Value>,
    execution_time: f64,
    compile_window: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
) -> NodeExecutionResult {
    let (compile_start, compile_end) = compile_window;
    NodeExecutionResult {
        unique_id: unique_id.to_owned(),
        status: NodeStatus::Success,
        execution_time,
        message: Some("compiled".to_string()),
        adapter_response: BTreeMap::new(),
        compiled_code: node_context
            .get("sql")
            .and_then(|v| v.as_str().map(ToString::to_string)),
        timing: vec![TimingEntry {
            name: "compile".to_string(),
            started_at: compile_start.to_rfc3339(),
            completed_at: compile_end.to_rfc3339(),
        }],
        failures: None,
        freshness: None,
        // Compile-only: nothing was written, so there is nothing to reference.
        compiled_code_ref: None,
        relation_name: None,
    }
}

/// Turn a freshness verdict into the status and message a node result reports.
///
/// Freshness grades three ways where the rest of the run grades two, and the
/// distinction is the whole point of the check: `warn_after` exceeded is not a
/// failure, and reporting it as success hid it from anyone reading results
/// rather than logs.
pub(super) fn grade_freshness(
    unique_id: &str,
    rt: NodeType,
    verdict: freshness::FreshnessVerdict,
) -> (NodeStatus, String, crate::types::FreshnessOutcome) {
    match verdict {
        freshness::FreshnessVerdict::Stale {
            outcome,
            max_allowed_secs,
        } => {
            let message = domain_failure_message(&DbtTemporalError::StaleSource {
                unique_id: unique_id.to_owned(),
                node_kind: rt.as_static_ref(),
                max_loaded_at: outcome.max_loaded_at.clone(),
                age_secs: outcome.max_loaded_at_time_ago_in_s,
                max_allowed_secs,
            });
            warn!(node = %unique_id, message = %message, "freshness error (error_after exceeded)");
            (NodeStatus::Error, message, outcome)
        }
        freshness::FreshnessVerdict::Warning(outcome) => {
            warn!(
                node = %unique_id,
                age_secs = outcome.max_loaded_at_time_ago_in_s,
                "freshness warning (warn_after exceeded)"
            );
            let message = freshness_summary(&outcome);
            (NodeStatus::Warn, message, outcome)
        }
        freshness::FreshnessVerdict::Fresh(outcome) => {
            let message = freshness_summary(&outcome);
            (NodeStatus::Success, message, outcome)
        }
    }
}

/// The one-line summary a passing or warning freshness check reports.
fn freshness_summary(outcome: &crate::types::FreshnessOutcome) -> String {
    format!(
        "freshness {} (age {:.0}s, max_loaded_at {})",
        outcome.status.as_str().to_uppercase(),
        outcome.max_loaded_at_time_ago_in_s,
        outcome.max_loaded_at
    )
}

/// The relation the node wrote, as `database.schema.identifier`.
///
/// Read from the patched `this` rather than from startup metadata: a workflow
/// that overrides the target or the profile's env writes somewhere else
/// entirely, and the catalog and `run_results.json` have no other way to learn
/// where.
pub(super) fn executed_relation_name(
    node_context: &BTreeMap<String, minijinja::Value>,
) -> Option<String> {
    let this = node_context.get("this")?;
    let part =
        |name: &str| -> Option<String> { Some(this.get_attr(name).ok()?.as_str()?.to_string()) };
    Some(format!("{}.{}.{}", part("database")?, part("schema")?, part("identifier")?))
}

/// Pick the final `compiled_code` for the result. Prefer the SQL the context
/// captured during render (the canonical compile output); fall back to the
/// stripped rendered output only when the context didn't get a `sql` set.
pub(super) fn finalize_compiled_code(
    compiled_sql: Option<String>,
    rendered: &str,
) -> Option<String> {
    compiled_sql.or_else(|| {
        let trimmed = rendered.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Build the compile + execute `TimingEntry` pair returned to the workflow.
pub(super) fn build_timing_entries(
    compile_start: chrono::DateTime<chrono::Utc>,
    compile_end: chrono::DateTime<chrono::Utc>,
    execute_start: chrono::DateTime<chrono::Utc>,
    execute_end: chrono::DateTime<chrono::Utc>,
) -> Vec<TimingEntry> {
    vec![
        TimingEntry {
            name: "compile".to_string(),
            started_at: compile_start.to_rfc3339(),
            completed_at: compile_end.to_rfc3339(),
        },
        TimingEntry {
            name: "execute".to_string(),
            started_at: execute_start.to_rfc3339(),
            completed_at: execute_end.to_rfc3339(),
        },
    ]
}

/// Build a human-readable success message from the adapter response.
/// E.g. "CREATE TABLE (42 rows)", "CREATE VIEW", "ephemeral".
pub(super) fn build_success_message(
    adapter_response: &BTreeMap<String, serde_json::Value>,
    materialization: &str,
) -> Option<String> {
    // Try adapter response first (has DDL/DML info + rows_affected).
    if let Some(msg) = adapter_response
        .get("message")
        .and_then(serde_json::Value::as_str)
    {
        let rows = adapter_response
            .get("rows_affected")
            .and_then(serde_json::Value::as_i64);
        return Some(rows.map_or_else(|| msg.to_string(), |n| format!("{msg} ({n} rows)")));
    }

    // Fallback: use the materialization type (e.g. "ephemeral", "view").
    if !materialization.is_empty() {
        return Some(materialization.to_string());
    }

    None
}

/// Run the freshness check instead of a materialization.
///
/// A stale node (error_after exceeded) reports `NodeStatus::Error` with its
/// measurement attached, rather than failing the activity — see
/// `domain_failure_message`. A warning (warn_after exceeded) does not fail
/// the activity either: it reports `NodeStatus::Warn`, with the measurement
/// on the outcome and the age in the node message.
pub(super) fn measure_freshness(
    run: &NodeRun<'_>,
    render_env: &RenderEnv,
    compiled: &CompiledNode,
    timeline: &Timeline,
    execute_start: chrono::DateTime<chrono::Utc>,
) -> Result<NodeExecutionResult, anyhow::Error> {
    let unique_id = run.unique_id;
    let rt = run.resource_type;
    let freshness_node =
        freshness::as_freshness_node(&run.state.resolver_state.nodes, unique_id, rt).ok_or_else(
            || DbtTemporalError::ProjectNotFound(format!("freshness node not found: {unique_id}")),
        )?;
    let verdict = freshness::run_freshness_check(
        freshness_node,
        &render_env.jinja_env,
        &compiled.node_context,
    )?;
    let execute_end = chrono::Utc::now();
    let execution_time = timeline.elapsed_secs();
    let (status, message, outcome) = grade_freshness(unique_id, rt, verdict);
    info!(node = %unique_id, message = %message, "freshness check complete");
    Ok(NodeExecutionResult {
        unique_id: unique_id.to_owned(),
        status,
        execution_time,
        message: Some(message),
        adapter_response: extract_adapter_response(&compiled.result_store),
        compiled_code: None,
        timing: build_timing_entries(
            timeline.compile_start,
            timeline.compile_end,
            execute_start,
            execute_end,
        ),
        failures: None,
        freshness: Some(outcome),
        compiled_code_ref: None,
        relation_name: executed_relation_name(&compiled.node_context),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- build_success_message ---

    #[test]
    fn build_success_message_prefers_adapter_message_with_row_count() {
        let mut response = BTreeMap::new();
        response.insert("message".to_string(), serde_json::json!("CREATE TABLE"));
        response.insert("rows_affected".to_string(), serde_json::json!(42));
        let msg = build_success_message(&response, "table");
        assert_eq!(msg.as_deref(), Some("CREATE TABLE (42 rows)"));
    }

    #[test]
    fn build_success_message_uses_message_only_when_rows_unavailable() {
        let mut response = BTreeMap::new();
        response.insert("message".to_string(), serde_json::json!("CREATE VIEW"));
        let msg = build_success_message(&response, "view");
        assert_eq!(msg.as_deref(), Some("CREATE VIEW"));
    }

    #[test]
    fn build_success_message_falls_back_to_materialization_when_no_response() {
        let response = BTreeMap::new();
        let msg = build_success_message(&response, "ephemeral");
        assert_eq!(msg.as_deref(), Some("ephemeral"));
    }

    #[test]
    fn build_success_message_returns_none_when_neither_available() {
        let response = BTreeMap::new();
        let msg = build_success_message(&response, "");
        assert!(msg.is_none());
    }

    #[test]
    fn build_success_message_treats_non_string_message_as_absent() {
        // A numeric "message" field doesn't satisfy as_str() — the builder
        // must fall through to the materialization fallback rather than
        // producing a malformed message.
        let mut response = BTreeMap::new();
        response.insert("message".to_string(), serde_json::json!(42));
        let msg = build_success_message(&response, "table");
        assert_eq!(msg.as_deref(), Some("table"));
    }

    // --- finalize_compiled_code ---

    #[test]
    fn finalize_compiled_code_prefers_context_sql() {
        // When the context captured an explicit `sql`, the rendered output
        // (often just an empty string from materialization templates) is
        // ignored.
        let code = finalize_compiled_code(Some("SELECT 1".to_string()), "");
        assert_eq!(code.as_deref(), Some("SELECT 1"));
        let code = finalize_compiled_code(Some("SELECT 1".to_string()), "ignored");
        assert_eq!(code.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn finalize_compiled_code_falls_back_to_rendered_when_context_absent() {
        let code = finalize_compiled_code(None, "  CREATE VIEW foo AS SELECT 1  ");
        assert_eq!(code.as_deref(), Some("CREATE VIEW foo AS SELECT 1"));
    }

    #[test]
    fn finalize_compiled_code_returns_none_when_both_empty() {
        assert!(finalize_compiled_code(None, "").is_none());
        assert!(finalize_compiled_code(None, "   \n  ").is_none());
    }

    // --- build_timing_entries ---

    #[test]
    fn build_timing_entries_emits_compile_then_execute() {
        let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t1 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_001, 0).unwrap();
        let t2 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_002, 0).unwrap();
        let t3 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_003, 0).unwrap();
        let entries = build_timing_entries(t0, t1, t2, t3);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "compile");
        assert_eq!(entries[0].started_at, t0.to_rfc3339());
        assert_eq!(entries[0].completed_at, t1.to_rfc3339());
        assert_eq!(entries[1].name, "execute");
        assert_eq!(entries[1].started_at, t2.to_rfc3339());
        assert_eq!(entries[1].completed_at, t3.to_rfc3339());
    }
}
