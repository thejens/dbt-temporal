use std::collections::BTreeMap;

use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;

use crate::types::{ExecutionPlan, NodeExecutionResult, NodeStatus, NodeStatusTree, RetryConfig};

use super::DbtRunWorkflow;

/// Build the initial node status tree from the execution plan, with all nodes set to Pending.
pub fn build_node_status_tree(plan: &ExecutionPlan) -> NodeStatusTree {
    let mut nodes = BTreeMap::new();
    for level in &plan.levels {
        for unique_id in level {
            if plan.nodes.contains_key(unique_id) {
                nodes.insert(unique_id.clone(), NodeStatus::Pending);
            }
        }
    }
    NodeStatusTree { nodes }
}

pub fn set_node_status(tree: &mut NodeStatusTree, unique_id: &str, status: NodeStatus) {
    if let Some(entry) = tree.nodes.get_mut(unique_id) {
        *entry = status;
    }
}

pub fn upsert_node_status(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    tree: &NodeStatusTree,
) -> Result<(), temporalio_sdk::WorkflowTermination> {
    use temporalio_common::protos::coresdk::AsJsonPayloadExt;
    ctx.upsert_memo([(
        "node_status".to_string(),
        tree.as_json_payload()
            .map_err(temporalio_sdk::WorkflowTermination::failed)?,
    )]);
    Ok(())
}

/// Maximum number of log lines to store in the memo.
/// Keeps the tail so the most recent progress is always visible.
/// Full logs are persisted in the artifact store at workflow completion.
const MEMO_LOG_MAX_LINES: usize = 200;

/// Maximum number of node status entries to store in the memo.
/// Beyond this threshold we store only non-terminal (pending/running) nodes
/// plus a summary of completed/error/skipped counts.
const MEMO_NODE_STATUS_MAX: usize = 2000;

/// Persist the run log and node status together in a single memo upsert.
///
/// Both payloads are capped to stay within Temporal memo size limits.
/// The full run log is stored as an artifact at workflow completion.
pub fn upsert_memo_state(
    ctx: &temporalio_sdk::WorkflowContext<DbtRunWorkflow>,
    tree: &NodeStatusTree,
    log_lines: &[String],
) -> Result<(), temporalio_sdk::WorkflowTermination> {
    use temporalio_common::protos::coresdk::AsJsonPayloadExt;

    let memo_log = truncate_log_for_memo(log_lines);
    let memo_tree = truncate_node_status_for_memo(tree);

    ctx.upsert_memo([
        (
            "node_status".to_string(),
            memo_tree
                .as_json_payload()
                .map_err(temporalio_sdk::WorkflowTermination::failed)?,
        ),
        (
            "log".to_string(),
            memo_log
                .as_json_payload()
                .map_err(temporalio_sdk::WorkflowTermination::failed)?,
        ),
    ]);
    Ok(())
}

/// Keep only the last `MEMO_LOG_MAX_LINES` lines, prepending a truncation notice.
fn truncate_log_for_memo(log_lines: &[String]) -> Vec<&str> {
    if log_lines.len() <= MEMO_LOG_MAX_LINES {
        return log_lines.iter().map(String::as_str).collect();
    }
    let skip = log_lines.len() - (MEMO_LOG_MAX_LINES - 1);
    let mut out = Vec::with_capacity(MEMO_LOG_MAX_LINES);
    out.push("... (log truncated, full log available in artifacts)");
    out.extend(log_lines[skip..].iter().map(String::as_str));
    out
}

/// When the node status tree exceeds the threshold, keep only non-terminal
/// nodes so operators can still see what's running/pending.
fn truncate_node_status_for_memo(tree: &NodeStatusTree) -> NodeStatusTree {
    if tree.nodes.len() <= MEMO_NODE_STATUS_MAX {
        return tree.clone();
    }
    let mut nodes = BTreeMap::new();
    for (id, status) in &tree.nodes {
        if matches!(status, NodeStatus::Pending | NodeStatus::Running) {
            nodes.insert(id.clone(), *status);
        }
    }
    nodes.insert(
        "__truncated".to_string(),
        NodeStatus::Skipped, // sentinel — signals the tree was truncated
    );
    NodeStatusTree { nodes }
}

pub fn skipped_result(unique_id: &str, message: &str) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: unique_id.to_string(),
        status: "skipped".to_string(),
        execution_time: 0.0,
        message: Some(message.to_string()),
        adapter_response: BTreeMap::default(),
        compiled_code: None,
        timing: vec![],
        failures: None,
    }
}

pub fn cancelled_result(unique_id: &str) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: unique_id.to_string(),
        status: "cancelled".to_string(),
        execution_time: 0.0,
        message: Some("cancelled".to_string()),
        adapter_response: BTreeMap::default(),
        compiled_code: None,
        timing: vec![],
        failures: None,
    }
}

pub fn error_result(unique_id: &str, message: String) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: unique_id.to_string(),
        status: "error".to_string(),
        execution_time: 0.0,
        message: Some(message),
        adapter_response: BTreeMap::default(),
        compiled_code: None,
        timing: vec![],
        failures: None,
    }
}

/// Build a human-readable label for a node, e.g. "table model waffle_hut.stg_customers".
pub fn node_label(plan: &ExecutionPlan, unique_id: &str) -> String {
    plan.nodes.get(unique_id).map_or_else(
        || unique_id.to_string(),
        |info| {
            let rt = info
                .resource_type
                .strip_prefix("NODE_TYPE_")
                .unwrap_or(&info.resource_type)
                .to_ascii_lowercase();
            let mat = info
                .materialization
                .as_deref()
                .map_or_else(String::new, |m| format!("{m} "));
            format!("{mat}{rt} {}.{}", info.package_name, info.name)
        },
    )
}

/// Extract a concise error message from a Temporal activity execution error.
pub fn short_activity_error(e: &temporalio_sdk::ActivityExecutionError) -> String {
    let full = e.to_string();

    // Truncate if very long.
    if full.len() > 200 {
        format!("{}...", &full[..200])
    } else {
        full
    }
}

/// Build a Temporal RetryPolicy from our RetryConfig.
pub fn build_retry_policy(config: &RetryConfig) -> RetryPolicy {
    RetryPolicy {
        initial_interval: Some(prost_types::Duration {
            seconds: i64::try_from(config.initial_interval_secs).unwrap_or(i64::MAX),
            nanos: 0,
        }),
        backoff_coefficient: config.backoff_coefficient,
        maximum_interval: Some(prost_types::Duration {
            seconds: i64::try_from(config.max_interval_secs).unwrap_or(i64::MAX),
            nanos: 0,
        }),
        maximum_attempts: i32::try_from(config.max_attempts).unwrap_or(i32::MAX),
        non_retryable_error_types: vec![
            "DbtTemporalError::Compilation".to_string(),
            "DbtTemporalError::Configuration".to_string(),
            "DbtTemporalError::ProjectNotFound".to_string(),
        ],
    }
}

#[cfg(test)]
// float_cmp: tests compare directly-assigned floats, not computed values.
// unwrap_used: unwrap is acceptable in tests for brevity.
#[allow(clippy::float_cmp, clippy::unwrap_used, clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use crate::types::NodeInfo;

    fn test_plan(
        levels: Vec<Vec<&str>>,
        nodes: &[(&str, Option<&str>, Vec<&str>)],
    ) -> ExecutionPlan {
        let mut node_map = BTreeMap::new();
        for (name, mat, deps) in nodes {
            let uid = format!("model.waffle.{name}");
            node_map.insert(
                uid.clone(),
                NodeInfo {
                    unique_id: uid,
                    name: name.to_string(),
                    resource_type: "model".to_string(),
                    materialization: mat.map(ToString::to_string),
                    package_name: "waffle".to_string(),
                    depends_on: deps.iter().map(|d| format!("model.waffle.{d}")).collect(),
                },
            );
        }
        ExecutionPlan {
            project: "waffle".to_string(),
            levels: levels
                .into_iter()
                .map(|l| l.into_iter().map(|s| format!("model.waffle.{s}")).collect())
                .collect(),
            nodes: node_map,
            manifest_json: None,
            manifest_ref: None,
            invocation_id: "test-inv".to_string(),
            search_attributes: BTreeMap::new(),
            write_artifacts: false,
        }
    }

    #[test]
    fn build_node_status_tree_all_pending() {
        let plan = test_plan(
            vec![vec!["a", "b"], vec!["c"]],
            &[
                ("a", Some("table"), vec![]),
                ("b", Some("view"), vec![]),
                ("c", Some("table"), vec!["a", "b"]),
            ],
        );

        let tree = build_node_status_tree(&plan);
        assert_eq!(tree.nodes.len(), 3);
        for status in tree.nodes.values() {
            assert_eq!(*status, NodeStatus::Pending);
        }
    }

    #[test]
    fn set_node_status_updates_existing() {
        let plan = test_plan(vec![vec!["a"]], &[("a", None, vec![])]);
        let mut tree = build_node_status_tree(&plan);

        set_node_status(&mut tree, "model.waffle.a", NodeStatus::Running);
        assert_eq!(tree.nodes["model.waffle.a"], NodeStatus::Running);

        set_node_status(&mut tree, "model.waffle.a", NodeStatus::Success);
        assert_eq!(tree.nodes["model.waffle.a"], NodeStatus::Success);
    }

    #[test]
    fn set_node_status_noop_for_unknown() {
        let plan = test_plan(vec![vec!["a"]], &[("a", None, vec![])]);
        let mut tree = build_node_status_tree(&plan);
        set_node_status(&mut tree, "model.waffle.nonexistent", NodeStatus::Error);
        // No panic, tree unchanged.
        assert_eq!(tree.nodes.len(), 1);
    }

    #[test]
    fn node_label_with_materialization() {
        let plan =
            test_plan(vec![vec!["stg_customers"]], &[("stg_customers", Some("table"), vec![])]);
        let label = node_label(&plan, "model.waffle.stg_customers");
        assert_eq!(label, "table model waffle.stg_customers");
    }

    #[test]
    fn node_label_without_materialization() {
        let plan = test_plan(vec![vec!["stg_customers"]], &[("stg_customers", None, vec![])]);
        let label = node_label(&plan, "model.waffle.stg_customers");
        assert_eq!(label, "model waffle.stg_customers");
    }

    #[test]
    fn node_label_unknown_falls_back_to_id() {
        let plan = test_plan(vec![], &[]);
        let label = node_label(&plan, "model.waffle.unknown");
        assert_eq!(label, "model.waffle.unknown");
    }

    #[test]
    fn skipped_result_fields() {
        let r = skipped_result("model.waffle.a", "upstream failure");
        assert_eq!(r.unique_id, "model.waffle.a");
        assert_eq!(r.status, "skipped");
        assert_eq!(r.execution_time, 0.0);
        assert_eq!(r.message.as_deref(), Some("upstream failure"));
    }

    #[test]
    fn cancelled_result_fields() {
        let r = cancelled_result("model.waffle.b");
        assert_eq!(r.unique_id, "model.waffle.b");
        assert_eq!(r.status, "cancelled");
        assert_eq!(r.message.as_deref(), Some("cancelled"));
    }

    #[test]
    fn error_result_fields() {
        let r = error_result("model.waffle.c", "timeout".to_string());
        assert_eq!(r.unique_id, "model.waffle.c");
        assert_eq!(r.status, "error");
        assert_eq!(r.message.as_deref(), Some("timeout"));
    }

    #[test]
    fn build_retry_policy_from_defaults() {
        let config = RetryConfig::default();
        let policy = build_retry_policy(&config);
        let initial = policy.initial_interval.unwrap();
        assert_eq!(initial.seconds, config.initial_interval_secs as i64);
        assert_eq!(policy.backoff_coefficient, config.backoff_coefficient);
        let max_interval = policy.maximum_interval.unwrap();
        assert_eq!(max_interval.seconds, config.max_interval_secs as i64);
        assert_eq!(policy.maximum_attempts, config.max_attempts as i32);
        assert_eq!(
            policy.non_retryable_error_types,
            vec![
                "DbtTemporalError::Compilation",
                "DbtTemporalError::Configuration",
                "DbtTemporalError::ProjectNotFound",
            ]
        );
    }

    #[test]
    fn build_retry_policy_custom_values() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_interval_secs: 5,
            max_interval_secs: 120,
            backoff_coefficient: 3.0,
            non_retryable_errors: vec!["permission denied".into()],
        };
        let policy = build_retry_policy(&config);
        assert_eq!(policy.maximum_attempts, 10);
        assert_eq!(policy.initial_interval.unwrap().seconds, 5);
        assert_eq!(policy.maximum_interval.unwrap().seconds, 120);
        assert_eq!(policy.backoff_coefficient, 3.0);
    }

    #[test]
    fn truncate_log_short_passes_through() {
        let lines: Vec<String> = (0..10).map(|i| format!("line {i}")).collect();
        let result = truncate_log_for_memo(&lines);
        assert_eq!(result.len(), 10);
        assert_eq!(result[0], "line 0");
    }

    #[test]
    fn truncate_log_over_limit_keeps_tail() {
        let lines: Vec<String> = (0..300).map(|i| format!("line {i}")).collect();
        let result = truncate_log_for_memo(&lines);
        assert_eq!(result.len(), MEMO_LOG_MAX_LINES);
        assert!(result[0].contains("truncated"));
        assert_eq!(result[result.len() - 1], "line 299");
    }

    #[test]
    fn truncate_node_status_small_passes_through() {
        let mut nodes = BTreeMap::new();
        nodes.insert("a".to_string(), NodeStatus::Success);
        nodes.insert("b".to_string(), NodeStatus::Running);
        let tree = NodeStatusTree { nodes };
        let result = truncate_node_status_for_memo(&tree);
        assert_eq!(result.nodes.len(), 2);
    }

    #[test]
    fn truncate_node_status_large_keeps_active_only() {
        let mut nodes = BTreeMap::new();
        for i in 0..2500 {
            let status = if i < 10 {
                NodeStatus::Running
            } else {
                NodeStatus::Success
            };
            nodes.insert(format!("node_{i}"), status);
        }
        let tree = NodeStatusTree { nodes };
        let result = truncate_node_status_for_memo(&tree);
        // 10 running + 1 sentinel
        assert_eq!(result.nodes.len(), 11);
        assert!(result.nodes.contains_key("__truncated"));
    }
}
