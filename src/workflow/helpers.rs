use std::collections::BTreeMap;

use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;

use crate::types::{
    DbtRunInput, ExecutionPlan, NodeExecutionResult, NodeStatus, NodeStatusTree, RetryConfig,
};

use super::DbtRunWorkflow;

/// Build the per-run effective env starting from `input.env` and exposing the
/// workflow input itself as `_` (mirroring bash's `$_`) so models and macros
/// can read the full payload via `env_var('_')`.
///
/// Always overrides any caller-supplied `_` because the only meaningful value
/// here is the actual payload that started this workflow. `serde_json::to_string`
/// is infallible on `DbtRunInput`.
pub fn build_effective_env(input: &DbtRunInput) -> BTreeMap<String, String> {
    let mut env = input.env.clone();
    if let Ok(json) = serde_json::to_string(input) {
        env.insert("_".to_string(), json);
    }
    env
}

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
        status: NodeStatus::Skipped,
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
        status: NodeStatus::Cancelled,
        execution_time: 0.0,
        message: Some("cancelled".to_string()),
        adapter_response: BTreeMap::default(),
        compiled_code: None,
        timing: vec![],
        failures: None,
    }
}

pub fn error_result(unique_id: &str, message: &str) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: unique_id.to_string(),
        status: NodeStatus::Error,
        execution_time: 0.0,
        message: Some(message.to_string()),
        adapter_response: BTreeMap::default(),
        compiled_code: None,
        timing: vec![],
        failures: None,
    }
}

/// English pluralisation suffix: empty for 1, "s" otherwise.
pub const fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// Build the dbt-style run-log header: version line, resource-type tally,
/// and concurrency line. Pure — no ctx side effects.
pub fn build_log_header(plan: &ExecutionPlan) -> Vec<String> {
    let mut log_lines = Vec::with_capacity(3);
    log_lines.push(format!("Running with dbt-temporal={}", env!("CARGO_PKG_VERSION")));

    let mut type_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for info in plan.nodes.values() {
        *type_counts.entry(info.resource_type.as_str()).or_default() += 1;
    }
    let parts: Vec<String> = type_counts
        .iter()
        .map(|(t, n)| format!("{n} {t}{}", plural(*n)))
        .collect();
    log_lines.push(format!("Found {}", parts.join(", ")));

    log_lines.push(format!(
        "Concurrency: {} parallel level{}",
        plan.levels.len(),
        plural(plan.levels.len())
    ));
    log_lines
}

/// Build the dbt-style summary lines that close out a run.
pub fn build_summary_lines(
    total_nodes: usize,
    elapsed_secs: f64,
    pass: usize,
    error: usize,
    skip: usize,
) -> [String; 2] {
    [
        format!(
            "Finished running {total_nodes} node{} in {elapsed_secs:.2}s.",
            plural(total_nodes)
        ),
        format!("Done. PASS={pass} ERROR={error} SKIP={skip} TOTAL={total_nodes}"),
    ]
}

/// Seconds between two deterministic workflow timestamps, or 0.0 if either
/// is missing (workflow_time() is None outside of an executing workflow).
///
/// Using `ctx.workflow_time()` instead of `Instant::now()` keeps the value
/// stable across history replay; the workflow file's own docstring forbids
/// wall-clock time for this reason.
pub fn elapsed_secs(
    start: Option<std::time::SystemTime>,
    end: Option<std::time::SystemTime>,
) -> f64 {
    start
        .zip(end)
        .and_then(|(s, e)| e.duration_since(s).ok())
        .map_or(0.0, |d| d.as_secs_f64())
}

/// On level cancellation, fill in cancelled results for every node in the
/// level that didn't get a chance to run.
///
/// Called from the cancellation branch of the per-future loop after the
/// triggering node has already been recorded. Idempotent: skips ids in
/// `processed_in_level`. Maintains the invariant that every planned node
/// gets exactly one entry in `all_results`.
pub fn mark_remaining_level_as_cancelled(
    level_node_ids: &[String],
    processed_in_level: &std::collections::BTreeSet<String>,
    node_status: &mut NodeStatusTree,
    all_results: &mut Vec<NodeExecutionResult>,
) {
    for id in level_node_ids {
        if !processed_in_level.contains(id) {
            set_node_status(node_status, id, NodeStatus::Cancelled);
            all_results.push(cancelled_result(id));
        }
    }
}

/// Build the per-node Temporal activity label (used for both `activity_id`
/// and `summary` in the UI). Pure — uses only the plan's nodemap.
///
/// Format: `<resource_type>:<name>` with the proto enum prefix stripped, so
/// `NODE_TYPE_MODEL` → `model`. Returns `None` when the id isn't in the plan,
/// matching the behaviour of `start_activity` (no label).
pub fn format_activity_label(plan: &ExecutionPlan, unique_id: &str) -> Option<String> {
    plan.nodes.get(unique_id).map(|info| {
        let rt = info
            .resource_type
            .strip_prefix("NODE_TYPE_")
            .unwrap_or(&info.resource_type)
            .to_ascii_lowercase();
        format!("{rt}:{}", info.name)
    })
}

/// True if `unique_id` has any upstream dependency in `failed_nodes`. Used by
/// the per-level scheduler to skip a node whose upstream model failed.
pub fn is_blocked_by_failed_upstream(
    plan: &ExecutionPlan,
    unique_id: &str,
    failed_nodes: &std::collections::BTreeSet<String>,
) -> bool {
    plan.nodes
        .get(unique_id)
        .is_some_and(|info| info.depends_on.iter().any(|dep| failed_nodes.contains(dep)))
}

/// Classify the result status from an activity completion. The level scheduler
/// treats anything that isn't an explicit `Error` or `Skipped` as a success
/// for log/memo purposes — the activity may have produced a richer status
/// internally but we're only interested in the success/error/skip trichotomy.
pub const fn classify_result_status(status: NodeStatus) -> NodeStatus {
    match status {
        NodeStatus::Error => NodeStatus::Error,
        NodeStatus::Skipped => NodeStatus::Skipped,
        _ => NodeStatus::Success,
    }
}

/// Format the right-hand side `[tag]` of a per-node log line — the bit after
/// `<package>.<name>  `. Pure; the caller wraps it in the brackets.
pub fn format_result_tag(status: NodeStatus, message: Option<&str>, execution_time: f64) -> String {
    match status {
        NodeStatus::Success => {
            let detail = message.unwrap_or("OK");
            format!("{detail} in {execution_time:.2}s")
        }
        NodeStatus::Error => {
            let msg = message.unwrap_or("error");
            format!("ERROR {msg}")
        }
        NodeStatus::Skipped => "SKIP".to_string(),
        // Pending / Running / Cancelled never appear on the result-tag path
        // (results are only built from completed activities), so the Debug
        // form is just a defensive default.
        _ => format!("{status:?}"),
    }
}

/// "1 of 17 START stg_customers" log lines. The verb is the only thing that
/// changes between START and SKIP — extract once so the formatting stays in
/// one place.
pub fn format_progress_line(num: usize, total: usize, verb: &str, label: &str) -> String {
    format!("{num} of {total} {verb} {label}")
}

/// Build the workflow-detail line shown in the Temporal UI while a level is
/// running, e.g. "level 3/8: model:stg_customers, model:stg_orders".
pub fn format_running_details(level_idx: usize, total_levels: usize, running: &[&str]) -> String {
    format!("level {}/{}: {}", level_idx + 1, total_levels, running.join(", "))
}

/// Decide whether to flush the memo at the end of a level.
///
/// Always flushes on a level that just had a failure (operators want failure
/// visibility quickly) or on a periodic boundary. The last level is left to
/// the orchestrator's terminal flush so we don't double-write.
pub const fn should_flush_memo(
    level_idx: usize,
    total_levels: usize,
    level_had_failure: bool,
    periodic_every_n: usize,
) -> bool {
    let is_last = level_idx == total_levels.saturating_sub(1);
    if is_last {
        return false;
    }
    if level_had_failure {
        return true;
    }
    (level_idx + 1).is_multiple_of(periodic_every_n)
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
            has_on_run_start: false,
            has_on_run_end: false,
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
        assert_eq!(r.status, NodeStatus::Skipped);
        assert_eq!(r.execution_time, 0.0);
        assert_eq!(r.message.as_deref(), Some("upstream failure"));
    }

    #[test]
    fn cancelled_result_fields() {
        let r = cancelled_result("model.waffle.b");
        assert_eq!(r.unique_id, "model.waffle.b");
        assert_eq!(r.status, NodeStatus::Cancelled);
        assert_eq!(r.message.as_deref(), Some("cancelled"));
    }

    #[test]
    fn error_result_fields() {
        let r = error_result("model.waffle.c", "timeout");
        assert_eq!(r.unique_id, "model.waffle.c");
        assert_eq!(r.status, NodeStatus::Error);
        assert_eq!(r.message.as_deref(), Some("timeout"));
    }

    #[test]
    fn mark_remaining_fills_unprocessed_only_once() {
        let level: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let mut processed = std::collections::BTreeSet::new();
        processed.insert("a".to_string());
        processed.insert("c".to_string());

        let mut tree = NodeStatusTree {
            nodes: level
                .iter()
                .map(|id| (id.clone(), NodeStatus::Pending))
                .collect(),
        };
        let mut results: Vec<NodeExecutionResult> =
            vec![error_result("a", "boom"), cancelled_result("c")];

        mark_remaining_level_as_cancelled(&level, &processed, &mut tree, &mut results);

        // Invariant: every level id has exactly one result entry.
        assert_eq!(results.len(), level.len());
        let mut ids: Vec<&str> = results.iter().map(|r| r.unique_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["a", "b", "c", "d"]);

        // Already-processed nodes keep their original status; unprocessed
        // nodes are now cancelled.
        assert_eq!(tree.nodes["a"], NodeStatus::Pending); // not changed by helper
        assert_eq!(tree.nodes["b"], NodeStatus::Cancelled);
        assert_eq!(tree.nodes["c"], NodeStatus::Pending);
        assert_eq!(tree.nodes["d"], NodeStatus::Cancelled);
    }

    #[test]
    fn mark_remaining_is_idempotent_when_all_processed() {
        let level: Vec<String> = ["a", "b"].iter().map(|s| (*s).to_string()).collect();
        let processed: std::collections::BTreeSet<String> = level.iter().cloned().collect();

        let mut tree = NodeStatusTree {
            nodes: level
                .iter()
                .map(|id| (id.clone(), NodeStatus::Success))
                .collect(),
        };
        let mut results: Vec<NodeExecutionResult> = vec![];

        mark_remaining_level_as_cancelled(&level, &processed, &mut tree, &mut results);

        assert!(results.is_empty());
        assert_eq!(tree.nodes["a"], NodeStatus::Success);
        assert_eq!(tree.nodes["b"], NodeStatus::Success);
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
    fn effective_env_carries_input_env_through() {
        let input = DbtRunInput {
            project: None,
            command: "build".into(),
            select: None,
            exclude: None,
            vars: BTreeMap::new(),
            target: None,
            full_refresh: false,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::from([("API_TOKEN".into(), "secret-123".into())]),
        };
        let env = build_effective_env(&input);
        assert_eq!(env.get("API_TOKEN").map(String::as_str), Some("secret-123"));
    }

    #[test]
    fn effective_env_injects_underscore_with_serialised_input() -> anyhow::Result<()> {
        // Regression: the workflow exposes the full payload via env_var('_'),
        // mirroring bash's `$_`. Macros parse this JSON to read the original
        // command, so the field must round-trip the input verbatim.
        let input = DbtRunInput {
            project: Some("waffle".into()),
            command: "compile".into(),
            select: Some("+stg_customers".into()),
            exclude: None,
            vars: BTreeMap::from([("env_label".into(), serde_json::json!("dev"))]),
            target: Some("dev".into()),
            full_refresh: true,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::new(),
        };
        let env = build_effective_env(&input);
        let underscore = env
            .get("_")
            .ok_or_else(|| anyhow::anyhow!("`_` must be set"))?;
        let parsed: DbtRunInput = serde_json::from_str(underscore)?;
        assert_eq!(parsed.command, "compile");
        assert_eq!(parsed.project.as_deref(), Some("waffle"));
        assert_eq!(parsed.select.as_deref(), Some("+stg_customers"));
        assert!(parsed.full_refresh);
        Ok(())
    }

    #[test]
    fn effective_env_overrides_caller_supplied_underscore() -> anyhow::Result<()> {
        // The whole point of `_` is "the payload that started this workflow",
        // so an explicit `_` in input.env must be replaced, not preserved.
        let input = DbtRunInput {
            project: None,
            command: "run".into(),
            select: None,
            exclude: None,
            vars: BTreeMap::new(),
            target: None,
            full_refresh: false,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::from([("_".into(), "garbage caller value".into())]),
        };
        let env = build_effective_env(&input);
        let underscore = env
            .get("_")
            .ok_or_else(|| anyhow::anyhow!("`_` must be set"))?;
        assert_ne!(underscore, "garbage caller value");
        assert!(underscore.contains("\"command\":\"run\""));
        Ok(())
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

    // --- plural ---

    #[test]
    fn plural_omits_s_for_one_only() {
        assert_eq!(plural(0), "s");
        assert_eq!(plural(1), "");
        assert_eq!(plural(2), "s");
        assert_eq!(plural(100), "s");
    }

    // --- elapsed_secs ---

    #[test]
    fn elapsed_secs_returns_zero_when_either_endpoint_missing() {
        let now = std::time::SystemTime::now();
        assert!(elapsed_secs(None, Some(now)).abs() < f64::EPSILON);
        assert!(elapsed_secs(Some(now), None).abs() < f64::EPSILON);
        assert!(elapsed_secs(None, None).abs() < f64::EPSILON);
    }

    #[test]
    fn elapsed_secs_returns_difference_when_both_present() {
        let start = std::time::SystemTime::UNIX_EPOCH;
        let end = start + std::time::Duration::from_millis(2500);
        let secs = elapsed_secs(Some(start), Some(end));
        assert!((secs - 2.5).abs() < 1e-9);
    }

    #[test]
    fn elapsed_secs_returns_zero_when_end_before_start() {
        // duration_since returns Err when end < start; helper should yield 0.
        let later = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
        let earlier = std::time::SystemTime::UNIX_EPOCH;
        assert!(elapsed_secs(Some(later), Some(earlier)).abs() < f64::EPSILON);
    }

    // --- build_log_header / build_summary_lines ---

    #[test]
    fn build_log_header_lists_resource_type_counts_in_alpha_order() {
        let plan = test_plan(
            vec![vec!["m1", "m2"], vec!["t1"]],
            &[
                ("m1", Some("table"), vec![]),
                ("m2", Some("view"), vec![]),
                ("t1", Some("test"), vec![]),
            ],
        );
        let header = build_log_header(&plan);
        assert_eq!(header.len(), 3);
        assert!(header[0].starts_with("Running with dbt-temporal="));
        // Resource types come from NodeInfo.resource_type which is "model" for everything
        // built by `test_plan` — confirms plural-pluralisation works through the helper.
        assert!(header[1].contains("3 model"));
        assert_eq!(header[2], "Concurrency: 2 parallel levels");
    }

    #[test]
    fn build_log_header_singular_for_single_level_and_node_type() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", Some("table"), vec![])]);
        let header = build_log_header(&plan);
        assert!(header[1].contains("1 model"), "got: {}", header[1]);
        assert!(!header[1].contains("models"));
        assert_eq!(header[2], "Concurrency: 1 parallel level");
    }

    #[test]
    fn build_summary_lines_pluralises_node_count() {
        let lines = build_summary_lines(5, 2.345_678, 4, 0, 1);
        assert_eq!(lines[0], "Finished running 5 nodes in 2.35s.");
        assert_eq!(lines[1], "Done. PASS=4 ERROR=0 SKIP=1 TOTAL=5");
    }

    #[test]
    fn build_summary_lines_singular_for_one_node() {
        let lines = build_summary_lines(1, 0.5, 1, 0, 0);
        assert_eq!(lines[0], "Finished running 1 node in 0.50s.");
    }

    // --- short_activity_error ---

    fn make_failed_error(message: &str) -> temporalio_sdk::ActivityExecutionError {
        use temporalio_common::protos::temporal::api::failure::v1::Failure;
        temporalio_sdk::ActivityExecutionError::Failed(Box::new(Failure {
            message: message.to_string(),
            ..Failure::default()
        }))
    }

    #[test]
    fn short_activity_error_passes_short_messages_through() {
        let err = make_failed_error("relation does not exist");
        let short = short_activity_error(&err);
        // Display formats as "Activity failed: <message>" — must be unchanged for short.
        assert_eq!(short, "Activity failed: relation does not exist");
    }

    #[test]
    fn short_activity_error_truncates_long_messages_to_200_plus_ellipsis() {
        // Message of 250 chars → full Display is even longer; truncated form is
        // exactly 200 chars + "..." suffix.
        let long_msg: String = "A".repeat(250);
        let err = make_failed_error(&long_msg);
        let short = short_activity_error(&err);
        assert!(short.ends_with("..."), "got: {short}");
        // 200 chars before the `...` suffix.
        assert_eq!(short.len(), 203);
    }

    #[test]
    fn short_activity_error_does_not_truncate_at_exactly_200() {
        // Construct a Failed with a message such that the full Display is exactly
        // 200 chars — no truncation should be applied.
        // Display is "Activity failed: <message>" (17 + len(message) chars).
        let msg = "B".repeat(200 - "Activity failed: ".len());
        let err = make_failed_error(&msg);
        let short = short_activity_error(&err);
        assert_eq!(short.len(), 200);
        assert!(!short.ends_with("..."));
    }

    // --- format_activity_label ---

    #[test]
    fn format_activity_label_strips_node_type_prefix_and_lowercases() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", Some("table"), vec![])]);
        // test_plan sets resource_type to "model"; format_activity_label expects
        // raw proto strings ("NODE_TYPE_MODEL"). Patch the entry to mirror the
        // shape that the planner produces.
        let mut plan = plan;
        plan.nodes.get_mut("model.waffle.m1").unwrap().resource_type =
            "NODE_TYPE_MODEL".to_string();
        assert_eq!(format_activity_label(&plan, "model.waffle.m1"), Some("model:m1".to_string()));
    }

    #[test]
    fn format_activity_label_passes_unknown_types_through_lowercase() {
        let plan = test_plan(vec![vec!["t1"]], &[("t1", None, vec![])]);
        let mut plan = plan;
        plan.nodes.get_mut("model.waffle.t1").unwrap().resource_type = "Test".to_string();
        // No NODE_TYPE_ prefix — the raw value is just lowercased.
        assert_eq!(format_activity_label(&plan, "model.waffle.t1"), Some("test:t1".to_string()));
    }

    #[test]
    fn format_activity_label_returns_none_for_unknown_id() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", None, vec![])]);
        assert!(format_activity_label(&plan, "model.waffle.nope").is_none());
    }

    // --- is_blocked_by_failed_upstream ---

    #[test]
    fn is_blocked_by_failed_upstream_true_when_any_dep_failed() {
        let plan = test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        assert!(is_blocked_by_failed_upstream(&plan, "model.waffle.b", &failed));
    }

    #[test]
    fn is_blocked_by_failed_upstream_false_when_no_dep_failed() {
        let plan = test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        let failed = std::collections::BTreeSet::new();
        assert!(!is_blocked_by_failed_upstream(&plan, "model.waffle.b", &failed));
    }

    #[test]
    fn is_blocked_by_failed_upstream_false_for_unknown_id() {
        let plan = test_plan(vec![vec!["a"]], &[("a", None, vec![])]);
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        // An id that isn't in the plan can't be blocked — the level loop will
        // simply skip it. Returning false here keeps the contract coherent.
        assert!(!is_blocked_by_failed_upstream(&plan, "model.waffle.nope", &failed));
    }

    // --- classify_result_status ---

    #[test]
    fn classify_result_status_collapses_to_three_terminals() {
        assert_eq!(classify_result_status(NodeStatus::Error), NodeStatus::Error);
        assert_eq!(classify_result_status(NodeStatus::Skipped), NodeStatus::Skipped);
        // All other variants — including Pending/Running/Cancelled/Success —
        // collapse to Success on the result-collection path.
        assert_eq!(classify_result_status(NodeStatus::Success), NodeStatus::Success);
        assert_eq!(classify_result_status(NodeStatus::Pending), NodeStatus::Success);
        assert_eq!(classify_result_status(NodeStatus::Running), NodeStatus::Success);
        assert_eq!(classify_result_status(NodeStatus::Cancelled), NodeStatus::Success);
    }

    // --- format_result_tag ---

    #[test]
    fn format_result_tag_success_uses_message_or_ok_default() {
        assert_eq!(
            format_result_tag(NodeStatus::Success, Some("CREATE TABLE (4 rows)"), 1.234),
            "CREATE TABLE (4 rows) in 1.23s"
        );
        assert_eq!(format_result_tag(NodeStatus::Success, None, 0.05), "OK in 0.05s");
    }

    #[test]
    fn format_result_tag_error_includes_message_or_default() {
        assert_eq!(
            format_result_tag(NodeStatus::Error, Some("permission denied"), 0.1),
            "ERROR permission denied"
        );
        assert_eq!(format_result_tag(NodeStatus::Error, None, 0.0), "ERROR error");
    }

    #[test]
    fn format_result_tag_skipped_is_constant() {
        assert_eq!(format_result_tag(NodeStatus::Skipped, Some("ignored"), 9.0), "SKIP");
    }

    // --- format_progress_line ---

    #[test]
    fn format_progress_line_renders_dbt_style() {
        assert_eq!(
            format_progress_line(3, 17, "START", "model waffle.stg_customers"),
            "3 of 17 START model waffle.stg_customers"
        );
        assert_eq!(format_progress_line(1, 1, "SKIP", "x"), "1 of 1 SKIP x");
    }

    // --- format_running_details ---

    #[test]
    fn format_running_details_one_indexes_level_and_joins_running() {
        // level_idx is zero-based but the UI shows 1-based.
        assert_eq!(
            format_running_details(2, 5, &["model:a", "model:b"]),
            "level 3/5: model:a, model:b"
        );
    }

    #[test]
    fn format_running_details_handles_empty_running() {
        assert_eq!(format_running_details(0, 1, &[]), "level 1/1: ");
    }

    // --- should_flush_memo ---

    #[test]
    fn should_flush_memo_skips_last_level() {
        // Last level (idx == total - 1) is always handled by the orchestrator's
        // terminal flush; we must not double-write here even on failure.
        assert!(!should_flush_memo(2, 3, true, 10));
        assert!(!should_flush_memo(0, 1, true, 10));
    }

    #[test]
    fn should_flush_memo_flushes_on_failure_in_intermediate_levels() {
        assert!(should_flush_memo(0, 3, true, 10));
        assert!(should_flush_memo(1, 3, true, 10));
    }

    #[test]
    fn should_flush_memo_flushes_on_periodic_boundary() {
        // Every 5th level on a 20-level run, intermediate (not last).
        assert!(should_flush_memo(4, 20, false, 5));
        assert!(should_flush_memo(9, 20, false, 5));
        assert!(!should_flush_memo(0, 20, false, 5));
        assert!(!should_flush_memo(3, 20, false, 5));
    }

    #[test]
    fn should_flush_memo_handles_zero_level_count() {
        // Empty plan — nothing to flush. saturating_sub keeps the math safe.
        assert!(!should_flush_memo(0, 0, true, 10));
    }
}
