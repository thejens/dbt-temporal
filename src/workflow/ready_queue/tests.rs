use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use futures::FutureExt;

use super::{MemoCadence, ReadyQueueRun, next_completed};
use crate::types::{
    DbtRunInput, ExecutionPlan, NodeExecutionResult, NodeInfo, NodeStatus, RetryConfig,
    TimeoutConfig,
};
use crate::workflow::levels::ResumePoint;
use crate::workflow::scheduler::SkipReason;
use dbt_schemas::schemas::telemetry::NodeType;

// --- memo cadence ---

#[test]
fn a_small_run_is_flushed_every_ten_completions() {
    let mut cadence = MemoCadence::new(30);
    let flushes: Vec<bool> = (0..20).map(|_| cadence.record(1, false)).collect();
    let at: Vec<usize> = flushes
        .iter()
        .enumerate()
        .filter(|(_, f)| **f)
        .map(|(i, _)| i + 1)
        .collect();
    assert_eq!(at, vec![10, 20]);
}

#[test]
fn a_large_run_is_flushed_about_twenty_times_in_all() {
    let total = 10_000;
    let mut cadence = MemoCadence::new(total);
    let flushes = (0..total).filter(|_| cadence.record(1, false)).count();
    assert_eq!(flushes, 20);
}

#[test]
fn a_failure_is_flushed_before_the_quiet_interval_but_not_immediately() {
    let mut cadence = MemoCadence::new(10_000); // quiet interval 500, failure gap 100
    assert!(!cadence.record(1, true), "a lone failure is not written at once");
    for _ in 0..98 {
        assert!(!cadence.record(1, false));
    }
    assert!(cadence.record(1, false), "written once the failure gap has passed");
    assert!(!cadence.record(1, false), "and the failure is no longer pending");
}

#[test]
fn a_run_of_failures_cannot_write_the_memo_per_node() {
    let total = 10_000;
    let mut cadence = MemoCadence::new(total);
    let flushes = (0..total).filter(|_| cadence.record(1, true)).count();
    assert_eq!(flushes, total / 100);
}

#[test]
fn a_flush_for_another_reason_restarts_the_count() {
    let mut cadence = MemoCadence::new(30);
    for _ in 0..9 {
        cadence.record(1, true);
    }
    cadence.flushed();
    assert!(!cadence.record(1, false), "failure no longer pending, count restarted");
}

#[test]
fn skips_count_as_events_in_one_step() {
    let mut cadence = MemoCadence::new(30);
    assert!(cadence.record(10, false));
}

// --- waiting for the first completion ---

type Boxed = Pin<Box<dyn Future<Output = u8>>>;

fn ready(value: u8) -> Boxed {
    Box::pin(std::future::ready(value))
}

fn never() -> Boxed {
    Box::pin(std::future::pending())
}

#[test]
fn the_first_ready_future_is_returned_with_its_index() {
    let mut pending = vec![never(), ready(7), never()];
    assert_eq!(next_completed(&mut pending).now_or_never(), Some((1, 7)));
}

#[test]
fn when_several_are_ready_the_lowest_index_wins() {
    let mut pending = vec![never(), ready(1), ready(2)];
    assert_eq!(next_completed(&mut pending).now_or_never(), Some((1, 1)));
    // The caller removes what it consumed; the other is returned next.
    drop(pending.remove(1));
    assert_eq!(next_completed(&mut pending).now_or_never(), Some((1, 2)));
}

#[test]
fn nothing_is_returned_while_every_future_is_pending() {
    let mut pending = vec![never(), never()];
    assert_eq!(next_completed(&mut pending).now_or_never(), None);
    let mut none: Vec<Boxed> = Vec::new();
    assert_eq!(next_completed(&mut none).now_or_never(), None);
}

// --- bookkeeping ---

fn node(id: &str, deps: &[&str]) -> (String, NodeInfo) {
    (
        id.to_string(),
        NodeInfo {
            unique_id: id.to_string(),
            name: id.to_string(),
            resource_type: NodeType::Model,
            materialization: Some("table".to_string()),
            package_name: "p".to_string(),
            depends_on: deps.iter().map(ToString::to_string).collect(),
            priority: None,
            on_error: None,
        },
    )
}

fn plan() -> ExecutionPlan {
    ExecutionPlan {
        project: "p".to_string(),
        levels: vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["c".to_string()],
        ],
        nodes: [node("a", &[]), node("b", &[]), node("c", &["a", "b"])].into(),
        manifest_json: None,
        manifest_ref: None,
        invocation_id: "inv".to_string(),
        search_attributes: BTreeMap::new(),
        write_artifacts: false,
        has_on_run_start: false,
        has_on_run_end: false,
        priority_scheduling: false,
        has_project_checks: false,
    }
}

fn result(id: &str, status: NodeStatus) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: id.to_string(),
        status,
        execution_time: 1.5,
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

struct Fixture {
    plan: ExecutionPlan,
    input: DbtRunInput,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    env: BTreeMap<String, String>,
}

fn fixture() -> Fixture {
    Fixture {
        plan: plan(),
        input: serde_json::from_value(serde_json::json!({ "command": "run" }))
            .unwrap_or_else(|e| panic!("input: {e}")),
        retry: RetryConfig::default(),
        timeouts: TimeoutConfig::default(),
        env: BTreeMap::new(),
    }
}

fn run_of(f: &Fixture) -> ReadyQueueRun<'_> {
    let resume = ResumePoint::fresh(&f.plan, true);
    match ReadyQueueRun::new(&f.plan, &f.input, &f.retry, &f.timeouts, &f.env, &resume) {
        Ok(run) => run,
        Err(e) => panic!("run: {e}"),
    }
}

#[test]
fn a_fresh_run_starts_with_the_log_header_and_every_node_pending() {
    let f = fixture();
    let run = run_of(&f);
    assert_eq!(run.total_nodes, 3);
    assert_eq!(run.log_lines.len(), 3, "the header the level loop also writes");
    assert_eq!(run.node_counter, 0);
    assert!(
        run.node_status
            .nodes
            .values()
            .all(|s| *s == NodeStatus::Pending)
    );
}

#[test]
fn starting_and_skipping_number_the_progress_lines_in_order() {
    let f = fixture();
    let mut run = run_of(&f);
    let header = run.log_lines.len();

    run.record_start("a");
    run.record_skip("c", SkipReason::UpstreamFailed);
    run.record_skip("b", SkipReason::FailFast);

    assert_eq!(
        run.log_lines[header..],
        [
            "1 of 3 START table model p.a",
            "2 of 3 SKIP table model p.c",
            "3 of 3 SKIP table model p.b",
        ]
    );
    assert_eq!(run.node_status.nodes["a"], NodeStatus::Running);
    assert_eq!(run.node_status.nodes["c"], NodeStatus::Skipped);
    let messages: Vec<_> = run
        .all_results
        .iter()
        .map(|r| (r.unique_id.as_str(), r.message.as_deref()))
        .collect();
    assert_eq!(
        messages,
        vec![
            ("c", Some("skipped: upstream dependency failed")),
            ("b", Some("skipped due to upstream failure (fail_fast)")),
        ]
    );
}

#[test]
fn a_result_is_logged_classified_and_kept() {
    let f = fixture();
    let mut run = run_of(&f);
    let header = run.log_lines.len();

    let ok = run.record_result("a", result("a", NodeStatus::Success));
    let warn = run.record_result("b", result("b", NodeStatus::Warn));
    let bad = run.record_result("c", result("c", NodeStatus::Pending));

    assert_eq!((ok, warn, bad), (NodeStatus::Success, NodeStatus::Warn, NodeStatus::Error));
    assert_eq!(
        run.log_lines[header..],
        [
            "table model p.a  [OK in 1.50s]",
            "table model p.b  [WARN warning in 1.50s]",
            "table model p.c  [ERROR error]",
        ]
    );
    assert_eq!(run.all_results.len(), 3);
    assert_eq!(run.node_status.nodes["b"], NodeStatus::Warn);
}

#[test]
fn an_activity_failure_is_logged_and_recorded_as_an_error_result() {
    let f = fixture();
    let mut run = run_of(&f);
    let header = run.log_lines.len();

    run.record_failure("a", "boom", "Activity task failed: boom in full");

    assert_eq!(run.log_lines[header..], ["table model p.a  [ERROR boom]"]);
    assert_eq!(run.node_status.nodes["a"], NodeStatus::Error);
    assert_eq!(
        run.all_results[0].message.as_deref(),
        Some("activity failed: Activity task failed: boom in full")
    );
}

#[test]
fn cancellation_gives_every_unfinished_node_exactly_one_cancelled_result() {
    let f = fixture();
    let mut run = run_of(&f);
    run.scheduler.advance(10, false);
    run.record_start("a");
    run.record_start("b");

    run.record_result("b", result("b", NodeStatus::Success));
    if let Err(e) = run.scheduler.on_complete("b", NodeStatus::Success) {
        panic!("{e}");
    }
    run.record_cancellation("a");
    if let Err(e) = run.scheduler.on_complete("a", NodeStatus::Cancelled) {
        panic!("{e}");
    }
    run.cancel_remaining();

    let ids: Vec<&str> = run
        .all_results
        .iter()
        .map(|r| r.unique_id.as_str())
        .collect();
    assert_eq!(ids, vec!["b", "a", "c"]);
    let unique: BTreeSet<&str> = ids.iter().copied().collect();
    assert_eq!(unique.len(), 3, "one result per node");
    assert_eq!(run.node_status.nodes["a"], NodeStatus::Cancelled);
    assert_eq!(run.node_status.nodes["c"], NodeStatus::Cancelled);
    assert!(
        run.log_lines
            .iter()
            .any(|l| l == "table model p.a  [CANCELLED]")
    );
    assert!(run.scheduler.is_finished());
}

#[test]
fn a_resumed_run_carries_statuses_failures_and_counter() {
    let f = fixture();
    let mut resume = ResumePoint::fresh(&f.plan, true);
    resume.log_lines = Vec::new();
    resume.node_counter = 2;
    resume.had_failure = true;
    resume.failed_nodes = ["a".to_string()].into();
    resume
        .node_status
        .nodes
        .insert("a".to_string(), NodeStatus::Error);
    resume
        .node_status
        .nodes
        .insert("b".to_string(), NodeStatus::Success);
    resume.prior_segments = vec!["seg-0.json".to_string()];

    let run = match ReadyQueueRun::new(&f.plan, &f.input, &f.retry, &f.timeouts, &f.env, &resume) {
        Ok(run) => run,
        Err(e) => panic!("run: {e}"),
    };

    assert_eq!(run.node_counter, 2);
    assert!(run.scheduler.had_failure());
    assert_eq!(run.prior_segments, vec!["seg-0.json".to_string()]);
    assert_eq!(run.scheduler.failed_nodes(), ["a".to_string()].into());
    assert_eq!(run.scheduler.terminal_status("b"), Some(NodeStatus::Success));
}

#[test]
fn a_plan_with_a_cycle_is_refused_so_the_caller_can_fall_back() {
    let mut f = fixture();
    if let Some(a) = f.plan.nodes.get_mut("a") {
        a.depends_on = vec!["c".to_string()];
    }
    let resume = ResumePoint::fresh(&f.plan, true);
    let result = ReadyQueueRun::new(&f.plan, &f.input, &f.retry, &f.timeouts, &f.env, &resume);
    assert!(result.is_err());
}
