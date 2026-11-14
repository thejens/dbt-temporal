use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::common::OnError;
use dbt_schemas::schemas::telemetry::NodeType;

use super::{Scheduler, SchedulerError, SkipReason, Step};
use crate::activities::dag::topological_levels;
use crate::types::{ExecutionPlan, NodeInfo, NodeStatus, NodeStatusTree};
use crate::workflow::helpers::is_blocked_by_failed_upstream;

/// What a test says about one node besides its edges.
#[derive(Clone, Default)]
struct Spec {
    deps: Vec<String>,
    on_error: Option<OnError>,
    priority: Option<u32>,
}

fn spec(deps: &[&str]) -> Spec {
    Spec {
        deps: deps.iter().map(ToString::to_string).collect(),
        ..Spec::default()
    }
}

/// A plan over `specs`, with levels computed the way the plan activity does.
fn build_plan(specs: &BTreeMap<String, Spec>) -> ExecutionPlan {
    let deps: BTreeMap<String, BTreeSet<String>> = specs
        .iter()
        .map(|(id, s)| (id.clone(), s.deps.iter().cloned().collect()))
        .collect();
    let levels = topological_levels(&deps).unwrap_or_default();
    let nodes = specs
        .iter()
        .map(|(id, s)| {
            (
                id.clone(),
                NodeInfo {
                    unique_id: id.clone(),
                    name: id.clone(),
                    resource_type: NodeType::Model,
                    materialization: None,
                    package_name: "p".to_string(),
                    depends_on: s.deps.clone(),
                    priority: s.priority,
                    on_error: s.on_error.clone(),
                },
            )
        })
        .collect();
    ExecutionPlan {
        project: "p".to_string(),
        levels,
        nodes,
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

fn specs(edges: &[(&str, &[&str])]) -> BTreeMap<String, Spec> {
    edges
        .iter()
        .map(|(id, deps)| ((*id).to_string(), spec(deps)))
        .collect()
}

fn pending_tree(plan: &ExecutionPlan) -> NodeStatusTree {
    NodeStatusTree {
        nodes: plan
            .nodes
            .keys()
            .map(|id| (id.clone(), NodeStatus::Pending))
            .collect(),
    }
}

fn scheduler(specs: &BTreeMap<String, Spec>) -> Result<Scheduler, SchedulerError> {
    let plan = build_plan(specs);
    Scheduler::new(&plan, &pending_tree(&plan), &BTreeSet::new(), false)
}

fn starts(steps: &[Step]) -> Vec<&str> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Start(id) => Some(id.as_str()),
            Step::Skip { .. } => None,
        })
        .collect()
}

fn skips(steps: &[Step]) -> Vec<(&str, SkipReason)> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Skip { node, reason } => Some((node.as_str(), *reason)),
            Step::Start(_) => None,
        })
        .collect()
}

const WIDE: usize = 1_000;

#[test]
fn a_chain_starts_one_node_at_a_time() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]))?;

    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new(), "b waits for a");
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["b"]);
    s.on_complete("b", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["c"]);
    assert!(!s.is_finished());
    s.on_complete("c", NodeStatus::Success)?;
    assert!(s.is_finished());
    Ok(())
}

#[test]
fn a_diamond_joins_only_after_both_branches() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["b", "c"])]))?;

    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["b", "c"]);
    s.on_complete("c", NodeStatus::Success)?;
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new(), "d still needs b");
    s.on_complete("b", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["d"]);
    Ok(())
}

#[test]
fn fan_out_starts_every_dependent_and_fan_in_waits_for_all() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[
        ("root", &[]),
        ("m1", &["root"]),
        ("m2", &["root"]),
        ("m3", &["root"]),
        ("sink", &["m1", "m2", "m3"]),
    ]))?;
    s.advance(WIDE, false);
    s.on_complete("root", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["m1", "m2", "m3"]);
    for m in ["m3", "m1"] {
        s.on_complete(m, NodeStatus::Success)?;
        assert_eq!(s.advance(WIDE, false), Vec::<Step>::new());
    }
    s.on_complete("m2", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["sink"]);
    Ok(())
}

/// The point of the scheduler: `fast_child` depends only on `fast`, so a slow
/// node in the same level must not hold it back.
#[test]
fn a_slow_node_does_not_block_unrelated_ready_nodes() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[
        ("slow", &[]),
        ("fast", &[]),
        ("slow_child", &["slow"]),
        ("fast_child", &["fast"]),
    ]))?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["fast", "slow"]);

    s.on_complete("fast", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["fast_child"]);
    assert_eq!(s.terminal_status("slow"), None, "slow is still running");
    Ok(())
}

#[test]
fn the_cap_is_respected_and_refilled_on_each_completion() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &[]), ("c", &[]), ("d", &[]), ("e", &[])]))?;
    let cap = 2;
    let mut running: Vec<String> = Vec::new();

    let first = s.advance(cap - running.len(), false);
    running.extend(starts(&first).iter().map(ToString::to_string));
    assert_eq!(running, vec!["a", "b"]);
    assert_eq!(s.advance(cap - running.len(), false), Vec::<Step>::new(), "window is full");

    for expected in ["c", "d", "e"] {
        let done = running.remove(0);
        s.on_complete(&done, NodeStatus::Success)?;
        let next = s.advance(cap - running.len(), false);
        assert_eq!(starts(&next), vec![expected], "one slot freed starts exactly one node");
        running.push(expected.to_string());
        assert!(running.len() <= cap);
    }
    Ok(())
}

#[test]
fn zero_free_slots_start_nothing_but_still_skip() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"]), ("c", &[])]))?;
    assert_eq!(s.advance(0, false), Vec::<Step>::new());
    s.advance(1, false);
    s.on_complete("a", NodeStatus::Error)?;
    assert_eq!(
        skips(&s.advance(0, false)),
        vec![("b", SkipReason::UpstreamFailed)],
        "a skip occupies no activity slot"
    );
    Ok(())
}

#[test]
fn ready_nodes_start_by_priority_then_plan_position() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[("a", &[]), ("b", &[]), ("c", &[]), ("d", &[])]);
    by_id.entry("c".to_string()).or_default().priority = Some(1);
    by_id.entry("d".to_string()).or_default().priority = Some(3);
    by_id.entry("b".to_string()).or_default().priority = Some(3);
    let mut s = scheduler(&by_id)?;

    // c (1) first; b and d tie at 3 and fall back to plan order; a has no key.
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["c", "b", "d", "a"]);
    Ok(())
}

#[test]
fn a_priority_cap_picks_the_most_urgent_ready_nodes() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[("a", &[]), ("b", &[]), ("c", &[])]);
    by_id.entry("c".to_string()).or_default().priority = Some(1);
    let mut s = scheduler(&by_id)?;
    assert_eq!(starts(&s.advance(1, false)), vec!["c"]);
    Ok(())
}

#[test]
fn a_failure_skips_exactly_the_transitive_dependents() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[
        ("bad", &[]),
        ("ok", &[]),
        ("child", &["bad"]),
        ("grandchild", &["child"]),
        ("sibling", &["ok"]),
        ("mixed", &["ok", "grandchild"]),
    ]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("ok", NodeStatus::Success)?;

    let steps = s.advance(WIDE, false);
    assert_eq!(starts(&steps), vec!["sibling"], "unrelated work still runs");
    assert_eq!(
        skips(&steps),
        vec![
            ("child", SkipReason::UpstreamFailed),
            ("grandchild", SkipReason::UpstreamFailed),
            ("mixed", SkipReason::UpstreamFailed),
        ]
    );
    assert!(s.had_failure());
    assert_eq!(
        s.failed_nodes(),
        ["bad", "child", "grandchild", "mixed"]
            .map(String::from)
            .into()
    );
    s.on_complete("sibling", NodeStatus::Success)?;
    assert!(s.is_finished());
    Ok(())
}

/// A node that waits on one failed and one still-running dependency is skipped
/// once the running one ends, and not started in between.
#[test]
fn a_blocked_node_waits_for_all_dependencies_before_skipping() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("bad", &[]), ("slow", &[]), ("join", &["bad", "slow"])]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new());
    s.on_complete("slow", NodeStatus::Success)?;
    assert_eq!(skips(&s.advance(WIDE, false)), vec![("join", SkipReason::UpstreamFailed)]);
    Ok(())
}

#[test]
fn on_error_continue_lets_dependents_run() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[
        ("soft", &[]),
        ("child", &["soft"]),
        ("hard", &[]),
        ("hchild", &["hard"]),
    ]);
    by_id.entry("soft".to_string()).or_default().on_error = Some(OnError::Continue);
    let mut s = scheduler(&by_id)?;
    s.advance(WIDE, false);
    s.on_complete("soft", NodeStatus::Error)?;
    s.on_complete("hard", NodeStatus::Error)?;

    let steps = s.advance(WIDE, false);
    assert_eq!(starts(&steps), vec!["child"]);
    assert_eq!(skips(&steps), vec![("hchild", SkipReason::UpstreamFailed)]);
    assert!(s.had_failure(), "a tolerated failure still counts for fail-fast");
    Ok(())
}

/// A model with `on_error: continue` that is itself skipped is failed but
/// exempt, so the skip stops there.
#[test]
fn a_skipped_continue_node_does_not_spread_the_skip() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[("bad", &[]), ("soft", &["bad"]), ("after", &["soft"])]);
    by_id.entry("soft".to_string()).or_default().on_error = Some(OnError::Continue);
    let mut s = scheduler(&by_id)?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;

    let steps = s.advance(WIDE, false);
    assert_eq!(skips(&steps), vec![("soft", SkipReason::UpstreamFailed)]);
    assert_eq!(starts(&steps), vec!["after"]);
    Ok(())
}

#[test]
fn warn_and_activity_skipped_nodes_do_not_block_dependents() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("w", &[]), ("sk", &[]), ("after", &["w", "sk"])]))?;
    s.advance(WIDE, false);
    s.on_complete("w", NodeStatus::Warn)?;
    s.on_complete("sk", NodeStatus::Skipped)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["after"]);
    assert!(!s.had_failure());
    Ok(())
}

#[test]
fn fail_fast_skips_every_unstarted_node_and_lets_running_ones_finish() -> Result<(), SchedulerError>
{
    let mut s = scheduler(&specs(&[
        ("bad", &[]),
        ("running", &[]),
        ("child", &["bad"]),
        ("unrelated", &["running"]),
        ("late", &["unrelated"]),
    ]))?;
    s.advance(2, true);
    s.on_complete("bad", NodeStatus::Error)?;

    let steps = s.advance(WIDE, true);
    assert_eq!(starts(&steps), Vec::<&str>::new(), "nothing starts after a failure");
    assert_eq!(
        skips(&steps),
        vec![
            ("child", SkipReason::FailFast),
            ("unrelated", SkipReason::FailFast),
            ("late", SkipReason::FailFast),
        ]
    );
    assert_eq!(s.terminal_status("running"), None, "an in-flight node is left to finish");
    s.on_complete("running", NodeStatus::Success)?;
    assert!(s.is_finished());
    assert_eq!(s.failed_nodes(), ["bad".to_string()].into(), "fail-fast skips are not failures");
    Ok(())
}

#[test]
fn fail_fast_off_keeps_scheduling_past_a_failure() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("bad", &[]), ("other", &[]), ("next", &["other"])]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("other", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["next"]);
    Ok(())
}

/// `set_fail_fast` can flip the setting mid-run; the next decision honours it.
#[test]
fn enabling_fail_fast_after_a_failure_takes_effect_at_the_next_advance()
-> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("bad", &[]), ("hold", &[]), ("next", &["hold"])]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("hold", NodeStatus::Success)?;
    assert_eq!(skips(&s.advance(WIDE, true)), vec![("next", SkipReason::FailFast)]);
    Ok(())
}

#[test]
fn cancelling_closes_out_running_ready_and_waiting_nodes() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &[]), ("c", &["a"]), ("d", &["c"])]))?;
    s.advance(1, false);
    s.on_complete("a", NodeStatus::Success)?;
    s.advance(1, false); // starts b; c stays ready

    assert_eq!(s.cancel_unfinished(), vec!["b", "c", "d"]);
    assert!(s.is_finished());
    assert_eq!(s.terminal_status("a"), Some(NodeStatus::Success));
    assert_eq!(s.terminal_status("b"), Some(NodeStatus::Cancelled));
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new());
    assert_eq!(s.cancel_unfinished(), Vec::<String>::new(), "idempotent");
    Ok(())
}

#[test]
fn resuming_skips_finished_nodes_and_honours_carried_failures() -> Result<(), SchedulerError> {
    let by_id = specs(&[
        ("done", &[]),
        ("bad", &[]),
        ("after_done", &["done"]),
        ("after_bad", &["bad"]),
        ("free", &[]),
    ]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("done".to_string(), NodeStatus::Success);
    tree.nodes.insert("bad".to_string(), NodeStatus::Error);
    let failed: BTreeSet<String> = ["bad".to_string(), "gone".to_string()].into();
    let mut s = Scheduler::new(&plan, &tree, &failed, true)?;

    assert!(s.had_failure());
    let steps = s.advance(WIDE, false);
    assert_eq!(starts(&steps), vec!["free", "after_done"]);
    assert_eq!(skips(&steps), vec![("after_bad", SkipReason::UpstreamFailed)]);
    assert!(s.failed_nodes().contains("gone"), "ids outside the plan are handed on");
    assert_eq!(s.counts().success, 1);
    assert_eq!(s.counts().error, 1);
    Ok(())
}

#[test]
fn resuming_with_fail_fast_and_a_carried_failure_skips_the_rest() -> Result<(), SchedulerError> {
    let by_id = specs(&[("bad", &[]), ("next", &["bad"]), ("other", &[])]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("bad".to_string(), NodeStatus::Error);
    let mut s = Scheduler::new(&plan, &tree, &["bad".to_string()].into(), true)?;

    let steps = s.advance(WIDE, true);
    assert_eq!(starts(&steps), Vec::<&str>::new());
    assert_eq!(skips(&steps).len(), 2);
    assert!(s.is_finished());
    Ok(())
}

/// A checkpoint records `Running` for nothing, but a stale one must not be
/// trusted as finished.
#[test]
fn a_running_status_in_a_checkpoint_is_rerun() -> Result<(), SchedulerError> {
    let by_id = specs(&[("a", &[])]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("a".to_string(), NodeStatus::Running);
    let mut s = Scheduler::new(&plan, &tree, &BTreeSet::new(), false)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    Ok(())
}

#[test]
fn a_fully_finished_resume_is_finished_immediately() -> Result<(), SchedulerError> {
    let by_id = specs(&[("a", &[]), ("b", &["a"])]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    for status in tree.nodes.values_mut() {
        *status = NodeStatus::Success;
    }
    let s = Scheduler::new(&plan, &tree, &BTreeSet::new(), false)?;
    assert!(s.is_finished());
    assert_eq!(s.completed_levels(), 2);
    Ok(())
}

#[test]
fn an_empty_plan_is_finished() -> Result<(), SchedulerError> {
    let s = scheduler(&BTreeMap::new())?;
    assert!(s.is_finished());
    Ok(())
}

#[test]
fn edges_to_ids_outside_the_plan_are_ignored() -> Result<(), SchedulerError> {
    let mut plan = build_plan(&specs(&[("a", &["not_in_plan"])]));
    // The edge points outside the selection, so only `a` is in the levels.
    plan.levels = vec![vec!["a".to_string()]];
    let mut s = Scheduler::new(&plan, &pending_tree(&plan), &BTreeSet::new(), false)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    Ok(())
}

#[test]
fn a_dependency_cycle_is_rejected() {
    let by_id = specs(&[("a", &["b"]), ("b", &["a"]), ("c", &[])]);
    let mut plan = build_plan(&by_id);
    // The level computation rejects cycles; a plan whose edges disagree with
    // its levels is the case the scheduler has to catch itself.
    plan.levels = vec![vec!["a".to_string(), "b".to_string(), "c".to_string()]];
    let result = Scheduler::new(&plan, &pending_tree(&plan), &BTreeSet::new(), false);
    assert_eq!(result.err(), Some(SchedulerError::Cycle(2)));
}

#[test]
fn completing_a_node_that_is_not_running_is_an_error() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"])]))?;
    assert_eq!(
        s.on_complete("a", NodeStatus::Success),
        Err(SchedulerError::NotRunning("a".to_string())),
        "ready but not started"
    );
    assert_eq!(
        s.on_complete("nope", NodeStatus::Success),
        Err(SchedulerError::NotRunning("nope".to_string()))
    );
    s.advance(WIDE, false);
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(
        s.on_complete("a", NodeStatus::Success),
        Err(SchedulerError::NotRunning("a".to_string())),
        "a node completes once"
    );
    Ok(())
}

#[test]
fn completed_levels_counts_leading_finished_levels() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("slow", &[]), ("b", &["a"]), ("c", &["b"])]))?;
    assert_eq!(s.completed_levels(), 0);
    s.advance(WIDE, false);
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(s.completed_levels(), 0, "level 0 still has `slow`");
    s.on_complete("slow", NodeStatus::Success)?;
    assert_eq!(s.completed_levels(), 1);
    s.advance(WIDE, false);
    s.on_complete("b", NodeStatus::Success)?;
    assert_eq!(s.completed_levels(), 2);
    Ok(())
}

#[test]
fn counts_track_running_and_terminal_states() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &[]), ("c", &["a"]), ("d", &["b"])]))?;
    s.advance(WIDE, false);
    assert_eq!(s.counts().running, 2);
    s.on_complete("a", NodeStatus::Warn)?;
    s.on_complete("b", NodeStatus::Error)?;
    s.advance(WIDE, false);
    let counts = s.counts();
    assert_eq!((counts.running, counts.warn, counts.error, counts.skipped), (1, 1, 1, 1));
    Ok(())
}

// --- exhaustive and generated runs ---

/// Deterministic generator so a failure reproduces from its seed.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap_or(1)).unwrap_or(0)
    }
}

fn random_specs(rng: &mut Lcg) -> BTreeMap<String, Spec> {
    let n = 1 + rng.below(30);
    let ids: Vec<String> = (0..n).map(|i| format!("n{i:02}")).collect();
    let mut by_id = BTreeMap::new();
    for (i, id) in ids.iter().enumerate() {
        let deps = (0..i)
            .filter(|_| rng.below(100) < 15)
            .map(|j| ids[j].clone())
            .collect();
        by_id.insert(
            id.clone(),
            Spec {
                deps,
                on_error: (rng.below(5) == 0).then_some(OnError::Continue),
                priority: (rng.below(3) > 0).then(|| 1 + u32::try_from(rng.below(5)).unwrap_or(0)),
            },
        );
    }
    by_id
}

fn outcome_for(id: &str, salt: u64) -> NodeStatus {
    let hash = id
        .bytes()
        .fold(salt, |acc, b| acc.wrapping_mul(31).wrapping_add(u64::from(b)));
    match hash % 10 {
        0 | 1 => NodeStatus::Error,
        2 => NodeStatus::Warn,
        3 => NodeStatus::Skipped,
        _ => NodeStatus::Success,
    }
}

/// The level loop's rules applied level by level, with fail-fast off.
fn level_loop_statuses(plan: &ExecutionPlan, salt: u64) -> BTreeMap<String, NodeStatus> {
    let mut failed = BTreeSet::new();
    let mut statuses = BTreeMap::new();
    for level in &plan.levels {
        for id in level {
            if is_blocked_by_failed_upstream(plan, id, &failed) {
                failed.insert(id.clone());
                statuses.insert(id.clone(), NodeStatus::Skipped);
                continue;
            }
            let status = outcome_for(id, salt);
            if status == NodeStatus::Error {
                failed.insert(id.clone());
            }
            statuses.insert(id.clone(), status);
        }
    }
    statuses
}

struct Trace {
    starts: Vec<String>,
    skip_count: usize,
    statuses: BTreeMap<String, NodeStatus>,
    max_running: usize,
}

/// Run a scheduler to completion, completing a randomly chosen running node
/// each round, and assert the safety invariants as it goes.
fn drive(
    plan: &ExecutionPlan,
    cap: usize,
    fail_fast: bool,
    salt: u64,
    rng: &mut Lcg,
) -> Result<Trace, SchedulerError> {
    let mut s = Scheduler::new(plan, &pending_tree(plan), &BTreeSet::new(), false)?;
    let mut running: Vec<String> = Vec::new();
    let mut trace = Trace {
        starts: Vec::new(),
        skip_count: 0,
        statuses: BTreeMap::new(),
        max_running: 0,
    };
    let mut failure_seen = false;

    for _ in 0..10_000 {
        let steps = s.advance(cap - running.len(), fail_fast);
        for step in steps {
            match step {
                Step::Start(id) => {
                    assert!(
                        !(fail_fast && failure_seen),
                        "{id} started after a failure under fail-fast"
                    );
                    for dep in &plan.nodes[&id].depends_on {
                        assert!(
                            s.terminal_status(dep).is_some(),
                            "{id} started before dependency {dep} was terminal"
                        );
                    }
                    running.push(id.clone());
                    trace.starts.push(id);
                }
                Step::Skip { node, .. } => {
                    trace.skip_count += 1;
                    assert_eq!(trace.statuses.insert(node, NodeStatus::Skipped), None);
                }
            }
        }
        trace.max_running = trace.max_running.max(running.len());
        assert!(running.len() <= cap, "{} running exceeds the cap {cap}", running.len());
        if s.is_finished() {
            return Ok(trace);
        }
        assert!(!running.is_empty(), "stalled with unfinished nodes and nothing running");

        let id = running.remove(rng.below(running.len()));
        let status = outcome_for(&id, salt);
        failure_seen |= status == NodeStatus::Error;
        s.on_complete(&id, status)?;
        assert_eq!(trace.statuses.insert(id, status), None, "a node completes once");
    }
    panic!("did not terminate");
}

#[test]
fn generated_dags_match_the_level_loop_without_fail_fast() -> Result<(), SchedulerError> {
    let mut rng = Lcg(7);
    for round in 0..300 {
        let plan = build_plan(&random_specs(&mut rng));
        let cap = 1 + rng.below(6);
        let salt = rng.next();
        let trace = drive(&plan, cap, false, salt, &mut rng)?;

        assert_eq!(
            trace.statuses,
            level_loop_statuses(&plan, salt),
            "round {round}: same outcomes must give the level loop's statuses"
        );
        // Every node ends in exactly one terminal status, and nodes that
        // ran are exactly those not skipped.
        assert_eq!(trace.statuses.len(), plan.nodes.len());
        let skipped_by_scheduler = trace.skip_count;
        let ran = trace.starts.len();
        assert_eq!(ran + skipped_by_scheduler, plan.nodes.len(), "round {round}");
        let unique: BTreeSet<&String> = trace.starts.iter().collect();
        assert_eq!(unique.len(), ran, "no node starts twice");
        assert!(trace.max_running <= cap);
    }
    Ok(())
}

#[test]
fn generated_dags_terminate_and_stop_starting_under_fail_fast() -> Result<(), SchedulerError> {
    let mut rng = Lcg(99);
    for _ in 0..300 {
        let plan = build_plan(&random_specs(&mut rng));
        let cap = 1 + rng.below(6);
        let salt = rng.next();
        let trace = drive(&plan, cap, true, salt, &mut rng)?;
        assert_eq!(trace.statuses.len(), plan.nodes.len());
        assert_eq!(trace.starts.len() + trace.skip_count, plan.nodes.len());
    }
    Ok(())
}

#[test]
fn a_wide_level_is_never_started_beyond_the_cap() -> Result<(), SchedulerError> {
    let by_id: BTreeMap<String, Spec> = (0..1_200)
        .map(|i| (format!("w{i:04}"), Spec::default()))
        .collect();
    let mut s = scheduler(&by_id)?;
    let first = s.advance(500, false);
    assert_eq!(starts(&first).len(), 500);
    assert_eq!(starts(&first)[0], "w0000");
    assert_eq!(s.advance(0, false), Vec::<Step>::new());
    Ok(())
}
