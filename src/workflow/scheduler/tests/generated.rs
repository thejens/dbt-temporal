//! Generated DAGs checked against the level loop and the scheduler's own
//! safety invariants.

use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::common::OnError;

use super::fixtures::*;
use crate::types::{ExecutionPlan, NodeStatus, NodeStatusTree};
use crate::workflow::helpers::is_blocked_by_failed_upstream;
use crate::workflow::scheduler::{Scheduler, SchedulerError, Step};

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
            let tree = NodeStatusTree {
                nodes: statuses.clone(),
            };
            if is_blocked_by_failed_upstream(plan, id, &failed, &tree, true) {
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
