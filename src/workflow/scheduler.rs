//! Readiness-based scheduling of a dbt run's nodes.
//!
//! A pure state machine: it knows the plan's dependency graph and which nodes
//! have finished, and answers "what may start now". It performs no Temporal
//! calls and reads no clock, so the same sequence of calls always produces the
//! same sequence of decisions — which is what lets workflow code drive it and
//! still replay deterministically.
//!
//! The rules mirror the level loop's, applied per node instead of per level:
//!
//! * a node starts once every dependency is terminal;
//! * a node whose dependency failed is skipped, unless that dependency is a
//!   model configured with `on_error: continue`; a node skipped this way counts
//!   as failed, so the skip spreads to its own dependents;
//! * with fail-fast on and any failure recorded, nothing further starts and
//!   every node that has not started is skipped;
//! * a node the activity itself reported as skipped, or as a warning, is not a
//!   failure and does not hold its dependents back.
//!
//! Ready nodes start in a fixed order — Temporal priority key (lower is more
//! urgent, absent last), then position in the plan's levels — so the order is a
//! function of the plan and of the completions seen so far, nothing else.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{ExecutionPlan, NodeStatus, NodeStatusTree};

use super::helpers::dep_continues_on_error;

#[cfg(test)]
mod tests;

/// Why a node was skipped without running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// A dependency failed and its failure is not exempted by `on_error: continue`.
    UpstreamFailed,
    /// Fail-fast is on and the run has already seen a failure.
    FailFast,
}

/// One scheduling decision for the driver to carry out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Start the node's activity.
    Start(String),
    /// Record the node as skipped; nothing runs for it.
    Skip { node: String, reason: SkipReason },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerError {
    /// The plan's dependency edges contain a cycle, so some nodes could never
    /// become ready. The plan activity rejects cyclic graphs, so this means the
    /// edges disagree with the levels.
    #[error("dependency cycle: {0} node(s) can never become ready")]
    Cycle(usize),
    /// A completion was reported for a node that was not running.
    #[error("node `{0}` is not running")]
    NotRunning(String),
}

/// How many nodes are in each state, kept current as the run advances so the
/// driver can publish progress without walking every node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatusCounts {
    pub running: usize,
    pub success: usize,
    pub warn: usize,
    pub error: usize,
    /// Skipped and cancelled nodes, folded together as the status snapshot does.
    pub skipped: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Waiting on at least one unfinished dependency.
    Waiting,
    /// Every dependency is terminal and none blocks it.
    Ready,
    /// Every dependency is terminal and one failed: to be skipped.
    Blocked,
    Running,
    Done(NodeStatus),
}

#[derive(Debug)]
struct Slot {
    id: String,
    level: usize,
    /// Temporal priority key; lower runs first, absent sorts last.
    priority: u32,
    /// A failure of this node does not block its dependents.
    continues_on_error: bool,
    deps: Vec<usize>,
    dependents: Vec<usize>,
    /// Dependencies not yet terminal.
    unmet: usize,
    state: State,
    /// Errored, or skipped because an upstream node failed.
    failed: bool,
}

/// The run's scheduling state. See the module docs for the rules.
#[derive(Debug)]
pub struct Scheduler {
    /// In plan order: level by level, each level in its own order.
    slots: Vec<Slot>,
    index: BTreeMap<String, usize>,
    /// Ready nodes keyed `(priority, plan position)`, so the first entry is the
    /// next to start.
    ready: BTreeSet<(u32, usize)>,
    blocked: BTreeSet<usize>,
    had_failure: bool,
    /// Failed ids carried in from an earlier segment that this plan no longer
    /// schedules; kept so the checkpoint hands them on unchanged.
    foreign_failed: BTreeSet<String>,
    level_remaining: Vec<usize>,
    completed_levels: usize,
    unfinished: usize,
    counts: StatusCounts,
}

impl Scheduler {
    /// Build the scheduler for `plan`, resuming from `statuses`.
    ///
    /// A node whose status is anything but `Pending` or `Running` is finished
    /// already — that is how a segment picks up where its predecessor stopped.
    /// `failed_nodes` and `had_failure` are the predecessor's, so downstream
    /// skipping and fail-fast keep working across the handover.
    pub fn new(
        plan: &ExecutionPlan,
        statuses: &NodeStatusTree,
        failed_nodes: &BTreeSet<String>,
        had_failure: bool,
    ) -> Result<Self, SchedulerError> {
        let mut slots = Vec::new();
        let mut index = BTreeMap::new();
        for (level, ids) in plan.levels.iter().enumerate() {
            for id in ids {
                let finished = match statuses.nodes.get(id) {
                    Some(NodeStatus::Pending | NodeStatus::Running) | None => None,
                    Some(status) => Some(*status),
                };
                let info = plan.nodes.get(id);
                index.insert(id.clone(), slots.len());
                slots.push(Slot {
                    id: id.clone(),
                    level,
                    priority: info.and_then(|n| n.priority).unwrap_or(u32::MAX),
                    continues_on_error: dep_continues_on_error(plan, id),
                    deps: Vec::new(),
                    dependents: Vec::new(),
                    unmet: 0,
                    state: finished.map_or(State::Waiting, State::Done),
                    failed: failed_nodes.contains(id),
                });
            }
        }

        // Edges to ids outside the plan are dropped: such a node is never run or
        // failed by this scheduler, so it can neither delay nor block anything.
        for position in 0..slots.len() {
            let deps: Vec<usize> = plan
                .nodes
                .get(&slots[position].id)
                .into_iter()
                .flat_map(|n| n.depends_on.iter())
                .filter_map(|dep| index.get(dep).copied())
                .collect();
            for &dep in &deps {
                slots[dep].dependents.push(position);
            }
            slots[position].unmet = deps
                .iter()
                .filter(|&&d| !matches!(slots[d].state, State::Done(_)))
                .count();
            slots[position].deps = deps;
        }

        let foreign_failed = failed_nodes
            .iter()
            .filter(|id| !index.contains_key(*id))
            .cloned()
            .collect();

        let mut level_remaining = vec![0; plan.levels.len()];
        let mut counts = StatusCounts::default();
        let mut unfinished = 0;
        for slot in &slots {
            if let State::Done(status) = slot.state {
                counts.record(status);
            } else {
                level_remaining[slot.level] += 1;
                unfinished += 1;
            }
        }

        let mut scheduler = Self {
            slots,
            index,
            ready: BTreeSet::new(),
            blocked: BTreeSet::new(),
            had_failure,
            foreign_failed,
            level_remaining,
            completed_levels: 0,
            unfinished,
            counts,
        };
        scheduler.reject_cycles()?;
        scheduler.advance_completed_levels();
        for position in 0..scheduler.slots.len() {
            if scheduler.slots[position].state == State::Waiting
                && scheduler.slots[position].unmet == 0
            {
                scheduler.settle(position);
            }
        }
        Ok(scheduler)
    }

    /// Kahn's algorithm over the unfinished nodes: every one of them must be
    /// reachable from a node with no unmet dependencies.
    fn reject_cycles(&self) -> Result<(), SchedulerError> {
        let mut unmet: Vec<usize> = self.slots.iter().map(|s| s.unmet).collect();
        let mut queue: Vec<usize> = (0..self.slots.len())
            .filter(|&i| self.slots[i].state == State::Waiting && unmet[i] == 0)
            .collect();
        let mut reached = 0;
        while let Some(position) = queue.pop() {
            reached += 1;
            for &dependent in &self.slots[position].dependents {
                if self.slots[dependent].state != State::Waiting {
                    continue;
                }
                unmet[dependent] -= 1;
                if unmet[dependent] == 0 {
                    queue.push(dependent);
                }
            }
        }
        if reached == self.unfinished {
            Ok(())
        } else {
            Err(SchedulerError::Cycle(self.unfinished - reached))
        }
    }

    /// Decide what to do with a node whose dependencies are all terminal.
    fn settle(&mut self, position: usize) {
        let blocked = self.slots[position]
            .deps
            .iter()
            .any(|&d| self.slots[d].failed && !self.slots[d].continues_on_error);
        if blocked {
            self.slots[position].state = State::Blocked;
            self.blocked.insert(position);
        } else {
            self.slots[position].state = State::Ready;
            self.ready.insert((self.slots[position].priority, position));
        }
    }

    /// Move `position` to a terminal state and release its dependents.
    ///
    /// `release` is false when the whole remainder of the run is being closed
    /// out at once, where waking dependents would only be undone.
    fn finish(&mut self, position: usize, status: NodeStatus, failed: bool, release: bool) {
        if self.slots[position].state == State::Running {
            self.counts.running -= 1;
        }
        self.counts.record(status);
        self.slots[position].state = State::Done(status);
        self.slots[position].failed = failed;
        self.unfinished -= 1;
        self.level_remaining[self.slots[position].level] -= 1;
        self.advance_completed_levels();

        if !release {
            return;
        }
        // The node is terminal, so nothing reads its dependents again.
        for dependent in std::mem::take(&mut self.slots[position].dependents) {
            if self.slots[dependent].state != State::Waiting {
                continue;
            }
            self.slots[dependent].unmet -= 1;
            if self.slots[dependent].unmet == 0 {
                self.settle(dependent);
            }
        }
    }

    fn advance_completed_levels(&mut self) {
        while self
            .level_remaining
            .get(self.completed_levels)
            .is_some_and(|&remaining| remaining == 0)
        {
            self.completed_levels += 1;
        }
    }

    /// Record the end of a running node's activity.
    ///
    /// `status` is the node's classified result: `Error` marks a failure,
    /// anything else lets dependents proceed.
    pub fn on_complete(&mut self, node: &str, status: NodeStatus) -> Result<(), SchedulerError> {
        let position = self
            .index
            .get(node)
            .copied()
            .filter(|&p| self.slots[p].state == State::Running)
            .ok_or_else(|| SchedulerError::NotRunning(node.to_string()))?;
        let failed = status == NodeStatus::Error;
        self.had_failure |= failed;
        self.finish(position, status, failed, true);
        Ok(())
    }

    /// Everything the scheduler can decide right now, given `free_slots` open
    /// activity slots and the effective fail-fast setting.
    ///
    /// Skips come first and cost no slot; starts follow, at most `free_slots`
    /// of them, in the fixed order from the module docs.
    pub fn advance(&mut self, free_slots: usize, fail_fast: bool) -> Vec<Step> {
        let mut steps = Vec::new();

        if fail_fast && self.had_failure {
            // Every node that has not started is skipped, so no dependents need
            // waking. The skipped nodes are not marked failed: the failure that
            // triggered this is already recorded.
            for position in 0..self.slots.len() {
                if matches!(
                    self.slots[position].state,
                    State::Waiting | State::Ready | State::Blocked
                ) {
                    self.finish(position, NodeStatus::Skipped, false, false);
                    steps.push(self.skip_step(position, SkipReason::FailFast));
                }
            }
            self.ready.clear();
            self.blocked.clear();
            return steps;
        }

        while let Some(position) = self.blocked.pop_first() {
            // A node skipped for a failed upstream is itself failed, so its
            // dependents are skipped in turn, and the run counts as failed.
            self.had_failure = true;
            self.finish(position, NodeStatus::Skipped, true, true);
            steps.push(self.skip_step(position, SkipReason::UpstreamFailed));
        }

        for _ in 0..free_slots {
            let Some((_, position)) = self.ready.pop_first() else {
                break;
            };
            self.slots[position].state = State::Running;
            self.counts.running += 1;
            steps.push(Step::Start(self.slots[position].id.clone()));
        }
        steps
    }

    fn skip_step(&self, position: usize, reason: SkipReason) -> Step {
        Step::Skip {
            node: self.slots[position].id.clone(),
            reason,
        }
    }

    /// Close out every node that has not finished — running, ready and waiting
    /// alike — as cancelled. Returns their ids in plan order.
    pub fn cancel_unfinished(&mut self) -> Vec<String> {
        let mut cancelled = Vec::new();
        for position in 0..self.slots.len() {
            if !matches!(self.slots[position].state, State::Done(_)) {
                self.finish(position, NodeStatus::Cancelled, false, false);
                cancelled.push(self.slots[position].id.clone());
            }
        }
        self.ready.clear();
        self.blocked.clear();
        cancelled
    }

    /// Whether every node has reached a terminal status.
    pub const fn is_finished(&self) -> bool {
        self.unfinished == 0
    }

    pub const fn had_failure(&self) -> bool {
        self.had_failure
    }

    /// Every failed node, including any carried in that this plan does not schedule.
    pub fn failed_nodes(&self) -> BTreeSet<String> {
        self.slots
            .iter()
            .filter(|s| s.failed)
            .map(|s| s.id.clone())
            .chain(self.foreign_failed.iter().cloned())
            .collect()
    }

    /// How many leading levels are entirely terminal.
    pub const fn completed_levels(&self) -> usize {
        self.completed_levels
    }

    pub const fn counts(&self) -> StatusCounts {
        self.counts
    }

    /// The node's terminal status, or `None` while it has not finished (or is
    /// not in the plan).
    pub fn terminal_status(&self, node: &str) -> Option<NodeStatus> {
        let position = *self.index.get(node)?;
        match self.slots[position].state {
            State::Done(status) => Some(status),
            _ => None,
        }
    }
}

impl StatusCounts {
    const fn record(&mut self, status: NodeStatus) {
        match status {
            NodeStatus::Success => self.success += 1,
            NodeStatus::Warn => self.warn += 1,
            NodeStatus::Error => self.error += 1,
            NodeStatus::Skipped | NodeStatus::Cancelled => self.skipped += 1,
            // A terminal state is never one of these; counted nowhere rather
            // than invented a bucket.
            NodeStatus::Pending | NodeStatus::Running => {}
        }
    }
}
