//! Plans and scheduler constructors shared by the scheduler tests.

use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::common::OnError;
use dbt_schemas::schemas::telemetry::NodeType;

use crate::activities::dag::topological_levels;
use crate::types::{ExecutionPlan, NodeInfo, NodeStatus, NodeStatusTree};
use crate::workflow::scheduler::{Scheduler, SchedulerError, SkipReason, Step};

/// What a test says about one node besides its edges.
#[derive(Clone, Default)]
pub(super) struct Spec {
    pub(super) deps: Vec<String>,
    pub(super) on_error: Option<OnError>,
    pub(super) priority: Option<u32>,
}

pub(super) fn spec(deps: &[&str]) -> Spec {
    Spec {
        deps: deps.iter().map(ToString::to_string).collect(),
        ..Spec::default()
    }
}

/// A plan over `specs`, with levels computed the way the plan activity does.
pub(super) fn build_plan(specs: &BTreeMap<String, Spec>) -> ExecutionPlan {
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

pub(super) fn specs(edges: &[(&str, &[&str])]) -> BTreeMap<String, Spec> {
    edges
        .iter()
        .map(|(id, deps)| ((*id).to_string(), spec(deps)))
        .collect()
}

pub(super) fn pending_tree(plan: &ExecutionPlan) -> NodeStatusTree {
    NodeStatusTree {
        nodes: plan
            .nodes
            .keys()
            .map(|id| (id.clone(), NodeStatus::Pending))
            .collect(),
    }
}

pub(super) fn scheduler(specs: &BTreeMap<String, Spec>) -> Result<Scheduler, SchedulerError> {
    let plan = build_plan(specs);
    Scheduler::new(&plan, &pending_tree(&plan), &BTreeSet::new(), false)
}

pub(super) fn starts(steps: &[Step]) -> Vec<&str> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Start(id) => Some(id.as_str()),
            Step::Skip { .. } => None,
        })
        .collect()
}

pub(super) fn skips(steps: &[Step]) -> Vec<(&str, SkipReason)> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Skip { node, reason } => Some((node.as_str(), *reason)),
            Step::Start(_) => None,
        })
        .collect()
}

pub(super) const WIDE: usize = 1_000;
