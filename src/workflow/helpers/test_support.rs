//! Fixtures shared by the helper modules' unit tests.

use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;

use crate::types::{ExecutionPlan, NodeInfo};

pub(super) fn test_plan(
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
                resource_type: NodeType::Model,
                materialization: mat.map(ToString::to_string),
                package_name: "waffle".to_string(),
                depends_on: deps.iter().map(|d| format!("model.waffle.{d}")).collect(),
                priority: None,
                on_error: None,
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
        priority_scheduling: false,
        has_project_checks: false,
    }
}
