//! Fixtures shared by the phase tests.

use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;

use crate::types::{DbtRunInput, ExecutionPlan, NodeInfo};

pub(super) fn empty_plan() -> ExecutionPlan {
    ExecutionPlan {
        project: "shop".to_string(),
        levels: vec![],
        nodes: BTreeMap::new(),
        manifest_json: None,
        manifest_ref: None,
        invocation_id: "inv-42".to_string(),
        search_attributes: BTreeMap::new(),
        write_artifacts: false,
        has_on_run_start: false,
        has_on_run_end: false,
        priority_scheduling: false,
        has_project_checks: false,
        warnings: vec![],
    }
}

pub(super) fn node_info(name: &str) -> NodeInfo {
    NodeInfo {
        unique_id: format!("model.shop.{name}"),
        name: name.to_string(),
        resource_type: NodeType::Model,
        materialization: Some("table".to_string()),
        package_name: "shop".to_string(),
        depends_on: vec![],
        priority: None,
        on_error: None,
    }
}

pub(super) fn empty_input() -> DbtRunInput {
    DbtRunInput {
        project: Some("shop".to_string()),
        indirect_selection: None,
        resource_types: Vec::new(),
        exclude_resource_types: Vec::new(),
        resume_from: None,
        command: "build".to_string(),
        select: None,
        exclude: None,
        vars: BTreeMap::new(),
        target: None,
        full_refresh: false,
        fail_fast: false,
        hooks: None,
        env: BTreeMap::new(),
        defer_manifest_ref: None,
        event_time_start: None,
        event_time_end: None,
        retry_from: None,
        state_manifest_ref: None,
    }
}
