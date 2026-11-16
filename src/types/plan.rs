use dbt_schemas::schemas::common::OnError;
use dbt_schemas::schemas::telemetry::NodeType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::HooksConfig;

/// Output of the plan_project activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // Wire type: independent serde flags, not a state machine.
pub struct ExecutionPlan {
    /// Resolved project name — always set by the plan activity.
    pub project: String,
    /// Topological levels — each level is a vec of unique_ids that can run in parallel.
    pub levels: Vec<Vec<String>>,
    /// Metadata per node, keyed by unique_id.
    pub nodes: BTreeMap<String, NodeInfo>,
    /// Inline manifest JSON. Planning stores the manifest and references it by
    /// `manifest_ref` instead, but a plan replayed from a recorded history may
    /// still carry the manifest inline, and `store_artifacts` must honour it.
    pub manifest_json: Option<String>,
    /// Artifact store reference for the manifest stored during planning.
    pub manifest_ref: Option<String>,
    /// Run identifier: the Temporal run id of the execution that planned (a
    /// random UUID when planning runs outside a Temporal context). It names
    /// the run's artifacts, keys its segment checkpoints and is carried
    /// unchanged through continue-as-new, so it differs from the workflow id.
    pub invocation_id: String,
    /// Search attributes to upsert on the workflow (static config + dynamic).
    #[serde(default)]
    pub search_attributes: BTreeMap<String, String>,
    /// Whether artifact writing is enabled for this run.
    #[serde(default)]
    pub write_artifacts: bool,
    /// Whether dbt_project.yml (or any loaded package) defines on-run-start hooks.
    #[serde(default)]
    pub has_on_run_start: bool,
    /// Whether dbt_project.yml (or any loaded package) defines on-run-end hooks.
    #[serde(default)]
    pub has_on_run_end: bool,
    /// Whether activities carry priority/fairness keys (worker config;
    /// requires Temporal server >= 1.31 to take effect).
    #[serde(default)]
    pub priority_scheduling: bool,
    /// Whether the project declares any check under `checks/`.
    ///
    /// The workflow schedules the check gate only when this is set, so a
    /// project with no checks issues no command for it — which is also what
    /// keeps histories recorded before the gate existed replaying cleanly.
    #[serde(default)]
    pub has_project_checks: bool,
}

/// Metadata about a single dbt node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub unique_id: String,
    pub name: String,
    /// Serialized as dbt's protobuf enum name (`NODE_TYPE_MODEL`,
    /// `NODE_TYPE_TEST`, ...), the spelling workflow histories already carry.
    pub resource_type: NodeType,
    /// "table", "view", "incremental", "ephemeral"
    pub materialization: Option<String>,
    pub package_name: String,
    pub depends_on: Vec<String>,
    /// Scheduling priority key (1 = highest .. 5 = lowest) from critical-path
    /// depth. Only set when priority scheduling is enabled on the worker.
    #[serde(default)]
    pub priority: Option<u32>,
    /// The model's `on_error` config (`continue` or `skip_children`), if set.
    /// Only models can set this; `None` for every other resource type and for
    /// models that don't override the default (propagate failure downstream).
    #[serde(default)]
    pub on_error: Option<OnError>,
}

/// Input to the resolve_config activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveConfigInput {
    /// Project name to look up in the registry.
    pub project: Option<String>,
    /// Hooks from the workflow input (full override if present).
    pub input_hooks: Option<HooksConfig>,
}

/// Output of the resolve_config activity: hooks + retry policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedProjectConfig {
    pub hooks: HooksConfig,
    pub retry: super::hooks::RetryConfig,
    #[serde(default)]
    pub timeouts: super::hooks::TimeoutConfig,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Every resource type dbt can tag a node with, beside the string a plan
    /// recorded in a workflow history spells it as.
    const RESOURCE_TYPE_WIRE: [(NodeType, &str); 17] = [
        (NodeType::Unspecified, "NODE_TYPE_UNSPECIFIED"),
        (NodeType::Model, "NODE_TYPE_MODEL"),
        (NodeType::Seed, "NODE_TYPE_SEED"),
        (NodeType::Snapshot, "NODE_TYPE_SNAPSHOT"),
        (NodeType::Source, "NODE_TYPE_SOURCE"),
        (NodeType::Test, "NODE_TYPE_TEST"),
        (NodeType::UnitTest, "NODE_TYPE_UNIT_TEST"),
        (NodeType::Macro, "NODE_TYPE_MACRO"),
        (NodeType::DocsMacro, "NODE_TYPE_DOCS_MACRO"),
        (NodeType::Analysis, "NODE_TYPE_ANALYSIS"),
        (NodeType::Operation, "NODE_TYPE_OPERATION"),
        (NodeType::Exposure, "NODE_TYPE_EXPOSURE"),
        (NodeType::Metric, "NODE_TYPE_METRIC"),
        (NodeType::SavedQuery, "NODE_TYPE_SAVED_QUERY"),
        (NodeType::SemanticModel, "NODE_TYPE_SEMANTIC_MODEL"),
        (NodeType::Function, "NODE_TYPE_FUNCTION"),
        (NodeType::Check, "NODE_TYPE_CHECK"),
    ];

    fn node_info_json(resource_type: &str) -> serde_json::Value {
        serde_json::json!({
            "unique_id": "model.shop.m",
            "name": "m",
            "resource_type": resource_type,
            "materialization": "table",
            "package_name": "shop",
            "depends_on": [],
            "priority": null,
            "on_error": null,
        })
    }

    #[test]
    fn node_info_resource_type_serializes_to_the_recorded_strings() {
        for (resource_type, wire) in RESOURCE_TYPE_WIRE {
            let info = NodeInfo {
                unique_id: "model.shop.m".into(),
                name: "m".into(),
                resource_type,
                materialization: Some("table".into()),
                package_name: "shop".into(),
                depends_on: vec![],
                priority: None,
                on_error: None,
            };
            assert_eq!(serde_json::to_value(&info).unwrap(), node_info_json(wire), "{wire}");
        }
    }

    #[test]
    fn node_info_recorded_with_string_resource_type_deserializes() {
        for (resource_type, wire) in RESOURCE_TYPE_WIRE {
            let info: NodeInfo = serde_json::from_value(node_info_json(wire)).unwrap();
            assert_eq!(info.resource_type, resource_type, "{wire}");
        }
    }

    #[test]
    fn node_info_on_error_serializes_to_the_recorded_strings() {
        for (on_error, wire) in [
            (OnError::Continue, "continue"),
            (OnError::SkipChildren, "skip_children"),
        ] {
            let mut json = node_info_json("NODE_TYPE_MODEL");
            json["on_error"] = serde_json::json!(wire);
            let info: NodeInfo = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(info.on_error.as_ref(), Some(&on_error), "{wire}");
            assert_eq!(serde_json::to_value(&info).unwrap(), json, "{wire}");
        }
    }

    #[test]
    fn node_info_without_on_error_deserializes_as_unset() {
        let mut json = node_info_json("NODE_TYPE_MODEL");
        json.as_object_mut().unwrap().remove("on_error");
        let info: NodeInfo = serde_json::from_value(json).unwrap();
        assert!(info.on_error.is_none());
    }

    #[test]
    fn node_info_with_an_unknown_resource_type_is_rejected() {
        // The planner only ever writes dbt's own enum names, so anything else
        // is a corrupt payload rather than a newer dbt's type to carry along.
        assert!(serde_json::from_value::<NodeInfo>(node_info_json("model")).is_err());
    }

    #[test]
    fn resolved_project_config_round_trip() -> anyhow::Result<()> {
        use super::super::hooks::RetryConfig;
        let config = ResolvedProjectConfig {
            timeouts: crate::types::TimeoutConfig::default(),
            hooks: HooksConfig::default(),
            retry: RetryConfig::default(),
        };
        let json = serde_json::to_string(&config)?;
        let back: ResolvedProjectConfig = serde_json::from_str(&json)?;
        assert!(back.hooks.pre_run.is_empty());
        assert_eq!(back.retry.max_attempts, 3);
        Ok(())
    }
}
