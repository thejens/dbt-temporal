//! The per-node data a plan carries to the workflow.

use dbt_schemas::schemas::common::OnError;

use crate::types::NodeInfo;

/// Extract NodeInfo from Nodes for a given unique_id.
pub(super) fn build_node_info(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
) -> Option<NodeInfo> {
    let node = nodes.get_node(unique_id)?;
    let common = node.common();
    let base = node.base();

    Some(NodeInfo {
        unique_id: common.unique_id.clone(),
        name: common.name.clone(),
        resource_type: node.resource_type(),
        materialization: Some(base.materialized.to_string()),
        package_name: common.package_name.clone(),
        depends_on: base
            .depends_on
            .nodes_with_ref_location
            .iter()
            .map(|(id, _)| id.clone())
            .collect(),
        priority: None,
        on_error: model_on_error(node),
    })
}

/// Read a model's `on_error` config, if set.
/// Only `DbtModel` carries this field — every other resource type gets `None`.
fn model_on_error(
    node: &dyn dbt_schemas::schemas::nodes::InternalDbtNodeAttributes,
) -> Option<OnError> {
    use dbt_schemas::schemas::nodes::DbtModel;

    let model = node.as_any().downcast_ref::<DbtModel>()?;
    model.deprecated_config.on_error.clone()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use dbt_schemas::schemas::telemetry::NodeType;

    use super::*;

    #[test]
    fn build_node_info_returns_none_for_unknown_id() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        assert!(build_node_info(&nodes, "model.shop.nope").is_none());
    }

    #[test]
    fn build_node_info_extracts_fields_and_dependencies() {
        use std::sync::Arc;

        use dbt_common::CodeLocationWithFile;
        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::common::NodeDependsOn;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

        let mut nodes = Nodes::default();
        let common = CommonAttributes {
            unique_id: "model.shop.orders".to_string(),
            name: "orders".to_string(),
            package_name: "shop".to_string(),
            ..CommonAttributes::default()
        };
        let depends_on = NodeDependsOn {
            nodes_with_ref_location: vec![
                ("model.shop.stg_orders".to_string(), CodeLocationWithFile::default()),
                ("model.shop.stg_customers".to_string(), CodeLocationWithFile::default()),
            ],
            ..NodeDependsOn::default()
        };
        nodes.models.insert(
            "model.shop.orders".to_string(),
            Arc::new(DbtModel {
                __common_attr__: common,
                __base_attr__: NodeBaseAttributes {
                    depends_on,
                    ..NodeBaseAttributes::default()
                },
                ..DbtModel::default()
            }),
        );

        let info = build_node_info(&nodes, "model.shop.orders").expect("orders is in the registry");
        assert_eq!(info.unique_id, "model.shop.orders");
        assert_eq!(info.name, "orders");
        assert_eq!(info.package_name, "shop");
        assert_eq!(info.resource_type, NodeType::Model);
        assert_eq!(info.depends_on.len(), 2);
        assert!(
            info.depends_on
                .contains(&"model.shop.stg_orders".to_string())
        );
        assert!(
            info.depends_on
                .contains(&"model.shop.stg_customers".to_string())
        );
    }

    #[test]
    fn build_node_info_extracts_on_error() {
        use std::sync::Arc;

        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};
        use dbt_schemas::schemas::project::ModelConfig;

        for on_error in [OnError::Continue, OnError::SkipChildren] {
            let mut nodes = Nodes::default();
            nodes.models.insert(
                "model.shop.flaky".to_string(),
                Arc::new(DbtModel {
                    __common_attr__: CommonAttributes {
                        unique_id: "model.shop.flaky".to_string(),
                        name: "flaky".to_string(),
                        package_name: "shop".to_string(),
                        ..CommonAttributes::default()
                    },
                    deprecated_config: ModelConfig {
                        on_error: Some(on_error.clone()),
                        ..ModelConfig::default()
                    },
                    ..DbtModel::default()
                }),
            );

            let info =
                build_node_info(&nodes, "model.shop.flaky").expect("flaky is in the registry");
            assert_eq!(info.on_error, Some(on_error));
        }
    }

    #[test]
    fn build_node_info_on_error_is_none_when_unset() {
        use std::sync::Arc;

        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.plain".to_string(),
            Arc::new(DbtModel {
                __common_attr__: CommonAttributes {
                    unique_id: "model.shop.plain".to_string(),
                    name: "plain".to_string(),
                    package_name: "shop".to_string(),
                    ..CommonAttributes::default()
                },
                ..DbtModel::default()
            }),
        );

        let info = build_node_info(&nodes, "model.shop.plain").expect("plain is in the registry");
        assert_eq!(info.on_error, None);
    }

    #[test]
    fn build_node_info_on_error_is_none_for_non_model_nodes() {
        use std::sync::Arc;

        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtTest};

        let mut nodes = Nodes::default();
        nodes.tests.insert(
            "test.shop.not_null_id".to_string(),
            Arc::new(DbtTest {
                __common_attr__: CommonAttributes {
                    unique_id: "test.shop.not_null_id".to_string(),
                    name: "not_null_id".to_string(),
                    package_name: "shop".to_string(),
                    ..CommonAttributes::default()
                },
                ..DbtTest::default()
            }),
        );

        let info =
            build_node_info(&nodes, "test.shop.not_null_id").expect("test is in the registry");
        assert_eq!(info.resource_type, NodeType::Test);
        assert_eq!(info.on_error, None);
    }
}
