use super::*;

/// Selection tests drive `--select`/`--exclude` alone; the expansion hook
/// is exercised by the indirect-selection tests and by `plan_project`.
fn no_expansion(ids: Vec<String>, _: &LazyDependencyMaps<'_>) -> Vec<String> {
    ids
}

use std::sync::Arc;

use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::nodes::DbtModel;

use test_support::{
    exposure_node, generic_test_node, metric_node, model_node, model_with_code, model_with_config,
    singular_test_node, source_node, versioned_model_node,
};

/// A model carrying the fields the three-model project varies between nodes.
fn make_model(
    unique_id: &str,
    name: &str,
    package: &str,
    path: &str,
    tags: &[&str],
    fqn_extra: &[&str],
) -> Arc<DbtModel> {
    let mut model = (*model_node(unique_id, name)).clone();
    model.__common_attr__.package_name = package.to_string();
    model.__common_attr__.original_file_path = std::path::PathBuf::from(path).into();
    model.__common_attr__.tags = tags.iter().map(|tag| (*tag).to_string()).collect();
    model.__common_attr__.fqn = std::iter::once(package.to_string())
        .chain(fqn_extra.iter().map(|part| (*part).to_string()))
        .chain(std::iter::once(name.to_string()))
        .collect();
    Arc::new(model)
}

fn build_three_model_project() -> (Vec<String>, Nodes) {
    let mut nodes = Nodes {
        project_name: Some("shop".to_string()),
        ..Nodes::default()
    };
    nodes.models.insert(
        "model.shop.stg_customers".to_string(),
        make_model(
            "model.shop.stg_customers",
            "stg_customers",
            "shop",
            "models/staging/stg_customers.sql",
            &["nightly"],
            &["staging"],
        ),
    );
    nodes.models.insert(
        "model.shop.stg_orders".to_string(),
        make_model(
            "model.shop.stg_orders",
            "stg_orders",
            "shop",
            "models/staging/stg_orders.sql",
            &["nightly", "hourly"],
            &["staging"],
        ),
    );
    nodes.models.insert(
        "model.shop.customers".to_string(),
        make_model(
            "model.shop.customers",
            "customers",
            "shop",
            "models/customers.sql",
            &["hourly"],
            &[],
        ),
    );

    let ids = vec![
        "model.shop.stg_customers".to_string(),
        "model.shop.stg_orders".to_string(),
        "model.shop.customers".to_string(),
    ];
    (ids, nodes)
}

mod basics;
mod config_and_resources;
mod graph;
mod state;
