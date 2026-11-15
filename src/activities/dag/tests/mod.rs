use std::sync::Arc;

use dbt_common::CodeLocationWithFile;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::{
    CommonAttributes, DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest, DbtUnitTest,
    NodeBaseAttributes,
};

use super::*;

mod dependency_map;
mod gate_reference;
mod gates;
mod levels;
mod unit_gates;

fn deps(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
    entries
        .iter()
        .map(|(node, node_deps)| {
            (node.to_string(), node_deps.iter().map(ToString::to_string).collect())
        })
        .collect()
}

fn selected(ids: &[&str]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

fn make_common(unique_id: &str) -> CommonAttributes {
    CommonAttributes {
        unique_id: unique_id.to_string(),
        name: unique_id
            .rsplit('.')
            .next()
            .unwrap_or(unique_id)
            .to_string(),
        ..Default::default()
    }
}

fn make_base(dep_ids: &[&str]) -> NodeBaseAttributes {
    NodeBaseAttributes {
        depends_on: NodeDependsOn {
            nodes_with_ref_location: dep_ids
                .iter()
                .map(|d| ((*d).to_string(), CodeLocationWithFile::default()))
                .collect(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn add_model(nodes: &mut Nodes, unique_id: &str, dep_ids: &[&str]) {
    let model = DbtModel {
        __common_attr__: make_common(unique_id),
        __base_attr__: make_base(dep_ids),
        ..Default::default()
    };
    nodes.models.insert(unique_id.to_string(), Arc::new(model));
}

fn add_test(nodes: &mut Nodes, unique_id: &str, dep_ids: &[&str]) {
    let test = DbtTest {
        __common_attr__: make_common(unique_id),
        __base_attr__: make_base(dep_ids),
        ..Default::default()
    };
    nodes.tests.insert(unique_id.to_string(), Arc::new(test));
}

fn add_unit_test_node(nodes: &mut Nodes, unique_id: &str, dep_ids: &[&str]) {
    let unit_test = DbtUnitTest {
        __common_attr__: make_common(unique_id),
        __base_attr__: make_base(dep_ids),
        ..Default::default()
    };
    nodes
        .unit_tests
        .insert(unique_id.to_string(), Arc::new(unit_test));
}

fn add_seed(nodes: &mut Nodes, unique_id: &str) {
    let seed = DbtSeed {
        __common_attr__: make_common(unique_id),
        __base_attr__: make_base(&[]),
        ..Default::default()
    };
    nodes.seeds.insert(unique_id.to_string(), Arc::new(seed));
}

fn add_source(nodes: &mut Nodes, unique_id: &str) {
    let source = DbtSource {
        __common_attr__: make_common(unique_id),
        __base_attr__: make_base(&[]),
        ..Default::default()
    };
    nodes
        .sources
        .insert(unique_id.to_string(), Arc::new(source));
}

fn add_snapshot(nodes: &mut Nodes, unique_id: &str, dep_ids: &[&str]) {
    let snapshot = DbtSnapshot {
        __common_attr__: make_common(unique_id),
        __base_attr__: make_base(dep_ids),
        ..Default::default()
    };
    nodes
        .snapshots
        .insert(unique_id.to_string(), Arc::new(snapshot));
}
