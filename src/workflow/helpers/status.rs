use std::collections::BTreeMap;

use crate::types::{ExecutionPlan, NodeStatus, NodeStatusTree};

/// Build the initial node status tree from the execution plan, with all nodes set to Pending.
pub fn build_node_status_tree(plan: &ExecutionPlan) -> NodeStatusTree {
    let mut nodes = BTreeMap::new();
    for level in &plan.levels {
        for unique_id in level {
            if plan.nodes.contains_key(unique_id) {
                nodes.insert(unique_id.clone(), NodeStatus::Pending);
            }
        }
    }
    NodeStatusTree { nodes }
}

pub fn set_node_status(tree: &mut NodeStatusTree, unique_id: &str, status: NodeStatus) {
    if let Some(entry) = tree.nodes.get_mut(unique_id) {
        *entry = status;
    }
}

#[cfg(test)]
// float_cmp: tests compare directly-assigned floats, not computed values.
// unwrap_used: unwrap is acceptable in tests for brevity.
#[allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap
)]
mod tests {
    use super::*;
    use crate::workflow::helpers::test_support::test_plan;

    #[test]
    fn build_node_status_tree_all_pending() {
        let plan = test_plan(
            vec![vec!["a", "b"], vec!["c"]],
            &[
                ("a", Some("table"), vec![]),
                ("b", Some("view"), vec![]),
                ("c", Some("table"), vec!["a", "b"]),
            ],
        );

        let tree = build_node_status_tree(&plan);
        assert_eq!(tree.nodes.len(), 3);
        for status in tree.nodes.values() {
            assert_eq!(*status, NodeStatus::Pending);
        }
    }

    #[test]
    fn set_node_status_updates_existing() {
        let plan = test_plan(vec![vec!["a"]], &[("a", None, vec![])]);
        let mut tree = build_node_status_tree(&plan);

        set_node_status(&mut tree, "model.waffle.a", NodeStatus::Running);
        assert_eq!(tree.nodes["model.waffle.a"], NodeStatus::Running);

        set_node_status(&mut tree, "model.waffle.a", NodeStatus::Success);
        assert_eq!(tree.nodes["model.waffle.a"], NodeStatus::Success);
    }

    #[test]
    fn set_node_status_noop_for_unknown() {
        let plan = test_plan(vec![vec!["a"]], &[("a", None, vec![])]);
        let mut tree = build_node_status_tree(&plan);
        set_node_status(&mut tree, "model.waffle.nonexistent", NodeStatus::Error);
        // No panic, tree unchanged.
        assert_eq!(tree.nodes.len(), 1);
    }
}
