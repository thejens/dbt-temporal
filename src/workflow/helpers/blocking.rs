use dbt_schemas::schemas::common::OnError;
use dbt_schemas::schemas::telemetry::NodeType;

use crate::types::{ExecutionPlan, NodeStatus, NodeStatusTree};

/// True if `unique_id` has any upstream dependency in `failed_nodes` whose
/// failure blocks it. Used by the per-level scheduler to skip a node whose
/// upstream failed.
///
/// A failed dependency blocks unless it carries `on_error: continue`; see
/// [`failure_blocks_dependents`] for the rest of the rule. `statuses` says which
/// failed dependencies were skipped rather than errored, and `transitive`
/// selects whether that matters ([`TRANSITIVE_SKIP_PATCH`]).
///
/// `--fail-fast` isn't threaded through here: the caller (`execute_one_level`)
/// skips the whole level before this runs whenever fail-fast is in effect and
/// a failure has been recorded, so a node's `on_error: continue` exemption is
/// only ever consulted when fail-fast would not have stopped the run anyway.
/// `failed_nodes` is not empty in that case; it simply never reaches here.
pub fn is_blocked_by_failed_upstream(
    plan: &ExecutionPlan,
    unique_id: &str,
    failed_nodes: &std::collections::BTreeSet<String>,
    statuses: &NodeStatusTree,
    transitive: bool,
) -> bool {
    plan.nodes.get(unique_id).is_some_and(|info| {
        info.depends_on.iter().any(|dep| {
            let skipped = transitive && statuses.nodes.get(dep) == Some(&NodeStatus::Skipped);
            failed_nodes.contains(dep)
                && failure_blocks_dependents(dep_continues_on_error(plan, dep), skipped)
        })
    })
}

/// Whether a failed node's dependents must be skipped.
///
/// Mirrors dbt-core, which on a failure marks every descendant of the failed
/// node as skipped — unless the node is a model with `on_error: continue`, in
/// which case its descendants run as if it had succeeded. A node that was itself
/// skipped because an ancestor failed counts as failed, and its own
/// `on_error: continue` is irrelevant: the ancestor that failed already marked
/// the whole subtree, so the skip has to carry through it. Both schedulers ask
/// this one question.
pub const fn failure_blocks_dependents(continues_on_error: bool, skipped: bool) -> bool {
    skipped || !continues_on_error
}

/// True when `dep`'s failure shouldn't propagate to its dependents — it's a
/// model configured with `on_error: continue`.
pub fn dep_continues_on_error(plan: &ExecutionPlan, dep_unique_id: &str) -> bool {
    plan.nodes.get(dep_unique_id).is_some_and(|info| {
        info.resource_type == NodeType::Model && info.on_error == Some(OnError::Continue)
    })
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
    use crate::workflow::helpers::{build_node_status_tree, set_node_status};

    // --- is_blocked_by_failed_upstream ---

    #[test]
    fn is_blocked_by_failed_upstream_true_when_any_dep_failed() {
        let plan = test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        assert!(is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.b",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }

    #[test]
    fn is_blocked_by_failed_upstream_false_when_no_dep_failed() {
        let plan = test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        let failed = std::collections::BTreeSet::new();
        assert!(!is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.b",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }

    #[test]
    fn is_blocked_by_failed_upstream_false_for_unknown_id() {
        let plan = test_plan(vec![vec!["a"]], &[("a", None, vec![])]);
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        // An id that isn't in the plan can't be blocked — the level loop will
        // simply skip it. Returning false here keeps the contract coherent.
        assert!(!is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.nope",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }

    /// Patch a `test_plan` node's `resource_type`/`on_error` in place — `test_plan`
    /// hardcodes every node as a plain model with no `on_error`, so tests that need
    /// a specific combination mutate the built plan directly.
    fn set_on_error(
        plan: &mut ExecutionPlan,
        unique_id: &str,
        resource_type: NodeType,
        on_error: Option<OnError>,
    ) {
        let info = plan.nodes.get_mut(unique_id).unwrap();
        info.resource_type = resource_type;
        info.on_error = on_error;
    }

    #[test]
    fn is_blocked_by_failed_upstream_false_when_failed_dep_continues_on_error() {
        let mut plan =
            test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        set_on_error(&mut plan, "model.waffle.a", NodeType::Model, Some(OnError::Continue));
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        assert!(!is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.b",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }

    #[test]
    fn is_blocked_by_failed_upstream_true_when_only_one_of_two_failed_deps_continues_on_error() {
        let mut plan = test_plan(
            vec![vec!["a", "b", "c"]],
            &[
                ("a", None, vec![]),
                ("b", None, vec![]),
                ("c", None, vec!["a", "b"]),
            ],
        );
        set_on_error(&mut plan, "model.waffle.a", NodeType::Model, Some(OnError::Continue));
        // b has no on_error override — its failure should still block c.
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        failed.insert("model.waffle.b".to_string());
        assert!(is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.c",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }

    #[test]
    fn is_blocked_by_failed_upstream_true_when_failed_dep_has_skip_children() {
        // Only `on_error: continue` is exempted — `skip_children` isn't handled
        // by our scheduler (dbt-core itself only wires up `continue` today).
        let mut plan =
            test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        set_on_error(&mut plan, "model.waffle.a", NodeType::Model, Some(OnError::SkipChildren));
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        assert!(is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.b",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }

    /// A model with `on_error: continue` that was skipped for an upstream failure
    /// is in the failed set, but it must not let its own dependents through.
    fn skipped_continue_dep() -> (ExecutionPlan, std::collections::BTreeSet<String>, NodeStatusTree)
    {
        let mut plan = test_plan(
            vec![vec!["a"], vec!["b"], vec!["c"]],
            &[
                ("a", None, vec![]),
                ("b", None, vec!["a"]),
                ("c", None, vec!["b"]),
            ],
        );
        set_on_error(&mut plan, "model.waffle.b", NodeType::Model, Some(OnError::Continue));
        let failed = ["model.waffle.a", "model.waffle.b"]
            .map(String::from)
            .into();
        let mut statuses = build_node_status_tree(&plan);
        set_node_status(&mut statuses, "model.waffle.a", NodeStatus::Error);
        set_node_status(&mut statuses, "model.waffle.b", NodeStatus::Skipped);
        (plan, failed, statuses)
    }

    #[test]
    fn is_blocked_by_failed_upstream_true_when_failed_dep_was_skipped_and_continues() {
        let (plan, failed, statuses) = skipped_continue_dep();
        assert!(is_blocked_by_failed_upstream(&plan, "model.waffle.c", &failed, &statuses, true));
    }

    /// Without the patch the skipped model's `on_error: continue` exempts its
    /// dependents, which is what histories recorded before it scheduled.
    #[test]
    fn is_blocked_by_failed_upstream_ignores_skips_when_not_transitive() {
        let (plan, failed, statuses) = skipped_continue_dep();
        assert!(!is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.c",
            &failed,
            &statuses,
            false
        ));
    }

    #[test]
    fn failure_blocks_dependents_unless_an_errored_node_continues() {
        assert!(failure_blocks_dependents(false, false));
        assert!(failure_blocks_dependents(true, true));
        assert!(failure_blocks_dependents(false, true));
        assert!(!failure_blocks_dependents(true, false));
    }

    #[test]
    fn is_blocked_by_failed_upstream_true_when_failed_dep_is_not_a_model() {
        // on_error: continue only exempts models — a failed non-model dependency
        // (e.g. a seed) always blocks, even if on_error happened to be set.
        let mut plan =
            test_plan(vec![vec!["a", "b"]], &[("a", None, vec![]), ("b", None, vec!["a"])]);
        set_on_error(&mut plan, "model.waffle.a", NodeType::Seed, Some(OnError::Continue));
        let mut failed = std::collections::BTreeSet::new();
        failed.insert("model.waffle.a".to_string());
        assert!(is_blocked_by_failed_upstream(
            &plan,
            "model.waffle.b",
            &failed,
            &build_node_status_tree(&plan),
            true
        ));
    }
}
