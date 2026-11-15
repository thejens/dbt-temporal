use dbt_schemas::schemas::telemetry::NodeType;

use crate::types::ExecutionPlan;

/// Build the per-node Temporal activity label, shown as the `summary` in the
/// UI. Pure — uses only the plan's nodemap.
///
/// Format: `<resource_type>:<name>` with the proto enum prefix stripped, so
/// `NODE_TYPE_MODEL` → `model`. Returns `None` when the id isn't in the plan,
/// matching the behaviour of `execute_activity` (no label).
///
/// The label is not unique — the same name can occur in two packages, in two
/// versions of a model, or on tests — so it is a display string, never an
/// identity. See [`format_activity_id`].
pub fn format_activity_label(plan: &ExecutionPlan, unique_id: &str) -> Option<String> {
    plan.nodes
        .get(unique_id)
        .map(|info| format!("{}:{}", resource_type_label(info.resource_type), info.name))
}

/// The Temporal activity id for a node.
///
/// Temporal requires activity ids to be unique among a workflow's open
/// activities, and the SDK matches an activity to its recorded history by id
/// and type. `unique_id` is unique per node in the manifest by construction —
/// it carries the package, the version of a versioned model and a test's hash —
/// and is how dbt itself names a node in its logs, so it is both collision-free
/// and readable. A scheme that kept the short label where it happened to be
/// unambiguous would make a node's id depend on which other nodes the plan
/// holds.
///
/// With `unique` false this is the short label, which only histories recorded
/// before [`UNIQUE_ACTIVITY_ID_PATCH`] still use. Returns `None` for an id the
/// plan does not contain, as the label does.
pub fn format_activity_id(plan: &ExecutionPlan, unique_id: &str, unique: bool) -> Option<String> {
    if unique {
        plan.nodes
            .contains_key(unique_id)
            .then(|| unique_id.to_string())
    } else {
        format_activity_label(plan, unique_id)
    }
}

/// The lower-case resource type as dbt's CLI spells it (`model`, `unit_test`):
/// the protobuf enum name without its `NODE_TYPE_` prefix.
pub(super) fn resource_type_label(resource_type: NodeType) -> String {
    let name = resource_type.as_str_name();
    name.strip_prefix("NODE_TYPE_")
        .unwrap_or(name)
        .to_ascii_lowercase()
}

/// Build a human-readable label for a node, e.g. "table model waffle_hut.stg_customers".
pub fn node_label(plan: &ExecutionPlan, unique_id: &str) -> String {
    plan.nodes.get(unique_id).map_or_else(
        || unique_id.to_string(),
        |info| {
            let rt = resource_type_label(info.resource_type);
            let mat = info
                .materialization
                .as_deref()
                .map_or_else(String::new, |m| format!("{m} "));
            format!("{mat}{rt} {}.{}", info.package_name, info.name)
        },
    )
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
    fn node_label_with_materialization() {
        let plan =
            test_plan(vec![vec!["stg_customers"]], &[("stg_customers", Some("table"), vec![])]);
        let label = node_label(&plan, "model.waffle.stg_customers");
        assert_eq!(label, "table model waffle.stg_customers");
    }

    #[test]
    fn node_label_without_materialization() {
        let plan = test_plan(vec![vec!["stg_customers"]], &[("stg_customers", None, vec![])]);
        let label = node_label(&plan, "model.waffle.stg_customers");
        assert_eq!(label, "model waffle.stg_customers");
    }

    #[test]
    fn node_label_unknown_falls_back_to_id() {
        let plan = test_plan(vec![], &[]);
        let label = node_label(&plan, "model.waffle.unknown");
        assert_eq!(label, "model.waffle.unknown");
    }

    // --- format_activity_label ---

    #[test]
    fn format_activity_label_strips_node_type_prefix_and_lowercases() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", Some("table"), vec![])]);
        assert_eq!(format_activity_label(&plan, "model.waffle.m1"), Some("model:m1".to_string()));
    }

    #[test]
    fn format_activity_label_spells_multi_word_types_with_underscores() {
        let mut plan = test_plan(vec![vec!["t1"]], &[("t1", None, vec![])]);
        plan.nodes.get_mut("model.waffle.t1").unwrap().resource_type = NodeType::UnitTest;
        assert_eq!(
            format_activity_label(&plan, "model.waffle.t1"),
            Some("unit_test:t1".to_string())
        );
    }

    #[test]
    fn node_label_prefixes_materialization_and_lowercases_the_type() {
        let mut plan = test_plan(vec![vec!["m1"]], &[("m1", Some("table"), vec![])]);
        assert_eq!(node_label(&plan, "model.waffle.m1"), "table model waffle.m1");
        plan.nodes.get_mut("model.waffle.m1").unwrap().resource_type = NodeType::Seed;
        assert_eq!(node_label(&plan, "model.waffle.m1"), "table seed waffle.m1");
    }

    /// Two packages may each define a model of the same name; the label cannot
    /// tell them apart but the activity id has to.
    #[test]
    fn format_activity_id_distinguishes_nodes_that_share_a_label() {
        let mut plan = test_plan(vec![vec!["m1"]], &[("m1", None, vec![])]);
        let mut other = plan.nodes["model.waffle.m1"].clone();
        other.unique_id = "model.other.m1".to_string();
        other.package_name = "other".to_string();
        plan.nodes.insert(other.unique_id.clone(), other);

        let first = "model.waffle.m1";
        let second = "model.other.m1";
        assert_eq!(format_activity_label(&plan, first), format_activity_label(&plan, second));
        assert_eq!(format_activity_id(&plan, first, true), Some(first.to_string()));
        assert_eq!(format_activity_id(&plan, second, true), Some(second.to_string()));
    }

    #[test]
    fn format_activity_id_is_the_short_label_when_not_unique() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", None, vec![])]);
        assert_eq!(
            format_activity_id(&plan, "model.waffle.m1", false),
            Some("model:m1".to_string())
        );
    }

    #[test]
    fn format_activity_id_returns_none_for_unknown_id() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", None, vec![])]);
        assert!(format_activity_id(&plan, "model.waffle.nope", true).is_none());
        assert!(format_activity_id(&plan, "model.waffle.nope", false).is_none());
    }

    #[test]
    fn format_activity_label_returns_none_for_unknown_id() {
        let plan = test_plan(vec![vec!["m1"]], &[("m1", None, vec![])]);
        assert!(format_activity_label(&plan, "model.waffle.nope").is_none());
    }
}
