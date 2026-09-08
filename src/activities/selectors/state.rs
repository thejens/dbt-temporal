//! The `state:` selector methods, backed by a comparison against a previous
//! `manifest.json`.

use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::nodes::InternalDbtNodeAttributes;

/// Top-level `manifest.json` maps that hold selectable nodes.
///
/// Every node `Nodes::iter` yields is written to one of these. Reading all of
/// them is what keeps a source or an exposure from looking new on every run
/// merely because the comparison only opened `nodes`.
const MANIFEST_NODE_SECTIONS: [&str; 8] = [
    "nodes",
    "sources",
    "unit_tests",
    "exposures",
    "metrics",
    "semantic_models",
    "saved_queries",
    "functions",
];

/// State-comparison sets backing `state:` selector methods.
///
/// Built by comparing the current project against a previous manifest.json
/// (`DbtRunInput.state_manifest_ref`).
#[derive(Debug, Default, Clone)]
pub struct StateSelector {
    /// Nodes absent from the previous manifest.
    pub new: BTreeSet<String>,
    /// New nodes plus nodes whose raw_code / checksum changed.
    pub modified: BTreeSet<String>,
    /// Nodes the previous manifest also knew about, whether or not they changed.
    pub existing: BTreeSet<String>,
}

impl StateSelector {
    /// Compare current nodes against a previous dbt manifest.
    ///
    /// A node is modified when it is new, or when its verbatim `raw_code`
    /// differs, or (when raw code is unavailable on either side) its file
    /// checksum differs. Nodes where neither comparison is possible count as
    /// modified — rebuilding too much is safer than silently skipping a
    /// changed node.
    ///
    /// Only bodies and checksums. dbt's `state:modified` is the union of that
    /// with config, relation, contract, description and macro changes, so this
    /// selects a subset — `StateSet::parse` warns about it, and refuses the
    /// `modified.<sub>` forms that name a dimension not read here.
    ///
    /// `modified` and `existing` partition the same way `state:modified` and
    /// `state:unmodified` do in dbt, so the conservatism above lands on the
    /// same side of both selectors.
    pub fn from_previous_manifest(nodes: &Nodes, previous_manifest: &serde_json::Value) -> Self {
        let previous: BTreeMap<&str, &serde_json::Value> = MANIFEST_NODE_SECTIONS
            .iter()
            .filter_map(|section| previous_manifest.get(section))
            .filter_map(serde_json::Value::as_object)
            .flat_map(|section| section.iter().map(|(id, node)| (id.as_str(), node)))
            .collect();

        let mut state = Self::default();
        for (unique_id, node) in nodes.iter() {
            let Some(prev) = previous.get(unique_id.as_str()) else {
                state.new.insert(unique_id.clone());
                state.modified.insert(unique_id.clone());
                continue;
            };
            state.existing.insert(unique_id.clone());
            if node_is_modified(node, prev) {
                state.modified.insert(unique_id.clone());
            }
        }
        state
    }
}

/// Stand-in dbt-fusion puts in `raw_code` when it did not load a node's body.
///
/// Two of these are equal to each other and to nothing real, so comparing them
/// answers "unmodified" for every such node — including one whose checksum has
/// moved. Both sides have to hold actual source before the body comparison
/// means anything.
const RAW_CODE_PLACEHOLDER: &str = "--placeholder--";

/// Raw code worth comparing, or `None` when the node carries no real body.
fn comparable_raw_code(raw: Option<&str>) -> Option<&str> {
    raw.filter(|s| !s.is_empty() && *s != RAW_CODE_PLACEHOLDER)
}

/// Compare one current node against its previous-manifest entry.
fn node_is_modified(node: &dyn InternalDbtNodeAttributes, prev: &serde_json::Value) -> bool {
    use dbt_schemas::schemas::common::DbtChecksum;

    if let (Some(cur_raw), Some(prev_raw)) = (
        comparable_raw_code(node.common().raw_code.as_deref()),
        comparable_raw_code(prev.get("raw_code").and_then(serde_json::Value::as_str)),
    ) {
        return cur_raw != prev_raw;
    }

    let cur_checksum = match &node.common().checksum {
        DbtChecksum::String(s) => s.clone(),
        DbtChecksum::Object(o) => o.checksum.clone(),
    };
    let prev_checksum = match prev.get("checksum") {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Object(o)) => o
            .get("checksum")
            .and_then(serde_json::Value::as_str)
            .map(String::from),
        _ => None,
    };
    match prev_checksum {
        // Empty checksums (e.g. generic tests use FileHash.empty()) carry no
        // signal — treat as modified rather than silently matching.
        Some(prev_cs) if !prev_cs.is_empty() && !cur_checksum.is_empty() => cur_checksum != prev_cs,
        _ => true,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use dbt_schemas::schemas::common::{DbtChecksum, DbtChecksumObject};
    use dbt_schemas::schemas::nodes::DbtModel;

    use crate::activities::selectors::test_support::{model_with_code, source_node};

    fn state_nodes() -> Nodes {
        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.unchanged".to_string(),
            model_with_code("model.shop.unchanged", "unchanged", "select 1"),
        );
        nodes.models.insert(
            "model.shop.edited".to_string(),
            model_with_code("model.shop.edited", "edited", "select 2 -- edited"),
        );
        nodes.models.insert(
            "model.shop.brand_new".to_string(),
            model_with_code("model.shop.brand_new", "brand_new", "select 3"),
        );
        nodes
    }

    fn previous_manifest() -> serde_json::Value {
        serde_json::json!({
            "nodes": {
                "model.shop.unchanged": {"raw_code": "select 1"},
                "model.shop.edited": {"raw_code": "select 2"},
                "model.shop.removed": {"raw_code": "select 0"},
            }
        })
    }

    #[test]
    fn classifies_new_modified_and_existing() {
        let state = StateSelector::from_previous_manifest(&state_nodes(), &previous_manifest());

        assert!(state.new.contains("model.shop.brand_new"));
        assert!(!state.new.contains("model.shop.edited"));

        assert!(state.modified.contains("model.shop.brand_new"));
        assert!(state.modified.contains("model.shop.edited"));
        assert!(!state.modified.contains("model.shop.unchanged"));

        assert!(state.existing.contains("model.shop.unchanged"));
        assert!(state.existing.contains("model.shop.edited"));
        assert!(!state.existing.contains("model.shop.brand_new"));
        // A node the previous manifest had and the project no longer defines is
        // not in any set — there is nothing left to select.
        assert!(!state.existing.contains("model.shop.removed"));
    }

    /// Sources, exposures and the rest live outside `nodes` in a manifest, so a
    /// comparison that only read `nodes` would call every one of them new.
    #[test]
    fn reads_every_node_bearing_manifest_section() {
        let mut nodes = Nodes::default();
        nodes.sources.insert(
            "source.shop.raw.orders".to_string(),
            source_node("source.shop.raw.orders", "orders", "raw"),
        );

        let manifest = serde_json::json!({
            "nodes": {},
            "sources": {"source.shop.raw.orders": {"checksum": "abc"}},
        });
        let state = StateSelector::from_previous_manifest(&nodes, &manifest);
        assert!(state.existing.contains("source.shop.raw.orders"));
        assert!(!state.new.contains("source.shop.raw.orders"));
    }

    #[test]
    fn falls_back_to_checksum_when_raw_code_is_missing() {
        let mut nodes = Nodes::default();
        let mut model = model_with_code("model.shop.csum", "csum", "");
        {
            let model = Arc::get_mut(&mut model).unwrap();
            model.__common_attr__.raw_code = None;
            model.__common_attr__.checksum = DbtChecksum::Object(DbtChecksumObject {
                name: "sha256".to_string(),
                checksum: "abc123".to_string(),
            });
        }
        nodes.models.insert("model.shop.csum".to_string(), model);

        let same = serde_json::json!({"nodes": {"model.shop.csum":
            {"checksum": {"name": "sha256", "checksum": "abc123"}}}});
        assert!(
            !StateSelector::from_previous_manifest(&nodes, &same)
                .modified
                .contains("model.shop.csum")
        );

        let changed = serde_json::json!({"nodes": {"model.shop.csum":
            {"checksum": {"name": "sha256", "checksum": "zzz999"}}}});
        assert!(
            StateSelector::from_previous_manifest(&nodes, &changed)
                .modified
                .contains("model.shop.csum")
        );
    }

    /// dbt-fusion writes `--placeholder--` into `raw_code` when it did not load
    /// a node's body. Two placeholders compare equal, so the body comparison
    /// used to answer "unmodified" for every such node and never look at the
    /// checksum that had actually moved — a CI build skipping a changed node.
    #[test]
    fn a_placeholder_body_falls_through_to_the_checksum() {
        let mut nodes = Nodes::default();
        let mut model = model_with_code("model.shop.ph", "ph", "--placeholder--");
        {
            let model = Arc::get_mut(&mut model).unwrap();
            model.__common_attr__.checksum = DbtChecksum::String("new-hash".to_string());
        }
        nodes.models.insert("model.shop.ph".to_string(), model);

        let previous = serde_json::json!({"nodes": {"model.shop.ph": {
            "raw_code": "--placeholder--",
            "checksum": "old-hash",
        }}});
        assert!(
            StateSelector::from_previous_manifest(&nodes, &previous)
                .modified
                .contains("model.shop.ph"),
            "a moved checksum behind two placeholders is still a modification"
        );

        let unchanged = serde_json::json!({"nodes": {"model.shop.ph": {
            "raw_code": "--placeholder--",
            "checksum": "new-hash",
        }}});
        assert!(
            !StateSelector::from_previous_manifest(&nodes, &unchanged)
                .modified
                .contains("model.shop.ph"),
            "and an unchanged checksum is still unmodified"
        );
    }

    /// An empty body says as little as a placeholder does.
    #[test]
    fn an_empty_body_falls_through_to_the_checksum() {
        let mut nodes = Nodes::default();
        let mut model = model_with_code("model.shop.empty", "empty", "");
        {
            let model = Arc::get_mut(&mut model).unwrap();
            model.__common_attr__.checksum = DbtChecksum::String("new-hash".to_string());
        }
        nodes.models.insert("model.shop.empty".to_string(), model);

        let previous = serde_json::json!({"nodes": {"model.shop.empty": {
            "raw_code": "",
            "checksum": "old-hash",
        }}});
        assert!(
            StateSelector::from_previous_manifest(&nodes, &previous)
                .modified
                .contains("model.shop.empty")
        );
    }

    fn model_with_checksum(checksum: DbtChecksum) -> DbtModel {
        let mut model = DbtModel::default();
        model.__common_attr__.raw_code = None;
        model.__common_attr__.checksum = checksum;
        model
    }

    #[test]
    fn node_is_modified_compares_checksums_when_raw_code_absent() {
        let model = model_with_checksum(DbtChecksum::String("abc".to_string()));
        // Same checksum, prev stored as a plain string.
        assert!(!node_is_modified(&model, &serde_json::json!({"checksum": "abc"})));
        // Same checksum, prev stored dbt-core style ({name, checksum}).
        assert!(!node_is_modified(
            &model,
            &serde_json::json!({"checksum": {"name": "sha256", "checksum": "abc"}})
        ));
        // Different checksum.
        assert!(node_is_modified(&model, &serde_json::json!({"checksum": "xyz"})));
        // Object-shaped current checksum.
        let model = model_with_checksum(DbtChecksum::Object(DbtChecksumObject {
            name: "sha256".to_string(),
            checksum: "abc".to_string(),
        }));
        assert!(!node_is_modified(&model, &serde_json::json!({"checksum": "abc"})));
    }

    #[test]
    fn node_is_modified_treats_empty_or_missing_checksums_as_modified() {
        let model = model_with_checksum(DbtChecksum::String("abc".to_string()));
        // No checksum in the previous manifest entry: no signal -> modified.
        assert!(node_is_modified(&model, &serde_json::json!({})));
        // Empty previous checksum (FileHash.empty()): conservative -> modified.
        assert!(node_is_modified(&model, &serde_json::json!({"checksum": ""})));
        // Empty current checksum: same.
        let empty = model_with_checksum(DbtChecksum::String(String::new()));
        assert!(node_is_modified(&empty, &serde_json::json!({"checksum": "abc"})));
    }
}
