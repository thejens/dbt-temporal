//! The search attributes upserted on every workflow run.

use std::collections::BTreeMap;

use crate::types::RunOutcome;

/// Build the search-attribute map upserted on every workflow run.
///
/// Starts from the worker-startup static config, then layers the always-set
/// dynamic attributes (`DbtProject`, `DbtCommand`, `DbtTarget`, `DbtSelector`,
/// `DbtStatus`) — without overriding caller-provided values for the same keys.
/// `DbtStatus` starts at `running`; the workflow re-upserts it to the terminal
/// outcome (`passed`/`failed`/`skipped`/`cancelled`) when the run ends, so the
/// workflow list can be filtered by outcome. Finally filters to only attributes
/// registered on the namespace; unregistered ones are dropped with a
/// per-attribute warning so workflow task failures don't surprise the user.
///
/// All values are strings, matching the `Keyword`/`Text` search-attribute types
/// the upsert pipeline serializes. Numeric attributes (e.g. a node count) would
/// need a different payload encoding and are intentionally not emitted here.
pub(super) fn build_search_attributes(
    static_config: &BTreeMap<String, String>,
    project: &str,
    command: &str,
    target: Option<&str>,
    select: Option<&str>,
    registered: &std::collections::BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut search_attributes = static_config.clone();
    search_attributes
        .entry("DbtProject".to_string())
        .or_insert_with(|| project.to_string());
    search_attributes
        .entry("DbtCommand".to_string())
        .or_insert_with(|| command.to_string());
    if let Some(target) = target {
        search_attributes
            .entry("DbtTarget".to_string())
            .or_insert_with(|| target.to_string());
    }
    if let Some(select) = select {
        search_attributes
            .entry("DbtSelector".to_string())
            .or_insert_with(|| select.to_string());
    }
    search_attributes
        .entry("DbtStatus".to_string())
        .or_insert_with(|| RunOutcome::Running.as_str().to_string());

    if !registered.is_empty() {
        let before = search_attributes.len();
        search_attributes.retain(|k, _| {
            if registered.contains(k) {
                true
            } else {
                tracing::warn!(
                    attribute = %k,
                    "search attribute not registered on namespace — skipping \
                     (register it with `temporal operator search-attribute create`)"
                );
                false
            }
        });
        if before > 0 && search_attributes.is_empty() {
            tracing::warn!(
                "all search attributes were skipped — none are registered on the namespace"
            );
        }
    }

    search_attributes
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    fn registered_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn build_search_attributes_layers_dynamic_over_static() {
        let static_config = BTreeMap::new();
        // Empty registered set means filtering is skipped (all kept).
        let registered = BTreeSet::new();
        let attrs = build_search_attributes(
            &static_config,
            "shop",
            "build",
            Some("prod"),
            None,
            &registered,
        );
        assert_eq!(attrs.get("DbtProject").map(String::as_str), Some("shop"));
        assert_eq!(attrs.get("DbtCommand").map(String::as_str), Some("build"));
        assert_eq!(attrs.get("DbtTarget").map(String::as_str), Some("prod"));
    }

    #[test]
    fn build_search_attributes_static_config_wins_over_dynamic() {
        // If the worker config explicitly pinned DbtProject (or DbtCommand
        // etc.), the dynamic value must not overwrite it. `entry().or_insert`
        // gives the static config priority.
        let mut static_config = BTreeMap::new();
        static_config.insert("DbtProject".to_string(), "pinned-project".to_string());
        let registered = BTreeSet::new();
        let attrs =
            build_search_attributes(&static_config, "shop", "build", None, None, &registered);
        assert_eq!(attrs.get("DbtProject").map(String::as_str), Some("pinned-project"));
    }

    #[test]
    fn build_search_attributes_skips_dbttarget_when_input_target_absent() {
        let static_config = BTreeMap::new();
        let registered = BTreeSet::new();
        let attrs =
            build_search_attributes(&static_config, "shop", "build", None, None, &registered);
        assert!(!attrs.contains_key("DbtTarget"));
    }

    #[test]
    fn build_search_attributes_filters_to_registered_when_set() {
        let mut static_config = BTreeMap::new();
        static_config.insert("env".to_string(), "test".to_string());
        static_config.insert("team".to_string(), "data".to_string());

        // Only DbtProject is registered → static + DbtCommand + DbtTarget all dropped.
        let registered = registered_set(&["DbtProject"]);
        let attrs = build_search_attributes(
            &static_config,
            "shop",
            "build",
            Some("prod"),
            None,
            &registered,
        );
        assert_eq!(attrs.len(), 1);
        assert!(attrs.contains_key("DbtProject"));
        assert!(!attrs.contains_key("env"));
        assert!(!attrs.contains_key("DbtCommand"));
    }

    #[test]
    fn build_search_attributes_keeps_all_when_registered_set_empty() {
        // Empty registered set is the "all attributes are valid" default —
        // skip the filter entirely so users without a configured registry
        // still get DbtProject/DbtCommand etc.
        let mut static_config = BTreeMap::new();
        static_config.insert("env".to_string(), "test".to_string());
        let registered = BTreeSet::new();
        let attrs =
            build_search_attributes(&static_config, "shop", "build", None, None, &registered);
        assert_eq!(attrs.len(), 4); // env + DbtProject + DbtCommand + DbtStatus
        assert!(attrs.contains_key("env"));
        assert!(attrs.contains_key("DbtProject"));
        assert!(attrs.contains_key("DbtCommand"));
        assert_eq!(attrs.get("DbtStatus").map(String::as_str), Some("running"));
    }

    #[test]
    fn build_search_attributes_sets_selector_and_running_status() {
        let static_config = BTreeMap::new();
        let registered = BTreeSet::new();
        let attrs = build_search_attributes(
            &static_config,
            "shop",
            "build",
            None,
            Some("tag:nightly+"),
            &registered,
        );
        // DbtSelector mirrors --select so runs can be filtered by what they built.
        assert_eq!(attrs.get("DbtSelector").map(String::as_str), Some("tag:nightly+"));
        // DbtStatus starts at "running"; the workflow re-upserts the terminal outcome.
        assert_eq!(attrs.get("DbtStatus").map(String::as_str), Some("running"));
    }

    #[test]
    fn build_search_attributes_omits_selector_when_absent() {
        let static_config = BTreeMap::new();
        let registered = BTreeSet::new();
        let attrs =
            build_search_attributes(&static_config, "shop", "build", None, None, &registered);
        assert!(!attrs.contains_key("DbtSelector"));
    }

    #[test]
    fn build_search_attributes_returns_empty_when_nothing_registered() {
        // Static config is non-empty but none of the keys are registered.
        let mut static_config = BTreeMap::new();
        static_config.insert("foo".to_string(), "bar".to_string());
        let registered = registered_set(&["DbtTarget"]); // present but no match
        let attrs =
            build_search_attributes(&static_config, "shop", "build", None, None, &registered);
        assert!(attrs.is_empty());
    }
}
