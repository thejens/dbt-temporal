//! Temporal priority keys derived from the dependency graph.

use std::collections::BTreeMap;

/// Map each node's critical-path height (longest chain of dependents) to a
/// Temporal priority key: height >= 4 -> 1 (highest) ... height 0 -> 5
/// (lowest). Processes topological levels bottom-up so each node's height is
/// 1 + the max height of its dependents.
pub(super) fn compute_priorities(
    levels: &[Vec<String>],
    deps: &BTreeMap<String, std::collections::BTreeSet<String>>,
) -> BTreeMap<String, u32> {
    // deps maps node -> prerequisites; invert to node -> dependents.
    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (node, prereqs) in deps {
        for prereq in prereqs {
            dependents
                .entry(prereq.as_str())
                .or_default()
                .push(node.as_str());
        }
    }

    let mut heights: BTreeMap<String, u32> = BTreeMap::new();
    for level in levels.iter().rev() {
        for unique_id in level {
            let height = dependents
                .get(unique_id.as_str())
                .into_iter()
                .flatten()
                .filter_map(|dep| heights.get(*dep))
                .max()
                .map_or(0, |h| h + 1);
            heights.insert(unique_id.clone(), height);
        }
    }

    heights
        .into_iter()
        .map(|(id, h)| (id, 5_u32.saturating_sub(h).max(1)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn deps_map(edges: &[(&str, &str)]) -> BTreeMap<String, BTreeSet<String>> {
        // edges are (node, prerequisite) pairs, matching the planner's deps shape.
        let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (node, prereq) in edges {
            deps.entry((*node).to_string())
                .or_default()
                .insert((*prereq).to_string());
        }
        deps
    }

    fn level(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn compute_priorities_linear_chain_maps_height_to_key() {
        // a -> b -> c -> d -> e -> f: heights 5..0 top-down, so keys clamp at 1
        // for anything 4+ levels from the bottom and reach 5 at the leaf.
        let levels = vec![
            level(&["a"]),
            level(&["b"]),
            level(&["c"]),
            level(&["d"]),
            level(&["e"]),
            level(&["f"]),
        ];
        let deps = deps_map(&[("b", "a"), ("c", "b"), ("d", "c"), ("e", "d"), ("f", "e")]);
        let priorities = compute_priorities(&levels, &deps);
        assert_eq!(priorities.get("a"), Some(&1)); // height 5, clamped
        assert_eq!(priorities.get("b"), Some(&1)); // height 4
        assert_eq!(priorities.get("c"), Some(&2)); // height 3
        assert_eq!(priorities.get("d"), Some(&3)); // height 2
        assert_eq!(priorities.get("e"), Some(&4)); // height 1
        assert_eq!(priorities.get("f"), Some(&5)); // height 0 (sink)
    }

    #[test]
    fn compute_priorities_uses_longest_dependent_chain() {
        // Diamond plus a long tail off one branch: the root's height must follow
        // the longest path (through b), not the shortest (through c).
        //   a -> b -> d -> e
        //   a -> c ---^
        let levels = vec![
            level(&["a"]),
            level(&["b", "c"]),
            level(&["d"]),
            level(&["e"]),
        ];
        let deps = deps_map(&[("b", "a"), ("c", "a"), ("d", "b"), ("d", "c"), ("e", "d")]);
        let priorities = compute_priorities(&levels, &deps);
        assert_eq!(priorities.get("a"), Some(&2)); // height 3
        assert_eq!(priorities.get("b"), Some(&3)); // height 2
        assert_eq!(priorities.get("c"), Some(&3)); // height 2
        assert_eq!(priorities.get("d"), Some(&4)); // height 1
        assert_eq!(priorities.get("e"), Some(&5)); // height 0
    }

    #[test]
    fn compute_priorities_isolated_node_gets_lowest_key() {
        let levels = vec![level(&["solo"])];
        let priorities = compute_priorities(&levels, &BTreeMap::new());
        assert_eq!(priorities.get("solo"), Some(&5));
    }
}
