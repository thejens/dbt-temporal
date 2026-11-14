//! Dependency-graph walks behind the `+model` / `model+` / `@model` operators.

use std::cell::OnceCell;
use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::Nodes;

use crate::activities::dag::build_dependency_map;

/// Reverse a dependency map: from "node -> deps" to "dep -> dependents".
pub fn reverse_dep_map(
    deps: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut reverse: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (node, node_deps) in deps {
        reverse.entry(node.clone()).or_default(); // ensure node exists
        for dep in node_deps {
            reverse.entry(dep.clone()).or_default().insert(node.clone());
        }
    }
    reverse
}

/// The project-wide dependency graph in both directions.
///
/// Built over every node, not just the command's selection: graph operators and
/// indirect selection both reach nodes the command would not execute (sources,
/// exposures, tests).
#[derive(Default)]
pub struct DependencyMaps {
    /// Node to its parents.
    pub deps: BTreeMap<String, BTreeSet<String>>,
    /// Node to its children.
    pub reverse: BTreeMap<String, BTreeSet<String>>,
}

impl DependencyMaps {
    pub fn build(nodes: &Nodes) -> Self {
        let all_ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
        let deps = build_dependency_map(nodes, &all_ids);
        let reverse = reverse_dep_map(&deps);
        Self { deps, reverse }
    }
}

/// [`DependencyMaps`] constructed on first use and then shared.
///
/// Building clones every node id in the project and a plan needs the maps for
/// graph operators and for indirect selection, possibly several times over;
/// selections that need neither never pay for them.
pub struct LazyDependencyMaps<'a> {
    nodes: &'a Nodes,
    maps: OnceCell<DependencyMaps>,
}

impl<'a> LazyDependencyMaps<'a> {
    pub const fn new(nodes: &'a Nodes) -> Self {
        Self {
            nodes,
            maps: OnceCell::new(),
        }
    }

    pub fn get(&self) -> &DependencyMaps {
        self.maps.get_or_init(|| DependencyMaps::build(self.nodes))
    }
}

/// Multi-source breadth-first walk: every node within `max_depth` edges of any
/// seed, seeds included.
///
/// One visited set serves all seeds, so each edge is followed at most once no
/// matter how many seeds share ancestry. Visiting in rounds from the whole
/// frontier is what keeps depth limits right: a node first reached in round `k`
/// is at its minimum distance `k` from the nearest seed, so it is expanded with
/// the most remaining depth any seed could give it.
pub fn expand_from_seeds(
    seeds: &BTreeSet<String>,
    graph: &BTreeMap<String, BTreeSet<String>>,
    max_depth: u32,
) -> BTreeSet<String> {
    let mut visited: BTreeSet<String> = seeds.clone();
    let mut frontier: Vec<&str> = seeds.iter().map(String::as_str).collect();
    let mut depth = 0;

    while !frontier.is_empty() && depth < max_depth {
        let mut next = Vec::new();
        for node in &frontier {
            for neighbor in graph.get(*node).into_iter().flatten() {
                if visited.insert(neighbor.clone()) {
                    next.push(neighbor.as_str());
                }
            }
        }
        frontier = next;
        depth += 1;
    }
    visited
}

/// The `@` operator: the seeds, all of their descendants, and every ancestor of
/// those descendants — everything needed to build the seeds' whole downstream.
pub fn childrens_parents(maps: &DependencyMaps, seeds: &BTreeSet<String>) -> BTreeSet<String> {
    let descendants = expand_from_seeds(seeds, &maps.reverse, u32::MAX);
    // Every ancestor of a descendant is an ancestor of some leaf, or is itself
    // a descendant that leads to one, so walking up from the leaves alone
    // reaches the same set as walking up from all descendants.
    let leaves: BTreeSet<String> = descendants
        .iter()
        .filter(|node| maps.reverse.get(*node).is_none_or(BTreeSet::is_empty))
        .cloned()
        .collect();
    expand_from_seeds(&leaves, &maps.deps, u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deps(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        entries
            .iter()
            .map(|(node, node_deps)| {
                ((*node).to_string(), node_deps.iter().map(ToString::to_string).collect())
            })
            .collect()
    }

    fn set(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(ToString::to_string).collect()
    }

    fn maps(entries: &[(&str, &[&str])]) -> DependencyMaps {
        let deps = deps(entries);
        let reverse = reverse_dep_map(&deps);
        DependencyMaps { deps, reverse }
    }

    /// One walk per seed, unioned: the definition the multi-source walk must
    /// agree with. Seeds are part of the result, as they are for the real walk.
    fn per_seed_reference(
        seeds: &BTreeSet<String>,
        graph: &BTreeMap<String, BTreeSet<String>>,
        max_depth: u32,
    ) -> BTreeSet<String> {
        let mut result = seeds.clone();
        for seed in seeds {
            let mut queue: Vec<&str> = vec![seed];
            let mut visited: BTreeSet<&str> = BTreeSet::from([seed.as_str()]);
            let mut depth = 0;
            while !queue.is_empty() && depth < max_depth {
                let mut next_queue = Vec::new();
                for node in &queue {
                    for neighbor in graph.get(*node).into_iter().flatten() {
                        if visited.insert(neighbor.as_str()) {
                            result.insert(neighbor.clone());
                            next_queue.push(neighbor.as_str());
                        }
                    }
                }
                queue = next_queue;
                depth += 1;
            }
        }
        result
    }

    /// Deterministic generator so a failing graph reproduces from its seed.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self, bound: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            usize::try_from(self.0 >> 33).unwrap_or(usize::MAX) % bound
        }
    }

    /// A random DAG: node `i` may depend only on nodes with a smaller index.
    fn random_dag(rng: &mut Lcg, size: usize) -> BTreeMap<String, BTreeSet<String>> {
        (0..size)
            .map(|i| {
                let parents = (0..i)
                    .filter(|_| rng.next(100) < 25)
                    .map(|p| format!("n{p:02}"))
                    .collect();
                (format!("n{i:02}"), parents)
            })
            .collect()
    }

    #[test]
    fn multi_source_walk_agrees_with_per_seed_walks_on_random_graphs() {
        for case in 0..200_u64 {
            let mut rng = Lcg(case + 1);
            let size = 5 + rng.next(30);
            let dep_map = random_dag(&mut rng, size);
            let reverse = reverse_dep_map(&dep_map);
            let seed_count = 1 + rng.next(6);
            let seeds: BTreeSet<String> = (0..seed_count)
                .map(|_| format!("n{:02}", rng.next(size)))
                .collect();
            for depth in [0, 1, 2, 3, 5, u32::MAX] {
                for graph in [&dep_map, &reverse] {
                    assert_eq!(
                        expand_from_seeds(&seeds, graph, depth),
                        per_seed_reference(&seeds, graph, depth),
                        "case {case}, depth {depth}, seeds {seeds:?}"
                    );
                }
            }
        }
    }

    /// `far` reaches `mid` at distance 2 and `near` reaches it at distance 1.
    /// Expanding `mid` with the depth left from `far` alone would stop short of
    /// `leaf`, so the walk has to take the nearer seed's route.
    #[test]
    fn a_node_reached_by_two_seeds_is_expanded_from_the_nearer_one() {
        let reverse = deps(&[
            ("far", &["hop"]),
            ("hop", &["mid"]),
            ("near", &["mid"]),
            ("mid", &["leaf"]),
            ("leaf", &[]),
        ]);
        let seeds = set(&["far", "near"]);
        assert_eq!(
            expand_from_seeds(&seeds, &reverse, 2),
            set(&["far", "hop", "mid", "near", "leaf"]),
        );
        assert_eq!(expand_from_seeds(&seeds, &reverse, 2), per_seed_reference(&seeds, &reverse, 2),);
    }

    #[test]
    fn walk_upstream_and_depth_limited() {
        // a -> b -> c -> d
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        assert_eq!(expand_from_seeds(&set(&["d"]), &dep_map, u32::MAX), set(&["a", "b", "c", "d"]));
        assert_eq!(expand_from_seeds(&set(&["d"]), &dep_map, 1), set(&["c", "d"]));
        assert_eq!(expand_from_seeds(&set(&["d"]), &dep_map, 0), set(&["d"]));
    }

    #[test]
    fn walk_downstream() {
        // a -> b -> c
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let reverse = reverse_dep_map(&dep_map);
        assert_eq!(expand_from_seeds(&set(&["a"]), &reverse, u32::MAX), set(&["a", "b", "c"]));
    }

    /// root -> left -> leaf, root -> right -> leaf, plus `other` feeding `right`
    /// and `unrelated` touching nothing. `@root` takes the whole downstream and
    /// everything that downstream is built from, and nothing unconnected.
    #[test]
    fn childrens_parents_takes_descendants_and_every_ancestor_of_them() {
        let m = maps(&[
            ("root", &[]),
            ("left", &["root"]),
            ("right", &["root", "other"]),
            ("leaf", &["left", "right"]),
            ("other", &["other_source"]),
            ("other_source", &[]),
            ("unrelated", &[]),
        ]);
        assert_eq!(
            childrens_parents(&m, &set(&["root"])),
            set(&["root", "left", "right", "leaf", "other", "other_source"])
        );
    }

    #[test]
    fn childrens_parents_of_a_mid_graph_seed_pulls_in_ancestors_of_its_descendants() {
        // b is the seed; c is its child; c also needs x, which needs y. a (b's
        // parent) is an ancestor of the seed itself and so of the leaf too.
        let m = maps(&[
            ("a", &[]),
            ("b", &["a"]),
            ("c", &["b", "x"]),
            ("x", &["y"]),
            ("y", &[]),
            ("sibling", &["a"]),
        ]);
        assert_eq!(childrens_parents(&m, &set(&["b"])), set(&["a", "b", "c", "x", "y"]));
    }

    #[test]
    fn childrens_parents_of_a_leaf_is_the_leaf_and_its_ancestors() {
        let m = maps(&[("a", &[]), ("b", &["a"])]);
        assert_eq!(childrens_parents(&m, &set(&["b"])), set(&["a", "b"]));
    }

    #[test]
    fn childrens_parents_of_several_seeds_is_the_union_of_each() {
        let m = maps(&[
            ("p", &[]),
            ("q", &[]),
            ("p_child", &["p", "r"]),
            ("q_child", &["q"]),
            ("r", &[]),
        ]);
        assert_eq!(
            childrens_parents(&m, &set(&["p", "q"])),
            set(&["p", "q", "p_child", "q_child", "r"])
        );
    }

    #[test]
    fn reverse_dep_map_inverts_edges_and_keeps_leaves() -> anyhow::Result<()> {
        let dep_map = deps(&[("a", &[]), ("b", &["a"]), ("c", &["a"])]);
        let reverse = reverse_dep_map(&dep_map);
        assert_eq!(
            reverse
                .get("a")
                .ok_or_else(|| anyhow::anyhow!("missing key 'a'"))?,
            &BTreeSet::from(["b".to_string(), "c".to_string()])
        );
        assert!(
            reverse
                .get("b")
                .ok_or_else(|| anyhow::anyhow!("missing key 'b'"))?
                .is_empty()
        );
        assert!(
            reverse
                .get("c")
                .ok_or_else(|| anyhow::anyhow!("missing key 'c'"))?
                .is_empty()
        );
        Ok(())
    }
}
