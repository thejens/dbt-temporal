use super::*;

/// Every node reachable from `id` through one or more dependency edges,
/// by plain traversal.
fn ancestors_by_traversal(deps: &BTreeMap<String, BTreeSet<String>>, id: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut stack: Vec<&String> = deps.get(id).into_iter().flatten().collect();
    while let Some(current) = stack.pop() {
        if found.insert(current.clone()) {
            stack.extend(deps.get(current).into_iter().flatten());
        }
    }
    found
}

/// Straightforward statement of dbt's rule: for each test T and each model
/// or snapshot N that directly depends on one of T's parents, N waits for T
/// unless N is an ancestor of T, provided all of T's parents are ancestors
/// of N. Ancestor sets come from one traversal per node. Kept as the
/// specification that `inject_test_gates` must match.
fn inject_test_gates_reference(
    nodes: &Nodes,
    selected_ids: &[String],
    deps: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let raw = deps.clone();
    let resource_type = |id: &String| nodes.get_node(id).map(InternalDbtNode::resource_type);
    for test in selected_ids
        .iter()
        .filter(|id| resource_type(id) == Some(NodeType::Test))
    {
        let parents = &raw[test];
        let test_ancestors = ancestors_by_traversal(&raw, test);
        for node in selected_ids
            .iter()
            .filter(|id| matches!(resource_type(id), Some(NodeType::Model | NodeType::Snapshot)))
        {
            let consumes_a_parent = raw[node].iter().any(|d| parents.contains(d));
            if !consumes_a_parent || test_ancestors.contains(node) {
                continue;
            }
            let node_ancestors = ancestors_by_traversal(&raw, node);
            if parents.iter().all(|p| node_ancestors.contains(p))
                && let Some(entry) = deps.get_mut(node)
            {
                entry.insert(test.clone());
            }
        }
    }
}

/// Small deterministic xorshift generator, so generated graphs are
/// reproducible without a randomness dependency.
struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % bound as u64).unwrap_or(0)
    }
}

/// Model dependency lists (indices of earlier models) for each graph shape.
fn model_edges(shape: &str, size: usize, rng: &mut Xorshift) -> Vec<Vec<usize>> {
    (0..size)
        .map(|i| match shape {
            "chain" => {
                if i == 0 {
                    vec![]
                } else {
                    vec![i - 1]
                }
            }
            // Layers of three; every model depends on the whole previous layer.
            "diamond" => {
                if i < 3 {
                    vec![]
                } else {
                    ((i / 3 - 1) * 3..(i / 3) * 3).collect()
                }
            }
            "fan_out" => {
                if i == 0 {
                    vec![]
                } else {
                    vec![0]
                }
            }
            // Two independent random DAGs, split at the midpoint.
            "disconnected" => {
                let floor = if i < size / 2 { 0 } else { size / 2 };
                let span = i - floor;
                if span == 0 {
                    vec![]
                } else {
                    (0..=rng.below(3))
                        .map(|_| floor + rng.below(span))
                        .collect()
                }
            }
            _ => {
                if i == 0 {
                    vec![]
                } else {
                    (0..=rng.below(3)).map(|_| rng.below(i)).collect()
                }
            }
        })
        .collect()
}

/// Builds a project of `size` models in the given shape, a few seeds and
/// snapshots (some upstream of the first models, some downstream of
/// random models), plus `test_count` tests that each reference one to four
/// random nodes of any of those kinds. Returns the selected ids (a random
/// few models are left unselected).
pub(super) fn gate_fixture(
    shape: &str,
    size: usize,
    test_count: usize,
    seed: u64,
) -> (Nodes, Vec<String>) {
    let mut rng = Xorshift(seed);
    let model_id = |i: usize| format!("model.g.m{i:03}");
    let seed_id = |i: usize| format!("seed.g.s{i}");
    let upstream_snapshot_id = |i: usize| format!("snapshot.g.u{i}");
    let downstream_snapshot_id = |i: usize| format!("snapshot.g.d{i}");
    let mut nodes = Nodes::default();
    let mut selected = Vec::new();
    for i in 0..3 {
        add_seed(&mut nodes, &seed_id(i));
        add_snapshot(&mut nodes, &upstream_snapshot_id(i), &[&seed_id(i)]);
        selected.push(seed_id(i));
        selected.push(upstream_snapshot_id(i));
    }
    for (i, edges) in model_edges(shape, size, &mut rng).iter().enumerate() {
        let mut dep_ids: Vec<String> = edges.iter().map(|&d| model_id(d)).collect();
        // Models without model parents read a seed or an upstream snapshot.
        if dep_ids.is_empty() {
            match rng.below(3) {
                0 => dep_ids.push(seed_id(rng.below(3))),
                1 => dep_ids.push(upstream_snapshot_id(rng.below(3))),
                _ => {}
            }
        }
        let dep_refs: Vec<&str> = dep_ids.iter().map(String::as_str).collect();
        add_model(&mut nodes, &model_id(i), &dep_refs);
        if rng.below(12) != 0 {
            selected.push(model_id(i));
        }
    }
    for i in 0..3 {
        let dep_ids: Vec<String> = (0..=rng.below(2))
            .map(|_| model_id(rng.below(size)))
            .collect();
        let dep_refs: Vec<&str> = dep_ids.iter().map(String::as_str).collect();
        add_snapshot(&mut nodes, &downstream_snapshot_id(i), &dep_refs);
        selected.push(downstream_snapshot_id(i));
    }
    for t in 0..test_count {
        let dep_ids: Vec<String> = (0..=rng.below(4))
            .map(|_| match rng.below(8) {
                0 => seed_id(rng.below(3)),
                1 => upstream_snapshot_id(rng.below(3)),
                2 => downstream_snapshot_id(rng.below(3)),
                _ => model_id(rng.below(size)),
            })
            .collect();
        let dep_refs: Vec<&str> = dep_ids.iter().map(String::as_str).collect();
        let id = format!("test.g.t{t:03}");
        add_test(&mut nodes, &id, &dep_refs);
        selected.push(id);
    }
    (nodes, selected)
}

/// The reachability-index implementation must produce exactly the edge
/// sets of the per-node traversal, across graph shapes and seeds, and
/// injecting gates into an acyclic graph must leave it acyclic. A graph
/// whose raw dependencies contain a cycle is compared too.
#[test]
fn inject_test_gates_matches_the_reference_on_generated_graphs() {
    let mut gated_edges = 0usize;
    let mut gated_snapshots = 0usize;
    let mut gated_by_seed_test = 0usize;
    let mut multi_parent_gates = 0usize;
    let mut compared = 0usize;
    for shape in ["chain", "diamond", "fan_out", "disconnected", "random"] {
        for seed in 1..=12u64 {
            let (nodes, selected) = gate_fixture(shape, 40, 25, seed * 0x9E37_79B9 + 7);
            let raw = build_dependency_map(&nodes, &selected);
            topological_levels(&raw).unwrap();
            let mut expected = raw.clone();
            inject_test_gates_reference(&nodes, &selected, &mut expected);
            let mut actual = raw.clone();
            inject_test_gates(&nodes, &selected, &mut actual);
            assert_eq!(actual, expected, "shape {shape}, seed {seed}");
            topological_levels(&actual)
                .unwrap_or_else(|e| panic!("shape {shape}, seed {seed}: {e}"));

            for (node, node_deps) in &actual {
                for test in node_deps.difference(&raw[node]) {
                    gated_edges += 1;
                    gated_snapshots += usize::from(node.starts_with("snapshot."));
                    gated_by_seed_test +=
                        usize::from(raw[test].iter().any(|p| p.starts_with("seed.")));
                    multi_parent_gates += usize::from(raw[test].len() > 1);
                }
            }
            compared += 1;
        }
    }
    assert_eq!(compared, 60);
    // The generated graphs must actually exercise the interesting cases.
    assert!(gated_edges > 0, "generated graphs never exercised a gate");
    assert!(gated_snapshots > 0, "no snapshot was gated");
    assert!(gated_by_seed_test > 0, "no test on a seed gated anything");
    assert!(multi_parent_gates > 0, "no multi-parent test gated anything");

    // Raw cycle (m000 <-> m001 and a self-loop on m002): not a valid
    // project, but both implementations must still agree.
    let (nodes, selected) = gate_fixture("chain", 6, 6, 99);
    let mut raw = build_dependency_map(&nodes, &selected);
    for (from, to) in [("m000", "m001"), ("m002", "m002")] {
        let (from, to) = (format!("model.g.{from}"), format!("model.g.{to}"));
        if let Some(entry) = raw.get_mut(&from) {
            entry.insert(to);
        }
    }
    let mut expected = raw.clone();
    inject_test_gates_reference(&nodes, &selected, &mut expected);
    let mut actual = raw;
    inject_test_gates(&nodes, &selected, &mut actual);
    assert_eq!(actual, expected, "cyclic raw graph");
}
