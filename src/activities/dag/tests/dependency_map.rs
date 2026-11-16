use super::*;

/// `build_dependency_map` skips deps that aren't in the selected set
/// (commonly ephemeral models — they're never executed standalone) and
/// promotes their transitive selected ancestors instead, so the runtime
/// DAG still respects the intended ordering.
#[test]
fn build_dependency_map_promotes_ephemeral_ancestors() {
    let mut nodes = Nodes::default();
    // a (selected) -> e1 (NOT selected, ephemeral) -> e2 (NOT selected) -> b (selected)
    // a transitively depends on b through two ephemeral hops.
    add_model(&mut nodes, "model.x.b", &[]);
    add_model(&mut nodes, "model.x.e2", &["model.x.b"]);
    add_model(&mut nodes, "model.x.e1", &["model.x.e2"]);
    add_model(&mut nodes, "model.x.a", &["model.x.e1"]);

    let selected: Vec<String> = ["model.x.a", "model.x.b"]
        .iter()
        .map(ToString::to_string)
        .collect();

    let deps_map = build_dependency_map(&nodes, &selected);

    // `a`'s direct ephemeral dep (e1) is not in selected — it should be
    // replaced by the first selected ancestor reachable through it (b).
    assert_eq!(deps_map["model.x.a"], BTreeSet::from(["model.x.b".to_string()]),);
    // `b` has no deps.
    assert!(deps_map["model.x.b"].is_empty());
}

#[test]
fn build_dependency_map_drops_external_deps_not_in_selected() {
    let mut nodes = Nodes::default();
    // a refs b, but only a is selected; b is in a different package and
    // not present in nodes at all (as if loaded but not selected).
    add_model(&mut nodes, "model.x.a", &["model.other.b"]);

    let selected: Vec<String> = std::iter::once("model.x.a".to_string()).collect();

    let deps_map = build_dependency_map(&nodes, &selected);
    // No selected ancestor reachable through model.other.b → empty dep set.
    assert!(deps_map["model.x.a"].is_empty());
}

/// The selected ancestors reachable from `start` through unselected nodes only,
/// found by a fresh walk per call.
fn reference_promotion(nodes: &Nodes, start: &str, selected: &BTreeSet<&str>) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut stack = vec![start.to_string()];
    while let Some(current) = stack.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(node) = nodes.get_node(&current) {
            for (dep, _) in &node.base().depends_on.nodes_with_ref_location {
                if selected.contains(dep.as_str()) {
                    found.insert(dep.clone());
                } else {
                    stack.push(dep.clone());
                }
            }
        }
    }
    found
}

/// Chains of ephemerals that fork and rejoin, with selected nodes at several
/// depths, give the same dependencies as a walk from scratch per edge.
#[test]
fn promotion_through_shared_ephemeral_ancestors_matches_a_fresh_walk() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.x.s1", &[]);
    add_model(&mut nodes, "model.x.s2", &[]);
    add_model(&mut nodes, "model.x.s3", &[]);
    // e_top reaches s1 and, through e_mid, s2; e_mid is also reached via e_alt.
    add_model(&mut nodes, "model.x.e_mid", &["model.x.s2", "model.x.missing"]);
    add_model(&mut nodes, "model.x.e_alt", &["model.x.e_mid", "model.x.s3"]);
    add_model(&mut nodes, "model.x.e_top", &["model.x.s1", "model.x.e_mid", "model.x.e_alt"]);
    add_model(&mut nodes, "model.x.e_leaf", &[]);
    add_model(&mut nodes, "model.x.a", &["model.x.e_top", "model.x.e_leaf"]);
    add_model(&mut nodes, "model.x.b", &["model.x.e_alt", "model.x.s1"]);
    add_model(&mut nodes, "model.x.c", &["model.x.e_mid"]);

    let selected_ids = selected(&[
        "model.x.s1",
        "model.x.s2",
        "model.x.s3",
        "model.x.a",
        "model.x.b",
        "model.x.c",
    ]);
    let selected_set: BTreeSet<&str> = selected_ids.iter().map(String::as_str).collect();
    let deps_map = build_dependency_map(&nodes, &selected_ids);

    assert_eq!(
        deps_map["model.x.a"],
        BTreeSet::from(["model.x.s1", "model.x.s2", "model.x.s3"].map(String::from)),
    );
    assert_eq!(
        deps_map["model.x.b"],
        BTreeSet::from(["model.x.s1", "model.x.s2", "model.x.s3"].map(String::from)),
    );
    assert_eq!(deps_map["model.x.c"], BTreeSet::from(["model.x.s2".to_string()]));
    for id in &selected_ids {
        let node = nodes.get_node(id).unwrap();
        let mut expected = BTreeSet::new();
        for (dep, _) in &node.base().depends_on.nodes_with_ref_location {
            if selected_set.contains(dep.as_str()) {
                expected.insert(dep.clone());
            } else {
                expected.extend(reference_promotion(&nodes, dep, &selected_set));
            }
        }
        assert_eq!(deps_map[id], expected, "dependencies of {id}");
    }
}

/// Every unselected node is expanded once, however many selected nodes reach
/// it: a long ephemeral chain shared by many consumers costs the length of the
/// chain, not its length times the number of consumers.
#[test]
fn promotion_expands_each_unselected_node_once() {
    const CHAIN: usize = 2000;
    const CONSUMERS: usize = 500;
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.x.root", &[]);
    add_model(&mut nodes, "model.x.e0", &["model.x.root"]);
    let chain_ids: Vec<String> = (0..CHAIN).map(|i| format!("model.x.e{i}")).collect();
    for i in 1..CHAIN {
        add_model(&mut nodes, &chain_ids[i], &[chain_ids[i - 1].as_str()]);
    }
    let head = chain_ids[CHAIN - 1].as_str();
    let consumer_ids: Vec<String> = (0..CONSUMERS).map(|i| format!("model.x.c{i}")).collect();
    for id in &consumer_ids {
        add_model(&mut nodes, id, &[head]);
    }
    let mut selected_ids = consumer_ids.clone();
    selected_ids.push("model.x.root".to_string());
    let selected_set: BTreeSet<&str> = selected_ids.iter().map(String::as_str).collect();

    let mut promoter = EphemeralPromoter::new(&nodes, &selected_set);
    for _ in 0..CONSUMERS {
        let reached = promoter.promoted(head);
        assert_eq!(*reached, BTreeSet::from(["model.x.root".to_string()]));
    }
    assert_eq!(promoter.expansions.get(), CHAIN);

    let deps_map = build_dependency_map(&nodes, &selected_ids);
    assert!(
        consumer_ids
            .iter()
            .all(|id| deps_map[id] == BTreeSet::from(["model.x.root".to_string()]))
    );
}

/// Dbt rejects cyclic projects, but a cycle among unselected nodes must not hang
/// planning if one reaches this code.
#[test]
fn promotion_terminates_on_a_cycle_of_unselected_nodes() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.x.s", &[]);
    add_model(&mut nodes, "model.x.e1", &["model.x.e2", "model.x.s"]);
    add_model(&mut nodes, "model.x.e2", &["model.x.e1"]);
    add_model(&mut nodes, "model.x.a", &["model.x.e1"]);
    let selected_ids = selected(&["model.x.s", "model.x.a"]);

    let deps_map = build_dependency_map(&nodes, &selected_ids);
    assert_eq!(deps_map["model.x.a"], BTreeSet::from(["model.x.s".to_string()]));
}
