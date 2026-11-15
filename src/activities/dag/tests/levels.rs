use super::*;

#[test]
fn test_topological_levels_single_node() {
    let d = deps(&[("a", &[])]);
    let levels = topological_levels(&d).unwrap();
    assert_eq!(levels, vec![vec!["a"]]);
}

#[test]
fn test_topological_levels_linear_chain() {
    // a -> b -> c
    let d = deps(&[("c", &["b"]), ("b", &["a"]), ("a", &[])]);
    let levels = topological_levels(&d).unwrap();
    assert_eq!(levels, vec![vec!["a"], vec!["b"], vec!["c"]]);
}

#[test]
fn test_topological_levels_diamond() {
    //   a
    //  / \
    // b   c
    //  \ /
    //   d
    let d = deps(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["b", "c"])]);
    let levels = topological_levels(&d).unwrap();
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0], vec!["a"]);
    assert_eq!(levels[1], vec!["b", "c"]); // sorted
    assert_eq!(levels[2], vec!["d"]);
}

#[test]
fn test_topological_levels_independent_nodes() {
    let d = deps(&[("a", &[]), ("b", &[]), ("c", &[])]);
    let levels = topological_levels(&d).unwrap();
    assert_eq!(levels, vec![vec!["a", "b", "c"]]);
}

#[test]
fn test_independent_nodes_flattens_a_real_dependency() {
    // An SLA-carrying model and the upstream it reads share a freshness
    // plan; both must be measured, so the plan must not order one behind
    // the other.
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.p.raw_orders", &[]);
    add_model(&mut nodes, "model.p.stg_orders", &["model.p.raw_orders"]);
    let selected = vec![
        "model.p.raw_orders".to_string(),
        "model.p.stg_orders".to_string(),
    ];

    let graph_deps = build_dependency_map(&nodes, &selected);
    assert_eq!(topological_levels(&graph_deps).unwrap().len(), 2);

    let flat = independent_nodes(&selected);
    assert!(flat.values().all(BTreeSet::is_empty));
    assert_eq!(
        topological_levels(&flat).unwrap(),
        vec![vec!["model.p.raw_orders", "model.p.stg_orders"]]
    );
}

#[test]
fn test_independent_nodes_empty_selection() {
    assert!(independent_nodes(&[]).is_empty());
}

#[test]
fn test_topological_levels_wide_fan_out() {
    // a -> b, c, d
    let d = deps(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["a"])]);
    let levels = topological_levels(&d).unwrap();
    assert_eq!(levels, vec![vec!["a"], vec!["b", "c", "d"]]);
}

#[test]
fn test_topological_levels_empty() {
    let d = deps(&[]);
    let levels = topological_levels(&d).unwrap();
    assert!(levels.is_empty(), "{levels:?}");
}

#[test]
fn test_topological_levels_cycle_detected() {
    // a -> b -> a (cycle)
    let d = deps(&[("a", &["b"]), ("b", &["a"])]);
    let err = topological_levels(&d).unwrap_err();
    assert!(err.to_string().contains("cyclic dependency"), "{err}");
}
