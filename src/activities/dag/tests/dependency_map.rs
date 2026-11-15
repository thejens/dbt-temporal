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
