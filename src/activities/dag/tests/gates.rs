use super::gate_reference::gate_fixture;
use super::*;

/// Guards against the panda-cascade marketing_model phantom cycle.
///
/// `test_country_base` refs four sibling models in a dep chain via a Jinja
/// loop — `marketing_spend_{daily,weekly,monthly,quarterly}`. Naively
/// gating each model on every test of its upstream models adds an edge
/// `weekly → test_country_base`, but `test_country_base` already depends
/// on `weekly`, so a cycle appears and Kahn's algorithm flags
/// `{weekly, monthly, quarterly, test_country_base}` as unreachable.
#[test]
fn inject_test_gates_skips_back_edges_to_self_gating_test() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.daily", &[]);
    add_model(&mut nodes, "model.m.weekly", &["model.m.daily"]);
    add_model(&mut nodes, "model.m.monthly", &["model.m.daily"]);
    add_model(&mut nodes, "model.m.quarterly", &["model.m.monthly"]);
    add_test(
        &mut nodes,
        "test.m.country_base",
        &[
            "model.m.daily",
            "model.m.weekly",
            "model.m.monthly",
            "model.m.quarterly",
        ],
    );

    let selected: Vec<String> = [
        "model.m.daily",
        "model.m.weekly",
        "model.m.monthly",
        "model.m.quarterly",
        "test.m.country_base",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    // None of the spend models should have been gated on a test that already depends on them.
    for sibling in ["model.m.weekly", "model.m.monthly", "model.m.quarterly"] {
        assert!(
            !deps_map[sibling].contains("test.m.country_base"),
            "{sibling} was gated on test.m.country_base, which would create a cycle"
        );
    }

    // And the graph must still planarise.
    let levels = topological_levels(&deps_map).unwrap();
    let placed: BTreeSet<&str> = levels.iter().flatten().map(String::as_str).collect();
    for id in &selected {
        assert!(placed.contains(id.as_str()), "{id} missing from levels");
    }
    // Test runs strictly after every model it depends on.
    let level_of = |id: &str| {
        levels
            .iter()
            .position(|l| l.iter().any(|n| n == id))
            .unwrap()
    };
    let test_level = level_of("test.m.country_base");
    for model in [
        "model.m.daily",
        "model.m.weekly",
        "model.m.monthly",
        "model.m.quarterly",
    ] {
        assert!(level_of(model) < test_level, "{model} should run before test.m.country_base");
    }
}

/// The standard gate must still fire when a downstream model has no
/// reverse path to the test — i.e. the test only depends on the upstream
/// model, not on the downstream consumer.
#[test]
fn inject_test_gates_adds_normal_forward_gate() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.upstream", &[]);
    add_model(&mut nodes, "model.m.downstream", &["model.m.upstream"]);
    add_test(&mut nodes, "test.m.upstream_check", &["model.m.upstream"]);

    let selected: Vec<String> = [
        "model.m.upstream",
        "model.m.downstream",
        "test.m.upstream_check",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(
        deps_map["model.m.downstream"].contains("test.m.upstream_check"),
        "downstream model should be gated on its upstream model's test"
    );

    let levels = topological_levels(&deps_map).unwrap();
    let level_of = |id: &str| {
        levels
            .iter()
            .position(|l| l.iter().any(|n| n == id))
            .unwrap()
    };
    assert!(level_of("model.m.upstream") < level_of("test.m.upstream_check"));
    assert!(level_of("test.m.upstream_check") < level_of("model.m.downstream"));
}

/// A test that depends on a model and its descendant gates neither of
/// them, and does not gate another consumer of the parent either: that
/// consumer is not downstream of the descendant, so the test is not one of
/// its upstream tests.
#[test]
fn inject_test_gates_ignores_consumers_outside_the_test_lineage() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.parent", &[]);
    add_model(&mut nodes, "model.m.child_a", &["model.m.parent"]);
    add_model(&mut nodes, "model.m.child_b", &["model.m.parent"]);
    add_model(&mut nodes, "model.m.grandchild", &["model.m.child_a"]);
    add_test(&mut nodes, "test.m.shared", &["model.m.parent", "model.m.child_a"]);

    let selected = selected(&[
        "model.m.parent",
        "model.m.child_a",
        "model.m.child_b",
        "model.m.grandchild",
        "test.m.shared",
    ]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(
        !deps_map["model.m.child_a"].contains("test.m.shared"),
        "child_a is referenced by the test — gating it would create a cycle"
    );
    assert!(
        !deps_map["model.m.child_b"].contains("test.m.shared"),
        "child_b is not downstream of child_a, which the test also covers"
    );
    // Downstream of every parent of the test, and not an ancestor of it.
    assert!(deps_map["model.m.grandchild"].contains("test.m.shared"));

    topological_levels(&deps_map).unwrap();
}

/// Two tests that each span two models, where each model is downstream of
/// the other test's second parent. Gating every consumer of a tested model
/// would give N1 → T1 → N2 → T2 → N1; the rule only gates what is
/// downstream of all of a test's parents, so no edge is added.
#[test]
fn inject_test_gates_adds_no_edge_that_closes_a_cycle_across_two_tests() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.m1", &[]);
    add_model(&mut nodes, "model.m.m2", &[]);
    add_model(&mut nodes, "model.m.n1", &["model.m.m1"]);
    add_model(&mut nodes, "model.m.n2", &["model.m.m2"]);
    add_test(&mut nodes, "test.m.t1", &["model.m.m1", "model.m.n2"]);
    add_test(&mut nodes, "test.m.t2", &["model.m.m2", "model.m.n1"]);
    let selected = selected(&[
        "model.m.m1",
        "model.m.m2",
        "model.m.n1",
        "model.m.n2",
        "test.m.t1",
        "test.m.t2",
    ]);

    let raw = build_dependency_map(&nodes, &selected);
    let mut deps_map = raw.clone();
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert_eq!(deps_map, raw);
    topological_levels(&deps_map).unwrap();
}

/// A relationships-style test on `orders` and `customers` gates what is
/// downstream of both, not what is downstream of only one.
#[test]
fn inject_test_gates_gates_only_nodes_downstream_of_every_parent() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.orders", &[]);
    add_model(&mut nodes, "model.m.customers", &[]);
    add_model(&mut nodes, "model.m.orders_only", &["model.m.orders"]);
    add_model(&mut nodes, "model.m.customers_only", &["model.m.customers"]);
    add_model(&mut nodes, "model.m.both", &["model.m.orders", "model.m.customers"]);
    add_model(&mut nodes, "model.m.via_both", &["model.m.both"]);
    add_test(&mut nodes, "test.m.relationships", &["model.m.orders", "model.m.customers"]);
    let selected = selected(&[
        "model.m.orders",
        "model.m.customers",
        "model.m.orders_only",
        "model.m.customers_only",
        "model.m.both",
        "model.m.via_both",
        "test.m.relationships",
    ]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["model.m.both"].contains("test.m.relationships"));
    for ungated in [
        "model.m.orders_only",
        "model.m.customers_only",
        "model.m.orders",
        "model.m.customers",
    ] {
        assert!(
            !deps_map[ungated].contains("test.m.relationships"),
            "{ungated} is not downstream of both parents"
        );
    }
    // `via_both` reaches the test through `both`, so it needs no edge of its own.
    assert!(!deps_map["model.m.via_both"].contains("test.m.relationships"));
    topological_levels(&deps_map).unwrap();
}

/// Only the nodes that consume a parent directly receive an edge; a node
/// further down is ordered after the test through the node in between.
#[test]
fn inject_test_gates_adds_edges_to_direct_consumers_only() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.a", &[]);
    add_model(&mut nodes, "model.m.b", &["model.m.a"]);
    add_model(&mut nodes, "model.m.c", &["model.m.b"]);
    add_test(&mut nodes, "test.m.a_check", &["model.m.a"]);
    let selected = selected(&["model.m.a", "model.m.b", "model.m.c", "test.m.a_check"]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["model.m.b"].contains("test.m.a_check"));
    assert!(!deps_map["model.m.c"].contains("test.m.a_check"));
}

/// A test on a seed gates the models and snapshots that read the seed.
#[test]
fn inject_test_gates_gates_consumers_of_a_tested_seed() {
    let mut nodes = Nodes::default();
    add_seed(&mut nodes, "seed.m.countries");
    add_model(&mut nodes, "model.m.stg", &["seed.m.countries"]);
    add_snapshot(&mut nodes, "snapshot.m.snap", &["seed.m.countries"]);
    add_model(&mut nodes, "model.m.unrelated", &[]);
    add_test(&mut nodes, "test.m.seed_check", &["seed.m.countries"]);
    let selected = selected(&[
        "seed.m.countries",
        "model.m.stg",
        "snapshot.m.snap",
        "model.m.unrelated",
        "test.m.seed_check",
    ]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["model.m.stg"].contains("test.m.seed_check"));
    assert!(deps_map["snapshot.m.snap"].contains("test.m.seed_check"));
    assert!(!deps_map["model.m.unrelated"].contains("test.m.seed_check"));
    assert!(!deps_map["seed.m.countries"].contains("test.m.seed_check"));
    topological_levels(&deps_map).unwrap();
}

/// A test on a snapshot gates the models downstream of the snapshot, and a
/// snapshot is itself gated by the tests of the models it reads.
#[test]
fn inject_test_gates_gates_through_and_into_snapshots() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.base", &[]);
    add_snapshot(&mut nodes, "snapshot.m.snap", &["model.m.base"]);
    add_model(&mut nodes, "model.m.report", &["snapshot.m.snap"]);
    add_test(&mut nodes, "test.m.base_check", &["model.m.base"]);
    add_test(&mut nodes, "test.m.snap_check", &["snapshot.m.snap"]);
    let selected = selected(&[
        "model.m.base",
        "snapshot.m.snap",
        "model.m.report",
        "test.m.base_check",
        "test.m.snap_check",
    ]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["snapshot.m.snap"].contains("test.m.base_check"));
    assert!(deps_map["model.m.report"].contains("test.m.snap_check"));
    assert!(!deps_map["model.m.report"].contains("test.m.base_check"));
    topological_levels(&deps_map).unwrap();
}

/// A test on a source gates the models that read the source.
#[test]
fn inject_test_gates_gates_consumers_of_a_tested_source() {
    let mut nodes = Nodes::default();
    add_source(&mut nodes, "source.m.raw.orders");
    add_model(&mut nodes, "model.m.stg", &["source.m.raw.orders"]);
    add_test(&mut nodes, "test.m.source_check", &["source.m.raw.orders"]);
    let selected = selected(&["source.m.raw.orders", "model.m.stg", "test.m.source_check"]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["model.m.stg"].contains("test.m.source_check"));
    assert!(!deps_map["source.m.raw.orders"].contains("test.m.source_check"));
    topological_levels(&deps_map).unwrap();
}

/// Unit tests are wired by `inject_unit_test_gates` (they run before the
/// model they test) and are not gated by data tests on their model's inputs.
#[test]
fn inject_test_gates_leaves_unit_tests_alone() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.upstream", &[]);
    add_model(&mut nodes, "model.m.tested", &["model.m.upstream"]);
    add_unit_test_node(&mut nodes, "unit_test.m.u", &["model.m.upstream"]);
    add_test(&mut nodes, "test.m.upstream_check", &["model.m.upstream"]);
    let selected = selected(&[
        "model.m.upstream",
        "model.m.tested",
        "unit_test.m.u",
        "test.m.upstream_check",
    ]);

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["model.m.tested"].contains("test.m.upstream_check"));
    assert_eq!(deps_map["unit_test.m.u"], BTreeSet::from(["model.m.upstream".to_string()]));
    assert_eq!(deps_map["test.m.upstream_check"].len(), 1);
}

/// Selection order must not change the planned edges.
#[test]
fn inject_test_gates_is_independent_of_selection_order() {
    let (nodes, selected) = gate_fixture("random", 40, 25, 0x00C0_FFEE);
    let mut reversed = selected.clone();
    reversed.reverse();

    let mut forward = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut forward);
    let mut backward = build_dependency_map(&nodes, &reversed);
    inject_test_gates(&nodes, &reversed, &mut backward);

    assert_eq!(forward, backward);
}

#[test]
fn ancestor_index_answers_for_unknown_nodes() {
    let graph = deps(&[("b", &["a"]), ("a", &[])]);
    let index = AncestorIndex::new(&graph, &["b"]);
    let a = "a".to_string();
    let missing = "missing".to_string();

    assert!(index.has_all_ancestors("b", [&a]));
    assert!(index.has_all_ancestors("b", []));
    assert!(!index.has_all_ancestors("b", [&a, &missing]));
    assert!(!index.has_all_ancestors("missing", [&a]));
    assert!(!index.has_all_ancestors("a", [&a]));
}

/// Phantom test nodes (zero `depends_on` — issue #2's symptom) must not
/// disrupt gate injection. They should appear at level 0 with no extra
/// edges added.
#[test]
fn inject_test_gates_tolerates_phantom_test_with_no_deps() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.m.real", &[]);
    add_test(&mut nodes, "test.m.phantom", &[]);

    let selected: Vec<String> = ["model.m.real", "test.m.phantom"]
        .iter()
        .map(ToString::to_string)
        .collect();

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    assert!(deps_map["model.m.real"].is_empty());
    assert!(deps_map["test.m.phantom"].is_empty());

    let levels = topological_levels(&deps_map).unwrap();
    assert_eq!(levels.len(), 1);
    assert!(levels[0].contains(&"model.m.real".to_string()));
    assert!(levels[0].contains(&"test.m.phantom".to_string()));
}

/// Guards against the panda-cascade `logo_activity_model` transitive cycle.
///
/// Reduced topology from the real project. The trigger is a test that refs
/// a *descendant* of a model M, while another node N depends on M without
/// being referenced by the test. Naive gate injection adds `N → T`, but T
/// transitively depends on N via the descendant chain, closing a cycle.
///
/// Concrete trace:
///   activity_events ← event_configuration
///   activity_events ← daily_activity_events
///   daily_activity_events, event_configuration ← logo_metadata
///   daily_activity_events, event_configuration ← _daily_active_users
///   _daily_active_users, logo_metadata ← logo_cohort_metrics
///   logo_cohort_metrics ← logo_metrics
///   test_dau refs daily_activity_events, event_configuration, logo_metrics
///
/// Naively, `logo_metadata → test_dau` is injected (logo_metadata depends
/// on event_configuration which has test_dau). But test_dau transitively
/// depends on logo_metadata via logo_metrics → logo_cohort_metrics →
/// logo_metadata. The transitive ancestor check must catch this.
#[test]
fn inject_test_gates_skips_transitive_back_edge() {
    let mut nodes = Nodes::default();
    add_model(&mut nodes, "model.l.activity_events", &[]);
    add_model(&mut nodes, "model.l.event_configuration", &["model.l.activity_events"]);
    add_model(&mut nodes, "model.l.daily_activity_events", &["model.l.activity_events"]);
    add_model(
        &mut nodes,
        "model.l.logo_metadata",
        &[
            "model.l.daily_activity_events",
            "model.l.event_configuration",
        ],
    );
    add_model(
        &mut nodes,
        "model.l._daily_active_users",
        &[
            "model.l.daily_activity_events",
            "model.l.event_configuration",
        ],
    );
    add_model(
        &mut nodes,
        "model.l.logo_cohort_metrics",
        &["model.l._daily_active_users", "model.l.logo_metadata"],
    );
    add_model(&mut nodes, "model.l.logo_metrics", &["model.l.logo_cohort_metrics"]);
    add_test(
        &mut nodes,
        "test.l.test_dau",
        &[
            "model.l.daily_activity_events",
            "model.l.event_configuration",
            "model.l.logo_metrics",
        ],
    );

    let selected: Vec<String> = [
        "model.l.activity_events",
        "model.l.event_configuration",
        "model.l.daily_activity_events",
        "model.l.logo_metadata",
        "model.l._daily_active_users",
        "model.l.logo_cohort_metrics",
        "model.l.logo_metrics",
        "test.l.test_dau",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let mut deps_map = build_dependency_map(&nodes, &selected);
    inject_test_gates(&nodes, &selected, &mut deps_map);

    // Every model on the path test_dau → … → logo_metadata is a transitive
    // ancestor of test_dau. Gating any of them on test_dau would cycle.
    for upstream in [
        "model.l.activity_events",
        "model.l.event_configuration",
        "model.l.daily_activity_events",
        "model.l.logo_metadata",
        "model.l._daily_active_users",
        "model.l.logo_cohort_metrics",
    ] {
        assert!(
            !deps_map[upstream].contains("test.l.test_dau"),
            "{upstream} is a transitive ancestor of test_dau — gating it would cycle"
        );
    }

    // The graph must planarise.
    let levels = topological_levels(&deps_map).unwrap();
    let level_of = |id: &str| {
        levels
            .iter()
            .position(|l| l.iter().any(|n| n == id))
            .unwrap()
    };
    // test_dau still runs after every model it depends on, transitively.
    let test_level = level_of("test.l.test_dau");
    for ancestor in [
        "model.l.activity_events",
        "model.l.event_configuration",
        "model.l.daily_activity_events",
        "model.l.logo_metadata",
        "model.l._daily_active_users",
        "model.l.logo_cohort_metrics",
        "model.l.logo_metrics",
    ] {
        assert!(level_of(ancestor) < test_level, "{ancestor} should run before test.l.test_dau");
    }
}
