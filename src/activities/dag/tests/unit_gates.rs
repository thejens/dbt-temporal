use dbt_schemas::schemas::nodes::DbtUnitTest;

use super::*;

fn add_unit_test(nodes: &mut Nodes, unique_id: &str, tested_model: &str) {
    let mut unit = DbtUnitTest {
        tested_node_unique_id: Some(tested_model.to_string()),
        ..Default::default()
    };
    unique_id.clone_into(&mut unit.__common_attr__.unique_id);
    nodes
        .unit_tests
        .insert(unique_id.to_string(), Arc::new(unit));
}

fn deps(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
    entries
        .iter()
        .map(|(node, node_deps)| {
            (node.to_string(), node_deps.iter().map(ToString::to_string).collect())
        })
        .collect()
}

fn selected(ids: &[&str]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

#[test]
fn unit_test_gates_model_and_inherits_inputs() {
    let mut nodes = Nodes::default();
    add_unit_test(&mut nodes, "unit_test.p.u", "model.p.m");
    let sel = selected(&["model.p.input", "model.p.m", "unit_test.p.u"]);
    let mut d = deps(&[
        ("model.p.input", &[]),
        ("model.p.m", &["model.p.input"]),
        ("unit_test.p.u", &["model.p.m"]),
    ]);
    inject_unit_test_gates(&nodes, &sel, &mut d);

    // The unit test does not wait for the model; it inherits the
    // model's inputs and gates the model instead.
    assert_eq!(d["unit_test.p.u"], deps(&[("x", &["model.p.input"])])["x"]);
    assert!(d["model.p.m"].contains("unit_test.p.u"));
    assert!(d["model.p.m"].contains("model.p.input"));
    // No cycle: levels must compute.
    topological_levels(&d).unwrap();
}

#[test]
fn two_unit_tests_on_same_model_do_not_chain() {
    let mut nodes = Nodes::default();
    add_unit_test(&mut nodes, "unit_test.p.u1", "model.p.m");
    add_unit_test(&mut nodes, "unit_test.p.u2", "model.p.m");
    let sel = selected(&["model.p.m", "unit_test.p.u1", "unit_test.p.u2"]);
    let mut d = deps(&[
        ("model.p.m", &[]),
        ("unit_test.p.u1", &["model.p.m"]),
        ("unit_test.p.u2", &["model.p.m"]),
    ]);
    inject_unit_test_gates(&nodes, &sel, &mut d);

    // Both unit tests run first, in parallel.
    assert!(d["unit_test.p.u1"].is_empty());
    assert!(d["unit_test.p.u2"].is_empty());
    assert!(d["model.p.m"].contains("unit_test.p.u1"));
    assert!(d["model.p.m"].contains("unit_test.p.u2"));
    topological_levels(&d).unwrap();
}

#[test]
fn unit_test_without_selected_model_runs_standalone() {
    // `dbt test`-style selection: the model is not in the plan, so the
    // unit test must not gain edges to it.
    let mut nodes = Nodes::default();
    add_unit_test(&mut nodes, "unit_test.p.u", "model.p.m");
    let sel = selected(&["unit_test.p.u"]);
    let mut d = deps(&[("unit_test.p.u", &[])]);
    inject_unit_test_gates(&nodes, &sel, &mut d);

    assert!(d["unit_test.p.u"].is_empty());
    assert!(!d.contains_key("model.p.m"));
    topological_levels(&d).unwrap();
}

#[test]
fn non_unit_test_nodes_are_untouched() {
    let nodes = Nodes::default();
    let sel = selected(&["model.p.a", "model.p.b"]);
    let mut d = deps(&[("model.p.a", &[]), ("model.p.b", &["model.p.a"])]);
    let before = d.clone();
    inject_unit_test_gates(&nodes, &sel, &mut d);
    assert_eq!(d, before);
}
