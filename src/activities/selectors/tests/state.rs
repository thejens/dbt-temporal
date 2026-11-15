use super::*;

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
fn state_selectors_filter_by_the_previous_manifest_comparison() {
    let nodes = state_nodes();
    let state = StateSelector::from_previous_manifest(&nodes, &previous_manifest());
    let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    let select = |sel: &str| {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, Some(&state), &no_expansion).unwrap()
    };

    assert_eq!(
        select("state:modified"),
        vec![
            "model.shop.brand_new".to_string(),
            "model.shop.edited".to_string()
        ]
    );
    assert_eq!(select("state:new"), vec!["model.shop.brand_new".to_string()]);
    // Every `modified.<sub>` coarsens to the full modified set.
    assert_eq!(select("state:modified.body").len(), 2);
    assert_eq!(select("state:unmodified"), vec!["model.shop.unchanged".to_string()]);
    assert_eq!(
        select("state:old"),
        vec![
            "model.shop.edited".to_string(),
            "model.shop.unchanged".to_string()
        ]
    );
}

#[test]
fn supported_state_subselectors_are_accepted() {
    let nodes = state_nodes();
    let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    for sel in [
        "state:new",
        "state:modified",
        "state:modified.body",
        "state:old",
        "state:unmodified",
    ] {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion)
            .unwrap_or_else(|e| panic!("{sel} should be accepted: {e:#}"));
    }
}

#[test]
fn unsupported_state_subselector_is_rejected() {
    let nodes = state_nodes();
    let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    for selector in ["state:sideways", "state:modified.bdoy"] {
        let err = apply_selectors(ids.clone(), &nodes, Some(selector), None, None, &no_expansion)
            .expect_err("has no backing set");
        assert!(format!("{err:#}").contains(selector), "got: {err:#}");
    }
}

/// These name dimensions the comparison does not read. Answering them with
/// the body comparison is a narrowing, not a coarsening: `modified.configs`
/// would match only nodes whose *body* changed, so a config-only change —
/// the thing it was asked about — selected nothing at all.
#[test]
fn a_modified_subselector_this_comparison_cannot_decide_is_rejected() {
    let nodes = state_nodes();
    let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    for sub in [
        "configs",
        "relation",
        "persisted_descriptions",
        "macros",
        "contract",
    ] {
        let selector = format!("state:modified.{sub}");
        let err = apply_selectors(ids.clone(), &nodes, Some(&selector), None, None, &no_expansion)
            .expect_err("this comparison cannot decide it");
        let msg = format!("{err:#}");
        assert!(msg.contains(sub), "should name the dimension: {msg}");
        assert!(msg.contains("state:modified.body"), "should point at what it can do: {msg}");
    }
}

#[test]
fn state_selector_without_sets_matches_nothing() {
    let nodes = state_nodes();
    let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    let out =
        apply_selectors(ids, &nodes, Some("state:modified"), None, None, &no_expansion).unwrap();
    assert!(out.is_empty(), "{out:?}");
}
