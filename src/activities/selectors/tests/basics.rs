use super::*;

#[test]
fn apply_selectors_no_filters_returns_input() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids.clone(), &nodes, None, None, None, &no_expansion).unwrap();
    assert_eq!(out, ids);
}

#[test]
fn apply_selectors_filters_by_tag() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids, &nodes, Some("tag:nightly"), None, None, &no_expansion).unwrap();
    assert_eq!(
        out,
        vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string(),
        ]
    );
}

#[test]
fn apply_selectors_filters_by_fqn_name_exact() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids, &nodes, Some("stg_orders"), None, None, &no_expansion).unwrap();
    assert_eq!(out, vec!["model.shop.stg_orders".to_string()]);
}

/// The matcher must not substring-match the dotted FQN, or `customers`
/// would also pull in `stg_customers`.
#[test]
fn apply_selectors_fqn_name_does_not_substring_match() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids, &nodes, Some("customers"), None, None, &no_expansion).unwrap();
    assert_eq!(out, vec!["model.shop.customers".to_string()]);
}

#[test]
fn apply_selectors_fqn_matches_dotted_path_with_and_without_package() {
    let (ids, nodes) = build_three_model_project();
    let expected = vec![
        "model.shop.stg_customers".to_string(),
        "model.shop.stg_orders".to_string(),
    ];

    let with_package =
        apply_selectors(ids.clone(), &nodes, Some("shop.staging"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(with_package, expected);

    let without_package =
        apply_selectors(ids, &nodes, Some("staging"), None, None, &no_expansion).unwrap();
    assert_eq!(without_package, expected);
}

#[test]
fn apply_selectors_fqn_supports_wildcards() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids, &nodes, Some("stg_*"), None, None, &no_expansion).unwrap();
    assert_eq!(
        out,
        vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string()
        ]
    );
}

#[test]
fn apply_selectors_filters_by_path_prefix() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids, &nodes, Some("path:models/staging"), None, None, &no_expansion)
        .unwrap();
    assert_eq!(
        out,
        vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string(),
        ]
    );
}

/// A value carrying a path separator parses as `path:` with no prefix.
#[test]
fn apply_selectors_bare_path_value_selects_by_path() {
    let (ids, nodes) = build_three_model_project();
    let out =
        apply_selectors(ids, &nodes, Some("models/staging"), None, None, &no_expansion).unwrap();
    assert_eq!(
        out,
        vec![
            "model.shop.stg_customers".to_string(),
            "model.shop.stg_orders".to_string(),
        ]
    );
}

/// And one ending in `.sql` parses as `file:`, which selects a node
/// rather than being turned away as unevaluable.
#[test]
fn apply_selectors_file_method_and_its_bare_sql_spelling() {
    let (ids, nodes) = build_three_model_project();
    let bare =
        apply_selectors(ids.clone(), &nodes, Some("customers.sql"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(bare, vec!["model.shop.customers".to_string()]);

    let prefixed = apply_selectors(
        ids.clone(),
        &nodes,
        Some("file:stg_orders.sql"),
        None,
        None,
        &no_expansion,
    )
    .unwrap();
    assert_eq!(prefixed, vec!["model.shop.stg_orders".to_string()]);

    let none =
        apply_selectors(ids, &nodes, Some("file:absent.sql"), None, None, &no_expansion).unwrap();
    assert!(none.is_empty(), "{none:?}");
}

#[test]
fn apply_selectors_filters_by_package() {
    let (ids, nodes) = build_three_model_project();
    let out =
        apply_selectors(ids, &nodes, Some("package:shop"), None, None, &no_expansion).unwrap();
    assert_eq!(out.len(), 3);
}

/// `package:this` names the root project, which the parsed node set carries.
#[test]
fn apply_selectors_package_this_resolves_to_the_root_project() {
    let (ids, mut nodes) = build_three_model_project();
    let all = apply_selectors(ids.clone(), &nodes, Some("package:this"), None, None, &no_expansion)
        .unwrap();
    assert_eq!(all.len(), 3);

    nodes.project_name = Some("marketing".to_string());
    let none =
        apply_selectors(ids, &nodes, Some("package:this"), None, None, &no_expansion).unwrap();
    assert!(none.is_empty(), "{none:?}");
}

#[test]
fn apply_selectors_excludes_subset() {
    let (ids, nodes) = build_three_model_project();
    let out = apply_selectors(ids, &nodes, None, Some("tag:nightly"), None, &no_expansion).unwrap();
    assert_eq!(out, vec!["model.shop.customers".to_string()]);
}

#[test]
fn apply_selectors_select_and_exclude_combined() {
    let (ids, nodes) = build_three_model_project();
    let out =
        apply_selectors(ids, &nodes, Some("tag:nightly"), Some("tag:hourly"), None, &no_expansion)
            .unwrap();
    assert_eq!(out, vec!["model.shop.stg_customers".to_string()]);
}

#[test]
fn apply_selectors_intersection_via_comma() {
    let (ids, nodes) = build_three_model_project();
    // Comma is intersection: nodes that are nightly AND hourly.
    let out =
        apply_selectors(ids, &nodes, Some("tag:nightly,tag:hourly"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(out, vec!["model.shop.stg_orders".to_string()]);
}

#[test]
fn apply_selectors_union_via_space() {
    let (ids, nodes) = build_three_model_project();
    // Whitespace-separated tokens are union (each parsed independently).
    let out =
        apply_selectors(ids, &nodes, Some("tag:hourly tag:nightly"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(out.len(), 3);
}
