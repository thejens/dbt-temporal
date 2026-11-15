use super::*;

/// A project whose three models differ only in the config keys under test.
fn build_config_project() -> (Vec<String>, Nodes) {
    let mut nodes = Nodes {
        project_name: Some("shop".to_string()),
        ..Nodes::default()
    };
    nodes.models.insert(
        "model.shop.a".to_string(),
        model_with_config(
            "model.shop.a",
            "a",
            "materialized: view\nschema: audit\nmeta:\n  owner: finance\n",
        ),
    );
    nodes.models.insert(
        "model.shop.b".to_string(),
        model_with_config(
            "model.shop.b",
            "b",
            "materialized: incremental\nschema: staging\nunique_key:\n  - order_id\n",
        ),
    );
    let ids = nodes.iter().map(|(id, _)| id.clone()).collect();
    (ids, nodes)
}

#[test]
fn apply_selectors_filters_by_config_materialized() {
    let (ids, nodes) = build_config_project();
    let out = apply_selectors(
        ids.clone(),
        &nodes,
        Some("config.materialized:view"),
        None,
        None,
        &no_expansion,
    )
    .unwrap();
    assert_eq!(out, vec!["model.shop.a".to_string()]);

    let none =
        apply_selectors(ids, &nodes, Some("config.materialized:table"), None, None, &no_expansion)
            .unwrap();
    assert!(none.is_empty(), "{none:?}");
}

/// The generalisation: any config key, nested keys, and list-valued keys.
#[test]
fn apply_selectors_filters_by_arbitrary_config_keys() {
    let (ids, nodes) = build_config_project();
    let select = |sel: &str| {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
    };

    assert_eq!(select("config.schema:audit"), vec!["model.shop.a".to_string()]);
    assert_eq!(select("config.schema:marts"), Vec::<String>::new());

    assert_eq!(select("config.meta.owner:finance"), vec!["model.shop.a".to_string()]);
    assert_eq!(select("config.meta.owner:marketing"), Vec::<String>::new());

    assert_eq!(select("config.unique_key:order_id"), vec!["model.shop.b".to_string()]);
    assert_eq!(select("config.unique_key:customer_id"), Vec::<String>::new());
}

/// A config key no node sets is a legitimately empty selection, not an
/// unevaluable method — the distinction the rejection path exists to draw.
#[test]
fn apply_selectors_unknown_config_key_selects_nothing_without_erroring() {
    let (ids, nodes) = build_config_project();
    let out = apply_selectors(ids, &nodes, Some("config.not_a_key:foo"), None, None, &no_expansion)
        .unwrap();
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn apply_selectors_config_naming_no_key_is_rejected() {
    let (ids, nodes) = build_config_project();
    let err = apply_selectors(ids, &nodes, Some("config:not_a_key"), None, None, &no_expansion)
        .expect_err("a config criterion naming no key must be rejected");
    assert!(err.to_string().contains("--select"), "got: {err:#}");
}

/// One node of every kind a resource-specific method names, so both a match
/// and a non-match are observable for each.
fn build_mixed_resource_project() -> (Vec<String>, Nodes) {
    let mut nodes = Nodes {
        project_name: Some("shop".to_string()),
        ..Nodes::default()
    };
    nodes
        .models
        .insert("model.shop.orders".to_string(), model_node("model.shop.orders", "orders"));
    nodes.sources.insert(
        "source.shop.raw.orders".to_string(),
        source_node("source.shop.raw.orders", "orders", "raw"),
    );
    nodes.exposures.insert(
        "exposure.shop.weekly".to_string(),
        exposure_node("exposure.shop.weekly", "weekly"),
    );
    nodes
        .metrics
        .insert("metric.shop.revenue".to_string(), metric_node("metric.shop.revenue", "revenue"));
    nodes.tests.insert(
        "test.shop.nn_orders".to_string(),
        generic_test_node("test.shop.nn_orders", "nn_orders", "not_null"),
    );
    nodes.tests.insert(
        "test.shop.assert_totals".to_string(),
        singular_test_node("test.shop.assert_totals", "assert_totals"),
    );

    let ids = nodes.iter().map(|(id, _)| id.clone()).collect();
    (ids, nodes)
}

#[test]
fn apply_selectors_source_method() {
    let (ids, nodes) = build_mixed_resource_project();
    let out =
        apply_selectors(ids.clone(), &nodes, Some("source:raw.orders"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(out, vec!["source.shop.raw.orders".to_string()]);

    let none =
        apply_selectors(ids, &nodes, Some("source:raw.customers"), None, None, &no_expansion)
            .unwrap();
    assert!(none.is_empty(), "{none:?}");
}

#[test]
fn apply_selectors_exposure_and_metric_methods() {
    let (ids, nodes) = build_mixed_resource_project();
    let select = |sel: &str| {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
    };

    assert_eq!(select("exposure:weekly"), vec!["exposure.shop.weekly".to_string()]);
    assert_eq!(select("exposure:monthly"), Vec::<String>::new());
    assert_eq!(select("metric:revenue"), vec!["metric.shop.revenue".to_string()]);
    assert_eq!(select("metric:cost"), Vec::<String>::new());
}

#[test]
fn apply_selectors_test_type_and_test_name_methods() {
    let (ids, nodes) = build_mixed_resource_project();
    let select = |sel: &str| {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
    };

    assert_eq!(select("test_type:generic"), vec!["test.shop.nn_orders".to_string()]);
    assert_eq!(select("test_type:singular"), vec!["test.shop.assert_totals".to_string()]);
    assert_eq!(select("test_type:unit"), Vec::<String>::new());

    // A generic test answers to the macro behind it, not its node name.
    assert_eq!(select("test_name:not_null"), vec!["test.shop.nn_orders".to_string()]);
    assert_eq!(select("test_name:unique"), Vec::<String>::new());
}

#[test]
fn apply_selectors_access_and_group_methods() {
    let (ids, mut nodes) = build_mixed_resource_project();
    let mut orders = (*nodes.models["model.shop.orders"]).clone();
    orders.__model_attr__.group = Some("finance".to_string());
    orders.__model_attr__.access = dbt_schemas::schemas::common::Access::Public;
    nodes
        .models
        .insert("model.shop.orders".to_string(), Arc::new(orders));

    let select = |sel: &str| {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
    };

    assert_eq!(select("group:finance"), vec!["model.shop.orders".to_string()]);
    assert_eq!(select("group:marketing"), Vec::<String>::new());
    assert_eq!(select("access:public"), vec!["model.shop.orders".to_string()]);
    assert_eq!(select("access:private"), Vec::<String>::new());
}

#[test]
fn apply_selectors_resource_type_filter() {
    let (ids, nodes) = build_mixed_resource_project();
    let out = apply_selectors(
        ids.clone(),
        &nodes,
        Some("resource_type:source"),
        None,
        None,
        &no_expansion,
    )
    .unwrap();
    assert_eq!(out, vec!["source.shop.raw.orders".to_string()]);

    // `relation` is dbt's alias for everything that is not a test or check.
    let relations =
        apply_selectors(ids, &nodes, Some("resource_type:relation"), None, None, &no_expansion)
            .unwrap();
    assert!(relations.contains(&"model.shop.orders".to_string()));
    assert!(!relations.contains(&"test.shop.nn_orders".to_string()));
}

#[test]
fn apply_selectors_version_method() {
    let mut nodes = Nodes::default();
    nodes.models.insert(
        "model.shop.orders.v1".to_string(),
        versioned_model_node("model.shop.orders.v1", "orders", Some("1"), Some("2")),
    );
    nodes.models.insert(
        "model.shop.orders.v2".to_string(),
        versioned_model_node("model.shop.orders.v2", "orders", Some("2"), Some("2")),
    );
    nodes
        .models
        .insert("model.shop.plain".to_string(), model_node("model.shop.plain", "plain"));
    let ids: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
    let select = |sel: &str| {
        apply_selectors(ids.clone(), &nodes, Some(sel), None, None, &no_expansion).unwrap()
    };

    assert_eq!(select("version:latest"), vec!["model.shop.orders.v2".to_string()]);
    assert_eq!(select("version:old"), vec!["model.shop.orders.v1".to_string()]);
    assert_eq!(select("version:none"), vec!["model.shop.plain".to_string()]);
    assert_eq!(select("version:prerelease"), Vec::<String>::new());
}

// --- unsupported method rejection ---

/// The failure mode this guards: an unevaluable method contributes an empty
/// set, so inside an `or` it silently drops nodes the user asked for.
#[test]
fn unsupported_method_in_union_is_rejected_not_ignored() {
    let (ids, nodes) = build_three_model_project();
    let err =
        apply_selectors(ids, &nodes, Some("tag:nightly result:success"), None, None, &no_expansion)
            .expect_err("result: is not evaluable and must be rejected");
    let msg = format!("{err:#}");
    assert!(msg.contains("result"), "should name the method: {msg}");
    assert!(msg.contains("unsupported selector method"), "got: {msg}");
}

/// And in `--exclude` it silently excludes nothing, which over-selects.
#[test]
fn unsupported_method_in_exclude_is_rejected() {
    let (ids, nodes) = build_three_model_project();
    let err =
        apply_selectors(ids, &nodes, None, Some("source_status:fresher"), None, &no_expansion)
            .expect_err("source_status: is not evaluable and must be rejected");
    let msg = format!("{err:#}");
    assert!(msg.contains("--exclude"), "got: {msg}");
    assert!(msg.contains("source_status"), "should name the method: {msg}");
}

/// Every method left unsupported, each named in the error it produces.
#[test]
fn methods_needing_data_the_planner_lacks_are_all_rejected() {
    let (ids, nodes) = build_three_model_project();
    for (selector, method) in [
        ("result:success", "result"),
        ("source_status:fresher", "source_status"),
        ("column:model.shop.customers.id", "column"),
        ("selector:nightly", "selector"),
    ] {
        let err = apply_selectors(ids.clone(), &nodes, Some(selector), None, None, &no_expansion)
            .expect_err("must be rejected");
        assert!(format!("{err:#}").contains(method), "{selector}: {err:#}");
    }
}

/// A value a resource-specific method cannot read is a rejection too — the
/// selector names no resource at all, so matching nothing would hide it.
#[test]
fn a_malformed_resource_specific_value_is_rejected() {
    let (ids, nodes) = build_three_model_project();
    for selector in ["exposure:a.b.c", "source:a.b.c.d", "test_type:integration"] {
        let err = apply_selectors(ids.clone(), &nodes, Some(selector), None, None, &no_expansion)
            .expect_err("must be rejected");
        assert!(
            format!("{err:#}").contains("unsupported selector method"),
            "{selector}: {err:#}"
        );
    }
}
