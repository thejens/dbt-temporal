use super::*;

/// Build a 4-node DAG: stg_customers, stg_orders -> orders -> ar_summary
fn build_dag_project() -> (Vec<String>, Nodes) {
    use dbt_common::CodeLocationWithFile;
    use dbt_schemas::schemas::common::NodeDependsOn;

    let mut nodes = Nodes::default();

    let mk = |uid: &str, name: &str, depends_on_uids: &[&str]| {
        let mut model = (*model_node(uid, name)).clone();
        model.__base_attr__.depends_on = NodeDependsOn {
            nodes_with_ref_location: depends_on_uids
                .iter()
                .map(|dep| ((*dep).to_string(), CodeLocationWithFile::default()))
                .collect(),
            ..NodeDependsOn::default()
        };
        Arc::new(model)
    };

    nodes.models.insert(
        "model.shop.stg_customers".to_string(),
        mk("model.shop.stg_customers", "stg_customers", &[]),
    );
    nodes.models.insert(
        "model.shop.stg_orders".to_string(),
        mk("model.shop.stg_orders", "stg_orders", &[]),
    );
    nodes.models.insert(
        "model.shop.orders".to_string(),
        mk("model.shop.orders", "orders", &["model.shop.stg_orders"]),
    );
    nodes.models.insert(
        "model.shop.ar_summary".to_string(),
        mk(
            "model.shop.ar_summary",
            "ar_summary",
            &["model.shop.orders", "model.shop.stg_customers"],
        ),
    );

    let ids = vec![
        "model.shop.stg_customers".to_string(),
        "model.shop.stg_orders".to_string(),
        "model.shop.orders".to_string(),
        "model.shop.ar_summary".to_string(),
    ];
    (ids, nodes)
}

#[test]
fn apply_selectors_parents_walks_upstream() {
    let (ids, nodes) = build_dag_project();
    // `+ar_summary` selects ar_summary plus all transitive parents.
    let out = apply_selectors(ids, &nodes, Some("+ar_summary"), None, None, &no_expansion).unwrap();
    let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
    assert!(set.contains("model.shop.ar_summary"));
    assert!(set.contains("model.shop.orders"));
    assert!(set.contains("model.shop.stg_orders"));
    assert!(set.contains("model.shop.stg_customers"));
}

#[test]
fn apply_selectors_parents_depth_limited() {
    let (ids, nodes) = build_dag_project();
    // `1+ar_summary` selects ar_summary plus direct parents only.
    let out =
        apply_selectors(ids, &nodes, Some("1+ar_summary"), None, None, &no_expansion).unwrap();
    let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
    assert!(set.contains("model.shop.ar_summary"));
    assert!(set.contains("model.shop.orders"));
    assert!(set.contains("model.shop.stg_customers"));
    // stg_orders is two hops away → excluded.
    assert!(!set.contains("model.shop.stg_orders"));
}

#[test]
fn apply_selectors_children_walks_downstream() {
    let (ids, nodes) = build_dag_project();
    // `stg_orders+` selects stg_orders plus everything downstream.
    let out = apply_selectors(ids, &nodes, Some("stg_orders+"), None, None, &no_expansion).unwrap();
    let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
    assert!(set.contains("model.shop.stg_orders"));
    assert!(set.contains("model.shop.orders"));
    assert!(set.contains("model.shop.ar_summary"));
    // stg_customers is unrelated → excluded.
    assert!(!set.contains("model.shop.stg_customers"));
}

#[test]
fn apply_selectors_children_depth_limited() {
    let (ids, nodes) = build_dag_project();
    // `stg_orders+1` selects stg_orders + direct children only.
    let out =
        apply_selectors(ids, &nodes, Some("stg_orders+1"), None, None, &no_expansion).unwrap();
    let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
    assert!(set.contains("model.shop.stg_orders"));
    assert!(set.contains("model.shop.orders"));
    // ar_summary is two hops away → excluded.
    assert!(!set.contains("model.shop.ar_summary"));
}

/// `@orders` is orders, everything downstream of it, and everything those
/// descendants are built from — not just one hop in each direction.
#[test]
fn apply_selectors_at_operator_selects_descendants_and_their_ancestors() {
    let (ids, nodes) = build_dag_project();
    let out = apply_selectors(ids, &nodes, Some("@orders"), None, None, &no_expansion).unwrap();
    let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
    assert_eq!(
        set,
        BTreeSet::from([
            "model.shop.orders",
            "model.shop.ar_summary",
            "model.shop.stg_orders",
            // ar_summary's other parent, two hops from the seed.
            "model.shop.stg_customers",
        ])
    );
}

/// The seed's own ancestors that feed no descendant stay out: `@stg_customers`
/// takes ar_summary and what ar_summary needs, which includes orders.
#[test]
fn apply_selectors_at_operator_follows_every_matched_seed() {
    let (ids, nodes) = build_dag_project();
    let out = apply_selectors(
        ids,
        &nodes,
        Some("@stg_customers"),
        Some("stg_orders"),
        None,
        &no_expansion,
    )
    .unwrap();
    let set: BTreeSet<&str> = out.iter().map(String::as_str).collect();
    assert_eq!(
        set,
        BTreeSet::from([
            "model.shop.stg_customers",
            "model.shop.ar_summary",
            "model.shop.orders",
        ])
    );
}

/// `@` composes with methods like any other graph operator.
#[test]
fn apply_selectors_at_operator_works_with_methods_and_intersection() {
    let (ids, mut nodes) = build_dag_project();
    let tagged = nodes
        .models
        .get_mut("model.shop.stg_orders")
        .map(|m| {
            let mut model = (**m).clone();
            model.__common_attr__.tags = vec!["nightly".to_string()];
            Arc::new(model)
        })
        .unwrap();
    nodes
        .models
        .insert("model.shop.stg_orders".to_string(), tagged);

    let by_tag =
        apply_selectors(ids.clone(), &nodes, Some("@tag:nightly"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(
        by_tag.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "model.shop.stg_orders",
            "model.shop.orders",
            "model.shop.ar_summary",
            "model.shop.stg_customers",
        ])
    );

    let intersected =
        apply_selectors(ids, &nodes, Some("@tag:nightly,stg_customers"), None, None, &no_expansion)
            .unwrap();
    assert_eq!(intersected, vec!["model.shop.stg_customers".to_string()]);
}

/// With a `+` operator present the `@` is ignored, as dbt v2 does.
#[test]
fn apply_selectors_plus_operators_take_precedence_over_at() {
    let (ids, nodes) = build_dag_project();
    let out = apply_selectors(ids, &nodes, Some("@+orders"), None, None, &no_expansion).unwrap();
    assert_eq!(
        out.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from(["model.shop.orders", "model.shop.stg_orders"])
    );
}

/// Several matched seeds, depth limits and both directions through the
/// public entry point: `tag:`-style multi-seed selection agrees with the
/// union of the single-seed selections.
#[test]
fn apply_selectors_multi_seed_depth_limits_match_the_union_of_single_seeds() {
    let (ids, nodes) = build_dag_project();
    for spec in ["{}+1", "1+{}", "{}+", "+{}", "2+{}"] {
        let single = |name: &str| {
            apply_selectors(
                ids.clone(),
                &nodes,
                Some(&spec.replace("{}", name)),
                None,
                None,
                &no_expansion,
            )
            .unwrap()
            .into_iter()
            .collect::<BTreeSet<_>>()
        };
        let union: BTreeSet<String> = ["stg_orders", "stg_customers"]
            .into_iter()
            .flat_map(single)
            .collect();
        let combined = apply_selectors(
            ids.clone(),
            &nodes,
            Some(&spec.replace("{}", "stg_*")),
            None,
            None,
            &no_expansion,
        )
        .unwrap()
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(combined, union, "{spec}");
    }
}

/// The nested exclude removes from the expanded selection.
#[test]
fn a_nested_exclude_applies_after_graph_expansion() {
    let (_, nodes) = build_dag_project();
    let parsed = parse_selector(Some("orders+")).unwrap().unwrap();
    let SelectExpression::Atom(mut criteria) = parsed else {
        panic!("single selector parses to an atom");
    };
    criteria.exclude = parse_selector(Some("ar_summary")).unwrap().map(Box::new);

    let graph = LazyDependencyMaps::new(&nodes);
    let out = resolve_criteria(&nodes, &graph, NO_STATE, &criteria).unwrap();
    assert_eq!(out, BTreeSet::from(["model.shop.orders".to_string()]));
}

const NO_STATE: MatchContext<'static> = MatchContext {
    state: None,
    project_name: None,
};

fn exclude_expression() -> SelectExpression {
    let inner = parse_selector(Some("orders")).unwrap().unwrap();
    SelectExpression::Exclude(Box::new(inner))
}

/// The text parser cannot build an `Exclude` node, so one reaching the
/// planner is refused rather than read as an include.
#[test]
fn an_exclude_expression_is_rejected_wherever_it_sits() {
    let atom = parse_selector(Some("stg_orders")).unwrap().unwrap();
    for expr in [
        exclude_expression(),
        SelectExpression::And(vec![atom.clone(), exclude_expression()]),
        SelectExpression::Or(vec![atom, exclude_expression()]),
    ] {
        let err = validate_selector_support(&expr).unwrap_err().to_string();
        assert!(err.contains("not supported"), "{err}");

        let (_, nodes) = build_dag_project();
        let graph = LazyDependencyMaps::new(&nodes);
        if matches!(expr, SelectExpression::Exclude(_)) {
            let err = resolve_expression(&nodes, &graph, NO_STATE, &expr)
                .unwrap_err()
                .to_string();
            assert!(err.contains("not supported"), "{err}");
        }
    }
}

#[test]
fn nested_exclude_inside_an_atom_is_checked_for_expression_excludes() {
    let parsed = parse_selector(Some("orders")).unwrap().unwrap();
    let SelectExpression::Atom(mut criteria) = parsed else {
        panic!("single selector parses to an atom");
    };
    criteria.exclude = Some(Box::new(exclude_expression()));
    assert!(validate_selector_support(&SelectExpression::Atom(criteria)).is_err());
}

/// The text parser's output never contains an `Exclude` node.
#[test]
fn parsed_selector_text_never_yields_an_exclude_expression() {
    let expr = parse_selector(Some("a,b c+ @d,e")).unwrap().unwrap();
    assert!(validate_selector_support(&expr).is_ok());
}

#[test]
fn selectors_use_state_reads_the_parsed_method() {
    assert!(selectors_use_state(Some("state:modified+"), None).unwrap());
    assert!(selectors_use_state(None, Some("state:new")).unwrap());
    assert!(selectors_use_state(Some("tag:nightly state:new"), None).unwrap());
    assert!(selectors_use_state(Some("tag:a,state:modified"), None).unwrap());

    // Mentions of the word that are not the method.
    assert!(!selectors_use_state(Some("tag:my_state:thing"), None).unwrap());
    assert!(!selectors_use_state(Some("models/state:dir"), None).unwrap());
    assert!(!selectors_use_state(Some("tag:a_state:x"), Some("tag:state:y")).unwrap());
    assert!(!selectors_use_state(None, None).unwrap());
}

#[test]
fn selectors_use_state_reports_which_flag_is_malformed() {
    let err = format!("{:#}", selectors_use_state(Some("@a+"), None).unwrap_err());
    assert!(err.contains("invalid --select"), "{err}");
    let err = format!("{:#}", selectors_use_state(None, Some("@a+")).unwrap_err());
    assert!(err.contains("invalid --exclude"), "{err}");
}
