use super::super::test_support::jinja_env_with_templates;
use super::*;

#[test]
fn extract_ephemeral_names_finds_each_unique_reference() {
    let sql = "with __dbt__cte__alpha as (select 1), __dbt__cte__beta as (select 2)\n\
               select * from __dbt__cte__alpha join __dbt__cte__beta using(id)";
    let names = extract_ephemeral_names(sql);
    assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
}

#[test]
fn extract_ephemeral_names_returns_empty_when_absent() {
    assert_eq!(extract_ephemeral_names("select 1"), Vec::<String>::new());
}

#[test]
fn upstream_ephemeral_wrap_handles_user_with_clause() -> anyhow::Result<()> {
    // Guards the BigQuery `Expected keyword DEPTH` failure: when
    // the user's compiled SQL starts with `WITH`, naively prepending the
    // ephemeral CTEs produces two `WITH` keywords in a row.
    // BigQuery's parser then tried to read the second `WITH` as part of a
    // recursive CTE's `... CYCLE ... DEPTH` clause and rejected it.
    //
    // The dbt-fusion helper splices the ephemeral CTEs into the user's
    // existing chain, so both become siblings under one `WITH`. The
    // `select * from (...)` wrapper and its `--EPHEMERAL-SELECT-WRAPPER-*`
    // markers are the *other* branch, taken only when there is no leading
    // `WITH` to splice into — see `..._emits_wrapper_markers` below.
    let dir = tempfile::tempdir()?;

    // Simulate the persist step for a single ephemeral named `alpha` —
    // mirrors what we do at runtime in `persist_ephemeral_chain`.
    let mut spans = MacroSpans::default();
    inject_and_persist_ephemeral_models(
        "select 1 as k, 2 as v".to_string(),
        &mut spans,
        "alpha",
        true,
        dir.path(),
    )
    .map_err(|e| anyhow::anyhow!("persist alpha: {e:#}"))?;

    let user_sql = "WITH user_cte AS (\n  SELECT k, v FROM __dbt__cte__alpha\n)\n\
                    SELECT * FROM user_cte";
    let mut user_spans = MacroSpans::default();
    let wrapped = inject_and_persist_ephemeral_models(
        user_sql.to_string(),
        &mut user_spans,
        "user_model",
        false,
        dir.path(),
    )
    .map_err(|e| anyhow::anyhow!("wrap user model: {e:#}"))?;

    assert!(
        wrapped.contains("__dbt__cte__alpha as (\nselect 1 as k, 2 as v\n)"),
        "expected alpha to be inlined; got:\n{wrapped}"
    );
    // Splice, not wrap: the ephemeral lands inside the user's own chain and
    // `user_cte` follows it as a sibling behind a comma.
    assert!(
        wrapped.starts_with("WITH  __dbt__cte__alpha as ("),
        "expected alpha spliced into the leading WITH; got:\n{wrapped}"
    );
    assert!(
        wrapped.contains("), user_cte AS ("),
        "expected user_cte to remain a sibling CTE; got:\n{wrapped}"
    );
    assert!(
        !wrapped.contains("--EPHEMERAL-SELECT-WRAPPER"),
        "splice path must not emit wrapper markers; got:\n{wrapped}"
    );
    // The pathology itself: exactly one `WITH` keyword may open the
    // statement. Two in a row is what BigQuery rejected.
    assert_eq!(
        wrapped.to_ascii_uppercase().matches("WITH").count(),
        1,
        "expected a single WITH keyword; got:\n{wrapped}"
    );
    Ok(())
}

/// Build Nodes containing one ephemeral model whose raw SQL is `body` and
/// whose original_file_path will resolve relative to `in_dir`.
fn build_nodes_with_ephemeral(
    in_dir: &Path,
    name: &str,
    body: &str,
) -> std::io::Result<dbt_schemas::schemas::Nodes> {
    use std::sync::Arc as A;

    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::schemas::common::DbtMaterialization;
    use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

    let rel = format!("models/{name}.sql");
    let abs = in_dir.join(&rel);
    if let Some(parent) = abs.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&abs, body)?;

    let common = CommonAttributes {
        unique_id: format!("model.shop.{name}"),
        name: name.to_string(),
        original_file_path: std::path::PathBuf::from(&rel).into(),
        ..CommonAttributes::default()
    };
    let base = NodeBaseAttributes {
        materialized: DbtMaterialization::Ephemeral,
        ..NodeBaseAttributes::default()
    };
    let model = DbtModel {
        __common_attr__: common,
        __base_attr__: base,
        ..DbtModel::default()
    };

    let mut nodes = Nodes::default();
    nodes
        .models
        .insert(format!("model.shop.{name}"), A::new(model));
    Ok(nodes)
}

#[test]
fn inject_ephemeral_ctes_walks_dependency_and_emits_wrapper_markers() -> anyhow::Result<()> {
    // End-to-end: a user model SQL that refs an ephemeral via the
    // __dbt__cte__alpha prefix. inject_ephemeral_ctes must (a) compile +
    // persist the ephemeral's raw SQL leaf-first, (b) wrap the user SQL
    // with the EPHEMERAL-SELECT-WRAPPER markers.
    let dir = tempfile::tempdir()?;
    let in_dir = dir.path().join("project");
    let ephemeral_dir = dir.path().join("ephemeral");
    std::fs::create_dir_all(&ephemeral_dir)?;

    let nodes = build_nodes_with_ephemeral(&in_dir, "alpha", "select 1 as k, 2 as v")?;
    let env = jinja_env_with_templates(&[]);

    let user_sql = "select * from __dbt__cte__alpha";
    let out = inject_ephemeral_ctes(
        user_sql,
        "user_model",
        &["model.shop.alpha".to_string()],
        &nodes,
        &env,
        EphemeralDirs {
            in_dir: &in_dir,
            ephemeral_dir: &ephemeral_dir,
            context_for: &|_| Ok(BTreeMap::new()),
        },
    )?;

    assert!(
        out.contains("--EPHEMERAL-SELECT-WRAPPER-START"),
        "expected wrapper start marker; got:\n{out}"
    );
    assert!(out.contains("--EPHEMERAL-SELECT-WRAPPER-END"), "got:\n{out}");
    // alpha's compiled body should appear inline.
    assert!(out.contains("select 1 as k, 2 as v"), "got:\n{out}");
    Ok(())
}

#[test]
fn inject_ephemeral_ctes_errors_when_ephemeral_source_missing_on_disk() -> anyhow::Result<()> {
    // Nodes registers an ephemeral model with an original_file_path that
    // doesn't exist on disk → persist_ephemeral_chain fails to read it.
    // The error must mention the model name + the file path so users can
    // diagnose it.
    use std::sync::Arc as A;

    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::schemas::common::DbtMaterialization;
    use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

    let dir = tempfile::tempdir()?;
    let in_dir = dir.path().to_path_buf();
    let ephemeral_dir = dir.path().join("ephemeral");
    std::fs::create_dir_all(&ephemeral_dir)?;

    let common = CommonAttributes {
        unique_id: "model.shop.missing".to_string(),
        name: "missing".to_string(),
        original_file_path: std::path::PathBuf::from("models/missing.sql").into(),
        ..CommonAttributes::default()
    };
    let base = NodeBaseAttributes {
        materialized: DbtMaterialization::Ephemeral,
        ..NodeBaseAttributes::default()
    };
    let model = DbtModel {
        __common_attr__: common,
        __base_attr__: base,
        ..DbtModel::default()
    };
    let mut nodes = Nodes::default();
    nodes
        .models
        .insert("model.shop.missing".to_string(), A::new(model));

    let env = jinja_env_with_templates(&[]);

    let err = inject_ephemeral_ctes(
        "select * from __dbt__cte__missing",
        "user_model",
        &["model.shop.missing".to_string()],
        &nodes,
        &env,
        EphemeralDirs {
            in_dir: &in_dir,
            ephemeral_dir: &ephemeral_dir,
            context_for: &|_| Ok(BTreeMap::new()),
        },
    )
    .expect_err("missing ephemeral source must fail");
    let msg = err.to_string();
    assert!(msg.contains("missing"), "error should mention model name: {msg}");
    Ok(())
}

/// Two packages may each define an ephemeral called `base`. Scanning the
/// whole project for a model with that name picked whichever the node map
/// yielded first, so a model could be compiled around a different
/// package's ephemeral entirely. The depending node's own dependencies say
/// which one it meant.
#[test]
fn inject_ephemeral_ctes_picks_the_dependency_not_the_first_same_named_model() -> anyhow::Result<()>
{
    use std::sync::Arc as A;

    use dbt_schemas::schemas::common::DbtMaterialization;
    use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

    let dir = tempfile::tempdir()?;
    let in_dir = dir.path().join("project");
    let ephemeral_dir = dir.path().join("ephemeral");
    std::fs::create_dir_all(&ephemeral_dir)?;

    // `model.shop.base` sorts before `model.vendor.base`, so a first-match
    // scan finds the wrong one.
    let mut nodes = build_nodes_with_ephemeral(&in_dir, "base", "select 'shop' as who")?;
    let rel = "models/vendor_base.sql";
    let abs = in_dir.join(rel);
    std::fs::write(&abs, "select 'vendor' as who")?;
    nodes.models.insert(
        "model.vendor.base".to_string(),
        A::new(DbtModel {
            __common_attr__: CommonAttributes {
                unique_id: "model.vendor.base".to_string(),
                name: "base".to_string(),
                original_file_path: std::path::PathBuf::from(rel).into(),
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes {
                materialized: DbtMaterialization::Ephemeral,
                ..NodeBaseAttributes::default()
            },
            ..DbtModel::default()
        }),
    );

    let env = jinja_env_with_templates(&[]);
    let out = inject_ephemeral_ctes(
        "select * from __dbt__cte__base",
        "user_model",
        &["model.vendor.base".to_string()],
        &nodes,
        &env,
        EphemeralDirs {
            in_dir: &in_dir,
            ephemeral_dir: &ephemeral_dir,
            context_for: &|_| Ok(BTreeMap::new()),
        },
    )?;

    assert!(out.contains("select 'vendor' as who"), "got:\n{out}");
    assert!(!out.contains("select 'shop' as who"), "got:\n{out}");
    Ok(())
}

/// Only the node's own dependencies are candidates: a nested ephemeral is
/// found when the walk reaches its parent, and a non-ephemeral dependency
/// is never a candidate.
#[test]
fn ephemeral_dependencies_returns_only_direct_ephemeral_dependencies() {
    use std::sync::Arc as A;

    use dbt_schemas::schemas::common::DbtMaterialization;
    use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

    let model = |name: &str, materialized| {
        A::new(DbtModel {
            __common_attr__: CommonAttributes {
                unique_id: format!("model.shop.{name}"),
                name: name.to_string(),
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes {
                materialized,
                ..NodeBaseAttributes::default()
            },
            ..DbtModel::default()
        })
    };
    let mut nodes = dbt_schemas::schemas::Nodes::default();
    for (name, materialized) in [
        ("outer", DbtMaterialization::Ephemeral),
        ("inner", DbtMaterialization::Ephemeral),
        ("table_dep", DbtMaterialization::Table),
        ("unrelated", DbtMaterialization::Ephemeral),
    ] {
        nodes
            .models
            .insert(format!("model.shop.{name}"), model(name, materialized));
    }

    // `outer` depends on `inner`; the user node depends on `outer` and a
    // table, and names `outer` twice and an id absent from the project.
    let deps: Vec<String> = [
        "model.shop.outer",
        "model.shop.table_dep",
        "model.shop.outer",
        "model.shop.missing",
    ]
    .map(String::from)
    .to_vec();
    let found = ephemeral_dependencies(&deps, &nodes).expect("no name clash");
    let entries: Vec<(&str, &str)> = found.iter().map(|(n, (id, _))| (*n, *id)).collect();
    assert_eq!(entries, vec![("outer", "model.shop.outer")]);

    // Walking into the nested ephemeral resolves its own dependencies.
    let inner_deps = vec!["model.shop.inner".to_string()];
    let found = ephemeral_dependencies(&inner_deps, &nodes).expect("no name clash");
    assert_eq!(found.keys().copied().collect::<Vec<_>>(), vec!["inner"]);
}

/// A CTE marker naming a dependency that is not ephemeral has no body to
/// inline. Skipping it would defer the failure to a missing persisted file,
/// so it is reported against the name in the SQL.
#[test]
fn inject_ephemeral_ctes_reports_a_non_ephemeral_dependency() -> anyhow::Result<()> {
    use std::sync::Arc as A;

    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

    let dir = tempfile::tempdir()?;
    let in_dir = dir.path().join("project");
    std::fs::create_dir_all(&in_dir)?;
    let ephemeral_dir = dir.path().join("ephemeral");
    std::fs::create_dir_all(&ephemeral_dir)?;

    // Default DbtModel is materialized = Snapshot (not Ephemeral).
    let common = CommonAttributes {
        unique_id: "model.shop.alpha".to_string(),
        name: "alpha".to_string(),
        ..CommonAttributes::default()
    };
    let model = DbtModel {
        __common_attr__: common,
        ..DbtModel::default()
    };
    let mut nodes = Nodes::default();
    nodes
        .models
        .insert("model.shop.alpha".to_string(), A::new(model));

    let env = jinja_env_with_templates(&[]);

    let err = inject_ephemeral_ctes(
        "select * from __dbt__cte__alpha",
        "user_model",
        &["model.shop.alpha".to_string()],
        &nodes,
        &env,
        EphemeralDirs {
            in_dir: &in_dir,
            ephemeral_dir: &ephemeral_dir,
            context_for: &|_| Ok(BTreeMap::new()),
        },
    )
    .expect_err("a non-ephemeral dependency has no CTE body to inline");
    let msg = err.to_string();
    assert!(msg.contains("alpha"), "should name the ephemeral in the SQL: {msg}");
    Ok(())
}

#[test]
fn inject_ephemeral_ctes_short_circuits_when_prefix_absent() -> anyhow::Result<()> {
    // Early return path: SQL without DBT_CTE_PREFIX — neither dependency
    // walk nor wrap step runs, returns the input verbatim. This is the
    // common case for non-ephemeral nodes.
    let dir = tempfile::tempdir()?;
    let nodes = dbt_schemas::schemas::Nodes::default();
    let env = jinja_env_with_templates(&[]);

    let plain = "select 1 as id";
    let out = inject_ephemeral_ctes(
        plain,
        "user_model",
        &[],
        &nodes,
        &env,
        EphemeralDirs {
            in_dir: dir.path(),
            ephemeral_dir: dir.path(),
            context_for: &|_| Ok(BTreeMap::new()),
        },
    )?;
    assert_eq!(out, plain);
    Ok(())
}

#[test]
fn upstream_ephemeral_wrap_is_noop_without_cte_references() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut spans = MacroSpans::default();
    let plain = "select 1 as id".to_string();
    let out = inject_and_persist_ephemeral_models(
        plain.clone(),
        &mut spans,
        "user_model",
        false,
        dir.path(),
    )
    .map_err(|e| anyhow::anyhow!("noop wrap: {e:#}"))?;
    assert_eq!(out, plain);
    Ok(())
}
