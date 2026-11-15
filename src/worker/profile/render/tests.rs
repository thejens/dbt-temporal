use super::*;
use crate::worker::profile::write_temp_profiles;

fn postgres(config: &DbConfig) -> &dbt_schemas::schemas::profiles::PostgresDbConfig {
    match config {
        DbConfig::Postgres(pg) => pg,
        other => panic!("expected Postgres DbConfig, got {other:?}"),
    }
}

#[test]
fn render_profile_errors_when_outputs_section_absent() -> Result<()> {
    // Profile exists but has no `outputs` map — should fail with a clear
    // hint rather than a confusing parse error somewhere downstream.
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
",
    )?;
    let env = BTreeMap::new();
    let err =
        render_profile_with_env(&path, "my_profile", "dev", &env).expect_err("missing outputs");
    assert!(format!("{err:#}").contains("outputs"), "got: {err:#}");
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn render_profile_errors_when_yaml_unparseable() -> Result<()> {
    let path = write_temp_profiles("my_profile:\n  : not-valid yaml :\n  - dangling")?;
    let env = BTreeMap::new();
    let err = render_profile_with_env(&path, "my_profile", "dev", &env)
        .expect_err("malformed YAML should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("YAML parse error") || msg.contains("resolving profile"),
        "got: {msg}"
    );
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn render_profile_errors_when_db_config_type_unknown() -> Result<()> {
    // outputs.dev exists but its `type` is unknown — DbConfig deserialise
    // fails with the wrapped "deserialising db config" context.
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      type: not_a_real_warehouse
      host: localhost
",
    )?;
    let env = BTreeMap::new();
    let err = render_profile_with_env(&path, "my_profile", "dev", &env)
        .expect_err("unknown adapter type should fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("deserialising db config"), "got: {msg}");
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_with_env_overrides() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'default-host') }}"
      port: 5432
      user: "{{ env_var('DB_USER', 'default-user') }}"
      password: "{{ env_var('DB_PASSWORD', 'default-pass') }}"
      dbname: testdb
      schema: public
"#,
    )?;

    let env = BTreeMap::from([
        ("DB_HOST".to_string(), "override-host".to_string()),
        ("DB_USER".to_string(), "override-user".to_string()),
        ("DB_PASSWORD".to_string(), "override-pass".to_string()),
    ]);

    let rendered = render_profile_with_env(&path, "my_profile", "dev", &env)?;
    assert_eq!(rendered.default_adapter, AdapterType::Postgres);
    assert_eq!(rendered.configs.len(), 1);

    let pg = postgres(rendered.default_config().context("no default config")?);
    assert_eq!(pg.host.as_deref(), Some("override-host"));
    assert_eq!(pg.user.as_deref(), Some("override-user"));
    assert_eq!(pg.password.as_deref(), Some("override-pass"));
    assert_eq!(pg.database.as_deref(), Some("testdb"));
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_with_env_partial_overrides_use_defaults() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'default-host') }}"
      port: 5432
      user: postgres
      password: postgres
      dbname: testdb
      schema: "{{ env_var('DB_SCHEMA', 'public') }}"
"#,
    )?;

    // Only override DB_HOST, DB_SCHEMA falls back to default.
    let env = BTreeMap::from([("DB_HOST".to_string(), "custom-host".to_string())]);

    let rendered = render_profile_with_env(&path, "my_profile", "dev", &env)?;
    let pg = postgres(rendered.default_config().context("no default config")?);
    assert_eq!(pg.host.as_deref(), Some("custom-host"));
    assert_eq!(pg.schema.as_deref(), Some("public"));
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_with_env_target_override() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'dev-host') }}"
      port: 5432
      user: postgres
      password: postgres
      dbname: devdb
      schema: public
    prod:
      type: postgres
      host: "{{ env_var('DB_HOST', 'prod-host') }}"
      port: 5432
      user: postgres
      password: postgres
      dbname: proddb
      schema: public
"#,
    )?;

    let env = BTreeMap::from([("DB_HOST".to_string(), "override-host".to_string())]);

    // Target "prod" should use the prod output.
    let rendered = render_profile_with_env(&path, "my_profile", "prod", &env)?;
    let pg = postgres(rendered.default_config().context("no default config")?);
    assert_eq!(pg.host.as_deref(), Some("override-host"));
    assert_eq!(pg.database.as_deref(), Some("proddb"));
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_wrong_profile_name_errors() -> Result<()> {
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: testdb
      schema: public
",
    )?;

    let env = BTreeMap::new();
    let err = render_profile_with_env(&path, "nonexistent_profile", "dev", &env)
        .expect_err("unknown profile");
    assert!(format!("{err:#}").contains("nonexistent_profile"), "{err:#}");
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_wrong_target_errors() -> Result<()> {
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: testdb
      schema: public
",
    )?;

    let env = BTreeMap::new();
    let err = render_profile_with_env(&path, "my_profile", "nonexistent", &env)
        .expect_err("unknown target");
    assert!(format!("{err:#}").contains("nonexistent"), "{err:#}");
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_missing_env_var_without_default_errors() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DBTT_TEST_MISSING_VAR_12345') }}"
      port: 5432
      user: postgres
      password: postgres
      dbname: testdb
      schema: public
"#,
    )?;

    let env = BTreeMap::new();
    let err = render_profile_with_env(&path, "my_profile", "dev", &env)
        .expect_err("missing env var without default");
    // `{:#}` walks the anyhow cause chain — the missing var name sits on the
    // inner minijinja error, not the top-level context.
    assert!(format!("{err:#}").contains("DBTT_TEST_MISSING_VAR_12345"), "{err:#}");
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn test_render_profile_static_profile_no_env_vars() -> Result<()> {
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: testdb
      schema: public
",
    )?;

    let env = BTreeMap::new();
    let rendered = render_profile_with_env(&path, "my_profile", "dev", &env)?;
    let pg = postgres(rendered.default_config().context("no default config")?);
    assert_eq!(pg.host.as_deref(), Some("localhost"));
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

/// The multi-adapter shape: a connection list, one entry marked default.
/// Declaration order is preserved and every adapter is rendered, so a node
/// selecting the non-default one has a config to build an engine from.
#[test]
fn render_profile_reads_every_adapter_of_a_connection_list_target() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      - type: postgres
        default: true
        host: "{{ env_var('DB_HOST', 'default-host') }}"
        port: 5432
        user: postgres
        password: postgres
        dbname: testdb
        schema: public
      - type: duckdb
        path: "{{ env_var('DUCKDB_PATH', '/tmp/fallback.duckdb') }}"
        schema: main
"#,
    )?;

    let env = BTreeMap::from([
        ("DB_HOST".to_string(), "override-host".to_string()),
        ("DUCKDB_PATH".to_string(), "/tmp/override.duckdb".to_string()),
    ]);

    let rendered = render_profile_with_env(&path, "my_profile", "dev", &env)?;
    assert_eq!(rendered.default_adapter, AdapterType::Postgres);
    assert_eq!(
        rendered
            .configs
            .iter()
            .map(DbConfig::adapter_type)
            .collect::<Vec<_>>(),
        vec![AdapterType::Postgres, AdapterType::DuckDB]
    );

    let pg = postgres(rendered.default_config().context("no default config")?);
    assert_eq!(pg.host.as_deref(), Some("override-host"));

    // The env override has to reach the non-default adapter too, or a node
    // routed there would connect with stale credentials.
    match &rendered.configs[1] {
        DbConfig::DuckDB(duck) => {
            assert_eq!(duck.path.as_deref(), Some("/tmp/override.duckdb"));
        }
        other => panic!("expected DuckDB DbConfig, got {other:?}"),
    }
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

/// A connection list where the default is not the first entry: the default
/// follows the `default: true` marker, not declaration order.
#[test]
fn render_profile_follows_the_default_marker_not_declaration_order() -> Result<()> {
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      - type: duckdb
        path: /tmp/first.duckdb
        schema: main
      - type: postgres
        default: true
        host: localhost
        port: 5432
        user: postgres
        password: postgres
        dbname: testdb
        schema: public
",
    )?;

    let rendered = render_profile_with_env(&path, "my_profile", "dev", &BTreeMap::new())?;
    assert_eq!(rendered.default_adapter, AdapterType::Postgres);
    assert_eq!(
        rendered
            .configs
            .iter()
            .map(DbConfig::adapter_type)
            .collect::<Vec<_>>(),
        vec![AdapterType::DuckDB, AdapterType::Postgres]
    );
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn default_config_finds_the_default_adapters_config() {
    let duckdb = DbConfig::DuckDB(Box::new(dbt_schemas::schemas::profiles::DuckDbConfig {
        path: Some(":memory:".to_string()),
        ..Default::default()
    }));
    let rendered = RenderedTarget {
        configs: vec![duckdb],
        default_adapter: AdapterType::DuckDB,
    };
    assert!(rendered.default_config().is_some());

    let orphaned = RenderedTarget {
        configs: rendered.configs,
        default_adapter: AdapterType::Postgres,
    };
    assert!(orphaned.default_config().is_none());
}
