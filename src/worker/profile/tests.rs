use super::*;

fn keys_of(path: &Path) -> std::collections::BTreeSet<String> {
    match profile_env_vars(path) {
        ProfileEnvVars::Keys(keys) => keys,
        ProfileEnvVars::Unknown => panic!("expected named keys"),
    }
}

#[test]
fn profile_env_vars_names_the_keys_it_reads() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      user: "{{ env_var ('DB_USER') }}"
      port: 5432
"#,
    )?;
    assert_eq!(
        keys_of(&path),
        ["DB_HOST".to_string(), "DB_USER".to_string()]
            .into_iter()
            .collect(),
        "both spellings count, whitespace before the paren included"
    );
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

#[test]
fn profile_env_vars_is_empty_for_a_static_profile() -> Result<()> {
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
",
    )?;
    assert!(keys_of(&path).is_empty());
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

/// A name that merely ends in `env_var` is not a call.
#[test]
fn profile_env_vars_ignores_a_longer_identifier() -> Result<()> {
    let path = write_temp_profiles(
        r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: my_env_variable
",
    )?;
    assert!(keys_of(&path).is_empty());
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

/// A key only rendering can resolve makes every override potentially
/// relevant — the conservative answer, not a guess at nothing.
#[test]
fn profile_env_vars_is_unknown_for_a_computed_key() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var(host_var) }}"
"#,
    )?;
    assert!(matches!(profile_env_vars(&path), ProfileEnvVars::Unknown));
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

/// Unreadable says nothing about the contents: assume env vars are in play
/// rather than pinning the run to startup credentials.
#[test]
fn profile_env_vars_is_unknown_when_the_file_cannot_be_read() {
    let path = std::path::PathBuf::from("/tmp/nonexistent-dbtt-test/profiles.yml");
    assert!(matches!(profile_env_vars(&path), ProfileEnvVars::Unknown));
}

/// The point of naming keys: `build_effective_env` puts the serialized
/// workflow input in `_` on every run, so a profile that does not read `_`
/// must not rebuild its engines once per node because of it.
#[test]
fn the_underscore_override_alone_does_not_affect_a_profile_that_ignores_it() {
    let reads_db_host = ProfileEnvVars::Keys(std::iter::once("DB_HOST".to_string()).collect());

    let only_underscore = ["_".to_string()];
    assert!(!reads_db_host.affected_by(only_underscore.iter()));

    let with_db_host = ["_".to_string(), "DB_HOST".to_string()];
    assert!(reads_db_host.affected_by(with_db_host.iter()));

    let unrelated = ["_".to_string(), "SOME_MODEL_VAR".to_string()];
    assert!(
        !reads_db_host.affected_by(unrelated.iter()),
        "an override the profile never reads changes nothing about the connection"
    );
}

/// `'A' ~ suffix` starts with a literal but names a different key; reading
/// it as `A` would let an override of the real key go unnoticed.
#[test]
fn profile_env_vars_is_unknown_for_a_key_built_by_concatenation() -> Result<()> {
    let path = write_temp_profiles(
        r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_' ~ region ~ '_HOST') }}"
"#,
    )?;
    assert!(matches!(profile_env_vars(&path), ProfileEnvVars::Unknown));
    std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
    Ok(())
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn reads_db_host() -> ProfileEnvVars {
    ProfileEnvVars::Keys(std::iter::once("DB_HOST".to_string()).collect())
}

/// Runs differ in the serialized input they carry as `_`; a profile that
/// never reads it must not get an engine set per run because of it.
#[test]
fn rebuild_key_ignores_overrides_the_profile_does_not_read() {
    let profile = reads_db_host();
    let a = RebuildKey::new("dev", &profile, &env(&[("DB_HOST", "h"), ("_", "run-1")]));
    let b = RebuildKey::new("dev", &profile, &env(&[("DB_HOST", "h"), ("_", "run-2")]));
    assert_eq!(a, b);
}

#[test]
fn rebuild_key_separates_different_values_of_a_key_the_profile_reads() {
    let profile = reads_db_host();
    let a = RebuildKey::new("dev", &profile, &env(&[("DB_HOST", "one")]));
    let b = RebuildKey::new("dev", &profile, &env(&[("DB_HOST", "two")]));
    assert_ne!(a, b);
}

#[test]
fn rebuild_key_separates_targets() {
    let profile = reads_db_host();
    let overrides = env(&[("DB_HOST", "h")]);
    assert_ne!(
        RebuildKey::new("dev", &profile, &overrides),
        RebuildKey::new("prod", &profile, &overrides)
    );
}

/// With an unnameable key set, any override may reach the profile, so the
/// whole map is the identity.
#[test]
fn rebuild_key_covers_every_override_when_the_profile_keys_are_unknown() {
    let a = RebuildKey::new("dev", &ProfileEnvVars::Unknown, &env(&[("_", "run-1")]));
    let b = RebuildKey::new("dev", &ProfileEnvVars::Unknown, &env(&[("_", "run-2")]));
    assert_ne!(a, b);
}

/// Env values can be credentials; a key must never print them.
#[test]
fn rebuild_key_debug_names_keys_and_never_prints_values() {
    let key = RebuildKey::new("dev", &reads_db_host(), &env(&[("DB_HOST", "hunter2-secret")]));
    let rendered = format!("{key:?}");
    assert!(rendered.contains("DB_HOST"), "{rendered}");
    assert!(!rendered.contains("hunter2-secret"), "{rendered}");
}

/// When the keys cannot be named, any override is assumed relevant.
#[test]
fn unknown_keys_treat_every_override_as_relevant() {
    let unknown = ProfileEnvVars::Unknown;
    assert!(unknown.affected_by(std::iter::once(&"_".to_string())));
    assert!(!unknown.affected_by(std::iter::empty()));
}

/// The contract of the `Debug` impl: a rebuild is logged by the target it
/// resolved to, never by what the engines hold — those carry credentials.
#[test]
fn rebuild_result_debug_projects_the_target_and_not_the_engines() -> Result<()> {
    let config = dbt_schemas::schemas::profiles::DbConfig::DuckDB(Box::new(
        dbt_schemas::schemas::profiles::DuckDbConfig {
            path: Some(":memory:".to_string()),
            ..Default::default()
        },
    ));
    let engines = crate::worker::adapter::build_adapter_engines(
        std::slice::from_ref(&config),
        dbt_adapter::AdapterType::DuckDB,
        dbt_schemas::schemas::common::ResolvedQuoting::default(),
        &crate::worker::adapter::AdapterSettings::default(),
        None,
    )?;

    let rendered = format!(
        "{:?}",
        RebuildResult {
            engines,
            targets: vec![(
                dbt_adapter::AdapterType::DuckDB,
                AdapterTarget {
                    schema: "wf_42".to_string(),
                    database: Some("warehouse".to_string()),
                },
            )],
            default_adapter: dbt_adapter::AdapterType::DuckDB,
        }
    );
    assert!(rendered.contains("RebuildResult"), "{rendered}");
    assert!(rendered.contains("wf_42"), "{rendered}");
    assert!(rendered.contains("warehouse"), "{rendered}");
    assert!(rendered.contains(".."), "expected finish_non_exhaustive marker: {rendered}");
    Ok(())
}

/// DuckDB names its database by its file, so its profile declares none.
/// Reporting that as an empty string made every downstream relation in the
/// compiled SQL claim it lived in a database called ``.
#[test]
fn a_target_without_a_database_resolves_to_none() {
    let config = dbt_schemas::schemas::profiles::DbConfig::DuckDB(Box::new(
        dbt_schemas::schemas::profiles::DuckDbConfig {
            path: Some(":memory:".to_string()),
            schema: Some("wf_42".to_string()),
            ..Default::default()
        },
    ));
    let target = adapter_target(&config);
    assert_eq!(target.schema, "wf_42");
    assert_eq!(target.database, None);
}

/// A target declaring several adapters gives each its own schema and
/// database. Applying the default adapter's to a node routed elsewhere told
/// its Jinja `target` about a warehouse it was not connected to.
#[test]
fn each_adapter_gets_its_own_resolved_target() -> Result<()> {
    let config = dbt_schemas::schemas::profiles::DbConfig::DuckDB(Box::new(
        dbt_schemas::schemas::profiles::DuckDbConfig {
            path: Some(":memory:".to_string()),
            ..Default::default()
        },
    ));
    let engines = crate::worker::adapter::build_adapter_engines(
        std::slice::from_ref(&config),
        dbt_adapter::AdapterType::DuckDB,
        dbt_schemas::schemas::common::ResolvedQuoting::default(),
        &crate::worker::adapter::AdapterSettings::default(),
        None,
    )?;
    let result = RebuildResult {
        engines,
        targets: vec![
            (
                dbt_adapter::AdapterType::DuckDB,
                AdapterTarget {
                    schema: "duck_schema".to_string(),
                    database: Some("duck_db".to_string()),
                },
            ),
            (
                dbt_adapter::AdapterType::Postgres,
                AdapterTarget {
                    schema: "pg_schema".to_string(),
                    database: Some("pg_db".to_string()),
                },
            ),
        ],
        default_adapter: dbt_adapter::AdapterType::DuckDB,
    };

    assert_eq!(result.target_for(dbt_adapter::AdapterType::Postgres).schema, "pg_schema");
    assert_eq!(result.target_for(dbt_adapter::AdapterType::DuckDB).schema, "duck_schema");

    // An adapter the rebuilt profile does not declare falls back to the
    // default's, which is where its node would run anyway.
    assert_eq!(
        result
            .target_for(dbt_adapter::AdapterType::Snowflake)
            .schema,
        "duck_schema"
    );
    Ok(())
}
