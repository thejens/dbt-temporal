use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::worker_state::WorkerState;

/// Result of rebuilding the adapter engine with per-workflow env overrides.
pub struct RebuildResult {
    pub engine: Arc<dyn dbt_adapter::AdapterEngine>,
    /// Profile-level schema after applying env overrides.
    pub schema: String,
    /// Profile-level database after applying env overrides.
    pub database: String,
    /// Keeps the CancellationTokenSource alive so the engine's token isn't
    /// immediately cancelled (the token holds a Weak ref to the source).
    /// Must be kept alive for the duration of engine usage.
    #[allow(dead_code)]
    pub cancellation_source: dbt_common::cancellation::CancellationTokenSource,
}

impl std::fmt::Debug for RebuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildResult")
            .field("schema", &self.schema)
            .field("database", &self.database)
            .field("cancellation_source", &"<alive>")
            .finish_non_exhaustive()
    }
}

/// Re-render profiles.yml with per-workflow env overrides and build a fresh AdapterEngine.
///
/// Called from `execute_node` when the workflow provides `env` overrides and
/// profiles.yml uses `env_var()`. This avoids touching process-level env vars,
/// so parallel workflows are fully isolated.
///
/// Returns the engine plus the resolved schema/database from the re-rendered profile,
/// so callers can patch node relation metadata (which was baked in at worker startup).
pub fn rebuild_adapter_engine_with_env(
    state: &WorkerState,
    target_override: Option<&str>,
    env_overrides: &BTreeMap<String, String>,
) -> Result<RebuildResult> {
    let db_config = render_profile_with_env(
        &state.profiles_path,
        &state.profile_name_in_project,
        target_override.unwrap_or(&state.default_target),
        env_overrides,
    )?;

    let schema = db_config.get_schema().cloned().unwrap_or_default();
    let database = db_config.get_database().cloned().unwrap_or_default();

    let cancellation_source = dbt_common::cancellation::CancellationTokenSource::new();
    let engine = super::adapter::build_adapter_engine(
        &db_config,
        state.resolver_state.root_project_quoting,
        &cancellation_source.token(),
        state.auth_override.clone(),
    )?;

    Ok(RebuildResult {
        engine,
        schema,
        database,
        cancellation_source,
    })
}

/// Render profiles.yml with env overrides and extract a DbConfig for the given profile/target.
///
/// Creates a lightweight Jinja environment with `env_var()` overridden to check
/// `env_overrides` before falling back to `std::env::var`. This avoids mutating
/// process-level env vars and is safe for parallel workflows.
pub fn render_profile_with_env(
    profiles_path: &std::path::Path,
    profile_name: &str,
    target: &str,
    env_overrides: &BTreeMap<String, String>,
) -> Result<dbt_schemas::schemas::profiles::DbConfig> {
    // Read raw profiles.yml.
    let raw = std::fs::read_to_string(profiles_path)
        .with_context(|| format!("reading {}", profiles_path.display()))?;

    // Build a lightweight Jinja env and override env_var with the workflow's env map.
    let mut jinja_env = dbt_jinja_utils::JinjaEnvBuilder::new().build();
    let env_map = Arc::new(env_overrides.clone());
    jinja_env.env.add_func_func("env_var", move |_state, args| {
        let var_name = args.first().and_then(|v| v.as_str()).ok_or_else(|| {
            minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                "env_var requires a string argument",
            )
        })?;

        // Check workflow overrides first.
        if let Some(val) = env_map.get(var_name) {
            return Ok(minijinja::Value::from(val.clone()));
        }

        // Fall back to process env.
        if let Ok(val) = std::env::var(var_name) {
            Ok(minijinja::Value::from(val))
        } else {
            if let Some(default) = args.get(1)
                && !default.is_undefined()
            {
                return Ok(default.clone());
            }
            Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("env_var: '{var_name}' not found"),
            ))
        }
    });

    // Render profiles.yml through Jinja (resolves env_var calls).
    let rendered = jinja_env
        .env
        .render_str(&raw, BTreeMap::<String, String>::new(), &[])
        .map_err(|e| anyhow::anyhow!("rendering profiles.yml with env overrides: {e}"))?;

    // Parse the rendered YAML.
    let yaml: dbt_yaml::Value = dbt_yaml::from_str(&rendered)
        .map_err(|e| anyhow::anyhow!("parsing rendered profiles.yml: {e}"))?;

    // Navigate: profile -> outputs -> target.
    let profile_val = yaml
        .get(profile_name)
        .ok_or_else(|| anyhow::anyhow!("profile '{profile_name}' not found in profiles.yml"))?;

    let outputs = profile_val
        .get("outputs")
        .ok_or_else(|| anyhow::anyhow!("no 'outputs' in profile"))?;

    let target_config = outputs
        .get(target)
        .ok_or_else(|| anyhow::anyhow!("target '{target}' not found in profile"))?;

    // Deserialize into DbConfig (tagged by `type` field).
    dbt_yaml::from_value(target_config.clone())
        .map_err(|e| anyhow::anyhow!("deserializing db config for target '{target}': {e}"))
}

/// Check whether a profiles.yml file contains `env_var()` calls.
pub fn profile_uses_env_vars(profiles_path: &std::path::Path) -> bool {
    std::fs::read_to_string(profiles_path)
        .map(|content| content.contains("env_var("))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn write_temp_profiles(content: &str) -> Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!("dbtt-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("profiles.yml");
        std::fs::write(&path, content)?;
        Ok(path)
    }

    #[test]
    fn test_profile_uses_env_vars_detects_usage() -> Result<()> {
        let path = write_temp_profiles(
            r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      port: 5432
"#,
        )?;
        assert!(profile_uses_env_vars(&path));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    #[test]
    fn test_profile_uses_env_vars_no_env_var() -> Result<()> {
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
        assert!(!profile_uses_env_vars(&path));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    #[test]
    fn test_profile_uses_env_vars_missing_file() {
        let path = std::path::PathBuf::from("/tmp/nonexistent-dbtt-test/profiles.yml");
        assert!(!profile_uses_env_vars(&path));
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

        let mut env = BTreeMap::new();
        env.insert("DB_HOST".to_string(), "override-host".to_string());
        env.insert("DB_USER".to_string(), "override-user".to_string());
        env.insert("DB_PASSWORD".to_string(), "override-pass".to_string());

        let db_config = render_profile_with_env(&path, "my_profile", "dev", &env)?;

        // Verify the DbConfig was parsed as postgres.
        match &db_config {
            dbt_schemas::schemas::profiles::DbConfig::Postgres(pg) => {
                assert_eq!(pg.host.as_deref(), Some("override-host"));
                assert_eq!(pg.user.as_deref(), Some("override-user"));
                assert_eq!(pg.password.as_deref(), Some("override-pass"));
                assert_eq!(pg.database.as_deref(), Some("testdb"));
            }
            other => panic!("expected Postgres DbConfig, got {other:?}"),
        }
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
        let mut env = BTreeMap::new();
        env.insert("DB_HOST".to_string(), "custom-host".to_string());

        let db_config = render_profile_with_env(&path, "my_profile", "dev", &env)?;

        match &db_config {
            dbt_schemas::schemas::profiles::DbConfig::Postgres(pg) => {
                assert_eq!(pg.host.as_deref(), Some("custom-host"));
                assert_eq!(pg.schema.as_deref(), Some("public"));
            }
            other => panic!("expected Postgres DbConfig, got {other:?}"),
        }
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

        let mut env = BTreeMap::new();
        env.insert("DB_HOST".to_string(), "override-host".to_string());

        // Target "prod" should use the prod output.
        let db_config = render_profile_with_env(&path, "my_profile", "prod", &env)?;
        match &db_config {
            dbt_schemas::schemas::profiles::DbConfig::Postgres(pg) => {
                assert_eq!(pg.host.as_deref(), Some("override-host"));
                assert_eq!(pg.database.as_deref(), Some("proddb"));
            }
            other => panic!("expected Postgres DbConfig, got {other:?}"),
        }
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
        let result = render_profile_with_env(&path, "nonexistent_profile", "dev", &env);
        let Err(err) = result else {
            anyhow::bail!("expected error")
        };
        assert!(err.to_string().contains("nonexistent_profile"));
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
        let result = render_profile_with_env(&path, "my_profile", "nonexistent", &env);
        let Err(err) = result else {
            anyhow::bail!("expected error")
        };
        assert!(err.to_string().contains("nonexistent"));
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
        let result = render_profile_with_env(&path, "my_profile", "dev", &env);
        let Err(err) = result else {
            anyhow::bail!("expected error")
        };
        assert!(err.to_string().contains("DBTT_TEST_MISSING_VAR_12345"));
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
        let db_config = render_profile_with_env(&path, "my_profile", "dev", &env)?;
        match &db_config {
            dbt_schemas::schemas::profiles::DbConfig::Postgres(pg) => {
                assert_eq!(pg.host.as_deref(), Some("localhost"));
            }
            other => panic!("expected Postgres DbConfig, got {other:?}"),
        }
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }
}
