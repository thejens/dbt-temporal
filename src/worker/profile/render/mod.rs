//! Re-rendering `profiles.yml` with a workflow's `env` overrides.
//!
//! Parsing is delegated to `dbt-profile`, the same crate `dbt_loader` resolves
//! profiles with, so both shapes of a target — the legacy single mapping and the
//! multi-adapter connection list — are read exactly as dbt reads them, including
//! YAML anchors, merge keys and adapter-name canonicalization. Only `env_var()`
//! is swapped out, for a lookup that consults the workflow's overrides first.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

use dbt_adapter::AdapterType;
use dbt_schemas::schemas::profiles::DbConfig;

/// One profile target, rendered with a workflow's env overrides.
///
/// Holds every adapter the target declares, in declaration order, one config
/// each — only an adapter's default connection is reachable, so the others are
/// dropped here as they are in `dbt_loader`.
#[derive(Debug)]
pub struct RenderedTarget {
    /// Declaration order. Never empty; exactly one entry has
    /// [`Self::default_adapter`]'s type.
    pub configs: Vec<DbConfig>,
    /// The adapter unannotated nodes run on.
    pub default_adapter: AdapterType,
}

impl RenderedTarget {
    /// The config nodes that select no adapter run on.
    ///
    /// `None` only if the rendered target somehow lost its default adapter,
    /// which profile parsing rules out; callers surface that as a configuration
    /// error rather than guessing at a substitute.
    pub fn default_config(&self) -> Option<&DbConfig> {
        self.configs
            .iter()
            .find(|config| config.adapter_type() == self.default_adapter)
    }
}

/// Render `profiles.yml` with `env_overrides` and extract every adapter the
/// named target declares.
///
/// `env_var()` consults `env_overrides` before the process environment, so
/// nothing here mutates process state and parallel workflows stay isolated.
pub fn render_profile_with_env(
    profiles_path: &Path,
    profile_name: &str,
    target: &str,
    env_overrides: &BTreeMap<String, String>,
) -> Result<RenderedTarget> {
    let mut penv = dbt_profile::ProfileEnvironment::new(BTreeMap::new());
    penv.ctx.env_var = env_var_with_overrides(env_overrides);

    let resolved = dbt_profile::resolve_with_env(&penv, profiles_path, profile_name, Some(target))
        .with_context(|| {
            format!(
                "resolving profile '{profile_name}' target '{target}' from {}",
                profiles_path.display()
            )
        })?;

    // The default is the adapter holding the connection marked `default: true`,
    // which the parser resolves target-wide — matching by position rather than
    // by adapter-type string, whose external spelling and `DbConfig` tag differ
    // for at least one adapter.
    let default_index = resolved
        .adapters
        .iter()
        .position(|adapter| adapter.connections.iter().any(|c| c.is_default))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "target '{target}' of profile '{profile_name}' marks no default adapter"
            )
        })?;

    let mut configs = Vec::with_capacity(resolved.adapters.len());
    for adapter in &resolved.adapters {
        let credentials = adapter.default_connection().credentials.clone();
        let value = dbt_yaml::Value::Mapping(credentials, dbt_yaml::Span::default());
        let config: DbConfig = dbt_yaml::from_value(value).with_context(|| {
            format!(
                "deserialising db config for adapter '{}' of target '{target}'",
                adapter.adapter_type
            )
        })?;
        configs.push(config);
    }

    let default_adapter = configs
        .get(default_index)
        .map(DbConfig::adapter_type)
        .ok_or_else(|| anyhow::anyhow!("target '{target}' declares no adapters"))?;

    Ok(RenderedTarget {
        configs,
        default_adapter,
    })
}

/// An `env_var()` that answers from `overrides` first, then the process
/// environment, then the call's `default` argument.
///
/// Deliberately not `dbt_jinja_utils::env_var`: that one records every lookup in
/// a process-global map, which a per-workflow rebuild has no business writing to.
fn env_var_with_overrides(overrides: &BTreeMap<String, String>) -> minijinja::Value {
    let overrides = Arc::new(overrides.clone());
    minijinja::Value::from_func_func("env_var", move |_state, args: &[minijinja::Value]| {
        let var_name = args
            .first()
            .and_then(minijinja::Value::as_str)
            .ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "env_var requires a string argument",
                )
            })?;

        if let Some(value) = overrides.get(var_name) {
            return Ok(minijinja::Value::from(value.as_str()));
        }

        if let Ok(value) = std::env::var(var_name) {
            return Ok(minijinja::Value::from(value));
        }

        if let Some(default) = args.get(1)
            && !default.is_undefined()
        {
            return Ok(default.clone());
        }

        Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!("env_var: '{var_name}' not found"),
        ))
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
