//! Which per-workflow overrides a worker accepts.
//!
//! A workflow's `env` and `target` take precedence over the worker's own
//! environment and default target, and the worker authenticates with whatever
//! the resulting `profiles.yml` names. Anyone who can start a workflow can
//! therefore point the worker's credentials at a host they control, unless the
//! operator lists what a workflow may override. Both lists are opt-in: unset
//! keeps every override allowed.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};

use crate::error::DbtTemporalError;

/// Env var listing the names a workflow's `env` may contain.
pub const ALLOWED_ENV_OVERRIDES_VAR: &str = "ALLOWED_ENV_OVERRIDES";
/// Env var listing the targets a workflow's `target` may select.
pub const ALLOWED_TARGETS_VAR: &str = "ALLOWED_TARGETS";

/// The one `env` key the worker writes itself rather than reading from the
/// caller: the serialized workflow input, exposed as `env_var('_')`. It is
/// exempt because the workflow overwrites any caller-supplied value, so it
/// cannot carry an override the caller chose.
const RESERVED_ENV_KEY: &str = "_";

/// The operator's restrictions on workflow `env` and `target`.
///
/// `None` is unrestricted; `Some` of an empty set permits nothing. The two
/// differ on purpose: an operator who sets the variable to an empty string has
/// asked for no overrides, and treating that as "unset" would silently leave
/// the worker open.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OverridePolicy {
    env: Option<BTreeSet<String>>,
    targets: Option<BTreeSet<String>>,
}

impl OverridePolicy {
    /// Read both lists from the environment.
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            env: parse_name_list(ALLOWED_ENV_OVERRIDES_VAR)?,
            targets: parse_name_list(ALLOWED_TARGETS_VAR)?,
        })
    }

    /// A policy from explicit lists, `None` meaning unrestricted.
    pub fn new(env: Option<&[&str]>, targets: Option<&[&str]>) -> Self {
        let set = |names: &[&str]| names.iter().map(ToString::to_string).collect();
        Self {
            env: env.map(set),
            targets: targets.map(set),
        }
    }

    /// Refuse a run whose `env` or `target` the operator has not allowed.
    ///
    /// Refusing is permanent (`Configuration`): the same input is refused on
    /// every attempt, and running it anyway would use the worker's own
    /// settings, which is a different warehouse from the one the caller asked
    /// for.
    pub fn check(
        &self,
        env: &BTreeMap<String, String>,
        target: Option<&str>,
        default_target: &str,
    ) -> Result<(), DbtTemporalError> {
        self.check_env(env)?;
        self.check_target(target, default_target)
    }

    fn check_env(&self, env: &BTreeMap<String, String>) -> Result<(), DbtTemporalError> {
        let Some(allowed) = &self.env else {
            return Ok(());
        };
        let refused: Vec<&str> = env
            .keys()
            .map(String::as_str)
            .filter(|key| *key != RESERVED_ENV_KEY && !allowed.contains(*key))
            .collect();
        if refused.is_empty() {
            return Ok(());
        }
        Err(DbtTemporalError::Configuration(format!(
            "this worker does not allow the workflow to override env var(s) {}; allowed by {ALLOWED_ENV_OVERRIDES_VAR}: {}",
            refused.join(", "),
            describe(allowed),
        )))
    }

    /// The worker's own default target is always selectable: naming it changes
    /// nothing, and an operator who restricts targets should not have to
    /// repeat it for callers that state it explicitly.
    fn check_target(
        &self,
        target: Option<&str>,
        default_target: &str,
    ) -> Result<(), DbtTemporalError> {
        let (Some(allowed), Some(target)) = (&self.targets, target) else {
            return Ok(());
        };
        if target == default_target || allowed.contains(target) {
            return Ok(());
        }
        Err(DbtTemporalError::Configuration(format!(
            "this worker does not allow the workflow to select target '{target}'; allowed by {ALLOWED_TARGETS_VAR}: {}",
            describe(allowed),
        )))
    }
}

fn describe(allowed: &BTreeSet<String>) -> String {
    if allowed.is_empty() {
        "none".to_string()
    } else {
        allowed
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Parse a comma-separated name list. Unset is `None`; set to anything,
/// including the empty string, is `Some`. Exact names only: a pattern would
/// need an escape for names that really contain `*`, and every name an example
/// or doc passes in `env` is a plain identifier.
fn parse_name_list(var: &str) -> Result<Option<BTreeSet<String>>> {
    match std::env::var(var) {
        Ok(raw) => Ok(Some(
            raw.split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(ToString::to_string)
                .collect(),
        )),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e @ std::env::VarError::NotUnicode(_)) => Err(e).context(format!("invalid {var}")),
    }
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests;
