//! Shared env-mutation helpers for `config::*` unit tests.
//!
//! Lives outside `mod tests` so multiple test modules can share a single Mutex
//! — without it, two test modules that mutate overlapping env vars race against
//! each other even though each holds its own lock.

#![allow(unsafe_code, clippy::redundant_pub_crate)]

use std::sync::Mutex;

use anyhow::{Result, anyhow};

static CONFIG_ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvVarGuard<'a> {
    saved: Vec<(&'a str, Option<String>)>,
}

impl Drop for EnvVarGuard<'_> {
    fn drop(&mut self) {
        for (k, orig) in &self.saved {
            match orig {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }
}

/// Run `f` with the given env vars applied, restoring the previous values
/// (including unset) on exit, even if `f` panics.
pub(crate) fn with_env<F, R>(vars: &[(&str, Option<&str>)], f: F) -> Result<R>
where
    F: FnOnce() -> Result<R>,
{
    let _lock = CONFIG_ENV_LOCK.lock().map_err(|e| anyhow!("{e}"))?;
    let saved: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    let _restore = EnvVarGuard { saved };
    for (k, v) in vars {
        match v {
            Some(val) => unsafe { std::env::set_var(k, val) },
            None => unsafe { std::env::remove_var(k) },
        }
    }
    f()
}
