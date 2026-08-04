//! Typed search-attribute updates built from config-driven attribute names.
//!
//! Temporal's typed search-attribute API keys everything on
//! `SearchAttributeKey<T>`, whose name is a `&'static str` so keys can be
//! declared as consts. Our attribute names are not known at compile time —
//! they come from project config and reach the workflow as owned `String`s on
//! the `ExecutionPlan` — so each distinct name is interned: leaked once and
//! reused by every later run. The set of names is bounded by the deployed
//! config (the built-in `Dbt*` attributes plus whatever the project declares),
//! so the interner reaches a fixed size and stops growing.
//!
//! Every attribute dbt-temporal writes is Keyword-typed, so one key type
//! covers them all. A name registered on the namespace as some other type is
//! rejected by the server on upsert, exactly as it was before typing existed.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};

use temporalio_common::search_attributes::{SearchAttributeKey, SearchAttributeUpdate};
use temporalio_sdk::WorkflowTermination;
use temporalio_sdk::error::ApplicationFailure;

/// Interned attribute names, so a name is leaked at most once per process.
static INTERNED_NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

/// Return a `&'static str` for `name`, leaking it only the first time it is
/// seen. Poisoning is impossible here: the guard is held across a `HashSet`
/// lookup and insert, neither of which can panic on the paths we hit, and the
/// lock is never held across an await.
fn intern(name: &str) -> Option<&'static str> {
    let mut names = INTERNED_NAMES.get_or_init(Mutex::default).lock().ok()?;
    let interned = names.get(name).copied().unwrap_or_else(|| {
        let leaked: &'static str = String::from(name).leak();
        names.insert(leaked);
        leaked
    });
    drop(names);
    Some(interned)
}

/// Build a Keyword-typed key for a runtime-determined attribute name.
fn keyword_key(name: &str) -> Result<SearchAttributeKey<String>, WorkflowTermination> {
    let interned = intern(name).ok_or_else(|| {
        WorkflowTermination::failed_application(ApplicationFailure::non_retryable(anyhow::anyhow!(
            "search attribute name interner was poisoned (attribute '{name}')"
        )))
    })?;
    Ok(SearchAttributeKey::keyword(interned))
}

/// Convert the plan's `name -> value` map into typed Keyword updates, tagging
/// any serialization failure with the attribute name that caused it.
pub fn build_search_attribute_updates(
    attributes: &BTreeMap<String, String>,
) -> Result<Vec<SearchAttributeUpdate>, WorkflowTermination> {
    attributes
        .iter()
        .map(|(name, value)| {
            keyword_key(name)?
                .try_value_set(value.clone())
                .map_err(|e| {
                    WorkflowTermination::failed_application(ApplicationFailure::non_retryable(
                        anyhow::anyhow!("encoding search attribute '{name}': {e:#}"),
                    ))
                })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn interning_the_same_name_twice_yields_the_same_pointer() {
        let first = intern("DbtProjectInternTest").expect("interner available");
        let second = intern("DbtProjectInternTest").expect("interner available");
        assert!(std::ptr::eq(first, second));
    }

    #[test]
    fn builds_one_update_per_attribute_preserving_names() {
        let attrs = BTreeMap::from([
            ("DbtProject".to_string(), "waffle_hut".to_string()),
            ("DbtStatus".to_string(), "running".to_string()),
        ]);
        let updates = build_search_attribute_updates(&attrs).expect("keyword encoding is total");
        let names: Vec<&str> = updates.iter().map(SearchAttributeUpdate::name).collect();
        assert_eq!(names, ["DbtProject", "DbtStatus"]);
        assert!(updates.iter().all(|u| !u.is_unset()));
    }

    #[test]
    fn empty_attributes_produce_no_updates() {
        let updates = build_search_attribute_updates(&BTreeMap::new()).expect("empty is ok");
        assert!(updates.is_empty());
    }
}
