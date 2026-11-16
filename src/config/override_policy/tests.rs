use super::*;

use crate::config::test_util::with_env;

fn env(keys: &[&str]) -> BTreeMap<String, String> {
    keys.iter()
        .map(|k| ((*k).to_string(), "v".to_string()))
        .collect()
}

fn policy_from(env_var: Option<&str>, targets_var: Option<&str>) -> OverridePolicy {
    with_env(
        &[
            (ALLOWED_ENV_OVERRIDES_VAR, env_var),
            (ALLOWED_TARGETS_VAR, targets_var),
        ],
        OverridePolicy::from_env,
    )
    .unwrap()
}

#[test]
fn unset_variables_leave_the_policy_unrestricted() {
    assert_eq!(policy_from(None, None), OverridePolicy::default());
}

#[test]
fn empty_variables_permit_nothing() {
    let policy = policy_from(Some(""), Some("  "));
    assert_eq!(policy, OverridePolicy::new(Some(&[]), Some(&[])));
}

#[test]
fn lists_are_split_trimmed_and_stripped_of_blanks() {
    let policy = policy_from(Some(" DB_HOST, DB_SCHEMA ,,DB_HOST"), Some("prod,staging"));
    assert_eq!(
        policy,
        OverridePolicy::new(Some(&["DB_HOST", "DB_SCHEMA"]), Some(&["prod", "staging"]))
    );
}

#[cfg(unix)]
#[test]
fn a_non_unicode_value_is_a_startup_error_naming_the_variable() {
    use std::os::unix::ffi::OsStringExt as _;

    let _guard = with_env(&[(ALLOWED_TARGETS_VAR, None)], || {
        // Set outside `with_env`'s string API; `with_env` restores the unset
        // state when this scope ends.
        unsafe { std::env::set_var(ALLOWED_TARGETS_VAR, std::ffi::OsString::from_vec(vec![0xff])) };
        let err = OverridePolicy::from_env().unwrap_err();
        assert!(format!("{err:#}").contains(ALLOWED_TARGETS_VAR), "{err:#}");
        Ok(())
    });
}

#[test]
fn an_unrestricted_policy_accepts_any_env_and_target() {
    let policy = OverridePolicy::default();
    policy
        .check(&env(&["DB_HOST", "ANYTHING"]), Some("whatever"), "dev")
        .unwrap();
}

#[test]
fn listed_env_names_are_accepted() {
    let policy = OverridePolicy::new(Some(&["DB_HOST", "DB_SCHEMA"]), None);
    policy
        .check(&env(&["DB_HOST", "DB_SCHEMA"]), None, "dev")
        .unwrap();
    policy.check(&env(&[]), None, "dev").unwrap();
}

#[test]
fn an_unlisted_env_name_is_refused_and_named() {
    let policy = OverridePolicy::new(Some(&["DB_SCHEMA"]), None);
    let err = policy
        .check(&env(&["DB_SCHEMA", "DB_HOST", "DB_ROLE"]), None, "dev")
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("DB_HOST, DB_ROLE"), "{message}");
    assert!(message.contains(ALLOWED_ENV_OVERRIDES_VAR), "{message}");
    // The permitted name appears only in the "allowed" list, not among the refused ones.
    assert!(message.contains("override env var(s) DB_HOST, DB_ROLE;"), "{message}");
}

#[test]
fn refusing_an_override_is_not_retried() {
    let policy = OverridePolicy::new(Some(&[]), None);
    let err = policy.check(&env(&["DB_HOST"]), None, "dev").unwrap_err();
    assert!(!err.is_retryable());
    assert!(DbtTemporalError::NON_RETRYABLE_TYPE_NAMES.contains(&err.type_name()));
}

#[test]
fn an_empty_allowlist_refuses_every_override_but_accepts_no_env() {
    let policy = OverridePolicy::new(Some(&[]), None);
    let err = policy.check(&env(&["DB_HOST"]), None, "dev").unwrap_err();
    assert!(
        err.to_string()
            .contains("allowed by ALLOWED_ENV_OVERRIDES: none"),
        "{err}"
    );
    policy.check(&env(&[]), None, "dev").unwrap();
}

/// The worker writes the serialized input under `_` on every run, so a policy
/// that refused it would refuse every run.
#[test]
fn the_reserved_underscore_key_is_always_accepted() {
    OverridePolicy::new(Some(&[]), None)
        .check(&env(&["_"]), None, "dev")
        .unwrap();
}

#[test]
fn env_names_match_exactly() {
    let policy = OverridePolicy::new(Some(&["DB_HOST"]), None);
    for name in ["db_host", "DB_HOST_2", "DB_*", "DB_HOS"] {
        assert!(policy.check(&env(&[name]), None, "dev").is_err(), "{name}");
    }
}

#[test]
fn a_listed_target_is_accepted_and_an_unlisted_one_refused_by_name() {
    let policy = OverridePolicy::new(None, Some(&["staging"]));
    policy.check(&env(&[]), Some("staging"), "dev").unwrap();

    let err = policy
        .check(&env(&[]), Some("attacker"), "dev")
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("'attacker'"), "{message}");
    assert!(message.contains(ALLOWED_TARGETS_VAR), "{message}");
    assert!(!err.is_retryable());
}

#[test]
fn no_target_and_the_default_target_are_accepted_whatever_the_list() {
    let policy = OverridePolicy::new(None, Some(&[]));
    policy.check(&env(&[]), None, "dev").unwrap();
    policy.check(&env(&[]), Some("dev"), "dev").unwrap();
    assert!(policy.check(&env(&[]), Some("prod"), "dev").is_err());
}

#[test]
fn env_and_target_restrictions_are_independent() {
    let env_only = OverridePolicy::new(Some(&["A"]), None);
    env_only
        .check(&env(&["A"]), Some("anything"), "dev")
        .unwrap();

    let target_only = OverridePolicy::new(None, Some(&["prod"]));
    target_only
        .check(&env(&["ANYTHING"]), Some("prod"), "dev")
        .unwrap();
}
