//! A worker whose operator restricted `env` and `target` overrides, against an
//! embedded DuckDB warehouse.
//!
//! The refusal has to reach the caller as a permanent error naming the key: an
//! override that was dropped quietly would run the node against the worker's
//! own warehouse instead of the one the caller asked for.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;
use dbt_temporal::config::OverridePolicy;
use dbt_temporal::error::DbtTemporalError;

const MODEL: &str = "model.spike.one";

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

async fn harness(policy: OverridePolicy) -> Harness {
    Harness::build_restricted(&[("one", "select 1 as n")], policy).await
}

#[tokio::test]
async fn an_unlisted_env_override_fails_the_node_permanently_and_names_the_key() {
    let h = harness(OverridePolicy::new(Some(&["DB_SCHEMA"]), None)).await;

    let err = h
        .run_err_uid_with_env(MODEL, &env(&[("DB_SCHEMA", "a"), ("DB_HOST", "evil.example")]))
        .await;

    assert!(matches!(err, DbtTemporalError::Configuration(_)), "{err}");
    assert!(!err.is_retryable());
    let message = err.to_string();
    assert!(message.contains("DB_HOST"), "{message}");
    assert!(!message.contains("evil.example"), "values must not be echoed: {message}");
}

#[tokio::test]
async fn a_listed_env_override_runs() {
    let h = harness(OverridePolicy::new(Some(&["DB_SCHEMA"]), None)).await;
    h.run_uid_with_env(MODEL, &env(&[("DB_SCHEMA", "a")]))
        .await
        .unwrap();
}

#[tokio::test]
async fn an_empty_allowlist_refuses_any_env_but_runs_without_one() {
    let h = harness(OverridePolicy::new(Some(&[]), None)).await;
    h.run_uid_with_env(MODEL, &BTreeMap::new()).await.unwrap();

    let err = h.run_err_uid_with_env(MODEL, &env(&[("ANY", "x")])).await;
    assert!(err.to_string().contains("ANY"), "{err}");
}

#[tokio::test]
async fn an_unlisted_target_fails_the_node_permanently_and_names_it() {
    let h = harness(OverridePolicy::new(None, Some(&["staging"]))).await;

    let err = h
        .run_uid_with_target(MODEL, "attacker")
        .await
        .expect_err("target should be refused")
        .downcast::<DbtTemporalError>()
        .expect("a DbtTemporalError");

    assert!(!err.is_retryable());
    assert!(err.to_string().contains("'attacker'"), "{err}");
}

/// The worker's own default target needs no listing, so naming it explicitly
/// keeps working under a target allowlist.
#[tokio::test]
async fn the_default_target_is_selectable_under_a_target_allowlist() {
    let h = harness(OverridePolicy::new(None, Some(&[]))).await;
    h.run_uid_with_target(MODEL, "dev").await.unwrap();
}

#[tokio::test]
async fn an_unrestricted_worker_accepts_any_env_and_runs_as_before() {
    let h = harness(OverridePolicy::default()).await;
    h.run_uid_with_env(MODEL, &env(&[("ANYTHING", "x")]))
        .await
        .unwrap();
}
