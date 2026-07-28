//! Per-workflow `--vars` and `--full-refresh` reaching the render context.
//!
//! Both were previously accepted on `DbtRunInput`, recorded in the workflow
//! memo, and then dropped: the Jinja `flags` object and the `var()` function
//! are built once from `InvocationArgs::default()` when the worker parses the
//! project. A run asking for a full refresh silently performed an incremental
//! merge, and `{{ var(...) }}` always resolved to project defaults.
//!
//! These assert on compiled SQL rather than warehouse state, so a failure
//! points at the renderer rather than at whatever DuckDB happened to store.
//! Each case gets its own `Harness` — re-materializing one model twice in a
//! single in-memory DuckDB trips a relation-rename conflict unrelated to what
//! is under test.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::Harness;

/// Compile one model and return its SQL, with the given overrides applied.
async fn compiled_with(
    model: &str,
    body: &str,
    vars: serde_json::Value,
    full_refresh: bool,
) -> String {
    let harness = Harness::build(&[(model, body)]).await;
    harness
        .run_ok_with_overrides(model, vars, full_refresh)
        .await
        .compiled_code
        .expect("compiled sql")
}

#[tokio::test]
async fn workflow_vars_override_project_defaults() {
    let body = "select {{ var('row_limit', 1) }} as n";

    let default = compiled_with("uses_var", body, serde_json::json!({}), false).await;
    assert!(default.contains("1 as n"), "default should apply: {default}");

    let overridden =
        compiled_with("uses_var", body, serde_json::json!({ "row_limit": 42 }), false).await;
    assert!(overridden.contains("42 as n"), "workflow var should win: {overridden}");
}

/// Only the named keys are overridden — everything else still resolves through
/// the resolver's own `var` object.
#[tokio::test]
async fn unlisted_vars_still_resolve_through_the_project() {
    let compiled = compiled_with(
        "two_vars",
        "select {{ var('a', 10) }} as a, {{ var('b', 20) }} as b",
        serde_json::json!({ "a": 99 }),
        false,
    )
    .await;
    assert!(compiled.contains("99 as a"), "overridden var: {compiled}");
    assert!(compiled.contains("20 as b"), "untouched var keeps default: {compiled}");
}

#[tokio::test]
async fn workflow_vars_accept_non_scalar_values() {
    let compiled = compiled_with(
        "list_var",
        "select '{{ var('names', []) | join(',') }}' as names",
        serde_json::json!({ "names": ["alpha", "beta"] }),
        false,
    )
    .await;
    assert!(compiled.contains("alpha,beta"), "list var should render: {compiled}");
}

#[tokio::test]
async fn full_refresh_flag_reaches_the_render_context() {
    let body = "select {% if flags.FULL_REFRESH %}1{% else %}0{% endif %} as fr";

    let normal = compiled_with("reads_flag", body, serde_json::json!({}), false).await;
    assert!(normal.contains("0 as fr"), "default is off: {normal}");

    let refreshed = compiled_with("reads_flag", body, serde_json::json!({}), true).await;
    assert!(refreshed.contains("1 as fr"), "flag should be set: {refreshed}");
}

/// dbt registers the flag under both spellings and real macros use each —
/// `should_full_refresh()` in dbt-adapters reads the lowercase one.
#[tokio::test]
async fn full_refresh_is_visible_under_both_spellings() {
    let compiled = compiled_with(
        "reads_both",
        "select {% if flags.full_refresh %}1{% else %}0{% endif %} as lower",
        serde_json::json!({}),
        true,
    )
    .await;
    assert!(compiled.contains("1 as lower"), "lowercase spelling: {compiled}");
}

/// Wrapping `flags` must not hide the keys the worker computed, nor the
/// `flags.get(...)` method adapter macros call. The `get()` of a key that is
/// *not* overridden has to reach the wrapped object, not stop at the overlay.
#[tokio::test]
async fn overlaying_flags_preserves_the_rest_of_the_object() {
    let compiled = compiled_with(
        "other_flags",
        "select {% if flags.INTROSPECT %}1{% else %}0{% endif %} as introspect, \
         {% if flags.get('full_refresh') %}1{% else %}0{% endif %} as via_get, \
         {% if flags.get('INTROSPECT') %}1{% else %}0{% endif %} as delegated_get",
        serde_json::json!({}),
        true,
    )
    .await;
    assert!(compiled.contains("1 as introspect"), "untouched flag survives: {compiled}");
    assert!(compiled.contains("1 as via_get"), "get() sees the override: {compiled}");
    assert!(
        compiled.contains("1 as delegated_get"),
        "get() of a non-overridden key must delegate to the wrapped flags: {compiled}"
    );
}

/// The per-workflow `env_var()` override has to reach templates, not just the
/// profile — parallel workflows rely on it for isolation, and it is replaced
/// per activity rather than via process env.
#[tokio::test]
async fn workflow_env_overrides_reach_env_var_in_model_sql() {
    let harness =
        Harness::build(&[("uses_env", "select '{{ env_var('RUN_LABEL', 'unset') }}' as label")])
            .await;

    let overridden = harness
        .run_uid_with_env(
            &format!("model.{}.uses_env", common::duckdb::PROJECT),
            &std::collections::BTreeMap::from([("RUN_LABEL".to_string(), "nightly".to_string())]),
        )
        .await
        .expect("model should succeed")
        .compiled_code
        .expect("compiled sql");
    assert!(overridden.contains("'nightly' as label"), "env override applied: {overridden}");
}

// The natural end-to-end case — build an incremental model, then rebuild it
// with full_refresh and assert `is_incremental()` flipped — is not covered
// here: re-materializing any existing relation fails against this harness
// ("Could not rename … another entry with this name already exists"),
// independently of these overrides. The three tests above cover what
// `should_full_refresh()` actually reads, which is the part this code owns.
