use super::{empty_pre_run_payload, make_hook};
use crate::hooks::*;
use crate::types::HookErrorMode;

// --- process_pre_run_completion ---

#[test]
fn process_pre_run_completion_continue_for_non_pre_run_events() {
    let mut outcome = HookExecutionOutcome::default();
    // Even with a skip sentinel, a non-pre_run event must not flip skip.
    let bytes = br#"{"skip": true}"#;
    for event in [HookEvent::OnSuccess, HookEvent::OnFailure] {
        let action = process_pre_run_completion(event, Some(bytes), &mut outcome);
        assert_eq!(action, HookCompletionAction::Continue);
        assert!(!outcome.skip);
    }
}

#[test]
fn process_pre_run_completion_continue_when_no_payload() {
    let mut outcome = HookExecutionOutcome::default();
    let action = process_pre_run_completion(HookEvent::PreRun, None, &mut outcome);
    assert_eq!(action, HookCompletionAction::Continue);
}

#[test]
fn process_pre_run_completion_merges_extra_env() {
    let mut outcome = HookExecutionOutcome::default();
    let bytes = br#"{"extra_env": {"DBT_PARTITION": "2026-04-26", "TIER": "premium"}}"#;
    let action = process_pre_run_completion(HookEvent::PreRun, Some(bytes), &mut outcome);
    assert_eq!(action, HookCompletionAction::Continue);
    assert_eq!(outcome.extra_env.get("DBT_PARTITION").map(String::as_str), Some("2026-04-26"));
    assert_eq!(outcome.extra_env.get("TIER").map(String::as_str), Some("premium"));
    assert!(!outcome.skip);
}

#[test]
fn process_pre_run_completion_returns_skip_with_reason() {
    let mut outcome = HookExecutionOutcome::default();
    let bytes = br#"{"skip": true, "reason": "no new data"}"#;
    let action = process_pre_run_completion(HookEvent::PreRun, Some(bytes), &mut outcome);
    assert_eq!(action, HookCompletionAction::Skip(Some("no new data".to_string())));
    assert!(outcome.skip);
    assert_eq!(outcome.skip_reason.as_deref(), Some("no new data"));
}

#[test]
fn process_pre_run_completion_returns_skip_without_reason_for_json_false() {
    let mut outcome = HookExecutionOutcome::default();
    let action = process_pre_run_completion(HookEvent::PreRun, Some(b"false"), &mut outcome);
    assert_eq!(action, HookCompletionAction::Skip(None));
    assert!(outcome.skip);
}

#[test]
fn process_pre_run_completion_merges_env_and_skip_in_one_payload() {
    let mut outcome = HookExecutionOutcome::default();
    let bytes = br#"{"skip": true, "reason": "soft", "extra_env": {"X": "y"}}"#;
    let action = process_pre_run_completion(HookEvent::PreRun, Some(bytes), &mut outcome);
    assert_eq!(action, HookCompletionAction::Skip(Some("soft".to_string())));
    assert_eq!(outcome.extra_env.get("X").map(String::as_str), Some("y"));
}

// --- record_hook_error ---

#[test]
fn record_hook_error_returns_err_for_fail_mode() {
    let mut hook = make_hook("h", None, false);
    hook.on_error = HookErrorMode::Fail;
    let payload = empty_pre_run_payload();
    let mut outcome = HookExecutionOutcome::default();
    let err = record_hook_error(&hook, &payload, "boom".to_string(), &mut outcome).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("hook 'h'"), "got: {msg}");
    assert!(msg.contains("boom"), "got: {msg}");
    // Outcome.errors stays empty — caller would never see Warn-style accumulation
    // when the hook is configured to abort the run.
    assert!(outcome.errors.is_empty());
}

#[test]
fn record_hook_error_pushes_for_warn_mode() {
    let mut hook = make_hook("notify", None, false);
    hook.on_error = HookErrorMode::Warn;
    let payload = empty_pre_run_payload();
    let mut outcome = HookExecutionOutcome::default();
    record_hook_error(&hook, &payload, "warn-msg".to_string(), &mut outcome).unwrap();
    assert_eq!(outcome.errors.len(), 1);
    let entry = &outcome.errors[0];
    assert_eq!(entry.hook_workflow_type, "notify");
    assert_eq!(entry.event, "pre_run");
    assert_eq!(entry.error, "warn-msg");
}

#[test]
fn record_hook_error_drops_silently_for_ignore_mode() {
    let mut hook = make_hook("h", None, false);
    hook.on_error = HookErrorMode::Ignore;
    let payload = empty_pre_run_payload();
    let mut outcome = HookExecutionOutcome::default();
    record_hook_error(&hook, &payload, "ignored".to_string(), &mut outcome).unwrap();
    assert!(outcome.errors.is_empty());
}

/// Parse a raw payload the way `process_pre_run_completion` does before
/// handing it to the sentinel/env parsers.
fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}

#[test]
fn skip_sentinel_json_false() {
    let result = parse_skip_sentinel(&json(b"false"));
    assert_eq!(result, Some((true, None)));
}

#[test]
fn skip_sentinel_object_skip_true() {
    let result = parse_skip_sentinel(&json(br#"{"skip": true}"#));
    assert_eq!(result, Some((true, None)));
}

#[test]
fn skip_sentinel_object_skip_true_with_reason() {
    let result = parse_skip_sentinel(&json(br#"{"skip": true, "reason": "not ready"}"#));
    assert_eq!(result, Some((true, Some("not ready".to_string()))));
}

#[test]
fn skip_sentinel_object_skip_false() {
    let result = parse_skip_sentinel(&json(br#"{"skip": false}"#));
    assert_eq!(result, Some((false, None)));
}

#[test]
fn skip_sentinel_json_true_is_not_skip() {
    // `true` doesn't match any sentinel pattern.
    assert_eq!(parse_skip_sentinel(&json(b"true")), None);
}

#[test]
fn skip_sentinel_string_is_not_skip() {
    assert_eq!(parse_skip_sentinel(&json(br#""hello""#)), None);
}

#[test]
fn skip_sentinel_null_is_not_skip() {
    assert_eq!(parse_skip_sentinel(&json(b"null")), None);
}

#[test]
fn process_pre_run_completion_ignores_unparseable_payload() {
    // Invalid JSON is dropped before the sentinel parsers run.
    let mut outcome = HookExecutionOutcome::default();
    let action = process_pre_run_completion(HookEvent::PreRun, Some(b"not json"), &mut outcome);
    assert_eq!(action, HookCompletionAction::Continue);
    assert!(!outcome.skip);
    assert!(outcome.extra_env.is_empty());
}

#[test]
fn skip_sentinel_object_without_skip_field() {
    // Object without "skip" field — doesn't match.
    assert_eq!(parse_skip_sentinel(&json(br#"{"success": true}"#)), None);
}

#[test]
fn extra_env_basic() {
    let value = json(br#"{"extra_env": {"FOO": "bar", "NUM": "42"}}"#);
    let Some(result) = parse_extra_env(&value) else {
        panic!("expected extra_env map");
    };
    assert_eq!(result.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(result.get("NUM").map(String::as_str), Some("42"));
}

#[test]
fn extra_env_combined_with_skip() {
    // A hook can return both extra_env and a skip sentinel.
    let value = json(br#"{"skip": true, "reason": "not ready", "extra_env": {"X": "1"}}"#);
    let Some(extra) = parse_extra_env(&value) else {
        panic!("expected extra_env map");
    };
    assert_eq!(extra.get("X").map(String::as_str), Some("1"));
    let skip = parse_skip_sentinel(&value);
    assert_eq!(skip, Some((true, Some("not ready".to_string()))));
}

#[test]
fn extra_env_non_string_values_skipped() {
    // Non-string values are silently dropped.
    let value = json(br#"{"extra_env": {"GOOD": "yes", "BAD": 123, "ALSO_BAD": null}}"#);
    let Some(result) = parse_extra_env(&value) else {
        panic!("expected extra_env map");
    };
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("GOOD").map(String::as_str), Some("yes"));
}

#[test]
fn extra_env_absent_returns_none() {
    assert!(parse_extra_env(&json(b"{}")).is_none());
    assert!(parse_extra_env(&json(br#"{"skip": true}"#)).is_none());
    assert!(parse_extra_env(&json(b"false")).is_none());
    assert!(parse_extra_env(&json(b"null")).is_none());
}
