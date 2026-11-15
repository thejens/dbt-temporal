use super::{empty_pre_run_payload, make_hook};
use crate::hooks::*;

// --- build_hook_workflow_id ---

#[test]
fn build_hook_workflow_id_includes_parent_event_index_and_type() {
    let id = build_hook_workflow_id("dbt-run-abc", HookEvent::PreRun, 2, "notify");
    assert_eq!(id, "dbt-run-abc-hook-pre_run-2-notify");
}

#[test]
fn build_hook_workflow_id_distinguishes_each_event() {
    let pre = build_hook_workflow_id("p", HookEvent::PreRun, 0, "h");
    let succ = build_hook_workflow_id("p", HookEvent::OnSuccess, 0, "h");
    let fail = build_hook_workflow_id("p", HookEvent::OnFailure, 0, "h");
    assert_ne!(pre, succ);
    assert_ne!(pre, fail);
    assert_ne!(succ, fail);
}

// --- build_hook_options ---

#[test]
fn build_hook_options_sets_workflow_id_and_task_queue() {
    let hook = make_hook("notify", Some(30), false);
    let opts = build_hook_options(&hook, "id-123".to_string(), HookEvent::PreRun, 0);
    assert_eq!(opts.workflow_id.as_deref(), Some("id-123"));
    assert_eq!(opts.task_queue.as_deref(), Some("q"));
    assert_eq!(opts.execution_timeout, Some(Duration::from_secs(30)));
}

#[test]
fn build_hook_options_uses_default_timeout_when_unset() {
    let hook = make_hook("notify", None, false);
    let opts = build_hook_options(&hook, "id".to_string(), HookEvent::PreRun, 0);
    assert_eq!(opts.execution_timeout, Some(Duration::from_secs(DEFAULT_HOOK_TIMEOUT_SECS)));
}

#[test]
fn build_hook_options_abandons_only_for_fire_and_forget() {
    let normal =
        build_hook_options(&make_hook("h", None, false), "id".to_string(), HookEvent::PreRun, 0);
    let fire =
        build_hook_options(&make_hook("h", None, true), "id".to_string(), HookEvent::PreRun, 0);
    assert_eq!(normal.parent_close_policy, ParentClosePolicy::Unspecified);
    assert_eq!(fire.parent_close_policy, ParentClosePolicy::Abandon);
}

#[test]
fn build_hook_options_sets_static_summary() {
    let opts = build_hook_options(
        &make_hook("notify-slack", None, false),
        "id".to_string(),
        HookEvent::OnFailure,
        2,
    );
    let summary = opts.static_summary.expect("summary set");
    assert!(summary.contains("notify-slack"), "got: {summary}");
    assert!(summary.contains("#2"), "got: {summary}");

    let fnf = build_hook_options(
        &make_hook("notify-slack", None, true),
        "id".to_string(),
        HookEvent::OnSuccess,
        0,
    );
    let summary = fnf.static_summary.expect("summary set");
    assert!(summary.contains("fire-and-forget"), "got: {summary}");
}

// --- build_hook_input_payload ---

#[test]
fn build_hook_input_payload_uses_custom_input_when_present() -> anyhow::Result<()> {
    let mut hook = make_hook("notify", None, false);
    hook.input = Some(serde_json::json!({"target": "bigquery", "force": true}));
    let payload = empty_pre_run_payload();

    let payloads = build_hook_input_payload(&hook, &payload)?;
    assert_eq!(payloads.len(), 1);
    // The serialized payload bytes should reflect the custom input, not
    // the canonical HookPayload (which would include `event` etc).
    let bytes = &payloads[0].data;
    let json: serde_json::Value = serde_json::from_slice(bytes)?;
    assert_eq!(json["target"], "bigquery");
    assert_eq!(json["force"], true);
    // No event field — confirms it's the custom input, not the default.
    assert!(json.get("event").is_none());
    Ok(())
}

#[test]
fn build_hook_input_payload_falls_back_to_default_payload() -> anyhow::Result<()> {
    let hook = make_hook("notify", None, false);
    // hook.input = None — fallback path
    let payload = empty_pre_run_payload();

    let payloads = build_hook_input_payload(&hook, &payload)?;
    assert_eq!(payloads.len(), 1);
    let bytes = &payloads[0].data;
    let json: serde_json::Value = serde_json::from_slice(bytes)?;
    // Default HookPayload includes `event`.
    assert_eq!(json["event"], "pre_run");
    assert_eq!(json["invocation_id"], "inv");
    Ok(())
}
