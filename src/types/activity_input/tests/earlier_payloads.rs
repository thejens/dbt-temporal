//! The flat payloads earlier workers recorded, and the refusal of the compact
//! shape by those workers.

use super::super::*;
use super::fixtures::{compact_node, effective, run_input};
use serde_json::json;

/// `NodeExecutionInput` exactly as the earlier release defined it. An
/// earlier worker deserializes into this; if a compact payload ever parses
/// into it, that worker runs the node with an empty env and the default
/// command instead of refusing it.
mod earlier {
    use serde::Deserialize;
    use std::collections::BTreeMap;

    fn default_command() -> String {
        "build".to_string()
    }

    #[derive(Debug, Deserialize)]
    pub struct NodeExecutionInput {
        pub unique_id: String,
        pub invocation_id: String,
        pub project: String,
        #[serde(default)]
        pub env: BTreeMap<String, String>,
        #[serde(default)]
        pub target: Option<String>,
        #[serde(default = "default_command")]
        pub command: String,
        #[serde(default)]
        pub defer_manifest_ref: Option<String>,
        #[serde(default)]
        pub event_time_start: Option<String>,
        #[serde(default)]
        pub event_time_end: Option<String>,
        #[serde(default)]
        pub vars: BTreeMap<String, serde_json::Value>,
        #[serde(default)]
        pub full_refresh: bool,
    }

    #[derive(Debug, Deserialize)]
    pub enum ProjectHookPhase {
        #[serde(rename = "on_run_start")]
        OnRunStart,
        #[serde(rename = "on_run_end")]
        OnRunEnd,
    }

    #[derive(Debug, Deserialize)]
    pub struct ProjectHooksInput {
        pub phase: ProjectHookPhase,
        pub project: String,
        pub invocation_id: String,
        #[serde(default)]
        pub env: BTreeMap<String, String>,
        #[serde(default)]
        pub target: Option<String>,
        #[serde(default)]
        pub node_results: Vec<serde_json::Value>,
        #[serde(default)]
        pub vars: BTreeMap<String, serde_json::Value>,
        #[serde(default)]
        pub full_refresh: bool,
        #[serde(default)]
        pub retry_on_error: bool,
        #[serde(default)]
        pub prior_segments: Vec<String>,
    }
}

// --- earlier payloads: the flat shape recorded by workflows that predate the compact one ---

#[test]
fn a_flat_node_payload_resolves_unchanged() {
    let payload = r#"{
        "unique_id": "model.shop.orders",
        "invocation_id": "inv-9",
        "project": "shop",
        "env": {"API_TOKEN": "secret", "_": "{\"command\":\"run\"}"},
        "target": "prod",
        "command": "compile",
        "defer_manifest_ref": "s3://bucket/manifest.json",
        "event_time_start": "2026-01-01T00:00:00Z",
        "event_time_end": "2026-01-02T00:00:00Z",
        "vars": {"ratio": 0.1, "nested": {"a": [1, 2]}},
        "full_refresh": true
    }"#;

    let input: NodeActivityInput = serde_json::from_str(payload).unwrap_or_else(|e| panic!("{e}"));
    assert!(matches!(input, ActivityInput::Full(_)));
    let resolved = input.resolve();

    assert_eq!(resolved.unique_id, "model.shop.orders");
    assert_eq!(resolved.invocation_id, "inv-9");
    assert_eq!(resolved.project, "shop");
    assert_eq!(
        resolved.env,
        BTreeMap::from([
            ("API_TOKEN".to_string(), "secret".to_string()),
            ("_".to_string(), "{\"command\":\"run\"}".to_string()),
        ])
    );
    assert_eq!(resolved.target.as_deref(), Some("prod"));
    assert_eq!(resolved.command, "compile");
    assert_eq!(resolved.defer_manifest_ref.as_deref(), Some("s3://bucket/manifest.json"));
    assert_eq!(resolved.event_time_start.as_deref(), Some("2026-01-01T00:00:00Z"));
    assert_eq!(resolved.event_time_end.as_deref(), Some("2026-01-02T00:00:00Z"));
    assert_eq!(resolved.vars["ratio"], json!(0.1));
    assert_eq!(resolved.vars["nested"], json!({"a": [1, 2]}));
    assert!(resolved.full_refresh);
    assert_eq!(resolved.run_started_at, None);
}

/// Fields the flat shape defaults when absent keep their defaults.
#[test]
fn a_minimal_flat_node_payload_keeps_its_defaults() {
    let payload = r#"{"unique_id": "model.shop.a", "invocation_id": "i", "project": "shop"}"#;
    let resolved = serde_json::from_str::<NodeActivityInput>(payload)
        .unwrap_or_else(|e| panic!("{e}"))
        .resolve();
    assert_eq!(resolved.command, "build");
    assert_eq!(resolved.env, BTreeMap::new());
    assert!(resolved.target.is_none());
    assert!(!resolved.full_refresh);
}

#[test]
fn a_flat_project_hooks_payload_resolves_unchanged() {
    let payload = r#"{
        "phase": "on_run_start",
        "project": "shop",
        "invocation_id": "inv-9",
        "env": {"A": "1"},
        "target": "prod",
        "node_results": [],
        "vars": {"k": "v"},
        "full_refresh": true,
        "retry_on_error": true,
        "prior_segments": ["inv-9/seg_0.json"]
    }"#;

    let input: ProjectHooksActivityInput =
        serde_json::from_str(payload).unwrap_or_else(|e| panic!("{e}"));
    assert!(matches!(input, ActivityInput::Full(_)));
    let resolved = input.resolve();

    assert_eq!(resolved.phase, ProjectHookPhase::OnRunStart);
    assert_eq!(resolved.project, "shop");
    assert_eq!(resolved.invocation_id, "inv-9");
    assert_eq!(resolved.env, BTreeMap::from([("A".to_string(), "1".to_string())]));
    assert_eq!(resolved.target.as_deref(), Some("prod"));
    assert_eq!(resolved.vars["k"], json!("v"));
    assert!(resolved.full_refresh);
    assert!(resolved.retry_on_error);
    assert_eq!(resolved.prior_segments, ["inv-9/seg_0.json"]);
}

// --- an earlier worker must refuse the compact shape ---

#[test]
fn an_earlier_worker_cannot_deserialize_a_compact_node_payload() {
    let input = run_input(&BTreeMap::new());
    let bytes = serde_json::to_vec(&compact_node(&input, &effective(&input, &[])))
        .unwrap_or_else(|e| panic!("{e}"));

    let Err(err) = serde_json::from_slice::<earlier::NodeExecutionInput>(&bytes) else {
        panic!("a compact payload must not parse as the earlier shape");
    };
    assert!(err.to_string().contains("unique_id"), "{err}");
}

#[test]
fn an_earlier_worker_cannot_deserialize_a_compact_project_hooks_payload() {
    let input = run_input(&BTreeMap::new());
    let compact = ProjectHooksActivityInput::compact(CompactProjectHooksInput {
        phase: ProjectHookPhase::OnRunStart,
        project: "shop".into(),
        invocation_id: "inv-1".into(),
        run: RunScope::new(&input, &effective(&input, &[]), None),
        node_results: Vec::new(),
        retry_on_error: false,
        prior_segments: Vec::new(),
    });
    let bytes = serde_json::to_vec(&compact).unwrap_or_else(|e| panic!("{e}"));

    let Err(err) = serde_json::from_slice::<earlier::ProjectHooksInput>(&bytes) else {
        panic!("a compact payload must not parse as the earlier shape");
    };
    assert!(err.to_string().contains("phase"), "{err}");
}

/// The control for the two tests above: the frozen copies do accept the
/// flat shape, so their failure on compact payloads is the shape's doing.
#[test]
fn the_frozen_earlier_structs_accept_the_flat_shape() {
    let node: earlier::NodeExecutionInput = serde_json::from_str(
        r#"{"unique_id": "u", "invocation_id": "i", "project": "p", "env": {"A": "1"},
            "target": "t", "command": "run", "defer_manifest_ref": "d",
            "event_time_start": "s", "event_time_end": "e", "vars": {"k": 1},
            "full_refresh": true}"#,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        (
            node.unique_id,
            node.invocation_id,
            node.project,
            node.env,
            node.target,
            node.command,
            node.defer_manifest_ref,
            node.event_time_start,
            node.event_time_end,
            node.vars,
            node.full_refresh,
        ),
        (
            "u".to_string(),
            "i".to_string(),
            "p".to_string(),
            BTreeMap::from([("A".to_string(), "1".to_string())]),
            Some("t".to_string()),
            "run".to_string(),
            Some("d".to_string()),
            Some("s".to_string()),
            Some("e".to_string()),
            BTreeMap::from([("k".to_string(), json!(1))]),
            true,
        )
    );

    let hooks: earlier::ProjectHooksInput = serde_json::from_str(
        r#"{"phase": "on_run_end", "project": "p", "invocation_id": "i", "env": {"A": "1"},
            "target": "t", "node_results": [], "vars": {"k": 1}, "full_refresh": true,
            "retry_on_error": true, "prior_segments": ["s"]}"#,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert!(matches!(hooks.phase, earlier::ProjectHookPhase::OnRunEnd));
    assert_eq!(
        (
            hooks.project,
            hooks.invocation_id,
            hooks.env,
            hooks.target,
            hooks.node_results,
            hooks.vars,
            hooks.full_refresh,
            hooks.retry_on_error,
            hooks.prior_segments,
        ),
        (
            "p".to_string(),
            "i".to_string(),
            BTreeMap::from([("A".to_string(), "1".to_string())]),
            Some("t".to_string()),
            Vec::new(),
            BTreeMap::from([("k".to_string(), json!(1))]),
            true,
            true,
            vec!["s".to_string()],
        )
    );
}

#[test]
fn a_malformed_compact_payload_reports_the_compact_shape() {
    let Err(err) = serde_json::from_str::<NodeActivityInput>(r#"{"compact": {"unique_id": "x"}}"#)
    else {
        panic!("missing fields");
    };
    assert!(err.to_string().contains("invocation_id"), "{err}");
}

#[test]
fn a_malformed_flat_payload_reports_the_flat_shape() {
    let Err(err) = serde_json::from_str::<NodeActivityInput>(r#"{"invocation_id": "i"}"#) else {
        panic!("missing fields");
    };
    assert!(err.to_string().contains("unique_id"), "{err}");
}
