use super::*;
use crate::types::RunResumeState;
use chrono::{DateTime, Utc};
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

fn run_input(env: &BTreeMap<String, String>) -> DbtRunInput {
    serde_json::from_value(json!({
        "project": "shop",
        "command": "run",
        "select": "tag:nightly",
        "vars": {
            "ratio": 0.1,
            "tiny": 1e-7,
            "big": 18_446_744_073_709_551_615_u64,
            "negative": -42,
            "nested": {"list": [1, 2.5, "three"], "quote": "say \"hi\"\n", "unicode": "åäö ✓"},
        },
        "target": "prod",
        "full_refresh": true,
        "hooks": {"pre_run": [], "on_success": [], "on_failure": []},
        "env": env,
        "defer_manifest_ref": "s3://bucket/manifest.json",
        "event_time_start": "2026-01-01T00:00:00Z",
        "event_time_end": "2026-01-02T00:00:00Z",
    }))
    .unwrap_or_else(|e| panic!("{e}"))
}

fn effective(input: &DbtRunInput, extra: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut env = input.base_env();
    env.extend(
        extra
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
    );
    env
}

fn compact_node(input: &DbtRunInput, env: &BTreeMap<String, String>) -> NodeActivityInput {
    NodeActivityInput::compact(CompactNodeInput::new(
        "model.shop.orders",
        "inv-1",
        "shop",
        &RunScope::new(input, env, None),
    ))
}

/// What a payload crosses the wire as: serialized by the workflow,
/// deserialized by the activity.
fn over_the_wire(input: &NodeActivityInput) -> NodeActivityInput {
    serde_json::from_slice(&serde_json::to_vec(input).unwrap_or_else(|e| panic!("{e}")))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// The activity must see exactly the env the workflow held — including the
/// bytes of `_`, which models read through `env_var('_')`.
#[test]
fn compact_node_input_resolves_to_the_workflows_effective_env() {
    let input = run_input(&BTreeMap::from([("API_TOKEN".into(), "secret".into())]));
    let env = effective(&input, &[]);

    let resolved = over_the_wire(&compact_node(&input, &env)).resolve();

    assert_eq!(resolved.env, env);
    assert_eq!(resolved.env.get("_"), env.get("_"));
    assert_eq!(resolved.unique_id, "model.shop.orders");
    assert_eq!(resolved.invocation_id, "inv-1");
    assert_eq!(resolved.project, "shop");
    assert_eq!(resolved.target.as_deref(), Some("prod"));
    assert_eq!(resolved.command, "run");
    assert_eq!(resolved.defer_manifest_ref.as_deref(), Some("s3://bucket/manifest.json"));
    assert_eq!(resolved.event_time_start.as_deref(), Some("2026-01-01T00:00:00Z"));
    assert_eq!(resolved.event_time_end.as_deref(), Some("2026-01-02T00:00:00Z"));
    assert_eq!(resolved.vars, input.vars);
    assert!(resolved.full_refresh);
}

/// A hook's `extra_env` overwrites the input's own entry and even `_`, as
/// `effective_env.extend(extra_env)` does in the workflow; entries the hook
/// leaves alone must not be touched.
#[test]
fn hook_injected_env_wins_exactly_as_it_does_in_the_workflow() {
    let input = run_input(&BTreeMap::from([
        ("API_TOKEN".into(), "from-input".into()),
        ("KEEP".into(), "kept".into()),
    ]));
    let env = effective(
        &input,
        &[
            ("API_TOKEN", "from-hook"),
            ("_", "hook-owned"),
            ("NEW", "1"),
        ],
    );

    let scope = RunScope::new(&input, &env, None);
    assert_eq!(
        scope
            .extra_env
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["API_TOKEN", "NEW", "_"],
        "only what differs from the base env travels",
    );

    let resolved = over_the_wire(&compact_node(&input, &env)).resolve();
    assert_eq!(resolved.env, env);
    assert_eq!(resolved.env.get("_").map(String::as_str), Some("hook-owned"));
    assert_eq!(resolved.env.get("KEEP").map(String::as_str), Some("kept"));
}

/// A hook that sets a key to the value the input already gives it changes
/// nothing, and costs nothing on the wire.
#[test]
fn hook_env_equal_to_the_base_is_not_repeated() {
    let input = run_input(&BTreeMap::from([("A".into(), "1".into())]));
    let env = effective(&input, &[("A", "1")]);
    assert!(RunScope::new(&input, &env, None).extra_env.is_empty());
}

/// The successor of a continued run holds the first segment's effective
/// env, whose `_` was serialized before `resume_from` existed. Its payload
/// must reproduce that `_` rather than ship it a second time.
#[test]
fn a_continued_run_reproduces_the_first_segments_underscore() {
    let first = run_input(&BTreeMap::from([("A".into(), "1".into())]));
    let env = effective(&first, &[("FROM_HOOK", "x")]);

    let mut continued = first;
    continued.resume_from = Some(RunResumeState {
        invocation_id: "inv-1".into(),
        state_ref: "inv-1/seg_0.json".into(),
        next_level: 3,
        segment: 1,
        fail_fast_override: None,
    });

    let scope = RunScope::new(&continued, &env, None);
    assert_eq!(scope.extra_env, BTreeMap::from([("FROM_HOOK".to_string(), "x".to_string())]));
    assert!(scope.input.resume_from.is_none());
    let resolved = over_the_wire(&compact_node(&continued, &env)).resolve();
    assert_eq!(resolved.env, env);
}

/// Deserializing a run and serializing it again must reproduce its bytes,
/// or the activity's `_` would drift from the workflow's. Vars carry the
/// awkward values: floats, the largest u64, escapes, non-ASCII.
#[test]
fn the_run_input_reserializes_to_the_same_bytes() {
    let input = run_input(&BTreeMap::new());
    let first = serde_json::to_string(&input).unwrap_or_else(|e| panic!("{e}"));
    let again: DbtRunInput = serde_json::from_str(&first).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(serde_json::to_string(&again).unwrap_or_else(|e| panic!("{e}")), first);
}

#[test]
fn compact_project_hooks_input_resolves_to_the_flat_form() {
    let input = run_input(&BTreeMap::from([("A".into(), "1".into())]));
    let env = effective(&input, &[("FROM_HOOK", "x")]);
    let compact = ProjectHooksActivityInput::compact(CompactProjectHooksInput {
        phase: ProjectHookPhase::OnRunEnd,
        project: "shop".into(),
        invocation_id: "inv-1".into(),
        run: RunScope::new(&input, &env, None),
        node_results: Vec::new(),
        retry_on_error: true,
        prior_segments: vec!["inv-1/seg_0.json".into()],
    });
    let wire: ProjectHooksActivityInput =
        serde_json::from_slice(&serde_json::to_vec(&compact).unwrap_or_else(|e| panic!("{e}")))
            .unwrap_or_else(|e| panic!("{e}"));

    let resolved = wire.resolve();
    assert_eq!(resolved.phase, ProjectHookPhase::OnRunEnd);
    assert_eq!(resolved.project, "shop");
    assert_eq!(resolved.invocation_id, "inv-1");
    assert_eq!(resolved.env, env);
    assert_eq!(resolved.target.as_deref(), Some("prod"));
    assert_eq!(resolved.vars, input.vars);
    assert!(resolved.full_refresh);
    assert!(resolved.retry_on_error);
    assert_eq!(resolved.prior_segments, ["inv-1/seg_0.json"]);
    assert!(resolved.node_results.is_empty());
}

// --- the run's start time ---

fn run_start() -> DateTime<Utc> {
    "2026-03-04T05:06:07.123456789Z"
        .parse()
        .unwrap_or_else(|e| panic!("{e}"))
}

/// Every node and hook of a run renders one `run_started_at`: the workflow's,
/// carried in the run scope and surviving the trip through history.
#[test]
fn the_runs_start_time_reaches_node_and_hook_inputs() {
    let input = run_input(&BTreeMap::new());
    let env = effective(&input, &[]);
    let scope = RunScope::new(&input, &env, Some(run_start()));

    let node = NodeActivityInput::compact(CompactNodeInput::new(
        "model.shop.orders",
        "inv-1",
        "shop",
        &scope,
    ));
    assert_eq!(over_the_wire(&node).resolve().run_started_at, Some(run_start()));

    let hooks = ProjectHooksActivityInput::compact(CompactProjectHooksInput {
        phase: ProjectHookPhase::OnRunStart,
        project: "shop".into(),
        invocation_id: "inv-1".into(),
        run: scope,
        node_results: Vec::new(),
        retry_on_error: false,
        prior_segments: Vec::new(),
    });
    let wire: ProjectHooksActivityInput =
        serde_json::from_slice(&serde_json::to_vec(&hooks).unwrap_or_else(|e| panic!("{e}")))
            .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(wire.resolve().run_started_at, Some(run_start()));
}

/// Without a start time the payload carries no key for it, so a run whose
/// clock was unreadable is byte-identical to one from an earlier release.
#[test]
fn an_unknown_start_time_is_left_out_of_the_payload() {
    let input = run_input(&BTreeMap::new());
    let scope = RunScope::new(&input, &effective(&input, &[]), None);
    let json = serde_json::to_value(&scope).unwrap_or_else(|e| panic!("{e}"));
    assert!(json.get("started_at").is_none(), "{json}");
}

/// Temporal redelivers the input an activity was scheduled with, so a compact
/// payload recorded before the start time existed must still deserialize.
#[test]
fn a_compact_payload_without_a_start_time_still_deserializes() {
    let input = run_input(&BTreeMap::new());
    let compact = compact_node(&input, &effective(&input, &[]));
    let mut payload = serde_json::to_value(&compact).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        payload["compact"]["run"]
            .as_object_mut()
            .is_some_and(|run| {
                run.remove("started_at");
                true
            })
    );

    let resolved = serde_json::from_value::<NodeActivityInput>(payload)
        .unwrap_or_else(|e| panic!("{e}"))
        .resolve();
    assert_eq!(resolved.run_started_at, None);
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

// --- size ---

/// The flat input the workflow built before the compact shape existed.
fn flat_node_input(input: &DbtRunInput, env: &BTreeMap<String, String>) -> NodeExecutionInput {
    NodeExecutionInput {
        unique_id: "model.shop.orders".into(),
        invocation_id: "inv-1".into(),
        project: "shop".into(),
        env: env.clone(),
        target: input.target.clone(),
        command: input.command.clone(),
        defer_manifest_ref: input.defer_manifest_ref.clone(),
        event_time_start: input.event_time_start.clone(),
        event_time_end: input.event_time_end.clone(),
        vars: input.vars.clone(),
        full_refresh: input.full_refresh,
        run_started_at: None,
    }
}

/// Every node's `ActivityTaskScheduled` event carries this payload, so its
/// size multiplies by the node count. A realistic env is a couple of dozen
/// entries of connection strings and tokens.
#[test]
fn the_compact_node_payload_is_about_half_the_flat_one() {
    let env: BTreeMap<String, String> = (0..20)
        .map(|i| (format!("SERVICE_SETTING_{i:02}"), format!("{i:02}").repeat(100)))
        .collect();
    let mut input = run_input(&env);
    input.vars = (0..10)
        .map(|i| {
            (
                format!("var_{i}"),
                json!({"values": [i, i + 1, i + 2], "label": "x".repeat(40)}),
            )
        })
        .collect();
    let effective = effective(&input, &[]);

    let flat = serde_json::to_vec(&NodeActivityInput::Full(flat_node_input(&input, &effective)))
        .unwrap_or_else(|e| panic!("{e}"))
        .len();
    let compact = serde_json::to_vec(&compact_node(&input, &effective))
        .unwrap_or_else(|e| panic!("{e}"))
        .len();

    assert!(
        compact * 100 < flat * 55,
        "compact payload is {compact} bytes against {flat} for the flat one (limit 55%)",
    );
}
