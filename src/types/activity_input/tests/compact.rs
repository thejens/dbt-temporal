//! The compact payloads resolve to the workflow's effective env and start time.

use super::super::*;
use super::fixtures::*;
use crate::types::RunResumeState;
use chrono::{DateTime, Utc};

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
