//! Run inputs and payload helpers shared by the activity-input tests.

use super::super::*;
use serde_json::json;

pub(super) fn run_input(env: &BTreeMap<String, String>) -> DbtRunInput {
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

pub(super) fn effective(input: &DbtRunInput, extra: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut env = input.base_env();
    env.extend(
        extra
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
    );
    env
}

pub(super) fn compact_node(
    input: &DbtRunInput,
    env: &BTreeMap<String, String>,
) -> NodeActivityInput {
    NodeActivityInput::compact(CompactNodeInput::new(
        "model.shop.orders",
        "inv-1",
        "shop",
        &RunScope::new(input, env, None),
    ))
}

/// What a payload crosses the wire as: serialized by the workflow,
/// deserialized by the activity.
pub(super) fn over_the_wire(input: &NodeActivityInput) -> NodeActivityInput {
    serde_json::from_slice(&serde_json::to_vec(input).unwrap_or_else(|e| panic!("{e}")))
        .unwrap_or_else(|e| panic!("{e}"))
}
