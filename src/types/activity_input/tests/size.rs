//! Payload size.

use super::super::*;
use super::fixtures::*;
use serde_json::json;

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
