//! dbt-native telemetry spans for node-execution activities.
//!
//! Each activity opens an `Invocation` root span followed by a `NodeEvaluated`
//! span — the same event types the dbt CLI emits — so dbt observability
//! tooling reads dbt-temporal runs like CLI runs. The upstream telemetry data
//! layer derives `trace_id` from `Invocation.invocation_id`, which means every
//! activity of one workflow run stitches into a single OTLP trace even when
//! activities land on different workers, with no Temporal header propagation.
//!
//! These spans only produce output on the OTLP tracing stack
//! (`DBT_EXPORT_TO_OTLP=1`, see `crate::tracing_setup`). On the default stack
//! they are ordinary `tracing` spans with no subscriber that understands them —
//! harmless. All failures here degrade to `Span::none()`: telemetry must never
//! fail an activity that would otherwise run.

use dbt_common::tracing::span_info::record_span_status_from_attrs;
use dbt_common::tracing::{create_info_span, create_root_info_span};
use dbt_telemetry::{
    ExecutionPhase, Invocation, NodeCancelReason, NodeErrorType, NodeEvaluated, NodeOutcome,
    NodeSkipReason,
};

use crate::project_registry::ProjectRegistry;
use crate::types::{NodeExecutionInput, NodeStatus};

/// Span pair wrapping one node-execution activity.
///
/// `invocation` must stay alive for the duration of the activity (it parents
/// `node`); both close on drop.
#[derive(Debug)]
pub struct NodeExecutionSpans {
    /// Held only to keep the per-run root span open while `node` runs — the
    /// data layer reads it for trace correlation; no code reads the field.
    #[allow(dead_code)]
    pub invocation: tracing::Span,
    pub node: tracing::Span,
}

/// Build the `Invocation` → `NodeEvaluated` span pair for an activity.
///
/// Returns no-op spans when the invocation id is not a UUID (the upstream data
/// layer panics on non-UUID invocation ids) or the node cannot be resolved —
/// the activity body surfaces those as real errors.
pub fn node_execution_spans(
    registry: &ProjectRegistry,
    input: &NodeExecutionInput,
) -> NodeExecutionSpans {
    let noop = || NodeExecutionSpans {
        invocation: tracing::Span::none(),
        node: tracing::Span::none(),
    };

    if uuid::Uuid::parse_str(&input.invocation_id).is_err() {
        return noop();
    }
    let Ok(state) = registry.get(Some(&input.project)) else {
        return noop();
    };
    let Some(node) = state.resolver_state.nodes.get_node(&input.unique_id) else {
        return noop();
    };

    let invocation = create_root_info_span(Invocation {
        invocation_id: input.invocation_id.clone(),
        raw_command: format!("dbt {}", input.command),
        eval_args: None,
        process_info: None,
        metrics: None,
        parent_span_id: None,
    });

    let event = node.get_node_evaluated_event(
        ExecutionPhase::Run,
        &state.io_args.in_dir,
        &state.io_args.out_dir,
    );
    // Contextual parenting: created while the invocation span is entered.
    let node_span = invocation.in_scope(|| create_info_span(event));

    NodeExecutionSpans {
        invocation,
        node: node_span,
    }
}

/// Record the node's terminal outcome on the `NodeEvaluated` span.
///
/// No-ops when the span carries no `NodeEvaluated` attributes (no-op spans,
/// default tracing stack).
pub fn record_outcome(spans: &NodeExecutionSpans, status: NodeStatus) {
    record_span_status_from_attrs(&spans.node, |attrs| {
        if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
            match status {
                NodeStatus::Success => ev.set_node_outcome(NodeOutcome::Success),
                NodeStatus::Error => {
                    ev.set_node_outcome(NodeOutcome::Error);
                    ev.set_node_error_type(NodeErrorType::User);
                }
                NodeStatus::Skipped => {
                    ev.set_node_outcome(NodeOutcome::Skipped);
                    ev.set_node_skip_reason(NodeSkipReason::Upstream);
                }
                NodeStatus::Cancelled => {
                    ev.set_node_outcome(NodeOutcome::Canceled);
                    ev.set_node_cancel_reason(NodeCancelReason::UserCancelled);
                }
                // Non-terminal statuses never reach here; leave Unspecified
                // rather than inventing an outcome.
                NodeStatus::Pending | NodeStatus::Running => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    fn input(invocation_id: &str) -> NodeExecutionInput {
        NodeExecutionInput {
            unique_id: "model.p.m".to_string(),
            invocation_id: invocation_id.to_string(),
            project: "p".to_string(),
            env: BTreeMap::new(),
            target: None,
            command: "build".to_string(),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
        }
    }

    #[test]
    fn non_uuid_invocation_id_yields_noop_spans() {
        let registry = ProjectRegistry::new(BTreeMap::new());
        let spans = node_execution_spans(&registry, &input("not-a-uuid"));
        assert!(spans.invocation.is_none());
        assert!(spans.node.is_none());
    }

    #[test]
    fn unknown_project_yields_noop_spans() {
        let registry = ProjectRegistry::new(BTreeMap::new());
        let spans = node_execution_spans(&registry, &input("00000000-0000-0000-0000-000000000001"));
        assert!(spans.invocation.is_none());
        assert!(spans.node.is_none());
    }

    #[test]
    fn record_outcome_on_noop_spans_does_not_panic() {
        let spans = NodeExecutionSpans {
            invocation: tracing::Span::none(),
            node: tracing::Span::none(),
        };
        for status in [
            NodeStatus::Success,
            NodeStatus::Error,
            NodeStatus::Skipped,
            NodeStatus::Cancelled,
            NodeStatus::Pending,
            NodeStatus::Running,
        ] {
            record_outcome(&spans, status);
        }
    }
}
