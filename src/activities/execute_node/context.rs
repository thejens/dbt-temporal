//! The values the phases of one node execution share.

use std::collections::BTreeMap;

use dbt_adapter::load_store::ResultStore;
use dbt_schemas::schemas::nodes::{CommonAttributes, InternalDbtNode, NodeBaseAttributes};
use dbt_schemas::schemas::telemetry::NodeType;
use tracing::info;

use crate::error::DbtTemporalError;
use crate::types::{DbtCommand, NodeExecutionInput};
use crate::worker_state::WorkerState;

use super::workspace::ActivityWorkspace;

/// The node being executed and the run it belongs to.
///
/// Built once per activity and read by every phase.
pub(super) struct NodeRun<'a> {
    pub(super) state: &'a WorkerState,
    pub(super) input: &'a NodeExecutionInput,
    pub(super) unique_id: &'a str,
    pub(super) node: &'a dyn InternalDbtNode,
    pub(super) common: &'a CommonAttributes,
    pub(super) base: &'a NodeBaseAttributes,
    pub(super) resource_type: NodeType,
    /// Measured, not built. Sources only ever appear in freshness plans; the
    /// unified `freshness` command additionally measures models carrying an
    /// SLA. Neither compiles model SQL nor materializes anything, so both skip
    /// straight from context building to the freshness query.
    pub(super) measures_freshness: bool,
}

impl<'a> NodeRun<'a> {
    /// Look the node up in the resolver state and refuse one that failed to
    /// resolve.
    pub(super) fn locate(
        state: &'a WorkerState,
        input: &'a NodeExecutionInput,
    ) -> Result<Self, anyhow::Error> {
        let unique_id = input.unique_id.as_str();

        // Look up the node in the resolver state.
        let node = state
            .resolver_state
            .nodes
            .get_node(unique_id)
            .ok_or_else(|| {
                DbtTemporalError::ProjectNotFound(format!("node not found: {unique_id}"))
            })?;

        // Fail early if this node had resolution errors (e.g. broken ref/source).
        if state
            .resolver_state
            .nodes_with_resolution_errors
            .contains(unique_id)
        {
            return Err(DbtTemporalError::Compilation(format!(
                "node {unique_id} has unresolved compilation errors"
            ))
            .into());
        }

        let resource_type = node.resource_type();
        info!(node = %unique_id, resource_type = resource_type.as_str_name(), "executing node");

        let measures_freshness = resource_type == NodeType::Source
            || (resource_type == NodeType::Model
                && DbtCommand::parse(&input.command) == Some(DbtCommand::Freshness));

        Ok(Self {
            state,
            input,
            unique_id,
            node,
            common: node.common(),
            base: node.base(),
            resource_type,
            measures_freshness,
        })
    }

    /// `dbt compile` renders SQL but sends nothing to the warehouse.
    pub(super) fn is_compile_only(&self) -> bool {
        DbtCommand::parse(&self.input.command) == Some(DbtCommand::Compile)
    }
}

/// The node's rendered Jinja context, ready for a materialization.
pub(super) struct CompiledNode {
    pub(super) node_context: BTreeMap<String, minijinja::Value>,
    /// The store the context's `store_result` / `load_result` closures write
    /// to, so every later result extraction (adapter_response, test failures,
    /// unit-test outcomes) must read from this one and not one built alongside
    /// it.
    pub(super) result_store: ResultStore,
    /// Owns the scratch directory the materialization reads compiled SQL back
    /// from; dropping it deletes the directory, so it must outlive the render.
    pub(super) _workspace: ActivityWorkspace,
}

/// Wall-clock marks of the phases that have finished, and the stopwatch the
/// node's `execution_time` is read from.
pub(super) struct Timeline {
    pub(super) started: std::time::Instant,
    pub(super) compile_start: chrono::DateTime<chrono::Utc>,
    pub(super) compile_end: chrono::DateTime<chrono::Utc>,
}

impl Timeline {
    pub(super) fn elapsed_secs(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    pub(super) const fn compile_window(
        &self,
    ) -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
        (self.compile_start, self.compile_end)
    }
}
