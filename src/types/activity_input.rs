//! Wire shapes for activity inputs that carry the run's own input.
//!
//! Every activity input lands in workflow history, and history size is what
//! forces a run to continue as new. A node's input is recorded once per node,
//! so repeating the run's `env`, `vars` and `target` in it — and the env map
//! holds the whole workflow input, serialized, under `_` — multiplies the run's
//! input by the node count. The compact shape states the run once and lets the
//! activity derive the rest.
//!
//! Two shapes travel on the wire:
//!
//! * **compact**: `{"compact": { ... }}`, built by the workflow.
//! * **full**: the flat [`NodeExecutionInput`] / [`ProjectHooksInput`] struct.
//!   Activities scheduled by an earlier release are still dispatched in it
//!   after an upgrade, so the activity must keep accepting it.
//!
//! # Rolling deploys and rollbacks
//!
//! A worker that predates the compact shape must refuse a compact payload
//! rather than run it. The full structs default most of their fields, so a
//! payload that merely lacked them would deserialize into a node with an empty
//! env and the default command, and run against whatever warehouse the worker
//! was started with. Their `unique_id` / `phase` / `project` fields have no
//! default, though, and a compact payload keeps all of its fields one level
//! down under `compact`, so the earlier struct fails with a missing-field
//! error instead. That error is the compatibility mechanism: do not add
//! `#[serde(default)]` to those fields, and do not hoist compact fields to the
//! top level.

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

use super::workflow::{
    DbtRunInput, NodeExecutionInput, NodeExecutionResult, ProjectHookPhase, ProjectHooksInput,
};

/// The run-level data an activity needs, stated once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunScope {
    /// The workflow input, minus the continue-as-new marker (which says where
    /// the run resumes, not how it executes).
    pub input: DbtRunInput,
    /// Entries of the workflow's effective env that [`DbtRunInput::base_env`]
    /// does not already produce: what `pre_run` hooks injected. They win over
    /// the input's own env, as they do in the workflow.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_env: BTreeMap<String, String>,
    /// When the run started: the workflow's own value, carried through
    /// continue-as-new, so every node and hook of the run renders the same
    /// `run_started_at`. It is stated here, with the other facts that are the
    /// same for every activity of the run, rather than per node. `None` when
    /// the workflow could not read its clock; the render then keeps the
    /// worker's parse-time value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

impl RunScope {
    /// Capture a run that started at `started_at` and whose effective env is
    /// `effective_env`.
    ///
    /// The effective env only ever grows from the base env (hooks add or
    /// overwrite entries, nothing removes one), so the entries that differ
    /// from it are exactly what has to travel. For a run that continued as new
    /// the effective env comes from the first segment, whose `_` was
    /// serialized from the input before `resume_from` was set; clearing it here
    /// makes that `_` reproducible and keeps it out of `extra_env`.
    pub fn new(
        input: &DbtRunInput,
        effective_env: &BTreeMap<String, String>,
        started_at: Option<DateTime<Utc>>,
    ) -> Self {
        let mut input = input.clone();
        input.resume_from = None;
        let base = input.base_env();
        let extra_env = effective_env
            .iter()
            .filter(|(key, value)| base.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Self {
            input,
            extra_env,
            started_at,
        }
    }

    /// The env the workflow held when it built this scope.
    fn env(&self) -> BTreeMap<String, String> {
        let mut env = self.input.base_env();
        env.extend(self.extra_env.clone());
        env
    }
}

/// A node's activity input in compact form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactNodeInput {
    pub unique_id: String,
    pub invocation_id: String,
    /// Resolved project name — the plan's, which `input.project` may leave unset.
    pub project: String,
    pub run: RunScope,
}

impl CompactNodeInput {
    pub fn new(unique_id: &str, invocation_id: &str, project: &str, run: &RunScope) -> Self {
        Self {
            unique_id: unique_id.to_string(),
            invocation_id: invocation_id.to_string(),
            project: project.to_string(),
            run: run.clone(),
        }
    }

    /// The flat form the activity executes.
    pub fn resolve(self) -> NodeExecutionInput {
        let env = self.run.env();
        let started_at = self.run.started_at;
        let run = self.run.input;
        NodeExecutionInput {
            unique_id: self.unique_id,
            invocation_id: self.invocation_id,
            project: self.project,
            env,
            target: run.target,
            command: run.command,
            defer_manifest_ref: run.defer_manifest_ref,
            event_time_start: run.event_time_start,
            event_time_end: run.event_time_end,
            vars: run.vars,
            full_refresh: run.full_refresh,
            run_started_at: started_at,
        }
    }
}

/// A project-hooks activity input in compact form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactProjectHooksInput {
    pub phase: ProjectHookPhase,
    pub project: String,
    pub invocation_id: String,
    pub run: RunScope,
    #[serde(default)]
    pub node_results: Vec<NodeExecutionResult>,
    #[serde(default)]
    pub retry_on_error: bool,
    #[serde(default)]
    pub prior_segments: Vec<String>,
}

impl CompactProjectHooksInput {
    /// The flat form the activity executes.
    pub fn resolve(self) -> ProjectHooksInput {
        let env = self.run.env();
        let started_at = self.run.started_at;
        let run = self.run.input;
        ProjectHooksInput {
            phase: self.phase,
            project: self.project,
            invocation_id: self.invocation_id,
            env,
            target: run.target,
            node_results: self.node_results,
            vars: run.vars,
            full_refresh: run.full_refresh,
            retry_on_error: self.retry_on_error,
            prior_segments: self.prior_segments,
            run_started_at: started_at,
        }
    }
}

/// Wire wrapper that keeps a compact payload's fields out of the top level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactEnvelope<T> {
    pub compact: T,
}

/// An activity input as it arrives: compact, or the full struct.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ActivityInput<C, F> {
    Compact(CompactEnvelope<C>),
    Full(F),
}

impl<'de, C, F> Deserialize<'de> for ActivityInput<C, F>
where
    C: DeserializeOwned,
    F: DeserializeOwned,
{
    // Dispatches on the `compact` key rather than trying each shape in turn
    // (`#[serde(untagged)]`), so a malformed payload reports the error of the
    // shape it claims to be instead of "did not match any variant".
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("compact").is_some() {
            serde_json::from_value(value)
                .map(Self::Compact)
                .map_err(D::Error::custom)
        } else {
            serde_json::from_value(value)
                .map(Self::Full)
                .map_err(D::Error::custom)
        }
    }
}

/// Input to the `execute_node` activity.
pub type NodeActivityInput = ActivityInput<CompactNodeInput, NodeExecutionInput>;

/// Input to the `run_project_hooks` activity.
pub type ProjectHooksActivityInput = ActivityInput<CompactProjectHooksInput, ProjectHooksInput>;

impl NodeActivityInput {
    pub const fn compact(input: CompactNodeInput) -> Self {
        Self::Compact(CompactEnvelope { compact: input })
    }

    /// The flat form the activity executes, whichever shape arrived.
    pub fn resolve(self) -> NodeExecutionInput {
        match self {
            Self::Compact(envelope) => envelope.compact.resolve(),
            Self::Full(input) => input,
        }
    }
}

impl ProjectHooksActivityInput {
    pub const fn compact(input: CompactProjectHooksInput) -> Self {
        Self::Compact(CompactEnvelope { compact: input })
    }

    /// The flat form the activity executes, whichever shape arrived.
    pub fn resolve(self) -> ProjectHooksInput {
        match self {
            Self::Compact(envelope) => envelope.compact.resolve(),
            Self::Full(input) => input,
        }
    }
}

#[cfg(test)]
mod tests;
