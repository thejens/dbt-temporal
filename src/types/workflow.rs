use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::hooks::{HookError, HooksConfig};
use super::{NodeExecutionResult, RunResumeState, StoreArtifactsOutput};

pub(super) fn default_command() -> String {
    "build".to_string()
}

/// `dbt source freshness` — measures sources only, and always writes
/// `sources.json`.
pub const SOURCE_FRESHNESS_COMMAND: &str = "source-freshness";

/// `dbt freshness` — measures sources *and* models carrying a freshness SLA,
/// and additionally writes `freshness.json`.
pub const FRESHNESS_COMMAND: &str = "freshness";

/// The dbt command a run executes.
///
/// The wire form stays a string — it is workflow input, spelled the way dbt
/// spells it — but everything that branches on it goes through this. An
/// unrecognized command would match no arm of the planner's `match` and
/// produce an empty plan: the run would succeed, do nothing, and say nothing.
/// Parsing it once at the boundary turns that into an error naming the
/// commands that exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbtCommand {
    Run,
    Build,
    Test,
    Seed,
    Snapshot,
    Compile,
    List,
    SourceFreshness,
    Freshness,
}

impl DbtCommand {
    /// Every command, in the order the error message lists them.
    pub const ALL: [Self; 9] = [
        Self::Build,
        Self::Run,
        Self::Test,
        Self::Seed,
        Self::Snapshot,
        Self::Compile,
        Self::List,
        Self::SourceFreshness,
        Self::Freshness,
    ];

    /// The wire spelling. Unchanged from the strings this replaces — a run
    /// already in flight, and every caller, spells the command this way.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Build => "build",
            Self::Test => "test",
            Self::Seed => "seed",
            Self::Snapshot => "snapshot",
            Self::Compile => "compile",
            Self::List => "list",
            Self::SourceFreshness => SOURCE_FRESHNESS_COMMAND,
            Self::Freshness => FRESHNESS_COMMAND,
        }
    }

    pub fn parse(command: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == command)
    }

    /// Measures freshness rather than building nodes.
    pub const fn is_freshness(self) -> bool {
        matches!(self, Self::SourceFreshness | Self::Freshness)
    }

    /// Whether a selection that matches nothing ends the run before its
    /// on-run-start and on-run-end hooks. dbt short-circuits these commands
    /// when the schedule is empty (`should_skip_tasks_when_no_selected_nodes`
    /// in `dbt-main/src/compilation.rs`), so hooks that issue warehouse calls
    /// do not fire for a run with nothing to do. The other commands run their
    /// hooks regardless.
    pub const fn skips_hooks_when_nothing_selected(self) -> bool {
        matches!(self, Self::Run | Self::Test | Self::Build | Self::Seed | Self::Snapshot)
    }

    /// Selects the whole graph rather than one resource type. Only these
    /// commands warn about resource types this worker does not support: the
    /// single-resource commands legitimately ignore everything else.
    pub const fn builds_whole_graph(self) -> bool {
        matches!(self, Self::Build | Self::List)
    }
}

impl std::fmt::Display for DbtCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Workflow input — what the user provides when starting the workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtRunInput {
    /// Project name or path. Optional if only one project is loaded.
    pub project: Option<String>,
    /// dbt command: "run", "build", "test", "seed", "snapshot", "compile",
    /// "list", "source-freshness", or "freshness" (default: "build")
    #[serde(default = "default_command")]
    pub command: String,
    /// --select filter
    pub select: Option<String>,
    /// --exclude filter
    pub exclude: Option<String>,
    /// `--indirect-selection`: how far to pull in tests hanging off the
    /// selected nodes. `eager` (dbt's default), `cautious`, `buildable`, or
    /// `empty`. Only meaningful alongside `select`/`exclude`.
    #[serde(default)]
    pub indirect_selection: Option<String>,
    /// `--resource-type`: restrict the plan to these resource types
    /// ("model", "source", "seed", "snapshot", "test", "unit_test", ...).
    /// Empty means no restriction. Composes with the command's own node-type
    /// filter — it can only narrow, never widen.
    #[serde(default)]
    pub resource_types: Vec<String>,
    /// `--exclude-resource-type`: drop these resource types from the plan.
    /// Applied after `resource_types`, so an excluded type loses.
    #[serde(default)]
    pub exclude_resource_types: Vec<String>,
    /// --vars overrides
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
    /// --target override
    pub target: Option<String>,
    /// --full-refresh
    #[serde(default)]
    pub full_refresh: bool,
    /// Stop on first failure
    #[serde(default)]
    pub fail_fast: bool,
    /// Lifecycle hooks — overrides file-based defaults from dbt_temporal.yml.
    #[serde(default)]
    pub hooks: Option<HooksConfig>,
    /// Per-workflow environment variable overrides.
    /// These override `env_var()` calls in model SQL, macros, and Jinja rendering.
    /// Each workflow gets its own isolated env — parallel workflows don't share state.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Path/URI of a previous manifest.json to defer to (--defer --state equivalent).
    /// When set, unbuilt upstream refs resolve against the relations in this manifest
    /// rather than failing. Typically the `manifest_path` from a prior run's artifacts.
    #[serde(default)]
    pub defer_manifest_ref: Option<String>,
    /// Microbatch window start (RFC 3339). When set together with `event_time_end`,
    /// ref() and source() calls in microbatch models are filtered to this time window.
    #[serde(default)]
    pub event_time_start: Option<String>,
    /// Microbatch window end (RFC 3339, exclusive).
    #[serde(default)]
    pub event_time_end: Option<String>,
    /// Path/URI of a previous run's `run_results.json` (e.g. the
    /// `run_results_path` from that run's artifacts). When set, the plan keeps
    /// only nodes that did NOT succeed in that run (error/skipped/cancelled) —
    /// the equivalent of `dbt retry` when started with the same
    /// select/exclude as the original run.
    #[serde(default)]
    pub retry_from: Option<String>,
    /// Path/URI of a previous manifest.json used by `state:` selectors
    /// (`--state` equivalent). Required when `select`/`exclude` use
    /// `state:modified` or `state:new`. Often the same artifact as
    /// `defer_manifest_ref`.
    #[serde(default)]
    pub state_manifest_ref: Option<String>,
    /// Set only by continue-as-new, never by a caller. Points at the state the
    /// previous segment of this run spilled to the artifact store.
    ///
    /// A dbt run can outgrow Temporal's per-workflow history limit (~51k
    /// events; roughly three per node plus retries and memo writes). When the
    /// server signals `continue_as_new_suggested`, the level loop stops at the
    /// next boundary and the run restarts from here with a fresh history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_from: Option<RunResumeState>,
}

impl DbtRunInput {
    /// The env a run executes with before any `pre_run` hook adds to it: the
    /// input's own `env`, plus the input itself serialized under `_` (mirroring
    /// bash's `$_`) so models and macros can read the full payload through
    /// `env_var('_')`.
    ///
    /// A caller-supplied `_` is always overwritten: the only meaningful value
    /// is the payload that actually started the workflow. Serializing a
    /// `DbtRunInput` cannot fail; should it ever, `_` is left unset rather than
    /// failing the run.
    pub fn base_env(&self) -> BTreeMap<String, String> {
        let mut env = self.env.clone();
        if let Ok(json) = serde_json::to_string(self) {
            env.insert("_".to_string(), json);
        }
        env
    }
}

/// Final workflow output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtRunOutput {
    pub invocation_id: String,
    pub success: bool,
    /// Whether the run was skipped by a pre_run hook returning a skip sentinel.
    #[serde(default)]
    pub skipped: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    /// Results from this execution.
    ///
    /// A run long enough to have continued as new reports the final segment's
    /// nodes here, not the whole run's: carrying every segment's results
    /// forward is what put the run's entire output into each successor's
    /// history. `run_results.json` still describes the complete run — the
    /// artifact activity collects each segment from its own checkpoint.
    pub node_results: Vec<NodeExecutionResult>,
    /// Whether `node_results` is only the final segment of a continued run.
    /// When true the complete set is in the `run_results.json` named by
    /// `artifacts.run_results_path`.
    #[serde(default)]
    pub node_results_partial: bool,
    pub elapsed_time: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<StoreArtifactsOutput>,
    /// Path/URI where log.txt was stored (if run-log writing was enabled).
    #[serde(default)]
    pub log_path: Option<String>,
    /// Errors from lifecycle hooks that used `on_error: warn`.
    #[serde(default)]
    pub hook_errors: Vec<HookError>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn freshness_commands_are_recognised() {
        assert!(DbtCommand::SourceFreshness.is_freshness());
        assert!(DbtCommand::Freshness.is_freshness());
        assert!(!DbtCommand::Build.is_freshness());
        assert!(!DbtCommand::Run.is_freshness());
        // Near-misses must not slip through: the spellings are exact.
        // The wire spelling is dbt's, hyphen included.
        assert_eq!(DbtCommand::parse("source freshness"), None);
        assert_eq!(DbtCommand::parse(""), None);
    }

    #[test]
    fn dbt_run_input_json_round_trip() -> anyhow::Result<()> {
        let input = DbtRunInput {
            project: Some("waffle".into()),
            indirect_selection: None,
            resource_types: Vec::new(),
            exclude_resource_types: Vec::new(),
            resume_from: None,
            command: "run".into(),
            select: Some("+stg_customers".into()),
            exclude: None,
            vars: BTreeMap::from([("key".into(), serde_json::json!("val"))]),
            target: Some("prod".into()),
            full_refresh: true,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::from([("PG_PORT".into(), "5433".into())]),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
            retry_from: None,
            state_manifest_ref: None,
        };
        let json = serde_json::to_string(&input)?;
        let back: DbtRunInput = serde_json::from_str(&json)?;
        assert_eq!(back.project.as_deref(), Some("waffle"));
        assert_eq!(back.command, "run");
        assert_eq!(back.select.as_deref(), Some("+stg_customers"));
        assert!(back.full_refresh);
        assert!(!back.fail_fast);
        assert_eq!(
            back.env
                .get("PG_PORT")
                .ok_or_else(|| anyhow::anyhow!("PG_PORT missing"))?,
            "5433"
        );
        Ok(())
    }

    #[test]
    fn dbt_run_input_command_defaults_to_build() -> anyhow::Result<()> {
        // Other fields all use the same `#[serde(default)]` shape; the explicit
        // `command` default is the interesting one (build vs. empty).
        let json = r#"{"project": null}"#;
        let input: DbtRunInput = serde_json::from_str(json)?;
        assert_eq!(input.command, "build");
        Ok(())
    }

    /// Outputs recorded before `node_results_partial` existed are complete
    /// ones, so the flag defaults to false.
    #[test]
    fn old_output_without_partial_flag_deserializes_as_complete() {
        let out: DbtRunOutput = serde_json::from_value(serde_json::json!({
            "invocation_id": "inv",
            "success": true,
            "node_results": [],
            "elapsed_time": 1.0,
        }))
        .unwrap();
        assert!(!out.node_results_partial);
    }

    #[test]
    fn partial_flag_round_trips() {
        let out: DbtRunOutput = serde_json::from_value(serde_json::json!({
            "invocation_id": "inv",
            "success": true,
            "node_results": [],
            "node_results_partial": true,
            "elapsed_time": 1.0,
        }))
        .unwrap();
        assert!(out.node_results_partial);
        let again: DbtRunOutput =
            serde_json::from_str(&serde_json::to_string(&out).unwrap()).unwrap();
        assert!(again.node_results_partial);
    }

    #[test]
    fn effective_env_carries_input_env_through() {
        let input = DbtRunInput {
            project: None,
            indirect_selection: None,
            resource_types: Vec::new(),
            exclude_resource_types: Vec::new(),
            resume_from: None,
            command: "build".into(),
            select: None,
            exclude: None,
            vars: BTreeMap::new(),
            target: None,
            full_refresh: false,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::from([("API_TOKEN".into(), "secret-123".into())]),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
            retry_from: None,
            state_manifest_ref: None,
        };
        let env = input.base_env();
        assert_eq!(env.get("API_TOKEN").map(String::as_str), Some("secret-123"));
    }

    #[test]
    fn effective_env_injects_underscore_with_serialised_input() -> anyhow::Result<()> {
        // The workflow exposes the full payload via env_var('_'),
        // mirroring bash's `$_`. Macros parse this JSON to read the original
        // command, so the field must round-trip the input verbatim.
        let input = DbtRunInput {
            project: Some("waffle".into()),
            indirect_selection: None,
            resource_types: Vec::new(),
            exclude_resource_types: Vec::new(),
            resume_from: None,
            command: "compile".into(),
            select: Some("+stg_customers".into()),
            exclude: None,
            vars: BTreeMap::from([("env_label".into(), serde_json::json!("dev"))]),
            target: Some("dev".into()),
            full_refresh: true,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::new(),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
            retry_from: None,
            state_manifest_ref: None,
        };
        let env = input.base_env();
        let underscore = env
            .get("_")
            .ok_or_else(|| anyhow::anyhow!("`_` must be set"))?;
        let parsed: DbtRunInput = serde_json::from_str(underscore)?;
        assert_eq!(parsed.command, "compile");
        assert_eq!(parsed.project.as_deref(), Some("waffle"));
        assert_eq!(parsed.select.as_deref(), Some("+stg_customers"));
        assert!(parsed.full_refresh);
        Ok(())
    }

    #[test]
    fn effective_env_overrides_caller_supplied_underscore() -> anyhow::Result<()> {
        // The whole point of `_` is "the payload that started this workflow",
        // so an explicit `_` in input.env must be replaced, not preserved.
        let input = DbtRunInput {
            project: None,
            indirect_selection: None,
            resource_types: Vec::new(),
            exclude_resource_types: Vec::new(),
            resume_from: None,
            command: "run".into(),
            select: None,
            exclude: None,
            vars: BTreeMap::new(),
            target: None,
            full_refresh: false,
            fail_fast: false,
            hooks: None,
            env: BTreeMap::from([("_".into(), "garbage caller value".into())]),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
            retry_from: None,
            state_manifest_ref: None,
        };
        let env = input.base_env();
        let underscore = env
            .get("_")
            .ok_or_else(|| anyhow::anyhow!("`_` must be set"))?;
        assert_ne!(underscore, "garbage caller value");
        assert!(underscore.contains("\"command\":\"run\""));
        Ok(())
    }
}
