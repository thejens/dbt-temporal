//! Inlining ephemeral models as CTEs into the SQL of the nodes that reference
//! them.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;

use dbt_common::constants::DBT_CTE_PREFIX;
use dbt_jinja_utils::utils::{inject_and_persist_ephemeral_models, render_sql_with_listeners};
use minijinja::MacroSpans;
use minijinja::listener::RenderingEventListener;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

/// Empty listener slice for SQL renders that don't subscribe to events.
/// Threaded through `render_sql_with_listeners` to keep call sites uniform.
const NO_LISTENERS: &[Rc<dyn RenderingEventListener>] = &[];

/// Inject ephemeral model CTEs into compiled SQL.
///
/// Walks the user's compiled SQL for `__dbt__cte__<name>` references, compiles
/// each transitive ephemeral dependency through Jinja, and persists per-ephemeral
/// CTE chains to `ephemeral_dir`. The final wrap is delegated to
/// `dbt_jinja_utils::utils::inject_and_persist_ephemeral_models`, matching
/// vanilla dbt-fusion's compile output.
///
/// That helper picks one of two shapes. SQL that already opens with `WITH` gets
/// the ephemeral CTEs spliced into its existing chain as siblings; everything
/// else is wrapped in `select * from (...)` and marked with
/// `--EPHEMERAL-SELECT-WRAPPER-START/END`, which dbt-fusion's adapter SQL
/// tokenizer / diff (`crates/dbt-adapter/src/sql/{tokenizer,diff}.rs`) read to
/// reason about ephemeral wrapping. Do not assume the markers are always
/// present.
///
/// `ephemeral_dir` must be unique per-activity; the persist step writes
/// `<name>.sql` files there, so concurrent activities sharing a directory would
/// race on the cumulative-CTE-chain layout.
pub fn inject_ephemeral_ctes(
    compiled_sql: &str,
    user_node_name: &str,
    depends_on: &[String],
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    dirs: EphemeralDirs<'_>,
) -> Result<String, DbtTemporalError> {
    if !compiled_sql.contains(DBT_CTE_PREFIX) {
        return Ok(compiled_sql.to_string());
    }

    // Walk the dependency DAG of ephemerals, persisting each leaf-first via
    // dbt-fusion's `inject_and_persist_ephemeral_models`. Each call writes a
    // cumulative CTE chain for that ephemeral into `ephemeral_dir`, so the
    // final user-model call only has to read one file per direct dep.
    let mut visited = BTreeSet::new();
    persist_ephemeral_chain(compiled_sql, depends_on, nodes, jinja_env, dirs, &mut visited)?;

    let mut spans = MacroSpans::default();
    inject_and_persist_ephemeral_models(
        compiled_sql.to_string(),
        &mut spans,
        user_node_name,
        false, // not an ephemeral — wraps and returns without persisting
        dirs.ephemeral_dir,
    )
    .map_err(|e| {
        DbtTemporalError::Compilation(format!(
            "wrapping ephemeral CTEs for {user_node_name}: {e:#}"
        ))
    })
}

/// Builds the Jinja context an ephemeral model renders with.
///
/// Takes the ephemeral node itself: dbt compiles every ephemeral model as that
/// model, so `this`, `model`, `config(...)` and node-scoped lookups must
/// resolve to the ephemeral's own values rather than those of the model that
/// consumes it.
pub type EphemeralContext<'a> =
    &'a dyn Fn(
        &dyn dbt_schemas::schemas::nodes::InternalDbtNode,
    ) -> Result<BTreeMap<String, minijinja::Value>, DbtTemporalError>;

/// Where the ephemeral walk reads from and writes to, and what it renders with.
///
/// `ephemeral_dir` must be unique per activity: the persist step writes
/// `<name>.sql` files there, so concurrent activities sharing one would race on
/// the cumulative-CTE-chain layout.
#[derive(Clone, Copy)]
pub struct EphemeralDirs<'a> {
    /// Project source root, where an ephemeral's raw SQL is read from.
    pub in_dir: &'a Path,
    /// The activity's private scratch directory for persisted CTE chains.
    pub ephemeral_dir: &'a Path,
    /// The render context for each ephemeral model.
    pub context_for: EphemeralContext<'a>,
}

/// An [`EphemeralContext`] that builds each ephemeral's run-phase node context
/// from the same base context, adapter and scratch IO the consuming node uses.
///
/// Only the per-node overlay differs between models, so the ephemeral gets its
/// own `this`, `model`, `config` and package scope while sharing everything
/// that is the same for the whole activity. The result store the context carries
/// is dropped: an ephemeral is never executed, so nothing reads it back.
pub fn ephemeral_context_builder<'a>(
    state: &'a WorkerState,
    base_context: &'a BTreeMap<String, minijinja::Value>,
    adapter_type: dbt_adapter::AdapterType,
    io_args: &'a dbt_common::io_args::IoArgs,
) -> impl Fn(
    &dyn dbt_schemas::schemas::nodes::InternalDbtNode,
) -> Result<BTreeMap<String, minijinja::Value>, DbtTemporalError>
+ 'a {
    move |node| {
        let common = node.common();
        let resource_type = node.resource_type();
        let config = crate::activities::node_serialization::get_node_config_yml(
            &state.resolver_state.nodes,
            &common.unique_id,
            resource_type,
        )
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "reading the config of ephemeral model {}: {e:#}",
                common.unique_id
            ))
        })?;
        let sql_header = crate::activities::node_serialization::get_sql_header(
            &state.resolver_state.nodes,
            &common.unique_id,
            resource_type,
        );
        let (context, _result_store) = dbt_jinja_utils::phases::run::build_run_node_context(
            node,
            &config,
            adapter_type,
            None,
            base_context,
            io_args,
            dbt_telemetry::ExecutionPhase::Run,
            sql_header,
            state.packages.clone(),
        );
        Ok(context)
    }
}

/// The ephemeral models among a node's resolved dependencies, indexed by the
/// name their CTE carries.
///
/// Two dependencies with the same name would produce two `__dbt__cte__<name>`
/// markers that no reader can tell apart, so that is refused rather than
/// silently resolved to one of them.
fn ephemeral_dependencies<'a>(
    depends_on: &[String],
    nodes: &'a dbt_schemas::schemas::Nodes,
) -> Result<
    BTreeMap<&'a str, (&'a str, &'a dyn dbt_schemas::schemas::nodes::InternalDbtNode)>,
    DbtTemporalError,
> {
    let mut by_name: BTreeMap<&str, (&str, &dyn dbt_schemas::schemas::nodes::InternalDbtNode)> =
        BTreeMap::new();
    // Sorted and deduplicated so a repeated dependency is not mistaken for a
    // name clash, and so a genuine clash is always reported with the same pair.
    let unique_ids: BTreeSet<&str> = depends_on.iter().map(String::as_str).collect();
    for unique_id in unique_ids {
        let Some(node) = nodes.get_node(unique_id) else {
            continue;
        };
        if !node
            .base()
            .materialized
            .to_string()
            .eq_ignore_ascii_case("ephemeral")
        {
            continue;
        }
        let name = node.common().name.as_str();
        if let Some((existing, _)) = by_name.insert(name, (node.common().unique_id.as_str(), node))
        {
            return Err(DbtTemporalError::Compilation(format!(
                "two ephemeral dependencies are both named '{name}' ({existing} and \
                 {unique_id}); their CTEs cannot be told apart in compiled SQL"
            )));
        }
    }
    Ok(by_name)
}

/// Recursively compile each ephemeral referenced (directly or transitively) by
/// the given SQL, persisting each leaf-first via
/// `inject_and_persist_ephemeral_models(is_current_model_ephemeral=true)`.
///
/// Leaf-first ordering is required: the function reads each direct dep's
/// persisted file when processing a parent ephemeral, so dependencies must
/// already be on disk.
fn persist_ephemeral_chain(
    sql: &str,
    depends_on: &[String],
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    dirs: EphemeralDirs<'_>,
    visited: &mut BTreeSet<String>,
) -> Result<(), DbtTemporalError> {
    let candidates = ephemeral_dependencies(depends_on, nodes)?;

    for name in extract_ephemeral_names(sql) {
        // The CTE name is what the SQL carries, but *which* node it names is
        // decided by the depending node's resolved dependencies — not by
        // scanning the whole project for a model with that name, which picks
        // an arbitrary one when two packages both define `base`.
        let Some(&(unique_id, node)) = candidates.get(name.as_str()) else {
            return Err(DbtTemporalError::Compilation(format!(
                "compiled SQL references ephemeral model '{name}', which is not among the \
                 node's ephemeral dependencies ({})",
                if candidates.is_empty() {
                    "none".to_string()
                } else {
                    candidates.keys().copied().collect::<Vec<_>>().join(", ")
                }
            )));
        };
        if !visited.insert(unique_id.to_string()) {
            continue;
        }

        let raw_path = dirs.in_dir.join(&node.common().original_file_path);
        let raw_sql = std::fs::read_to_string(&raw_path).map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "reading ephemeral model '{name}' at {}: {e:#}",
                raw_path.display()
            ))
        })?;
        let node_context = (dirs.context_for)(node)?;
        let compiled = render_sql_with_listeners(
            &raw_sql,
            jinja_env,
            &node_context,
            NO_LISTENERS,
            &[],
            &raw_path,
            None,
        )
        .map_err(|e| {
            // Same reasoning as the model render path: an ephemeral's body can
            // reach the warehouse through `run_query` or introspection, so a
            // transient failure here has to stay retryable.
            crate::error::classify_adapter_execution_error(
                &*e,
                &format!("compiling ephemeral model '{name}'"),
            )
        })?;

        // Recurse first so this ephemeral's deps land on disk before we persist
        // it — and from *its* dependencies, so a nested ephemeral is resolved
        // against the node that actually references it.
        persist_ephemeral_chain(
            &compiled,
            &node.base().depends_on.nodes,
            nodes,
            jinja_env,
            dirs,
            visited,
        )?;

        let mut spans = MacroSpans::default();
        inject_and_persist_ephemeral_models(
            compiled,
            &mut spans,
            &name,
            true, // ephemeral — persists cumulative CTE chain to disk
            dirs.ephemeral_dir,
        )
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "persisting ephemeral CTE chain for '{name}': {e:#}"
            ))
        })?;
    }
    Ok(())
}

/// Extract ephemeral model names from `__dbt__cte__<name>` references in SQL.
///
/// dbt-fusion has a private equivalent (`extract_ephemeral_model_names` in
/// `dbt-jinja-utils/src/utils.rs`); we keep our own because we need it to walk
/// the dep DAG before each ephemeral is persisted, and the upstream version
/// isn't exported.
fn extract_ephemeral_names(sql: &str) -> Vec<String> {
    #[allow(clippy::expect_used)]
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(&format!(r"{DBT_CTE_PREFIX}(\w+)")).expect("ephemeral CTE name regex")
    });
    RE.captures_iter(sql)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
