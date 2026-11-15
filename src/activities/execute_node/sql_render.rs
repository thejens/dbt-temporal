//! Rendering a node's SQL into its Jinja context: the raw text, ephemeral CTEs,
//! the workflow's relation rewrite, and the compiled file the materialization
//! reads back.

use std::collections::BTreeMap;

use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::schemas::telemetry::NodeType;
use tracing::info;

use crate::error::DbtTemporalError;

use super::super::node_helpers::{EphemeralDirs, inject_ephemeral_ctes};
use super::context::NodeRun;
use super::sql_rewrite::{RelationRewrite, rewrite_relations};
use super::unit_test;
use super::workspace::compiled_sql_path;

/// What turning a node's source text into the SQL a materialization receives
/// reads besides the node context.
pub(super) struct SqlRendering<'a> {
    pub(super) jinja_env: &'a JinjaEnv,
    pub(super) dirs: EphemeralDirs<'a>,
    pub(super) relation_rewrite: &'a RelationRewrite,
    pub(super) io_args: &'a dbt_common::io_args::IoArgs,
}

impl SqlRendering<'_> {
    /// Assemble a unit test's SQL from the tested model and its fixtures.
    pub(super) fn inject_unit_test_sql(
        &self,
        run: &NodeRun<'_>,
        node_context: &mut BTreeMap<String, minijinja::Value>,
    ) -> Result<(), anyhow::Error> {
        let state = run.state;
        let unit = state
            .resolver_state
            .nodes
            .unit_tests
            .get(run.unique_id)
            .ok_or_else(|| {
                DbtTemporalError::ProjectNotFound(format!("unit test not found: {}", run.unique_id))
            })?;
        let sql = unit_test::build_unit_test_sql(
            state,
            unit,
            &state.resolver_state.nodes,
            self.jinja_env,
            node_context,
            self.dirs,
        )?;
        let sql = rewrite_relations(sql, self.relation_rewrite, run.base.adapter);
        // minijinja Values share their string via Arc — clone the Value, not the SQL.
        let sql_value = minijinja::Value::from(sql);
        node_context.insert("sql".to_owned(), sql_value.clone());
        node_context.insert("compiled_code".to_owned(), sql_value);
        Ok(())
    }

    /// Render the node's raw SQL and record the result in the context as `sql`
    /// and `compiled_code`.
    pub(super) fn render_raw_sql(
        &self,
        run: &NodeRun<'_>,
        raw_sql_result: Result<String, (std::path::PathBuf, std::io::Error)>,
        node_context: &mut BTreeMap<String, minijinja::Value>,
    ) -> Result<(), anyhow::Error> {
        let rt = run.resource_type;
        let unique_id = run.unique_id;
        let state = run.state;
        match raw_sql_result {
            Ok(_) if rt == NodeType::UnitTest || run.measures_freshness => {
                // Unit test SQL is assembled above; freshness nodes have no
                // runnable SQL of their own (their queries come from macros).
            }
            Ok(raw_sql) if !raw_sql.trim().is_empty() => {
                // Use the model's original file path as the rendering filename so any
                // Jinja error references the source file rather than `<unknown>`.
                let render_filename = state.io_args.in_dir.join(&run.common.original_file_path);
                let compiled = dbt_jinja_utils::utils::render_sql_with_listeners(
                    &raw_sql,
                    self.jinja_env,
                    node_context,
                    &[],
                    &[],
                    &render_filename,
                    None,
                )
                .map_err(|e| {
                    // Rendering is not only templating: a model body can call
                    // `run_query`, `adapter.get_relation` or another introspection
                    // macro, so a warehouse that is briefly unreachable fails
                    // *here*. Treating every render failure as permanent put those
                    // outside the retry contract entirely — the same distinction
                    // the project-hook path already makes.
                    //
                    // `FsResult<T> = Result<T, Box<FsError>>`, so the deref keeps
                    // the concrete type the classifier downcasts for.
                    crate::error::classify_adapter_execution_error(
                        &*e,
                        &format!("compiling SQL for {unique_id}"),
                    )
                })?;
                // Inject ephemeral model CTEs (ref('ephemeral_model') → __dbt__cte__<name>).
                let compiled = inject_ephemeral_ctes(
                    &compiled,
                    &run.common.name,
                    &run.base.depends_on.nodes,
                    &state.resolver_state.nodes,
                    self.jinja_env,
                    self.dirs,
                )?;
                // Resolve the relations the startup manifest baked into this text —
                // the upstream `ref()`s that still name startup schemas.
                let compiled = rewrite_relations(compiled, self.relation_rewrite, run.base.adapter);

                // Write compiled SQL to the temp dir so model.compiled_code / model.compiled_sql
                // resolve correctly when accessed by the materialization template.
                let dest = compiled_sql_path(run.node, &self.io_args.in_dir, &self.io_args.out_dir);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        DbtTemporalError::Configuration(format!(
                            "creating compiled SQL directory {}: {e:#}",
                            parent.display()
                        ))
                    })?;
                }
                std::fs::write(&dest, &compiled).map_err(|e| {
                    DbtTemporalError::Configuration(format!(
                        "writing compiled SQL for {unique_id} to {}: {e:#}",
                        dest.display()
                    ))
                })?;

                // Set both `sql` and `compiled_code` in the context. View materializations
                // reference `sql`, while table/incremental materializations reference
                // `compiled_code` (passed to create_table_as / bq_create_table_as).
                // minijinja Values share their string via Arc — clone the Value, not the SQL.
                let compiled_value = minijinja::Value::from(compiled);
                node_context.insert("sql".to_owned(), compiled_value.clone());
                node_context.insert("compiled_code".to_owned(), compiled_value);
            }
            Ok(_) => {
                empty_raw_sql_dispatch(rt, unique_id)?;
                info!(node = %unique_id, "raw SQL is empty");
            }
            Err((path, e)) => {
                raw_sql_read_error_dispatch(rt, unique_id, &path, &e)?;
                info!(
                    node = %unique_id,
                    path = %path.display(),
                    error = %e,
                    "failed to read raw SQL file"
                );
            }
        }
        Ok(())
    }
}

/// Decide what to do when raw SQL came back empty. Models, snapshots, and
/// tests *must* have a non-empty body — empty SQL there is a real bug.
/// Hooks and operations are allowed to compile to nothing.
fn empty_raw_sql_dispatch(rt: NodeType, unique_id: &str) -> Result<(), DbtTemporalError> {
    match rt {
        NodeType::Model | NodeType::Snapshot | NodeType::Test => {
            Err(DbtTemporalError::Compilation(format!("raw SQL is empty for {unique_id}")))
        }
        _ => Ok(()),
    }
}

/// Decide what to do when reading raw SQL from disk failed. Same shape as
/// `empty_raw_sql_dispatch` — hard error for nodes that need SQL, soft for
/// hooks/operations.
fn raw_sql_read_error_dispatch(
    rt: NodeType,
    unique_id: &str,
    path: &std::path::Path,
    err: &std::io::Error,
) -> Result<(), DbtTemporalError> {
    match rt {
        NodeType::Model | NodeType::Snapshot | NodeType::Test => {
            Err(DbtTemporalError::Compilation(format!(
                "reading raw SQL for {unique_id} at {}: {err:#}",
                path.display()
            )))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_raw_sql_dispatch_errors_for_writer_types() {
        for rt in [NodeType::Model, NodeType::Snapshot, NodeType::Test] {
            let err = empty_raw_sql_dispatch(rt, "model.x.foo").unwrap_err();
            assert!(matches!(err, DbtTemporalError::Compilation(_)));
            assert!(err.to_string().contains("model.x.foo"));
            assert!(err.to_string().contains("empty"));
        }
    }

    #[test]
    fn empty_raw_sql_dispatch_ok_for_other_types() {
        // Hooks and operations are allowed to compile to nothing.
        for rt in [NodeType::Operation, NodeType::Seed] {
            assert!(empty_raw_sql_dispatch(rt, "x").is_ok());
        }
    }

    #[test]
    fn raw_sql_read_error_dispatch_errors_for_writer_types() {
        let path = std::path::PathBuf::from("/missing/foo.sql");
        let io_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        for rt in [NodeType::Model, NodeType::Snapshot, NodeType::Test] {
            let err = raw_sql_read_error_dispatch(rt, "model.x.foo", &path, &io_err).unwrap_err();
            assert!(matches!(err, DbtTemporalError::Compilation(_)));
            let msg = err.to_string();
            assert!(msg.contains("model.x.foo"));
            assert!(msg.contains("/missing/foo.sql"));
        }
    }

    #[test]
    fn raw_sql_read_error_dispatch_ok_for_other_types() {
        let path = std::path::PathBuf::from("/missing/foo.sql");
        let io_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert!(raw_sql_read_error_dispatch(NodeType::Operation, "x", &path, &io_err).is_ok());
    }

    // --- build_test_kwargs_map ---
}
