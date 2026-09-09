//! Reading the project-check index: an in-process DuckDB over the index parquet.
//!
//! The index is parquet on disk, not a warehouse relation, so a check never
//! touches the project's own adapter — it runs against a throwaway in-memory
//! DuckDB with the index files registered as views. That keeps a parse-time
//! quality gate off the warehouse entirely: no credentials, no connection, and
//! no cost on the project's target.
//!
//! Two schemas, and the split is a correctness boundary rather than tidiness.
//! The index's own tables register into `dbt_internal`; the parse-safe views
//! over them into `dbt`, which is the vocabulary `info_schema()` renders. A
//! check that reaches past the views into a raw table would read columns that
//! stay empty until compile — and an empty column is not an error, it is zero
//! rows, which a check reports as a pass.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use dbt_adapter::AdapterEngine;
use dbt_adbc::QueryCtx;
use dbt_common::cancellation::CancellationTokenSource;
use dbt_index_core::info_schema::parse_safe::{self, BASE_SCHEMA, VIEW_SCHEMA};
use dbt_schemas::schemas::profiles::{DbConfig, DuckDbConfig};
use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;

/// An open connection to the index, with the parse-safe views registered.
///
/// Holds its own cancellation source: an adapter token carries only a weak
/// reference to one, so dropping the source mid-activity would make every
/// subsequent query report itself cancelled.
pub struct IndexReader {
    engine: Arc<dyn AdapterEngine>,
    connection: Box<dyn dbt_adbc::Connection>,
    cancellation: CancellationTokenSource,
}

impl std::fmt::Debug for IndexReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IndexReader")
    }
}

impl IndexReader {
    /// Open `index_dir` and register everything a check may read.
    pub fn open(index_dir: &Path) -> Result<Self> {
        let engine = duckdb_engine()?;
        let cancellation = CancellationTokenSource::new();
        let connection = engine
            .new_connection(None, None)
            .map_err(|e| anyhow::anyhow!("opening an in-memory duckdb connection: {e}"))?;
        let mut reader = Self {
            engine,
            connection,
            cancellation,
        };
        reader.register_views(index_dir)?;
        Ok(reader)
    }

    /// Run a check's rendered SQL and return the rows it reports.
    pub fn query(&mut self, sql: &str) -> Result<RecordBatch, String> {
        self.execute(sql, "dbt check").map_err(|e| e.to_string())
    }

    fn execute(&mut self, sql: &str, label: &str) -> Result<RecordBatch> {
        let ctx = QueryCtx::new(label);
        self.engine
            .execute(None, self.connection.as_mut(), &ctx, sql, self.cancellation.token())
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Register the index's parquet as `dbt_internal.*`, then the parse-safe
    /// views over them as `dbt.*`.
    ///
    /// A missing parquet is skipped rather than stubbed: a check that needed it
    /// then fails with "table does not exist", which names the problem. An
    /// empty stand-in would make that same check pass having read nothing.
    /// Views whose every table registered are created; one missing a table is
    /// skipped, so an absent `node_columns` cannot take down `dbt.models`.
    fn register_views(&mut self, index_dir: &Path) -> Result<()> {
        for schema in [VIEW_SCHEMA, BASE_SCHEMA] {
            self.execute(&format!("create schema if not exists {schema}"), "dbt check setup")
                .with_context(|| format!("creating schema {schema}"))?;
        }

        let mut registered = std::collections::HashSet::new();
        for table in parse_safe::base_tables() {
            let path = index_dir.join(format!("dbt.{table}.parquet"));
            if !path.exists() {
                continue;
            }
            // Absolute, and single-quote escaped: the index's own `views.sql`
            // emits relative paths, which would resolve against whatever
            // directory the worker happens to be running in.
            let quoted = path.to_string_lossy().replace('\'', "''");
            self.execute(
                &format!(
                    "create or replace view {BASE_SCHEMA}.{table} as \
                     select * from read_parquet('{quoted}')"
                ),
                "dbt check setup",
            )
            .with_context(|| format!("registering {BASE_SCHEMA}.{table}"))?;
            registered.insert(table);
        }

        for view in parse_safe::VIEWS {
            if !view.base_tables().iter().all(|t| registered.contains(t)) {
                continue;
            }
            let sql = view
                .create_view_sql()
                .map_err(|e| anyhow::anyhow!("building the {} view: {e}", view.name))?;
            self.execute(&sql, "dbt check setup")
                .with_context(|| format!("registering {VIEW_SCHEMA}.{}", view.name))?;
        }
        Ok(())
    }
}

/// A real (not mock) in-memory DuckDB engine.
///
/// Built from a hand-rolled config rather than the project's profile: the
/// project's adapter points at its warehouse, and the index is local parquet.
fn duckdb_engine() -> Result<Arc<dyn AdapterEngine>> {
    let config = DuckDbConfig {
        path: Some(":memory:".to_string()),
        schema: Some("main".to_string()),
        ..Default::default()
    };
    crate::worker::adapter::build_adapter_engine(
        &DbConfig::DuckDB(Box::new(config)),
        DEFAULT_RESOLVED_QUOTING,
        // The check index is a local DuckDB over the project's own metadata —
        // no warehouse to comment on, no project behaviour to honour.
        &crate::worker::adapter::AdapterSettings::default(),
        None,
    )
    .context("building the duckdb engine for the project-check index")
}
