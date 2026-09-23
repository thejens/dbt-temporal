//! Reading the project's metadata the way a check sees it: an in-process DuckDB
//! with dbt's parse-safe views over the parse epochs.
//!
//! The metadata is parquet on disk, not a warehouse relation, so a check never
//! touches the project's own adapter — it runs against a throwaway in-memory
//! DuckDB. That keeps a parse-time quality gate off the warehouse entirely: no
//! credentials, no connection, and no cost on the project's target.
//!
//! The views come from dbt itself (`parse_safe_statements`), the same DDL dbt's
//! own check gate executes, and they read the epoch files directly. Only
//! parse-safe relations and columns are published into `dbt`: a column that
//! stays empty until compile is absent rather than NULL, because an empty
//! column is not an error, it is zero rows, which a check reports as a pass.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use dbt_adapter::AdapterEngine;
use dbt_adbc::QueryCtx;
use dbt_common::cancellation::CancellationTokenSource;
use dbt_index_core::info_schema::epoch::EPOCH_RELATIONS;
use dbt_index_core::info_schema::epoch_views::parse_safe_statements;
use dbt_schemas::schemas::profiles::{DbConfig, DuckDbConfig};
use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;

/// An open connection with the parse-safe views registered.
///
/// Holds its own cancellation source: an adapter token carries only a weak
/// reference to one, so dropping the source mid-activity would make every
/// subsequent query report itself cancelled.
pub struct MetadataReader {
    engine: Arc<dyn AdapterEngine>,
    connection: Box<dyn dbt_adbc::Connection>,
    cancellation: CancellationTokenSource,
}

impl std::fmt::Debug for MetadataReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MetadataReader")
    }
}

impl MetadataReader {
    /// Register everything a check may read over the epochs in `metadata_dir`.
    ///
    /// Refuses before opening a connection when there are no epochs at all. The
    /// generator skips a relation whose files are absent, so over an empty
    /// directory every check would fail with "table does not exist" one by one
    /// — or, worse, one that reads nothing would pass. Failing the whole gate
    /// names the actual problem.
    ///
    /// A statement that fails to register is also an error, as it is in dbt:
    /// a check missing its view would otherwise report on a surface that is
    /// not there.
    pub fn open(metadata_dir: &Path) -> Result<Self> {
        anyhow::ensure!(
            EPOCH_RELATIONS.iter().any(|r| r.has_files(metadata_dir)),
            "no project metadata at {}",
            metadata_dir.display()
        );
        let statements = parse_safe_statements(metadata_dir).map_err(|e| {
            anyhow::anyhow!("reading project metadata at {}: {e}", metadata_dir.display())
        })?;

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
        for statement in &statements {
            reader
                .execute(statement, "dbt check setup")
                .context("registering the check views")?;
        }
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
}

/// A real (not mock) in-memory DuckDB engine.
///
/// Built from a hand-rolled config rather than the project's profile: the
/// project's adapter points at its warehouse, and the metadata is local parquet.
fn duckdb_engine() -> Result<Arc<dyn AdapterEngine>> {
    let config = DuckDbConfig {
        path: Some(":memory:".to_string()),
        schema: Some("main".to_string()),
        ..Default::default()
    };
    crate::worker::adapter::build_adapter_engine(
        &DbConfig::DuckDB(Box::new(config)),
        DEFAULT_RESOLVED_QUOTING,
        // A local DuckDB over the project's own metadata —
        // no warehouse to comment on, no project behaviour to honour.
        &crate::worker::adapter::AdapterSettings::default(),
        None,
    )
    .context("building the duckdb engine for project checks")
}
