//! A fault-injecting `AdapterEngine` wrapper for tests.
//!
//! Wraps a real (embedded DuckDB) engine and delegates every method to it,
//! except it can be told to fail `new_connection` or the next N query
//! executions with a synthetic `AdapterError` — any kind / SQLSTATE / vendor
//! code / message. This drives the real `execute_node` path with deterministic,
//! warehouse-shaped failures (a dropped connection, Snowflake throttling, a
//! BigQuery rate limit, a deadlock) without a live warehouse, and exercises
//! retry-then-recover by failing a bounded number of attempts before passing
//! through to the real engine.

#![allow(
    dead_code,
    missing_debug_implementations,
    clippy::unwrap_used,
    clippy::too_many_arguments
)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use dbt_adapter::AdapterEngine;
use dbt_adapter::AdapterType;
use dbt_adapter::cache::RelationCache;
use dbt_adapter::engine::query_comment::QueryCommentConfig;
use dbt_adapter::engine::{Options, SidecarClient};
use dbt_adapter::sql_types::TypeOps;
use dbt_adapter::stmt_splitter::StmtSplitter;
use dbt_adbc::{Backend, Connection, QueryCtx, Statement};
use dbt_auth::AdapterConfig;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::schemas::common::ResolvedQuoting;
use minijinja::State;

/// A synthetic warehouse error to inject. Mirrors the fields the retry
/// classifier (`error::classify_adapter_execution_error`) inspects.
#[derive(Clone)]
pub struct AdapterFault {
    kind: AdapterErrorKind,
    message: String,
    sqlstate: [u8; 5],
    vendor_code: Option<i32>,
}

impl AdapterFault {
    pub fn new(
        kind: AdapterErrorKind,
        message: &str,
        sqlstate: [u8; 5],
        vendor_code: Option<i32>,
    ) -> Self {
        Self {
            kind,
            message: message.to_string(),
            sqlstate,
            vendor_code,
        }
    }

    /// Postgres/Redshift connection failure (SQLSTATE 08006) — transient.
    pub fn connection_failure() -> Self {
        Self::new(AdapterErrorKind::Driver, "connection to server was lost", *b"08006", None)
    }

    /// Serialization/deadlock (SQLSTATE 40001) — transient.
    pub fn deadlock() -> Self {
        Self::new(AdapterErrorKind::SqlExecution, "deadlock detected", *b"40001", None)
    }

    /// Snowflake service throttling (HTTP 503), no SQLSTATE — transient.
    pub fn snowflake_throttled() -> Self {
        Self::new(
            AdapterErrorKind::Driver,
            "503 Service Unavailable: request was throttled",
            *b"00000",
            None,
        )
    }

    /// BigQuery rate limit, reason string, no SQLSTATE — transient.
    pub fn bigquery_rate_limited() -> Self {
        Self::new(
            AdapterErrorKind::Driver,
            "rateLimitExceeded: Exceeded rate limits for this project",
            *b"00000",
            None,
        )
    }

    /// Azure SQL / SQL Server transient error number (40501, service busy).
    pub fn azure_service_busy() -> Self {
        Self::new(
            AdapterErrorKind::Driver,
            "The service is currently busy",
            *b"00000",
            Some(40501),
        )
    }

    /// A permanent SQL error (undefined table, SQLSTATE 42P01) — must NOT retry.
    pub fn permanent_undefined_table() -> Self {
        Self::new(
            AdapterErrorKind::SqlExecution,
            "relation \"missing\" does not exist",
            *b"42P01",
            None,
        )
    }

    fn to_error(&self) -> AdapterError {
        AdapterError::new_with_sqlstate_and_vendor_code(
            self.kind,
            self.message.clone(),
            self.sqlstate,
            self.vendor_code,
        )
    }
}

#[derive(Default)]
struct Faults {
    connection: Option<AdapterFault>,
    executes: VecDeque<AdapterFault>,
}

/// Handle for configuring faults on a live [`FaultInjectingEngine`]. Cloneable
/// and shared with the engine, so a test can inject faults after the harness is
/// built.
#[derive(Clone, Default)]
pub struct FaultHandle(Arc<Mutex<Faults>>);

impl FaultHandle {
    /// Fail every `new_connection` with `fault` until [`clear`](Self::clear).
    pub fn fail_connections(&self, fault: AdapterFault) {
        self.0.lock().unwrap().connection = Some(fault);
    }

    /// Fail the next `n` query executions with `fault`, then pass through — for
    /// testing retry-then-recover.
    pub fn fail_next_executes(&self, n: usize, fault: &AdapterFault) {
        let mut guard = self.0.lock().unwrap();
        for _ in 0..n {
            guard.executes.push_back(fault.clone());
        }
    }

    /// Clear all pending faults.
    pub fn clear(&self) {
        let mut guard = self.0.lock().unwrap();
        guard.connection = None;
        guard.executes.clear();
    }
}

/// `AdapterEngine` decorator that injects configured faults, delegating
/// everything else to `inner`.
pub struct FaultInjectingEngine {
    inner: Arc<dyn AdapterEngine>,
    faults: FaultHandle,
}

impl FaultInjectingEngine {
    pub fn new(inner: Arc<dyn AdapterEngine>, faults: FaultHandle) -> Self {
        Self { inner, faults }
    }
}

impl AdapterEngine for FaultInjectingEngine {
    fn adapter_type(&self) -> AdapterType {
        self.inner.adapter_type()
    }
    fn backend(&self) -> Backend {
        self.inner.backend()
    }
    fn quoting(&self) -> ResolvedQuoting {
        self.inner.quoting()
    }
    fn splitter(&self) -> &dyn StmtSplitter {
        self.inner.splitter()
    }
    fn type_ops(&self) -> &Arc<dyn TypeOps> {
        self.inner.type_ops()
    }
    fn query_comment(&self) -> &QueryCommentConfig {
        self.inner.query_comment()
    }
    fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.inner.config(key)
    }
    fn get_config(&self) -> &AdapterConfig {
        self.inner.get_config()
    }
    fn relation_cache(&self) -> &Arc<RelationCache> {
        self.inner.relation_cache()
    }
    fn behavior(&self) -> &Arc<Behavior> {
        self.inner.behavior()
    }
    fn behavior_flag_overrides(&self) -> &BTreeMap<String, bool> {
        self.inner.behavior_flag_overrides()
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        let connection_fault = self.faults.0.lock().unwrap().connection.clone();
        if let Some(fault) = connection_fault {
            return Err(fault.to_error());
        }
        self.inner.new_connection(state, node_id)
    }

    fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let connection_fault = self.faults.0.lock().unwrap().connection.clone();
        if let Some(fault) = connection_fault {
            return Err(fault.to_error());
        }
        self.inner.new_connection_with_config(config)
    }

    fn has_query_cache(&self) -> bool {
        self.inner.has_query_cache()
    }
    fn new_query_cache_statement(
        &self,
        stmt: Box<dyn Statement>,
    ) -> AdapterResult<Box<dyn Statement>> {
        self.inner.new_query_cache_statement(stmt)
    }
    fn set_query_cache_reverse_deps(
        &self,
        deps: BTreeMap<String, BTreeSet<String>>,
    ) -> AdapterResult<()> {
        self.inner.set_query_cache_reverse_deps(deps)
    }

    fn execute_with_options(
        &self,
        state: Option<&State>,
        ctx: &QueryCtx,
        conn: &mut dyn Connection,
        sql: &str,
        options: Options,
        fetch: bool,
        token: CancellationToken,
    ) -> AdapterResult<RecordBatch> {
        let execute_fault = self.faults.0.lock().unwrap().executes.pop_front();
        if let Some(fault) = execute_fault {
            return Err(fault.to_error());
        }
        self.inner
            .execute_with_options(state, ctx, conn, sql, options, fetch, token)
    }

    fn threads(&self) -> Option<usize> {
        self.inner.threads()
    }
    fn is_mock(&self) -> bool {
        self.inner.is_mock()
    }
    fn is_sidecar(&self) -> bool {
        self.inner.is_sidecar()
    }
    fn is_replay(&self) -> bool {
        self.inner.is_replay()
    }
    fn generation(&self) -> u64 {
        self.inner.generation()
    }
    fn physical_backend(&self) -> Option<Backend> {
        self.inner.physical_backend()
    }
    fn sidecar_client(&self) -> Option<&dyn SidecarClient> {
        self.inner.sidecar_client()
    }
    fn get_configured_database_name(&self) -> Option<Cow<'_, str>> {
        self.inner.get_configured_database_name()
    }
}
