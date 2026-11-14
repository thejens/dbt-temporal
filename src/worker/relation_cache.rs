//! What a project's naming macros (`generate_schema_name`,
//! `generate_database_name`) resolved to, remembered per run.
//!
//! When a run's env overrides move the profile schema or database, each node
//! activity re-renders the naming macros for itself and for every ancestor its
//! SQL can mention. Siblings share most of those ancestors, so a 300-node run
//! rendered the same macro for the same node thousands of times. Within one run
//! the answer cannot change — the macros see only the node and the run's
//! overrides — so it is computed once and reused.
//!
//! Also holds where the macros are defined, which is a property of the loaded
//! project and so is looked up once per worker rather than once per call.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use crate::worker::single_flight::SingleFlightCache;

/// How many runs' resolved names to keep.
///
/// A worker serves an unbounded number of runs, so the oldest-used run's names
/// are dropped past this. A run still executing that falls out only loses
/// reuse: its next node recomputes what it needs. Within a run the map holds at
/// most one small entry per project node.
const MAX_REMEMBERED_RUNS: usize = 64;

/// Where one node's relation lives for this workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRelation {
    pub database: String,
    pub schema: String,
}

/// Everything a naming macro's answer can depend on besides the node.
///
/// The invocation id names the run. The rest is what that run's activities
/// render with, kept in full so two runs that happen to share an id (a resumed
/// run with different overrides) can never read each other's answers. Env
/// values can be credentials, so [`Debug`](std::fmt::Debug) prints keys only.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RunScope {
    invocation_id: String,
    target: Option<String>,
    env: BTreeMap<String, String>,
    /// `--vars`, serialized: `serde_json::Value` is not `Hash`, and a macro can
    /// read any of them through `var()`.
    vars: String,
    env_schema: Option<String>,
    env_database: Option<String>,
}

impl RunScope {
    pub fn new(
        invocation_id: &str,
        target: Option<&str>,
        env: &BTreeMap<String, String>,
        vars: &BTreeMap<String, serde_json::Value>,
        env_schema: Option<&str>,
        env_database: Option<&str>,
    ) -> Self {
        Self {
            invocation_id: invocation_id.to_string(),
            target: target.map(str::to_string),
            env: env.clone(),
            // Serialization of a string-keyed map cannot fail; were it to, an
            // empty string only makes the scope coarser, never wrong.
            vars: serde_json::to_string(vars).unwrap_or_default(),
            env_schema: env_schema.map(str::to_string),
            env_database: env_database.map(str::to_string),
        }
    }
}

impl std::fmt::Debug for RunScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunScope")
            .field("invocation_id", &self.invocation_id)
            .field("target", &self.target)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// The relations one run has resolved so far, by node unique id.
#[derive(Debug, Default)]
pub struct RunRelations {
    resolved: Mutex<HashMap<String, NodeRelation>>,
}

impl RunRelations {
    /// This node's relation, computed by `compute` unless the run has already
    /// resolved it. Failures are not remembered.
    ///
    /// The lock is not held while computing — rendering a macro can be slow and
    /// must not serialize the run's other nodes. Two nodes racing on the same
    /// ancestor may both compute it; they get the same answer, and the first
    /// stored wins.
    pub fn resolve<E>(
        &self,
        unique_id: &str,
        compute: impl FnOnce() -> Result<NodeRelation, E>,
    ) -> Result<NodeRelation, E> {
        if let Some(known) = self.lock().get(unique_id) {
            return Ok(known.clone());
        }
        let computed = compute()?;
        Ok(self
            .lock()
            .entry(unique_id.to_string())
            .or_insert(computed)
            .clone())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, NodeRelation>> {
        // A plain map of finished answers is consistent between statements, so
        // a poisoned lock is safe to keep using.
        self.resolved.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Which naming macro a template belongs to.
#[derive(Debug, Clone, Copy)]
pub enum NamingMacro {
    Schema,
    Database,
}

/// Naming-macro state shared by every activity on a worker.
pub struct NamingCache {
    runs: SingleFlightCache<RunScope, RunRelations>,
    schema_template: OnceLock<Option<String>>,
    database_template: OnceLock<Option<String>>,
}

impl Default for NamingCache {
    fn default() -> Self {
        Self {
            runs: SingleFlightCache::new(MAX_REMEMBERED_RUNS),
            schema_template: OnceLock::new(),
            database_template: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for NamingCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamingCache").finish_non_exhaustive()
    }
}

impl NamingCache {
    /// The record of what `scope`'s run has resolved.
    pub fn run(&self, scope: RunScope) -> Arc<RunRelations> {
        // The builder cannot fail; the only error a caller can see is a waiter
        // inheriting a panicked build. Recomputing in a private record is
        // always correct, just unshared.
        self.runs
            .get_or_build(scope, || Ok(RunRelations::default()))
            .unwrap_or_default()
    }

    /// The template defining `which`, searched for by `find` the first time and
    /// remembered after.
    ///
    /// The loaded project's templates do not change while the worker lives, so
    /// the answer — including "no template defines it" — is the same for every
    /// activity.
    pub fn template(
        &self,
        which: NamingMacro,
        find: impl FnOnce() -> Option<String>,
    ) -> Option<&str> {
        let slot = match which {
            NamingMacro::Schema => &self.schema_template,
            NamingMacro::Database => &self.database_template,
        };
        slot.get_or_init(find).as_deref()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn relation(database: &str, schema: &str) -> NodeRelation {
        NodeRelation {
            database: database.to_string(),
            schema: schema.to_string(),
        }
    }

    fn scope(invocation_id: &str, env: &[(&str, &str)]) -> RunScope {
        let env = env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        RunScope::new(invocation_id, None, &env, &BTreeMap::new(), Some("wf"), None)
    }

    #[test]
    fn the_second_lookup_does_not_compute() {
        let run = RunRelations::default();
        let calls = Cell::new(0);
        let compute = || -> Result<NodeRelation, String> {
            calls.set(calls.get() + 1);
            Ok(relation("db", "wf_marts"))
        };
        let first = run.resolve("model.p.a", compute).unwrap();
        let second = run.resolve("model.p.a", compute).unwrap();
        assert_eq!(first, second);
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn nodes_are_remembered_independently() {
        let run = RunRelations::default();
        let a = run
            .resolve("model.p.a", || Ok::<_, String>(relation("db", "a")))
            .unwrap();
        let b = run
            .resolve("model.p.b", || Ok::<_, String>(relation("db", "b")))
            .unwrap();
        assert_eq!((a.schema.as_str(), b.schema.as_str()), ("a", "b"));
    }

    #[test]
    fn a_failure_is_not_remembered() {
        let run = RunRelations::default();
        let err = run
            .resolve("model.p.a", || Err::<NodeRelation, _>("macro blew up".to_string()))
            .unwrap_err();
        assert_eq!(err, "macro blew up");
        let retried = run
            .resolve("model.p.a", || Ok::<_, String>(relation("db", "ok")))
            .unwrap();
        assert_eq!(retried.schema, "ok");
    }

    #[test]
    fn a_run_is_shared_by_its_scope_and_kept_apart_from_others() {
        let cache = NamingCache::default();
        let same_a = cache.run(scope("inv-1", &[("TENANT", "acme")]));
        let same_b = cache.run(scope("inv-1", &[("TENANT", "acme")]));
        assert!(Arc::ptr_eq(&same_a, &same_b));

        assert!(!Arc::ptr_eq(&same_a, &cache.run(scope("inv-2", &[("TENANT", "acme")]))));
        assert!(
            !Arc::ptr_eq(&same_a, &cache.run(scope("inv-1", &[("TENANT", "globex")]))),
            "a different override under the same invocation id is a different run"
        );
    }

    /// Past the cap the least recently used run's names are dropped, and the
    /// run in use survives.
    #[test]
    fn remembered_runs_are_bounded() {
        let cache = NamingCache::default();
        let live = cache.run(scope("live", &[]));
        for i in 0..MAX_REMEMBERED_RUNS * 2 {
            cache.run(scope(&format!("inv-{i}"), &[]));
            // The live run keeps being used, as a running workflow's nodes do.
            assert!(Arc::ptr_eq(&live, &cache.run(scope("live", &[]))));
        }
        assert!(!Arc::ptr_eq(&live, &cache.run(scope("inv-0", &[]))));
    }

    #[test]
    fn the_template_is_looked_up_once() {
        let cache = NamingCache::default();
        let searches = Cell::new(0);
        let find = || {
            searches.set(searches.get() + 1);
            Some("proj.generate_schema_name".to_string())
        };
        assert_eq!(cache.template(NamingMacro::Schema, find), Some("proj.generate_schema_name"));
        assert_eq!(cache.template(NamingMacro::Schema, find), Some("proj.generate_schema_name"));
        assert_eq!(searches.get(), 1);
    }

    /// "No template defines it" is an answer too; searching again cannot change
    /// it, and the two macros are looked up independently.
    #[test]
    fn a_missing_template_is_remembered_per_macro() {
        let cache = NamingCache::default();
        assert_eq!(cache.template(NamingMacro::Database, || None), None);
        assert_eq!(cache.template(NamingMacro::Database, || panic!("already searched")), None);
        assert_eq!(
            cache.template(NamingMacro::Schema, || Some("p.generate_schema_name".to_string())),
            Some("p.generate_schema_name")
        );
    }

    #[test]
    fn run_scope_debug_never_prints_env_values() {
        let rendered = format!("{:?}", scope("inv-1", &[("DB_PASSWORD", "hunter2-secret")]));
        assert!(rendered.contains("DB_PASSWORD"), "{rendered}");
        assert!(!rendered.contains("hunter2-secret"), "{rendered}");
    }
}
