//! Which target schemas this worker has already created, per run.
//!
//! dbt creates a node's target schema before materializing it. Doing that per
//! node means a project with three hundred models in one schema issues three
//! hundred `CREATE SCHEMA IF NOT EXISTS` statements and the metadata round
//! trips behind them, all but the first of which change nothing.
//!
//! Keyed by invocation, not by process: a schema dropped between runs has to be
//! created again, so a worker-lifetime flag would be wrong. Within one run the
//! schema cannot vanish without the run already being broken.

use std::collections::HashSet;
use std::sync::Mutex;

/// How many (invocation, adapter, database, schema) entries to remember before
/// starting over.
///
/// The set is bounded by how many runs a worker serves, which is unbounded over
/// its lifetime — so it is cleared wholesale rather than grown. Clearing costs
/// one redundant `CREATE SCHEMA IF NOT EXISTS` per schema still in flight.
const MAX_REMEMBERED: usize = 4096;

/// Target schemas already created during a given run.
#[derive(Debug, Default)]
pub struct CreatedSchemas {
    seen: Mutex<HashSet<Key>>,
}

type Key = (String, dbt_adapter::AdapterType, String, String);

impl CreatedSchemas {
    /// Claim the right to create this schema, or `false` if this run already
    /// has.
    ///
    /// Two activities racing here both get `true` and both issue the statement.
    /// That is deliberate: `CREATE SCHEMA IF NOT EXISTS` is idempotent, and the
    /// alternative — holding a lock across the warehouse round trip — would
    /// serialize every node that shares a schema.
    pub fn claim(
        &self,
        invocation_id: &str,
        adapter: dbt_adapter::AdapterType,
        database: &str,
        schema: &str,
    ) -> bool {
        let key = (invocation_id.to_string(), adapter, database.to_string(), schema.to_string());
        let Ok(mut seen) = self.seen.lock() else {
            // A poisoned lock means a previous holder panicked. Creating the
            // schema again is harmless; skipping it is not.
            return true;
        };
        if seen.len() >= MAX_REMEMBERED {
            seen.clear();
        }
        seen.insert(key)
    }

    /// Forget a schema this run failed to create, so the next node that needs
    /// it tries again rather than assuming it is there.
    pub fn release(
        &self,
        invocation_id: &str,
        adapter: dbt_adapter::AdapterType,
        database: &str,
        schema: &str,
    ) {
        if let Ok(mut seen) = self.seen.lock() {
            seen.remove(&(
                invocation_id.to_string(),
                adapter,
                database.to_string(),
                schema.to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUCKDB: dbt_adapter::AdapterType = dbt_adapter::AdapterType::DuckDB;

    #[test]
    fn the_first_claim_wins_and_the_rest_are_skipped() {
        let created = CreatedSchemas::default();
        assert!(created.claim("inv-1", DUCKDB, "db", "analytics"));
        assert!(!created.claim("inv-1", DUCKDB, "db", "analytics"));
        assert!(!created.claim("inv-1", DUCKDB, "db", "analytics"));
    }

    /// A schema dropped between runs has to be created again, so the memory is
    /// per run rather than per worker.
    #[test]
    fn a_later_run_creates_the_schema_again() {
        let created = CreatedSchemas::default();
        assert!(created.claim("inv-1", DUCKDB, "db", "analytics"));
        assert!(created.claim("inv-2", DUCKDB, "db", "analytics"));
    }

    #[test]
    fn database_and_schema_are_both_part_of_the_identity() {
        let created = CreatedSchemas::default();
        assert!(created.claim("inv-1", DUCKDB, "db", "analytics"));
        assert!(created.claim("inv-1", DUCKDB, "other_db", "analytics"));
        assert!(created.claim("inv-1", DUCKDB, "db", "staging"));
    }

    /// A failed creation must not leave the run believing the schema is there.
    #[test]
    fn releasing_a_failed_claim_lets_the_next_node_retry() {
        let created = CreatedSchemas::default();
        assert!(created.claim("inv-1", DUCKDB, "db", "analytics"));
        created.release("inv-1", DUCKDB, "db", "analytics");
        assert!(created.claim("inv-1", DUCKDB, "db", "analytics"));
    }
}
