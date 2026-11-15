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
//!
//! Creation is single-flight per schema. A caller that finds the schema being
//! created by another activity waits for that attempt instead of skipping
//! ahead: skipping would let it materialize into a schema whose `CREATE` is
//! still in flight, and on the first run into a fresh schema that is a
//! "schema does not exist" failure (permanent on Postgres). Callers run on
//! dbt's blocking thread pool, one activity per thread, so waiting on a
//! condition variable costs nothing async code depends on. Only callers
//! sharing a schema wait on each other; the map-wide lock is never held across
//! the warehouse round trip.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// How many (invocation, adapter, database, schema) entries to remember before
/// starting over.
///
/// The map is bounded by how many runs a worker serves, which is unbounded over
/// its lifetime, so it is swept rather than grown. A sweep keeps every entry
/// some caller is still using (in flight, or being waited on) and drops the
/// rest; dropping a settled entry costs one redundant
/// `CREATE SCHEMA IF NOT EXISTS` for a schema still in use.
const MAX_REMEMBERED: usize = 4096;

type Key = (String, dbt_adapter::AdapterType, String, String);

/// Where one schema's creation stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// No attempt is running and none has settled the schema: the next caller
    /// creates it.
    Vacant,
    /// One caller is issuing the statement; everyone else waits.
    InFlight,
    /// Created, or failed in a way retrying will not change. Never attempted
    /// again this run.
    Settled,
}

/// Shared by every caller targeting one schema. Callers hold it by `Arc`, so a
/// sweep of the map cannot strand a waiter on an orphaned entry.
#[derive(Debug)]
struct Entry {
    phase: Mutex<Phase>,
    changed: Condvar,
}

/// Target schemas created, or being created, during a given run.
#[derive(Debug, Default)]
pub struct CreatedSchemas {
    entries: Mutex<HashMap<Key, Arc<Entry>>>,
}

/// Ends the creator's attempt on every exit path, including a panic in the
/// creation closure, so waiters are always woken and the key is never left
/// `InFlight`.
struct Flight<'a> {
    entry: &'a Entry,
    settled: bool,
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        // Recovering from poisoning: the phase is a plain enum that is always
        // valid, and not waking waiters would hang them.
        let mut phase = self
            .entry
            .phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *phase = if self.settled {
            Phase::Settled
        } else {
            Phase::Vacant
        };
        drop(phase);
        self.entry.changed.notify_all();
    }
}

impl CreatedSchemas {
    /// Make sure this schema has been created during this run, running
    /// `create` if nobody has.
    ///
    /// The first caller for a schema runs `create`. Callers that arrive while
    /// it runs block until it finishes:
    ///
    /// - `Ok(())` settles the schema for the rest of the run — it was created,
    ///   or failed in a way that retrying will not fix and the caller chose to
    ///   tolerate (a role without CREATE on an existing schema). Waiters return
    ///   `Ok(())` and nobody issues the statement again.
    /// - `Err(_)` means the attempt did not settle the schema (a transient
    ///   warehouse failure). The creator gets the error, the schema is
    ///   forgotten, and one blocked waiter becomes the creator and runs its own
    ///   `create`. A waiter never returns `Ok(())` for a schema nobody
    ///   created.
    ///
    /// A panic in `create` counts as `Err(_)` for the waiters. If a lock is
    /// poisoned, `create` simply runs: creating twice is harmless, skipping is
    /// not.
    pub fn ensure<E>(
        &self,
        invocation_id: &str,
        adapter: dbt_adapter::AdapterType,
        database: &str,
        schema: &str,
        create: impl FnOnce() -> Result<(), E>,
    ) -> Result<(), E> {
        let key = (invocation_id.to_string(), adapter, database.to_string(), schema.to_string());
        let Some(entry) = self.entry_for(key) else {
            return create();
        };

        let Ok(mut phase) = entry.phase.lock() else {
            return create();
        };
        // Loop because a woken waiter may find the creator failed (`Vacant`) and
        // another waiter already took over (`InFlight` again).
        loop {
            match *phase {
                Phase::Settled => return Ok(()),
                Phase::Vacant => break,
                Phase::InFlight => {
                    let Ok(woken) = entry.changed.wait(phase) else {
                        return create();
                    };
                    phase = woken;
                }
            }
        }
        *phase = Phase::InFlight;
        drop(phase);

        let mut flight = Flight {
            entry: &entry,
            settled: false,
        };
        let outcome = create();
        flight.settled = outcome.is_ok();
        outcome
    }

    /// The shared entry for `key`, or `None` if the map lock is poisoned.
    fn entry_for(&self, key: Key) -> Option<Arc<Entry>> {
        let mut entries = self.entries.lock().ok()?;
        if entries.len() >= MAX_REMEMBERED && !entries.contains_key(&key) {
            // Callers clone the `Arc` while holding this lock, so a count above
            // one means someone is mid-`ensure` on the entry.
            entries.retain(|_, entry| Arc::strong_count(entry) > 1);
        }
        let entry = entries.entry(key).or_insert_with(|| {
            Arc::new(Entry {
                phase: Mutex::new(Phase::Vacant),
                changed: Condvar::new(),
            })
        });
        let entry = Arc::clone(entry);
        drop(entries);
        Some(entry)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
