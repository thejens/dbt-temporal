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
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    const DUCKDB: dbt_adapter::AdapterType = dbt_adapter::AdapterType::DuckDB;

    fn ensure_counted(created: &CreatedSchemas, inv: &str, schema: &str, runs: &AtomicUsize) {
        let result: Result<(), &str> = created.ensure(inv, DUCKDB, "db", schema, || {
            runs.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn a_settled_schema_is_not_created_again() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        for _ in 0..3 {
            ensure_counted(&created, "inv-1", "analytics", &runs);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    /// A schema dropped between runs has to be created again, so the memory is
    /// per run rather than per worker.
    #[test]
    fn a_later_run_creates_the_schema_again() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        ensure_counted(&created, "inv-1", "analytics", &runs);
        ensure_counted(&created, "inv-2", "analytics", &runs);
        ensure_counted(&created, "inv-1", "analytics", &runs);
        ensure_counted(&created, "inv-2", "analytics", &runs);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn database_and_schema_are_both_part_of_the_identity() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let ensure = |database: &str, schema: &str| {
            let result: Result<(), ()> = created.ensure("inv-1", DUCKDB, database, schema, || {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            assert_eq!(result, Ok(()));
        };
        ensure("db", "analytics");
        ensure("other_db", "analytics");
        ensure("db", "staging");
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }

    /// The race this type exists to prevent: the second caller must not get
    /// past `ensure` while the first caller's `CREATE` is still in flight.
    #[test]
    fn a_racing_caller_waits_for_the_creation_in_flight() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let creation_finished = AtomicBool::new(false);
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        // `Receiver` is not `Sync`; the lock lets the creating thread borrow it.
        let release_rx = Mutex::new(release_rx);
        let (second_tx, second_rx) = mpsc::channel::<bool>();

        thread::scope(|scope| {
            scope.spawn(|| {
                let result: Result<(), ()> = created.ensure("inv", DUCKDB, "db", "s", || {
                    runs.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    creation_finished.store(true, Ordering::SeqCst);
                    Ok(())
                });
                assert_eq!(result, Ok(()));
            });
            // The first closure is now running, so the second caller is
            // guaranteed to find the schema in flight.
            started_rx.recv().unwrap();
            scope.spawn(|| {
                ensure_counted(&created, "inv", "s", &runs);
                second_tx
                    .send(creation_finished.load(Ordering::SeqCst))
                    .unwrap();
            });
            // Negative check only: a caller that skipped the wait would have
            // answered by now. Ordering itself is asserted on the flag below.
            assert!(second_rx.recv_timeout(Duration::from_millis(100)).is_err());
            release_tx.send(()).unwrap();
            assert!(second_rx.recv().unwrap(), "returned before creation finished");
        });
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn different_schemas_do_not_block_each_other() {
        let created = CreatedSchemas::default();
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        // `Receiver` is not `Sync`; the lock lets the creating thread borrow it.
        let release_rx = Mutex::new(release_rx);

        thread::scope(|scope| {
            scope.spawn(|| {
                let result: Result<(), ()> = created.ensure("inv", DUCKDB, "db", "slow", || {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    Ok(())
                });
                assert_eq!(result, Ok(()));
            });
            started_rx.recv().unwrap();
            // Would deadlock the test if "fast" waited on "slow".
            let runs = AtomicUsize::new(0);
            ensure_counted(&created, "inv", "fast", &runs);
            assert_eq!(runs.load(Ordering::SeqCst), 1);
            release_tx.send(()).unwrap();
        });
    }

    #[test]
    fn many_callers_share_one_settled_failure() {
        const CALLERS: usize = 8;
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let barrier = Barrier::new(CALLERS);
        thread::scope(|scope| {
            for _ in 0..CALLERS {
                scope.spawn(|| {
                    barrier.wait();
                    // A tolerated, non-retryable failure is reported as settled.
                    let result: Result<(), ()> = created.ensure("inv", DUCKDB, "db", "s", || {
                        runs.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    });
                    assert_eq!(result, Ok(()));
                });
            }
        });
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    /// A transient failure must reach the creator, and a waiter blocked behind
    /// it must create the schema itself rather than assume it exists.
    #[test]
    fn a_waiter_takes_over_after_a_transient_failure() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        // `Receiver` is not `Sync`; the lock lets the creating thread borrow it.
        let release_rx = Mutex::new(release_rx);

        thread::scope(|scope| {
            let creator = scope.spawn(|| {
                created.ensure("inv", DUCKDB, "db", "s", || {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    Err("connection reset")
                })
            });
            started_rx.recv().unwrap();
            let waiter = scope.spawn(|| {
                let result: Result<(), &str> = created.ensure("inv", DUCKDB, "db", "s", || {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                });
                result
            });
            release_tx.send(()).unwrap();
            assert_eq!(creator.join().unwrap(), Err("connection reset"));
            assert_eq!(waiter.join().unwrap(), Ok(()));
        });
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        // The waiter's success settled the schema.
        ensure_counted(&created, "inv", "s", &runs);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn creation_is_attempted_again_after_every_transient_failure() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        for _ in 0..3 {
            let result = created.ensure("inv", DUCKDB, "db", "s", || {
                runs.fetch_add(1, Ordering::SeqCst);
                Err("throttled")
            });
            assert_eq!(result, Err("throttled"));
        }
        assert_eq!(runs.load(Ordering::SeqCst), 3);
        ensure_counted(&created, "inv", "s", &runs);
        ensure_counted(&created, "inv", "s", &runs);
        assert_eq!(runs.load(Ordering::SeqCst), 4);
    }

    /// Sweeping the map must not orphan an entry a caller is working on: the
    /// waiter that arrives after the sweep still has to see the same attempt.
    #[test]
    fn a_sweep_does_not_discard_an_entry_in_flight() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        // `Receiver` is not `Sync`; the lock lets the creating thread borrow it.
        let release_rx = Mutex::new(release_rx);
        let (waiter_tx, waiter_rx) = mpsc::channel::<bool>();
        let creation_finished = AtomicBool::new(false);

        thread::scope(|scope| {
            scope.spawn(|| {
                let result: Result<(), ()> = created.ensure("inv", DUCKDB, "db", "slow", || {
                    runs.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    creation_finished.store(true, Ordering::SeqCst);
                    Ok(())
                });
                assert_eq!(result, Ok(()));
            });
            started_rx.recv().unwrap();

            let filler = AtomicUsize::new(0);
            for n in 0..=MAX_REMEMBERED {
                ensure_counted(&created, "inv", &format!("filler-{n}"), &filler);
            }
            assert!(created.entries.lock().unwrap().len() < MAX_REMEMBERED);

            scope.spawn(|| {
                ensure_counted(&created, "inv", "slow", &runs);
                waiter_tx
                    .send(creation_finished.load(Ordering::SeqCst))
                    .unwrap();
            });
            assert!(waiter_rx.recv_timeout(Duration::from_millis(100)).is_err());
            release_tx.send(()).unwrap();
            assert!(waiter_rx.recv().unwrap(), "returned before creation finished");
        });
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_panicking_creator_does_not_strand_a_waiter() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        // `Receiver` is not `Sync`; the lock lets the creating thread borrow it.
        let release_rx = Mutex::new(release_rx);

        thread::scope(|scope| {
            let creator = scope.spawn(|| {
                let _: Result<(), ()> = created.ensure("inv", DUCKDB, "db", "s", || {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    // Unwinds without invoking the panic hook, keeping test
                    // output clean.
                    std::panic::resume_unwind(Box::new("creator panicked"))
                });
            });
            started_rx.recv().unwrap();
            let waiter = scope.spawn(|| ensure_counted(&created, "inv", "s", &runs));
            release_tx.send(()).unwrap();
            assert!(creator.join().is_err());
            waiter.join().unwrap();
        });
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_poisoned_map_lock_still_creates() {
        let created = CreatedSchemas::default();
        let _ = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _held = created.entries.lock().unwrap();
                    std::panic::resume_unwind(Box::new("poison"));
                })
                .join()
        });
        let runs = AtomicUsize::new(0);
        ensure_counted(&created, "inv", "s", &runs);
        ensure_counted(&created, "inv", "s", &runs);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_poisoned_entry_lock_still_creates() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        ensure_counted(&created, "inv", "s", &runs);
        let entry = {
            let entries = created.entries.lock().unwrap();
            Arc::clone(entries.values().next().unwrap())
        };
        let _ = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _held = entry.phase.lock().unwrap();
                    std::panic::resume_unwind(Box::new("poison"));
                })
                .join()
        });
        ensure_counted(&created, "inv", "s", &runs);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    /// Poisoning hit while parked on the condition variable: the waiter creates
    /// rather than hanging or skipping.
    #[test]
    fn a_waiter_survives_the_entry_lock_being_poisoned_while_it_waits() {
        let created = CreatedSchemas::default();
        let runs = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        // `Receiver` is not `Sync`; the lock lets the creating thread borrow it.
        let release_rx = Mutex::new(release_rx);

        thread::scope(|scope| {
            scope.spawn(|| {
                let _: Result<(), ()> = created.ensure("inv", DUCKDB, "db", "s", || {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    Ok(())
                });
            });
            started_rx.recv().unwrap();
            let entry = Arc::clone(created.entries.lock().unwrap().values().next().unwrap());
            let waiter = scope.spawn(|| ensure_counted(&created, "inv", "s", &runs));
            // Poison while the creator is still in flight, then let it finish;
            // whichever side of the wait the waiter is on, it must come back.
            let _ = scope
                .spawn(move || {
                    let _held = entry.phase.lock().unwrap();
                    std::panic::resume_unwind(Box::new("poison"));
                })
                .join();
            release_tx.send(()).unwrap();
            waiter.join().unwrap();
        });
        assert!(runs.load(Ordering::SeqCst) <= 1);
    }
}
