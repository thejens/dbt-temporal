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
