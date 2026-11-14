//! A bounded, single-flight cache for values that are expensive to build and
//! must be built on a blocking thread.
//!
//! Used for the adapter engines a profile rebuild produces (and for the per-run
//! sets of resolved relation names): every node of a run asks for the same set
//! at once, and building it re-renders `profiles.yml` and
//! constructs one engine per adapter. The first caller for a key builds; the
//! rest block until it finishes and share the result.
//!
//! The waiting is synchronous (`Mutex` + `Condvar`) on purpose. Callers are
//! already on the dbt pool (see `crate::dbt_pool`), where blocking is what the
//! thread is for, and the builder is itself a pool thread that is running
//! before any waiter arrives — so a waiter never holds the thread the builder
//! needs.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// How a build ended, as seen by the callers waiting on it.
enum Outcome<V> {
    Building,
    Ready(Arc<V>),
    /// The builder failed or panicked. Carries the rendered error: `anyhow`
    /// errors are not `Clone`, and every waiter should see why.
    Failed(Arc<str>),
}

/// One in-flight or finished build, shared by everyone asking for its key.
struct Slot<V> {
    outcome: Mutex<Outcome<V>>,
    finished: Condvar,
}

impl<V> Slot<V> {
    const fn building() -> Self {
        Self {
            outcome: Mutex::new(Outcome::Building),
            finished: Condvar::new(),
        }
    }

    fn finish(&self, outcome: Outcome<V>) {
        *lock(&self.outcome) = outcome;
        self.finished.notify_all();
    }

    fn is_ready(&self) -> bool {
        matches!(*lock(&self.outcome), Outcome::Ready(_))
    }

    /// Block until the builder finishes, then report what it produced.
    fn wait(&self) -> anyhow::Result<Arc<V>> {
        let mut outcome = lock(&self.outcome);
        loop {
            match &*outcome {
                Outcome::Ready(value) => return Ok(Arc::clone(value)),
                Outcome::Failed(message) => return Err(anyhow::anyhow!("{message}")),
                Outcome::Building => {}
            }
            outcome = self
                .finished
                .wait(outcome)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

struct Entry<V> {
    slot: Arc<Slot<V>>,
    /// Value of [`State::clock`] at the last lookup; the smallest is the least
    /// recently used.
    last_used: u64,
}

struct State<K, V> {
    entries: HashMap<K, Entry<V>>,
    clock: u64,
}

/// Builds each key's value at most once at a time, keeps the most recently used
/// `capacity` finished values, and never keeps a failure.
pub struct SingleFlightCache<K, V> {
    capacity: usize,
    state: Mutex<State<K, V>>,
}

/// Projects the bound only: the values and keys can hold credentials.
impl<K, V> std::fmt::Debug for SingleFlightCache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleFlightCache")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

/// The lock guards plain bookkeeping that is consistent between statements, so
/// a poisoned lock (a panic elsewhere while holding it) is safe to keep using.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<K: Hash + Eq + Clone, V> SingleFlightCache<K, V> {
    /// A cache keeping at most `capacity` finished values.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State {
                entries: HashMap::new(),
                clock: 0,
            }),
        }
    }

    /// The value for `key`, building it with `build` if no one has.
    ///
    /// Exactly one concurrent caller per key runs `build`; the others block
    /// until it ends. If it fails, the error goes to the callers already
    /// waiting and nothing is cached, so the next caller builds afresh. The same
    /// holds if `build` panics: waiters are released with an error rather than
    /// left blocked.
    pub fn get_or_build(
        &self,
        key: K,
        build: impl FnOnce() -> anyhow::Result<V>,
    ) -> anyhow::Result<Arc<V>> {
        let (slot, builder) = self.claim(&key);
        if !builder {
            return slot.wait();
        }

        // Armed until the build returns, so a panic in `build` still removes the
        // slot and wakes the waiters (see `Drop`).
        let mut guard = BuildGuard {
            cache: self,
            key,
            slot: &slot,
            settled: false,
        };
        match build() {
            Ok(value) => {
                let value = Arc::new(value);
                guard.settled = true;
                slot.finish(Outcome::Ready(Arc::clone(&value)));
                Ok(value)
            }
            Err(error) => {
                guard.fail(format!("{error:#}"));
                Err(error)
            }
        }
    }

    /// The slot for `key`, and whether the caller must build it. A key with no
    /// slot gets a fresh one, evicting to make room.
    fn claim(&self, key: &K) -> (Arc<Slot<V>>, bool) {
        let mut state = lock(&self.state);
        state.clock += 1;
        let now = state.clock;
        let claimed = if let Some(entry) = state.entries.get_mut(key) {
            entry.last_used = now;
            (Arc::clone(&entry.slot), false)
        } else {
            evict_to_fit(&mut state.entries, self.capacity);
            let slot = Arc::new(Slot::building());
            state.entries.insert(
                key.clone(),
                Entry {
                    slot: Arc::clone(&slot),
                    last_used: now,
                },
            );
            (slot, true)
        };
        drop(state);
        claimed
    }

    /// How many entries, finished or in flight, the cache currently holds.
    #[cfg(test)]
    fn len(&self) -> usize {
        lock(&self.state).entries.len()
    }
}

/// Make room for one more entry by dropping the least recently used finished
/// ones. Builds in flight are never evicted — their waiters hold the slot — so
/// the bound can be exceeded only by the number of concurrent builds.
fn evict_to_fit<K: Hash + Eq + Clone, V>(entries: &mut HashMap<K, Entry<V>>, capacity: usize) {
    while entries.len() >= capacity.max(1) {
        let victim = entries
            .iter()
            .filter(|(_, entry)| entry.slot.is_ready())
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone());
        match victim {
            Some(key) => {
                entries.remove(&key);
            }
            None => return,
        }
    }
}

/// Settles a build that did not return normally.
struct BuildGuard<'a, K: Hash + Eq + Clone, V> {
    cache: &'a SingleFlightCache<K, V>,
    key: K,
    slot: &'a Arc<Slot<V>>,
    settled: bool,
}

impl<K: Hash + Eq + Clone, V> BuildGuard<'_, K, V> {
    /// Forget the slot, then release its waiters with `message`.
    ///
    /// The slot leaves the map first so that a caller arriving after the failure
    /// builds afresh instead of inheriting it.
    fn fail(&mut self, message: String) {
        self.settled = true;
        let mut state = lock(&self.cache.state);
        if state
            .entries
            .get(&self.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.slot, self.slot))
        {
            state.entries.remove(&self.key);
        }
        drop(state);
        self.slot.finish(Outcome::Failed(message.into()));
    }
}

impl<K: Hash + Eq + Clone, V> Drop for BuildGuard<'_, K, V> {
    fn drop(&mut self) {
        if !self.settled {
            self.fail("the build panicked".to_string());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn concurrent_callers_for_one_key_share_a_single_build() {
        let cache = SingleFlightCache::<&str, u32>::new(4);
        let builds = AtomicUsize::new(0);
        let results: Vec<Arc<u32>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        cache
                            .get_or_build("run", || {
                                builds.fetch_add(1, Ordering::SeqCst);
                                // Long enough that every other thread has
                                // arrived and is waiting on this build.
                                std::thread::sleep(Duration::from_millis(100));
                                Ok(7)
                            })
                            .unwrap()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(results.iter().all(|r| Arc::ptr_eq(r, &results[0])));
    }

    #[test]
    fn a_finished_value_is_returned_without_building_again() {
        let cache = SingleFlightCache::<&str, u32>::new(4);
        let first = cache.get_or_build("k", || Ok(1)).unwrap();
        let second = cache
            .get_or_build("k", || panic!("must not build twice"))
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn different_keys_build_separately() {
        let cache = SingleFlightCache::<&str, u32>::new(4);
        let builds = AtomicUsize::new(0);
        let build = |value| {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(value)
        };
        let a = cache.get_or_build("a", || build(1)).unwrap();
        let b = cache.get_or_build("b", || build(2)).unwrap();
        assert_eq!((*a, *b), (1, 2));
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_failure_is_not_cached() {
        let cache = SingleFlightCache::<&str, u32>::new(4);
        let error = cache
            .get_or_build("k", || Err(anyhow::anyhow!("no credentials")))
            .unwrap_err();
        assert!(format!("{error:#}").contains("no credentials"));
        assert_eq!(cache.len(), 0, "a failed build leaves nothing behind");

        let value = cache.get_or_build("k", || Ok(5)).unwrap();
        assert_eq!(*value, 5);
    }

    /// Callers already waiting see the builder's error instead of each running
    /// the failing build in turn.
    #[test]
    fn waiters_receive_the_builders_failure() {
        let cache = SingleFlightCache::<&str, u32>::new(4);
        let builds = AtomicUsize::new(0);
        let messages: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        let error = cache
                            .get_or_build("k", || {
                                builds.fetch_add(1, Ordering::SeqCst);
                                std::thread::sleep(Duration::from_millis(100));
                                Err(anyhow::anyhow!("boom"))
                            })
                            .unwrap_err();
                        format!("{error:#}")
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(messages.iter().all(|m| m.contains("boom")), "{messages:?}");
    }

    #[test]
    fn a_panicking_build_releases_its_waiters_and_is_not_cached() {
        let cache = Arc::new(SingleFlightCache::<&str, u32>::new(4));
        let (started_tx, started_rx) = std::sync::mpsc::channel();

        let builder = {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let _ = cache.get_or_build("k", || {
                    started_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(100));
                    panic!("builder blew up");
                });
            })
        };
        started_rx.recv().unwrap();
        let waiter_error = cache
            .get_or_build("k", || Ok(1))
            .expect_err("the waiter inherits the failed build");
        assert!(format!("{waiter_error:#}").contains("panicked"));
        assert!(builder.join().is_err(), "the panic still reaches the builder's thread");

        assert_eq!(*cache.get_or_build("k", || Ok(9)).unwrap(), 9);
    }

    #[test]
    fn the_least_recently_used_finished_entry_is_evicted() {
        let cache = SingleFlightCache::<u32, u32>::new(2);
        cache.get_or_build(1, || Ok(1)).unwrap();
        cache.get_or_build(2, || Ok(2)).unwrap();
        // Touch 1 so that 2 is the least recently used.
        cache.get_or_build(1, || panic!("cached")).unwrap();
        cache.get_or_build(3, || Ok(3)).unwrap();

        assert_eq!(cache.len(), 2);
        cache
            .get_or_build(1, || panic!("1 was used last and must survive"))
            .unwrap();
        let rebuilt = AtomicUsize::new(0);
        cache
            .get_or_build(2, || {
                rebuilt.fetch_add(1, Ordering::SeqCst);
                Ok(2)
            })
            .unwrap();
        assert_eq!(rebuilt.load(Ordering::SeqCst), 1, "2 was evicted");
    }

    #[test]
    fn the_cache_stays_within_its_bound() {
        let cache = SingleFlightCache::<u32, u32>::new(3);
        for key in 0..20 {
            cache.get_or_build(key, || Ok(key)).unwrap();
            assert!(cache.len() <= 3);
        }
    }

    /// A build in flight is held by its waiters, so eviction must not drop it
    /// even when the cache is full of other keys.
    #[test]
    fn a_build_in_flight_is_never_evicted() {
        let cache = Arc::new(SingleFlightCache::<u32, u32>::new(1));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let slow = {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                cache
                    .get_or_build(1, || {
                        started_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok(1)
                    })
                    .unwrap()
            })
        };
        started_rx.recv().unwrap();
        // Capacity is 1 and key 1 is still building: key 2 must not displace it.
        cache.get_or_build(2, || Ok(2)).unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(*slow.join().unwrap(), 1);
        cache
            .get_or_build(1, || panic!("key 1 finished and was kept"))
            .unwrap();
    }
}
