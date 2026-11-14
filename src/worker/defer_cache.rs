//! Decoded defer manifests, shared across the activities of a run.
//!
//! `--defer` points a run at a previous run's `manifest.json`. Loading it per
//! node meant every activity downloaded the same artifact and ran the same
//! typed deserialization over it — on a large project, a manifest of hundreds
//! of kilobytes, once per node. Nodes of a new run also all arrive at once, so
//! the load is single-flight: the first activity loads, the rest wait for it.
//!
//! Keyed by the artifact reference, which names an immutable object: a
//! manifest at a given path is the run that produced it. Bounded, because a
//! worker serves an unbounded number of runs over its lifetime.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use dbt_schemas::schemas::Nodes;
use tokio::sync::OnceCell;

/// How many decoded manifests to keep.
///
/// Small: a worker is usually deferring to one or two manifests (a production
/// build, maybe a previous CI run). When a new reference arrives with the cache
/// full, every manifest already loaded is dropped at once — no recency is
/// tracked, since the cost of a miss is one decode. Loads in progress are kept,
/// so their waiters still share them.
const MAX_CACHED_MANIFESTS: usize = 4;

type Cell = Arc<OnceCell<Arc<Nodes>>>;

/// Defer manifests this worker has already decoded.
#[derive(Debug, Default)]
pub struct DeferManifestCache {
    entries: Mutex<HashMap<String, Cell>>,
}

impl DeferManifestCache {
    /// The decoded nodes for this reference, loading them with `load` if no one
    /// has.
    ///
    /// Concurrent callers for one reference share a single `load`; the others
    /// wait for it. A failed load is not cached: the error goes to the caller
    /// that ran it, and a waiter then runs `load` itself.
    pub async fn get_or_load<F, Fut>(
        &self,
        manifest_ref: &str,
        load: F,
    ) -> anyhow::Result<Arc<Nodes>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Arc<Nodes>>>,
    {
        let cell = self.cell(manifest_ref);
        // The map lock is released before awaiting: it is never held across the
        // download.
        cell.get_or_try_init(load).await.map(Arc::clone)
    }

    fn cell(&self, manifest_ref: &str) -> Cell {
        // The map is consistent between statements, so a poisoned lock is safe
        // to keep using.
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(cell) = entries.get(manifest_ref) {
            return Arc::clone(cell);
        }
        if entries.len() >= MAX_CACHED_MANIFESTS {
            // A cell is mid-load exactly when someone besides this map holds it
            // and it has no value yet. Failed or abandoned loads have no other
            // holder, so they go too.
            entries.retain(|_, cell| !cell.initialized() && Arc::strong_count(cell) > 1);
        }
        let cell = Cell::default();
        entries.insert(manifest_ref.to_string(), Arc::clone(&cell));
        cell
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    async fn load(cache: &DeferManifestCache, reference: &str) -> Arc<Nodes> {
        cache
            .get_or_load(reference, || async { Ok(Arc::new(Nodes::default())) })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_decoded_manifest_is_returned_to_the_next_caller() {
        let cache = DeferManifestCache::default();
        let first = load(&cache, "gs://bucket/inv-1/manifest.json").await;
        let second = cache
            .get_or_load("gs://bucket/inv-1/manifest.json", || async {
                panic!("must not load twice")
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second), "the same decode, not another one");
    }

    #[tokio::test]
    async fn manifests_are_kept_apart_by_reference() {
        let cache = DeferManifestCache::default();
        let a = load(&cache, "a/manifest.json").await;
        let b = load(&cache, "b/manifest.json").await;
        assert!(!Arc::ptr_eq(&a, &b));
    }

    /// The nodes of a new run all arrive together; only one may download and
    /// decode.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_first_users_share_one_load() {
        let cache = Arc::new(DeferManifestCache::default());
        let loads = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let loads = Arc::clone(&loads);
                tokio::spawn(async move {
                    cache
                        .get_or_load("run-1/manifest.json", || async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            // Long enough for every other task to be waiting.
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            Ok(Arc::new(Nodes::default()))
                        })
                        .await
                        .unwrap()
                })
            })
            .collect();
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert!(results.iter().all(|r| Arc::ptr_eq(r, &results[0])));
    }

    #[tokio::test]
    async fn a_failed_load_is_not_cached() {
        let cache = DeferManifestCache::default();
        let error = cache
            .get_or_load("run-1/manifest.json", || async { Err(anyhow::anyhow!("403")) })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("403"));

        let loaded = load(&cache, "run-1/manifest.json").await;
        let again = cache
            .get_or_load("run-1/manifest.json", || async { panic!("cached after success") })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&loaded, &again));
    }

    /// A load cancelled mid-flight (its activity was cancelled) leaves the
    /// reference loadable by the next caller.
    #[tokio::test]
    async fn a_cancelled_load_does_not_strand_the_reference() {
        let cache = DeferManifestCache::default();
        let abandoned = tokio::time::timeout(
            Duration::from_millis(20),
            cache.get_or_load("run-1/manifest.json", || async {
                tokio::time::sleep(Duration::from_mins(1)).await;
                Ok(Arc::new(Nodes::default()))
            }),
        )
        .await;
        assert!(abandoned.is_err(), "the load was cut off");
        load(&cache, "run-1/manifest.json").await;
    }

    /// A worker serves an unbounded number of runs, so the cache cannot grow
    /// with them.
    #[tokio::test]
    async fn the_cache_does_not_grow_without_bound() {
        let cache = DeferManifestCache::default();
        for i in 0..MAX_CACHED_MANIFESTS * 3 {
            load(&cache, &format!("run-{i}/manifest.json")).await;
        }
        let held = cache.entries.lock().expect("not poisoned").len();
        assert!(held <= MAX_CACHED_MANIFESTS, "held {held}");
    }

    /// Eviction must not drop a load that is still running, or its waiters and
    /// the next arrival would each download the manifest again.
    #[tokio::test]
    async fn eviction_keeps_a_load_in_progress() {
        let cache = Arc::new(DeferManifestCache::default());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let slow = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                cache
                    .get_or_load("slow/manifest.json", || async move {
                        release_rx.await.unwrap();
                        Ok(Arc::new(Nodes::default()))
                    })
                    .await
                    .unwrap()
            })
        };
        // Let the slow load start, then overflow the cache around it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for i in 0..MAX_CACHED_MANIFESTS * 2 {
            load(&cache, &format!("run-{i}/manifest.json")).await;
        }
        let joined = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                cache
                    .get_or_load("slow/manifest.json", || async {
                        panic!("joined the in-flight load")
                    })
                    .await
                    .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        release_tx.send(()).unwrap();
        let (a, b) = (slow.await.unwrap(), joined.await.unwrap());
        assert!(Arc::ptr_eq(&a, &b));
    }
}
