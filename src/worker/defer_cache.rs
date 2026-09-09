//! Decoded defer manifests, shared across the activities of a run.
//!
//! `--defer` points a run at a previous run's `manifest.json`. Loading it per
//! node meant every activity downloaded the same artifact and ran the same
//! typed deserialization over it — on a large project, a manifest of hundreds
//! of kilobytes, once per node.
//!
//! Keyed by the artifact reference, which names an immutable object: a
//! manifest at a given path is the run that produced it. Bounded, because a
//! worker serves an unbounded number of runs over its lifetime.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dbt_schemas::schemas::Nodes;

/// How many decoded manifests to keep.
///
/// Small: a worker is usually deferring to one or two manifests (a production
/// build, maybe a previous CI run). Past that the least-recently-stored entry
/// is dropped wholesale rather than tracked, since the cost of a miss is one
/// decode.
const MAX_CACHED_MANIFESTS: usize = 4;

/// Defer manifests this worker has already decoded.
#[derive(Debug, Default)]
pub struct DeferManifestCache {
    entries: Mutex<HashMap<String, Arc<Nodes>>>,
}

impl DeferManifestCache {
    /// The decoded nodes for this reference, if they are already in hand.
    pub fn get(&self, manifest_ref: &str) -> Option<Arc<Nodes>> {
        self.entries.lock().ok()?.get(manifest_ref).map(Arc::clone)
    }

    /// Remember a decoded manifest, evicting wholesale when full.
    pub fn insert(&self, manifest_ref: &str, nodes: Arc<Nodes>) {
        let Ok(mut entries) = self.entries.lock() else {
            // A poisoned lock costs a decode per node, not correctness.
            return;
        };
        if entries.len() >= MAX_CACHED_MANIFESTS {
            entries.clear();
        }
        entries.insert(manifest_ref.to_string(), nodes);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_decoded_manifest_is_returned_to_the_next_caller() {
        let cache = DeferManifestCache::default();
        assert!(cache.get("gs://bucket/inv-1/manifest.json").is_none());

        let nodes = Arc::new(Nodes::default());
        cache.insert("gs://bucket/inv-1/manifest.json", Arc::clone(&nodes));

        let hit = cache
            .get("gs://bucket/inv-1/manifest.json")
            .expect("cached");
        assert!(Arc::ptr_eq(&hit, &nodes), "the same decode, not another one");
    }

    #[test]
    fn manifests_are_kept_apart_by_reference() {
        let cache = DeferManifestCache::default();
        cache.insert("a/manifest.json", Arc::new(Nodes::default()));
        assert!(cache.get("b/manifest.json").is_none());
    }

    /// A worker serves an unbounded number of runs, so the cache cannot grow
    /// with them.
    #[test]
    fn the_cache_does_not_grow_without_bound() {
        let cache = DeferManifestCache::default();
        for i in 0..MAX_CACHED_MANIFESTS * 3 {
            cache.insert(&format!("run-{i}/manifest.json"), Arc::new(Nodes::default()));
        }
        let held = cache.entries.lock().expect("not poisoned").len();
        assert!(held <= MAX_CACHED_MANIFESTS, "held {held}");
    }
}
