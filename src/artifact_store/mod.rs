mod local;
#[cfg(any(feature = "gcs", feature = "aws"))]
mod object_store_backend;

pub use local::LocalArtifactStore;
#[cfg(any(feature = "gcs", feature = "aws"))]
pub use object_store_backend::{ObjectStoreArtifactStore, parse_object_store_url};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// Abstraction for storing dbt artifacts (run_results.json, manifest.json).
///
/// Payloads are `Bytes` in both directions so that ownership travels rather
/// than the content being copied at the boundary: a manifest is routinely tens
/// of megabytes, and it crossed this trait as a borrowed slice that each
/// backend then had to copy, and came back as a `Vec` that the object-store
/// backend built by copying the `Bytes` it already held.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store content and return the path/URI where it was written.
    async fn store(&self, invocation_id: &str, filename: &str, content: Bytes) -> Result<String>;

    /// Retrieve content by path/URI.
    ///
    /// Used by `plan_project` to load the manifests behind `state_manifest_ref`
    /// and `retry_from`, and by `execute_node` for `defer_manifest_ref`.
    async fn retrieve(&self, path: &str) -> Result<Bytes>;
}
