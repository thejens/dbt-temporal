mod local;
#[cfg(any(feature = "gcs", feature = "aws"))]
mod object_store_backend;

pub use local::LocalArtifactStore;
#[cfg(any(feature = "gcs", feature = "aws"))]
pub use object_store_backend::{ObjectStoreArtifactStore, parse_object_store_url};

use anyhow::Result;
use async_trait::async_trait;

/// Abstraction for storing dbt artifacts (run_results.json, manifest.json).
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store content and return the path/URI where it was written.
    async fn store(&self, invocation_id: &str, filename: &str, content: &[u8]) -> Result<String>;

    /// Retrieve content by path/URI.
    #[allow(dead_code)] // Part of the public interface; not yet used by the worker.
    async fn retrieve(&self, path: &str) -> Result<Vec<u8>>;
}
