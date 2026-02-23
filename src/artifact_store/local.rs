use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::PathBuf;

use super::ArtifactStore;

/// Stores artifacts on the local filesystem.
#[derive(Debug)]
pub struct LocalArtifactStore {
    base_dir: PathBuf,
}

impl LocalArtifactStore {
    pub const fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn store(&self, invocation_id: &str, filename: &str, content: &[u8]) -> Result<String> {
        let dir = self.base_dir.join(invocation_id);
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating artifact dir {}", dir.display()))?;

        let path = dir.join(filename);
        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("writing artifact {}", path.display()))?;

        Ok(path.to_string_lossy().into_owned())
    }

    async fn retrieve(&self, path: &str) -> Result<Vec<u8>> {
        tokio::fs::read(path)
            .await
            .with_context(|| format!("reading artifact {path}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn store_and_retrieve_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(dir.clone());

        let path = store
            .store("inv-123", "run_results.json", b"{\"results\":[]}")
            .await?;

        assert!(std::path::Path::new(&path).exists());

        let content = store.retrieve(&path).await?;
        assert_eq!(content, b"{\"results\":[]}");

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn store_creates_nested_dirs() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(dir.clone());

        let path = store.store("deep/inv", "file.txt", b"hello").await?;
        assert!(std::path::Path::new(&path).exists());

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn retrieve_missing_file_errors() {
        let dir = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(dir);

        let result = store.retrieve("/tmp/nonexistent-dbtt-artifact-file").await;
        assert!(result.is_err());
    }
}
