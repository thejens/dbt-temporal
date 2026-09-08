use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::{Component, Path, PathBuf};

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

    /// Resolve an artifact reference to a real path inside the store.
    ///
    /// This is the security boundary of the whole type. `retrieve` is reached
    /// with values that arrive on workflow input — `defer_manifest_ref`,
    /// `state_manifest_ref`, `resume_from.state_ref` — so a reference that is
    /// allowed to name any path makes "can start a workflow" mean "can read any
    /// file the worker can read".
    ///
    /// References handed out by `store` are absolute paths under `base_dir`, so
    /// those keep working; a relative reference is taken as store-relative.
    /// Both sides are canonicalized before comparison, so a symlink planted
    /// inside the store cannot point out of it.
    fn resolve_for_read(&self, reference: &str) -> Result<PathBuf> {
        let candidate = Path::new(reference);
        let joined = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            confined_join(&self.base_dir, &[reference])?
        };

        let base = self.base_dir.canonicalize().with_context(|| {
            format!("resolving artifact store root {}", self.base_dir.display())
        })?;
        let resolved = joined
            .canonicalize()
            .with_context(|| format!("resolving artifact {reference}"))?;

        anyhow::ensure!(
            resolved.starts_with(&base),
            "artifact reference resolves outside the artifact store: {reference}"
        );
        Ok(resolved)
    }
}

/// Join caller-supplied fragments under `base`, refusing any that could climb
/// out of it.
///
/// Ordinary nesting is allowed — `invocation_id` may contain separators — but a
/// `..`, an absolute path, or a drive prefix is rejected rather than normalized
/// away, so a malformed reference fails loudly instead of landing somewhere
/// surprising.
fn confined_join(base: &Path, parts: &[&str]) -> Result<PathBuf> {
    let mut out = base.to_path_buf();
    for part in parts {
        for component in Path::new(part).components() {
            match component {
                Component::Normal(segment) => out.push(segment),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    anyhow::bail!("artifact path escapes the artifact store: {part}")
                }
            }
        }
    }
    anyhow::ensure!(out != base, "artifact path names the artifact store root itself: {parts:?}");
    Ok(out)
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn store(&self, invocation_id: &str, filename: &str, content: &[u8]) -> Result<String> {
        let path = confined_join(&self.base_dir, &[invocation_id, filename])?;
        let dir = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("artifact path has no parent: {}", path.display()))?;
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("creating artifact dir {}", dir.display()))?;

        // Publish by rename. A direct write leaves a truncated file visible at
        // the destination for as long as it takes to finish — and a cancelled
        // activity or a killed worker leaves it there for good, where the next
        // reader takes it for a complete artifact. Rename within one directory
        // is atomic, so a reader sees either no file or the whole file.
        let tmp = dir.join(format!(".{}.{}.tmp", filename, uuid::Uuid::new_v4()));
        if let Err(e) = tokio::fs::write(&tmp, content).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("writing artifact {}", path.display()));
        }
        if let Err(e) = tokio::fs::rename(&tmp, &path).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("publishing artifact {}", path.display()));
        }

        Ok(path.to_string_lossy().into_owned())
    }

    async fn retrieve(&self, path: &str) -> Result<Vec<u8>> {
        let resolved = self.resolve_for_read(path)?;
        tokio::fs::read(&resolved)
            .await
            .with_context(|| format!("reading artifact {}", resolved.display()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn store_and_retrieve_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(dir.clone());

        let path = store
            .store("inv-123", "run_results.json", b"{\"results\":[]}")
            .await?;

        assert!(Path::new(&path).exists());

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
        assert!(Path::new(&path).exists());

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

    /// `retrieve` is reached with values that arrive on workflow input, so an
    /// unconfined read here would let anyone who can start a workflow read any
    /// file the worker can read.
    #[tokio::test]
    async fn retrieve_refuses_an_absolute_path_outside_the_store() -> Result<()> {
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let outside = std::env::temp_dir().join(format!("dbtt-outside-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&base).await?;
        tokio::fs::write(&outside, b"not yours").await?;

        let store = LocalArtifactStore::new(base.clone());
        let err = store
            .retrieve(&outside.to_string_lossy())
            .await
            .expect_err("a path outside the store must be refused");
        assert!(
            format!("{err:#}").contains("outside the artifact store"),
            "unexpected error: {err:#}"
        );

        std::fs::remove_file(&outside).ok();
        std::fs::remove_dir_all(&base).ok();
        Ok(())
    }

    #[tokio::test]
    async fn retrieve_refuses_a_relative_reference_that_climbs_out() -> Result<()> {
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&base).await?;
        let store = LocalArtifactStore::new(base.clone());

        let err = store
            .retrieve("../../etc/passwd")
            .await
            .expect_err("a climbing reference must be refused");
        assert!(
            format!("{err:#}").contains("escapes the artifact store"),
            "unexpected error: {err:#}"
        );

        std::fs::remove_dir_all(&base).ok();
        Ok(())
    }

    /// A symlink planted inside the store must not widen what it can read —
    /// which is why both sides are canonicalized before being compared.
    #[cfg(unix)]
    #[tokio::test]
    async fn retrieve_refuses_a_symlink_pointing_out_of_the_store() -> Result<()> {
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let outside = std::env::temp_dir().join(format!("dbtt-outside-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&base).await?;
        tokio::fs::write(&outside, b"not yours").await?;
        std::os::unix::fs::symlink(&outside, base.join("escape.json"))?;

        let store = LocalArtifactStore::new(base.clone());
        let err = store
            .retrieve("escape.json")
            .await
            .expect_err("a symlink out of the store must be refused");
        assert!(
            format!("{err:#}").contains("outside the artifact store"),
            "unexpected error: {err:#}"
        );

        std::fs::remove_file(&outside).ok();
        std::fs::remove_dir_all(&base).ok();
        Ok(())
    }

    #[tokio::test]
    async fn retrieve_accepts_a_store_relative_reference() -> Result<()> {
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(base.clone());
        store.store("inv-1", "manifest.json", b"{}").await?;

        assert_eq!(store.retrieve("inv-1/manifest.json").await?, b"{}");

        std::fs::remove_dir_all(&base).ok();
        Ok(())
    }

    #[tokio::test]
    async fn store_refuses_an_invocation_id_that_climbs_out() -> Result<()> {
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(base.clone());

        let err = store
            .store("../escape", "manifest.json", b"{}")
            .await
            .expect_err("a climbing invocation id must be refused");
        assert!(
            format!("{err:#}").contains("escapes the artifact store"),
            "unexpected error: {err:#}"
        );

        std::fs::remove_dir_all(&base).ok();
        Ok(())
    }

    /// Publication is a rename, so the destination never holds a partial file —
    /// and the scratch file it renames from must not survive the write.
    #[tokio::test]
    async fn store_publishes_atomically_and_leaves_no_scratch_file() -> Result<()> {
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let store = LocalArtifactStore::new(base.clone());
        store.store("inv-1", "manifest.json", b"{}").await?;

        let entries: Vec<String> = std::fs::read_dir(base.join("inv-1"))?
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect();
        assert_eq!(entries, vec!["manifest.json".to_string()], "got: {entries:?}");

        std::fs::remove_dir_all(&base).ok();
        Ok(())
    }

    #[tokio::test]
    async fn store_errors_when_base_dir_is_a_file() -> Result<()> {
        // base_dir is a regular file, so creating the invocation subdir under
        // it fails — exercises the create_dir_all error path.
        let file =
            std::env::temp_dir().join(format!("dbtt-artifact-file-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&file, b"not a directory").await?;
        let store = LocalArtifactStore::new(file.clone());

        let result = store.store("inv", "run_results.json", b"{}").await;
        assert!(result.is_err(), "expected create_dir_all under a file to fail");

        std::fs::remove_file(&file)?;
        Ok(())
    }

    #[tokio::test]
    async fn store_errors_when_target_path_is_a_directory() -> Result<()> {
        // The artifact filename collides with an existing directory, so the
        // write fails after the parent dir is created — exercises the write
        // error path.
        let base = std::env::temp_dir().join(format!("dbtt-artifact-{}", uuid::Uuid::new_v4()));
        let collide = base.join("inv").join("catalog.json");
        tokio::fs::create_dir_all(&collide).await?;
        let store = LocalArtifactStore::new(base.clone());

        let result = store.store("inv", "catalog.json", b"{}").await;
        assert!(result.is_err(), "expected write to a directory path to fail");

        std::fs::remove_dir_all(&base)?;
        Ok(())
    }
}
