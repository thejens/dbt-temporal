mod git;
#[cfg(any(feature = "gcs", feature = "aws"))]
mod object_store_backend;

use anyhow::{Context, Result};
use std::path::PathBuf;

/// A fetched model store, and the dbt projects found inside it.
///
/// Owning the directory is the point. A fetch used to allocate a UUID-named
/// directory under the system temp dir and return only paths, so nothing could
/// ever remove it: a worker that restarts often enough fills its temp
/// filesystem with copies of the same project, and a clone that failed halfway
/// left a partial one behind under a name nothing would look at again. The
/// directory now goes away when the registry that loaded from it does.
#[derive(Debug)]
pub struct FetchedProjects {
    /// Removed on drop. `None` for a project directory the worker was pointed
    /// at rather than fetched — that one belongs to whoever created it.
    _dir: Option<tempfile::TempDir>,
    /// Project roots discovered inside.
    pub projects: Vec<PathBuf>,
}

impl FetchedProjects {
    /// A fetched store whose directory this value owns.
    const fn owned(dir: tempfile::TempDir, projects: Vec<PathBuf>) -> Self {
        Self {
            _dir: Some(dir),
            projects,
        }
    }

    /// A local path the worker was pointed at, which it must not remove.
    pub const fn borrowed(projects: Vec<PathBuf>) -> Self {
        Self {
            _dir: None,
            projects,
        }
    }
}

/// Fetch dbt project(s) from a model store to local disk.
///
/// Supported URL schemes:
/// - `git+https://github.com/org/repo.git#branch` — clone from git (set `GITHUB_TOKEN` or `GIT_TOKEN` for private repos)
/// - `git+ssh://git@github.com/org/repo.git#branch` — clone via SSH
/// - `gs://bucket/prefix` — download from GCS (requires `gcs` feature)
/// - `s3://bucket/prefix` — download from S3/Minio (requires `aws` feature)
///
/// The returned value owns the directory it fetched into; keep it for as long
/// as the projects are in use.
pub async fn fetch_models(url: &str) -> Result<FetchedProjects> {
    if url.starts_with("git+") {
        git::fetch(url).await
    } else {
        #[cfg(any(feature = "gcs", feature = "aws"))]
        {
            object_store_backend::fetch(url).await
        }
        #[cfg(not(any(feature = "gcs", feature = "aws")))]
        {
            let _ = url;
            anyhow::bail!(
                "cloud storage URLs (gs://, s3://) require the 'gcs' or 'aws' feature flag"
            )
        }
    }
}

/// Create the temp directory a fetch downloads into.
///
/// `tempfile` rather than a hand-rolled UUID path: it creates the directory
/// with the right permissions, and its `TempDir` is what gives the caller
/// something to own.
fn fetch_dir() -> Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix("dbtt-models-")
        .tempdir()
        .context("creating model store directory")
}

/// Scan a directory for dbt projects. If the directory itself is a project, returns it.
/// Otherwise scans immediate subdirs for `dbt_project.yml`.
fn scan_for_projects(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    if dir.join("dbt_project.yml").exists() {
        return Ok(vec![dir.to_path_buf()]);
    }

    let mut projects: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading model store dir {}", dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("reading entry in model store {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() && path.join("dbt_project.yml").exists() {
            projects.push(path);
        }
    }

    projects.sort();

    if projects.is_empty() {
        anyhow::bail!("no dbt projects found in model store at {}", dir.display());
    }
    Ok(projects)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn scan_dir_is_a_project() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-ms-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("dbt_project.yml"), "name: proj")?;

        let projects = scan_for_projects(&dir)?;
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0], dir);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn scan_finds_subdir_projects() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-ms-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("proj_a"))?;
        std::fs::write(dir.join("proj_a/dbt_project.yml"), "name: a")?;
        std::fs::create_dir_all(dir.join("proj_b"))?;
        std::fs::write(dir.join("proj_b/dbt_project.yml"), "name: b")?;
        std::fs::create_dir_all(dir.join("not_a_project"))?;

        let projects = scan_for_projects(&dir)?;
        assert_eq!(projects.len(), 2);
        assert!(projects[0].ends_with("proj_a"));
        assert!(projects[1].ends_with("proj_b"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn scan_empty_dir_errors() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-ms-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;

        let result = scan_for_projects(&dir);
        assert!(result.is_err());

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    #[cfg(not(any(feature = "gcs", feature = "aws")))]
    async fn fetch_models_rejects_cloud_url_without_feature() {
        // Without gcs/aws compile flags, gs:// and s3:// must error with an
        // actionable hint pointing at the feature flag.
        let err = fetch_models("s3://bucket/prefix")
            .await
            .expect_err("s3:// URL without aws feature should fail");
        let msg = err.to_string();
        assert!(msg.contains("gcs") || msg.contains("aws"), "got: {msg}");
        assert!(msg.contains("feature"), "got: {msg}");

        let err = fetch_models("gs://bucket/prefix")
            .await
            .expect_err("gs:// URL without gcs feature should fail");
        assert!(err.to_string().contains("feature"));
    }
}
