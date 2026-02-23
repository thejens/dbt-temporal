mod git;
#[cfg(any(feature = "gcs", feature = "aws"))]
mod object_store_backend;

use anyhow::Result;
use std::path::PathBuf;

/// Fetch dbt project(s) from a model store to local disk.
///
/// Supported URL schemes:
/// - `git+https://github.com/org/repo.git#branch` — clone from git (set `GITHUB_TOKEN` or `GIT_TOKEN` for private repos)
/// - `git+ssh://git@github.com/org/repo.git#branch` — clone via SSH
/// - `gs://bucket/prefix` — download from GCS (requires `gcs` feature)
/// - `s3://bucket/prefix` — download from S3/Minio (requires `aws` feature)
///
/// Returns the discovered project directories.
pub async fn fetch_models(url: &str) -> Result<Vec<PathBuf>> {
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

/// Scan a directory for dbt projects. If the directory itself is a project, returns it.
/// Otherwise scans immediate subdirs for `dbt_project.yml`.
fn scan_for_projects(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    if dir.join("dbt_project.yml").exists() {
        return Ok(vec![dir.to_path_buf()]);
    }

    let mut projects: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir)
        .map_err(|e| anyhow::anyhow!("reading model store dir {}: {e}", dir.display()))?
    {
        let entry = entry
            .map_err(|e| anyhow::anyhow!("reading entry in model store {}: {e}", dir.display()))?;
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
}
