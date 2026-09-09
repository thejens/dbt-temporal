use anyhow::{Context, Result};
use futures::TryStreamExt;
use tracing::info;

/// Fetch dbt project(s) from an object store (GCS, S3/Minio).
///
/// Downloads all files under the URL prefix to a local temp directory,
/// then scans for `dbt_project.yml` to find project roots.
pub async fn fetch(url: &str) -> Result<super::FetchedProjects> {
    let (store, prefix) = crate::artifact_store::parse_object_store_url(url)?;

    // Owned from here on: a download that fails partway removes what it wrote
    // instead of leaving an incomplete project behind.
    let dir = super::fetch_dir()?;
    let dest = dir.path().to_path_buf();

    info!(url = url, dest = %dest.display(), "downloading models from object store");

    let prefix_path = object_store::path::Path::from(prefix.as_str());
    let mut list_stream = store.list(Some(&prefix_path));
    let mut count = 0u64;

    while let Some(meta) = list_stream.try_next().await? {
        let key = meta.location.to_string();
        let relative = key
            .strip_prefix(&prefix)
            .unwrap_or(&key)
            .trim_start_matches('/');

        if relative.is_empty() {
            continue;
        }

        let local_path = dest.join(relative);
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating dir {}", parent.display()))?;
        }

        let data = store.get(&meta.location).await?.bytes().await?;
        tokio::fs::write(&local_path, data)
            .await
            .with_context(|| format!("writing {}", local_path.display()))?;
        count += 1;
    }

    info!(files = count, "downloaded model store files");

    let projects = super::scan_for_projects(&dest)?;
    Ok(super::FetchedProjects::owned(dir, projects))
}
