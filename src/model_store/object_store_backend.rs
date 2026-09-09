use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use tracing::info;

/// How many objects to download at once.
///
/// A dbt project is mostly small files, and fetching them one at a time makes
/// the download a queue of round trips: the wall clock is latency, not
/// bandwidth. The bound is what keeps that from turning into "every object at
/// once", whose memory and connection use is set by the project rather than by
/// us.
const MAX_CONCURRENT_DOWNLOADS: usize = 16;

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

    let count = download_prefix(store.as_ref(), &prefix, &dest).await?;

    info!(files = count, "downloaded model store files");

    let projects = super::scan_for_projects(&dest)?;
    Ok(super::FetchedProjects::owned(dir, projects))
}

/// Copy everything under `prefix` into `dest`, at most
/// `MAX_CONCURRENT_DOWNLOADS` objects at a time. Returns the file count.
async fn download_prefix(
    store: &dyn object_store::ObjectStore,
    prefix: &str,
    dest: &std::path::Path,
) -> Result<u64> {
    let prefix_path = object_store::path::Path::from(prefix);
    store
        .list(Some(&prefix_path))
        .map_err(anyhow::Error::from)
        .try_filter_map(|meta| async move {
            let key = meta.location.to_string();
            let relative = key
                .strip_prefix(prefix)
                .unwrap_or(&key)
                .trim_start_matches('/')
                .to_owned();
            // The prefix itself lists as a zero-length "directory" object on
            // some backends; it names no file to write.
            Ok((!relative.is_empty()).then_some((meta.location, relative)))
        })
        .map_ok(|(location, relative)| async move {
            download_one(store, &location, &dest.join(relative)).await
        })
        .try_buffer_unordered(MAX_CONCURRENT_DOWNLOADS)
        .try_fold(0u64, |n, ()| async move { Ok(n + 1) })
        .await
}

/// Stream one object to disk.
///
/// Streamed rather than buffered: a project can carry a seed of hundreds of
/// megabytes, and holding it whole in memory — times the concurrency bound —
/// is a cost with nothing to show for it.
async fn download_one(
    store: &dyn object_store::ObjectStore,
    location: &object_store::path::Path,
    local_path: &std::path::Path,
) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating dir {}", parent.display()))?;
    }

    let mut body = store
        .get(location)
        .await
        .with_context(|| format!("reading {location}"))?
        .into_stream();
    let mut file = tokio::fs::File::create(local_path)
        .await
        .with_context(|| format!("creating {}", local_path.display()))?;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.with_context(|| format!("reading {location}"))?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .with_context(|| format!("writing {}", local_path.display()))?;
    }
    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .with_context(|| format!("writing {}", local_path.display()))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use object_store::{ObjectStore as _, memory::InMemory, path::Path as ObjectPath};

    async fn store_with(files: &[(&str, &[u8])]) -> InMemory {
        let store = InMemory::new();
        for (key, body) in files {
            store
                .put(&ObjectPath::from(*key), bytes::Bytes::copy_from_slice(body).into())
                .await
                .expect("seed object");
        }
        store
    }

    #[tokio::test]
    async fn downloads_every_object_under_the_prefix() -> Result<()> {
        let store = store_with(&[
            ("proj/dbt_project.yml", b"name: p"),
            ("proj/models/a.sql", b"select 1"),
            ("proj/models/nested/b.sql", b"select 2"),
            ("other/c.sql", b"select 3"),
        ])
        .await;
        let dir = tempfile::tempdir()?;

        let count = download_prefix(&store, "proj", dir.path()).await?;

        assert_eq!(count, 3);
        assert_eq!(std::fs::read_to_string(dir.path().join("dbt_project.yml"))?, "name: p");
        assert_eq!(std::fs::read_to_string(dir.path().join("models/a.sql"))?, "select 1");
        assert_eq!(std::fs::read_to_string(dir.path().join("models/nested/b.sql"))?, "select 2");
        // Outside the prefix, so not ours to download.
        assert!(!dir.path().join("c.sql").exists());
        Ok(())
    }

    /// More objects than the concurrency bound, so the queue actually has to
    /// cycle rather than start everything at once.
    #[tokio::test]
    async fn downloads_more_objects_than_the_concurrency_bound() -> Result<()> {
        let count = MAX_CONCURRENT_DOWNLOADS * 3;
        let names: Vec<String> = (0..count).map(|i| format!("proj/m{i}.sql")).collect();
        let files: Vec<(&str, &[u8])> = names
            .iter()
            .map(|n| (n.as_str(), b"select 1".as_slice()))
            .collect();
        let store = store_with(&files).await;
        let dir = tempfile::tempdir()?;

        let downloaded = download_prefix(&store, "proj", dir.path()).await?;

        assert_eq!(downloaded, count as u64);
        for i in 0..count {
            assert!(dir.path().join(format!("m{i}.sql")).exists(), "m{i}.sql missing");
        }
        Ok(())
    }

    /// A body larger than one chunk exercises the streaming write.
    #[tokio::test]
    async fn writes_a_body_that_arrives_in_several_chunks() -> Result<()> {
        let body: Vec<u8> = (0u8..=255).cycle().take(2_000_000).collect();
        let store = store_with(&[("proj/big.csv", &body)]).await;
        let dir = tempfile::tempdir()?;

        download_prefix(&store, "proj", dir.path()).await?;

        assert_eq!(std::fs::read(dir.path().join("big.csv"))?, body);
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_prefix_downloads_nothing() -> Result<()> {
        let store = store_with(&[("other/a.sql", b"select 1")]).await;
        let dir = tempfile::tempdir()?;
        assert_eq!(download_prefix(&store, "proj", dir.path()).await?, 0);
        Ok(())
    }
}
