//! Integration test: ObjectStoreArtifactStore against Minio (S3-compatible).
//!
//! Requires the `aws` feature flag:
//!   cargo test --features aws --test artifact_store_s3

#![cfg(feature = "aws")]

use anyhow::{Context, Result};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use dbt_temporal::artifact_store::{ArtifactStore, ObjectStoreArtifactStore};

const BUCKET: &str = "test-artifacts";
const PREFIX: &str = "dbt-artifacts";

/// Start Minio, create the test bucket, and return the ObjectStoreArtifactStore.
async fn setup() -> Result<(testcontainers::ContainerAsync<MinIO>, ObjectStoreArtifactStore)> {
    let container = MinIO::default()
        .start()
        .await
        .context("starting minio container")?;

    let host = container.get_host().await.context("get minio host")?;
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .context("get minio port")?;
    let endpoint = format!("http://{host}:{port}");

    // Create the bucket via the AWS SDK (path-style required for Minio).
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .credentials_provider(aws_credential_types::Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "test",
        ))
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    s3_client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .context("creating test bucket")?;

    // Build the ObjectStoreArtifactStore pointing at Minio.
    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_endpoint(&endpoint)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()
        .context("building S3 object store for Minio")?;

    let artifact_store = ObjectStoreArtifactStore::new(Box::new(store), PREFIX.to_string());

    Ok((container, artifact_store))
}

#[tokio::test]
async fn test_store_and_retrieve() -> Result<()> {
    let (_container, store) = setup().await?;

    let content = b"hello world from dbt-temporal";
    let path = store.store("inv-001", "test.json", content).await?;

    assert_eq!(path, "dbt-artifacts/inv-001/test.json");

    let retrieved = store.retrieve(&path).await?;
    assert_eq!(retrieved, content);

    Ok(())
}

#[tokio::test]
async fn test_store_large_payload() -> Result<()> {
    let (_container, store) = setup().await?;

    // 1 MB payload.
    let content: Vec<u8> = (0u8..=255).cycle().take(1_000_000).collect();
    let path = store.store("inv-002", "manifest.json", &content).await?;

    let retrieved = store.retrieve(&path).await?;
    assert_eq!(retrieved.len(), content.len());
    assert_eq!(retrieved, content);

    Ok(())
}

#[tokio::test]
async fn test_store_multiple_files_same_invocation() -> Result<()> {
    let (_container, store) = setup().await?;

    let manifest = br#"{"metadata":{},"nodes":{}}"#;
    let run_results = br#"{"results":[]}"#;

    let p1 = store.store("inv-003", "manifest.json", manifest).await?;
    let p2 = store
        .store("inv-003", "run_results.json", run_results)
        .await?;

    assert_eq!(p1, "dbt-artifacts/inv-003/manifest.json");
    assert_eq!(p2, "dbt-artifacts/inv-003/run_results.json");

    assert_eq!(store.retrieve(&p1).await?, manifest);
    assert_eq!(store.retrieve(&p2).await?, run_results);

    Ok(())
}

#[tokio::test]
async fn test_retrieve_nonexistent_returns_error() -> Result<()> {
    let (_container, store) = setup().await?;

    let result = store.retrieve("dbt-artifacts/no-such/file.json").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_overwrite_existing_file() -> Result<()> {
    let (_container, store) = setup().await?;

    let path = store.store("inv-004", "data.json", b"v1").await?;
    assert_eq!(store.retrieve(&path).await?, b"v1");

    let path2 = store.store("inv-004", "data.json", b"v2").await?;
    assert_eq!(path, path2);
    assert_eq!(store.retrieve(&path).await?, b"v2");

    Ok(())
}
