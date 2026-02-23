use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

const TOUCH_INTERVAL: Duration = Duration::from_secs(15);
const STALE_THRESHOLD: Duration = Duration::from_secs(60);

/// Touch the health file once (create or update mtime).
pub async fn touch(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        filetime::set_file_mtime(path, filetime::FileTime::now())?;
    } else {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, b"").await?;
    }
    Ok(())
}

/// Spawn a background task that touches the health file every 15 seconds.
/// Returns the JoinHandle so the caller can abort it on shutdown.
pub fn spawn_health_touch(path: PathBuf) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(path = %path.display(), "health file tracker started");
        loop {
            if let Err(e) = touch(&path).await {
                warn!(path = %path.display(), error = %e, "failed to touch health file");
            }
            tokio::time::sleep(TOUCH_INTERVAL).await;
        }
    })
}

/// Check whether the health file exists and has been touched within the staleness threshold.
fn is_healthy(path: &Path) -> bool {
    std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .is_some_and(|mtime| mtime.elapsed().unwrap_or(Duration::MAX) < STALE_THRESHOLD)
}

/// Spawn a minimal HTTP health server on the given port.
///
/// Responds to any request with `200 OK` if the health file is fresh, `503 Service Unavailable`
/// otherwise. Uses raw TCP + hand-written HTTP to avoid pulling in an HTTP framework.
pub fn spawn_health_server(port: u16, path: PathBuf) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(port, error = %e, "failed to bind health server");
                return;
            }
        };
        info!(port, "health HTTP server listening");

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!(error = %e, "health server accept error");
                    continue;
                }
            };

            let healthy = is_healthy(&path);
            let (status, body) = if healthy {
                ("200 OK", "ok\n")
            } else {
                ("503 Service Unavailable", "stale\n")
            };

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );

            let _ = stream.write_all(response.as_bytes()).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn touch_creates_new_file() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-health-{}", uuid::Uuid::new_v4()));
        let path = dir.join("health");

        touch(&path).await?;
        assert!(path.exists());

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn spawn_health_touch_creates_and_updates_file() {
        let dir = std::env::temp_dir().join(format!("dbtt-health-spawn-{}", uuid::Uuid::new_v4()));
        let path = dir.join("health");

        let handle = spawn_health_touch(path.clone());

        // Wait enough for at least one touch.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(path.exists());

        handle.abort();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn touch_updates_mtime_on_existing() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("dbtt-health-{}", uuid::Uuid::new_v4()));
        let path = dir.join("health");

        touch(&path).await?;
        let mtime1 = std::fs::metadata(&path)?.modified()?;

        // Small sleep to ensure mtime differs.
        tokio::time::sleep(Duration::from_millis(50)).await;

        touch(&path).await?;
        let mtime2 = std::fs::metadata(&path)?.modified()?;

        assert!(mtime2 > mtime1);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }
}
