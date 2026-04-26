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
async fn is_healthy(path: &Path) -> bool {
    fs::metadata(path).await.is_ok_and(|m| {
        m.modified()
            .ok()
            .is_some_and(|mtime| mtime.elapsed().unwrap_or(Duration::MAX) < STALE_THRESHOLD)
    })
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
        run_health_server(listener, path).await;
    })
}

async fn run_health_server(listener: tokio::net::TcpListener, path: PathBuf) {
    let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
    info!(port, "health HTTP server listening");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "health server accept error");
                continue;
            }
        };

        let path = path.clone();
        // Per-connection task so a slow probe client doesn't stall accept.
        tokio::spawn(async move {
            handle_health_connection(stream, &path).await;
        });
    }
}

async fn handle_health_connection(mut stream: tokio::net::TcpStream, path: &Path) {
    use tokio::io::AsyncReadExt;

    // Drain the request before responding. Without this, dropping the stream
    // with unread data in the OS receive buffer turns the close into an RST
    // instead of FIN, and the client sees "connection reset by peer" before
    // it finishes reading the response. A typical probe (`GET / HTTP/1.1...`)
    // fits in one read, so we don't loop — pipelined clients are out of scope
    // for a liveness probe.
    let mut buf = [0u8; 1024];
    if stream.read(&mut buf).await.is_err() {
        return;
    }

    let healthy = is_healthy(path).await;
    let (status, body) = if healthy {
        ("200 OK", "ok\n")
    } else {
        ("503 Service Unavailable", "stale\n")
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );

    if let Err(e) = stream.write_all(response.as_bytes()).await {
        tracing::debug!(error = %e, "health server failed to write response");
        return;
    }
    // Send FIN so the client can drain cleanly instead of seeing RST.
    let _ = stream.shutdown().await;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

    #[tokio::test]
    async fn is_healthy_false_for_missing_file() {
        let path = std::env::temp_dir().join(format!("dbtt-missing-{}.tmp", uuid::Uuid::new_v4()));
        assert!(!is_healthy(&path).await);
    }

    #[tokio::test]
    async fn is_healthy_true_for_fresh_file() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!("dbtt-fresh-{}.tmp", uuid::Uuid::new_v4()));
        touch(&path).await?;
        assert!(is_healthy(&path).await);
        std::fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn is_healthy_false_for_stale_file() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!("dbtt-stale-{}.tmp", uuid::Uuid::new_v4()));
        touch(&path).await?;
        // Backdate mtime well past the staleness threshold.
        let stale = filetime::FileTime::from_system_time(
            std::time::SystemTime::now() - Duration::from_secs(STALE_THRESHOLD.as_secs() + 30),
        );
        filetime::set_file_mtime(&path, stale)?;
        assert!(!is_healthy(&path).await);
        std::fs::remove_file(&path).ok();
        Ok(())
    }

    /// Hit a freshly-bound listener once and capture the response.
    async fn http_get(addr: std::net::SocketAddr) -> std::io::Result<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut buf = String::new();
        stream.read_to_string(&mut buf).await?;
        Ok(buf)
    }

    #[tokio::test]
    async fn server_returns_200_when_file_is_fresh() -> anyhow::Result<()> {
        let path =
            std::env::temp_dir().join(format!("dbtt-srv-fresh-{}.tmp", uuid::Uuid::new_v4()));
        touch(&path).await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_path = path.clone();
        let handle = tokio::spawn(async move { run_health_server(listener, server_path).await });

        let resp = http_get(addr).await?;
        assert!(resp.contains("200 OK"), "got: {resp}");
        assert!(resp.contains("ok"), "got: {resp}");

        handle.abort();
        std::fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn server_returns_503_when_file_is_stale() -> anyhow::Result<()> {
        let path =
            std::env::temp_dir().join(format!("dbtt-srv-stale-{}.tmp", uuid::Uuid::new_v4()));
        // No touch — file doesn't exist, which counts as not-healthy.

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_path = path.clone();
        let handle = tokio::spawn(async move { run_health_server(listener, server_path).await });

        let resp = http_get(addr).await?;
        assert!(resp.contains("503 Service Unavailable"), "got: {resp}");
        assert!(resp.contains("stale"), "got: {resp}");

        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn spawn_health_server_exits_when_bind_fails() -> anyhow::Result<()> {
        // Bind 0.0.0.0:0 as a probe — spawn_health_server binds the same address,
        // so reusing the picked port produces a deterministic EADDRINUSE without
        // needing privileged ports.
        let probe = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
        let port = probe.local_addr()?.port();

        let path =
            std::env::temp_dir().join(format!("dbtt-bind-fail-{}.tmp", uuid::Uuid::new_v4()));
        let handle = spawn_health_server(port, path);

        // The spawned task should observe EADDRINUSE and return immediately.
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .map_err(|_| anyhow::anyhow!("server task did not exit after bind failure"))?
            .map_err(|e| anyhow::anyhow!("server task panicked: {e}"))?;
        Ok(())
    }
}
