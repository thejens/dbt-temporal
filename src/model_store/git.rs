use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing::info;

/// Parsed components of a git model store URL.
struct GitUrl<'a> {
    repo_url: &'a str,
    branch: &'a str,
    subdir: Option<&'a str>,
}

/// Parse a git model store URL into its components.
///
/// Accepted formats (after stripping the `git+` prefix):
/// - `https://github.com/org/repo.git#branch`
/// - `https://github.com/org/repo.git#branch:path/to/subdir`
fn parse_git_url(url: &str) -> Result<GitUrl<'_>> {
    let url = url.strip_prefix("git+").unwrap_or(url);
    let (repo_url, fragment) = url
        .rsplit_once('#')
        .ok_or_else(|| anyhow::anyhow!("git model store URL must have #branch suffix: {url}"))?;

    let (branch, subdir) = match fragment.split_once(':') {
        Some((b, s)) => (b, Some(s)),
        None => (fragment, None),
    };

    Ok(GitUrl {
        repo_url,
        branch,
        subdir,
    })
}

/// Fetch dbt project(s) by cloning a git repository.
///
/// URL format: `git+https://github.com/org/repo.git#branch[:subdir]`
///
/// Authentication token is read from `GITHUB_TOKEN`, then `GIT_TOKEN` (first non-empty wins).
/// The token reaches git through `GIT_CONFIG_*` env vars (a `url.<with-token>.insteadOf`
/// rewrite) rather than the clone URL itself — an argv-embedded token is visible in the
/// process list while git runs, and git persists the remote URL verbatim into the cloned
/// repo's `.git/config`.
pub async fn fetch(url: &str) -> Result<Vec<PathBuf>> {
    let parsed = parse_git_url(url)?;

    let dest = std::env::temp_dir().join(format!("dbtt-models-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dest)
        .with_context(|| format!("creating model store dir {}", dest.display()))?;

    info!(branch = parsed.branch, dest = %dest.display(), "cloning model store from git");

    let mut cmd = tokio::process::Command::new("git");
    cmd.args([
        "clone",
        "--depth",
        "1",
        "--branch",
        parsed.branch,
        parsed.repo_url,
    ])
    .arg(&dest);
    if let Some((key, value)) = token_rewrite_config(parsed.repo_url)? {
        cmd.env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", key)
            .env("GIT_CONFIG_VALUE_0", value);
    }
    let output = cmd.output().await.context("running git clone")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = redact_token(&stderr);
        anyhow::bail!("git clone failed: {stderr}");
    }

    info!(dest = %dest.display(), "git clone complete");

    let scan_root = match parsed.subdir {
        Some(sub) => {
            let root = dest.join(sub);
            if !root.is_dir() {
                anyhow::bail!("subdirectory '{sub}' not found in cloned repository");
            }
            root
        }
        None => dest,
    };

    let dirs = super::scan_for_projects(&scan_root)?;
    Ok(dirs)
}

/// Read the git auth token from `GITHUB_TOKEN` or `GIT_TOKEN` (first non-empty wins).
fn git_token() -> Option<String> {
    for var in ["GITHUB_TOKEN", "GIT_TOKEN"] {
        if let Ok(t) = std::env::var(var)
            && !t.is_empty()
        {
            return Some(t);
        }
    }
    None
}

/// Build the `url.<with-token>.insteadOf` git-config pair that authenticates an
/// HTTPS clone. Returns `None` when no token is set or the URL isn't HTTPS
/// (SSH auth comes from the agent/keys, not a token).
fn token_rewrite_config(repo_url: &str) -> Result<Option<(String, String)>> {
    let Some(token) = git_token() else {
        return Ok(None);
    };

    let parsed = url::Url::parse(repo_url).context("parsing git URL")?;
    if parsed.scheme() != "https" {
        return Ok(None);
    }

    let mut with_token = parsed;
    with_token
        .set_username(&token)
        .map_err(|()| anyhow::anyhow!("failed to set git token in URL"))?;
    Ok(Some((format!("url.{with_token}.insteadOf"), repo_url.to_string())))
}

/// Redact any git token from error output.
fn redact_token(msg: &str) -> String {
    git_token().map_or_else(|| msg.to_string(), |token| msg.replace(&token, "***"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialize all git-token env var tests to avoid races.
    static GIT_ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that restores GITHUB_TOKEN/GIT_TOKEN on drop, including on panic.
    /// The previous version restored after `f()` returned, so a test assertion panic
    /// would leak the test's tokens into subsequent tests.
    #[allow(unsafe_code)]
    struct TokenGuard {
        github: Option<String>,
        git: Option<String>,
    }

    #[allow(unsafe_code)]
    impl Drop for TokenGuard {
        fn drop(&mut self) {
            unsafe {
                std::env::remove_var("GITHUB_TOKEN");
                std::env::remove_var("GIT_TOKEN");
                if let Some(v) = &self.github {
                    std::env::set_var("GITHUB_TOKEN", v);
                }
                if let Some(v) = &self.git {
                    std::env::set_var("GIT_TOKEN", v);
                }
            }
        }
    }

    /// Helper: run a closure with specific GITHUB_TOKEN and GIT_TOKEN values, restoring after.
    #[allow(unsafe_code)]
    fn with_git_tokens<F: FnOnce()>(github: Option<&str>, git: Option<&str>, f: F) {
        let Ok(_lock) = GIT_ENV_LOCK.lock() else {
            panic!("GIT_ENV_LOCK poisoned");
        };

        let _restore = TokenGuard {
            github: std::env::var("GITHUB_TOKEN").ok(),
            git: std::env::var("GIT_TOKEN").ok(),
        };

        unsafe {
            std::env::remove_var("GITHUB_TOKEN");
            std::env::remove_var("GIT_TOKEN");
            if let Some(v) = github {
                std::env::set_var("GITHUB_TOKEN", v);
            }
            if let Some(v) = git {
                std::env::set_var("GIT_TOKEN", v);
            }
        }

        f();
    }

    #[test]
    fn git_token_prefers_github_token() {
        with_git_tokens(Some("gh-tok"), Some("git-tok"), || {
            assert_eq!(git_token(), Some("gh-tok".to_string()));
        });
    }

    #[test]
    fn git_token_falls_back_to_git_token() {
        with_git_tokens(None, Some("git-tok"), || {
            assert_eq!(git_token(), Some("git-tok".to_string()));
        });
    }

    #[test]
    fn git_token_none_when_both_unset() {
        with_git_tokens(None, None, || {
            assert!(git_token().is_none());
        });
    }

    #[test]
    fn git_token_skips_empty_values() {
        with_git_tokens(Some(""), Some("real-tok"), || {
            assert_eq!(git_token(), Some("real-tok".to_string()));
        });
    }

    #[test]
    fn token_rewrite_config_builds_instead_of_pair_for_https() {
        with_git_tokens(Some("my-token"), None, || {
            let Ok(Some((key, value))) = token_rewrite_config("https://github.com/org/repo.git")
            else {
                panic!("expected a rewrite pair");
            };
            // The tokenized URL lives only in the config key; the clean URL is
            // what git stores as remote.origin.url.
            assert_eq!(key, "url.https://my-token@github.com/org/repo.git.insteadOf");
            assert_eq!(value, "https://github.com/org/repo.git");
        });
    }

    #[test]
    fn token_rewrite_config_none_without_token() {
        with_git_tokens(None, None, || {
            let Ok(pair) = token_rewrite_config("https://github.com/org/repo.git") else {
                panic!("token_rewrite_config failed");
            };
            assert!(pair.is_none());
        });
    }

    #[test]
    fn token_rewrite_config_none_for_non_https() {
        with_git_tokens(Some("tok"), None, || {
            let Ok(pair) = token_rewrite_config("ssh://git@github.com/org/repo.git") else {
                panic!("token_rewrite_config failed");
            };
            assert!(pair.is_none());
        });
    }

    #[test]
    fn redact_token_replaces_token_in_message() {
        with_git_tokens(Some("secret123"), None, || {
            let msg = redact_token("fatal: auth failed for secret123@github.com");
            assert!(!msg.contains("secret123"));
            assert!(msg.contains("***"));
        });
    }

    #[test]
    fn redact_token_noop_without_token() {
        with_git_tokens(None, None, || {
            let msg = redact_token("some error message");
            assert_eq!(msg, "some error message");
        });
    }

    #[test]
    fn parse_git_url_branch_only() -> Result<()> {
        let parsed = parse_git_url("git+https://github.com/org/repo.git#main")?;
        assert_eq!(parsed.repo_url, "https://github.com/org/repo.git");
        assert_eq!(parsed.branch, "main");
        assert!(parsed.subdir.is_none());
        Ok(())
    }

    #[test]
    fn parse_git_url_branch_and_subdir() -> Result<()> {
        let parsed = parse_git_url("git+https://github.com/org/repo.git#main:dbt/projects")?;
        assert_eq!(parsed.repo_url, "https://github.com/org/repo.git");
        assert_eq!(parsed.branch, "main");
        assert_eq!(parsed.subdir, Some("dbt/projects"));
        Ok(())
    }

    #[test]
    fn parse_git_url_ssh_with_subdir() -> Result<()> {
        let parsed = parse_git_url("git+ssh://git@github.com/org/repo.git#v2.0:src")?;
        assert_eq!(parsed.repo_url, "ssh://git@github.com/org/repo.git");
        assert_eq!(parsed.branch, "v2.0");
        assert_eq!(parsed.subdir, Some("src"));
        Ok(())
    }

    #[test]
    fn parse_git_url_missing_fragment() {
        let result = parse_git_url("git+https://github.com/org/repo.git");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_with_subdir() -> Result<()> {
        // Create a local git repo with a nested dbt project.
        let repo_dir =
            std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(repo_dir.join("subdir/my_project"))?;
        std::fs::write(repo_dir.join("subdir/my_project/dbt_project.yml"), "name: my_project")?;

        // git init + commit
        let run = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
        };
        run(&["init", "-b", "main"])?;
        run(&["add", "."])?;
        run(&["commit", "-m", "init"])?;

        let file_url = format!("git+file://{}#main:subdir", repo_dir.display());
        let dirs = fetch(&file_url).await?;

        assert_eq!(dirs.len(), 1);
        assert!(dirs[0].ends_with("my_project"));

        std::fs::remove_dir_all(&repo_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn fetch_with_subdir_pointing_at_project() -> Result<()> {
        // Subdir points directly at a dbt project (not a parent of projects).
        let repo_dir =
            std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(repo_dir.join("dbt"))?;
        std::fs::write(repo_dir.join("dbt/dbt_project.yml"), "name: direct_project")?;

        let run = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
        };
        run(&["init", "-b", "main"])?;
        run(&["add", "."])?;
        run(&["commit", "-m", "init"])?;

        let file_url = format!("git+file://{}#main:dbt", repo_dir.display());
        let dirs = fetch(&file_url).await?;

        assert_eq!(dirs.len(), 1);
        assert!(dirs[0].ends_with("dbt"));

        std::fs::remove_dir_all(&repo_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn fetch_with_invalid_subdir() -> Result<()> {
        let repo_dir =
            std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&repo_dir)?;
        std::fs::write(repo_dir.join("dbt_project.yml"), "name: root_project")?;

        let run = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
        };
        run(&["init", "-b", "main"])?;
        run(&["add", "."])?;
        run(&["commit", "-m", "init"])?;

        let file_url = format!("git+file://{}#main:nonexistent/path", repo_dir.display());
        let result = fetch(&file_url).await;
        let Err(err) = result else {
            panic!("expected error");
        };
        assert!(
            err.to_string()
                .contains("subdirectory 'nonexistent/path' not found")
        );

        std::fs::remove_dir_all(&repo_dir).ok();
        Ok(())
    }
}
