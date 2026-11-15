use anyhow::{Context, Result};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use tracing::info;

use super::FetchedProjects;

/// How long a clone may take before the worker gives up on it.
///
/// A worker that cannot start is visible; a worker stuck in `git clone` looks
/// like a slow start-up forever. Generous enough for a large shallow clone over
/// a slow link, short enough that an unreachable host or a prompt nobody can
/// answer surfaces as an error.
const CLONE_TIMEOUT: Duration = Duration::from_mins(10);

/// Transports a project source may use, in `GIT_ALLOW_PROTOCOL` syntax.
///
/// git also speaks transports that run a command (`ext::`) or reach
/// unauthenticated daemons (`git://`); none of that belongs in a project
/// source. `file` is the local-mirror case. The same list gates the URL up
/// front (so a bad scheme is a configuration error, not git's stderr) and is
/// handed to git itself, which also applies it to submodule and redirect
/// transports the URL check cannot see.
const ALLOWED_SCHEMES: [&str; 3] = ["https", "ssh", "file"];

/// Parsed components of a git model store URL.
struct GitUrl<'a> {
    repo_url: &'a str,
    /// Branch, tag, or full commit id.
    rev: &'a str,
    subdir: Option<&'a str>,
}

/// Parse a git model store URL into its components.
///
/// Accepted formats (after stripping the `git+` prefix):
/// - `https://github.com/org/repo.git#rev`
/// - `https://github.com/org/repo.git#rev:path/to/subdir`
///
/// `rev` is a branch, a tag, or a full commit id.
fn parse_git_url(url: &str) -> Result<GitUrl<'_>> {
    let url = url.strip_prefix("git+").unwrap_or(url);
    let (repo_url, fragment) = url
        .rsplit_once('#')
        .ok_or_else(|| anyhow::anyhow!("git model store URL must have a #rev suffix: {url}"))?;

    let (rev, subdir) = match fragment.split_once(':') {
        Some((r, s)) => (r, Some(s)),
        None => (fragment, None),
    };

    // git reads a leading `-` as an option wherever it appears among
    // positional arguments; `--` guards the argv, this refuses the value
    // before it gets there.
    for (what, value) in [("repository URL", repo_url), ("rev", rev)] {
        if value.starts_with('-') {
            anyhow::bail!("git model store {what} must not start with '-': {value}");
        }
    }
    let scheme = url::Url::parse(repo_url)
        .map(|u| u.scheme().to_string())
        .with_context(|| {
            format!("git model store URL must be an absolute URL such as https://host/repo.git: {repo_url}")
        })?;
    if !ALLOWED_SCHEMES.contains(&scheme.as_str()) {
        anyhow::bail!(
            "git model store URL scheme '{scheme}' is not allowed (use one of {}): {repo_url}",
            ALLOWED_SCHEMES.join(", ")
        );
    }

    Ok(GitUrl {
        repo_url,
        rev,
        subdir,
    })
}

/// Whether `rev` is a full commit id: 40 hex digits (SHA-1) or 64 (SHA-256).
///
/// Abbreviated ids are deliberately not recognised. The fetch protocol only
/// resolves full ids, and a short hex string is just as valid a branch name —
/// `#deadbeef` keeps meaning the branch or tag of that name.
fn is_commit_id(rev: &str) -> bool {
    matches!(rev.len(), 40 | 64) && rev.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Fetch dbt project(s) by cloning a git repository.
///
/// URL format: `git+https://github.com/org/repo.git#rev[:subdir]`
///
/// Authentication token is read from `GITHUB_TOKEN`, then `GIT_TOKEN` (first non-empty wins),
/// and sent as the username unless `GIT_USERNAME` is set, in which case it is the password.
/// The token reaches git through `GIT_CONFIG_*` env vars (a `url.<with-token>.insteadOf`
/// rewrite) rather than the clone URL itself — an argv-embedded token is visible in the
/// process list while git runs, and git persists the remote URL verbatim into the cloned
/// repo's `.git/config`.
pub async fn fetch(url: &str) -> Result<FetchedProjects> {
    let parsed = parse_git_url(url)?;

    // Owned from here on: every `?` below removes the directory rather than
    // leaving a half-cloned repository behind.
    let dir = super::fetch_dir()?;
    let dest = dir.path();

    info!(rev = parsed.rev, dest = %dest.display(), "cloning model store from git");

    // One deadline for the whole checkout: a commit id takes several git
    // invocations, and it is the total a stuck start-up has to be bounded by.
    tokio::time::timeout(CLONE_TIMEOUT, checkout(&parsed, dest))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "git clone of {} timed out after {}s",
                redact_token(parsed.repo_url),
                CLONE_TIMEOUT.as_secs()
            )
        })??;

    info!(dest = %dest.display(), "git clone complete");

    let scan_root = match parsed.subdir {
        Some(sub) => {
            let root = confined_subdir(dest, sub)?;
            if !root.is_dir() {
                anyhow::bail!("subdirectory '{sub}' not found in cloned repository");
            }
            root
        }
        None => dest.to_path_buf(),
    };

    let projects = super::scan_for_projects(&scan_root)?;
    Ok(FetchedProjects::owned(dir, projects))
}

/// Check out `parsed.rev` into `dest` with history depth 1.
///
/// `git clone --branch` only takes branch and tag names, so a commit id is
/// fetched by id into a fresh repository instead. That relies on the server
/// serving unadvertised commits, which protocol v2 (git's default) and the
/// major hosts do.
async fn checkout(parsed: &GitUrl<'_>, dest: &Path) -> Result<()> {
    if !is_commit_id(parsed.rev) {
        let mut clone = git_command(parsed.repo_url)?;
        clone
            .args([
                "clone",
                "--depth",
                "1",
                "--branch",
                parsed.rev,
                "--",
                parsed.repo_url,
            ])
            .arg(dest);
        return run_git(clone, "clone").await;
    }

    let mut init = git_command(parsed.repo_url)?;
    init.args(["init", "--quiet"]).arg(dest);
    run_git(init, "init").await?;

    // Fetch from the URL rather than a configured remote, so the repository
    // never records one; the token rewrite applies to the URL either way.
    let mut fetch = git_command(parsed.repo_url)?;
    fetch.arg("-C").arg(dest).args([
        "fetch",
        "--quiet",
        "--depth",
        "1",
        "--",
        parsed.repo_url,
        parsed.rev,
    ]);
    run_git(fetch, "fetch").await?;

    let mut switch = git_command(parsed.repo_url)?;
    switch
        .arg("-C")
        .arg(dest)
        .args(["checkout", "--quiet", "--detach", "FETCH_HEAD"]);
    run_git(switch, "checkout").await
}

/// A `git` invocation set up for an unattended worker, authenticated for
/// `repo_url` when a token is configured.
fn git_command(repo_url: &str) -> Result<tokio::process::Command> {
    let mut cmd = tokio::process::Command::new("git");
    // Nobody is at a terminal. Without this a private repo with no usable
    // token leaves git waiting on a username prompt until something kills the
    // worker, which reads as a hang rather than an authentication failure.
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ALLOW_PROTOCOL", ALLOWED_SCHEMES.join(":"))
        .env("GIT_ASKPASS", "")
        .env("SSH_ASKPASS", "")
        // Kill the child if its future is dropped — the checkout timeout does
        // exactly that, and an orphaned git would keep writing into a
        // directory the worker is about to remove.
        .kill_on_drop(true);
    if let Some((key, value)) = token_rewrite_config(repo_url)? {
        // Append to whatever `GIT_CONFIG_*` entries the deployment sets itself
        // (an extra header, a CA bundle) instead of overwriting the first one.
        let index = std::env::var("GIT_CONFIG_COUNT")
            .ok()
            .and_then(|n| n.trim().parse::<usize>().ok())
            .unwrap_or(0);
        cmd.env("GIT_CONFIG_COUNT", (index + 1).to_string())
            .env(format!("GIT_CONFIG_KEY_{index}"), key)
            .env(format!("GIT_CONFIG_VALUE_{index}"), value);
    }
    Ok(cmd)
}

/// Run a git command to completion, failing with its (token-redacted) stderr.
async fn run_git(mut cmd: tokio::process::Command, what: &str) -> Result<()> {
    let output = cmd
        .output()
        .await
        .with_context(|| format!("running git {what}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git {what} failed: {}", redact_token(&stderr));
    }
    Ok(())
}

/// Resolve a URL's `:subdir` fragment inside the clone.
///
/// The fragment comes from configuration, but configuration is not always
/// written by hand — `DBT_PROJECT_DIRS` supports `${VAR}` expansion, so the
/// value can arrive from the environment. A `..` in it would point the project
/// scan somewhere outside the clone entirely, so it is refused rather than
/// normalized away.
fn confined_subdir(clone: &Path, subdir: &str) -> Result<PathBuf> {
    let mut out = clone.to_path_buf();
    for component in Path::new(subdir).components() {
        match component {
            Component::Normal(segment) => out.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("subdirectory '{subdir}' escapes the cloned repository")
            }
        }
    }
    Ok(out)
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

/// The username to send with the token, from `GIT_USERNAME`.
///
/// Hosts disagree on where a token goes. GitHub and Gitea accept it as the
/// username, which is the default; GitLab (`oauth2`), Bitbucket
/// (`x-token-auth`) and Azure DevOps (any name) want it as the password.
fn git_username() -> Option<String> {
    std::env::var("GIT_USERNAME").ok().filter(|u| !u.is_empty())
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
    let set = match git_username() {
        Some(user) => with_token
            .set_username(&user)
            .and_then(|()| with_token.set_password(Some(&token))),
        None => with_token.set_username(&token),
    };
    set.map_err(|()| anyhow::anyhow!("failed to set git token in URL"))?;
    Ok(Some((format!("url.{with_token}.insteadOf"), repo_url.to_string())))
}

/// Redact any git token from error output.
///
/// git echoes URLs in the form it was given them, and the token reaches it
/// percent-encoded (`@` as `%40`), so both spellings are replaced. The encoded
/// one goes first: encoding only lengthens a token, and replacing the raw form
/// first could cut into the encoded one and leave a fragment behind.
fn redact_token(msg: &str) -> String {
    let Some(token) = git_token() else {
        return msg.to_string();
    };
    let mut redacted = msg.to_string();
    if let Some(encoded) = url_encoded(&token) {
        redacted = redacted.replace(&encoded, "***");
    }
    redacted.replace(&token, "***")
}

/// `value` as it is spelled once placed in a URL's userinfo.
fn url_encoded(value: &str) -> Option<String> {
    let mut url = url::Url::parse("https://host/").ok()?;
    url.set_username(value).ok()?;
    Some(url.username().to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
