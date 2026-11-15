use super::*;
use std::sync::Mutex;

/// Serialize all git-token env var tests to avoid races.
static GIT_ENV_LOCK: Mutex<()> = Mutex::new(());

/// Every variable the git source reads from the environment.
const GIT_ENV_VARS: [&str; 4] = [
    "GITHUB_TOKEN",
    "GIT_TOKEN",
    "GIT_USERNAME",
    "GIT_CONFIG_COUNT",
];

/// RAII guard that restores the git env vars on drop, including on panic,
/// so a failed assertion cannot leak one test's tokens into the next.
#[allow(unsafe_code)]
struct GitEnvGuard(Vec<(&'static str, Option<String>)>);

#[allow(unsafe_code)]
impl Drop for GitEnvGuard {
    fn drop(&mut self) {
        for (var, value) in &self.0 {
            unsafe {
                match value {
                    Some(v) => std::env::set_var(var, v),
                    None => std::env::remove_var(var),
                }
            }
        }
    }
}

/// Run a closure with specific GITHUB_TOKEN and GIT_TOKEN values and every
/// other git env var cleared, restoring all of them after.
#[allow(unsafe_code)]
fn with_git_tokens<F: FnOnce()>(github: Option<&str>, git: Option<&str>, f: F) {
    let Ok(_lock) = GIT_ENV_LOCK.lock() else {
        panic!("GIT_ENV_LOCK poisoned");
    };

    let _restore = GitEnvGuard(
        GIT_ENV_VARS
            .iter()
            .map(|&var| (var, std::env::var(var).ok()))
            .collect(),
    );

    unsafe {
        for var in GIT_ENV_VARS {
            std::env::remove_var(var);
        }
        if let Some(v) = github {
            std::env::set_var("GITHUB_TOKEN", v);
        }
        if let Some(v) = git {
            std::env::set_var("GIT_TOKEN", v);
        }
    }

    f();
}

/// Set an env var inside a `with_git_tokens` closure, which restores it.
#[allow(unsafe_code)]
fn set_git_env(var: &str, value: &str) {
    unsafe { std::env::set_var(var, value) };
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
        let Ok(Some((key, value))) = token_rewrite_config("https://github.com/org/repo.git") else {
            panic!("expected a rewrite pair");
        };
        // The tokenized URL lives only in the config key; the clean URL is
        // what git stores as remote.origin.url.
        assert_eq!(key, "url.https://my-token@github.com/org/repo.git.insteadOf");
        assert_eq!(value, "https://github.com/org/repo.git");
    });
}

#[test]
fn token_rewrite_config_sends_the_token_as_password_with_a_username() {
    with_git_tokens(None, Some("glpat-tok"), || {
        set_git_env("GIT_USERNAME", "oauth2");
        let Ok(Some((key, _))) = token_rewrite_config("https://gitlab.example.com/g/r.git") else {
            panic!("expected a rewrite pair");
        };
        assert_eq!(key, "url.https://oauth2:glpat-tok@gitlab.example.com/g/r.git.insteadOf"); // pragma: allowlist secret
    });
}

#[test]
fn the_token_rewrite_is_appended_to_existing_git_config_env() {
    with_git_tokens(Some("tok"), None, || {
        set_git_env("GIT_CONFIG_COUNT", "2");
        let Ok(cmd) = git_command("https://github.com/org/repo.git") else {
            panic!("git_command failed");
        };
        let envs: std::collections::HashMap<_, _> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| Some((k.to_str()?.to_string(), v?.to_str()?.to_string())))
            .collect();
        assert_eq!(envs["GIT_CONFIG_COUNT"], "3");
        assert_eq!(envs["GIT_CONFIG_KEY_2"], "url.https://tok@github.com/org/repo.git.insteadOf");
        assert!(!envs.contains_key("GIT_CONFIG_KEY_0"), "entry 0 is the deployment's");
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
fn redact_token_replaces_the_url_encoded_form() {
    with_git_tokens(Some("p@ss w/rd%1"), None, || {
        let msg = redact_token(
            "fatal: unable to access 'https://p%40ss%20w%2Frd%1@host/r.git': raw p@ss w/rd%1",
        );
        assert_eq!(msg, "fatal: unable to access 'https://***@host/r.git': raw ***");
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
fn parse_git_url_rev_only() -> Result<()> {
    let parsed = parse_git_url("git+https://github.com/org/repo.git#main")?;
    assert_eq!(parsed.repo_url, "https://github.com/org/repo.git");
    assert_eq!(parsed.rev, "main");
    assert!(parsed.subdir.is_none());
    Ok(())
}

#[test]
fn parse_git_url_rev_and_subdir() -> Result<()> {
    let parsed = parse_git_url("git+https://github.com/org/repo.git#main:dbt/projects")?;
    assert_eq!(parsed.repo_url, "https://github.com/org/repo.git");
    assert_eq!(parsed.rev, "main");
    assert_eq!(parsed.subdir, Some("dbt/projects"));
    Ok(())
}

#[test]
fn parse_git_url_ssh_with_subdir() -> Result<()> {
    let parsed = parse_git_url("git+ssh://git@github.com/org/repo.git#v2.0:src")?;
    assert_eq!(parsed.repo_url, "ssh://git@github.com/org/repo.git");
    assert_eq!(parsed.rev, "v2.0");
    assert_eq!(parsed.subdir, Some("src"));
    Ok(())
}

#[test]
fn parse_git_url_refuses_values_git_would_read_as_options() {
    for url in [
        "git+--upload-pack=touch /tmp/x#main",
        "git+-oProxyCommand=x#main",
        "git+https://github.com/org/repo.git#--upload-pack=x",
        "git+https://github.com/org/repo.git#-b:dir",
    ] {
        let Err(err) = parse_git_url(url) else {
            panic!("{url} must be refused");
        };
        assert!(err.to_string().contains("must not start with '-'"), "{url}: {err}");
    }
}

#[test]
fn parse_git_url_refuses_transports_outside_the_allow_list() {
    for url in [
        "git+ext::sh -c touch% /tmp/x#main",
        "git+git://github.com/org/repo.git#main",
        "git+http://github.com/org/repo.git#main",
        "git+ftp://github.com/org/repo.git#main",
    ] {
        let Err(err) = parse_git_url(url) else {
            panic!("{url} must be refused");
        };
        assert!(err.to_string().contains("not allowed"), "{url}: {err}");
    }
}

#[test]
fn parse_git_url_refuses_scp_style_urls() {
    let Err(err) = parse_git_url("git+git@github.com:org/repo.git#main") else {
        panic!("an scp-style URL has no scheme to check");
    };
    assert!(err.to_string().contains("absolute URL"), "{err}");
}

#[test]
fn git_commands_restrict_the_transports() {
    with_git_tokens(None, None, || {
        let Ok(cmd) = git_command("https://github.com/org/repo.git") else {
            panic!("git_command failed");
        };
        let allowed = cmd
            .as_std()
            .get_envs()
            .find(|(k, _)| *k == "GIT_ALLOW_PROTOCOL")
            .and_then(|(_, v)| v)
            .and_then(|v| v.to_str());
        assert_eq!(allowed, Some("https:ssh:file"));
    });
}

/// A command-running transport must be refused before git runs, so no
/// fetch directory is created for it either.
#[tokio::test]
async fn fetch_refuses_a_disallowed_scheme_before_running_git() {
    let before = temp_model_dirs();
    let err = fetch("git+ext::sh -c 'touch /tmp/dbtt-ext-ran'#main")
        .await
        .expect_err("ext:: must be refused");
    assert!(err.to_string().contains("not allowed"), "{err}");
    assert_eq!(temp_model_dirs(), before);
}

#[test]
fn parse_git_url_missing_fragment() {
    let result = parse_git_url("git+https://github.com/org/repo.git");
    assert!(result.is_err());
}

#[tokio::test]
async fn fetch_with_subdir() -> Result<()> {
    // Create a local git repo with a nested dbt project.
    let repo_dir = std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
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
    let fetched = fetch(&file_url).await?;

    assert_eq!(fetched.projects.len(), 1);
    assert!(fetched.projects[0].ends_with("my_project"));

    std::fs::remove_dir_all(&repo_dir).ok();
    Ok(())
}

#[tokio::test]
async fn fetch_with_subdir_pointing_at_project() -> Result<()> {
    // Subdir points directly at a dbt project (not a parent of projects).
    let repo_dir = std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
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
    let fetched = fetch(&file_url).await?;

    assert_eq!(fetched.projects.len(), 1);
    assert!(fetched.projects[0].ends_with("dbt"));

    std::fs::remove_dir_all(&repo_dir).ok();
    Ok(())
}

#[test]
fn only_full_hex_ids_are_commit_ids() {
    assert!(is_commit_id(&"a".repeat(40)));
    assert!(is_commit_id(&"0123456789abcdefABCDEF".repeat(3)[..64]));
    // Abbreviated ids, or anything else of the right length, stay ref names.
    for rev in [
        "main",
        "v1.2.3",
        "deadbeef",
        &"a".repeat(39),
        &"g".repeat(40),
        &"a".repeat(41),
    ] {
        assert!(!is_commit_id(rev), "{rev} must not be taken for a commit id");
    }
}

/// Run git in `dir` with a fixed identity, failing the test on error.
fn git_in(dir: &Path, args: &[&str]) -> Result<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

/// Pinning a commit is what makes a deploy reproducible: the checkout must
/// be that commit, not whatever the branch has moved on to.
#[tokio::test]
async fn fetch_pins_a_commit_behind_the_branch_tip() -> Result<()> {
    let repo_dir = std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo_dir)?;
    git_in(&repo_dir, &["init", "-b", "main"])?;
    std::fs::write(repo_dir.join("dbt_project.yml"), "name: pinned")?;
    git_in(&repo_dir, &["add", "."])?;
    git_in(&repo_dir, &["commit", "-m", "pinned"])?;
    let pinned = git_in(&repo_dir, &["rev-parse", "HEAD"])?;
    std::fs::write(repo_dir.join("dbt_project.yml"), "name: tip")?;
    git_in(&repo_dir, &["commit", "-am", "tip"])?;

    let fetched = fetch(&format!("git+file://{}#{pinned}", repo_dir.display())).await?;

    assert_eq!(fetched.projects.len(), 1);
    let project = std::fs::read_to_string(fetched.projects[0].join("dbt_project.yml"))?;
    assert_eq!(project, "name: pinned");

    std::fs::remove_dir_all(&repo_dir).ok();
    Ok(())
}

#[tokio::test]
async fn fetch_of_an_unknown_commit_fails() -> Result<()> {
    let repo_dir = std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo_dir)?;
    git_in(&repo_dir, &["init", "-b", "main"])?;
    std::fs::write(repo_dir.join("dbt_project.yml"), "name: p")?;
    git_in(&repo_dir, &["add", "."])?;
    git_in(&repo_dir, &["commit", "-m", "init"])?;

    let missing = "0".repeat(40);
    let err = fetch(&format!("git+file://{}#{missing}", repo_dir.display()))
        .await
        .expect_err("a commit the repository does not have must fail");
    assert!(err.to_string().contains("git fetch failed"), "unexpected error: {err}");

    std::fs::remove_dir_all(&repo_dir).ok();
    Ok(())
}

/// `DBT_PROJECT_DIRS` expands `${VAR}`, so a subdir fragment can arrive from
/// the environment. A `..` in one would point the project scan outside the
/// clone entirely.
#[test]
fn a_subdir_that_climbs_out_of_the_clone_is_refused() {
    let clone = Path::new("/tmp/clone");
    for subdir in ["../elsewhere", "a/../../elsewhere", "/etc"] {
        let err = confined_subdir(clone, subdir).expect_err("a climbing subdir must be refused");
        assert!(
            err.to_string().contains("escapes the cloned repository"),
            "unexpected error for {subdir}: {err}"
        );
    }
}

#[test]
fn an_ordinary_subdir_resolves_under_the_clone() -> Result<()> {
    let clone = Path::new("/tmp/clone");
    assert_eq!(confined_subdir(clone, "dbt")?, clone.join("dbt"));
    assert_eq!(confined_subdir(clone, "./a/b")?, clone.join("a").join("b"));
    assert_eq!(confined_subdir(clone, "")?, clone.to_path_buf());
    Ok(())
}

/// A failed clone must not leave its half-written directory behind: it
/// would sit under a UUID name nothing would ever look at again.
#[tokio::test]
async fn a_failed_clone_leaves_nothing_behind() {
    let before = temp_model_dirs();
    fetch("git+file:///nonexistent-dbtt-repo#main")
        .await
        .expect_err("cloning a repository that does not exist must fail");
    assert_eq!(temp_model_dirs(), before, "the fetch directory must be gone");
}

/// Names of the model-store directories currently in the system temp dir.
fn temp_model_dirs() -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("dbtt-models-"))
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn fetch_with_invalid_subdir() -> Result<()> {
    let repo_dir = std::env::temp_dir().join(format!("dbtt-test-repo-{}", uuid::Uuid::new_v4()));
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
