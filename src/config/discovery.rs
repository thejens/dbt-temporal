use std::path::PathBuf;

/// Check whether a project source string is a remote URL (vs a local path).
pub fn is_remote_source(s: &str) -> bool {
    s.starts_with("git+") || s.starts_with("gs://") || s.starts_with("s3://")
}

/// Expand `${VAR_NAME}` references in a string with their environment variable values.
///
/// This enables pinning remote model store URLs to specific versions via env vars:
/// ```text
/// DBT_PROJECT_DIRS=git+https://github.com/org/repo.git#${GIT_COMMIT}
/// ```
fn expand_env_vars(s: &str) -> Result<String, String> {
    use std::sync::LazyLock;
    static RE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")
            .unwrap_or_else(|e| panic!("env var regex is invalid: {e}"))
    });

    let mut result = String::with_capacity(s.len());
    let mut last_end = 0;

    for m in RE.find_iter(s) {
        result.push_str(&s[last_end..m.start()]);
        let var_name = &m.as_str()[2..m.as_str().len() - 1];
        match std::env::var(var_name) {
            Ok(val) => result.push_str(&val),
            Err(_) => {
                return Err(format!(
                    "environment variable '${{{var_name}}}' referenced in project source URL is not set"
                ));
            }
        }
        last_end = m.end();
    }
    result.push_str(&s[last_end..]);
    Ok(result)
}

/// Discover dbt project directories from environment variables or cwd.
///
/// Entries can be local paths or remote URLs. Local paths are validated for
/// `dbt_project.yml`; remote URLs are passed through to be resolved at startup.
pub fn discover_project_dirs() -> Result<Vec<String>, String> {
    // 1. Explicit list: DBT_PROJECT_DIRS=path1,path2,git+https://...
    //    Supports ${VAR} env var substitution for pinning versions in URLs.
    if let Ok(val) = std::env::var("DBT_PROJECT_DIRS") {
        let entries: Vec<String> = val
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if entries.is_empty() {
            return Err("DBT_PROJECT_DIRS is set but contains no entries".to_string());
        }
        // Expand env vars in each entry (e.g. ${GIT_COMMIT} → abc123).
        let entries: Vec<String> = entries
            .into_iter()
            .map(|e| expand_env_vars(&e))
            .collect::<Result<Vec<_>, _>>()?;
        for entry in &entries {
            if !is_remote_source(entry) {
                let dir = PathBuf::from(entry);
                if !dir.join("dbt_project.yml").exists() {
                    return Err(format!("no dbt_project.yml found in {}", dir.display()));
                }
            }
        }
        return Ok(entries);
    }

    // 2. Base directory to scan: DBT_PROJECTS_DIR
    if let Ok(val) = std::env::var("DBT_PROJECTS_DIR") {
        let base = PathBuf::from(val);
        let dirs = scan_for_projects(&base)?;
        if dirs.is_empty() {
            return Err(format!("no dbt projects found in {}", base.display()));
        }
        return Ok(dirs
            .into_iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect());
    }

    // 3. Legacy single-project: DBT_PROJECT_DIR
    if let Ok(val) = std::env::var("DBT_PROJECT_DIR") {
        let dir = PathBuf::from(&val);
        if !dir.join("dbt_project.yml").exists() {
            return Err(format!("no dbt_project.yml found in {}", dir.display()));
        }
        return Ok(vec![val]);
    }

    // 4. Fallback: use cwd
    let cwd =
        std::env::current_dir().map_err(|e| format!("failed to get current directory: {e}"))?;

    if cwd.join("dbt_project.yml").exists() {
        return Ok(vec![cwd.to_string_lossy().to_string()]);
    }

    let dirs = scan_for_projects(&cwd)?;
    if dirs.is_empty() {
        return Err(format!(
            "no dbt projects found in {} (set DBT_PROJECT_DIRS or DBT_PROJECTS_DIR)",
            cwd.display()
        ));
    }
    Ok(dirs
        .into_iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect())
}

/// Scan a directory for immediate subdirs containing `dbt_project.yml`.
pub fn scan_for_projects(base: &PathBuf) -> Result<Vec<PathBuf>, String> {
    let entries = std::fs::read_dir(base)
        .map_err(|e| format!("failed to read directory {}: {e}", base.display()))?;

    let mut dirs: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| format!("failed to read entry in {}: {e}", base.display()))?;
        let path = entry.path();
        if path.is_dir() && path.join("dbt_project.yml").exists() {
            dirs.push(path);
        }
    }

    dirs.sort();
    Ok(dirs)
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    static CFG_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce() -> anyhow::Result<()>>(
        vars: &[(&str, Option<&str>)],
        f: F,
    ) -> anyhow::Result<()> {
        let _guard = CFG_ENV_LOCK.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        let saved: Vec<(&str, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        let result = f();
        for (k, orig) in &saved {
            match orig {
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        result
    }

    // --- scan_for_projects ---

    #[test]
    fn scan_for_projects_finds_subdirs() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-test-scan-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(tmp.join("project_a"))?;
        std::fs::write(tmp.join("project_a/dbt_project.yml"), "name: a")?;
        std::fs::create_dir_all(tmp.join("project_b"))?;
        std::fs::write(tmp.join("project_b/dbt_project.yml"), "name: b")?;
        // A dir without dbt_project.yml should be excluded.
        std::fs::create_dir_all(tmp.join("not_a_project"))?;

        let dirs = scan_for_projects(&tmp).map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(dirs.len(), 2);
        assert!(dirs[0].ends_with("project_a"));
        assert!(dirs[1].ends_with("project_b"));

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn scan_for_projects_empty_dir() -> anyhow::Result<()> {
        let tmp =
            std::env::temp_dir().join(format!("dbtt-test-scan-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;

        let dirs = scan_for_projects(&tmp).map_err(|e| anyhow::anyhow!("{e}"))?;
        assert!(dirs.is_empty());

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn scan_for_projects_nonexistent_dir() {
        let tmp = PathBuf::from("/tmp/dbtt-test-scan-nonexistent-99999999");
        assert!(scan_for_projects(&tmp).is_err());
    }

    // --- discover_project_dirs ---

    #[test]
    fn discover_dirs_from_dbt_project_dirs() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-discover-dirs-{}", uuid::Uuid::new_v4()));
        let proj_a = tmp.join("a");
        let proj_b = tmp.join("b");
        std::fs::create_dir_all(&proj_a)?;
        std::fs::create_dir_all(&proj_b)?;
        std::fs::write(proj_a.join("dbt_project.yml"), "name: a")?;
        std::fs::write(proj_b.join("dbt_project.yml"), "name: b")?;

        with_env(
            &[
                ("DBT_PROJECT_DIRS", Some(&format!("{},{}", proj_a.display(), proj_b.display()))),
                ("DBT_PROJECTS_DIR", None),
                ("DBT_PROJECT_DIR", None),
            ],
            || {
                let dirs = discover_project_dirs().map_err(|e| anyhow::anyhow!("{e}"))?;
                assert_eq!(dirs.len(), 2);
                Ok(())
            },
        )?;

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn discover_dirs_from_dbt_project_dirs_empty_errors() -> anyhow::Result<()> {
        with_env(
            &[
                ("DBT_PROJECT_DIRS", Some("")),
                ("DBT_PROJECTS_DIR", None),
                ("DBT_PROJECT_DIR", None),
            ],
            || {
                assert!(discover_project_dirs().is_err());
                Ok(())
            },
        )
    }

    #[test]
    fn discover_dirs_from_dbt_project_dirs_missing_yml_errors() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-discover-noml-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;

        with_env(
            &[
                ("DBT_PROJECT_DIRS", Some(&tmp.display().to_string())),
                ("DBT_PROJECTS_DIR", None),
                ("DBT_PROJECT_DIR", None),
            ],
            || {
                let Err(err) = discover_project_dirs() else {
                    anyhow::bail!("expected error for missing dbt_project.yml");
                };
                assert!(err.contains("no dbt_project.yml"));
                Ok(())
            },
        )?;

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn discover_dirs_from_dbt_project_dirs_with_remote_urls() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-discover-mix-{}", uuid::Uuid::new_v4()));
        let local = tmp.join("local_proj");
        std::fs::create_dir_all(&local)?;
        std::fs::write(local.join("dbt_project.yml"), "name: local")?;

        let entries = format!("{},git+https://github.com/org/repo.git#main:dbt", local.display());

        with_env(
            &[
                ("DBT_PROJECT_DIRS", Some(&entries)),
                ("DBT_PROJECTS_DIR", None),
                ("DBT_PROJECT_DIR", None),
            ],
            || {
                let dirs = discover_project_dirs().map_err(|e| anyhow::anyhow!("{e}"))?;
                assert_eq!(dirs.len(), 2);
                assert_eq!(dirs[0], local.to_string_lossy().to_string());
                assert_eq!(dirs[1], "git+https://github.com/org/repo.git#main:dbt");
                Ok(())
            },
        )?;

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn discover_dirs_from_dbt_projects_dir() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-discover-base-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(tmp.join("proj"))?;
        std::fs::write(tmp.join("proj/dbt_project.yml"), "name: proj")?;

        with_env(
            &[
                ("DBT_PROJECT_DIRS", None),
                ("DBT_PROJECTS_DIR", Some(&tmp.display().to_string())),
                ("DBT_PROJECT_DIR", None),
            ],
            || {
                let dirs = discover_project_dirs().map_err(|e| anyhow::anyhow!("{e}"))?;
                assert_eq!(dirs.len(), 1);
                Ok(())
            },
        )?;

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn discover_dirs_from_dbt_projects_dir_empty_errors() -> anyhow::Result<()> {
        let tmp =
            std::env::temp_dir().join(format!("dbtt-discover-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;

        with_env(
            &[
                ("DBT_PROJECT_DIRS", None),
                ("DBT_PROJECTS_DIR", Some(&tmp.display().to_string())),
                ("DBT_PROJECT_DIR", None),
            ],
            || {
                assert!(discover_project_dirs().is_err());
                Ok(())
            },
        )?;

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn discover_dirs_from_dbt_project_dir_legacy() -> anyhow::Result<()> {
        let tmp =
            std::env::temp_dir().join(format!("dbtt-discover-legacy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;
        std::fs::write(tmp.join("dbt_project.yml"), "name: x")?;

        with_env(
            &[
                ("DBT_PROJECT_DIRS", None),
                ("DBT_PROJECTS_DIR", None),
                ("DBT_PROJECT_DIR", Some(&tmp.display().to_string())),
            ],
            || {
                let dirs = discover_project_dirs().map_err(|e| anyhow::anyhow!("{e}"))?;
                assert_eq!(dirs, vec![tmp.to_string_lossy().to_string()]);
                Ok(())
            },
        )?;

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    // --- expand_env_vars ---

    #[test]
    fn expand_env_vars_single_var() -> anyhow::Result<()> {
        with_env(&[("DBTT_TEST_COMMIT", Some("abc123"))], || {
            let result = expand_env_vars("git+https://github.com/org/repo.git#${DBTT_TEST_COMMIT}")
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            assert_eq!(result, "git+https://github.com/org/repo.git#abc123");
            Ok(())
        })
    }

    #[test]
    fn expand_env_vars_multiple_vars() -> anyhow::Result<()> {
        with_env(
            &[
                ("DBTT_TEST_COMMIT_A", Some("aaa111")),
                ("DBTT_TEST_COMMIT_B", Some("bbb222")),
            ],
            || {
                let result = expand_env_vars("${DBTT_TEST_COMMIT_A}-${DBTT_TEST_COMMIT_B}")
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                assert_eq!(result, "aaa111-bbb222");
                Ok(())
            },
        )
    }

    #[test]
    fn expand_env_vars_no_vars() -> anyhow::Result<()> {
        let result = expand_env_vars("git+https://github.com/org/repo.git#main")
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(result, "git+https://github.com/org/repo.git#main");
        Ok(())
    }

    #[test]
    fn expand_env_vars_missing_var_errors() -> anyhow::Result<()> {
        with_env(&[("DBTT_TEST_MISSING_VAR_XYZ", None)], || {
            let result =
                expand_env_vars("git+https://github.com/org/repo.git#${DBTT_TEST_MISSING_VAR_XYZ}");
            let Err(err) = result else {
                anyhow::bail!("expected error for missing env var");
            };
            assert!(err.contains("DBTT_TEST_MISSING_VAR_XYZ"));
            Ok(())
        })
    }

    #[test]
    fn expand_env_vars_empty_value_is_ok() -> anyhow::Result<()> {
        with_env(&[("DBTT_TEST_EMPTY", Some(""))], || {
            let result = expand_env_vars("prefix-${DBTT_TEST_EMPTY}-suffix")
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            assert_eq!(result, "prefix--suffix");
            Ok(())
        })
    }

    #[test]
    fn expand_env_vars_preserves_dollar_without_braces() -> anyhow::Result<()> {
        let result = expand_env_vars("$HOME/path").map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(result, "$HOME/path");
        Ok(())
    }

    // --- is_remote_source ---

    #[test]
    fn is_remote_source_git_https() {
        assert!(is_remote_source("git+https://github.com/org/repo.git#main"));
    }

    #[test]
    fn is_remote_source_git_ssh() {
        assert!(is_remote_source("git+ssh://git@github.com/org/repo.git#main"));
    }

    #[test]
    fn is_remote_source_gcs() {
        assert!(is_remote_source("gs://my-bucket/prefix"));
    }

    #[test]
    fn is_remote_source_s3() {
        assert!(is_remote_source("s3://my-bucket/prefix"));
    }

    #[test]
    fn is_remote_source_local_path() {
        assert!(!is_remote_source("/home/user/project"));
        assert!(!is_remote_source("./relative/path"));
        assert!(!is_remote_source("project"));
    }
}
