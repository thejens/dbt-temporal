//! Local project directories: loaded in place, unless dbt would have to write
//! to one it cannot.

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use tracing::info;

use dbt_common::constants::DBT_INTERNAL_PACKAGES_DIR_NAME;

use super::FetchedProjects;

/// A local project directory, ready for dbt to load.
///
/// Normally that is the directory itself. The exception is the one write dbt's
/// loader makes into the project on every load: when the project has a
/// `.gitignore` that does not list `dbt_internal_packages/`, the loader appends
/// the line, and fails the whole load if it cannot. A project baked into an
/// image run with a read-only root filesystem (the Helm chart's default), or
/// mounted from a ConfigMap (always read-only), hits exactly that. Such a
/// project is copied to a temp directory this value owns, and loaded from
/// there. Everything else dbt writes goes to the resolve output directory,
/// which is already under the temp dir.
pub async fn prepare(dir: PathBuf) -> Result<FetchedProjects> {
    tokio::task::spawn_blocking(move || prepare_blocking(&dir))
        .await
        .context("preparing local project directory")?
}

fn prepare_blocking(dir: &Path) -> Result<FetchedProjects> {
    if !loader_cannot_write(dir)? {
        return Ok(FetchedProjects::borrowed(vec![dir.to_path_buf()]));
    }

    let staged = tempfile::Builder::new()
        .prefix("dbtt-project-")
        .tempdir()
        .context("creating staging directory for a read-only project")?;
    let project = staged.path().join(
        dir.file_name()
            .context("read-only project path has no directory name")?,
    );
    copy_tree(dir, &project, &mut HashSet::new()).with_context(|| {
        format!("copying read-only project {} to {}", dir.display(), project.display())
    })?;
    info!(
        from = %dir.display(),
        to = %project.display(),
        "project directory is read-only and dbt writes to its .gitignore: loading a copy"
    );
    Ok(FetchedProjects::owned(staged, vec![project]))
}

/// Whether dbt's load would try to append to `.gitignore` and fail.
///
/// Mirrors the loader's own condition, so a project that is read-only but
/// needs no write is still loaded in place. The probe opens the file for
/// writing without truncating or writing anything.
fn loader_cannot_write(dir: &Path) -> Result<bool> {
    let gitignore = dir.join(".gitignore");
    let content = match std::fs::read_to_string(&gitignore) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e).with_context(|| format!("reading {}", gitignore.display())),
    };
    if content.contains(&format!("{DBT_INTERNAL_PACKAGES_DIR_NAME}/")) {
        return Ok(false);
    }
    match std::fs::OpenOptions::new().write(true).open(&gitignore) {
        Ok(_) => Ok(false),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ReadOnlyFilesystem | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            Ok(true)
        }
        Err(e) => Err(e).with_context(|| format!("opening {} for writing", gitignore.display())),
    }
}

/// Copy `from` into a new directory `to`, following symlinks.
///
/// Following them matters: a ConfigMap volume is a tree of symlinks into a
/// hidden `..data` directory, and copying the links would copy pointers back
/// into the read-only mount. `ancestors` holds the canonical paths of the
/// directories being copied above this one, so a link back up the tree is cut
/// instead of copied forever, while two links to the same directory elsewhere
/// both still get copied. `.git` is skipped: it is never project input, and can
/// be large.
fn copy_tree(from: &Path, to: &Path, ancestors: &mut HashSet<PathBuf>) -> Result<()> {
    let canonical = std::fs::canonicalize(from)?;
    if ancestors.contains(&canonical) {
        return Ok(());
    }
    std::fs::create_dir(to)?;
    ancestors.insert(canonical.clone());
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        if entry.file_name() == ".git" {
            continue;
        }
        let source = entry.path();
        let target = to.join(entry.file_name());
        // fs::metadata, not entry.metadata: resolve symlinks to what they name.
        let metadata =
            std::fs::metadata(&source).with_context(|| format!("reading {}", source.display()))?;
        if metadata.is_dir() {
            copy_tree(&source, &target, ancestors)?;
        } else if metadata.is_file() {
            std::fs::copy(&source, &target)
                .with_context(|| format!("copying {}", source.display()))?;
            // fs::copy carries the mode over, and a read-only file would leave
            // the copy as unwritable as the original.
            let mut permissions = metadata.permissions();
            permissions.set_mode(permissions.mode() | 0o200);
            std::fs::set_permissions(&target, permissions)?;
        }
    }
    ancestors.remove(&canonical);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn project(gitignore: Option<&str>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dbt_project.yml"), "name: p").unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(dir.path().join("models/m.sql"), "select 1").unwrap();
        if let Some(content) = gitignore {
            std::fs::write(dir.path().join(".gitignore"), content).unwrap();
        }
        dir
    }

    /// Makes the file read-only. Returns false when that does not stop writes
    /// (running as root, which ignores mode bits): the test has nothing to
    /// check then.
    fn make_read_only(path: &Path) -> bool {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444)).unwrap();
        std::fs::OpenOptions::new().write(true).open(path).is_err()
    }

    #[test]
    fn writable_project_loads_in_place() {
        let dir = project(Some("target/\n"));
        let prepared = prepare_blocking(dir.path()).unwrap();
        assert_eq!(prepared.projects, vec![dir.path().to_path_buf()]);
    }

    #[test]
    fn project_without_gitignore_loads_in_place() {
        let dir = project(None);
        assert!(!loader_cannot_write(dir.path()).unwrap());
    }

    #[test]
    fn read_only_gitignore_that_already_lists_the_dir_loads_in_place() {
        let dir = project(Some("target/\ndbt_internal_packages/\n"));
        if !make_read_only(&dir.path().join(".gitignore")) {
            return;
        }
        assert!(!loader_cannot_write(dir.path()).unwrap());
    }

    #[test]
    fn read_only_gitignore_needing_the_line_is_staged() {
        let dir = project(Some("target/\n"));
        let gitignore = dir.path().join(".gitignore");
        if !make_read_only(&gitignore) {
            return;
        }
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::os::unix::fs::symlink(dir.path(), dir.path().join("models/loop")).unwrap();
        std::fs::create_dir(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/s.sql"), "select 2").unwrap();
        std::os::unix::fs::symlink(dir.path().join("shared"), dir.path().join("models/shared"))
            .unwrap();

        let prepared = prepare_blocking(dir.path()).unwrap();
        let [staged] = prepared.projects.as_slice() else {
            panic!("expected one project, got {:?}", prepared.projects);
        };
        assert_ne!(staged, dir.path());
        assert_eq!(staged.file_name(), dir.path().file_name());
        assert_eq!(std::fs::read_to_string(staged.join("models/m.sql")).unwrap(), "select 1");
        // The copy is what dbt writes to, so it must be writable.
        std::fs::OpenOptions::new()
            .write(true)
            .open(staged.join(".gitignore"))
            .unwrap();
        assert!(!staged.join(".git").exists());
        // The link back up the tree is cut, not followed forever...
        assert!(!staged.join("models/loop").exists());
        // ...while a directory reached twice is copied at both places.
        assert_eq!(std::fs::read_to_string(staged.join("shared/s.sql")).unwrap(), "select 2");
        assert_eq!(
            std::fs::read_to_string(staged.join("models/shared/s.sql")).unwrap(),
            "select 2"
        );
        // The original is untouched.
        assert_eq!(std::fs::read_to_string(&gitignore).unwrap(), "target/\n");
    }

    #[test]
    fn staged_copy_is_removed_with_its_owner() {
        let dir = project(Some(""));
        if !make_read_only(&dir.path().join(".gitignore")) {
            return;
        }
        let prepared = prepare_blocking(dir.path()).unwrap();
        let staged = prepared.projects[0].clone();
        assert!(staged.exists());
        drop(prepared);
        assert!(!staged.exists());
    }
}
