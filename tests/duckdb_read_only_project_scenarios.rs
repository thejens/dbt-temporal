//! Loading a project the worker cannot write to, through dbt's real loader.
//!
//! dbt appends `dbt_internal_packages/` to a project's `.gitignore` on every
//! load that finds it missing, and fails the load when the write does.
//! `model_store::prepare_local` stages a copy for exactly that case; these
//! scenarios check that dbt accepts the copy, and that the premise still
//! holds: once dbt stops failing on the read-only original, the first
//! scenario fails and the staging can go.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::os::unix::fs::PermissionsExt as _;

use dbt_temporal::model_store::prepare_local;
use dbt_temporal::worker::initialize_project;
use tempfile::TempDir;

struct Project {
    dir: TempDir,
    profiles: TempDir,
    _db: TempDir,
}

/// A one-model DuckDB project whose `.gitignore` lacks the line dbt adds, made
/// read-only. `None` when mode bits do not stop writes (running as root).
fn read_only_project() -> Option<Project> {
    let dir = TempDir::new().unwrap();
    let profiles = TempDir::new().unwrap();
    let db = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("dbt_project.yml"),
        "name: ro\nversion: \"1.0.0\"\nprofile: ro\n",
    )
    .unwrap();
    std::fs::create_dir(dir.path().join("models")).unwrap();
    std::fs::write(dir.path().join("models/m.sql"), "select 1 as id").unwrap();
    std::fs::write(
        profiles.path().join("profiles.yml"),
        format!(
            "ro:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: \"{}\"\n      \
             schema: main\n      threads: 1\n",
            db.path().join("ro.duckdb").display()
        ),
    )
    .unwrap();

    let gitignore = dir.path().join(".gitignore");
    std::fs::write(&gitignore, "target/\n").unwrap();
    std::fs::set_permissions(&gitignore, std::fs::Permissions::from_mode(0o444)).unwrap();
    if std::fs::OpenOptions::new()
        .write(true)
        .open(&gitignore)
        .is_ok()
    {
        return None;
    }
    Some(Project {
        dir,
        profiles,
        _db: db,
    })
}

#[tokio::test]
async fn dbt_cannot_load_the_read_only_project_in_place() {
    let Some(project) = read_only_project() else {
        return;
    };
    let config = common::duckdb::build_config(project.dir.path(), project.profiles.path());

    let result = initialize_project(project.dir.path(), &config, None).await;

    assert!(
        result.is_err(),
        "dbt loaded a project whose .gitignore it could not update: the read-only \
         staging in model_store::local may no longer be needed"
    );
}

#[tokio::test]
async fn the_staged_copy_loads() {
    let Some(project) = read_only_project() else {
        return;
    };
    let config = common::duckdb::build_config(project.dir.path(), project.profiles.path());

    let prepared = prepare_local(project.dir.path().to_path_buf())
        .await
        .unwrap();
    let [staged] = prepared.projects.as_slice() else {
        panic!("expected one project, got {:?}", prepared.projects);
    };
    assert_ne!(staged, project.dir.path(), "a read-only project must be staged");

    let state = initialize_project(staged, &config, None)
        .await
        .expect("dbt loads the staged copy");
    assert_eq!(state.project_name, "ro");
    assert_eq!(
        std::fs::read_to_string(project.dir.path().join(".gitignore")).unwrap(),
        "target/\n",
        "the original stays untouched"
    );
}
