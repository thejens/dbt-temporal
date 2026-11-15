use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

use crate::config::DbtTemporalConfig;
use crate::model_store::FetchedProjects;
use crate::project_registry::ProjectRegistry;

use super::project_init::initialize_project;

/// Partition project source entries into local paths and remote URLs.
///
/// Fetches remote sources and returns one [`FetchedProjects`] per entry. Each
/// owns the directory it fetched into, so the caller has to keep them for as
/// long as the projects are in use — see [`ProjectRegistry`].
pub(super) async fn resolve_project_sources(entries: &[String]) -> Result<Vec<FetchedProjects>> {
    let mut sources = Vec::new();

    for entry in entries {
        if crate::config::is_remote_source(entry) {
            info!(url = %entry, "fetching dbt projects from remote source");
            sources.push(crate::model_store::fetch_models(entry).await?);
        } else {
            sources.push(crate::model_store::prepare_local(entry.into()).await?);
        }
    }

    Ok(sources)
}

#[allow(clippy::large_futures)] // Each project initialization loads dbt manifests, producing large futures.
pub(super) async fn build_project_registry(
    project_dirs: &[std::path::PathBuf],
    config: &DbtTemporalConfig,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<ProjectRegistry> {
    let mut projects = BTreeMap::new();

    for project_dir in project_dirs {
        let state = initialize_project(project_dir, config, auth_override.clone())
            .await
            .with_context(|| format!("initializing dbt project at {}", project_dir.display()))?;
        let name = state.project_name.clone();
        if projects.contains_key(&name) {
            anyhow::bail!("duplicate project name '{}' (from {})", name, project_dir.display());
        }
        let dir_name = project_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        info!(project = %name, dir = %project_dir.display(), directory = dir_name, "loaded project");
        if !dir_name.is_empty() && name != dir_name {
            tracing::warn!(
                project_name = %name,
                directory = %dir_name,
                "dbt_project.yml name '{}' differs from directory '{}' — \
                 workflows must use '{}' as the project key",
                name, dir_name, name
            );
        }
        projects.insert(name, Arc::new(state));
    }

    let registry = ProjectRegistry::new(projects);
    let names = registry.project_names();
    info!(projects = ?names, "project registry built ({} project(s))", names.len());

    Ok(registry)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_project_sources_local_only() -> Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-resolve-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;
        std::fs::write(tmp.join("dbt_project.yml"), "name: test")?;

        let entries = vec![tmp.to_string_lossy().to_string()];
        let sources = resolve_project_sources(&entries).await?;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].projects, vec![tmp.clone()]);

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[tokio::test]
    async fn resolve_project_sources_empty() -> Result<()> {
        let entries: Vec<String> = vec![];
        let dirs = resolve_project_sources(&entries).await?;
        assert!(dirs.is_empty());
        Ok(())
    }
}
