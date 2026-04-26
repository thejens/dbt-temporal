use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

/// Registry of loaded dbt projects, keyed by project name.
///
/// Shared via `Arc<ProjectRegistry>` on `DbtActivities`.
/// Activities look up the correct `WorkerState` by project name from the workflow input.
pub type ProjectRegistry = Registry<WorkerState>;

/// Generic over the entry type so tests can use a placeholder (`()`) instead of
/// constructing a real `WorkerState` (which depends on the dbt-fusion stack).
pub struct Registry<T> {
    projects: BTreeMap<String, Arc<T>>,
    default_project: Option<String>,
}

impl<T> std::fmt::Debug for Registry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("projects", &self.projects.keys().collect::<Vec<_>>())
            .field("default_project", &self.default_project)
            .finish()
    }
}

impl<T> Registry<T> {
    pub fn new(projects: BTreeMap<String, Arc<T>>) -> Self {
        let default_project = if projects.len() == 1 {
            projects.keys().next().cloned()
        } else {
            None
        };
        Self {
            projects,
            default_project,
        }
    }

    /// Look up a project by name. If `name` is None, use the default (single-project case).
    pub fn get(&self, name: Option<&str>) -> Result<&Arc<T>, DbtTemporalError> {
        let key = name.or(self.default_project.as_deref()).ok_or_else(|| {
            let names: Vec<&str> = self.projects.keys().map(String::as_str).collect();
            DbtTemporalError::ProjectNotFound(format!(
                "no project specified and multiple projects loaded; available projects: {}",
                names.join(", ")
            ))
        })?;
        self.projects.get(key).ok_or_else(|| {
            let names: Vec<&str> = self.projects.keys().map(String::as_str).collect();
            DbtTemporalError::ProjectNotFound(format!(
                "project '{key}' not found; available projects: {}",
                names.join(", ")
            ))
        })
    }

    pub fn project_names(&self) -> Vec<&str> {
        self.projects.keys().map(String::as_str).collect()
    }

    /// Number of loaded projects.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.projects.len()
    }

    /// Whether the registry contains no projects.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.projects.is_empty()
    }

    /// Whether a default project is set (single-project mode).
    #[cfg(test)]
    pub const fn has_default(&self) -> bool {
        self.default_project.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a registry over `()` so tests don't need real `WorkerState` instances.
    /// Genericizing `Registry<T>` over the entry type lets us use `Arc<()>` here, which
    /// replaces the previous `Arc::from_raw` of uninitialized memory hack.
    fn test_registry(names: &[&str]) -> Registry<()> {
        let projects: BTreeMap<String, Arc<()>> = names
            .iter()
            .map(|n| ((*n).to_string(), Arc::new(())))
            .collect();
        Registry::new(projects)
    }

    #[test]
    fn single_project_has_default() {
        let reg = test_registry(&["waffle"]);
        assert_eq!(reg.len(), 1);
        assert!(reg.has_default());
        assert_eq!(reg.project_names(), vec!["waffle"]);
    }

    #[test]
    fn multiple_projects_no_default() {
        let reg = test_registry(&["waffle", "stripe"]);
        assert_eq!(reg.len(), 2);
        assert!(!reg.has_default());
    }

    #[test]
    fn empty_registry() {
        let reg = test_registry(&[]);
        assert_eq!(reg.len(), 0);
        assert!(!reg.has_default());
        assert!(reg.project_names().is_empty());
    }

    #[test]
    fn get_by_name_missing_lists_available() {
        let reg = test_registry(&["waffle", "stripe"]);
        match reg.get(Some("nope")) {
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("nope"));
                assert!(msg.contains("waffle"));
                assert!(msg.contains("stripe"));
            }
            Ok(_) => panic!("expected ProjectNotFound for unknown name 'nope'"),
        }
    }

    #[test]
    fn get_none_with_multiple_projects_errors() {
        let reg = test_registry(&["a", "b"]);
        match reg.get(None) {
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("no project specified"));
                assert!(msg.contains('a'));
                assert!(msg.contains('b'));
            }
            Ok(_) => panic!("expected ProjectNotFound when no name and >1 projects"),
        }
    }

    #[test]
    fn get_none_with_single_project_succeeds() {
        let reg = test_registry(&["only"]);
        assert!(reg.get(None).is_ok());
    }

    #[test]
    fn get_by_name_succeeds() {
        let reg = test_registry(&["waffle", "stripe"]);
        assert!(reg.get(Some("waffle")).is_ok());
        assert!(reg.get(Some("stripe")).is_ok());
    }

    #[test]
    fn project_names_sorted() {
        let reg = test_registry(&["z_proj", "a_proj", "m_proj"]);
        assert_eq!(reg.project_names(), vec!["a_proj", "m_proj", "z_proj"]);
    }

    #[test]
    fn debug_includes_project_names_and_default() {
        let reg = test_registry(&["waffle"]);
        let s = format!("{reg:?}");
        assert!(s.contains("Registry"));
        assert!(s.contains("waffle"));
        assert!(s.contains("default_project"));

        let multi = test_registry(&["a", "b"]);
        let s = format!("{multi:?}");
        assert!(s.contains('a'));
        assert!(s.contains('b'));
        // Multiple projects → no default, formatted as `None`.
        assert!(s.contains("None"));
    }

    #[test]
    fn is_empty_distinguishes_zero_from_nonzero() {
        assert!(test_registry(&[]).is_empty());
        assert!(!test_registry(&["x"]).is_empty());
    }
}
