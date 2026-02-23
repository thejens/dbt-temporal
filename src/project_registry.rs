use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

/// Registry of loaded dbt projects, keyed by project name.
///
/// Shared via `Arc<ProjectRegistry>` on `DbtActivities`.
/// Activities look up the correct `WorkerState` by project name from the workflow input.
pub struct ProjectRegistry {
    projects: BTreeMap<String, Arc<WorkerState>>,
    default_project: Option<String>,
}

impl std::fmt::Debug for ProjectRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectRegistry")
            .field("projects", &self.projects.keys().collect::<Vec<_>>())
            .field("default_project", &self.default_project)
            .finish()
    }
}

impl ProjectRegistry {
    pub fn new(projects: BTreeMap<String, Arc<WorkerState>>) -> Self {
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
    pub fn get(&self, name: Option<&str>) -> Result<&Arc<WorkerState>, DbtTemporalError> {
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

    /// Build a registry with the given project names.
    ///
    /// Uses `MaybeUninit` to avoid needing real `WorkerState` internals.
    /// Returns `ManuallyDrop` because the fake `Arc<WorkerState>` values have
    /// invalid refcounts and must never be dropped.
    #[allow(unsafe_code)]
    fn test_registry(names: &[&str]) -> std::mem::ManuallyDrop<ProjectRegistry> {
        let projects: BTreeMap<String, Arc<WorkerState>> = names
            .iter()
            .map(|n| {
                let raw = Box::into_raw(Box::<WorkerState>::new_uninit());
                let arc = unsafe { Arc::from_raw(raw as *const WorkerState) };
                (n.to_string(), arc)
            })
            .collect();
        std::mem::ManuallyDrop::new(ProjectRegistry::new(projects))
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
            Ok(_) => panic!("expected error"),
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
            Ok(_) => panic!("expected error"),
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
}
