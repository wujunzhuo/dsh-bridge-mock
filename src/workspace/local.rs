use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use async_trait::async_trait;
use uuid::Uuid;

use super::{SessionWorkspace, WorkspaceFactory};

/// Prototype workspace provider backed by temporary local directories.
pub struct LocalWorkspaceFactory {
    root: PathBuf,
    app_dirs: Mutex<HashMap<String, String>>,
}

impl LocalWorkspaceFactory {
    pub fn new() -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!("dsh-bridge-{}", Uuid::new_v4()));
        fs::create_dir(&root).map_err(|error| format!("create workspace root: {error}"))?;
        Ok(Self {
            root,
            app_dirs: Mutex::new(HashMap::new()),
        })
    }

    fn create_session_dir(&self, app_id: &str) -> Result<PathBuf, String> {
        let app_dir = {
            let mut app_dirs = self
                .app_dirs
                .lock()
                .map_err(|_| "workspace app directory lock poisoned".to_string())?;
            app_dirs
                .entry(app_id.to_owned())
                .or_insert_with(|| Uuid::new_v4().to_string())
                .clone()
        };
        let session_dir = self
            .root
            .join(format!("app-{app_dir}"))
            .join("sessions")
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&session_dir)
            .map_err(|error| format!("create session workspace: {error}"))?;
        Ok(session_dir)
    }
}

impl Drop for LocalWorkspaceFactory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct LocalSessionWorkspace {
    path: PathBuf,
}

#[async_trait]
impl SessionWorkspace for LocalSessionWorkspace {
    fn working_directory(&self) -> &Path {
        &self.path
    }

    async fn sync(&self) -> Result<(), String> {
        Ok(())
    }
}

#[async_trait]
impl WorkspaceFactory for LocalWorkspaceFactory {
    async fn create(
        &self,
        app_id: &str,
        _session_id: &str,
    ) -> Result<std::sync::Arc<dyn SessionWorkspace>, String> {
        Ok(std::sync::Arc::new(LocalSessionWorkspace {
            path: self.create_session_dir(app_id)?,
        }))
    }

    async fn delete(&self, workspace: &dyn SessionWorkspace) -> Result<(), String> {
        let path = workspace.working_directory();
        if !path.starts_with(&self.root) {
            return Err("refusing to remove workspace outside temporary root".into());
        }
        match fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("remove session workspace: {error}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use super::LocalWorkspaceFactory;
    use crate::workspace::WorkspaceFactory;

    #[tokio::test]
    async fn sessions_get_isolated_directories_under_the_app() {
        let factory = LocalWorkspaceFactory::new().unwrap();
        let first = factory.create("app-a", "session-a").await.unwrap();
        let second = factory.create("app-a", "session-b").await.unwrap();
        let other_app = factory.create("app-b", "session-a").await.unwrap();

        assert_ne!(first.working_directory(), second.working_directory());
        assert_eq!(
            first.working_directory().parent(),
            second.working_directory().parent()
        );
        assert_ne!(
            first.working_directory().parent().unwrap(),
            other_app.working_directory().parent().unwrap()
        );
    }

    #[tokio::test]
    async fn factory_deletes_a_session_workspace() {
        let factory = LocalWorkspaceFactory::new().unwrap();
        let workspace = factory.create("app-a", "session-a").await.unwrap();
        let path: PathBuf = workspace.working_directory().to_owned();
        factory.delete(workspace.as_ref()).await.unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn dropping_factory_removes_the_temporary_root() {
        let factory = Arc::new(LocalWorkspaceFactory::new().unwrap());
        let root = factory.root.clone();
        drop(factory);
        assert!(!root.exists());
    }
}
