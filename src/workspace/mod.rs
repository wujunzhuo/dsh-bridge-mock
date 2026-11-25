mod local;

pub use local::LocalWorkspaceFactory;

use std::path::Path;

use async_trait::async_trait;

#[async_trait]
pub trait SessionWorkspace: Send + Sync {
    /// Local mount point used as the agent process working directory.
    fn working_directory(&self) -> &Path;

    /// Persist local changes to the backing workspace, if needed.
    async fn sync(&self) -> Result<(), String>;
}

#[async_trait]
pub trait WorkspaceFactory: Send + Sync {
    async fn create(
        &self,
        app_id: &str,
        session_id: &str,
    ) -> Result<std::sync::Arc<dyn SessionWorkspace>, String>;

    async fn delete(&self, workspace: &dyn SessionWorkspace) -> Result<(), String>;
}
