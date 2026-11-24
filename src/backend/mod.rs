pub mod subprocess;

use std::sync::Arc;

use async_trait::async_trait;

use crate::{config::ModelConfig, protocol::RpcFrame, session::SessionPublisher};

#[async_trait]
pub trait SessionBackend: Send + Sync {
    fn is_running(&self) -> bool;
    async fn send(&self, frame: RpcFrame) -> Result<(), String>;
    async fn shutdown(&self);
}

#[async_trait]
pub trait SessionBackendFactory: Send + Sync {
    async fn start(
        &self,
        tenant_id: &str,
        session_id: &str,
        model: &ModelConfig,
        output: Arc<dyn SessionPublisher>,
    ) -> Result<Arc<dyn SessionBackend>, String>;
}
