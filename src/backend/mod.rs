pub mod dsh;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::{
    config::ModelConfig,
    protocol::{RpcFrame, SessionEvent},
};

#[async_trait]
pub trait SessionBackend: Send + Sync {
    fn session_id(&self) -> &str;
    fn is_running(&self) -> bool;
    async fn send(&self, frame: RpcFrame) -> Result<(), String>;
    fn subscribe(&self) -> broadcast::Receiver<RpcFrame>;
    fn publish(&self, frame: RpcFrame);
    async fn history(&self, after_event_id: Option<&str>) -> Vec<SessionEvent>;
    async fn shutdown(&self);
}

#[async_trait]
pub trait SessionBackendFactory: Send + Sync {
    async fn start(
        &self,
        tenant_id: &str,
        session_id: &str,
        model: &ModelConfig,
    ) -> Result<Arc<dyn SessionBackend>, String>;
}
