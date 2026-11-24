mod in_memory;

pub use in_memory::InMemorySessionOutputFactory;

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::protocol::{RpcFrame, SessionEvent};

#[async_trait]
pub trait SessionOutput: Send + Sync {
    fn publisher(&self) -> Arc<dyn SessionPublisher>;
    fn subscribe(&self) -> broadcast::Receiver<RpcFrame>;
    async fn history(&self, after_event_id: Option<&str>) -> Vec<SessionEvent>;
}

#[async_trait]
pub trait SessionPublisher: Send + Sync {
    fn publish(&self, frame: RpcFrame);
    async fn publish_event(&self, session_id: &str, event: Value);
}

#[async_trait]
pub trait SessionOutputFactory: Send + Sync {
    async fn create(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> Result<Arc<dyn SessionOutput>, String>;
}
