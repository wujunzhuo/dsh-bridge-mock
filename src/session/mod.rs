mod in_memory;

pub use in_memory::{InMemorySessionHistoryFactory, InMemorySessionOutputFactory};

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::protocol::{RpcFrame, SessionEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationMessage {
    pub role: MessageRole,
    pub content: String,
}

#[async_trait]
pub trait SessionOutput: Send + Sync {
    fn publisher(&self) -> Arc<dyn SessionPublisher>;
    fn subscribe(&self) -> broadcast::Receiver<RpcFrame>;
    async fn history(&self, after_event_id: Option<&str>) -> Vec<SessionEvent>;
}

#[async_trait]
pub trait SessionPublisher: Send + Sync {
    fn publish(&self, frame: RpcFrame);
    async fn publish_event(&self, session_id: &str, turn_id: &str, event: Value);
}

#[async_trait]
pub trait SessionOutputFactory: Send + Sync {
    async fn create(
        &self,
        app_id: &str,
        session_id: &str,
    ) -> Result<Arc<dyn SessionOutput>, String>;
}

#[async_trait]
pub trait SessionHistory: Send + Sync {
    async fn messages(&self) -> Vec<ConversationMessage>;
    async fn append_turn(&self, user_message: String, assistant_message: String);
}

#[async_trait]
pub trait SessionHistoryFactory: Send + Sync {
    async fn create(
        &self,
        app_id: &str,
        session_id: &str,
    ) -> Result<Arc<dyn SessionHistory>, String>;
}
