pub mod subprocess;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{
    config::ModelConfig,
    session::{ConversationMessage, SessionPublisher},
    workspace::SessionWorkspace,
};

pub struct TurnRequest {
    pub app_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub model: ModelConfig,
    pub workspace: Arc<dyn SessionWorkspace>,
    pub history: Vec<ConversationMessage>,
    pub prompt: String,
    pub cancellation: watch::Receiver<bool>,
}

#[derive(Debug)]
pub enum TurnError {
    Cancelled,
    Failed(String),
}

impl From<String> for TurnError {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

impl From<&str> for TurnError {
    fn from(message: &str) -> Self {
        Self::Failed(message.to_owned())
    }
}

/// Executes exactly one agent-loop turn; session history is owned by the application.
#[async_trait]
pub trait TurnRunner: Send + Sync {
    async fn run_turn(
        &self,
        request: TurnRequest,
        output: Arc<dyn SessionPublisher>,
    ) -> Result<String, TurnError>;
}
