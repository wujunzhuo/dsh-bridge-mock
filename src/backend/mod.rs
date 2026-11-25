pub mod subprocess;

use std::sync::Arc;

use async_trait::async_trait;

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
}

/// Executes exactly one agent-loop turn; session history is owned by the application.
#[async_trait]
pub trait TurnRunner: Send + Sync {
    async fn run_turn(
        &self,
        request: TurnRequest,
        output: Arc<dyn SessionPublisher>,
    ) -> Result<String, String>;
}
