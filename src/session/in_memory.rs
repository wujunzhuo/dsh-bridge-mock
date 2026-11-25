use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use crate::protocol::{RpcFrame, SessionEvent};

use super::{
    ConversationMessage, MessageRole, SessionHistory, SessionHistoryFactory, SessionOutput,
    SessionOutputFactory, SessionPublisher,
};

const EVENT_METHOD: &str = "session.event";

#[derive(Default)]
pub struct InMemorySessionHistoryFactory;

#[async_trait]
impl SessionHistoryFactory for InMemorySessionHistoryFactory {
    async fn create(
        &self,
        _app_id: &str,
        _session_id: &str,
    ) -> Result<Arc<dyn SessionHistory>, String> {
        Ok(Arc::new(InMemorySessionHistory::default()))
    }
}

#[derive(Default)]
pub struct InMemorySessionHistory {
    messages: RwLock<Vec<ConversationMessage>>,
}

#[async_trait]
impl SessionHistory for InMemorySessionHistory {
    async fn messages(&self) -> Vec<ConversationMessage> {
        self.messages.read().await.clone()
    }

    async fn append_turn(&self, user_message: String, assistant_message: String) {
        let mut messages = self.messages.write().await;
        messages.push(ConversationMessage {
            role: MessageRole::User,
            content: user_message,
        });
        messages.push(ConversationMessage {
            role: MessageRole::Assistant,
            content: assistant_message,
        });
    }
}

#[derive(Default)]
pub struct InMemorySessionOutputFactory;

#[async_trait]
impl SessionOutputFactory for InMemorySessionOutputFactory {
    async fn create(
        &self,
        _app_id: &str,
        _session_id: &str,
    ) -> Result<Arc<dyn SessionOutput>, String> {
        Ok(Arc::new(InMemorySessionOutput::new()))
    }
}

pub struct InMemorySessionOutput {
    frames: broadcast::Sender<RpcFrame>,
    events: Arc<RwLock<Vec<SessionEvent>>>,
}

impl InMemorySessionOutput {
    pub fn new() -> Self {
        let (frames, _) = broadcast::channel(128);
        Self {
            frames,
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl SessionOutput for InMemorySessionOutput {
    fn publisher(&self) -> Arc<dyn SessionPublisher> {
        Arc::new(InMemorySessionPublisher {
            frames: self.frames.clone(),
            events: self.events.clone(),
        })
    }

    fn subscribe(&self) -> broadcast::Receiver<RpcFrame> {
        self.frames.subscribe()
    }

    async fn history(&self, after_event_id: Option<&str>) -> Vec<SessionEvent> {
        let events = self.events.read().await;
        match after_event_id {
            Some(id) => events
                .iter()
                .position(|event| event.event_id == id)
                .map(|index| events.iter().skip(index + 1).cloned().collect())
                .unwrap_or_else(|| events.clone()),
            None => events.clone(),
        }
    }
}

impl Default for InMemorySessionOutput {
    fn default() -> Self {
        Self::new()
    }
}

struct InMemorySessionPublisher {
    frames: broadcast::Sender<RpcFrame>,
    events: Arc<RwLock<Vec<SessionEvent>>>,
}

#[async_trait]
impl SessionPublisher for InMemorySessionPublisher {
    fn publish(&self, frame: RpcFrame) {
        let _ = self.frames.send(frame);
    }

    async fn publish_event(&self, session_id: &str, turn_id: &str, event: Value) {
        let ty = event
            .get("type")
            .or_else(|| event.get("event"))
            .and_then(Value::as_str)
            .unwrap_or("session.event")
            .to_owned();
        let session_event = SessionEvent {
            event_id: Uuid::new_v4().to_string(),
            ty,
            payload: json!({
                "event": event,
                "session_id": session_id,
                "turn_id": turn_id,
            }),
        };
        self.events.write().await.push(session_event.clone());
        self.publish(RpcFrame::notification(
            EVENT_METHOD,
            serde_json::to_value(session_event).unwrap_or(Value::Null),
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::{InMemorySessionHistory, InMemorySessionOutput, InMemorySessionOutputFactory};
    use crate::session::{SessionHistory, SessionOutput, SessionOutputFactory};

    #[tokio::test]
    async fn published_events_are_broadcast_and_kept_in_output_history() {
        let output = InMemorySessionOutput::new();
        let publisher = output.publisher();
        let mut receiver = output.subscribe();

        publisher
            .publish_event(
                "service-session",
                "turn-1",
                json!({"type": "text", "text": "hello"}),
            )
            .await;

        let frame = receiver.recv().await.expect("published event");
        assert_eq!(frame.method.as_deref(), Some("session.event"));
        let history = output.history(None).await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].payload["session_id"], "service-session");
        assert_eq!(history[0].payload["turn_id"], "turn-1");
        assert_eq!(history[0].payload["event"]["text"], "hello");
    }

    #[tokio::test]
    async fn factory_creates_an_output_behind_the_trait() {
        let factory = InMemorySessionOutputFactory;
        let output = factory.create("app", "service-session").await.unwrap();
        let _: Arc<dyn SessionOutput> = output;
    }

    #[tokio::test]
    async fn history_keeps_only_user_and_assistant_turn_messages() {
        let history = InMemorySessionHistory::default();
        history.append_turn("hello".into(), "hi there".into()).await;

        let messages = history.messages().await;
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role.as_str(), "user");
        assert_eq!(messages[0].content, "hello");
        assert_eq!(messages[1].role.as_str(), "assistant");
        assert_eq!(messages[1].content, "hi there");
    }
}
