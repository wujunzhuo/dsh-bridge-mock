use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use crate::protocol::{RpcFrame, SessionEvent};

use super::{SessionOutput, SessionOutputFactory, SessionPublisher};

const EVENT_METHOD: &str = "session.event";

#[derive(Default)]
pub struct InMemorySessionOutputFactory;

#[async_trait]
impl SessionOutputFactory for InMemorySessionOutputFactory {
    async fn create(
        &self,
        _tenant_id: &str,
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

    async fn publish_event(&self, session_id: &str, event: Value) {
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

    use super::{InMemorySessionOutput, InMemorySessionOutputFactory};
    use crate::session::{SessionOutput, SessionOutputFactory};

    #[tokio::test]
    async fn published_events_are_broadcast_and_kept_in_output_history() {
        let output = InMemorySessionOutput::new();
        let publisher = output.publisher();
        let mut receiver = output.subscribe();

        publisher
            .publish_event("service-session", json!({"type": "text", "text": "hello"}))
            .await;

        let frame = receiver.recv().await.expect("published event");
        assert_eq!(frame.method.as_deref(), Some("session.event"));
        let history = output.history(None).await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].payload["session_id"], "service-session");
        assert_eq!(history[0].payload["event"]["text"], "hello");
    }

    #[tokio::test]
    async fn factory_creates_an_output_behind_the_trait() {
        let factory = InMemorySessionOutputFactory;
        let output = factory.create("tenant", "service-session").await.unwrap();
        let _: Arc<dyn SessionOutput> = output;
    }
}
