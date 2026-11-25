use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use tokio::sync::{watch, Mutex, RwLock};
use uuid::Uuid;

use crate::{
    backend::{TurnError, TurnRequest, TurnRunner},
    config::{App, ModelConfig},
    session::{SessionHistory, SessionHistoryFactory, SessionOutput, SessionOutputFactory},
    workspace::{SessionWorkspace, WorkspaceFactory},
};

struct OwnedSession {
    app_id: String,
    model: ModelConfig,
    output: Arc<dyn SessionOutput>,
    history: Arc<dyn SessionHistory>,
    workspace: Arc<dyn SessionWorkspace>,
    turn_lock: Mutex<()>,
    active_turn: Mutex<Option<ActiveTurn>>,
    closed: AtomicBool,
}

struct ActiveTurn {
    turn_id: String,
    cancellation: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct SessionHandle {
    pub output: Arc<dyn SessionOutput>,
}

type SessionMap = Arc<RwLock<HashMap<String, Arc<OwnedSession>>>>;

#[derive(Clone)]
pub struct AppState {
    pub apps: Arc<ArcSwap<Vec<App>>>,
    sessions: SessionMap,
    runner: Arc<dyn TurnRunner>,
    output_factory: Arc<dyn SessionOutputFactory>,
    history_factory: Arc<dyn SessionHistoryFactory>,
    workspaces: Arc<dyn WorkspaceFactory>,
}

impl AppState {
    pub fn new(
        apps: Arc<ArcSwap<Vec<App>>>,
        runner: Arc<dyn TurnRunner>,
        output_factory: Arc<dyn SessionOutputFactory>,
        history_factory: Arc<dyn SessionHistoryFactory>,
        workspaces: Arc<dyn WorkspaceFactory>,
    ) -> Self {
        Self {
            apps,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            runner,
            output_factory,
            history_factory,
            workspaces,
        }
    }

    pub async fn get_or_create_session(
        &self,
        app_id: &str,
        session_id: &str,
    ) -> Result<SessionHandle, String> {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get(session_id) {
            if session.app_id != app_id {
                return Err("session belongs to another app".into());
            }
            if !session.closed.load(Ordering::Acquire) {
                return Ok(SessionHandle {
                    output: session.output.clone(),
                });
            }
            return Err("session is closed".into());
        }

        let app = self
            .apps
            .load()
            .iter()
            .find(|app| app.app_id == app_id)
            .cloned()
            .ok_or_else(|| format!("unknown app {app_id}"))?;
        let output = self.output_factory.create(app_id, session_id).await?;
        let history = self.history_factory.create(app_id, session_id).await?;
        let workspace = self.workspaces.create(app_id, session_id).await?;
        let session = Arc::new(OwnedSession {
            app_id: app_id.to_owned(),
            model: app.llm,
            output: output.clone(),
            history,
            workspace,
            turn_lock: Mutex::new(()),
            active_turn: Mutex::new(None),
            closed: AtomicBool::new(false),
        });
        sessions.insert(session_id.to_owned(), session);
        Ok(SessionHandle { output })
    }

    pub async fn run_turn(
        &self,
        app_id: &str,
        session_id: &str,
        prompt: String,
    ) -> Result<(String, String), TurnError> {
        let session = {
            let sessions = self.sessions.read().await;
            let session = sessions
                .get(session_id)
                .cloned()
                .ok_or_else(|| TurnError::Failed(format!("unknown session {session_id}")))?;
            if session.app_id != app_id {
                return Err(TurnError::Failed("session belongs to another app".into()));
            }
            session
        };
        let _turn_guard = session.turn_lock.lock().await;
        if session.closed.load(Ordering::Acquire) {
            return Err(TurnError::Failed("session is closed".into()));
        }

        let turn_id = Uuid::new_v4().to_string();
        let (cancellation, cancellation_rx) = watch::channel(false);
        *session.active_turn.lock().await = Some(ActiveTurn {
            turn_id: turn_id.clone(),
            cancellation,
        });
        let history = session.history.messages().await;
        let output = session.output.publisher();
        output
            .publish_event(
                session_id,
                &turn_id,
                serde_json::json!({"type": "turn_status", "status": "started"}),
            )
            .await;
        let result = self
            .runner
            .run_turn(
                TurnRequest {
                    app_id: app_id.to_owned(),
                    session_id: session_id.to_owned(),
                    turn_id: turn_id.clone(),
                    model: session.model.clone(),
                    workspace: session.workspace.clone(),
                    history,
                    prompt: prompt.clone(),
                    cancellation: cancellation_rx,
                },
                output.clone(),
            )
            .await;
        {
            let mut active_turn = session.active_turn.lock().await;
            if active_turn
                .as_ref()
                .is_some_and(|active| active.turn_id == turn_id)
            {
                *active_turn = None;
            }
        }
        let sync_result = session.workspace.sync().await;
        match (result, sync_result) {
            (Ok(answer), Ok(())) => {
                session.history.append_turn(prompt, answer.clone()).await;
                output
                    .publish_event(
                        session_id,
                        &turn_id,
                        serde_json::json!({
                            "type": "turn_status",
                            "status": "completed",
                            "reason": "natural",
                            "answer": answer,
                        }),
                    )
                    .await;
                Ok((turn_id, answer))
            }
            (Err(TurnError::Cancelled), Ok(())) => {
                output
                    .publish_event(
                        session_id,
                        &turn_id,
                        serde_json::json!({"type": "turn_status", "status": "cancelled"}),
                    )
                    .await;
                Err(TurnError::Cancelled)
            }
            (Ok(_), Err(error)) => {
                let message = format!("workspace sync failed: {error}");
                publish_turn_failure(&output, session_id, &turn_id, &message).await;
                Err(TurnError::Failed(message))
            }
            (Err(TurnError::Failed(error)), Ok(())) => {
                publish_turn_failure(&output, session_id, &turn_id, &error).await;
                Err(TurnError::Failed(error))
            }
            (Err(TurnError::Cancelled), Err(error)) => {
                let message = format!("turn cancelled; workspace sync failed: {error}");
                publish_turn_failure(&output, session_id, &turn_id, &message).await;
                Err(TurnError::Failed(message))
            }
            (Err(TurnError::Failed(turn_error)), Err(sync_error)) => {
                let message = format!("{turn_error}; workspace sync failed: {sync_error}");
                publish_turn_failure(&output, session_id, &turn_id, &message).await;
                Err(TurnError::Failed(message))
            }
        }
    }

    pub async fn cancel_turn(
        &self,
        app_id: &str,
        session_id: &str,
        turn_id: &str,
    ) -> Result<bool, String> {
        let session = {
            let sessions = self.sessions.read().await;
            let session = sessions
                .get(session_id)
                .cloned()
                .ok_or_else(|| format!("unknown session {session_id}"))?;
            if session.app_id != app_id {
                return Err("session belongs to another app".into());
            }
            session
        };
        let active_turn = session.active_turn.lock().await;
        let Some(active_turn) = active_turn
            .as_ref()
            .filter(|active_turn| active_turn.turn_id == turn_id)
        else {
            return Ok(false);
        };
        Ok(active_turn.cancellation.send(true).is_ok())
    }

    pub async fn destroy_session(&self, app_id: &str, session_id: &str) -> Result<(), String> {
        let session = {
            let sessions = self.sessions.read().await;
            let Some(session) = sessions.get(session_id).cloned() else {
                return Err(format!("unknown session {session_id}"));
            };
            if session.app_id != app_id {
                return Err("session belongs to another app".into());
            }
            session
        };

        let _turn_guard = session.turn_lock.lock().await;
        session.closed.store(true, Ordering::Release);
        {
            let mut sessions = self.sessions.write().await;
            if sessions
                .get(session_id)
                .is_some_and(|current| Arc::ptr_eq(current, &session))
            {
                sessions.remove(session_id);
            }
        }
        self.workspaces.delete(session.workspace.as_ref()).await
    }
}

async fn publish_turn_failure(
    output: &Arc<dyn crate::session::SessionPublisher>,
    session_id: &str,
    turn_id: &str,
    message: &str,
) {
    output
        .publish_event(
            session_id,
            turn_id,
            serde_json::json!({
                "type": "turn_status",
                "status": "failed",
                "message": message,
            }),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex as StdMutex},
    };

    use arc_swap::ArcSwap;
    use async_trait::async_trait;
    use tokio::sync::{Mutex, Notify};

    use crate::{
        backend::{TurnError, TurnRequest, TurnRunner},
        config::{App, ModelConfig},
        session::{InMemorySessionHistoryFactory, InMemorySessionOutputFactory, SessionPublisher},
        workspace::LocalWorkspaceFactory,
    };

    use super::AppState;

    #[derive(Default)]
    struct RecordingRunner {
        histories: Mutex<Vec<Vec<crate::session::ConversationMessage>>>,
        workspaces: StdMutex<Vec<PathBuf>>,
    }

    #[async_trait]
    impl TurnRunner for RecordingRunner {
        async fn run_turn(
            &self,
            request: TurnRequest,
            _output: Arc<dyn SessionPublisher>,
        ) -> Result<String, TurnError> {
            self.histories.lock().await.push(request.history);
            self.workspaces
                .lock()
                .unwrap()
                .push(request.workspace.working_directory().to_owned());
            Ok(format!("reply to {}", request.prompt))
        }
    }

    #[derive(Default)]
    struct WaitingRunner {
        started: Notify,
    }

    #[async_trait]
    impl TurnRunner for WaitingRunner {
        async fn run_turn(
            &self,
            request: TurnRequest,
            _output: Arc<dyn SessionPublisher>,
        ) -> Result<String, TurnError> {
            self.started.notify_one();
            let mut cancellation = request.cancellation;
            loop {
                if *cancellation.borrow() {
                    return Err(TurnError::Cancelled);
                }
                if cancellation.changed().await.is_err() {
                    return Err(TurnError::Failed("cancellation channel closed".into()));
                }
            }
        }
    }

    #[tokio::test]
    async fn turns_reuse_history_and_workspace_until_session_is_destroyed() {
        let runner = Arc::new(RecordingRunner::default());
        let state = AppState::new(
            Arc::new(ArcSwap::from_pointee(vec![App {
                api_key: "api-key".into(),
                app_id: "app-a".into(),
                name: "App A".into(),
                llm: ModelConfig {
                    base_url: "https://example.test/v1".into(),
                    api_key: "model-key".into(),
                    model: "model".into(),
                },
            }])),
            runner.clone(),
            Arc::new(InMemorySessionOutputFactory),
            Arc::new(InMemorySessionHistoryFactory),
            Arc::new(LocalWorkspaceFactory::new().unwrap()),
        );

        state
            .get_or_create_session("app-a", "session-a")
            .await
            .unwrap();
        state
            .run_turn("app-a", "session-a", "first question".into())
            .await
            .unwrap();
        state
            .run_turn("app-a", "session-a", "second question".into())
            .await
            .unwrap();

        let workspace = {
            let histories = runner.histories.lock().await;
            assert!(histories[0].is_empty());
            assert_eq!(histories[1].len(), 2);
            assert_eq!(histories[1][0].role.as_str(), "user");
            assert_eq!(histories[1][0].content, "first question");
            assert_eq!(histories[1][1].role.as_str(), "assistant");
            assert_eq!(histories[1][1].content, "reply to first question");
            let workspaces = runner.workspaces.lock().unwrap();
            assert_eq!(workspaces[0], workspaces[1]);
            workspaces[0].clone()
        };

        state.destroy_session("app-a", "session-a").await.unwrap();
        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn client_cancellation_reaches_the_active_turn_and_is_published() {
        let runner = Arc::new(WaitingRunner::default());
        let state = AppState::new(
            Arc::new(ArcSwap::from_pointee(vec![App {
                api_key: "api-key".into(),
                app_id: "app-a".into(),
                name: "App A".into(),
                llm: ModelConfig {
                    base_url: "https://example.test/v1".into(),
                    api_key: "model-key".into(),
                    model: "model".into(),
                },
            }])),
            runner.clone(),
            Arc::new(InMemorySessionOutputFactory),
            Arc::new(InMemorySessionHistoryFactory),
            Arc::new(LocalWorkspaceFactory::new().unwrap()),
        );
        let session = state
            .get_or_create_session("app-a", "session-cancel")
            .await
            .unwrap();
        let mut events = session.output.subscribe();
        let turn_state = state.clone();
        let turn = tokio::spawn(async move {
            turn_state
                .run_turn("app-a", "session-cancel", "long task".into())
                .await
        });

        runner.started.notified().await;
        let started = events.recv().await.unwrap();
        let turn_id = started.params.unwrap()["payload"]["turn_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(state
            .cancel_turn("app-a", "session-cancel", &turn_id)
            .await
            .unwrap());
        assert!(matches!(turn.await.unwrap(), Err(TurnError::Cancelled)));

        let cancelled = events.recv().await.unwrap();
        assert_eq!(
            cancelled.params.unwrap()["payload"]["event"]["status"],
            "cancelled"
        );
        assert!(!state
            .cancel_turn("app-a", "session-cancel", &turn_id)
            .await
            .unwrap());
    }
}
