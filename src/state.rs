use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::{
    backend::{TurnRequest, TurnRunner},
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
    closed: AtomicBool,
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
    ) -> Result<(String, String), String> {
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
        let _turn_guard = session.turn_lock.lock().await;
        if session.closed.load(Ordering::Acquire) {
            return Err("session is closed".into());
        }

        let turn_id = Uuid::new_v4().to_string();
        let history = session.history.messages().await;
        let output = session.output.publisher();
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
                },
                output,
            )
            .await;
        let sync_result = session.workspace.sync().await;
        let answer = match (result, sync_result) {
            (Ok(answer), Ok(())) => answer,
            (Ok(_), Err(error)) => return Err(format!("workspace sync failed: {error}")),
            (Err(error), Ok(())) => return Err(error),
            (Err(turn_error), Err(sync_error)) => {
                return Err(format!("{turn_error}; workspace sync failed: {sync_error}"));
            }
        };
        session.history.append_turn(prompt, answer.clone()).await;
        Ok((turn_id, answer))
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

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex as StdMutex},
    };

    use arc_swap::ArcSwap;
    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::{
        backend::{TurnRequest, TurnRunner},
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
        ) -> Result<String, String> {
            self.histories.lock().await.push(request.history);
            self.workspaces
                .lock()
                .unwrap()
                .push(request.workspace.working_directory().to_owned());
            Ok(format!("reply to {}", request.prompt))
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
}
