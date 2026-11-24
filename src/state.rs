use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use tokio::sync::RwLock;

use crate::{
    backend::{SessionBackend, SessionBackendFactory},
    config::Tenant,
    session::{SessionOutput, SessionOutputFactory},
};

#[derive(Clone)]
pub struct OwnedSession {
    pub tenant_id: String,
    pub backend: Arc<dyn SessionBackend>,
    pub output: Arc<dyn SessionOutput>,
}

#[derive(Clone)]
pub struct SessionHandle {
    pub backend: Arc<dyn SessionBackend>,
    pub output: Arc<dyn SessionOutput>,
}

pub type SessionMap = Arc<RwLock<HashMap<String, OwnedSession>>>;

#[derive(Clone)]
pub struct AppState {
    pub tenants: Arc<ArcSwap<Vec<Tenant>>>,
    pub sessions: SessionMap,
    factory: Arc<dyn SessionBackendFactory>,
    output_factory: Arc<dyn SessionOutputFactory>,
}

impl AppState {
    pub fn new(
        tenants: Arc<ArcSwap<Vec<Tenant>>>,
        factory: Arc<dyn SessionBackendFactory>,
        output_factory: Arc<dyn SessionOutputFactory>,
    ) -> Self {
        Self {
            tenants,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            factory,
            output_factory,
        }
    }

    pub async fn get_or_start_session(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> Result<SessionHandle, String> {
        let mut sessions = self.sessions.write().await;
        let mut output = None;
        if let Some(session) = sessions.get(session_id) {
            if session.tenant_id != tenant_id {
                return Err("session belongs to another tenant".into());
            }
            if session.backend.is_running() {
                return Ok(SessionHandle {
                    backend: session.backend.clone(),
                    output: session.output.clone(),
                });
            }
            output = Some(session.output.clone());
        }

        let tenant = self
            .tenants
            .load()
            .iter()
            .find(|tenant| tenant.tenant_id == tenant_id)
            .cloned()
            .ok_or_else(|| format!("unknown tenant {tenant_id}"))?;
        let output = match output {
            Some(output) => output,
            None => self.output_factory.create(tenant_id, session_id).await?,
        };
        let backend = self
            .factory
            .start(tenant_id, session_id, &tenant.llm, output.publisher())
            .await?;
        sessions.insert(
            session_id.to_owned(),
            OwnedSession {
                tenant_id: tenant_id.to_owned(),
                backend: backend.clone(),
                output: output.clone(),
            },
        );
        Ok(SessionHandle { backend, output })
    }

    pub async fn destroy_session(&self, tenant_id: &str, session_id: &str) -> Result<(), String> {
        let session = {
            let mut sessions = self.sessions.write().await;
            let Some(session) = sessions.get(session_id) else {
                return Err(format!("unknown session {session_id}"));
            };
            if session.tenant_id != tenant_id {
                return Err("session belongs to another tenant".into());
            }
            sessions
                .remove(session_id)
                .expect("session was checked above")
        };

        session.backend.shutdown().await;
        Ok(())
    }
}
