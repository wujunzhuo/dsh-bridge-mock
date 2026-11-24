use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use tokio::sync::RwLock;

use crate::{
    backend::{DshSessionBackend, DshSessionBackendFactory},
    config::Tenant,
};

pub struct OwnedSession {
    pub tenant_id: String,
    pub backend: Arc<dyn DshSessionBackend>,
}

pub type SessionMap = Arc<RwLock<HashMap<String, OwnedSession>>>;

#[derive(Clone)]
pub struct AppState {
    pub tenants: Arc<ArcSwap<Vec<Tenant>>>,
    pub sessions: SessionMap,
    factory: Arc<dyn DshSessionBackendFactory>,
}

impl AppState {
    pub fn new(
        tenants: Arc<ArcSwap<Vec<Tenant>>>,
        factory: Arc<dyn DshSessionBackendFactory>,
    ) -> Self {
        Self {
            tenants,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            factory,
        }
    }

    pub async fn get_or_start_session(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> Result<Arc<dyn DshSessionBackend>, String> {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get(session_id) {
            if session.tenant_id != tenant_id {
                return Err("session belongs to another tenant".into());
            }
            if session.backend.is_running() {
                return Ok(session.backend.clone());
            }
            sessions.remove(session_id);
        }

        let tenant = self
            .tenants
            .load()
            .iter()
            .find(|tenant| tenant.tenant_id == tenant_id)
            .cloned()
            .ok_or_else(|| format!("unknown tenant {tenant_id}"))?;
        let backend = self
            .factory
            .start(tenant_id, session_id, &tenant.llm)
            .await?;
        sessions.insert(
            session_id.to_owned(),
            OwnedSession {
                tenant_id: tenant_id.to_owned(),
                backend: backend.clone(),
            },
        );
        Ok(backend)
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
            sessions.remove(session_id).expect("session was checked above")
        };

        session.backend.shutdown().await;
        Ok(())
    }
}
