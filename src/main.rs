mod auth;
mod backend;
mod config;
mod protocol;
mod state;
mod ws;

use std::{env, sync::Arc};

use arc_swap::ArcSwap;
use axum::{routing::get, Router};
use backend::dsh::DshBackendFactory;
use state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tenants = Arc::new(ArcSwap::from_pointee(config::load_tenants()?));
    let state = AppState::new(tenants, Arc::new(DshBackendFactory::from_env()?));
    let app = Router::new()
        .route("/api/remote.mux", get(ws::ws_handler))
        .with_state(state.clone());

    let address = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("listening on {address}");
    axum::serve(listener, app).await?;

    for session in state.sessions.read().await.values() {
        session.backend.shutdown().await;
    }
    Ok(())
}
