mod auth;
mod backend;
mod config;
mod protocol;
mod session;
mod state;
mod workspace;
mod ws;

use std::{env, sync::Arc};

use arc_swap::ArcSwap;
use axum::{routing::get, Router};
use backend::subprocess::SubprocessTurnRunner;
use session::{InMemorySessionHistoryFactory, InMemorySessionOutputFactory};
use state::AppState;
use workspace::LocalWorkspaceFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let apps = Arc::new(ArcSwap::from_pointee(config::load_apps()?));
    let state = AppState::new(
        apps,
        Arc::new(SubprocessTurnRunner::from_env()?),
        Arc::new(InMemorySessionOutputFactory),
        Arc::new(InMemorySessionHistoryFactory),
        Arc::new(LocalWorkspaceFactory::new()?),
    );
    let app = Router::new()
        .route("/api/remote.mux", get(ws::ws_handler))
        .with_state(state.clone());

    let address = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("listening on {address}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}
