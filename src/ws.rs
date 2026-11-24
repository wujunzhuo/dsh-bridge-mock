use std::collections::HashMap;

use axum::{
    extract::{ws::Message, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    auth::check_api_key,
    protocol::{RpcFrame, SessionInfo},
    state::AppState,
};

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let Some(api_key) = params.get("api_key") else {
        eprintln!("[ws] rejected connection: missing api_key");
        return (StatusCode::UNAUTHORIZED, "missing api_key").into_response();
    };
    let tenants = state.tenants.load();
    let Some(tenant_id) = check_api_key(&tenants, api_key) else {
        eprintln!("[ws] rejected connection: invalid tenant api_key");
        return (StatusCode::UNAUTHORIZED, "invalid api_key").into_response();
    };

    let is_new = !params.contains_key("session_id");
    let session_id = params
        .get("session_id")
        .cloned()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let backend = match state.get_or_start_session(&tenant_id, &session_id).await {
        Ok(backend) => backend,
        Err(error) if error == "session belongs to another tenant" => {
            eprintln!("[ws] rejected connection: session belongs to another tenant");
            return (StatusCode::FORBIDDEN, error).into_response();
        }
        Err(error) => return (StatusCode::BAD_GATEWAY, error).into_response(),
    };

    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();
        if is_new {
            let info = SessionInfo {
                session_id: backend.session_id().to_owned(),
                process_id: backend.process_id(),
            };
            let init = RpcFrame::response(
                Some(Value::String("init".into())),
                serde_json::to_value(info).unwrap_or(Value::Null),
            );
            if sink
                .send(Message::Text(
                    serde_json::to_string(&init).unwrap_or_default().into(),
                ))
                .await
                .is_err()
            {
                return;
            }
        }

        let mut output_rx = backend.subscribe();
        let send_task = tokio::spawn(async move {
            loop {
                match output_rx.recv().await {
                    Ok(frame) => {
                        let Ok(text) = serde_json::to_string(&frame) else {
                            continue;
                        };
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        while let Some(Ok(message)) = stream.next().await {
            let text = match message {
                Message::Text(text) => text,
                Message::Close(_) => break,
                _ => continue,
            };
            let Ok(frame) = serde_json::from_str::<RpcFrame>(&text) else {
                continue;
            };

            if frame.method.as_deref() == Some("session.attachment") {
                let after = frame
                    .params
                    .as_ref()
                    .and_then(|params| params.get("after_event_id"))
                    .and_then(Value::as_str);
                let response = RpcFrame::response(
                    frame.id,
                    serde_json::json!({"events": backend.history(after).await}),
                );
                backend.publish(response);
            } else if let Err(error) = backend.send(frame.clone()).await {
                backend.publish(RpcFrame::error(frame.id, error));
            }
        }
        send_task.abort();
    })
}
