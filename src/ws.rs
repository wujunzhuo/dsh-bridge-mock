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
    let apps = state.apps.load();
    let Some(app_id) = check_api_key(&apps, api_key) else {
        eprintln!("[ws] rejected connection: invalid app api_key");
        return (StatusCode::UNAUTHORIZED, "invalid api_key").into_response();
    };

    let is_new = !params.contains_key("session_id");
    let session_id = params
        .get("session_id")
        .cloned()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let session = match state.get_or_create_session(&app_id, &session_id).await {
        Ok(session) => session,
        Err(error) if error == "session belongs to another app" => {
            eprintln!("[ws] rejected connection: session belongs to another app");
            return (StatusCode::FORBIDDEN, error).into_response();
        }
        Err(error) => return (StatusCode::BAD_GATEWAY, error).into_response(),
    };
    let output = session.output;
    let publisher = output.publisher();
    let session_state = state.clone();
    let session_app = app_id.clone();
    let session_id_for_close = session_id.clone();

    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();
        if is_new {
            let info = SessionInfo {
                session_id: session_id.clone(),
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

        let mut output_rx = output.subscribe();
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

            if frame.method.as_deref() == Some("session.close") {
                let state = session_state.clone();
                let app_id = session_app.clone();
                let session_id = session_id_for_close.clone();
                let publisher = publisher.clone();
                tokio::spawn(async move {
                    match state.destroy_session(&app_id, &session_id).await {
                        Ok(()) => {
                            publisher.publish(RpcFrame::notification(
                                "session.closed",
                                serde_json::json!({"session_id": session_id}),
                            ));
                            publisher.publish(RpcFrame::response(
                                frame.id,
                                serde_json::json!({
                                    "closed": true,
                                    "session_id": session_id,
                                }),
                            ));
                        }
                        Err(error) => publisher.publish(RpcFrame::error(frame.id, error)),
                    }
                });
            } else if frame.method.as_deref() == Some("session.attachment") {
                let after = frame
                    .params
                    .as_ref()
                    .and_then(|params| params.get("after_event_id"))
                    .and_then(Value::as_str);
                let response = RpcFrame::response(
                    frame.id,
                    serde_json::json!({"events": output.history(after).await}),
                );
                publisher.publish(response);
            } else if frame.method.as_deref() == Some("session.prompt") {
                let Some(prompt) = frame
                    .params
                    .as_ref()
                    .and_then(|params| params.get("prompt"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                else {
                    publisher.publish(RpcFrame::error(
                        frame.id,
                        "session.prompt requires a string params.prompt",
                    ));
                    continue;
                };
                let state = session_state.clone();
                let app_id = session_app.clone();
                let session_id = session_id_for_close.clone();
                let publisher = publisher.clone();
                tokio::spawn(async move {
                    let response = match state.run_turn(&app_id, &session_id, prompt).await {
                        Ok((turn_id, answer)) => RpcFrame::response(
                            frame.id,
                            serde_json::json!({
                                "ok": true,
                                "completion": "natural",
                                "session_id": session_id,
                                "turn_id": turn_id,
                                "answer": answer,
                            }),
                        ),
                        Err(crate::backend::TurnError::Cancelled) => RpcFrame::response(
                            frame.id,
                            serde_json::json!({"ok": false, "cancelled": true}),
                        ),
                        Err(crate::backend::TurnError::Failed(error)) => {
                            RpcFrame::error(frame.id, error)
                        }
                    };
                    publisher.publish(response);
                });
            } else if frame.method.as_deref() == Some("session.cancel") {
                let turn_id = frame
                    .params
                    .as_ref()
                    .and_then(|params| params.get("turn_id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let Some(turn_id) = turn_id else {
                    publisher.publish(RpcFrame::error(
                        frame.id,
                        "session.cancel requires a string params.turn_id",
                    ));
                    continue;
                };
                let state = session_state.clone();
                let app_id = session_app.clone();
                let session_id = session_id_for_close.clone();
                let publisher = publisher.clone();
                tokio::spawn(async move {
                    match state.cancel_turn(&app_id, &session_id, &turn_id).await {
                        Ok(accepted) => publisher.publish(RpcFrame::response(
                            frame.id,
                            serde_json::json!({"accepted": accepted, "turn_id": turn_id}),
                        )),
                        Err(error) => publisher.publish(RpcFrame::error(frame.id, error)),
                    }
                });
            } else {
                publisher.publish(RpcFrame::error(
                    frame.id,
                    "only session.prompt, session.cancel, session.attachment, and session.close are supported",
                ));
            }
        }
        send_task.abort();
    })
}
