use std::{
    env, fs,
    path::PathBuf,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot, Mutex, RwLock},
};
use uuid::Uuid;

use crate::{config::ModelConfig, protocol::RpcFrame, session::SessionPublisher};

use super::{SessionBackend, SessionBackendFactory};

pub struct SubprocessBackendFactory {
    command: String,
    profile: String,
}

impl SubprocessBackendFactory {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            command: env::var("DSH_COMMAND").unwrap_or_else(|_| "dsh".into()),
            profile: env::var("DSH_PROFILE").unwrap_or_else(|_| "headless".into()),
        })
    }
}

struct PromptRequest {
    id: Option<Value>,
    prompt: String,
}

struct TemporaryModelPatch(PathBuf);

impl TemporaryModelPatch {
    fn create(model: &ModelConfig) -> Result<Self, String> {
        let path = env::temp_dir().join(format!("dsh-model-{}.json", Uuid::new_v4()));
        let patch = serde_json::json!([
            {
                "id": "llm-pi-ai",
                "config": {
                    "providers": {
                        "tenant-openai": {
                            "apiKeyEnv": "DSH_TENANT_MODEL_API_KEY",
                            "api": "openai-completions",
                            "baseURL": model.base_url,
                            "models": [{"id": model.model}]
                        }
                    }
                }
            },
            {
                "id": "agent-default-model",
                "config": {
                    "provider": "tenant-openai",
                    "model": model.model
                }
            }
        ]);
        fs::write(
            &path,
            serde_json::to_vec(&patch).map_err(|e| format!("encode dsh model patch: {e}"))?,
        )
        .map_err(|e| format!("write dsh model patch: {e}"))?;
        Ok(Self(path))
    }
}

impl Drop for TemporaryModelPatch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub struct SubprocessSession {
    session_id: String,
    prompt_tx: mpsc::Sender<PromptRequest>,
    running: Arc<AtomicBool>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl SessionBackendFactory for SubprocessBackendFactory {
    async fn start(
        &self,
        tenant_id: &str,
        session_id: &str,
        model: &ModelConfig,
        output: Arc<dyn SessionPublisher>,
    ) -> Result<Arc<dyn SessionBackend>, String> {
        let (prompt_tx, mut prompt_rx) = mpsc::channel::<PromptRequest>(16);
        let dsh_session_id = Arc::new(RwLock::new(None));
        let running = Arc::new(AtomicBool::new(true));
        let (stop_tx, mut stop_rx) = oneshot::channel();

        let task_output = output;
        let task_dsh_session_id = dsh_session_id.clone();
        let task_running = running.clone();
        let command = self.command.clone();
        let profile = self.profile.clone();
        let task_tenant_id = tenant_id.to_owned();
        let task_session_id = session_id.to_owned();
        let task_model = model.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    request = prompt_rx.recv() => {
                        let Some(request) = request else { break };
                        let keep_running = run_prompt(
                            &command,
                            &profile,
                            &task_tenant_id,
                            &task_session_id,
                            &task_model,
                            request,
                            task_output.as_ref(),
                            &task_dsh_session_id,
                            &mut stop_rx,
                        ).await;
                        if !keep_running {
                            break;
                        }
                    }
                }
            }
            task_running.store(false, Ordering::Release);
        });

        Ok(Arc::new(SubprocessSession {
            session_id: session_id.to_owned(),
            prompt_tx,
            running,
            stop: Mutex::new(Some(stop_tx)),
        }))
    }
}

#[async_trait]
impl SessionBackend for SubprocessSession {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    async fn send(&self, frame: RpcFrame) -> Result<(), String> {
        if frame.method.as_deref() != Some("session.prompt") {
            return Err("only session.prompt is supported by the dsh headless backend".into());
        }
        let prompt = frame
            .params
            .as_ref()
            .and_then(|params| params.get("prompt"))
            .and_then(Value::as_str)
            .ok_or_else(|| "session.prompt requires a string params.prompt".to_string())?
            .to_owned();
        self.prompt_tx
            .send(PromptRequest {
                id: frame.id,
                prompt,
            })
            .await
            .map_err(|_| format!("dsh session {} is not accepting prompts", self.session_id))
    }

    async fn shutdown(&self) {
        if let Some(stop) = self.stop.lock().await.take() {
            let _ = stop.send(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_prompt(
    executable: &str,
    profile: &str,
    tenant_id: &str,
    service_session_id: &str,
    model: &ModelConfig,
    request: PromptRequest,
    output: &dyn SessionPublisher,
    dsh_session_id: &Arc<RwLock<Option<String>>>,
    stop: &mut oneshot::Receiver<()>,
) -> bool {
    let response_id = request.id.clone();
    let model_patch = match TemporaryModelPatch::create(model) {
        Ok(patch) => patch,
        Err(error) => {
            if let Some(id) = response_id {
                output.publish(RpcFrame::error(Some(id), error));
            }
            return true;
        }
    };
    let resume_id = dsh_session_id.read().await.clone();
    let mut command = Command::new(executable);
    command
        .arg("--profile")
        .arg(profile)
        .arg("--patch")
        .arg(&model_patch.0)
        .arg("--json");
    if let Some(id) = resume_id.as_deref() {
        command.arg("--session-id").arg(id);
    }
    let mut child = match command
        .arg("-")
        .env("DSH_TENANT_ID", tenant_id)
        .env("DSH_SERVICE_SESSION_ID", service_session_id)
        .env("DSH_TENANT_MODEL_API_KEY", &model.api_key)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            if let Some(id) = response_id.clone() {
                output.publish(RpcFrame::error(
                    Some(id),
                    format!("failed to start {executable}: {error}"),
                ));
            }
            return true;
        }
    };

    let stderr_task = child.stderr.take().map(|mut stderr| {
        tokio::spawn(async move {
            let mut bytes = Vec::new();
            let _ = stderr.read_to_end(&mut bytes).await;
            String::from_utf8_lossy(&bytes).into_owned()
        })
    });
    let mut final_answer: Option<String> = None;
    let mut dsh_error: Option<String> = None;

    if let Some(mut stdin) = child.stdin.take() {
        if let Err(error) = stdin.write_all(request.prompt.as_bytes()).await {
            let _ = child.start_kill();
            let _ = child.wait().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            if let Some(id) = response_id.clone() {
                output.publish(RpcFrame::error(
                    Some(id),
                    format!("failed to send prompt to dsh: {error}"),
                ));
            }
            return true;
        }
        let _ = stdin.shutdown().await;
    }

    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            if let Some(id) = response_id.clone() {
                output.publish(RpcFrame::error(Some(id), "failed to read dsh stdout"));
            }
            return true;
        }
    };
    let mut lines = BufReader::new(stdout).lines();
    loop {
        tokio::select! {
            _ = &mut *stop => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                if let Some(task) = stderr_task {
                    let _ = task.await;
                }
                return false;
            }
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        let event = serde_json::from_str::<Value>(&line)
                            .unwrap_or_else(|_| serde_json::json!({"text": line}));
                        if let Some(id) = find_dsh_session_id(&event) {
                            *dsh_session_id.write().await = Some(id);
                        }
                        if event.get("type").and_then(Value::as_str) == Some("final") {
                            final_answer = event
                                .get("text")
                                .or_else(|| event.get("answer"))
                                .and_then(Value::as_str)
                                .map(str::to_owned);
                            if event.get("reason").is_some_and(|reason| {
                                reason.get("kind").and_then(Value::as_str) != Some("completed")
                            }) {
                                dsh_error = event.get("reason").map(Value::to_string);
                            }
                        } else if event.get("type").and_then(Value::as_str) == Some("error") {
                            dsh_error = event
                                .get("message")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                                .or_else(|| event.get("error").map(Value::to_string));
                        }
                        output.publish_event(service_session_id, event).await;
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        if let Some(task) = stderr_task {
                            let _ = task.await;
                        }
                        if let Some(id) = response_id.clone() {
                            output.publish(RpcFrame::error(
                                Some(id),
                                format!("failed to read dsh output: {error}"),
                            ));
                        }
                        return true;
                    }
                }
            }
        }
    }

    let status = child.wait().await;
    let stderr = match stderr_task {
        Some(task) => task.await.unwrap_or_default(),
        None => String::new(),
    };
    let status_ok = status.as_ref().is_ok_and(|status| status.success());
    if let Some(id) = response_id {
        let result = serde_json::json!({
            "ok": status_ok,
            "session_id": service_session_id,
            "answer": final_answer.unwrap_or_default(),
        });
        let frame = if result["ok"] == true {
            RpcFrame::response(Some(id), result)
        } else {
            let mut details = Vec::new();
            if let Some(error) = dsh_error {
                details.push(error);
            }
            if !stderr.trim().is_empty() {
                details.push(stderr.trim().to_owned());
            }
            let detail = details.join("\n").replace(&model.api_key, "[REDACTED]");
            let detail: String = detail
                .chars()
                .rev()
                .take(2000)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            let message = if detail.is_empty() {
                "dsh command exited unsuccessfully".to_owned()
            } else {
                format!("dsh command exited unsuccessfully:\n{detail}")
            };
            RpcFrame::error(Some(id), message)
        };
        output.publish(frame);
    }
    true
}

fn find_dsh_session_id(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            for key in ["session_id", "sessionId"] {
                if let Some(id) = map.get(key).and_then(Value::as_str) {
                    if !id.is_empty() {
                        return Some(id.to_owned());
                    }
                }
            }
            map.values().find_map(find_dsh_session_id)
        }
        Value::Array(items) => items.iter().find_map(find_dsh_session_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::find_dsh_session_id;

    #[test]
    fn finds_nested_dsh_session_id() {
        let event = serde_json::json!({
            "type": "session.started",
            "data": {"session_id": "dsh-session-123"}
        });
        assert_eq!(
            find_dsh_session_id(&event).as_deref(),
            Some("dsh-session-123")
        );
    }
}
