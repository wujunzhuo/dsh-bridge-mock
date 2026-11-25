use std::{env, fs, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    time::timeout,
};
use uuid::Uuid;

use crate::{
    backend::{TurnError, TurnRequest, TurnRunner},
    session::SessionPublisher,
};

pub struct SubprocessTurnRunner {
    command: String,
    profile: String,
}

impl SubprocessTurnRunner {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            command: env::var("DSH_COMMAND").unwrap_or_else(|_| "dsh".into()),
            profile: env::var("DSH_PROFILE").unwrap_or_else(|_| "headless".into()),
        })
    }
}

struct TemporaryModelPatch(PathBuf);

impl TemporaryModelPatch {
    fn create(model: &crate::config::ModelConfig) -> Result<Self, String> {
        let path = env::temp_dir().join(format!("dsh-model-{}.json", Uuid::new_v4()));
        let patch = serde_json::json!([
            {
                "id": "llm-pi-ai",
                "config": {
                    "providers": {
                        "app-openai": {
                            "apiKeyEnv": "DSH_APP_MODEL_API_KEY",
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
                    "provider": "app-openai",
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

#[async_trait]
impl TurnRunner for SubprocessTurnRunner {
    async fn run_turn(
        &self,
        request: TurnRequest,
        output: Arc<dyn SessionPublisher>,
    ) -> Result<String, TurnError> {
        let model_patch = TemporaryModelPatch::create(&request.model)?;
        let prompt = format_prompt(&request.history, &request.prompt);
        let mut cancellation = request.cancellation;
        let mut command = Command::new(&self.command);
        command
            .arg("--profile")
            .arg(&self.profile)
            .arg("--patch")
            .arg(&model_patch.0)
            .arg("--json")
            .arg("-")
            .env("DSH_APP_ID", &request.app_id)
            .env("DSH_SERVICE_SESSION_ID", &request.session_id)
            .env("DSH_APP_MODEL_API_KEY", &request.model.api_key)
            .current_dir(request.workspace.working_directory())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }
        let mut child = command
            .spawn()
            .map_err(|error| format!("failed to start {}: {error}", self.command))?;

        let stderr_task = child.stderr.take().map(|mut stderr| {
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let _ = stderr.read_to_end(&mut bytes).await;
                String::from_utf8_lossy(&bytes).into_owned()
            })
        });

        if let Some(mut stdin) = child.stdin.take() {
            let write_result = tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    if changed.is_ok() && *cancellation.borrow() {
                        terminate_child(&mut child).await;
                        if let Some(task) = stderr_task {
                            let _ = task.await;
                        }
                        return Err(TurnError::Cancelled);
                    }
                    stdin.write_all(prompt.as_bytes()).await
                }
                result = stdin.write_all(prompt.as_bytes()) => result,
            };
            if let Err(error) = write_result {
                terminate_child(&mut child).await;
                if let Some(task) = stderr_task {
                    let _ = task.await;
                }
                return Err(format!("failed to send prompt to dsh: {error}").into());
            }
            let _ = stdin.shutdown().await;
        }

        let Some(stdout) = child.stdout.take() else {
            terminate_child(&mut child).await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            return Err("failed to read dsh stdout".into());
        };

        let mut lines = BufReader::new(stdout).lines();
        let mut final_answer: Option<String> = None;
        let mut dsh_error: Option<String> = None;
        let mut completed_final_seen = false;
        let mut read_error = None;
        loop {
            let next_line = tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    if changed.is_ok() && *cancellation.borrow() {
                        terminate_child(&mut child).await;
                        if let Some(task) = stderr_task {
                            let _ = task.await;
                        }
                        return Err(TurnError::Cancelled);
                    }
                    continue;
                }
                line = lines.next_line() => line,
            };
            match next_line {
                Ok(Some(line)) => {
                    let event = serde_json::from_str::<Value>(&line)
                        .unwrap_or_else(|_| serde_json::json!({"text": line}));
                    if event.get("type").and_then(Value::as_str) == Some("final") {
                        final_answer = event
                            .get("text")
                            .or_else(|| event.get("answer"))
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                        if event
                            .get("reason")
                            .and_then(|reason| reason.get("kind"))
                            .and_then(Value::as_str)
                            == Some("completed")
                        {
                            completed_final_seen = true;
                        } else {
                            dsh_error = event.get("reason").map(Value::to_string);
                        }
                    } else if event.get("type").and_then(Value::as_str) == Some("error") {
                        dsh_error = event
                            .get("message")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .or_else(|| event.get("error").map(Value::to_string));
                    }
                    output
                        .publish_event(&request.session_id, &request.turn_id, event)
                        .await;
                }
                Ok(None) => break,
                Err(error) => {
                    read_error = Some(format!("failed to read dsh output: {error}"));
                    terminate_child(&mut child).await;
                    break;
                }
            }
        }

        let status = tokio::select! {
            biased;
            status = child.wait() => status,
            changed = cancellation.changed() => {
                if changed.is_ok() && *cancellation.borrow() {
                    terminate_child(&mut child).await;
                    if let Some(task) = stderr_task {
                        let _ = task.await;
                    }
                    return Err(TurnError::Cancelled);
                }
                child.wait().await
            }
        };
        let stderr = match stderr_task {
            Some(task) => task.await.unwrap_or_default(),
            None => String::new(),
        };
        if let Some(error) = read_error {
            return Err(error.into());
        }
        if status.as_ref().is_ok_and(|status| status.success()) {
            if completed_final_seen {
                return Ok(final_answer.unwrap_or_default());
            }
            dsh_error.get_or_insert_with(|| {
                "dsh exited without a final event with reason.kind=completed".into()
            });
        }

        let mut details = Vec::new();
        if let Some(error) = dsh_error {
            details.push(error);
        }
        if !stderr.trim().is_empty() {
            details.push(stderr.trim().to_owned());
        }
        let detail = details
            .join("\n")
            .replace(&request.model.api_key, "[REDACTED]");
        let detail: String = detail
            .chars()
            .rev()
            .take(2000)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if detail.is_empty() {
            Err("dsh command exited unsuccessfully".into())
        } else {
            Err(format!("dsh command exited unsuccessfully:\n{detail}").into())
        }
    }
}

async fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    {
        if let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) {
            // The process group includes tools launched by dsh, not just dsh itself.
            unsafe {
                libc::kill(-pid, libc::SIGTERM);
            }
            let exited = timeout(Duration::from_secs(2), child.wait()).await.is_ok();
            // Also clean up any descendants that ignored SIGTERM after dsh exited.
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
            if !exited {
                let _ = child.wait().await;
            }
            return;
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

fn format_prompt(history: &[crate::session::ConversationMessage], prompt: &str) -> String {
    if history.is_empty() {
        return prompt.to_owned();
    }
    let context: Vec<_> = history
        .iter()
        .map(|message| {
            serde_json::json!({
                "role": message.role.as_str(),
                "content": message.content,
            })
        })
        .collect();
    format!(
        "Conversation history (JSON):\n{}\n\nCurrent user message:\n{}",
        serde_json::to_string_pretty(&context).unwrap_or_default(),
        prompt,
    )
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

    #[cfg(unix)]
    use std::sync::Arc;

    use super::format_prompt;
    use crate::session::{ConversationMessage, MessageRole};

    #[cfg(unix)]
    use crate::{
        backend::{TurnError, TurnRequest, TurnRunner},
        config::ModelConfig,
        session::{InMemorySessionOutputFactory, SessionOutputFactory},
        workspace::{LocalWorkspaceFactory, WorkspaceFactory},
    };

    #[cfg(unix)]
    use tokio::{sync::watch, time::timeout};

    #[cfg(unix)]
    struct TemporaryCommand(PathBuf);

    #[cfg(unix)]
    impl TemporaryCommand {
        fn new(script: &str) -> Self {
            let path = std::env::temp_dir().join(format!("dsh-test-{}.sh", uuid::Uuid::new_v4()));
            fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).unwrap();
            Self(path)
        }
    }

    #[cfg(unix)]
    impl Drop for TemporaryCommand {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[cfg(unix)]
    async fn request(cancellation: watch::Receiver<bool>) -> (TurnRequest, LocalWorkspaceFactory) {
        let factory = LocalWorkspaceFactory::new().unwrap();
        let workspace = factory.create("test-app", "test-session").await.unwrap();
        let request = TurnRequest {
            app_id: "test-app".into(),
            session_id: "test-session".into(),
            turn_id: "test-turn".into(),
            model: ModelConfig {
                base_url: "https://example.test/v1".into(),
                api_key: "test-key".into(),
                model: "test-model".into(),
            },
            workspace,
            history: Vec::new(),
            prompt: "test prompt".into(),
            cancellation,
        };
        (request, factory)
    }

    #[test]
    fn adds_previous_user_and_assistant_messages_as_context() {
        let history = vec![
            ConversationMessage {
                role: MessageRole::User,
                content: "hello".into(),
            },
            ConversationMessage {
                role: MessageRole::Assistant,
                content: "hi".into(),
            },
        ];
        let prompt = format_prompt(&history, "tell me more");
        assert!(prompt.contains("\"role\": \"user\""));
        assert!(prompt.contains("\"role\": \"assistant\""));
        assert!(prompt.contains("Current user message:\ntell me more"));
    }

    #[test]
    fn first_turn_is_not_wrapped_in_empty_context() {
        assert_eq!(format_prompt(&[], "hello"), "hello");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_turn_requires_a_completed_final_event() {
        let command = TemporaryCommand::new(
            "cat >/dev/null\nprintf '%s\\n' '{\"type\":\"final\",\"text\":\"done\",\"reason\":{\"kind\":\"completed\"}}'",
        );
        let runner = super::SubprocessTurnRunner {
            command: command.0.to_string_lossy().into_owned(),
            profile: "headless".into(),
        };
        let (_cancel, cancellation) = watch::channel(false);
        let output = InMemorySessionOutputFactory
            .create("test-app", "test-session")
            .await
            .unwrap();
        let (request, factory) = request(cancellation).await;
        let answer = runner.run_turn(request, output.publisher()).await.unwrap();
        assert_eq!(answer, "done");
        drop(factory);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_turn_rejects_non_completed_final_reasons() {
        let command = TemporaryCommand::new(
            "cat >/dev/null\nprintf '%s\\n' '{\"type\":\"final\",\"text\":\"partial\",\"reason\":{\"kind\":\"stopped\"}}'",
        );
        let runner = super::SubprocessTurnRunner {
            command: command.0.to_string_lossy().into_owned(),
            profile: "headless".into(),
        };
        let (_cancel, cancellation) = watch::channel(false);
        let (request, factory) = request(cancellation).await;
        let output = InMemorySessionOutputFactory
            .create("test-app", "test-session")
            .await
            .unwrap();
        assert!(matches!(
            runner.run_turn(request, output.publisher()).await,
            Err(TurnError::Failed(_))
        ));
        drop(factory);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_turn_can_be_cancelled_by_the_client() {
        let command = TemporaryCommand::new(
            "cat >/dev/null\nprintf '%s\\n' '{\"type\":\"status\",\"phase\":\"working\"}'\nsleep 30",
        );
        let runner = Arc::new(super::SubprocessTurnRunner {
            command: command.0.to_string_lossy().into_owned(),
            profile: "headless".into(),
        });
        let (cancel_tx, cancellation) = watch::channel(false);
        let output = InMemorySessionOutputFactory
            .create("test-app", "test-session")
            .await
            .unwrap();
        let mut events = output.subscribe();
        let publisher = output.publisher();
        let (request, factory) = request(cancellation).await;
        let turn = tokio::spawn(async move {
            let _factory = factory;
            runner.run_turn(request, publisher).await
        });

        timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("subprocess event")
            .unwrap();
        cancel_tx.send(true).unwrap();
        assert!(matches!(
            timeout(std::time::Duration::from_secs(5), turn)
                .await
                .expect("cancelled subprocess exits")
                .unwrap(),
            Err(TurnError::Cancelled)
        ));
    }
}
