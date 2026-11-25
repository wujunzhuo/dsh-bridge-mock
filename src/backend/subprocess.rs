use std::{env, fs, path::PathBuf, process::Stdio, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
};
use uuid::Uuid;

use crate::{
    backend::{TurnRequest, TurnRunner},
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
    ) -> Result<String, String> {
        let model_patch = TemporaryModelPatch::create(&request.model)?;
        let prompt = format_prompt(&request.history, &request.prompt);
        let mut child = Command::new(&self.command)
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
            .kill_on_drop(true)
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
            if let Err(error) = stdin.write_all(prompt.as_bytes()).await {
                let _ = child.start_kill();
                let _ = child.wait().await;
                if let Some(task) = stderr_task {
                    let _ = task.await;
                }
                return Err(format!("failed to send prompt to dsh: {error}"));
            }
            let _ = stdin.shutdown().await;
        }

        let Some(stdout) = child.stdout.take() else {
            let _ = child.start_kill();
            let _ = child.wait().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            return Err("failed to read dsh stdout".into());
        };

        let mut lines = BufReader::new(stdout).lines();
        let mut final_answer: Option<String> = None;
        let mut dsh_error: Option<String> = None;
        let mut read_error = None;

        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let event = serde_json::from_str::<Value>(&line)
                        .unwrap_or_else(|_| serde_json::json!({"text": line}));
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
                    output
                        .publish_event(&request.session_id, &request.turn_id, event)
                        .await;
                }
                Ok(None) => break,
                Err(error) => {
                    read_error = Some(format!("failed to read dsh output: {error}"));
                    let _ = child.start_kill();
                    break;
                }
            }
        }

        let status = child.wait().await;
        let stderr = match stderr_task {
            Some(task) => task.await.unwrap_or_default(),
            None => String::new(),
        };
        if let Some(error) = read_error {
            return Err(error);
        }
        if status.as_ref().is_ok_and(|status| status.success()) {
            return Ok(final_answer.unwrap_or_default());
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
            Err(format!("dsh command exited unsuccessfully:\n{detail}"))
        }
    }
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
    use super::format_prompt;
    use crate::session::{ConversationMessage, MessageRole};

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
}
