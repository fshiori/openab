use crate::agent::Agent;
use crate::llm::AnthropicProvider;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub id: Option<u64>,
    pub method: Option<String>,
    pub params: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: &'static str,
    pub method: String,
    pub params: Value,
}

pub struct AcpServer {
    // TODO(v0.2): add session TTL and periodic cleanup to prevent OOM
    sessions: HashMap<String, Agent>,
    working_dir: String,
    /// Active model name (safe alternative to env mutation)
    active_model: Option<String>,
    /// Active provider name: "anthropic" or "openai" (safe alternative to env mutation)
    active_provider: Option<String>,
}

impl AcpServer {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            working_dir: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/tmp".to_string()),
            active_model: None,
            active_provider: None,
        }
    }

    pub async fn run(&mut self) {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        std::thread::spawn(move || {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                #[allow(clippy::collapsible_match)]
                match line {
                    Ok(l) if !l.trim().is_empty() => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        let mut stdout = io::stdout();

        while let Some(line) = rx.recv().await {
            let req: JsonRpcRequest = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let id = match req.id {
                Some(id) => id,
                None => continue,
            };

            let output = match req.method.as_deref() {
                Some("initialize") => vec![self.handle_initialize(id)],
                Some("session/new") => vec![self.handle_session_new(id)],
                Some("session/prompt") => {
                    let params = req.params.unwrap_or(json!({}));
                    self.handle_session_prompt(id, &params).await
                }
                Some("session/cancel") => {
                    // TODO(v0.2): implement cancellation token to abort in-progress agent.run()
                    vec![self.ok_response(id, json!({}))]
                }
                Some("session/set_config_option") => {
                    let params = req.params.unwrap_or(json!({}));
                    vec![self.handle_set_config_option(id, &params)]
                }
                Some(method) => {
                    vec![self.error_response(id, -32601, &format!("method not found: {method}"))]
                }
                None => continue,
            };

            for line in output {
                let _ = writeln!(stdout, "{}", line);
            }
            let _ = stdout.flush();
        }
    }

    fn handle_initialize(&self, id: u64) -> String {
        let resp = JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({
                "protocolVersion": 1,
                "agentInfo": {
                    "name": "openab-agent",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "agentCapabilities": {
                    "streaming": false,
                    "loadSession": false
                }
            })),
            error: None,
        };
        serde_json::to_string(&resp).unwrap()
    }

    fn handle_session_new(&mut self, id: u64) -> String {
        let session_id = Uuid::new_v4().to_string();

        // Use struct config if set, then env, then auto-detect
        let provider_choice = self
            .active_provider
            .clone()
            .or_else(|| std::env::var("OPENAB_AGENT_PROVIDER").ok())
            .unwrap_or_default();
        let (provider, active_provider): (Box<dyn crate::llm::LlmProvider>, &str) =
            match provider_choice.as_str() {
                "anthropic" => match AnthropicProvider::from_env() {
                    Ok(p) => (Box::new(p), "anthropic"),
                    Err(e) => return self.error_response(id, -32000, &e),
                },
                "openai" | "codex" => match crate::llm::OpenAiProvider::from_auth_store() {
                    Ok(p) => (Box::new(p), "openai"),
                    Err(e) => return self.error_response(id, -32000, &e),
                },
                _ => {
                    // Auto-detect: try API key first, then OAuth token
                    match AnthropicProvider::from_env() {
                        Ok(p) => (Box::new(p), "anthropic"),
                        Err(_) => match crate::llm::OpenAiProvider::from_auth_store() {
                            Ok(p) => (Box::new(p), "openai"),
                            Err(e) => {
                                return self.error_response(
                                    id,
                                    -32000,
                                    &format!("No credentials: set ANTHROPIC_API_KEY or run `openab-agent auth codex-oauth`. {e}"),
                                )
                            }
                        },
                    }
                }
            };

        let agent = Agent::new_boxed(provider, self.working_dir.clone());
        self.sessions.insert(session_id.clone(), agent);

        let model_name = self
            .active_model
            .clone()
            .or_else(|| std::env::var("OPENAB_AGENT_MODEL").ok())
            .unwrap_or_else(|| {
                if active_provider == "anthropic" {
                    "claude-sonnet-4-20250514".to_string()
                } else {
                    "gpt-4.1-nano".to_string()
                }
            });

        let resp = JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({
                "sessionId": session_id,
                "configOptions": [{
                    "id": "model",
                    "name": "Model",
                    "category": "model",
                    "type": "enum",
                    "currentValue": model_name,
                    "options": Self::available_models()
                }]
            })),
            error: None,
        };
        serde_json::to_string(&resp).unwrap()
    }

    /// List available models based on configured credentials.
    fn available_models() -> Vec<Value> {
        let mut models = Vec::new();
        if std::env::var("ANTHROPIC_API_KEY").is_ok() {
            models.push(json!({"value": "claude-sonnet-4-20250514", "name": "Claude Sonnet 4"}));
            models.push(json!({"value": "claude-haiku-4-20250514", "name": "Claude Haiku 4"}));
        }
        if crate::auth::load_tokens().is_ok() {
            models.push(json!({"value": "gpt-4.1-nano", "name": "GPT-4.1 Nano"}));
            models.push(json!({"value": "gpt-4.1-mini", "name": "GPT-4.1 Mini"}));
            models.push(json!({"value": "o4-mini", "name": "o4-mini"}));
        }
        if models.is_empty() {
            models.push(json!({"value": "none", "name": "No credentials configured"}));
        }
        models
    }

    async fn handle_session_prompt(&mut self, id: u64, params: &Value) -> Vec<String> {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let prompt_text = params
            .get("prompt")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();

        if prompt_text.trim().is_empty() {
            return vec![self.error_response(id, -32602, "prompt is empty")];
        }

        let agent = match self.sessions.get_mut(session_id) {
            Some(a) => a,
            None => {
                return vec![self.error_response(id, -32600, "unknown session")];
            }
        };

        let mut output_lines = Vec::new();
        let session_id_owned = session_id.to_string();

        match agent.run(&prompt_text).await {
            Ok(response_text) => {
                let notification = serde_json::to_string(&JsonRpcNotification {
                    jsonrpc: "2.0",
                    method: "session/update".to_string(),
                    params: json!({
                        "sessionId": session_id_owned,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": response_text }
                        }
                    }),
                })
                .unwrap();
                output_lines.push(notification);
                output_lines.push(self.ok_response(id, json!({ "stopReason": "end_turn" })));
            }
            Err(e) => {
                output_lines.push(self.error_response(id, -32000, &format!("agent error: {e}")));
            }
        }

        output_lines
    }

    fn handle_set_config_option(&mut self, id: u64, params: &Value) -> String {
        let config_id = params
            .get("configId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let value = params.get("value").and_then(|v| v.as_str()).unwrap_or("");
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if config_id != "model" || value.is_empty() {
            return self.error_response(id, -32602, "unsupported configId or empty value");
        }

        // Validate model against available options
        let models = Self::available_models();
        let valid = models
            .iter()
            .any(|m| m.get("value").and_then(|v| v.as_str()) == Some(value));
        if !valid {
            return self.error_response(
                id,
                -32602,
                &format!("unknown model: {value}. Use one from available_models."),
            );
        }

        // Determine provider from model name
        let provider_name = if value.starts_with("claude") {
            "anthropic"
        } else {
            "openai"
        };

        // Store in struct (safe — no env mutation)
        self.active_model = Some(value.to_string());
        self.active_provider = Some(provider_name.to_string());

        // Rebuild the current session's provider so the switch takes effect immediately
        if !session_id.is_empty() {
            if let Some(_agent) = self.sessions.remove(session_id) {
                let new_provider: Result<Box<dyn crate::llm::LlmProvider>, String> =
                    match provider_name {
                        "anthropic" => AnthropicProvider::from_env().map(|p| Box::new(p) as _),
                        _ => {
                            crate::llm::OpenAiProvider::from_auth_store().map(|p| Box::new(p) as _)
                        }
                    };
                if let Ok(p) = new_provider {
                    let agent = Agent::new_boxed(p, self.working_dir.clone());
                    self.sessions.insert(session_id.to_string(), agent);
                }
            }
        }

        self.ok_response(
            id,
            json!({
                "configOptions": [{
                    "id": "model",
                    "name": "Model",
                    "category": "model",
                    "type": "enum",
                    "currentValue": value,
                    "options": models
                }]
            }),
        )
    }

    fn ok_response(&self, id: u64, result: Value) -> String {
        serde_json::to_string(&JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        })
        .unwrap()
    }

    fn error_response(&self, id: u64, code: i64, message: &str) -> String {
        serde_json::to_string(&JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(json!({ "code": code, "message": message })),
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initialize_response() {
        let server = AcpServer::new();
        let resp_str = server.handle_initialize(1);
        let resp: Value = serde_json::from_str(&resp_str).unwrap();
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 1);
        assert_eq!(resp["result"]["agentInfo"]["name"], "openab-agent");
        assert_eq!(resp["result"]["agentCapabilities"]["streaming"], false);
    }

    #[test]
    fn test_session_new() {
        // Set a fake key so from_env() succeeds in CI
        unsafe { std::env::set_var("ANTHROPIC_API_KEY", "test-key") };
        let mut server = AcpServer::new();
        let resp_str = server.handle_session_new(2);
        let resp: Value = serde_json::from_str(&resp_str).unwrap();
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 2);
        assert!(resp["result"]["sessionId"].as_str().unwrap().len() > 0);
        // Verify configOptions are returned for /models support
        let config_options = resp["result"]["configOptions"].as_array().unwrap();
        assert!(!config_options.is_empty());
        assert_eq!(config_options[0]["id"], "model");
        assert_eq!(config_options[0]["category"], "model");
        assert!(!config_options[0]["options"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_session_new_missing_key() {
        // Ensure no OAuth token exists either
        let auth_path =
            std::path::PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string()))
                .join(".openab/agent/auth.json");
        let _ = std::fs::remove_file(&auth_path);
        unsafe { std::env::remove_var("ANTHROPIC_API_KEY") };
        let mut server = AcpServer::new();
        let resp_str = server.handle_session_new(3);
        let resp: Value = serde_json::from_str(&resp_str).unwrap();
        assert!(resp["error"].is_object());
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("ANTHROPIC_API_KEY"));
    }
}
