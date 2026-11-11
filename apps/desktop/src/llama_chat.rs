//! Local AI chat assistant powered by llama.cpp + a small Qwen GGUF.
//!
//! The architecture mirrors how the rest of the engine talks to
//! external runtimes: we don't link the inference engine into our
//! binary; we shell out to a pre-built `llama-server` that's
//! installed (alongside the model file) into the user's app-data
//! directory by `engine_manager::install("llamacpp", ...)`.
//!
//! The server exposes an OpenAI-compatible chat-completions API on
//! `http://127.0.0.1:<port>/v1/chat/completions`, so chat streaming
//! is the same SSE parse loop our `xf.ai.llm` connector already uses.
//!
//! Lifecycle (managed by lib.rs):
//!   - First chat message: spawn server (lazy boot)
//!   - Subsequent messages: reuse the running server
//!   - App shutdown: kill child

use std::io::{BufRead, BufReader, Read};
use std::net::{IpAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// System prompt that teaches Qwen to emit Duckle pipeline JSON when
/// the user asks for one. Kept terse so it doesn't eat the model's
/// 4k context window; lists only the most common components since
/// the Qwen 1.5B model can't reliably distinguish 300 vendor tiles.
pub const SYSTEM_PROMPT: &str = r#"You are Duckie, the AI assistant inside Duckle (open-source ETL/ELT: pipelines you author and deploy to your own servers or cloud). When the user asks for a data pipeline, output ONE valid JSON pipeline inside a ```json fenced code block, then a one-sentence summary. Every transform node MUST have all of its required properties filled in with real column names from the request. NEVER emit a node with an empty "properties" object.

Pipeline schema (note: the key is "properties", and "type" holds the component id):
{
  "nodes": [
    { "id": "<short-id>", "type": "<component-id>", "data": { "label": "<name>", "properties": { ... } } }
  ],
  "edges": [
    { "id": "<edge-id>", "source": "<node-id>", "target": "<node-id>", "sourceHandle": "main", "targetHandle": "main" }
  ]
}

Component IDs (use these exact strings):
- Sources: src.csv, src.json, src.parquet, src.excel, src.postgres, src.mysql, src.sqlite, src.duckdb, src.s3, src.rest
- Transforms: xf.filter, xf.groupby, xf.sort, xf.topn, xf.addcol, xf.cast, xf.distinct, xf.join.inner, xf.join.left, xf.lookup, xf.union, xf.rename, xf.project
- Sinks: snk.csv, snk.json, snk.parquet, snk.postgres, snk.mysql, snk.s3, snk.duckdb
- Code: code.sql, code.shell, code.javascript

Required properties per node (fill ALL of them with real names from the user's request):
- src.* / snk.* file nodes: { "path": "data.csv" }
- xf.filter: { "predicate": "amount > 100" }
- xf.groupby: { "groupKeys": ["customer"], "aggregations": [ { "column": "sales", "function": "sum", "alias": "total_sales" } ] }   (function is one of: sum, avg, min, max, count)
- xf.sort: { "sortColumn": "total_sales", "direction": "desc" }   (direction is "asc" or "desc"; sorts on ONE column)
- xf.topn: { "count": 10 }   (keep the first N rows; put a Sort before it to get "top N")
- xf.addcol: { "name": "profit", "expression": "revenue - cost" }
- xf.join.inner / xf.join.left: { "leftKey": "id", "rightKey": "id" }   (wire the second table into targetHandle "lookup")
- code.sql: { "sql": "SELECT ... FROM input" }   (refer to the upstream node as input)

Worked example - "retrieve the top 10 customers by sales amount, highest first":
```json
{
  "nodes": [
    { "id": "s1", "type": "src.csv", "data": { "label": "Sales", "properties": { "path": "sales.csv" } } },
    { "id": "t1", "type": "xf.groupby", "data": { "label": "Sales per customer", "properties": { "groupKeys": ["customer"], "aggregations": [ { "column": "sales", "function": "sum", "alias": "total_sales" } ] } } },
    { "id": "t2", "type": "xf.sort", "data": { "label": "Highest first", "properties": { "sortColumn": "total_sales", "direction": "desc" } } },
    { "id": "t3", "type": "xf.topn", "data": { "label": "Top 10", "properties": { "count": 10 } } },
    { "id": "k1", "type": "snk.csv", "data": { "label": "Result", "properties": { "path": "top_customers.csv" } } }
  ],
  "edges": [
    { "id": "e1", "source": "s1", "target": "t1", "sourceHandle": "main", "targetHandle": "main" },
    { "id": "e2", "source": "t1", "target": "t2", "sourceHandle": "main", "targetHandle": "main" },
    { "id": "e3", "source": "t2", "target": "t3", "sourceHandle": "main", "targetHandle": "main" },
    { "id": "e4", "source": "t3", "target": "k1", "sourceHandle": "main", "targetHandle": "main" }
  ]
}
```

Connect sources to transforms to sinks via main edges. Keep IDs short (s1, t1, k1). If the user is just chatting, reply conversationally without JSON.
"#;

/// llama-server is a separate process; we manage its lifecycle here.
pub struct LlamaServer {
    child: Child,
    port: u16,
}

impl LlamaServer {
    /// Spawn the server with the Qwen model loaded. Picks a free port
    /// (lets the OS choose by binding to :0 then dropping). Waits up
    /// to 30s for the /health endpoint to return ready.
    pub fn spawn(bin: &PathBuf, model: &PathBuf) -> Result<Self, String> {
        if !bin.exists() {
            return Err(format!(
                "llama-server not installed at {}. Run AI Assistant install.",
                bin.display()
            ));
        }
        if !model.exists() {
            return Err(format!(
                "Qwen model not present at {}. Run AI Assistant install.",
                model.display()
            ));
        }
        // macOS (#89): make sure the downloaded binaries are Gatekeeper-clean and
        // ad-hoc signed before we exec, which also repairs installs done by a
        // Duckle build that predated the signing fix. Marker-gated and a no-op
        // off macOS, so this is essentially free after the first launch.
        if let Some(dir) = bin.parent() {
            crate::engine_manager::ensure_macos_launchable(dir, bin)?;
        }
        // Pick a free port. There's a small TOCTOU race window between
        // close(listener) and the child binding, but localhost is
        // single-user so collisions are rare in practice.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0")
                .map_err(|e| format!("pick port: {}", e))?;
            l.local_addr().unwrap().port()
        };
        let mut cmd = Command::new(bin);
        cmd.arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--model")
            .arg(model)
            // Small context for a small machine - 2048 tokens fits the
            // system prompt + a few conversation turns + a pipeline
            // response. Bump later if users want longer chats.
            .arg("--ctx-size")
            .arg("2048")
            .arg("--threads")
            .arg(num_threads().to_string())
            // Quiet output - we don't capture it but llama-server is
            // very chatty about token sampling.
            .arg("--log-disable");
        cmd.stdout(Stdio::null());
        // #89: capture stderr so a failed boot (missing/corrupt model, port in
        // use, missing shared lib) surfaces in the error instead of a blank
        // "didn't become ready".
        cmd.stderr(Stdio::piped());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn llama-server: {}", e))?;
        // Drain stderr on a thread into a bounded tail (the project's
        // pipe-buffer-deadlock lesson: never read a child pipe only after exit).
        let stderr_tail = Arc::new(Mutex::new(String::new()));
        if let Some(err) = child.stderr.take() {
            let tail = Arc::clone(&stderr_tail);
            std::thread::spawn(move || {
                let mut reader = BufReader::new(err);
                let mut buf = [0u8; 1024];
                while let Ok(n) = reader.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    if let Ok(mut t) = tail.lock() {
                        t.push_str(&String::from_utf8_lossy(&buf[..n]));
                        if t.len() > 4096 {
                            let cut = t.len() - 4096;
                            t.drain(..cut);
                        }
                    }
                }
            });
        }
        let mut server = LlamaServer { child, port };
        // Poll /health until ready, or a (configurable) deadline. Default 120s:
        // a cold load of the GGUF on a slow disk/CPU can exceed the old 30s (#89).
        let timeout = ready_timeout_secs();
        let deadline = Instant::now() + Duration::from_secs(timeout);
        let url = format!("http://127.0.0.1:{}/health", port);
        let tail = || stderr_tail.lock().map(|t| t.trim().to_string()).unwrap_or_default();
        loop {
            if let Ok(resp) = ureq::get(&url).timeout(Duration::from_millis(500)).call() {
                if resp.status() < 400 {
                    return Ok(server);
                }
            }
            // Server died before becoming ready (bad model, port in use, missing
            // lib): report why instead of waiting out the whole deadline.
            if let Ok(Some(code)) = server.child.try_wait() {
                let t = tail();
                let _ = server.kill();
                return Err(format!(
                    "llama-server exited before it was ready ({}). model {}. {}",
                    code,
                    model.display(),
                    if t.is_empty() { "no stderr captured".to_string() } else { format!("stderr: {}", t) }
                ));
            }
            if Instant::now() > deadline {
                let t = tail();
                let _ = server.kill();
                return Err(format!(
                    "llama-server didn't become ready within {}s (port {}, model {}). {} Set DUCKLE_LLAMA_READY_TIMEOUT_SECS to wait longer.",
                    timeout,
                    port,
                    model.display(),
                    if t.is_empty() { String::new() } else { format!("Last stderr: {}", t) }
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Send SIGKILL (or TerminateProcess on Windows) and reap.
    pub fn kill(mut self) -> Result<(), String> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        Ok(())
    }
}

/// #89: how long to wait for llama-server `/health`. Cold GGUF loads can exceed
/// 30s on slow machines; default 120s, override via DUCKLE_LLAMA_READY_TIMEOUT_SECS.
fn ready_timeout_secs() -> u64 {
    std::env::var("DUCKLE_LLAMA_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(120)
}

/// Best-effort thread count for inference. Use half the cores so the
/// rest of the desktop stays responsive.
fn num_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() / 2).max(2))
        .unwrap_or(4)
}

/// State the lib.rs holds: at most one server running at a time.
pub static LLAMA_SERVER: Mutex<Option<LlamaServer>> = Mutex::new(None);

/// One streamed event the frontend reads off a Tauri Channel.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChatEvent {
    /// One token (or short text run) from the model.
    Token { text: String },
    /// Conversation finished cleanly.
    Done,
    /// Something broke mid-stream - send to the user as an error toast.
    Error { message: String },
}

/// One message in a chat conversation. Matches OpenAI's shape so we
/// can forward straight to llama-server's chat completions endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// True when the endpoint is the bundled llama-server on loopback.
///
/// Remote endpoints go through the shared TLS agent so they trust the OS
/// certificate store; loopback deliberately does not, because that agent also
/// applies the configured proxy and a corporate proxy must never capture
/// 127.0.0.1. Parsed as a real address rather than matched as a prefix, so a
/// remote host like `127.0.0.1.example.com` is correctly treated as remote.
fn is_loopback_endpoint(endpoint: &str) -> bool {
    let rest = endpoint.split_once("://").map(|(_, r)| r).unwrap_or(endpoint);
    let authority = rest.split('/').next().unwrap_or("");
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""), // [::1]:8080
        None => authority.split(':').next().unwrap_or(""),
    };
    host == "localhost" || host.parse::<IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
}

/// Send a user message + prior history to the running llama-server,
/// stream tokens out via the `on_event` callback as they arrive. The
/// system prompt is prepended automatically.
/// One entry from an OpenAI-compatible `/v1/models` listing.
#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct ModelChoice {
    pub id: String,
    /// What the endpoint says this model is for. OpenAI itself sends no such
    /// field, so `None` means "an ordinary chat model" rather than "unknown".
    pub kind: Option<String>,
    /// Whether it can answer a chat completion at all, which is the only thing
    /// the assistant can use. See [`usable_for_chat`].
    pub chat: bool,
}

/// Whether a listed model can serve the assistant.
///
/// Vercel's AI Gateway tags every entry with a `type`, and most of them cannot
/// hold a conversation: an embedding, a transcription or an image model would
/// fail on the first request. `typesafe-ai/jev` is tagged `evaluation` and
/// carries `max_tokens: 0` - it returns typed decisions and emits no text at
/// all, so offering it here would be offering a broken assistant. An endpoint
/// that sends no `type` (OpenAI, Ollama, LM Studio, vLLM) is taken at its word.
fn usable_for_chat(kind: Option<&str>) -> bool {
    matches!(kind, None | Some("language") | Some("chat"))
}

/// Parse an OpenAI-compatible `/v1/models` body into choices, chat models
/// first and each group sorted by id.
pub fn parse_models(body: &serde_json::Value) -> Vec<ModelChoice> {
    let items = body
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<ModelChoice> = items
        .iter()
        .filter_map(|m| {
            let id = m.get("id").and_then(|v| v.as_str())?.to_string();
            if id.is_empty() {
                return None;
            }
            let kind = m.get("type").and_then(|v| v.as_str()).map(|s| s.to_string());
            let chat = usable_for_chat(kind.as_deref());
            Some(ModelChoice { id, kind, chat })
        })
        .collect();
    out.sort_by(|a, b| b.chat.cmp(&a.chat).then_with(|| a.id.cmp(&b.id)));
    out.dedup_by(|a, b| a.id == b.id);
    out
}

/// List what an OpenAI-compatible endpoint offers, so a model can be picked
/// rather than typed from memory.
///
/// The key is optional on purpose: Vercel's AI Gateway answers this route
/// unauthenticated, so the list can be browsed before a key exists. OpenAI
/// requires one and says so in its own error, which is passed through.
pub fn list_models(base_url: &str, api_key: Option<&str>) -> Result<Vec<ModelChoice>, String> {
    let endpoint = format!("{}/v1/models", base_url.trim_end_matches('/'));
    // Same split as chat: a remote host needs the shared agent for OS roots and
    // the configured proxy; loopback must not go through that proxy.
    let mut req = if is_loopback_endpoint(&endpoint) {
        ureq::get(&endpoint)
    } else {
        duckle_duckdb_engine::tls::http_agent().get(&endpoint)
    };
    req = req.timeout(Duration::from_secs(30));
    if let Some(key) = api_key.filter(|k| !k.trim().is_empty()) {
        req = req.set("Authorization", &format!("Bearer {}", key));
    }
    let body: serde_json::Value = req
        .call()
        .map_err(|e| format!("list models: {}", e))?
        .into_json()
        .map_err(|e| format!("list models: {}", e))?;
    Ok(parse_models(&body))
}

pub fn chat_stream<F: FnMut(ChatEvent)>(
    endpoint: &str,
    api_key: Option<&str>,
    model: &str,
    history: &[ChatMessage],
    mut on_event: F,
) -> Result<(), String> {
    let mut messages: Vec<serde_json::Value> = Vec::with_capacity(history.len() + 1);
    messages.push(serde_json::json!({
        "role": "system",
        "content": SYSTEM_PROMPT,
    }));
    for m in history {
        messages.push(serde_json::json!({
            "role": m.role,
            "content": m.content,
        }));
    }
    let body = serde_json::json!({
        "model": model,
        "messages": messages,
        "stream": true,
        "temperature": 0.2,
        "top_p": 0.9,
    });
    // #92: works for the local llama-server or an external OpenAI-compatible
    // endpoint (the caller passes the full chat-completions URL + optional key).
    //
    // #183: a remote endpoint has to go through the shared agent so it trusts
    // the OS certificate store. A private corporate CA lives there and not in
    // the bundled Mozilla roots, which is exactly why the same endpoint worked
    // from xf.ai.llm (engine, shared agent) but failed here with
    // "invalid peer certificate: UnknownIssuer". The agent also applies any
    // proxy configured in Settings.
    //
    // The local llama-server stays on a bare ureq call: that proxy applies to
    // every request the agent makes, so routing loopback through a corporate
    // proxy would break local chat for the very users this fixes.
    let mut req = if is_loopback_endpoint(endpoint) {
        ureq::post(endpoint)
    } else {
        duckle_duckdb_engine::tls::http_agent().post(endpoint)
    };
    req = req
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(300));
    if let Some(key) = api_key {
        req = req.set("Authorization", &format!("Bearer {}", key));
    }
    let resp = req
        .send_string(&body.to_string())
        .map_err(|e| format!("chat send: {}", e))?;
    let reader = BufReader::new(resp.into_reader());
    // OpenAI-style SSE: each event is a "data: <json>" line. The
    // final line is "data: [DONE]". Empty lines separate events.
    for line in reader.lines() {
        let Ok(line) = line else { break };
        let Some(payload) = line.strip_prefix("data: ") else {
            continue;
        };
        if payload.trim() == "[DONE]" {
            on_event(ChatEvent::Done);
            return Ok(());
        }
        // Parse the JSON chunk; choices[0].delta.content has the text.
        let chunk: serde_json::Value = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let text = chunk
            .pointer("/choices/0/delta/content")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !text.is_empty() {
            on_event(ChatEvent::Token {
                text: text.to_string(),
            });
        }
        // OpenAI's finish_reason ends the stream too (some servers
        // don't emit [DONE]).
        if chunk
            .pointer("/choices/0/finish_reason")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .is_some()
        {
            on_event(ChatEvent::Done);
            return Ok(());
        }
    }
    on_event(ChatEvent::Done);
    Ok(())
}

/// Pull a JSON pipeline definition out of an assistant message.
/// Looks for a ```json fenced code block and parses its contents
/// as { nodes, edges }. Returns Err if no JSON block, parse fails,
/// or the shape doesn't match.
pub fn extract_pipeline(assistant_text: &str) -> Result<serde_json::Value, String> {
    // Find the first ```json ... ``` block.
    let lower = assistant_text.to_ascii_lowercase();
    let start = lower
        .find("```json")
        .or_else(|| lower.find("```"))
        .ok_or_else(|| "no fenced code block found".to_string())?;
    let after_fence = &assistant_text[start..];
    // Move past the opening fence + language tag + newline.
    let body_start = after_fence
        .find('\n')
        .map(|n| start + n + 1)
        .ok_or_else(|| "unterminated code-block opener".to_string())?;
    let body_after = &assistant_text[body_start..];
    let end = body_after
        .find("```")
        .ok_or_else(|| "unterminated code block".to_string())?;
    let body = &body_after[..end];
    let parsed: serde_json::Value = serde_json::from_str(body.trim())
        .map_err(|e| format!("JSON parse: {}", e))?;
    // Minimum shape check - nodes must be an array.
    if !parsed.get("nodes").map(|v| v.is_array()).unwrap_or(false) {
        return Err("pipeline JSON missing `nodes` array".into());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_pipeline_pulls_json_from_fenced_block() {
        let text = "Sure! Here's a CSV-to-Parquet pipeline:\n\n```json\n{\n  \"nodes\": [{\"id\":\"s\",\"type\":\"src.csv\"},{\"id\":\"k\",\"type\":\"snk.parquet\"}],\n  \"edges\": [{\"source\":\"s\",\"target\":\"k\"}]\n}\n```\n\nLet me know if you want to add a filter.";
        let pipe = extract_pipeline(text).expect("should parse");
        assert_eq!(pipe["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(pipe["edges"].as_array().unwrap().len(), 1);
    }

    /// A listing offers only what can actually hold a conversation.
    ///
    /// Vercel's AI Gateway tags every entry, and most of the 380 it returns are
    /// not chat models. `typesafe-ai/jev` is the one tagged `evaluation`: it
    /// returns typed decisions with probabilities and emits no text at all
    /// (`max_tokens: 0`), and evaluation is not served over chat completions at
    /// all. Offering it as the assistant's model would be offering a control
    /// that cannot work. An endpoint that sends no `type` at all - OpenAI,
    /// Ollama, LM Studio - is taken at its word.
    #[test]
    fn a_listing_separates_models_that_can_chat_from_those_that_cannot() {
        let body = serde_json::json!({
            "data": [
                { "id": "openai/gpt-4.1-mini", "type": "language" },
                { "id": "typesafe-ai/jev", "type": "evaluation", "max_tokens": 0 },
                { "id": "openai/text-embedding-3-small", "type": "embedding" },
                { "id": "anthropic/claude-sonnet-4", "type": "language" },
                { "id": "llama3.1" },
                { "id": "" }
            ]
        });
        let got = super::parse_models(&body);
        let ids: Vec<&str> = got.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "anthropic/claude-sonnet-4",
                "llama3.1",
                "openai/gpt-4.1-mini",
                "openai/text-embedding-3-small",
                "typesafe-ai/jev",
            ],
            "chat models first, each group sorted, and the empty id dropped"
        );
        let jev = got.iter().find(|m| m.id == "typesafe-ai/jev").unwrap();
        assert!(!jev.chat, "an evaluation model cannot answer the assistant");
        assert_eq!(jev.kind.as_deref(), Some("evaluation"), "and it says why");
        assert!(
            got.iter().find(|m| m.id == "llama3.1").unwrap().chat,
            "an endpoint that declares no type is an ordinary chat endpoint"
        );
        assert!(!got.iter().find(|m| m.id == "openai/text-embedding-3-small").unwrap().chat);
    }

    #[test]
    fn loopback_endpoints_bypass_the_shared_agent() {
        // The bundled llama-server. Must stay on a bare ureq call so a proxy
        // configured for corporate egress cannot capture it.
        assert!(is_loopback_endpoint("http://127.0.0.1:8080/v1/chat/completions"));
        assert!(is_loopback_endpoint("http://localhost:1234/v1/chat/completions"));
        assert!(is_loopback_endpoint("http://[::1]:9000/v1/chat/completions"));
        assert!(is_loopback_endpoint("http://127.1.2.3:80/v1"));
    }

    #[test]
    fn remote_endpoints_use_the_shared_agent() {
        // #183: these must go through the shared agent to pick up the OS trust
        // store, or a private corporate CA yields UnknownIssuer.
        assert!(!is_loopback_endpoint("https://llm.internal.example/v1/chat/completions"));
        assert!(!is_loopback_endpoint("https://api.openai.com/v1/chat/completions"));
        // Hosts that merely start with a loopback-looking label are remote. A
        // prefix match would have got both of these wrong.
        assert!(!is_loopback_endpoint("https://localhost.example.com/v1"));
        assert!(!is_loopback_endpoint("https://127.0.0.1.example.com/v1"));
    }

    #[test]
    fn extract_pipeline_handles_unmarked_fence() {
        // Model sometimes uses bare ``` without the json tag.
        let text = "```\n{\"nodes\":[{\"id\":\"a\"}]}\n```";
        let pipe = extract_pipeline(text).expect("should parse");
        assert_eq!(pipe["nodes"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn extract_pipeline_errors_when_no_fence() {
        assert!(extract_pipeline("just chatting, no pipeline here").is_err());
    }

    #[test]
    fn extract_pipeline_errors_when_no_nodes() {
        let text = "```json\n{\"not_a_pipeline\": true}\n```";
        assert!(extract_pipeline(text).is_err());
    }
}
