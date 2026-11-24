//! Chat models for the swarm, behind one small interface.
//!
//! Two wire protocols reach nearly every model a person can use: Anthropic's
//! Messages API (Claude, by API key or by the `ant` CLI's sign-in), and the
//! Chat Completions protocol that OpenAI, Hugging Face's router, Ollama,
//! LM Studio, vLLM and OpenRouter all speak. Both are streamed, so the swarm
//! view shows an agent's words as they arrive.
//!
//! A conversation is kept in the provider's own message format and handed
//! back unchanged, so nothing the model produced (Claude's thinking blocks
//! and their signatures included) is rewritten between turns.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The error a turn ends with when the swarm was stopped.
pub const CANCELLED: &str = "stopped";

/// The Messages API version every request names.
const ANTHROPIC_VERSION: &str = "2026-10-21";
pub const ANTHROPIC_BASE: &str = "https://api.anthropic.com";

/// One tool the model may call.
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub schema: Value,
}

/// A tool call the model asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// The arguments, or why they could not be read.
    pub input: Result<Value, String>,
}

/// What one tool call answered, for the model's next turn.
pub struct ToolOutput {
    pub id: String,
    pub text: String,
    pub is_error: bool,
}

/// Why a turn ended.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Stop {
    /// The model finished what it had to say.
    #[default]
    Done,
    /// It wants its tool calls run.
    Tools,
    /// It ran out of output tokens partway.
    Truncated,
    /// It declined, with the category when the provider gives one.
    Refused(String),
}

/// One assistant turn, already appended to the conversation.
#[derive(Debug, Default)]
pub struct Turn {
    pub text: String,
    pub calls: Vec<ToolCall>,
    pub stop: Stop,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// What streams out of a turn while it is being written.
pub enum Delta<'a> {
    Text(&'a str),
    Thinking(&'a str),
    /// The turn failed partway and is being asked for again: drop what
    /// arrived of it.
    Retry(&'a str),
}

pub trait Model {
    /// Ask for the next assistant turn and append it to `history`.
    fn turn(
        &mut self,
        system: &str,
        tools: &[ToolSpec],
        history: &mut Vec<Value>,
        on: &mut dyn FnMut(Delta),
        cancel: &AtomicBool,
    ) -> Result<Turn, String>;
    /// Append the answers to the last turn's tool calls.
    fn tool_results(&self, history: &mut Vec<Value>, results: &[ToolOutput]);
}

/// A plain user message, the same in both protocols.
pub fn user_message(text: &str) -> Value {
    json!({ "role": "user", "content": text })
}

// ---------------------------------------------------------------------------
// Anthropic
// ---------------------------------------------------------------------------

pub enum AnthropicAuth {
    Key(String),
    /// The `ant` CLI's sign-in: a short-lived token, asked for before each
    /// turn so a long run never sends one that has run out.
    Account(PathBuf),
}

pub struct Anthropic {
    pub base_url: String,
    pub model: String,
    pub auth: AnthropicAuth,
    pub effort: Option<String>,
}

/// Whether a Claude model takes adaptive thinking and an effort level.
fn adaptive(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    [
        "claude-opus-5",
        "claude-sonnet-5",
        "claude-haiku-5",
        "claude-fable-5",
        "claude-mythos-5",
    ]
    .iter()
    .any(|p| m.starts_with(p))
}

/// Whether the API can rerun a turn a safety classifier declined on another
/// model. Without it a false positive ends the agent.
fn server_fallback(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    ["claude-opus-5", "claude-sonnet-5", "claude-fable-5"]
        .iter()
        .any(|p| m.starts_with(p))
}

impl Anthropic {
    pub fn body(&self, system: &str, tools: &[ToolSpec], history: &[Value]) -> Value {
        let tools: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.schema,
                    "eager_input_streaming": true,
                })
            })
            .collect();
        let mut body = json!({
            "model": self.model,
            "max_tokens": 32000,
            "stream": true,
            "system": system,
            "tools": tools,
            "messages": history,
            // Each turn re-sends the whole conversation; caching it makes the
            // repeat cheap.
            "cache_control": { "type": "ephemeral" },
        });
        if adaptive(&self.model) {
            body["thinking"] = json!({ "type": "adaptive", "display": "summarized" });
            body["output_config"] = json!({ "effort": self.effort.as_deref().unwrap_or("medium") });
        }
        if server_fallback(&self.model) {
            body["fallbacks"] = json!("default");
        }
        body
    }

    fn headers(&self) -> Result<Vec<(String, String)>, String> {
        let mut headers = vec![
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ];
        let mut betas = Vec::new();
        if server_fallback(&self.model) {
            betas.push("server-side-fallback-2026-07-01");
        }
        match &self.auth {
            AnthropicAuth::Key(key) => headers.push(("x-api-key".to_string(), key.clone())),
            AnthropicAuth::Account(bin) => {
                let token = ant_token(bin)?;
                headers.push(("authorization".to_string(), format!("Bearer {token}")));
                betas.push("oauth-2025-04-20");
            }
        }
        if !betas.is_empty() {
            headers.push(("anthropic-beta".to_string(), betas.join(",")));
        }
        Ok(headers)
    }
}

impl Model for Anthropic {
    fn turn(
        &mut self,
        system: &str,
        tools: &[ToolSpec],
        history: &mut Vec<Value>,
        on: &mut dyn FnMut(Delta),
        cancel: &AtomicBool,
    ) -> Result<Turn, String> {
        let url = format!("{}/v1/messages", self.base_url.trim_end_matches('/'));
        let body = self.body(system, tools, history).to_string();
        let account = matches!(self.auth, AnthropicAuth::Account(_));
        let (turn, content) = with_retries(cancel, on, |on| {
            let headers = self.headers().map_err(Failure::Status)?;
            let mut stream = AnthropicStream::default();
            post_sse(&url, &headers, &body, cancel, &mut |event, data| {
                stream.feed(event, data, on)
            })?;
            Ok(stream.finish())
        })
        .map_err(|e| anthropic_hint(e, account))?;
        history.push(json!({ "role": "assistant", "content": content }));
        Ok(turn)
    }

    fn tool_results(&self, history: &mut Vec<Value>, results: &[ToolOutput]) {
        let blocks: Vec<Value> = results
            .iter()
            .map(|r| {
                json!({
                    "type": "tool_result",
                    "tool_use_id": r.id,
                    "content": r.text,
                    "is_error": r.is_error,
                })
            })
            .collect();
        history.push(json!({ "role": "user", "content": blocks }));
    }
}

/// A failed request's message, with what to do about the failures a person
/// can fix.
fn anthropic_hint(message: String, account: bool) -> String {
    if message.starts_with("HTTP 401") {
        if account {
            return format!(
                "{message}. Your Anthropic sign-in has run out: run `ant auth login` again."
            );
        }
        return format!("{message}. Check the API key.");
    }
    message
}

/// One streamed Messages API answer, put back together block by block.
#[derive(Default)]
pub(crate) struct AnthropicStream {
    blocks: Vec<Value>,
    partial_json: Vec<String>,
    bad_input: Vec<Option<String>>,
    stop_reason: Option<String>,
    stop_category: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
}

impl AnthropicStream {
    fn slot(&mut self, i: usize) {
        while self.blocks.len() <= i {
            self.blocks.push(json!({}));
            self.partial_json.push(String::new());
            self.bad_input.push(None);
        }
    }

    pub(crate) fn feed(
        &mut self,
        event: &str,
        data: &str,
        on: &mut dyn FnMut(Delta),
    ) -> Result<bool, String> {
        let v: Value = serde_json::from_str(data)
            .map_err(|e| format!("an unreadable event from Anthropic: {e}"))?;
        let kind = v.get("type").and_then(Value::as_str).unwrap_or(event);
        let index = v.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        match kind {
            "message_start" => {
                if let Some(u) = v.pointer("/message/usage") {
                    self.input_tokens = input_tokens(u);
                    self.output_tokens =
                        u.get("output_tokens").and_then(Value::as_u64).unwrap_or(0);
                }
            }
            "content_block_start" => {
                self.slot(index);
                // Kept whole, whatever its type: a block this code does not
                // know (a fallback marker, say) goes back exactly as it came.
                self.blocks[index] = v.get("content_block").cloned().unwrap_or_else(|| json!({}));
            }
            "content_block_delta" => {
                self.slot(index);
                let d = v.get("delta").cloned().unwrap_or(Value::Null);
                let text = |k: &str| d.get(k).and_then(Value::as_str).unwrap_or("").to_string();
                match d.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        let t = text("text");
                        append(&mut self.blocks[index], "text", &t);
                        on(Delta::Text(&t));
                    }
                    Some("thinking_delta") => {
                        let t = text("thinking");
                        append(&mut self.blocks[index], "thinking", &t);
                        on(Delta::Thinking(&t));
                    }
                    Some("signature_delta") => {
                        append(&mut self.blocks[index], "signature", &text("signature"));
                    }
                    Some("input_json_delta") => {
                        self.partial_json[index].push_str(&text("partial_json"))
                    }
                    Some("citations_delta") => {
                        if let Some(c) = d.get("citation") {
                            let block = &mut self.blocks[index];
                            if !block.get("citations").is_some_and(Value::is_array) {
                                block["citations"] = json!([]);
                            }
                            if let Some(list) = block["citations"].as_array_mut() {
                                list.push(c.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                self.slot(index);
                if self.blocks[index].get("type").and_then(Value::as_str) == Some("tool_use") {
                    let raw = self.partial_json[index].trim();
                    if !raw.is_empty() {
                        match serde_json::from_str::<Value>(raw) {
                            Ok(input) if input.is_object() => self.blocks[index]["input"] = input,
                            Ok(_) => {
                                self.bad_input[index] =
                                    Some("the arguments were not an object".into())
                            }
                            Err(e) => {
                                self.bad_input[index] =
                                    Some(format!("the arguments were not valid JSON ({e})"))
                            }
                        }
                    }
                }
            }
            "message_delta" => {
                if let Some(r) = v.pointer("/delta/stop_reason").and_then(Value::as_str) {
                    self.stop_reason = Some(r.to_string());
                }
                if let Some(c) = v
                    .pointer("/delta/stop_details/category")
                    .and_then(Value::as_str)
                {
                    self.stop_category = Some(c.to_string());
                }
                if let Some(n) = v.pointer("/usage/output_tokens").and_then(Value::as_u64) {
                    self.output_tokens = n;
                }
            }
            "error" => {
                let message = v
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("the answer broke off");
                return Err(format!("Anthropic: {message}"));
            }
            _ => {}
        }
        Ok(true)
    }

    /// The turn, and the assistant content to append to the conversation.
    pub(crate) fn finish(self) -> (Turn, Vec<Value>) {
        let mut turn = Turn {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            ..Turn::default()
        };
        for (i, block) in self.blocks.iter().enumerate() {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => turn
                    .text
                    .push_str(block.get("text").and_then(Value::as_str).unwrap_or("")),
                Some("tool_use") => turn.calls.push(ToolCall {
                    id: block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    name: block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    input: match &self.bad_input[i] {
                        Some(why) => Err(why.clone()),
                        None => Ok(block.get("input").cloned().unwrap_or_else(|| json!({}))),
                    },
                }),
                _ => {}
            }
        }
        turn.stop = match self.stop_reason.as_deref() {
            Some("tool_use") => Stop::Tools,
            Some("max_tokens") => Stop::Truncated,
            Some("refusal") => Stop::Refused(self.stop_category.clone().unwrap_or_default()),
            _ => Stop::Done,
        };
        (turn, self.blocks)
    }
}

fn append(block: &mut Value, key: &str, more: &str) {
    let have = block
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    block[key] = Value::String(have + more);
}

/// Everything the model read this turn, cached or not.
fn input_tokens(usage: &Value) -> u64 {
    [
        "input_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    ]
    .iter()
    .filter_map(|k| usage.get(*k).and_then(Value::as_u64))
    .sum()
}

// ---------------------------------------------------------------------------
// OpenAI-compatible Chat Completions
// ---------------------------------------------------------------------------

pub struct OpenAiCompatible {
    /// Up to and including the version segment, e.g. https://api.openai.com/v1.
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
    /// Set once the server refused `stream_options`: token counts are not asked for again.
    pub usage_refused: bool,
}

impl OpenAiCompatible {
    pub fn body(&self, system: &str, tools: &[ToolSpec], history: &[Value]) -> Value {
        let mut messages = vec![json!({ "role": "system", "content": system })];
        messages.extend(history.iter().cloned());
        let tools: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": { "name": t.name, "description": t.description, "parameters": t.schema },
                })
            })
            .collect();
        let mut body =
            json!({ "model": self.model, "stream": true, "messages": messages, "tools": tools });
        // Token counts stream only when asked for.
        if !self.usage_refused {
            body["stream_options"] = json!({ "include_usage": true });
        }
        body
    }
}

impl Model for OpenAiCompatible {
    fn turn(
        &mut self,
        system: &str,
        tools: &[ToolSpec],
        history: &mut Vec<Value>,
        on: &mut dyn FnMut(Delta),
        cancel: &AtomicBool,
    ) -> Result<Turn, String> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let mut headers = vec![("content-type".to_string(), "application/json".to_string())];
        if let Some(key) = &self.api_key {
            headers.push(("authorization".to_string(), format!("Bearer {key}")));
        }
        let (turn, message) = loop {
            let body = self.body(system, tools, history).to_string();
            let answer = with_retries(cancel, on, |on| {
                let mut stream = OpenAiStream::default();
                post_sse(&url, &headers, &body, cancel, &mut |_, data| {
                    stream.feed(data, on)
                })?;
                Ok(stream.finish())
            });
            // A server that forbids fields it does not know names this one: token
            // counts are not worth a failed turn, so it is asked again without them.
            match answer {
                Err(e) if !self.usage_refused && e.contains("stream_options") => {
                    self.usage_refused = true
                }
                answer => break answer?,
            }
        };
        history.push(message);
        Ok(turn)
    }

    fn tool_results(&self, history: &mut Vec<Value>, results: &[ToolOutput]) {
        for r in results {
            let text = if r.is_error {
                format!("Error: {}", r.text)
            } else {
                r.text.clone()
            };
            history.push(json!({ "role": "tool", "tool_call_id": r.id, "content": text }));
        }
    }
}

/// One streamed Chat Completions answer, put back together.
#[derive(Default)]
pub(crate) struct OpenAiStream {
    text: String,
    calls: Vec<(String, String, String)>,
    finish: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
}

impl OpenAiStream {
    pub(crate) fn feed(&mut self, data: &str, on: &mut dyn FnMut(Delta)) -> Result<bool, String> {
        if data.trim() == "[DONE]" {
            return Ok(false);
        }
        let v: Value =
            serde_json::from_str(data).map_err(|e| format!("an unreadable chunk: {e}"))?;
        if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| err.to_string());
            return Err(message);
        }
        if let Some(u) = v.get("usage").filter(|u| u.is_object()) {
            self.input_tokens = u
                .get("prompt_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.input_tokens);
            self.output_tokens = u
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.output_tokens);
        }
        for choice in v
            .get("choices")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let d = choice.get("delta").cloned().unwrap_or(Value::Null);
            if let Some(t) = d.get("content").and_then(Value::as_str) {
                self.text.push_str(t);
                on(Delta::Text(t));
            }
            for key in ["reasoning_content", "reasoning"] {
                if let Some(t) = d.get(key).and_then(Value::as_str) {
                    on(Delta::Thinking(t));
                }
            }
            for (n, call) in d
                .get("tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
            {
                let i = call
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|i| i as usize)
                    .unwrap_or(n);
                while self.calls.len() <= i {
                    self.calls.push(Default::default());
                }
                let slot = &mut self.calls[i];
                if let Some(id) = call
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    slot.0 = id.to_string();
                }
                if let Some(name) = call.pointer("/function/name").and_then(Value::as_str) {
                    if slot.1.is_empty() {
                        slot.1 = name.to_string();
                    }
                }
                match call.pointer("/function/arguments") {
                    Some(Value::String(s)) => slot.2.push_str(s),
                    // A few servers send the arguments as an object.
                    Some(other) if other.is_object() => slot.2 = other.to_string(),
                    _ => {}
                }
            }
            if let Some(f) = choice.get("finish_reason").and_then(Value::as_str) {
                self.finish = Some(f.to_string());
            }
        }
        Ok(true)
    }

    /// The turn, and the assistant message to append to the conversation.
    pub(crate) fn finish(self) -> (Turn, Value) {
        let mut calls = Vec::new();
        let mut wire = Vec::new();
        for (i, (id, name, args)) in self.calls.into_iter().enumerate() {
            // Some local servers leave the id out; the answer must still name it.
            let id = if id.is_empty() {
                format!("call_{i}")
            } else {
                id
            };
            let input = if args.trim().is_empty() {
                Ok(json!({}))
            } else {
                match serde_json::from_str::<Value>(&args) {
                    Ok(v) if v.is_object() => Ok(v),
                    Ok(_) => Err("the arguments were not an object".to_string()),
                    Err(e) => Err(format!("the arguments were not valid JSON ({e})")),
                }
            };
            wire.push(json!({
                "id": id,
                "type": "function",
                "function": { "name": name, "arguments": if args.is_empty() { "{}".to_string() } else { args } },
            }));
            calls.push(ToolCall { id, name, input });
        }
        let stop = match self.finish.as_deref() {
            Some("length") => Stop::Truncated,
            _ if !calls.is_empty() => Stop::Tools,
            Some("content_filter") => Stop::Refused("content_filter".to_string()),
            _ => Stop::Done,
        };
        let mut message = json!({ "role": "assistant", "content": self.text });
        if !wire.is_empty() {
            message["tool_calls"] = Value::Array(wire);
        }
        let turn = Turn {
            text: self.text,
            calls,
            stop,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
        };
        (turn, message)
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

pub(crate) enum Failure {
    /// Worth asking again: a busy server, a dropped connection.
    Retry(String, Option<Duration>),
    /// Asking again would get the same answer.
    Status(String),
    Cancelled,
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        if message == CANCELLED {
            Failure::Cancelled
        } else {
            Failure::Retry(message, None)
        }
    }
}

/// Run `attempt` up to three times while its failure is worth retrying.
fn with_retries<T>(
    cancel: &AtomicBool,
    on: &mut dyn FnMut(Delta),
    mut attempt: impl FnMut(&mut dyn FnMut(Delta)) -> Result<T, Failure>,
) -> Result<T, String> {
    let mut tries = 0u64;
    loop {
        tries += 1;
        match attempt(on) {
            Ok(t) => return Ok(t),
            Err(Failure::Retry(message, wait)) if tries < 3 && !cancel.load(Ordering::Relaxed) => {
                on(Delta::Retry(&message));
                let wait = wait
                    .unwrap_or(Duration::from_secs(2 * tries))
                    .min(Duration::from_secs(30));
                let until = std::time::Instant::now() + wait;
                while std::time::Instant::now() < until {
                    if cancel.load(Ordering::Relaxed) {
                        return Err(CANCELLED.to_string());
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            Err(Failure::Retry(message, _)) | Err(Failure::Status(message)) => return Err(message),
            Err(Failure::Cancelled) => return Err(CANCELLED.to_string()),
        }
    }
}

/// POST `body` and hand each Server-Sent Event of the answer to `on`.
fn post_sse(
    url: &str,
    headers: &[(String, String)],
    body: &str,
    cancel: &AtomicBool,
    on: &mut dyn FnMut(&str, &str) -> Result<bool, String>,
) -> Result<(), Failure> {
    if cancel.load(Ordering::Relaxed) {
        return Err(Failure::Cancelled);
    }
    // A read may stall while the model thinks, so the per-read deadline is long.
    let (read, connect) = (Duration::from_secs(600), Duration::from_secs(30));
    let agent = if is_loopback(url) {
        // A model served on this machine (Ollama, LM Studio) is not reachable
        // through a proxy, so it is asked directly.
        ureq::AgentBuilder::new()
            .timeout_read(read)
            .timeout_connect(connect)
            .build()
    } else {
        // The engine's agent, so a proxy and the trusted roots set for Duckle
        // apply here too.
        duckle_duckdb_engine::tls::http_agent_with(&duckle_duckdb_engine::tls::HttpTransport {
            read_timeout_secs: Some(read.as_secs()),
            connect_timeout_secs: Some(connect.as_secs()),
            user_agent: Some(format!("duckle/{}", duckle_duckdb_engine::VERSION)),
            ..Default::default()
        })
    };
    let mut request = agent.post(url);
    for (k, v) in headers {
        request = request.set(k, v);
    }
    let response = match request.send_string(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(code, r)) => {
            let wait = r
                .header("retry-after")
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(Duration::from_secs);
            let text = r.into_string().unwrap_or_default();
            let message = status_message(code, &text);
            return Err(
                if matches!(code, 408 | 409 | 429 | 500 | 502 | 503 | 504 | 529) {
                    Failure::Retry(message, wait)
                } else {
                    Failure::Status(message)
                },
            );
        }
        Err(ureq::Error::Transport(t)) => return Err(Failure::Retry(format!("{url}: {t}"), None)),
    };
    read_sse(response.into_reader(), cancel, on).map_err(Failure::from)
}

/// A URL's host, lower-cased, without user info or port.
pub fn host_of(url: &str) -> String {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    match host_port.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => host_port.split(':').next().unwrap_or(""),
    }
    .to_ascii_lowercase()
}

/// Whether a URL names this machine.
pub fn is_loopback(url: &str) -> bool {
    let host = host_of(url);
    host == "localhost"
        || host.ends_with(".localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `HTTP <code>: <the provider's own message>`.
fn status_message(code: u16, body: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let message = parsed
        .as_ref()
        .and_then(|v| {
            v.pointer("/error/message")
                .or_else(|| v.get("message"))
                .or_else(|| v.get("error").filter(|e| e.is_string()))
                .or_else(|| v.get("detail").filter(|e| e.is_string()))
        })
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| body.trim().chars().take(300).collect());
    if message.is_empty() {
        format!("HTTP {code}")
    } else {
        format!("HTTP {code}: {message}")
    }
}

/// Read Server-Sent Events, handing each event's name and data to `on`, which
/// answers false to stop reading. A stop is noticed between lines.
pub(crate) fn read_sse(
    reader: impl Read,
    cancel: &AtomicBool,
    on: &mut dyn FnMut(&str, &str) -> Result<bool, String>,
) -> Result<(), String> {
    let mut lines = BufReader::new(reader);
    let (mut event, mut data, mut line) = (String::new(), String::new(), String::new());
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(CANCELLED.to_string());
        }
        line.clear();
        let n = lines
            .read_line(&mut line)
            .map_err(|e| format!("the answer broke off: {e}"))?;
        let l = line.trim_end_matches(['\r', '\n']);
        if n == 0 || l.is_empty() {
            if !data.is_empty() && !on(&event, &data)? {
                return Ok(());
            }
            event.clear();
            data.clear();
            if n == 0 {
                return Ok(());
            }
        } else if let Some(v) = l.strip_prefix("event:") {
            event = v.trim().to_string();
        } else if let Some(v) = l.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.strip_prefix(' ').unwrap_or(v));
        }
    }
}

// ---------------------------------------------------------------------------
// The `ant` CLI: signing in to an Anthropic account without an API key
// ---------------------------------------------------------------------------

fn hidden(bin: &Path) -> Command {
    let mut cmd = Command::new(bin);
    cmd.stdin(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd
}

/// Anthropic's CLI, where it is installed. Apache Ant's launcher has the same
/// name, so each candidate must say it is Anthropic's.
pub fn find_ant() -> Option<PathBuf> {
    let exe = if cfg!(windows) { "ant.exe" } else { "ant" };
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(p) = std::env::var_os("DUCKLE_ANT_BIN") {
        candidates.push(PathBuf::from(p));
    }
    if let Some(path) = std::env::var_os("PATH") {
        candidates.extend(std::env::split_paths(&path).map(|d| d.join(exe)));
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        candidates.push(
            PathBuf::from(local)
                .join("Programs")
                .join("anthropic-cli")
                .join(exe),
        );
    }
    if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        let home = PathBuf::from(home);
        candidates.push(home.join("go").join("bin").join(exe));
        candidates.push(home.join(".local").join("bin").join(exe));
    }
    if !cfg!(windows) {
        candidates.push(PathBuf::from("/opt/homebrew/bin/ant"));
        candidates.push(PathBuf::from("/usr/local/bin/ant"));
    }
    candidates.into_iter().find(|p| {
        p.is_file()
            && hidden(p)
                .arg("--version")
                .output()
                .map(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .trim_start()
                        .starts_with("ant version")
                })
                .unwrap_or(false)
    })
}

/// A current access token for the signed-in account. `print-credentials`
/// renews it when it has run out.
pub fn ant_token(bin: &Path) -> Result<String, String> {
    let out = hidden(bin)
        .args(["auth", "print-credentials", "--access-token"])
        .output()
        .map_err(|e| format!("running {}: {e}", bin.display()))?;
    let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() && !token.is_empty() && !token.contains(char::is_whitespace) {
        Ok(token)
    } else {
        Err(
            "Not signed in to an Anthropic account on this computer. Run `ant auth login`, \
             approve in the browser, then start the swarm again."
                .to_string(),
        )
    }
}

/// Whether `ant` has an account signed in. `ant auth status` sends no request
/// and always exits 0; only its Credentials block says who is signed in.
pub fn ant_signed_in(bin: &Path) -> bool {
    let Ok(out) = hidden(bin).args(["auth", "status"]).output() else {
        return false;
    };
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push('\n');
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    signed_in_from_status(&text)
}

pub(crate) fn signed_in_from_status(text: &str) -> bool {
    let mut in_block = false;
    for line in text.lines() {
        let t = line.trim();
        if t == "Credentials" {
            in_block = true;
        } else if in_block {
            if t.is_empty() {
                return false;
            }
            if t.starts_with("(active) *") && t.contains("Profile") {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(stream: &str) -> Vec<(String, String)> {
        let mut seen = Vec::new();
        read_sse(stream.as_bytes(), &AtomicBool::new(false), &mut |e, d| {
            seen.push((e.to_string(), d.to_string()));
            Ok(true)
        })
        .unwrap();
        seen
    }

    #[test]
    fn server_sent_events_are_split_on_blank_lines_and_keep_their_names() {
        let seen =
            collect("event: ping\ndata: {}\n\n: a comment\ndata: one\ndata: two\r\n\r\ndata: last");
        assert_eq!(
            seen,
            vec![
                ("ping".to_string(), "{}".to_string()),
                (String::new(), "one\ntwo".to_string()),
                (String::new(), "last".to_string()),
            ]
        );
    }

    /// A stream replayed through the assembler, as the wire delivers it.
    fn anthropic(events: &[Value]) -> (Turn, Vec<Value>, String, String) {
        let mut stream = AnthropicStream::default();
        let (mut text, mut thinking) = (String::new(), String::new());
        for e in events {
            stream
                .feed("", &e.to_string(), &mut |d| match d {
                    Delta::Text(t) => text.push_str(t),
                    Delta::Thinking(t) => thinking.push_str(t),
                    Delta::Retry(_) => {}
                })
                .unwrap();
        }
        let (turn, content) = stream.finish();
        (turn, content, text, thinking)
    }

    #[test]
    fn a_claude_turn_keeps_its_thinking_and_signature_and_reads_its_tool_call() {
        let (turn, content, text, thinking) = anthropic(&[
            json!({"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":90,"output_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Look at the "}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"files."}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig=="}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Checking."}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"inspect_source","input":{}}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"path\": \"da"}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"ta/a.csv\"}"}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":42}}),
            json!({"type":"message_stop"}),
        ]);
        assert_eq!(text, "Checking.");
        assert_eq!(thinking, "Look at the files.");
        assert_eq!(turn.text, "Checking.");
        assert_eq!(turn.stop, Stop::Tools);
        assert_eq!((turn.input_tokens, turn.output_tokens), (100, 42));
        assert_eq!(
            turn.calls,
            vec![ToolCall {
                id: "toolu_1".into(),
                name: "inspect_source".into(),
                input: Ok(json!({"path": "data/a.csv"}))
            }]
        );
        // Handed back unchanged: the thinking block with its signature, and
        // the tool call with the arguments the model wrote.
        assert_eq!(
            content[0],
            json!({"type":"thinking","thinking":"Look at the files.","signature":"sig=="})
        );
        assert_eq!(content[2]["input"], json!({"path": "data/a.csv"}));
    }

    #[test]
    fn a_tool_call_cut_off_mid_arguments_is_reported_rather_than_run_with_less() {
        let (turn, _, _, _) = anthropic(&[
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"create_pipeline","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"name\": \"x\", \"pipeline\": {\"nodes\": ["}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":5}}),
        ]);
        assert_eq!(turn.stop, Stop::Truncated);
        assert!(turn.calls[0]
            .input
            .as_ref()
            .unwrap_err()
            .contains("not valid JSON"));
    }

    #[test]
    fn a_declined_claude_turn_names_its_category() {
        let (turn, _, _, _) = anthropic(&[json!({
            "type":"message_delta",
            "delta":{"stop_reason":"refusal","stop_details":{"category":"cyber"}}
        })]);
        assert_eq!(turn.stop, Stop::Refused("cyber".into()));
    }

    #[test]
    fn an_error_event_mid_stream_fails_the_turn_with_its_message() {
        let mut stream = AnthropicStream::default();
        let err = stream
            .feed(
                "error",
                r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
                &mut |_| {},
            )
            .unwrap_err();
        assert_eq!(err, "Anthropic: Overloaded");
    }

    fn openai(chunks: &[&str]) -> (Turn, Value, String) {
        let mut stream = OpenAiStream::default();
        let mut text = String::new();
        for c in chunks {
            if !stream
                .feed(c, &mut |d| {
                    if let Delta::Text(t) = d {
                        text.push_str(t)
                    }
                })
                .unwrap()
            {
                break;
            }
        }
        let (turn, message) = stream.finish();
        (turn, message, text)
    }

    #[test]
    fn an_openai_turn_joins_tool_call_fragments_by_index() {
        let (turn, message, text) = openai(&[
            r#"{"choices":[{"delta":{"role":"assistant","content":"Let me look."}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"list_workspace_files","arguments":""}}]}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_b","function":{"name":"inspect_source","arguments":"{\"pa"}}]}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"th\":\"a.csv\"}"}}]}}]}"#,
            r#"{"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}"#,
            "[DONE]",
            r#"{"choices":[{"delta":{"content":"after done"}}]}"#,
        ]);
        assert_eq!(text, "Let me look.");
        assert_eq!(turn.stop, Stop::Tools);
        assert_eq!((turn.input_tokens, turn.output_tokens), (7, 3));
        assert_eq!(
            turn.calls[0],
            ToolCall {
                id: "call_a".into(),
                name: "list_workspace_files".into(),
                input: Ok(json!({}))
            }
        );
        assert_eq!(turn.calls[1].input, Ok(json!({"path": "a.csv"})));
        assert_eq!(
            message["tool_calls"][1]["function"]["arguments"],
            json!("{\"path\":\"a.csv\"}")
        );
        assert_eq!(
            message["tool_calls"][0]["function"]["arguments"],
            json!("{}")
        );
    }

    #[test]
    fn a_local_server_that_omits_ids_and_says_stop_still_gets_its_calls_run() {
        // Ollama answers a tool call with finish_reason "stop" and no id.
        let (turn, message, _) = openai(&[
            r#"{"choices":[{"delta":{"tool_calls":[{"function":{"name":"list_components","arguments":{"kind":"sink"}}}]},"finish_reason":"stop"}]}"#,
        ]);
        assert_eq!(turn.stop, Stop::Tools);
        assert_eq!(turn.calls[0].id, "call_0");
        assert_eq!(turn.calls[0].input, Ok(json!({"kind": "sink"})));
        assert_eq!(message["tool_calls"][0]["id"], json!("call_0"));
    }

    #[test]
    fn an_error_chunk_fails_the_turn() {
        let mut stream = OpenAiStream::default();
        let err = stream
            .feed(r#"{"error":{"message":"model not found"}}"#, &mut |_| {})
            .unwrap_err();
        assert_eq!(err, "model not found");
    }

    #[test]
    fn claude_requests_ask_for_summarized_thinking_and_fallbacks_only_where_they_exist() {
        let tools = [ToolSpec {
            name: "t".into(),
            description: "d".into(),
            schema: json!({"type":"object"}),
        }];
        let opus = Anthropic {
            base_url: ANTHROPIC_BASE.into(),
            model: "claude-opus-5-5".into(),
            auth: AnthropicAuth::Key("k".into()),
            effort: None,
        };
        let body = opus.body("sys", &tools, &[user_message("hi")]);
        assert_eq!(
            body["thinking"],
            json!({"type":"adaptive","display":"summarized"})
        );
        assert_eq!(body["output_config"], json!({"effort":"medium"}));
        assert_eq!(body["fallbacks"], json!("default"));
        assert_eq!(body["tools"][0]["eager_input_streaming"], json!(true));
        assert_eq!(body["tools"][0]["input_schema"], json!({"type":"object"}));
        let headers = opus.headers().unwrap();
        assert!(headers.contains(&(
            "anthropic-beta".into(),
            "server-side-fallback-2026-07-01".into()
        )));
        assert!(headers.contains(&("x-api-key".into(), "k".into())));

        let haiku = Anthropic {
            model: "claude-haiku-5-5".into(),
            ..opus
        };
        let body = haiku.body("sys", &tools, &[]);
        assert!(
            body.get("fallbacks").is_none(),
            "Haiku 5.5 has no server-side fallback"
        );
        assert!(body.get("thinking").is_some());
        assert!(!haiku
            .headers()
            .unwrap()
            .iter()
            .any(|(k, _)| k == "anthropic-beta"));
    }

    #[test]
    fn tool_answers_go_back_in_each_protocols_own_shape() {
        let results = [
            ToolOutput {
                id: "a".into(),
                text: "ok".into(),
                is_error: false,
            },
            ToolOutput {
                id: "b".into(),
                text: "no such file".into(),
                is_error: true,
            },
        ];
        let claude = Anthropic {
            base_url: String::new(),
            model: "claude-opus-5-5".into(),
            auth: AnthropicAuth::Key(String::new()),
            effort: None,
        };
        let mut history = Vec::new();
        claude.tool_results(&mut history, &results);
        assert_eq!(history.len(), 1, "every answer in ONE user message");
        assert_eq!(history[0]["content"][1]["is_error"], json!(true));

        let open = OpenAiCompatible {
            base_url: String::new(),
            model: "m".into(),
            api_key: None,
            usage_refused: false,
        };
        let mut history = Vec::new();
        open.tool_results(&mut history, &results);
        assert_eq!(history.len(), 2);
        assert_eq!(
            history[1],
            json!({"role":"tool","tool_call_id":"b","content":"Error: no such file"})
        );
    }

    #[test]
    fn chat_completions_requests_ask_for_token_usage_and_read_it_from_the_last_chunk() {
        // The counts stream only when asked for, in a chunk of their own after the finish
        // reason, with no choices (the shape Ollama and OpenAI send).
        let open = OpenAiCompatible {
            base_url: String::new(),
            model: "m".into(),
            api_key: None,
            usage_refused: false,
        };
        assert_eq!(
            open.body("s", &[], &[])["stream_options"],
            json!({ "include_usage": true })
        );
        let (turn, _, _) = openai(&[
            r#"{"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}"#,
            r#"{"choices":[],"usage":{"prompt_tokens":31,"completion_tokens":3}}"#,
            "[DONE]",
        ]);
        assert_eq!((turn.input_tokens, turn.output_tokens), (31, 3));
    }

    #[test]
    fn a_server_that_refuses_stream_options_is_asked_without_it_from_then_on() {
        // Some servers forbid fields they do not know (Mistral answers 422 and names it).
        use std::io::{BufRead, BufReader, Read, Write};
        let server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = server.local_addr().unwrap().port();
        let bodies = std::thread::spawn(move || {
            let mut bodies = Vec::new();
            for answer in 0..3 {
                let (mut stream, _) = server.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                bodies.push(serde_json::from_slice::<Value>(&body).unwrap());
                let reply = if answer == 0 {
                    let refusal = r#"{"detail":[{"type":"extra_forbidden","loc":["body","stream_options"],"msg":"Extra inputs are not permitted"}]}"#;
                    format!("HTTP/1.1 422 Unprocessable Entity\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{refusal}", refusal.len())
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_string()
                };
                stream.write_all(reply.as_bytes()).unwrap();
            }
            bodies
        });
        let mut open = OpenAiCompatible {
            base_url: format!("http://127.0.0.1:{port}/v1"),
            model: "m".into(),
            api_key: None,
            usage_refused: false,
        };
        let cancel = AtomicBool::new(false);
        let mut history = vec![json!({"role": "user", "content": "go"})];
        for _ in 0..2 {
            let turn = open
                .turn("s", &[], &mut history, &mut |_| {}, &cancel)
                .unwrap();
            assert_eq!(turn.text, "hi");
        }
        let bodies = bodies.join().unwrap();
        assert_eq!(
            bodies[0]["stream_options"],
            json!({ "include_usage": true })
        );
        assert!(
            bodies[1].get("stream_options").is_none(),
            "asked again without it"
        );
        assert!(bodies[2].get("stream_options").is_none(), "and never again");
    }

    #[test]
    fn signed_in_is_read_from_the_credentials_block_only() {
        // A profile can set its own base URL, and that block then names it.
        let out =
            "Credentials\n  (profile \"default\" not configured \u{2014} run `ant auth login`)\n\n\
                   Base URL\n  (active) * Profile config   https://gateway.example\n";
        assert!(
            !signed_in_from_status(out),
            "an (active) row after the block does not count"
        );
        let signed =
            "Credentials\n  (active) * Profile (user_oauth)   expires 2026-12-01\n\nBase URL\n";
        assert!(signed_in_from_status(signed));
        let key = "Credentials\n  (active) * ANTHROPIC_API_KEY   sk-ant-...\n\n";
        assert!(
            !signed_in_from_status(key),
            "a key is not an account sign-in"
        );
    }

    #[test]
    fn only_this_machine_counts_as_loopback() {
        for local in [
            "http://localhost:11434/v1",
            "http://127.0.0.1:1234/v1",
            "http://[::1]:8000/v1",
            "http://ollama.localhost/v1",
            "http://user:pw@127.0.0.1/v1",
        ] {
            assert!(is_loopback(local), "{local}");
        }
        for remote in [
            "https://api.openai.com/v1",
            "https://router.huggingface.co/v1",
            "http://127.0.0.1.example.com/v1",
            "http://localhost@evil.example/v1",
            "http://10.0.0.5:8000/v1",
        ] {
            assert!(!is_loopback(remote), "{remote}");
        }
    }

    #[test]
    fn a_provider_error_body_becomes_its_message() {
        assert_eq!(
            status_message(
                401,
                r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#
            ),
            "HTTP 401: invalid x-api-key"
        );
        assert_eq!(
            status_message(404, r#"{"error":"model 'x' not found"}"#),
            "HTTP 404: model 'x' not found"
        );
        assert_eq!(status_message(502, ""), "HTTP 502");
        assert_eq!(
            anthropic_hint("HTTP 401: expired".into(), true),
            "HTTP 401: expired. Your Anthropic sign-in has run out: run `ant auth login` again."
        );
    }
}
