//! duckle-mcp: a Model Context Protocol server for Duckle.
//!
//! Speaks newline-delimited JSON-RPC 2.0 over stdio (the MCP stdio transport):
//! one JSON message per line on stdin, one JSON response per line on stdout.
//! Any MCP client (Claude Desktop / Claude Code, etc.) can connect and:
//!   - browse the component catalog and per-component property schemas,
//!   - generate a pipeline straight into a chosen working directory,
//!   - validate (compile without running) and run pipelines headlessly,
//!   - read existing pipelines and their run logs,
//!   - build a standalone single-file artifact, and
//!   - list / create workspace saved connections.
//!
//! The server reuses the DuckDB engine in-process for validate + run, so it
//! needs no GUI and no Node runtime. The component catalog is embedded from a
//! committed catalog.json exported from the frontend manifest.

use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

mod catalog;
mod tools;

/// MCP protocol revision this server implements. 2024-11-05 is broadly
/// supported by current clients.
const PROTOCOL_VERSION: &str = "2024-11-05";

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // stdin closed
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                write_error(&mut out, Value::Null, -32700, &format!("parse error: {e}"));
                continue;
            }
        };

        let method = msg
            .get("method")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();
        let params = msg.get("params").cloned().unwrap_or(Value::Null);

        // A request has an id; a notification (e.g. notifications/initialized)
        // does not, and gets no reply.
        let id = match msg.get("id") {
            Some(i) if !i.is_null() => i.clone(),
            _ => continue,
        };

        match dispatch(&method, params) {
            Ok(result) => write_response(&mut out, id, result),
            Err((code, message)) => write_error(&mut out, id, code, &message),
        }
    }
}

/// Route a JSON-RPC method to its handler. Returns the `result` value on
/// success or a `(code, message)` JSON-RPC error on failure.
fn dispatch(method: &str, params: Value) -> Result<Value, (i64, String)> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": {}, "resources": {}, "prompts": {} },
            "serverInfo": { "name": "duckle-mcp", "version": duckle_duckdb_engine::VERSION },
            "instructions": tools::INSTRUCTIONS
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools::list_tools() })),
        "tools/call" => tools::call_tool(params),
        "resources/list" => Ok(json!({ "resources": tools::list_resources() })),
        "resources/read" => tools::read_resource(params),
        "prompts/list" => Ok(json!({ "prompts": tools::list_prompts() })),
        "prompts/get" => tools::get_prompt(params),
        other => Err((-32601, format!("method not found: {other}"))),
    }
}

fn write_response(out: &mut impl Write, id: Value, result: Value) {
    write_msg(out, json!({ "jsonrpc": "2.0", "id": id, "result": result }));
}

fn write_error(out: &mut impl Write, id: Value, code: i64, message: &str) {
    write_msg(
        out,
        json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }),
    );
}

/// Write one JSON-RPC message as a single newline-terminated line and flush,
/// per the MCP stdio framing (messages must not contain embedded newlines).
fn write_msg(out: &mut impl Write, v: Value) {
    if let Ok(s) = serde_json::to_string(&v) {
        let _ = out.write_all(s.as_bytes());
        let _ = out.write_all(b"\n");
        let _ = out.flush();
    }
}
