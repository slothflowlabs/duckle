//! The `serve` subcommand: a lightweight web management console for running
//! and monitoring Duckle pipelines on a server, with no desktop app.
//!
//! It hosts a small self-contained web panel (embedded HTML, no Node, no extra
//! binary) backed by a tiny std-only HTTP server, so the whole console ships
//! inside the runner you already deploy. The panel has three views:
//!   - Operations: run history across all pipelines (status, duration, rows,
//!     errors) plus per-pipeline run logs.
//!   - Pipelines:  every pipeline in the workspace with its last status and an
//!     editable interval schedule.
//!   - Run:        trigger any pipeline on demand and see the result.
//!
//! Runs execute in-process through the same engine as `duckle-runner run`, are
//! serialized by a single lock (so a manual run and a scheduled run never
//! collide on the shared workspace env), and append the same run history
//! (`<workspace>/runs/<id>.json`) and NDJSON logs (`<workspace>/logs/<id>/`)
//! the desktop and runner already write. A background scheduler triggers any
//! pipeline whose interval has elapsed. No authentication: bind it to a
//! trusted network or localhost.

use duckle_duckdb_engine::{append_run_record, load_run_history, DuckdbEngine, PipelineDoc, RunRecord};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const PANEL_HTML: &str = include_str!("panel.html");

struct ServeArgs {
    host: String,
    port: u16,
    workspace: PathBuf,
    duckdb: Option<PathBuf>,
}

fn parse_serve_args() -> Result<ServeArgs, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8080;
    let mut workspace: Option<PathBuf> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        let mut take = |label: &str| it.next().ok_or_else(|| format!("{} needs a value", label));
        match arg.as_str() {
            "--host" => host = take("--host")?,
            "--port" => {
                port = take("--port")?
                    .parse()
                    .map_err(|_| "--port must be a number".to_string())?
            }
            "--workspace" => workspace = Some(PathBuf::from(take("--workspace")?)),
            "--duckdb" => duckdb = Some(PathBuf::from(take("--duckdb")?)),
            "-h" | "--help" => {
                println!(
                    "duckle-runner serve - web management console\n\n\
                     USAGE:\n    duckle-runner serve [--host <ip>] [--port <n>] [--workspace <dir>] [--duckdb <path>]\n\n\
                     OPTIONS:\n    \
                     --host <ip>        Bind address (default 127.0.0.1; use 0.0.0.0 for remote access)\n    \
                     --port <n>         Port (default 8080)\n    \
                     --workspace <dir>  Workspace root holding pipelines, runs/, logs/ (default: current dir)\n    \
                     --duckdb <path>    DuckDB CLI (default: DUCKLE_DUCKDB_BIN, sibling bin/duckdb, or PATH)\n\n\
                     No authentication. Bind to localhost or a trusted network."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown serve argument: {}", other)),
        }
    }
    let workspace = workspace.unwrap_or_else(|| PathBuf::from("."));
    Ok(ServeArgs { host, port, workspace, duckdb })
}

struct State {
    workspace: PathBuf,
    duckdb: PathBuf,
    /// Serializes pipeline execution: the shared workspace env vars and DuckDB
    /// process make concurrent runs unsafe, so manual + scheduled runs queue.
    run_lock: Mutex<()>,
}

pub fn run() -> Result<(), String> {
    let args = parse_serve_args()?;
    let workspace = args
        .workspace
        .canonicalize()
        .unwrap_or_else(|_| args.workspace.clone());
    let duckdb = crate::resolve_duckdb(args.duckdb.clone())?;

    // Set the workspace env once for the process; runs are serialized so these
    // stay consistent for every execution (matches the runner's run path).
    std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", workspace.join("logs"));

    let state = Arc::new(State { workspace: workspace.clone(), duckdb: duckdb.clone(), run_lock: Mutex::new(()) });

    spawn_scheduler(state.clone());

    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).map_err(|e| format!("bind {}: {}", addr, e))?;
    eprintln!("duckle-runner: management console on http://{}", addr);
    eprintln!("duckle-runner: workspace {}", workspace.display());
    eprintln!("duckle-runner: DuckDB {}", duckdb.display());
    if args.host != "127.0.0.1" && args.host != "localhost" {
        eprintln!("duckle-runner: WARNING - no authentication; exposed on {}", args.host);
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let st = state.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle(s, &st) {
                        eprintln!("duckle-runner: request error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("duckle-runner: accept error: {}", e),
        }
    }
    Ok(())
}

// ── Web editor mode (#75 phase 2 spike): serve the full frontend + an
//    HTTP command bridge so the React editor runs in a browser, backed by the
//    server-side engine/filesystem. Single-tenant, no auth (localhost / proxy).

struct WebArgs {
    host: String,
    port: u16,
    workspace: PathBuf,
    duckdb: Option<PathBuf>,
    dist: PathBuf,
}

fn parse_web_args() -> Result<WebArgs, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8090;
    let mut workspace: Option<PathBuf> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut dist: Option<PathBuf> = None;
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        let mut take = |label: &str| it.next().ok_or_else(|| format!("{} needs a value", label));
        match arg.as_str() {
            "--host" => host = take("--host")?,
            "--port" => {
                port = take("--port")?.parse().map_err(|_| "--port must be a number".to_string())?
            }
            "--workspace" => workspace = Some(PathBuf::from(take("--workspace")?)),
            "--duckdb" => duckdb = Some(PathBuf::from(take("--duckdb")?)),
            "--dist" => dist = Some(PathBuf::from(take("--dist")?)),
            "-h" | "--help" => {
                println!(
                    "duckle-runner web - serve the Duckle editor as a web app (spike)\n\n\
                     USAGE:\n    duckle-runner web --dist <dir> [--host <ip>] [--port <n>] [--workspace <dir>]\n\n\
                     No authentication. Bind to localhost or a trusted network."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown web argument: {}", other)),
        }
    }
    Ok(WebArgs {
        host,
        port,
        workspace: workspace.unwrap_or_else(|| PathBuf::from(".")),
        duckdb,
        dist: dist.ok_or("web mode needs --dist <frontend dist dir>")?,
    })
}

struct WebState {
    workspace: PathBuf,
    duckdb: PathBuf,
    dist: PathBuf,
    /// Serialize runs: the shared workspace env + DuckDB process make concurrent
    /// executions unsafe, so browser run requests queue.
    run_lock: Mutex<()>,
}

pub fn run_web() -> Result<(), String> {
    let args = parse_web_args()?;
    let workspace = args.workspace.canonicalize().unwrap_or_else(|_| args.workspace.clone());
    // Drop the Windows extended-length prefix (\\?\) so the path the browser
    // sees and echoes back in /api/fs calls stays a plain C:\... path.
    let workspace = {
        let s = workspace.to_string_lossy().to_string();
        PathBuf::from(s.strip_prefix(r"\\?\").map(|x| x.to_string()).unwrap_or(s))
    };
    let duckdb = crate::resolve_duckdb(args.duckdb.clone())?;
    let dist = args.dist.canonicalize().map_err(|e| format!("--dist {}: {}", args.dist.display(), e))?;
    std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", workspace.join("logs"));
    let state = Arc::new(WebState {
        workspace: workspace.clone(),
        duckdb: duckdb.clone(),
        dist: dist.clone(),
        run_lock: Mutex::new(()),
    });
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).map_err(|e| format!("bind {}: {}", addr, e))?;
    eprintln!("duckle-runner: web editor on http://{}", addr);
    eprintln!("duckle-runner: workspace {}", workspace.display());
    eprintln!("duckle-runner: serving {}", dist.display());
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let st = state.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_web(s, &st) {
                        eprintln!("duckle-runner: request error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("duckle-runner: accept error: {}", e),
        }
    }
    Ok(())
}

fn handle_web(mut stream: TcpStream, state: &WebState) -> Result<(), String> {
    let req = read_request(&mut stream)?;
    if req.method == "POST" && req.path.starts_with("/api/cmd/") {
        let cmd = req.path.trim_start_matches("/api/cmd/").to_string();
        return dispatch_cmd(&mut stream, state, &cmd, &req.body);
    }
    if req.method == "POST" && req.path.starts_with("/api/fs/") {
        let op = req.path.trim_start_matches("/api/fs/").to_string();
        return dispatch_fs(&mut stream, state, &op, &req.body);
    }
    if req.method == "POST" && req.path == "/api/run_stream" {
        return run_stream(&mut stream, state, &req.body);
    }
    // Static frontend: map the URL path into the dist dir; unknown non-asset
    // paths fall back to index.html (SPA routing).
    serve_static(&mut stream, state, &req.path)
}

/// Server-side filesystem bridge for the web editor. The browser cannot touch
/// the server's disk, so the frontend's workspace file ops (read/write/list)
/// route here. Every path is confined to the workspace dir (no traversal out).
fn dispatch_fs(stream: &mut TcpStream, state: &WebState, op: &str, body: &[u8]) -> Result<(), String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let path_arg = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let target = match confine_to_workspace(&state.workspace, path_arg) {
        Ok(p) => p,
        Err(e) => return respond_err(stream, "400 Bad Request", &e),
    };
    match op {
        "exists" => respond_json(stream, &serde_json::json!({ "exists": target.exists() })),
        "read" => match std::fs::read_to_string(&target) {
            Ok(content) => respond_json(stream, &serde_json::json!({ "content": content })),
            Err(e) => respond_err(stream, "404 Not Found", &e.to_string()),
        },
        "write" => {
            let content = args.get("content").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(parent) = target.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&target, content) {
                Ok(()) => respond_json(stream, &serde_json::json!({ "ok": true })),
                Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
            }
        }
        "mkdir" => match std::fs::create_dir_all(&target) {
            Ok(()) => respond_json(stream, &serde_json::json!({ "ok": true })),
            Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
        },
        "remove" => {
            let r = if target.is_dir() { std::fs::remove_dir_all(&target) } else { std::fs::remove_file(&target) };
            match r {
                Ok(()) => respond_json(stream, &serde_json::json!({ "ok": true })),
                Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
            }
        }
        "readdir" => {
            let mut entries = Vec::new();
            if let Ok(rd) = std::fs::read_dir(&target) {
                for e in rd.flatten() {
                    let ft = e.file_type();
                    entries.push(serde_json::json!({
                        "name": e.file_name().to_string_lossy(),
                        "isFile": ft.as_ref().map(|t| t.is_file()).unwrap_or(false),
                        "isDirectory": ft.as_ref().map(|t| t.is_dir()).unwrap_or(false),
                    }));
                }
            }
            respond_json(stream, &Value::Array(entries))
        }
        _ => respond_err(stream, "404 Not Found", &format!("unknown fs op: {}", op)),
    }
}

/// Resolve `path` (absolute or relative) and ensure it stays inside the
/// workspace. Lexical normalization (no symlink follow needed) is enough since
/// we only ever read/write plain files the editor created.
fn confine_to_workspace(workspace: &Path, path: &str) -> Result<PathBuf, String> {
    if path.is_empty() {
        return Err("path required".into());
    }
    let raw = PathBuf::from(path.replace('\\', "/"));
    let joined = if raw.is_absolute() { raw } else { workspace.join(raw) };
    // Normalize . and .. lexically.
    let mut normalized = PathBuf::new();
    for comp in joined.components() {
        use std::path::Component::*;
        match comp {
            ParentDir => {
                normalized.pop();
            }
            CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    // Compare normalized strings: tolerate \ vs /, the \\?\ prefix, and (on
    // Windows) case so the browser-built path matches the server workspace.
    let norm = |p: &Path| {
        p.to_string_lossy()
            .replace('\\', "/")
            .trim_start_matches("//?/")
            .trim_end_matches('/')
            .to_lowercase()
    };
    if !norm(&normalized).starts_with(&norm(workspace)) {
        return Err("path escapes the workspace".into());
    }
    Ok(normalized)
}

fn dispatch_cmd(stream: &mut TcpStream, state: &WebState, cmd: &str, body: &[u8]) -> Result<(), String> {
    match cmd {
        // Drives the editor's runtime indicator offline -> ready.
        "ping" => respond_json(stream, &Value::String("pong".into())),
        // Connection secrets: pass the payload through unchanged in the web MVP
        // (no at-rest encryption yet; use ${ENV:KEY} for secrets). Echoing the
        // payloadJson keeps the frontend's JSON.parse round-trip lossless -
        // returning null here would blank out the connection's fields on save.
        "connection_encrypt_payload" | "connection_decrypt_payload" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let payload = args.get("payloadJson").and_then(|v| v.as_str()).unwrap_or("null");
            respond_json(stream, &Value::String(payload.to_string()))
        }
        // Execute a pipeline on the server engine and return the RunResult (the
        // same shape the desktop returns). The frontend reads the final result
        // from this response; live per-stage events (the Channel) are not
        // streamed in the MVP. Runs are serialized via run_lock.
        "run_pipeline" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            let name = args.get("pipelineName").and_then(|v| v.as_str()).unwrap_or("web").to_string();
            let _guard = state.run_lock.lock().unwrap_or_else(|p| p.into_inner());
            let engine = DuckdbEngine::new(state.duckdb.clone());
            let result = engine.execute_pipeline_named(&doc, &name);
            match serde_json::to_value(&result) {
                Ok(v) => respond_json(stream, &v),
                Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
            }
        }
        // Compile to per-stage SQL for the Plan tab.
        "compile_pipeline" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            match duckle_duckdb_engine::compile_pipeline_sql(&doc) {
                Ok(stages) => match serde_json::to_value(&stages) {
                    Ok(v) => respond_json(stream, &v),
                    Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
                },
                Err(e) => respond_err(stream, "400 Bad Request", &e.to_string()),
            }
        }
        // Tells the browser editor which server workspace it is editing, so it
        // can auto-load it (there is no native folder picker on the web).
        "web_bootstrap" => respond_json(
            stream,
            &serde_json::json!({ "workspace": state.workspace.to_string_lossy() }),
        ),
        // The browser build skips the engine-setup gate, but answer truthfully.
        "engine_status" => respond_json(
            stream,
            &serde_json::json!([{
                "id": "duckdb",
                "name": "DuckDB",
                "description": "DuckDB engine",
                "required": true,
                "installed": true,
                "outdated": false,
                "version": "1.5.4",
                "target_version": "1.5.4",
                "path": state.duckdb.to_string_lossy(),
                "available": true,
            }]),
        ),
        // Unimplemented commands answer null so the editor (already resilient to
        // backend failures) keeps booting. Real handlers land in the MVP.
        _ => respond_json(stream, &Value::Null),
    }
}

/// Run a pipeline and STREAM its progress to the browser as Server-Sent Events:
/// each engine PipelineEvent is a `data:` line; the final RunResult is an
/// `event: result` line. The frontend turns these back into the same live
/// per-node animation the desktop gets from the Tauri Channel.
fn run_stream(stream: &mut TcpStream, state: &WebState, body: &[u8]) -> Result<(), String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
        Ok(d) => d,
        Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
    };
    let name = args.get("pipelineName").and_then(|v| v.as_str()).unwrap_or("web").to_string();
    // SSE response head (no Content-Length; we stream until the run ends).
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;

    let _guard = state.run_lock.lock().unwrap_or_else(|p| p.into_inner());
    // A second handle to the same socket for the event callback (the run is
    // synchronous, so events stream first, the result line follows).
    let mut ev = stream.try_clone().map_err(|e| e.to_string())?;
    let engine = DuckdbEngine::new(state.duckdb.clone());
    let result = engine.execute_pipeline_with_events(&doc, None, Some(&name), |evt| {
        if let Ok(j) = serde_json::to_string(&evt) {
            let _ = ev.write_all(format!("data: {}\n\n", j).as_bytes());
            let _ = ev.flush();
        }
    });
    let rj = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
    stream
        .write_all(format!("event: result\ndata: {}\n\n", rj).as_bytes())
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(())
}

fn serve_static(stream: &mut TcpStream, state: &WebState, url_path: &str) -> Result<(), String> {
    let rel = url_path.trim_start_matches('/');
    let candidate = if rel.is_empty() { state.dist.join("index.html") } else { state.dist.join(rel) };
    // Confine to the dist dir, and SPA-fallback to index.html for non-asset paths.
    let file = match candidate.canonicalize() {
        Ok(p) if p.starts_with(&state.dist) && p.is_file() => p,
        _ => state.dist.join("index.html"),
    };
    match std::fs::read(&file) {
        Ok(bytes) => respond(stream, "200 OK", web_content_type(&file), &bytes),
        Err(e) => respond_err(stream, "404 Not Found", &format!("{}: {}", file.display(), e)),
    }
}

fn web_content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "wasm" => "application/wasm",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

// ── HTTP (minimal, std-only) ──

struct Request {
    method: String,
    path: String,
    query: HashMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Result<Request, String> {
    // Read until the end of headers (\r\n\r\n), then the body by Content-Length.
    let mut buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 2048];
    let header_end;
    loop {
        let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed before request".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = pos;
            break;
        }
        if buf.len() > 1 << 20 {
            return Err("request headers too large".into());
        }
    }
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or("empty request")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let raw_target = parts.next().unwrap_or("/").to_string();
    let (path, query) = split_query(&raw_target);

    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok(Request { method, path, query, body })
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn split_query(target: &str) -> (String, HashMap<String, String>) {
    let mut q = HashMap::new();
    let (path, qs) = match target.split_once('?') {
        Some((p, s)) => (p.to_string(), s),
        None => (target.to_string(), ""),
    };
    for pair in qs.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        q.insert(url_decode(k), url_decode(v));
    }
    (path, q)
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]));
                if let (Some(a), Some(b)) = h {
                    out.push(a * 16 + b);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) -> Result<(), String> {
    let header = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        content_type,
        body.len()
    );
    stream.write_all(header.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())
}

fn respond_json(stream: &mut TcpStream, value: &Value) -> Result<(), String> {
    respond(stream, "200 OK", "application/json", value.to_string().as_bytes())
}

fn respond_err(stream: &mut TcpStream, status: &str, msg: &str) -> Result<(), String> {
    respond(stream, status, "application/json", json!({ "error": msg }).to_string().as_bytes())
}

fn handle(mut stream: TcpStream, state: &State) -> Result<(), String> {
    let req = read_request(&mut stream)?;
    let route = (req.method.as_str(), req.path.as_str());
    match route {
        ("GET", "/") | ("GET", "/index.html") => {
            respond(&mut stream, "200 OK", "text/html; charset=utf-8", PANEL_HTML.as_bytes())
        }
        ("GET", "/api/summary") => respond_json(&mut stream, &api_summary(state)),
        ("GET", "/api/pipelines") => respond_json(&mut stream, &api_pipelines(state)),
        ("GET", "/api/pipeline") => match req.query.get("file") {
            Some(f) => match read_pipeline_file(state, f) {
                Ok(v) => respond_json(&mut stream, &v),
                Err(e) => respond_err(&mut stream, "404 Not Found", &e),
            },
            None => respond_err(&mut stream, "400 Bad Request", "missing file"),
        },
        ("GET", "/api/runs") => respond_json(&mut stream, &api_runs(state, req.query.get("id").map(|s| s.as_str()))),
        ("GET", "/api/log") => respond_json(&mut stream, &api_log(state, &req.query)),
        ("GET", "/api/schedules") => respond_json(&mut stream, &load_schedules(state)),
        ("POST", "/api/schedules") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            match save_schedule(state, &body) {
                Ok(v) => respond_json(&mut stream, &v),
                Err(e) => respond_err(&mut stream, "400 Bad Request", &e),
            }
        }
        ("POST", "/api/run") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let file = match body.get("file").and_then(|v| v.as_str()) {
                Some(f) => f.to_string(),
                None => return respond_err(&mut stream, "400 Bad Request", "missing file"),
            };
            match execute_one(state, &file, "manual") {
                Ok(v) => respond_json(&mut stream, &v),
                Err(e) => respond_err(&mut stream, "400 Bad Request", &e),
            }
        }
        _ => respond_err(&mut stream, "404 Not Found", "not found"),
    }
}

// ── Pipeline discovery ──

/// Scan the workspace for pipeline files (a `.json` with a top-level `nodes`
/// array), skipping bookkeeping folders. Returns (absolute path, id, value).
fn discover_pipelines(workspace: &Path) -> Vec<(PathBuf, String, Value)> {
    let mut out = Vec::new();
    let skip = ["runs", "logs", "connections", "node_modules", ".duckle", ".git", "target"];
    let mut stack = vec![workspace.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !skip.contains(&name) {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let text = match std::fs::read_to_string(&path) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let v: Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if v.get("nodes").and_then(|n| n.as_array()).is_some() {
                let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
                out.push((path, id, v));
            }
        }
    }
    out.sort_by(|a, b| a.1.to_lowercase().cmp(&b.1.to_lowercase()));
    out
}

fn rel(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn last_run(workspace: &Path, id: &str) -> Option<RunRecord> {
    // History is appended in order; the most recent record is last.
    load_run_history(workspace, id).into_iter().last()
}

fn api_pipelines(state: &State) -> Value {
    let scheds = load_schedules(state);
    let items: Vec<Value> = discover_pipelines(&state.workspace)
        .into_iter()
        .map(|(path, id, v)| {
            let last = last_run(&state.workspace, &id);
            let sched = scheds.get(&id).cloned().unwrap_or(json!({ "enabled": false, "intervalMinutes": 0 }));
            json!({
                "file": rel(&state.workspace, &path),
                "id": id,
                "name": v.get("name").and_then(|x| x.as_str()).unwrap_or(""),
                "nodeCount": v.get("nodes").and_then(|n| n.as_array()).map(|a| a.len()).unwrap_or(0),
                "edgeCount": v.get("edges").and_then(|e| e.as_array()).map(|a| a.len()).unwrap_or(0),
                "lastStatus": last.as_ref().map(|r| r.status.clone()),
                "lastAt": last.as_ref().map(|r| r.at.clone()),
                "lastDurationMs": last.as_ref().map(|r| r.duration_ms),
                "lastRows": last.as_ref().map(|r| r.rows),
                "schedule": sched,
            })
        })
        .collect();
    json!({ "pipelines": items })
}

fn api_summary(state: &State) -> Value {
    let pipes = discover_pipelines(&state.workspace);
    let mut total_runs = 0u64;
    let mut ok = 0u64;
    let mut failed = 0u64;
    for (_, id, _) in &pipes {
        for r in load_run_history(&state.workspace, id) {
            total_runs += 1;
            if r.status == "ok" {
                ok += 1;
            } else {
                failed += 1;
            }
        }
    }
    json!({
        "pipelineCount": pipes.len(),
        "totalRuns": total_runs,
        "ok": ok,
        "failed": failed,
        "workspace": state.workspace.to_string_lossy(),
    })
}

/// Run history across all pipelines (or one, when `id` is given), newest first,
/// each record tagged with its pipeline id/name.
fn api_runs(state: &State, only: Option<&str>) -> Value {
    let mut rows: Vec<Value> = Vec::new();
    for (path, id, v) in discover_pipelines(&state.workspace) {
        if let Some(want) = only {
            if want != id {
                continue;
            }
        }
        let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
        for r in load_run_history(&state.workspace, &id) {
            rows.push(json!({
                "id": id,
                "name": name,
                "file": rel(&state.workspace, &path),
                "at": r.at,
                "status": r.status,
                "durationMs": r.duration_ms,
                "rows": r.rows,
                "nodeCount": r.node_count,
                "trigger": r.trigger,
                "error": r.error,
                "category": r.category,
            }));
        }
    }
    // RunRecord.at is RFC3339 UTC, so a string sort orders by time; newest first.
    rows.sort_by(|a, b| {
        b.get("at").and_then(|v| v.as_str()).unwrap_or("")
            .cmp(a.get("at").and_then(|v| v.as_str()).unwrap_or(""))
    });
    json!({ "runs": rows })
}

fn read_pipeline_file(state: &State, file: &str) -> Result<Value, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))
}

/// Resolve a workspace-relative path and refuse anything that escapes the
/// workspace (no `..` traversal beyond the root).
fn resolve_in_workspace(workspace: &Path, file: &str) -> Result<PathBuf, String> {
    let candidate = workspace.join(file);
    let canon = candidate.canonicalize().map_err(|_| format!("not found: {}", file))?;
    if !canon.starts_with(workspace) {
        return Err("path escapes workspace".into());
    }
    Ok(canon)
}

fn api_log(state: &State, query: &HashMap<String, String>) -> Value {
    let id = match query.get("id") {
        Some(i) => i,
        None => return json!({ "entries": [] }),
    };
    let tail: usize = query.get("tail").and_then(|t| t.parse().ok()).unwrap_or(200);
    let file = state.workspace.join("logs").join(sanitize_segment(id)).join("runtime.log");
    let text = match std::fs::read_to_string(&file) {
        Ok(t) => t,
        Err(_) => return json!({ "entries": [], "file": file.to_string_lossy() }),
    };
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = lines.len().saturating_sub(tail);
    let entries: Vec<Value> = lines[start..]
        .iter()
        .map(|l| serde_json::from_str::<Value>(l).unwrap_or_else(|_| json!({ "raw": l })))
        .collect();
    json!({ "entries": entries, "file": file.to_string_lossy() })
}

/// Match the engine's per-pipeline log-folder sanitization (run_log.rs).
fn sanitize_segment(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect();
    if s.is_empty() { "pipeline".into() } else { s }
}

// ── Schedules ──

fn schedules_path(workspace: &Path) -> PathBuf {
    workspace.join("panel-schedules.json")
}

/// Schedule store: { "<pipeline id>": { "enabled": bool, "intervalMinutes": n } }.
fn load_schedules(state: &State) -> Value {
    std::fs::read_to_string(schedules_path(&state.workspace))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_else(|| json!({}))
}

fn save_schedule(state: &State, body: &Value) -> Result<Value, String> {
    let id = body.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
    let enabled = body.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let interval = body.get("intervalMinutes").and_then(|v| v.as_u64()).unwrap_or(0);
    let mut all = load_schedules(state);
    let obj = all.as_object_mut().ok_or("schedule store corrupt")?;
    obj.insert(id.to_string(), json!({ "enabled": enabled, "intervalMinutes": interval }));
    std::fs::write(schedules_path(&state.workspace), all.to_string())
        .map_err(|e| format!("write schedules: {}", e))?;
    Ok(json!({ "ok": true }))
}

// ── Execution ──

/// Run one pipeline by its workspace-relative file path, end to end: resolve
/// env/time placeholders (as the runner does), execute through the engine,
/// append a run-history record, and return a result summary. Serialized by the
/// run lock so a scheduled run never overlaps a manual one.
fn execute_one(state: &State, file: &str, trigger: &str) -> Result<Value, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;

    let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "pipeline".into());

    let _guard = state.run_lock.lock().map_err(|_| "run lock poisoned".to_string())?;

    // Same placeholder resolution as `duckle-runner run`: ${ENV:KEY} secrets,
    // then the dynamic ${date}/${datetime}/... builtins.
    let env_file = state.workspace.join("secrets.env");
    crate::apply_env_pass(&mut doc, &state.workspace, &env_file)?;
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);

    let engine = DuckdbEngine::new(state.duckdb.clone());
    let result = engine.execute_pipeline_named(&doc, &id);

    let _ = append_run_record(&state.workspace, &id, RunRecord::from_result(&result, trigger));

    Ok(json!({
        "id": id,
        "status": result.status,
        "durationMs": result.duration_ms,
        "error": result.error,
        "nodes": result.nodes.iter().map(|(nid, st)| json!({
            "id": nid, "status": st.status, "rows": st.rows, "durationMs": st.duration_ms, "error": st.error,
        })).collect::<Vec<_>>(),
    }))
}

// ── Scheduler ──

/// Background loop: every 30s, run any enabled pipeline whose interval has
/// elapsed since it last ran here. Timing is tracked in-memory from process
/// start (first run fires one interval after boot), so no clock parsing and no
/// surprise burst of runs on restart.
fn spawn_scheduler(state: Arc<State>) {
    std::thread::spawn(move || {
        let mut last_fired: HashMap<String, Instant> = HashMap::new();
        loop {
            std::thread::sleep(Duration::from_secs(30));
            let scheds = load_schedules(&state);
            let obj = match scheds.as_object() {
                Some(o) => o,
                None => continue,
            };
            // Map id -> its file path for the enabled, due ones.
            let pipes: HashMap<String, PathBuf> =
                discover_pipelines(&state.workspace).into_iter().map(|(p, id, _)| (id, p)).collect();
            for (id, cfg) in obj {
                let enabled = cfg.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                let minutes = cfg.get("intervalMinutes").and_then(|v| v.as_u64()).unwrap_or(0);
                if !enabled || minutes == 0 {
                    last_fired.remove(id);
                    continue;
                }
                let interval = Duration::from_secs(minutes * 60);
                let due = match last_fired.get(id) {
                    Some(t) => t.elapsed() >= interval,
                    None => false, // first sighting: start the clock, fire next interval
                };
                let now = Instant::now();
                if last_fired.get(id).is_none() {
                    last_fired.insert(id.clone(), now);
                    continue;
                }
                if due {
                    if let Some(path) = pipes.get(id) {
                        let file = rel(&state.workspace, path);
                        last_fired.insert(id.clone(), now);
                        match execute_one(&state, &file, "scheduled") {
                            Ok(v) => eprintln!(
                                "duckle-runner: scheduled {} -> {}",
                                id,
                                v.get("status").and_then(|s| s.as_str()).unwrap_or("?")
                            ),
                            Err(e) => eprintln!("duckle-runner: scheduled {} failed: {}", id, e),
                        }
                    }
                }
            }
        }
    });
}
