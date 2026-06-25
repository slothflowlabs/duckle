//! Per-workspace app settings (currently just an HTTP/HTTPS proxy), persisted to
//! `<workspace>/.duckle/settings.json`. The proxy is pushed into the engine's
//! shared HTTP layer via `duckle_duckdb_engine::tls::set_proxy`, so a user on a
//! locked-down corporate machine can route REST / cloud connectors and the
//! in-app updater through a proxy WITHOUT setting any system environment
//! variable (issue #80). Applied on startup and on every workspace switch.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct AppSettings {
    /// HTTP/HTTPS proxy URL, e.g. "http://user:pass@proxy:8080". None / empty
    /// means a direct connection.
    https_proxy: Option<String>,
    /// #92: optional external OpenAI-compatible endpoint for the Duckie AI
    /// assistant (base URL, e.g. https://api.openai.com or an Ollama/LM Studio
    /// URL). When set, chat goes to it instead of the local Qwen model.
    ai_base_url: Option<String>,
    /// Model id for the external endpoint (e.g. "gpt-4o-mini", "llama3.1").
    ai_model: Option<String>,
    /// API key for the external endpoint (sent as `Authorization: Bearer ...`).
    /// Stored alongside the proxy creds in the workspace's local .duckle dir.
    ai_api_key: Option<String>,
    /// #102: total DuckDB memory cap in MB, applied as DUCKLE_MEMORY_LIMIT for
    /// every run in this workspace (batched and per-stage). None = DuckDB
    /// default (~80% of RAM). Stages run sequentially, so this caps peak RAM.
    memory_limit_mb: Option<u32>,
    /// Path to a key/value file (.env / .properties / .csv / .json) whose
    /// entries auto-load into the global context for every run in this
    /// workspace, so ${KEY} resolves without wiring a node. Relative paths
    /// resolve against the workspace root.
    context_file: Option<String>,
}

/// The external-AI config returned to the Settings UI. camelCase for JS.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiConfig {
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
}

fn settings_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("settings.json")
}

fn load(workspace: &Path) -> AppSettings {
    match std::fs::read(settings_path(workspace)) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => AppSettings::default(),
    }
}

fn store(workspace: &Path, s: &AppSettings) -> Result<(), String> {
    let dir = workspace.join(".duckle");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let json = serde_json::to_string_pretty(s).map_err(|e| e.to_string())?;
    std::fs::write(settings_path(workspace), json).map_err(|e| format!("write settings: {e}"))
}

/// Load the workspace's saved proxy and apply it to the engine HTTP layer.
/// Best-effort: a missing / unreadable settings file leaves the current
/// (environment-derived) proxy in place.
pub fn apply_for_workspace(workspace: &str) {
    if workspace.is_empty() {
        return;
    }
    let s = load(Path::new(workspace));
    let proxy = s
        .https_proxy
        .clone()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if proxy.is_some() {
        duckle_duckdb_engine::tls::set_proxy(proxy);
    }
    // #102: apply the per-workspace memory cap as DUCKLE_MEMORY_LIMIT (the env
    // var the engine reads in both batched and per-stage modes).
    if let Some(mb) = s.memory_limit_mb.filter(|m| *m > 0) {
        std::env::set_var("DUCKLE_MEMORY_LIMIT", format!("{}MB", mb));
    }
}

#[tauri::command]
pub fn settings_get_proxy(workspace: String) -> Option<String> {
    if workspace.is_empty() {
        return None;
    }
    load(Path::new(&workspace))
        .https_proxy
        .filter(|s| !s.trim().is_empty())
}

#[tauri::command]
pub fn settings_set_proxy(workspace: String, url: Option<String>) -> Result<(), String> {
    if workspace.is_empty() {
        return Err("no workspace is open".into());
    }
    let url = url.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let mut s = load(Path::new(&workspace));
    s.https_proxy = url.clone();
    store(Path::new(&workspace), &s)?;
    // Apply immediately so the current session uses it without a relaunch.
    duckle_duckdb_engine::tls::set_proxy(url);
    Ok(())
}

#[tauri::command]
pub fn settings_get_memory_limit(workspace: String) -> Option<u32> {
    if workspace.is_empty() {
        return None;
    }
    load(Path::new(&workspace)).memory_limit_mb.filter(|m| *m > 0)
}

#[tauri::command]
pub fn settings_set_memory_limit(workspace: String, mb: Option<u32>) -> Result<(), String> {
    if workspace.is_empty() {
        return Err("no workspace is open".into());
    }
    let mb = mb.filter(|m| *m > 0);
    let mut s = load(Path::new(&workspace));
    s.memory_limit_mb = mb;
    store(Path::new(&workspace), &s)?;
    // Apply immediately so the current session's runs use it without a relaunch.
    match mb {
        Some(m) => std::env::set_var("DUCKLE_MEMORY_LIMIT", format!("{}MB", m)),
        None => std::env::remove_var("DUCKLE_MEMORY_LIMIT"),
    }
    Ok(())
}

#[tauri::command]
pub fn settings_get_context_file(workspace: String) -> Option<String> {
    if workspace.is_empty() {
        return None;
    }
    load(Path::new(&workspace))
        .context_file
        .filter(|s| !s.trim().is_empty())
}

#[tauri::command]
pub fn settings_set_context_file(workspace: String, path: Option<String>) -> Result<(), String> {
    if workspace.is_empty() {
        return Err("no workspace is open".into());
    }
    let path = path.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let mut s = load(Path::new(&workspace));
    s.context_file = path;
    store(Path::new(&workspace), &s)
}

/// Resolve the global-context key/value file into a flat var map for the
/// desktop run path (the frontend pre-substitutes ${...} before the engine
/// sees the pipeline; the headless runner / web server resolve it engine-side
/// via context_vars_for_workspace).
#[tauri::command]
pub fn settings_load_context_vars(workspace: String) -> std::collections::HashMap<String, String> {
    if workspace.is_empty() {
        return std::collections::HashMap::new();
    }
    duckle_duckdb_engine::context::context_file_vars(Path::new(&workspace))
}

#[tauri::command]
pub fn settings_get_ai(workspace: String) -> AiConfig {
    if workspace.is_empty() {
        return AiConfig::default();
    }
    let s = load(Path::new(&workspace));
    let clean = |o: Option<String>| o.map(|x| x.trim().to_string()).filter(|x| !x.is_empty());
    AiConfig {
        base_url: clean(s.ai_base_url),
        model: clean(s.ai_model),
        api_key: clean(s.ai_api_key),
    }
}

#[tauri::command]
pub fn settings_set_ai(
    workspace: String,
    base_url: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
) -> Result<(), String> {
    if workspace.is_empty() {
        return Err("no workspace is open".into());
    }
    let clean = |o: Option<String>| o.map(|x| x.trim().to_string()).filter(|x| !x.is_empty());
    let mut s = load(Path::new(&workspace));
    s.ai_base_url = clean(base_url);
    s.ai_model = clean(model);
    s.ai_api_key = clean(api_key);
    store(Path::new(&workspace), &s)
}

/// Internal: the workspace's external-AI config (base_url, model, api_key) for
/// chat routing. All None when no external endpoint is configured.
pub fn ai_config(workspace: &str) -> (Option<String>, Option<String>, Option<String>) {
    if workspace.is_empty() {
        return (None, None, None);
    }
    let s = load(Path::new(workspace));
    let clean = |o: Option<String>| o.map(|x| x.trim().to_string()).filter(|x| !x.is_empty());
    (clean(s.ai_base_url), clean(s.ai_model), clean(s.ai_api_key))
}
