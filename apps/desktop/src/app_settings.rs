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
    let proxy = load(Path::new(workspace))
        .https_proxy
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if proxy.is_some() {
        duckle_duckdb_engine::tls::set_proxy(proxy);
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
