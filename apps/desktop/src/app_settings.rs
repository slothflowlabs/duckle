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
    /// #143: allow loading UNSIGNED / community DuckDB extensions (e.g. a custom
    /// `quack` build). Applied as DUCKLE_ALLOW_UNSIGNED_EXTENSIONS, which makes the
    /// engine pass `-unsigned` to the DuckDB CLI. None/false = signed-only (default).
    allow_unsigned_extensions: Option<bool>,
    /// Power mode: how many pipelines may execute at once. Applied as
    /// DUCKLE_MAX_CONCURRENT_RUNS, which both the desktop scheduler and
    /// `duckle serve` honour. None leaves each host on its own default.
    ///
    /// This is the throughput lever with a measured number behind it:
    /// independent DuckDB queries scaled about 3.8x across 8 concurrent
    /// processes on a 20-core box. Splitting ONE query across processes was
    /// measured slower, which is why no such option exists.
    max_concurrent_runs: Option<u32>,
    /// Power mode: directory for DuckDB's spill files, applied as
    /// DUCKLE_TEMP_DIR. Points spilling work at a bigger or faster disk than
    /// the default (which sits beside the run's own database file). Each run
    /// gets a private subdirectory beneath it, so concurrent runs cannot
    /// collide over spill cleanup.
    spill_dir: Option<String>,
    /// GPU acceleration through NVIDIA Sirius: `on` or `off`. None is the
    /// default, auto. Applied as DUCKLE_GPU.
    gpu: Option<String>,
}

/// The power-mode settings returned to the UI. camelCase for JS.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PowerConfig {
    pub max_concurrent_runs: Option<u32>,
    pub memory_limit_mb: Option<u32>,
    pub spill_dir: Option<String>,
    pub gpu: Option<String>,
    /// Logical cores, so the UI can suggest a ceiling instead of guessing.
    pub cpu_count: u32,
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

/// Load the workspace's saved settings and apply them to the process.
///
/// Every setting is applied, set or not. This used to apply only the ones the
/// workspace HAD, so opening a workspace with none inherited the previous
/// workspace's proxy, memory cap and opt-in to unsigned extensions. A setting
/// this workspace leaves unset goes back to what the app was launched with
/// (`launch_env`), which for a proxy is the environment-derived one.
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
    duckle_duckdb_engine::tls::set_proxy(proxy);
    // #102: apply the per-workspace memory cap as DUCKLE_MEMORY_LIMIT (the env
    // var the engine reads in both batched and per-stage modes).
    apply_memory_limit(s.memory_limit_mb);
    // #143: opt-in unsigned-extension loading for this workspace.
    apply_allow_unsigned(s.allow_unsigned_extensions == Some(true));
    apply_power(&s);
}

fn apply_memory_limit(mb: Option<u32>) {
    let cap = mb.filter(|m| *m > 0).map(|m| format!("{}MB", m));
    duckle_duckdb_engine::launch_env::set_or_restore("DUCKLE_MEMORY_LIMIT", cap.as_deref());
}

fn apply_allow_unsigned(allow: bool) {
    duckle_duckdb_engine::launch_env::set_or_restore(
        "DUCKLE_ALLOW_UNSIGNED_EXTENSIONS",
        allow.then_some("1"),
    );
}

/// Publish the power-mode settings as the env vars the scheduler and engine
/// read. Unset means "leave the host on its own default" - what the app was
/// launched with - so switching to a workspace that has never configured power
/// mode does not silently inherit the last one's settings.
fn apply_power(s: &AppSettings) {
    let runs = s.max_concurrent_runs.filter(|n| *n > 0).map(|n| n.to_string());
    duckle_duckdb_engine::launch_env::set_or_restore("DUCKLE_MAX_CONCURRENT_RUNS", runs.as_deref());
    let spill = s.spill_dir.as_deref().map(str::trim).filter(|d| !d.is_empty());
    duckle_duckdb_engine::launch_env::set_or_restore("DUCKLE_TEMP_DIR", spill);
    duckle_duckdb_engine::launch_env::set_or_restore("DUCKLE_GPU", s.gpu.as_deref());
}

#[tauri::command]
pub fn settings_get_power(workspace: String) -> PowerConfig {
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    if workspace.is_empty() {
        return PowerConfig { cpu_count, ..Default::default() };
    }
    let s = load(Path::new(&workspace));
    PowerConfig {
        max_concurrent_runs: s.max_concurrent_runs.filter(|n| *n > 0),
        memory_limit_mb: s.memory_limit_mb.filter(|m| *m > 0),
        spill_dir: s
            .spill_dir
            .map(|d| d.trim().to_string())
            .filter(|d| !d.is_empty()),
        gpu: s.gpu,
        cpu_count,
    }
}

#[tauri::command]
pub fn settings_set_power(
    workspace: String,
    max_concurrent_runs: Option<u32>,
    memory_limit_mb: Option<u32>,
    spill_dir: Option<String>,
    gpu: Option<String>,
) -> Result<(), String> {
    if workspace.is_empty() {
        return Err("no workspace is open".into());
    }
    // Auto is the default, so it is stored as nothing set.
    let gpu = match gpu.as_deref().map(duckle_duckdb_engine::gpu::parse_mode).transpose()? {
        None | Some(duckle_duckdb_engine::gpu::GpuMode::Auto) => None,
        Some(m) => Some(m.as_str().to_string()),
    };
    let spill = spill_dir
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty());
    // Fail before saving rather than at the first run that tries to spill.
    if let Some(d) = spill.as_deref() {
        std::fs::create_dir_all(d).map_err(|e| format!("spill folder {}: {e}", d))?;
    }
    let mut s = load(Path::new(&workspace));
    s.max_concurrent_runs = max_concurrent_runs.filter(|n| *n > 0);
    s.memory_limit_mb = memory_limit_mb.filter(|m| *m > 0);
    s.spill_dir = spill;
    s.gpu = gpu;
    store(Path::new(&workspace), &s)?;
    // Apply immediately so the current session picks it up without a relaunch.
    apply_power(&s);
    apply_memory_limit(s.memory_limit_mb);
    Ok(())
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
    apply_memory_limit(mb);
    Ok(())
}

#[tauri::command]
pub fn settings_get_allow_unsigned(workspace: String) -> bool {
    if workspace.is_empty() {
        return false;
    }
    load(Path::new(&workspace)).allow_unsigned_extensions == Some(true)
}

#[tauri::command]
pub fn settings_set_allow_unsigned(workspace: String, allow: bool) -> Result<(), String> {
    if workspace.is_empty() {
        return Err("no workspace is open".into());
    }
    let mut s = load(Path::new(&workspace));
    s.allow_unsigned_extensions = if allow { Some(true) } else { None };
    store(Path::new(&workspace), &s)?;
    // Apply immediately so the current session's runs use it without a relaunch.
    apply_allow_unsigned(allow);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Both tests set and restore the same process variables through
    /// `apply_for_workspace`, and tests run on parallel threads.
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Opening a workspace applied only the settings it HAD, so one with none
    /// inherited the previous workspace's memory cap and, worse, its opt-in to
    /// unsigned DuckDB extensions. No other desktop test touches these
    /// variables, and the cap is generous so a run elsewhere is unaffected.
    #[test]
    fn opening_a_workspace_without_settings_does_not_inherit_the_last_ones() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let opted_in = tempfile::tempdir().unwrap();
        let settings = AppSettings {
            memory_limit_mb: Some(4096),
            allow_unsigned_extensions: Some(true),
            ..Default::default()
        };
        store(opted_in.path(), &settings).unwrap();
        let plain = tempfile::tempdir().unwrap();
        let unset = || {
            (
                std::env::var_os("DUCKLE_MEMORY_LIMIT"),
                std::env::var_os("DUCKLE_ALLOW_UNSIGNED_EXTENSIONS"),
            )
        };
        let before = unset();

        apply_for_workspace(opted_in.path().to_str().unwrap());
        assert_eq!(std::env::var("DUCKLE_ALLOW_UNSIGNED_EXTENSIONS").as_deref(), Ok("1"));
        assert_eq!(std::env::var("DUCKLE_MEMORY_LIMIT").as_deref(), Ok("4096MB"));

        apply_for_workspace(plain.path().to_str().unwrap());
        assert_eq!(
            unset(),
            before,
            "a workspace with no settings kept the previous one's cap and unsigned-extension opt-in"
        );
    }

    /// GPU acceleration is kept per workspace like the rest of power mode:
    /// saved as asked, auto stored as the default it is, a typo refused before
    /// anything is saved, and a workspace that never set it not inheriting the
    /// last one's.
    #[test]
    fn the_gpu_setting_is_saved_applied_and_not_inherited() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let ws = tempfile::tempdir().unwrap();
        let w = ws.path().to_str().unwrap().to_string();
        let before = std::env::var_os("DUCKLE_GPU");

        settings_set_power(w.clone(), None, None, None, Some("ON".into())).unwrap();
        assert_eq!(settings_get_power(w.clone()).gpu.as_deref(), Some("on"));
        assert_eq!(std::env::var("DUCKLE_GPU").as_deref(), Ok("on"), "applied as it is saved");

        settings_set_power(w.clone(), None, None, None, Some("auto".into())).unwrap();
        assert_eq!(settings_get_power(w.clone()).gpu, None, "auto is the default, stored as unset");

        let err = settings_set_power(w.clone(), None, None, None, Some("yes".into())).unwrap_err();
        assert!(err.contains("auto, on or off"), "{err}");

        settings_set_power(w.clone(), None, None, None, Some("off".into())).unwrap();
        let plain = tempfile::tempdir().unwrap();
        apply_for_workspace(plain.path().to_str().unwrap());
        assert_eq!(std::env::var_os("DUCKLE_GPU"), before, "a workspace that never set it");
    }
}
