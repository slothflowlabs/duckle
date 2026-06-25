//! Duckle desktop shell.
//!
//! Boots the Tauri runtime, wires it to `duckle-runtime`, and exposes
//! invoke commands to the frontend.

use duckle_connectors::CsvConnector;
use duckle_duckdb_engine::{
    append_run_record, compile_pipeline_sql, load_run_history, DuckdbEngine, PipelineDoc,
    PipelineEvent, RunRecord, RunResult, StageSql,
};
use duckle_metadata::Schema;
use duckle_plugin_sdk::{InspectError, SchemaInspector};
use duckle_scheduler::{Schedule, Scheduler};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::OnceLock;
use tauri::ipc::Channel;
use tauri::Manager;
use tracing_subscriber::EnvFilter;

mod app_settings;
mod ci_status;
mod dbt_engine;
mod engine_manager;
mod llama_chat;
mod secrets;
mod self_update;
mod update_check;
mod workspace_git;
use engine_manager::{EngineStatus, InstallProgress};
use llama_chat::{ChatEvent, ChatMessage};

/// The headless duckle-runner, embedded at compile time (apps/desktop/build.rs
/// stages a freshly built runner and points DUCKLE_EMBEDDED_RUNNER at it).
/// "Build Pipeline" writes these bytes to a temp stub and uses it both as the
/// builder and as the artifact stub, so no separate runner download or
/// compile-on-click is needed.
const EMBEDDED_RUNNER: &[u8] = include_bytes!(env!("DUCKLE_EMBEDDED_RUNNER"));

/// The STATIC Linux duckle-runner, embedded at compile time when staged at
/// apps/desktop/bin/duckle-runner-linux-x64 (built by
/// scripts/build-runner-linux.sh). Empty when this build did not bundle it. Used
/// as the artifact stub when "Build Pipeline" targets Linux from a non-Linux
/// host, so a Linux single-file artifact can be produced without a Linux box.
const EMBEDDED_RUNNER_LINUX: &[u8] = include_bytes!(env!("DUCKLE_EMBEDDED_RUNNER_LINUX"));

/// The duckle-mcp server, embedded at compile time when staged. Empty when this
/// build did not bundle it (see build.rs embed_mcp). Written to a stable
/// app-data path on demand so an MCP client config can point at it.
const EMBEDDED_MCP: &[u8] = include_bytes!(env!("DUCKLE_EMBEDDED_MCP"));

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    tracing::info!("duckle starting");

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            // Resolve where the downloaded DuckDB CLI lives, so the
            // engine can shell out to it. The binary may not exist yet
            // (first run installs it via the setup screen); the engine
            // just errors clearly until then.
            //
            // ALSO publish the path as DUCKLE_DUCKDB_BIN. The engine's
            // primary execution path takes the binary as a constructor
            // arg, but rest_source_apply (used by REST-shaped sources:
            // Oracle, SQL Server, Snowflake, Databricks, Synapse,
            // BigQuery, and the various SaaS aliases that materialize
            // their inline result set) is a free helper that reads the
            // env var directly. Without this set, those sources fail
            // with "DUCKLE_DUCKDB_BIN not set" while plain file flows
            // work fine. See issue #2.
            if let Ok(dir) = app.path().app_data_dir() {
                let bin = engine_manager::duckdb_path(&dir);
                std::env::set_var("DUCKLE_DUCKDB_BIN", &bin);
                let _ = DUCKDB_BIN.set(bin);

                // dbt for the xf.dbt node. Publishing an already-provisioned
                // dbt is cheap (no network), so do it inline. If Fusion (the
                // preferred fast engine) is not yet present, kick off a one-time,
                // best-effort background fetch: dbt Fusion from dbt's public CDN,
                // falling back to free Apache dbt-core via uv when Fusion can't
                // be fetched. This also upgrades earlier dbt-core-only installs.
                // ensure() is idempotent: a no-op once Fusion is in place.
                // Only publish an ALREADY-provisioned dbt (cheap, no spawn). Do
                // NOT auto-provision at startup: ensure() shells out to `uv`,
                // whose python grandchildren get their own console on Windows
                // (CREATE_NO_WINDOW does not propagate to grandchildren), so a
                // failed-Fusion-fetch retry would flash a console on every
                // launch and slow startup. dbt is provisioned on demand instead
                // (the dbt node's Install action -> dbt_install), and the engine
                // errors clearly if xf.dbt runs before dbt is present.
                dbt_engine::publish_if_present(&dir);
            }
            // Boot the scheduler. The `.setup` hook runs on the main
            // thread, OUTSIDE any tokio runtime, so calling spawn_ticker
            // (which uses tokio::spawn) directly here panics with
            // "there is no reactor running". Hop onto Tauri's async
            // runtime first.
            if let Ok(eng) = engine() {
                let s = Scheduler::new(eng);
                let _ = SCHEDULER.set(s.clone());
                tauri::async_runtime::spawn(async move {
                    s.spawn_ticker();
                });
            }
            // The window launches hidden (visible:false) so there's no
            // white flash - the frontend calls show() once it has
            // painted. Safety net: reveal it after a few seconds no
            // matter what, so a frontend hiccup can't leave the window
            // stuck invisible.
            if let Some(win) = app.get_webview_window("main") {
                // Open maximized (fill the work area) on every OS. The
                // config `maximized: true` is unreliable when the window
                // starts hidden (visible:false), so maximize explicitly
                // while it is still hidden - it then reveals already
                // maximized with no resize flicker.
                let _ = win.maximize();
                tauri::async_runtime::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                    let _ = win.show();
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            ping,
            autodetect_schema,
            run_pipeline,
            run_pipeline_partial,
            run_history,
            watermark_list,
            watermark_set,
            watermark_clear,
            cancel_pipeline,
            compile_pipeline,
            pipeline_column_lineage,
            schedule_set_workspace,
            schedule_list,
            schedule_upsert,
            schedule_delete,
            schedule_run_now,
            engine_status,
            engine_install,
            dbt_status,
            dbt_install,
            chat_send,
            chat_extract_pipeline,
            workspace_git_status,
            workspace_git_init,
            workspace_git_commit,
            workspace_git_push,
            workspace_git_pull,
            workspace_git_branches,
            workspace_git_branch_create,
            workspace_git_branch_checkout,
            workspace_git_remote_set,
            workspace_git_save_pat,
            workspace_git_clear_pat,
            secrets::connection_encrypt_payload,
            secrets::connection_decrypt_payload,
            app_settings::settings_get_proxy,
            app_settings::settings_set_proxy,
            app_settings::settings_get_ai,
            app_settings::settings_set_ai,
            app_settings::settings_get_memory_limit,
            app_settings::settings_set_memory_limit,
            app_settings::settings_get_context_file,
            app_settings::settings_set_context_file,
            app_settings::settings_load_context_vars,
            workspace_ci_status,
            check_for_update,
            build_pipeline_bundle,
            build_capabilities,
            mcp_connection_info,
            connect_claude_code,
            mcp_inject_config,
            open_web_panel,
            self_update
        ])
        .build(tauri::generate_context!())
        .expect("error while building duckle")
        .run(|_app, event| {
            // Stop the web-panel server (if running) when the app exits so it
            // does not linger as an orphaned headless process.
            if let tauri::RunEvent::Exit = event {
                stop_web_panel_silent();
            }
        });
}

/// Liveness probe. Returns the string `"pong"`.
#[tauri::command]
fn ping() -> &'static str {
    "pong"
}

#[derive(Debug, Serialize)]
pub struct InspectionPayload {
    pub columns: Schema,
    #[serde(rename = "sampleRows")]
    pub sample_rows: Vec<JsonValue>,
}

static DUCKDB_BIN: OnceLock<PathBuf> = OnceLock::new();
static DUCKDB_ENGINE: OnceLock<DuckdbEngine> = OnceLock::new();

/// The engine driving the current interactive run, so `cancel_pipeline` can
/// stop THAT run specifically. Each run uses a fresh per-run cancel flag (via
/// `for_new_run`), so cancelling the interactive run never touches concurrent
/// scheduler runs, and a finished run can't be cancelled by a stale request.
static CURRENT_RUN: std::sync::Mutex<Option<DuckdbEngine>> = std::sync::Mutex::new(None);

/// The shared engine, pointed at the downloaded DuckDB CLI. Cheap to
/// build (just holds a path); cached so the cancel flag is shared
/// between a run and a cancel.
fn engine() -> Result<DuckdbEngine, String> {
    let bin = DUCKDB_BIN
        .get()
        .cloned()
        .ok_or_else(|| "Engine path not resolved yet".to_string())?;
    Ok(DUCKDB_ENGINE
        .get_or_init(|| DuckdbEngine::new(bin))
        .clone())
}

/// Inspect a source's schema. The frontend hands us a format string
/// (`"csv"`, `"parquet"`, `"json"`, `"sqlite"`, `"duckdb"`, ...) and the
/// connector-specific options, and we return inferred columns plus a
/// small sample for the Preview tab.
///
/// Most formats go through DuckDB's native readers - `read_csv_auto`,
/// `read_parquet`, `read_json_auto`, `sqlite_scan`. The hand-rolled
/// `CsvConnector` stays as a backup for environments where the DuckDB
/// engine fails to come up.
#[tauri::command]
async fn autodetect_schema(
    format: String,
    options: JsonValue,
) -> Result<InspectionPayload, String> {
    let inspection = match engine() {
        Ok(eng) => match eng.inspect(&format, options.clone()) {
            Ok(insp) => insp,
            Err(e) => {
                tracing::warn!(
                    "DuckDB autodetect failed for {} ({}); falling back",
                    format,
                    e
                );
                if matches!(format.as_str(), "csv" | "tsv") {
                    CsvConnector
                        .inspect(options)
                        .await
                        .map_err(format_inspect_error)?
                } else {
                    return Err(e.to_string());
                }
            }
        },
        Err(boot_err) => {
            tracing::error!("DuckDB engine failed to start: {}", boot_err);
            if matches!(format.as_str(), "csv" | "tsv") {
                CsvConnector
                    .inspect(options)
                    .await
                    .map_err(format_inspect_error)?
            } else {
                return Err(format!("DuckDB engine unavailable: {}", boot_err));
            }
        }
    };
    Ok(InspectionPayload {
        columns: inspection.schema,
        sample_rows: inspection.sample_rows,
    })
}

fn format_inspect_error(err: InspectError) -> String {
    err.to_string()
}

/// Run a pipeline through the DuckDB engine. Receives the React Flow
/// nodes + edges as JSON; compiles to SQL; executes via DuckDB; returns
/// per-node status + preview rows for any leaf node that didn't feed a
/// sink.
///
/// `on_event` is a Tauri Channel - every stage start / stage finish /
/// cancellation is pushed through it so the frontend can light up
/// status badges in real time.
#[tauri::command]
async fn run_pipeline(
    pipeline: PipelineDoc,
    on_event: Channel<PipelineEvent>,
    pipeline_id: Option<String>,
    pipeline_name: Option<String>,
    workspace_path: Option<String>,
) -> Result<RunResult, String> {
    let engine = engine()?.for_new_run();
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = Some(engine.clone());
    let name = pipeline_name.clone();
    let joined = tokio::task::spawn_blocking(move || {
        engine.execute_pipeline_with_events(&pipeline, None, name.as_deref(), |evt| {
            let _ = on_event.send(evt);
        })
    })
    .await;
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = None;
    let result = joined.map_err(|e| e.to_string())?;
    record_history(&pipeline_id, &workspace_path, &result, "manual");
    Ok(result)
}

fn record_history(
    pipeline_id: &Option<String>,
    workspace_path: &Option<String>,
    result: &RunResult,
    trigger: &str,
) {
    if let (Some(id), Some(ws)) = (pipeline_id, workspace_path) {
        let record = RunRecord::from_result(result, trigger);
        if let Err(e) = append_run_record(std::path::Path::new(ws), id, record) {
            tracing::warn!("Failed to record run history: {}", e);
        }
    }
}

/// Same as `run_pipeline` but only executes the subgraph upstream of
/// (and including) `target_node_id`. The target becomes the leaf and
/// returns a preview.
#[tauri::command]
async fn run_pipeline_partial(
    pipeline: PipelineDoc,
    target_node_id: String,
    on_event: Channel<PipelineEvent>,
    pipeline_id: Option<String>,
    pipeline_name: Option<String>,
    workspace_path: Option<String>,
) -> Result<RunResult, String> {
    let engine = engine()?.for_new_run();
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = Some(engine.clone());
    let target = target_node_id;
    let name = pipeline_name.clone();
    let joined = tokio::task::spawn_blocking(move || {
        engine.execute_pipeline_with_events(
            &pipeline,
            Some(target.as_str()),
            name.as_deref(),
            |evt| {
                let _ = on_event.send(evt);
            },
        )
    })
    .await;
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = None;
    let result = joined.map_err(|e| e.to_string())?;
    record_history(&pipeline_id, &workspace_path, &result, "partial");
    Ok(result)
}

/// Read the run history for a pipeline (newest first).
#[tauri::command]
fn run_history(workspace_path: String, pipeline_id: String) -> Result<Vec<RunRecord>, String> {
    let mut records = load_run_history(std::path::Path::new(&workspace_path), &pipeline_id);
    records.reverse();
    Ok(records)
}

// ---- Backfill: xf.incremental / src.ducklake.changes saved state --------

/// List the saved watermarks/snapshots for a pipeline (one per
/// xf.incremental / src.ducklake.changes node that has run). `pipeline_name`
/// is the run-log / state folder name (the pipeline's display name).
#[tauri::command]
fn watermark_list(
    workspace_path: String,
    pipeline_name: String,
) -> Result<Vec<duckle_duckdb_engine::watermark::WatermarkEntry>, String> {
    Ok(duckle_duckdb_engine::watermark::list(
        std::path::Path::new(&workspace_path),
        &pipeline_name,
    ))
}

/// Set a node's saved state for backfill. `kind` selects the shape:
/// "snapshot" writes a DuckLake CDC snapshot id (value parsed as u64);
/// anything else writes an incremental watermark { value, type }.
#[tauri::command]
fn watermark_set(
    workspace_path: String,
    pipeline_name: String,
    node_id: String,
    kind: String,
    value: String,
    value_type: Option<String>,
) -> Result<(), String> {
    let ws = std::path::Path::new(&workspace_path);
    if kind == "snapshot" {
        let id: u64 = value
            .trim()
            .parse()
            .map_err(|_| format!("snapshot id must be a number, got '{}'", value))?;
        duckle_duckdb_engine::watermark::set_snapshot(ws, &pipeline_name, &node_id, id)
            .map_err(|e| e.to_string())
    } else {
        duckle_duckdb_engine::watermark::set_incremental(
            ws,
            &pipeline_name,
            &node_id,
            &value,
            value_type.as_deref(),
        )
        .map_err(|e| e.to_string())
    }
}

/// Delete a node's saved state so the next run does a full reload.
#[tauri::command]
fn watermark_clear(
    workspace_path: String,
    pipeline_name: String,
    node_id: String,
) -> Result<(), String> {
    duckle_duckdb_engine::watermark::clear(
        std::path::Path::new(&workspace_path),
        &pipeline_name,
        &node_id,
    )
    .map_err(|e| e.to_string())
}

/// Signal the engine to stop at the next stage boundary. The current
/// stage (if mid-flight) still finishes; subsequent stages are
/// skipped.
#[tauri::command]
fn cancel_pipeline() -> Result<(), String> {
    // Cancel the active interactive run's own flag (not a shared global), so we
    // don't also stop concurrent scheduler runs.
    if let Some(e) = CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        e.request_cancel();
    }
    Ok(())
}

/// Compile a pipeline to DuckDB SQL without executing. Used by the
/// "Copy SQL" / "Export SQL" features so users can copy the generated
/// statements out of the app.
#[tauri::command]
fn compile_pipeline(pipeline: PipelineDoc) -> Result<Vec<StageSql>, String> {
    compile_pipeline_sql(&pipeline).map_err(|e| e.to_string())
}

/// Column-level lineage for the whole pipeline: each node's output columns
/// mapped to the root source columns they trace back to (#103). Read-only.
#[tauri::command]
fn pipeline_column_lineage(
    pipeline: PipelineDoc,
) -> Result<
    std::collections::HashMap<String, Vec<(String, Vec<duckle_duckdb_engine::lineage::RootColumn>)>>,
    String,
> {
    engine()?
        .pipeline_column_lineage(&pipeline)
        .map_err(|e| e.to_string())
}

// ---- Scheduler commands ------------------------------------------------

static SCHEDULER: OnceLock<Scheduler> = OnceLock::new();

fn scheduler() -> Result<&'static Scheduler, String> {
    SCHEDULER
        .get()
        .ok_or_else(|| "Scheduler not initialized".to_string())
}

/// Point the scheduler at a workspace folder; loads schedules from
/// `<workspace>/schedules.json`. Called by the frontend whenever the
/// workspace path changes.
#[tauri::command]
fn schedule_set_workspace(path: String) -> Result<(), String> {
    let sched = scheduler()?;
    // Publish the workspace to the engine so child-pipeline references
    // (Run Job / Iterate / Foreach) stored as bare pipeline ids resolve to
    // their `<workspace>/pipelines/<id>.json` file, including for headless
    // scheduled runs that never pass through the frontend. Called whenever
    // the workspace changes, so this stays in sync.
    if path.is_empty() {
        std::env::remove_var("DUCKLE_WORKSPACE");
        std::env::remove_var("DUCKLE_LOG_DIR");
    } else {
        std::env::set_var("DUCKLE_WORKSPACE", &path);
        // Universal, component-level run logging lands in the user's chosen
        // workspace under logs/ (NDJSON) for Splunk / Dynatrace ingestion.
        std::env::set_var("DUCKLE_LOG_DIR", PathBuf::from(&path).join("logs"));
        // Apply this workspace's saved HTTP proxy (if any) to the engine HTTP
        // layer so REST / cloud connectors and the updater route through it
        // without the user setting a system env var (#80).
        app_settings::apply_for_workspace(&path);
    }
    let p = if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(path))
    };
    sched.set_workspace(p);
    Ok(())
}

#[tauri::command]
fn schedule_list() -> Result<Vec<Schedule>, String> {
    Ok(scheduler()?.list())
}

#[tauri::command]
fn schedule_upsert(schedule: Schedule) -> Result<Schedule, String> {
    scheduler()?.upsert(schedule)
}

#[tauri::command]
fn schedule_delete(id: String) -> Result<(), String> {
    scheduler()?.delete(&id)
}

#[tauri::command]
async fn schedule_run_now(id: String) -> Result<RunResult, String> {
    scheduler()?.run_now(&id).await
}

// ---- Engine install (first-run guided setup) ---------------------------

/// Which engines are present in the app-data dir, and whether each can
/// be installed on this platform.
#[tauri::command]
fn engine_status(app: tauri::AppHandle) -> Result<Vec<EngineStatus>, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    Ok(engine_manager::status(&dir))
}

/// Download + install an engine (duckdb / slothdb / llamacpp) into
/// app-data, streaming progress.
#[tauri::command]
async fn engine_install(
    app: tauri::AppHandle,
    engine: String,
    on_progress: Channel<InstallProgress>,
) -> Result<String, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    let result = tokio::task::spawn_blocking(move || {
        engine_manager::install(&dir, &engine, |p| {
            let _ = on_progress.send(p);
        })
    })
    .await
    .map_err(|e| e.to_string())?;
    if let Err(ref e) = result {
        tracing::warn!("Engine install failed: {}", e);
    }
    result
}

/// Whether a free dbt engine (Apache dbt-core + dbt-duckdb, provisioned via uv)
/// is already installed in app-data. The xf.dbt node needs it; first launch
/// fetches it automatically in the background.
#[tauri::command]
fn dbt_status(app: tauri::AppHandle) -> Result<bool, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    Ok(dbt_engine::is_installed(&dir))
}

/// Provision (or re-provision) the free dbt engine on demand and return its
/// path. Idempotent: returns instantly if already installed. Use this to retry
/// after a failed first-launch background fetch.
#[tauri::command]
async fn dbt_install(app: tauri::AppHandle) -> Result<String, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || dbt_engine::ensure(&dir))
        .await
        .map_err(|e| e.to_string())?
        .map(|p| p.to_string_lossy().into_owned())
}

// ---- AI chat assistant -------------------------------------------------

/// Send a message to the local Qwen model and stream tokens back over
/// the `on_event` channel. Lazy-boots `llama-server` on the first call
/// of an app run; reuses the same subprocess for subsequent calls.
#[tauri::command]
async fn chat_send(
    app: tauri::AppHandle,
    history: Vec<ChatMessage>,
    on_event: Channel<ChatEvent>,
    workspace: Option<String>,
) -> Result<(), String> {
    // #92: route to an external OpenAI-compatible endpoint when one is
    // configured for this workspace, instead of booting the local Qwen model.
    let (base, model, key) = app_settings::ai_config(workspace.as_deref().unwrap_or(""));
    if let Some(base) = base {
        let endpoint = format!("{}/v1/chat/completions", base.trim_end_matches('/'));
        let model = model.unwrap_or_else(|| "gpt-4o-mini".to_string());
        return tokio::task::spawn_blocking(move || {
            if let Err(e) =
                llama_chat::chat_stream(&endpoint, key.as_deref(), &model, &history, |evt| {
                    let _ = on_event.send(evt);
                })
            {
                let _ = on_event.send(ChatEvent::Error { message: e.clone() });
                return Err(e);
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|e| e.to_string())?;
    }
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    let bin = engine_manager::llamacpp_path(&dir);
    let model = engine_manager::llama_model_path(&dir);
    tokio::task::spawn_blocking(move || {
        // Lazy boot: hold the mutex only long enough to check + spawn.
        let port = {
            let mut guard = llama_chat::LLAMA_SERVER.lock().unwrap();
            if guard.is_none() {
                match llama_chat::LlamaServer::spawn(&bin, &model) {
                    Ok(srv) => {
                        let p = srv.port();
                        *guard = Some(srv);
                        p
                    }
                    Err(e) => {
                        let _ = on_event.send(ChatEvent::Error { message: e.clone() });
                        return Err(e);
                    }
                }
            } else {
                guard.as_ref().unwrap().port()
            }
        };
        let url = format!("http://127.0.0.1:{}/v1/chat/completions", port);
        if let Err(e) = llama_chat::chat_stream(&url, None, "qwen2.5-coder", &history, |evt| {
            let _ = on_event.send(evt);
        }) {
            let _ = on_event.send(ChatEvent::Error { message: e.clone() });
            return Err(e);
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Pull a Duckle pipeline JSON out of an assistant message - the
/// model is asked to wrap pipelines in ```json fenced code blocks.
/// Returns the parsed JSON for the frontend to merge into the canvas.
#[tauri::command]
fn chat_extract_pipeline(text: String) -> Result<JsonValue, String> {
    llama_chat::extract_pipeline(&text)
}

// ---- In-app Git integration -------------------------------------------
// Wraps the system git CLI on the user's workspace folder so they can
// commit / push / pull / branch from inside Duckle. Auth: try without
// explicit creds first (system credential helper), fall back to a PAT
// prompt from the frontend on 401.

fn ws_path(workspace_path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(workspace_path)
}

#[tauri::command]
fn workspace_git_status(workspace_path: String) -> Result<workspace_git::GitStatus, String> {
    workspace_git::status(&ws_path(&workspace_path))
}

#[tauri::command]
fn workspace_git_init(workspace_path: String) -> Result<(), String> {
    workspace_git::init(&ws_path(&workspace_path))
}

#[tauri::command]
fn workspace_git_commit(workspace_path: String, message: String) -> Result<String, String> {
    let p = ws_path(&workspace_path);
    workspace_git::add_all(&p)?;
    workspace_git::commit(&p, &message)
}

#[tauri::command]
fn workspace_git_push(workspace_path: String) -> Result<String, String> {
    workspace_git::push(&ws_path(&workspace_path))
}

#[tauri::command]
fn workspace_git_pull(workspace_path: String) -> Result<String, String> {
    workspace_git::pull(&ws_path(&workspace_path))
}

#[tauri::command]
fn workspace_git_branches(workspace_path: String) -> Result<Vec<String>, String> {
    workspace_git::branches(&ws_path(&workspace_path))
}

#[tauri::command]
fn workspace_git_branch_create(workspace_path: String, name: String) -> Result<(), String> {
    workspace_git::branch_create(&ws_path(&workspace_path), &name)
}

#[tauri::command]
fn workspace_git_branch_checkout(workspace_path: String, name: String) -> Result<(), String> {
    workspace_git::branch_checkout(&ws_path(&workspace_path), &name)
}

#[tauri::command]
fn workspace_git_remote_set(workspace_path: String, url: String) -> Result<(), String> {
    workspace_git::remote_set(&ws_path(&workspace_path), &url)
}

#[tauri::command]
fn workspace_git_save_pat(workspace_path: String, token: String) -> Result<(), String> {
    workspace_git::save_pat(&ws_path(&workspace_path), &token)
}

#[tauri::command]
fn workspace_git_clear_pat(workspace_path: String) -> Result<(), String> {
    workspace_git::clear_pat(&ws_path(&workspace_path))
}

#[tauri::command]
async fn workspace_ci_status(workspace_path: String) -> Result<ci_status::CiStatus, String> {
    // HTTP call - keep off the main runtime thread.
    let p = ws_path(&workspace_path);
    tokio::task::spawn_blocking(move || ci_status::poll(&p))
        .await
        .map_err(|e| e.to_string())?
}

/// Check Duckle's GitHub releases for a build newer than this one. Returns a
/// quiet, non-fatal result (offline -> error field set, update_available
/// false) so the frontend can show an upgrade banner without ever blocking.
#[tauri::command]
async fn check_for_update() -> Result<update_check::UpdateInfo, String> {
    tokio::task::spawn_blocking(update_check::check)
        .await
        .map_err(|e| e.to_string())
}

/// In-app self-update: download + checksum-verify the latest release binary for
/// this OS, swap it over the running executable, then restart onto the new
/// build - so users never manually download a new file. Streams progress over
/// the channel; on success the app restarts itself.
#[tauri::command]
async fn self_update(
    app: tauri::AppHandle,
    on_progress: tauri::ipc::Channel<self_update::Progress>,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        self_update::run(|p| {
            let _ = on_progress.send(p);
        })
    })
    .await
    .map_err(|e| e.to_string())??;
    // The verified new binary is in place; relaunch onto it. (restart() is
    // typed `-> !`; on a worker thread it defers to the next exit event.)
    app.restart();
}

/// Test-only entry point for the headless update self-test (see
/// `self_update::selftest_main`). Compiled only with `--features
/// update-selftest`; never present in releases.
#[cfg(feature = "update-selftest")]
pub fn self_update_selftest() -> ! {
    self_update::selftest_main()
}

/// Test-only: drive the full update run() (check -> download -> verify -> swap)
/// against a local fake-release. Compiled only with `--features update-selftest`.
#[cfg(feature = "update-selftest")]
pub fn self_update_run_selftest() -> ! {
    self_update::selftest_run_main()
}

/// Write the embedded HOST duckle-runner bytes to a temp stub file and return
/// the path. The host runner is always the BUILDER (run with `build ...`); for
/// a same-OS target it is also the artifact stub. The file must have no open
/// handle while it runs, or Windows CreateProcess fails with
/// ERROR_SHARING_VIOLATION.
fn staged_stub() -> Result<PathBuf, String> {
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    stage_stub_bytes(EMBEDDED_RUNNER, suffix, "")
}

/// Write the embedded LINUX runner bytes to a temp stub file and return the
/// path. Used ONLY as the artifact --stub when Build Pipeline targets Linux
/// from a non-Linux host; it is read as bytes (prepended to the artifact),
/// never executed on the host. Errors if this build did not bundle a Linux
/// runner (see build.rs embed_runner_linux).
fn staged_linux_stub() -> Result<PathBuf, String> {
    if EMBEDDED_RUNNER_LINUX.is_empty() {
        return Err(
            "This build cannot target Linux: no Linux runner was bundled. Rebuild the desktop app after staging it with: bash scripts/build-runner-linux.sh"
                .to_string(),
        );
    }
    stage_stub_bytes(EMBEDDED_RUNNER_LINUX, "", "linux-")
}

/// Stage `bytes` to a temp file keyed by `tag` + byte length, returning the
/// path. Caching by size means repeated builds reuse the same already-on-disk
/// (already AV-scanned) file instead of rewriting and immediately executing a
/// fresh exe every time. Writes to a unique sibling then renames into place so
/// a concurrent build never sees a half-written stub.
fn stage_stub_bytes(bytes: &[u8], suffix: &str, tag: &str) -> Result<PathBuf, String> {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("duckle-stub-{}{}{}", tag, bytes.len(), suffix));
    let correct = |p: &std::path::Path| {
        std::fs::metadata(p)
            .map(|m| m.len() as usize == bytes.len())
            .unwrap_or(false)
    };
    if correct(&path) {
        return Ok(path);
    }
    let tmp = dir.join(format!(
        "duckle-stub-{}{}-{}{}",
        tag,
        bytes.len(),
        std::process::id(),
        suffix
    ));
    std::fs::write(&tmp, bytes).map_err(|e| format!("write stub: {}", e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("chmod stub: {}", e))?;
    }
    match std::fs::rename(&tmp, &path) {
        Ok(()) => Ok(path),
        // Windows rename fails if the destination exists; if another build
        // already staged a correct copy, use it.
        Err(_) if correct(&path) => {
            let _ = std::fs::remove_file(&tmp);
            Ok(path)
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(format!("stage stub: {}", e))
        }
    }
}

/// What target OSes this build of Duckle can produce a "Build Pipeline"
/// artifact for. The same-OS target always works; a Linux target on a non-Linux
/// host needs the bundled static Linux runner (embedded only when staged at
/// build time). macOS and Windows can only be built on their own OS. The
/// frontend uses this so it never offers a target it cannot actually produce.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildCapabilities {
    host_os: String,
    can_target_linux: bool,
}

#[tauri::command]
fn build_capabilities() -> BuildCapabilities {
    let host = std::env::consts::OS;
    BuildCapabilities {
        host_os: host.to_string(),
        // On a Linux host the native build already covers Linux; on any other
        // host it requires the bundled Linux runner.
        can_target_linux: host == "linux" || !EMBEDDED_RUNNER_LINUX.is_empty(),
    }
}

/// Build a self-contained, server-runnable single file for a workspace
/// pipeline using the embedded `duckle-runner build` subcommand. The embedded
/// HOST runner is always the builder; for a same-OS target it is also the
/// artifact stub, and for a Linux target on a non-Linux host the bundled Linux
/// runner is the stub and a Linux DuckDB is fetched. macOS can only be built on
/// a Mac. Returns the path to the produced single file on success.
#[tauri::command]
async fn build_pipeline_bundle(
    app: tauri::AppHandle,
    workspace_path: String,
    pipeline_id: String,
    out_file: String,
    context: Option<String>,
    secrets_mode: String,
    passphrase: Option<String>,
    target_os: Option<String>,
) -> Result<String, String> {
    if secrets_mode != "env" && secrets_mode != "passphrase" {
        return Err(format!("secrets mode must be env|passphrase, got {}", secrets_mode));
    }
    if secrets_mode == "passphrase" && passphrase.as_deref().unwrap_or("").is_empty() {
        return Err("Passphrase is required for passphrase mode".to_string());
    }

    let host = std::env::consts::OS;
    let target = target_os.as_deref().unwrap_or(host).to_string();
    match target.as_str() {
        "windows" | "linux" | "macos" => {}
        other => return Err(format!("target OS must be windows|linux|macos, got {}", other)),
    }

    // A Linux artifact can be cross-built on a non-Linux host using the bundled
    // static Linux runner + a fetched Linux DuckDB. macOS and Windows artifacts
    // can only be produced on their own OS (Apple's toolchain is Mac-only; we do
    // not bundle a cross Windows runner).
    let cross_linux = target != host && target == "linux";
    if target != host {
        match target.as_str() {
            "macos" => {
                return Err(
                    "Building a macOS file requires a Mac. Apple's toolchain and code signing are only available on macOS, so run Duckle on a Mac to build the macOS artifact."
                        .to_string(),
                )
            }
            "windows" => {
                return Err(
                    "Building a Windows file requires running Duckle on Windows.".to_string(),
                )
            }
            "linux" => {
                if EMBEDDED_RUNNER_LINUX.is_empty() {
                    return Err(
                        "This build cannot target Linux: no Linux runner was bundled. Rebuild the desktop app after staging it with: bash scripts/build-runner-linux.sh"
                            .to_string(),
                    );
                }
            }
            _ => unreachable!(),
        }
    }

    // Same-OS target uses the host engine; cross-Linux fetches the target engine
    // inside the blocking task (network).
    let host_duckdb = if cross_linux {
        None
    } else {
        let duckdb = DUCKDB_BIN
            .get()
            .cloned()
            .ok_or_else(|| "Engine path not resolved yet (open the app fully first)".to_string())?;
        // The runner treats a non-existent --duckdb as "no duckdb" and still
        // produces a (best-effort) artifact that needs duckdb on PATH. That's by
        // design, but warn so the missing-self-contained case is visible in the
        // logs rather than silent. See issue #2 (DUCKDB_BIN is set unconditionally
        // during setup even before the CLI is installed).
        if !duckdb.exists() {
            tracing::warn!(
                "build_pipeline_bundle: duckdb not found at {} - the file will not embed duckdb and will rely on it being on PATH at run time",
                duckdb.display()
            );
        }
        Some(duckdb)
    };
    let app_data = if cross_linux {
        Some(app.path().app_data_dir().map_err(|e| e.to_string())?)
    } else {
        None
    };

    let out_fallback = out_file.clone();

    let output = tokio::task::spawn_blocking(move || {
        // The host runner is always the builder (executed on this OS).
        let builder = staged_stub()?;
        // The artifact stub + duckdb are for the TARGET OS.
        let (artifact_stub, duckdb) = if cross_linux {
            let stub = staged_linux_stub()?;
            let app_data = app_data.expect("app_data resolved for cross-linux build");
            // The bundled Linux runner stub is x86_64-only (built by
            // scripts/build-runner-linux.sh as x86_64-musl), so the bundled
            // DuckDB and the manifest arch must be x86_64 too, regardless of the
            // build host's arch. Pinning here keeps an ARM host from pairing an
            // aarch64 duckdb with the x86_64 stub.
            let duckdb = engine_manager::ensure_cross_duckdb(&app_data, "linux", "x86_64")?;
            (stub, duckdb)
        } else {
            (builder.clone(), host_duckdb.expect("host duckdb resolved for same-os build"))
        };
        let spawn_once = || {
            let mut cmd = std::process::Command::new(&builder);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
            }
            cmd.arg("build")
                .arg("--workspace").arg(&workspace_path)
                .arg("--pipeline-id").arg(&pipeline_id)
                .arg("--out").arg(&out_file)
                .arg("--secrets").arg(&secrets_mode)
                .arg("--stub").arg(&artifact_stub)
                .arg("--duckdb").arg(&duckdb);
            if cross_linux {
                cmd.arg("--target-os").arg("linux").arg("--target-arch").arg("x86_64");
            }
            if let Some(ctx) = context.as_deref() {
                if !ctx.is_empty() {
                    cmd.arg("--context").arg(ctx);
                }
            }
            if secrets_mode == "passphrase" {
                cmd.env("DUCKLE_BUNDLE_PASSPHRASE", passphrase.clone().unwrap_or_default());
            }
            cmd.output()
        };
        // Windows antivirus can briefly lock a just-written exe, so the first
        // execute returns ERROR_SHARING_VIOLATION (os error 32). Retry a few
        // times before giving up; the cached stub means later builds skip this.
        let mut attempt = 0;
        loop {
            match spawn_once() {
                Ok(o) => return Ok(o),
                Err(e) if e.raw_os_error() == Some(32) && attempt < 10 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                Err(e) => return Err(format!("failed to start duckle-runner: {}", e)),
            }
        }
    })
    .await
    .map_err(|e| e.to_string())??;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if err.is_empty() { "duckle-runner build failed".to_string() } else { err });
    }

    // The build subcommand prints `duckle-runner build: wrote <path>` to STDERR.
    let stderr = String::from_utf8_lossy(&output.stderr);
    const PREFIX: &str = "duckle-runner build: wrote ";
    let file_path = stderr
        .lines()
        .filter_map(|l| l.trim().strip_prefix(PREFIX))
        .last()
        .map(|s| s.trim().to_string());
    match file_path {
        Some(p) => Ok(p),
        None => {
            // The runner reliably emits the prefix (build.rs); if it ever
            // stops, fall back to the chosen out file but warn loudly.
            tracing::warn!(
                "build_pipeline_bundle: runner did not print the '{}' line; returning the out file as a fallback path",
                PREFIX.trim()
            );
            Ok(out_fallback)
        }
    }
}

// ---- MCP server connection -------------------------------------------------

/// What the MCP popup needs to show the user: the staged binary paths, a
/// ready-to-paste `claude mcp add` command, a generic mcpServers JSON config,
/// and flags for whether the server is bundled / DuckDB is installed / the
/// Claude Code CLI is present.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct McpConnInfo {
    bundled: bool,
    duckdb_found: bool,
    claude_cli: bool,
    mcp_path: String,
    duckdb_path: String,
    runner_path: String,
    claude_command: String,
    config_json: String,
}

/// Write `bytes` to `path` only when the on-disk size differs, via a unique
/// sibling + rename so a concurrent reader never sees a half-written file.
fn write_if_changed(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    let same = std::fs::metadata(path)
        .map(|m| m.len() as usize == bytes.len())
        .unwrap_or(false);
    if same {
        return Ok(());
    }
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    std::fs::write(&tmp, bytes).map_err(|e| format!("write {}: {}", tmp.display(), e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755));
    }
    // Put the new file in place. A plain rename over the destination fails on
    // Windows with "Access denied" when the destination .exe is locked - e.g. a
    // running duckle-mcp/duckle-runner that an MCP client still has open. Windows
    // DOES allow renaming a locked file out of the way, so on failure we move the
    // old one aside and retry; the displaced copy is removed best-effort (it goes
    // away on a later run once nothing holds it open).
    if std::fs::rename(&tmp, path).is_ok() {
        return Ok(());
    }
    let aside = path.with_extension(format!("old{}", std::process::id()));
    let _ = std::fs::remove_file(&aside);
    if path.exists() && std::fs::rename(path, &aside).is_ok() {
        match std::fs::rename(&tmp, path) {
            Ok(()) => {
                let _ = std::fs::remove_file(&aside);
                return Ok(());
            }
            Err(e) => {
                // Restore the original so we never leave the slot empty.
                let _ = std::fs::rename(&aside, path);
                let _ = std::fs::remove_file(&tmp);
                if std::fs::metadata(path)
                    .map(|m| m.len() as usize == bytes.len())
                    .unwrap_or(false)
                {
                    return Ok(());
                }
                return Err(format!("stage {}: {}", path.display(), e));
            }
        }
    }
    // Last resort: an existing file of the right size is good enough (another
    // instance staged it concurrently).
    let _ = std::fs::remove_file(&tmp);
    if std::fs::metadata(path)
        .map(|m| m.len() as usize == bytes.len())
        .unwrap_or(false)
    {
        return Ok(());
    }
    Err(format!("stage {}: locked (close other Duckle instances)", path.display()))
}

/// Stage the embedded MCP server into a stable app-data dir, with the embedded
/// runner written alongside it (so duckle-mcp's sibling lookup finds the runner
/// for build_pipeline). Returns (mcp_path, runner_path).
fn stage_mcp(app_data: &std::path::Path) -> Result<(PathBuf, PathBuf), String> {
    if EMBEDDED_MCP.is_empty() {
        return Err("This build does not bundle the duckle-mcp server".to_string());
    }
    let dir = app_data.join("engines").join("mcp");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {}", dir.display(), e))?;
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let mcp = dir.join(format!("duckle-mcp{suffix}"));
    write_if_changed(&mcp, EMBEDDED_MCP)?;
    let runner = dir.join(format!("duckle-runner{suffix}"));
    write_if_changed(&runner, EMBEDDED_RUNNER)?;
    Ok((mcp, runner))
}

/// Double-quote a token for a copyable shell command line (paths have spaces).
fn shell_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\\\""))
}

// ── Web panel (duckle-runner serve) ──

/// A running web-panel server child, kept so re-opening reuses it and app exit
/// can stop it.
struct WebPanel {
    port: u16,
    child: std::process::Child,
}

static WEB_PANEL: std::sync::Mutex<Option<WebPanel>> = std::sync::Mutex::new(None);

/// `duckle serve [args...]` - launch the web management console straight from a
/// terminal using just the desktop binary (no separate runner download needed).
/// Delegates to the embedded headless runner's `serve` subcommand and forwards
/// any args (`--workspace`, `--port`, ...). Returns false when the first arg is
/// not `serve` (normal GUI launch); on the serve path it never returns - it runs
/// the server and exits with its status.
pub fn run_serve_cli() -> bool {
    let mut it = std::env::args();
    let _exe = it.next();
    if it.next().as_deref() != Some("serve") {
        return false;
    }
    let rest: Vec<String> = it.collect();
    // A GUI-subsystem binary has no console of its own; reattach to the terminal
    // that launched us so the runner's output is visible.
    #[cfg(windows)]
    unsafe {
        extern "system" {
            fn AttachConsole(dw_process_id: u32) -> i32;
        }
        AttachConsole(0xFFFF_FFFFu32); // ATTACH_PARENT_PROCESS
    }
    if EMBEDDED_RUNNER.is_empty() {
        eprintln!("duckle serve: this build does not bundle the runner");
        std::process::exit(1);
    }
    let dir = std::env::temp_dir().join("duckle-serve");
    let _ = std::fs::create_dir_all(&dir);
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let runner = dir.join(format!("duckle-runner{suffix}"));
    if let Err(e) = write_if_changed(&runner, EMBEDDED_RUNNER) {
        eprintln!("duckle serve: {e}");
        std::process::exit(1);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755));
    }
    let code = std::process::Command::new(&runner)
        .arg("serve")
        .args(&rest)
        .status()
        .map(|s| s.code().unwrap_or(0))
        .unwrap_or_else(|e| {
            eprintln!("duckle serve: failed to start: {e}");
            1
        });
    std::process::exit(code);
}

/// Stage the embedded host runner into a stable app-data dir so the long-lived
/// `serve` process runs from a fixed path (not a temp stub).
fn stage_panel_runner(app_data: &std::path::Path) -> Result<PathBuf, String> {
    if EMBEDDED_RUNNER.is_empty() {
        return Err("This build does not bundle duckle-runner".to_string());
    }
    let dir = app_data.join("engines").join("panel");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {}", dir.display(), e))?;
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let runner = dir.join(format!("duckle-runner{suffix}"));
    write_if_changed(&runner, EMBEDDED_RUNNER)?;
    Ok(runner)
}

/// Pick a port for the panel: prefer 8080, else an OS-assigned free port.
fn pick_panel_port() -> u16 {
    if std::net::TcpListener::bind(("127.0.0.1", 8080u16)).is_ok() {
        return 8080;
    }
    std::net::TcpListener::bind(("127.0.0.1", 0u16))
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|a| a.port())
        .unwrap_or(8080)
}

/// Start (or reuse) the web management console for the current workspace and
/// return its URL. Spawns the embedded `duckle-runner serve` against the
/// workspace on a local port; the frontend then opens the URL in the browser.
#[tauri::command]
fn open_web_panel(app: tauri::AppHandle, workspace: String) -> Result<String, String> {
    if workspace.trim().is_empty() {
        return Err("Open or create a workspace first".to_string());
    }
    let mut guard = WEB_PANEL.lock().map_err(|_| "panel lock poisoned".to_string())?;
    // Reuse a still-running panel.
    if let Some(p) = guard.as_mut() {
        if matches!(p.child.try_wait(), Ok(None)) {
            return Ok(format!("http://127.0.0.1:{}", p.port));
        }
        *guard = None; // previous server exited; start a fresh one
    }

    let app_data = app.path().app_data_dir().map_err(|e| e.to_string())?;
    let duckdb = engine_manager::duckdb_path(&app_data);
    let runner = stage_panel_runner(&app_data)?;
    let port = pick_panel_port();

    let mut cmd = std::process::Command::new(&runner);
    cmd.arg("serve")
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--workspace")
        .arg(&workspace)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // Only pass --duckdb when the resolved binary exists; otherwise let the
    // runner fall back (env / sibling / PATH) instead of erroring on a missing
    // explicit path.
    if duckdb.exists() {
        cmd.arg("--duckdb").arg(&duckdb).env("DUCKLE_DUCKDB_BIN", &duckdb);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let child = cmd.spawn().map_err(|e| format!("start web panel: {}", e))?;

    // Wait until the server accepts connections (up to ~3s).
    let addr = format!("127.0.0.1:{}", port);
    let mut up = false;
    for _ in 0..30 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            up = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    *guard = Some(WebPanel { port, child });
    if !up {
        return Err("web panel did not start in time".to_string());
    }
    Ok(format!("http://{}", addr))
}

/// Kill the running web-panel server, if any. Best effort; called on app exit.
fn stop_web_panel_silent() {
    if let Ok(mut guard) = WEB_PANEL.lock() {
        if let Some(mut p) = guard.take() {
            let _ = p.child.kill();
        }
    }
}

/// Best-effort probe for the Claude Code CLI (`claude --version`).
fn claude_available() -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        std::process::Command::new("cmd")
            .creation_flags(0x0800_0000) // CREATE_NO_WINDOW
            .raw_arg("/C claude --version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
    #[cfg(not(windows))]
    {
        std::process::Command::new("claude")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}

/// Stage the bundled MCP server and return everything the popup renders:
/// resolved paths, a `claude mcp add` one-liner, and an mcpServers JSON config.
#[tauri::command]
fn mcp_connection_info(app: tauri::AppHandle) -> Result<McpConnInfo, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|e| format!("app data dir: {}", e))?;
    let bundled = !EMBEDDED_MCP.is_empty();
    let (mcp_path, runner_path) = if bundled {
        stage_mcp(&app_data)?
    } else {
        (PathBuf::new(), PathBuf::new())
    };
    let duckdb = DUCKDB_BIN.get().cloned().unwrap_or_default();
    let duckdb_found = duckdb.exists();

    let mcp_s = mcp_path.to_string_lossy().to_string();
    let runner_s = runner_path.to_string_lossy().to_string();
    let duckdb_s = duckdb.to_string_lossy().to_string();

    let claude_command = format!(
        "claude mcp add duckle --env {} --env {} -- {}",
        shell_quote(&format!("DUCKLE_DUCKDB_BIN={}", duckdb_s)),
        shell_quote(&format!("DUCKLE_RUNNER_BIN={}", runner_s)),
        shell_quote(&mcp_s),
    );

    let config = serde_json::json!({
        "mcpServers": {
            "duckle": {
                "command": mcp_s,
                "env": { "DUCKLE_DUCKDB_BIN": duckdb_s, "DUCKLE_RUNNER_BIN": runner_s }
            }
        }
    });
    let config_json = serde_json::to_string_pretty(&config).unwrap_or_default();

    Ok(McpConnInfo {
        bundled,
        duckdb_found,
        claude_cli: claude_available(),
        mcp_path: mcp_s,
        duckdb_path: duckdb_s,
        runner_path: runner_s,
        claude_command,
        config_json,
    })
}

/// Run `claude mcp add duckle ...` so the user is connected to Claude Code in
/// one click. Returns the CLI output on success; errors (with a hint to copy
/// the command) when the CLI is missing or the add fails.
#[tauri::command]
async fn connect_claude_code(app: tauri::AppHandle) -> Result<String, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|e| format!("app data dir: {}", e))?;
    let (mcp_path, runner_path) = stage_mcp(&app_data)?;
    let duckdb = DUCKDB_BIN.get().cloned().unwrap_or_default();
    let mcp_s = mcp_path.to_string_lossy().to_string();
    let runner_s = runner_path.to_string_lossy().to_string();
    let duckdb_s = duckdb.to_string_lossy().to_string();

    let output = tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            // raw_arg so cmd resolves the claude.cmd npm shim and our quoting
            // survives; each path is wrapped so spaces do not split args.
            let line = format!(
                "/C claude mcp add duckle --env \"DUCKLE_DUCKDB_BIN={}\" --env \"DUCKLE_RUNNER_BIN={}\" -- \"{}\"",
                duckdb_s, runner_s, mcp_s
            );
            std::process::Command::new("cmd")
                .creation_flags(0x0800_0000) // CREATE_NO_WINDOW
                .raw_arg(line)
                .output()
        }
        #[cfg(not(windows))]
        {
            std::process::Command::new("claude")
                .arg("mcp")
                .arg("add")
                .arg("duckle")
                .arg("--env")
                .arg(format!("DUCKLE_DUCKDB_BIN={}", duckdb_s))
                .arg("--env")
                .arg(format!("DUCKLE_RUNNER_BIN={}", runner_s))
                .arg("--")
                .arg(&mcp_s)
                .output()
        }
    })
    .await
    .map_err(|e| format!("join: {}", e))?;

    match output {
        Ok(o) if o.status.success() => {
            let out = String::from_utf8_lossy(&o.stdout);
            let err = String::from_utf8_lossy(&o.stderr);
            let msg = format!("{} {}", out.trim(), err.trim());
            Ok(if msg.trim().is_empty() {
                "Added the duckle MCP server to Claude Code.".to_string()
            } else {
                msg.trim().to_string()
            })
        }
        Ok(o) => {
            let err = String::from_utf8_lossy(&o.stderr);
            let out = String::from_utf8_lossy(&o.stdout);
            let detail = if err.trim().is_empty() { out.trim() } else { err.trim() };
            Err(format!("claude mcp add failed: {}", detail))
        }
        Err(e) => Err(format!(
            "Claude Code CLI not found (is `claude` installed and on PATH?). Copy the command instead. ({})",
            e
        )),
    }
}

/// The MCP servers config file for a desktop client.
fn mcp_client_config_path(app: &tauri::AppHandle, client: &str) -> Result<PathBuf, String> {
    match client {
        // %APPDATA%/Claude/... (Windows standalone), ~/Library/Application
        // Support/Claude/... (macOS), ~/.config/Claude/... (Linux).
        // The Windows STORE (MSIX) Claude Desktop sandboxes its config under
        // %LOCALAPPDATA%/Packages/Claude_*/LocalCache/Roaming/Claude/ and
        // ignores the standalone path entirely - prefer the MSIX path when the
        // packaged install is present.
        "claude_desktop" => {
            #[cfg(windows)]
            {
                if let Ok(local) = app.path().local_data_dir() {
                    if let Ok(entries) = std::fs::read_dir(local.join("Packages")) {
                        for e in entries.flatten() {
                            if e.file_name().to_string_lossy().starts_with("Claude_") {
                                let dir = e
                                    .path()
                                    .join("LocalCache")
                                    .join("Roaming")
                                    .join("Claude");
                                if dir.is_dir() {
                                    return Ok(dir.join("claude_desktop_config.json"));
                                }
                            }
                        }
                    }
                }
            }
            let cfg = app.path().config_dir().map_err(|e| format!("config dir: {}", e))?;
            Ok(cfg.join("Claude").join("claude_desktop_config.json"))
        }
        // Cursor reads a global ~/.cursor/mcp.json.
        "cursor" => {
            let home = app.path().home_dir().map_err(|e| format!("home dir: {}", e))?;
            Ok(home.join(".cursor").join("mcp.json"))
        }
        other => Err(format!("unknown MCP client: {}", other)),
    }
}

/// Inject (merge) a "duckle" entry into a desktop MCP client's config file,
/// preserving any existing servers. Returns the written config path. These are
/// per-user config files (no elevation needed); on a permission/parse failure
/// the error tells the user to retry elevated or copy the config manually.
#[tauri::command]
fn mcp_inject_config(app: tauri::AppHandle, client: String) -> Result<String, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|e| format!("app data dir: {}", e))?;
    let (mcp_path, runner_path) = stage_mcp(&app_data)?;
    let duckdb = DUCKDB_BIN.get().cloned().unwrap_or_default();
    let target = mcp_client_config_path(&app, &client)?;

    // Read the existing config (preserve other servers) or start fresh.
    let mut root: JsonValue = if target.exists() {
        let text = std::fs::read_to_string(&target)
            .map_err(|e| format!("read {}: {}", target.display(), e))?;
        if text.trim().is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&text).map_err(|e| {
                format!(
                    "{} is not valid JSON ({}); add the duckle entry manually instead",
                    target.display(),
                    e
                )
            })?
        }
    } else {
        serde_json::json!({})
    };

    {
        let obj = root
            .as_object_mut()
            .ok_or_else(|| format!("{} root is not a JSON object", target.display()))?;
        let servers = obj
            .entry("mcpServers")
            .or_insert_with(|| serde_json::json!({}));
        let servers = servers
            .as_object_mut()
            .ok_or_else(|| "mcpServers is not a JSON object".to_string())?;
        servers.insert(
            "duckle".to_string(),
            serde_json::json!({
                "command": mcp_path.to_string_lossy(),
                "args": [],
                "env": {
                    "DUCKLE_DUCKDB_BIN": duckdb.to_string_lossy(),
                    "DUCKLE_RUNNER_BIN": runner_path.to_string_lossy()
                }
            }),
        );
    }

    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {}", parent.display(), e))?;
    }
    let pretty = serde_json::to_string_pretty(&root).map_err(|e| e.to_string())?;
    // Write to a sibling temp file then rename over the original so a mid-write
    // failure can't truncate the user's existing MCP client config.
    let tmp = target.with_extension(format!("duckletmp{}", std::process::id()));
    let write_err = |e: std::io::Error| {
        format!(
            "could not write {} ({}). If this needs elevated permissions, run Duckle as administrator and retry, or copy the config manually.",
            target.display(),
            e
        )
    };
    std::fs::write(&tmp, pretty).map_err(write_err)?;
    std::fs::rename(&tmp, &target).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        write_err(e)
    })?;
    Ok(target.to_string_lossy().to_string())
}
