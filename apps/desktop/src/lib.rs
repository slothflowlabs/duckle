//! Duckle desktop shell.
//!
//! Boots the Tauri runtime, wires it to `duckle-runtime`, and exposes
//! invoke commands to the frontend.

use duckle_connectors::CsvConnector;
use duckle_duckdb_engine::{
    append_run_record, compile_pipeline_sql, load_run_history, plans, DuckdbEngine, PipelineDoc,
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
mod deploy;
mod pixeltable_engine;
mod engine_manager;
mod llama_chat;
mod samples;
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

/// The LanceDB sidecar (duckle-lance), embedded at compile time when staged at
/// apps/desktop/bin/. Empty when this build did not bundle it (see build.rs
/// embed_lance). Staged to a temp stub + DUCKLE_LANCE_BIN at startup so
/// src.lancedb / snk.lancedb work out of the box; absent -> the engine falls
/// back to a duckle-lance on PATH.
const EMBEDDED_LANCE: &[u8] = include_bytes!(env!("DUCKLE_EMBEDDED_LANCE"));

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // #169: on Linux the webview is webkitgtk, whose GTK/GDK stack crashes at
    // startup on several Wayland compositors - reported on KDE Plasma 6 with the
    // NVIDIA proprietary driver - with "Error 71 (Protocol error) dispatching to
    // Wayland display", so the window never opens. Route the toolkit through
    // XWayland, and disable webkit's DMA-BUF renderer (which separately blanks
    // the window on NVIDIA + newer webkitgtk). Both respect an explicit user
    // override, and must be set before GTK initializes (before the builder).
    #[cfg(target_os = "linux")]
    {
        if std::env::var_os("WAYLAND_DISPLAY").is_some()
            && std::env::var_os("GDK_BACKEND").is_none()
        {
            std::env::set_var("GDK_BACKEND", "x11");
        }
        if std::env::var_os("WEBKIT_DISABLE_DMABUF_RENDERER").is_none() {
            std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
        }
    }

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
            // Resolve the engine binary, honoring an externally-set
            // DUCKLE_DUCKDB_BIN first (issue #179) so a user-supplied DuckDB
            // wins over the bundled one, then publish the resolved path back to
            // DUCKLE_DUCKDB_BIN. The engine's primary execution path takes the
            // binary as a constructor arg, but rest_source_apply (used by
            // REST-shaped sources: Oracle, SQL Server, Snowflake, Databricks,
            // Synapse, BigQuery, and the various SaaS aliases that materialize
            // their inline result set) is a free helper that reads the env var
            // directly. Without this set, those sources fail with
            // "DUCKLE_DUCKDB_BIN not set" while plain file flows work fine. See
            // issue #2.
            if let Ok(dir) = app.path().app_data_dir() {
                let bin = resolve_duckdb_bin(&dir);
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
                pixeltable_engine::publish_if_present(&dir);
            }
            // Stage the bundled LanceDB sidecar (if this build carries one) and
            // point the engine at it, so src.lancedb / snk.lancedb work without a
            // separate install. Empty -> the engine falls back to a duckle-lance
            // on PATH or DUCKLE_LANCE_BIN.
            if !EMBEDDED_LANCE.is_empty() {
                match staged_lance() {
                    Ok(p) => std::env::set_var("DUCKLE_LANCE_BIN", p),
                    Err(e) => tracing::warn!("duckle-lance staging failed: {e}"),
                }
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
            describe_node_columns,
            pipeline_column_lineage,
            pipeline_trust_report,
            schedule_set_workspace,
            schedule_list,
            schedule_upsert,
            schedule_delete,
            schedule_run_now,
            runner_stage,
            plans_list,
            plans_save,
            plans_delete,
            plans_run,
            workspace_catalog,
            workspace_catalog_rebuild,
            workspace_catalog_annotate,
            workspace_catalog_inspect,
            engine_status,
            engine_install,
            llama_models,
            llama_default_model,
            dbt_status,
            dbt_install,
            pixeltable_status,
            pixeltable_install,
            seed_sample_workspace,
            import_job_file,
            chat_send,
            chat_extract_pipeline,
            workspace_git_status,
            workspace_git_init,
            workspace_git_commit,
            workspace_git_push,
            workspace_git_pull,
            workspace_git_branches,
            deploy_targets,
            deploy_target_save,
            deploy_target_probe,
            deploy_target_claim,
            deploy_target_remove,
            deploy_target_check,
            deploy_pipeline,
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
            app_settings::settings_get_allow_unsigned,
            app_settings::settings_set_allow_unsigned,
            app_settings::settings_get_power,
            app_settings::settings_set_power,
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

/// Resolve the DuckDB CLI the desktop should drive. An externally-set
/// `DUCKLE_DUCKDB_BIN` takes precedence, so a user can point Duckle at a
/// system-installed DuckDB, a specific version, or a custom binary (embedded
/// scenarios); otherwise the bundled/downloaded engine path is used. Issue #179.
fn resolve_duckdb_bin(app_data: &std::path::Path) -> PathBuf {
    pick_duckdb_bin(std::env::var_os("DUCKLE_DUCKDB_BIN"), app_data)
}

/// Pure precedence logic behind [`resolve_duckdb_bin`], split out so it can be
/// tested without mutating the process environment: a non-empty override wins,
/// otherwise the bundled engine path.
fn pick_duckdb_bin(env_override: Option<std::ffi::OsString>, app_data: &std::path::Path) -> PathBuf {
    env_override
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| engine_manager::duckdb_path(app_data))
}

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
/// #223: the Pixeltable connector needs a private Python, and `ensure` was
/// only reachable from a Tauri command that nothing ever called - so a user
/// who had never installed it never could, and the node fell back to bare
/// `python3`. Provision it here, the first time a run actually contains one
/// of its nodes, which is the contract the feature was documented with.
///
/// Failure is deliberately not fatal: the engine still raises its own, more
/// specific error when the interpreter turns out to lack pixeltable.
fn ensure_pixeltable_if_used(app: &tauri::AppHandle, pipeline: &PipelineDoc) {
    let used = pipeline.nodes.iter().any(|n| {
        n.data
            .component_id
            .as_deref()
            .is_some_and(|c| c == "src.pixeltable" || c == "snk.pixeltable")
    });
    if !used {
        return;
    }
    let Ok(dir) = app.path().app_data_dir() else {
        return;
    };
    if let Err(e) = pixeltable_engine::ensure(&dir) {
        eprintln!("pixeltable provisioning failed: {e}");
    }
}

#[tauri::command]
async fn run_pipeline(
    app: tauri::AppHandle,
    pipeline: PipelineDoc,
    on_event: Channel<PipelineEvent>,
    pipeline_id: Option<String>,
    pipeline_name: Option<String>,
    workspace_path: Option<String>,
) -> Result<RunResult, String> {
    let engine = engine()?.for_new_run();
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = Some(engine.clone());
    // Resolve ${ENV:NAME} from the OS environment before running, so canvas runs
    // see process env vars like the headless runner does (issue #137). The
    // frontend already resolved ${workspace}/${context}/date builtins.
    // Saved Salesforce connections resolve first (#166 stage 2) so a
    // connection field stored as ${ENV:...} still expands below.
    let mut pipeline = pipeline;
    resolve_saved_connections(&mut pipeline, &workspace_path)?;
    duckle_duckdb_engine::context::apply_env(&mut pipeline);
    duckle_duckdb_engine::context::apply_vault(&mut pipeline);
    ensure_pixeltable_if_used(&app, &pipeline);
    let name = pipeline_name.clone();
    let receipt = begin_desktop_run(&workspace_path, &pipeline, pipeline_id.as_deref().unwrap_or("pipeline"), "desktop");
    let joined = tokio::task::spawn_blocking(move || {
        engine.execute_pipeline_with_events(&pipeline, None, name.as_deref(), |evt| {
            let _ = on_event.send(evt);
        })
    })
    .await;
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = None;
    let result = joined.map_err(|e| e.to_string())?;
    if let Some((ws, r)) = receipt {
        duckle_duckdb_engine::retry::finish(&ws, r, &result.status, duckle_duckdb_engine::retry::nodes_of(&result));
    }
    record_history(&pipeline_id, &workspace_path, &result, "manual");
    Ok(result)
}

/// #259: record a canvas run, before and after.
///
/// The desktop is the most-used way to start a run, and it recorded no run id
/// at all: `record_history` goes through `RunRecord::from_result`, which
/// hard-codes `run_id: None`. So the runs people actually start were the ones
/// that could not be found afterwards, which is the hole #259 exists to close.
fn begin_desktop_run(
    workspace: &Option<String>,
    pipeline: &duckle_duckdb_engine::PipelineDoc,
    pipeline_id: &str,
    trigger: &str,
) -> Option<(std::path::PathBuf, duckle_duckdb_engine::retry::RunReceipt)> {
    // No workspace means nowhere to record into, which is the scratch-canvas
    // case rather than an error.
    let workspace = std::path::PathBuf::from(workspace.as_deref().filter(|w| !w.is_empty())?);
    let workspace = &workspace;
    let hash = duckle_duckdb_engine::retry::pipeline_hash(pipeline);
    let run_id = duckle_duckdb_engine::retry::new_run_id(pipeline_id, trigger);
    Some((
        workspace.clone(),
        duckle_duckdb_engine::retry::begin(
            workspace,
            &run_id,
            trigger,
            pipeline_id,
            &workspace
                .join("pipelines")
                .join(format!("{pipeline_id}.json"))
                .display()
                .to_string(),
            &hash,
            None,
        ),
    ))
}

/// #166 stage 2: expand saved Salesforce connection refs into node auth props
/// before the `${ENV:}` pass, so the node stores only a `connectionRef` and
/// secrets never land in the pipeline file. A ref without a workspace to
/// resolve it from is a clear error rather than a downstream auth failure.
fn resolve_saved_connections(
    pipeline: &mut PipelineDoc,
    workspace_path: &Option<String>,
) -> Result<(), String> {
    match workspace_path {
        Some(ws) => {
            duckle_secrets::resolve_connection_refs(std::path::Path::new(ws), &mut pipeline.nodes)
        }
        None if duckle_secrets::has_connection_refs(&pipeline.nodes) => Err(
            "this pipeline uses a saved connection; run it from a workspace \
             so the connection can be resolved"
            .into(),
        ),
        None => Ok(()),
    }
}

fn record_history(
    pipeline_id: &Option<String>,
    workspace_path: &Option<String>,
    result: &RunResult,
    trigger: &str,
) {
    if let (Some(id), Some(ws)) = (pipeline_id, workspace_path) {
        let record =
            RunRecord::from_result_in(std::path::Path::new(ws), id, result, trigger);
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
    app: tauri::AppHandle,
    pipeline: PipelineDoc,
    target_node_id: String,
    on_event: Channel<PipelineEvent>,
    pipeline_id: Option<String>,
    pipeline_name: Option<String>,
    workspace_path: Option<String>,
) -> Result<RunResult, String> {
    let engine = engine()?.for_new_run();
    *CURRENT_RUN.lock().unwrap_or_else(|p| p.into_inner()) = Some(engine.clone());
    // Resolve ${ENV:NAME} from the OS environment before running (issue #137);
    // saved Salesforce connections resolve first (#166 stage 2).
    let mut pipeline = pipeline;
    resolve_saved_connections(&mut pipeline, &workspace_path)?;
    duckle_duckdb_engine::context::apply_env(&mut pipeline);
    duckle_duckdb_engine::context::apply_vault(&mut pipeline);
    ensure_pixeltable_if_used(&app, &pipeline);
    let target = target_node_id;
    let name = pipeline_name.clone();
    // Run-to-here is still a run, and the one most likely to be asked about
    // afterwards ("what did that node actually produce?").
    let receipt = begin_desktop_run(&workspace_path, &pipeline, pipeline_id.as_deref().unwrap_or("pipeline"), "desktop-partial");
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
    if let Some((ws, r)) = receipt {
        duckle_duckdb_engine::retry::finish(&ws, r, &result.status, duckle_duckdb_engine::retry::nodes_of(&result));
    }
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

/// #226: the columns a node really produces, without running it.
///
/// The editor derives each node's schema from a per-component table in the
/// frontend, and there are far more components than entries in it. A component
/// that is missing falls through to "schema unchanged", so columns it adds
/// never reach the Schema or Preview tabs and a column it drops stays listed
/// and renders empty. This asks DuckDB instead, by running the node's own
/// compiled SQL against a zero-row typed stub of its inputs.
///
/// Reads nothing: no file is opened, no credential used, no network touched.
#[tauri::command]
fn describe_node_columns(
    pipeline: PipelineDoc,
    node_id: String,
    inputs: Vec<(String, Vec<duckle_duckdb_engine::Column>)>,
) -> Result<Vec<duckle_duckdb_engine::Column>, String> {
    engine()?
        .describe_node_columns(&pipeline, &node_id, &inputs)
        .map_err(|e| e.to_string())
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

/// An explainable trust scorecard for the open pipeline: compile status,
/// structural risks and ungoverned PII, each costed into a 0-100 score. Static
/// by default (no source reads), so it is fast and deterministic in the editor.
/// With `check_drift`, also reads each source's live schema and folds breaking
/// drift into the score; `${workspace}`/`${date}` are resolved first so drift
/// hits the real files.
#[tauri::command]
fn pipeline_trust_report(
    pipeline: serde_json::Value,
    check_drift: Option<bool>,
    workspace_path: Option<String>,
) -> Result<serde_json::Value, String> {
    if check_drift.unwrap_or(false) {
        if let Ok(mut doc) = serde_json::from_value::<PipelineDoc>(pipeline.clone()) {
            let engine = engine()?;
            duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
            if let Some(ws) = workspace_path.as_deref() {
                duckle_duckdb_engine::context::apply_workspace_context(&mut doc, std::path::Path::new(ws));
            }
            let resolved = serde_json::to_value(&doc).map_err(|e| e.to_string())?;
            return Ok(duckle_duckdb_engine::trust::trust_report(&resolved, Some(&engine)));
        }
    }
    Ok(duckle_duckdb_engine::trust::trust_report(&pipeline, None))
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
    // Propagated, not flattened to an empty list: a schedules.json that will
    // not parse must not be shown as "you have no schedules".
    scheduler()?.list()
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

// ---- Server setup: putting the runner where somebody can start it --------

/// Where the runner was put, and the command that starts it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StagedRunner {
    /// The file on disk.
    path: String,
    /// Which machine it will run on, so the UI can label it honestly.
    platform: String,
    /// The folder holding it, for "show me where that is".
    folder: String,
    /// A command that starts this exact file, ready to paste. Empty for a Linux runner,
    /// which is going to be uploaded and run somewhere else.
    command: String,
}

/// Put a copy of the headless runner somewhere the person setting up a server can reach it.
///
/// Nothing is downloaded. Both runners are compiled into this app already - the native one
/// so `duckle serve` works, and a static Linux one so Build Pipeline can target Linux - so
/// setup hands over a binary that is guaranteed to match this exact build, works with no
/// network, and cannot be a different thing than what was tested.
///
/// It goes to `<app_data>/server`, NOT a temp file. The first version of this used the
/// same temp stub the engine uses internally, which meant telling somebody to run their
/// server from `%TEMP%\duckle-stub-runner-14012375.exe` - a path that is cleaned up behind
/// them and whose name does not look like a thing you would trust.
///
/// `target` is "native" for a server on this machine, or "linux" for a cloud VM, which is
/// what every AWS, Azure and Google recipe lands on.
#[tauri::command]
fn runner_stage(
    app: tauri::AppHandle,
    target: String,
    workspace_path: Option<String>,
) -> Result<StagedRunner, String> {
    let dir = app
        .path()
        .app_data_dir()
        .map_err(|e| e.to_string())?
        .join("server");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {}", dir.display(), e))?;

    let (path, platform, runnable) = match target.as_str() {
        "linux" => {
            if EMBEDDED_RUNNER_LINUX.is_empty() {
                return Err(
                    "This build does not bundle a Linux runner. Use the container image instead."
                        .into(),
                );
            }
            let p = dir.join("duckle-runner-linux-x64");
            write_embedded_if_changed(&p, EMBEDDED_RUNNER_LINUX)?;
            (p, "Linux x64", false)
        }
        "native" => {
            if EMBEDDED_RUNNER.is_empty() {
                return Err("This build does not bundle the headless runner".into());
            }
            let suffix = if cfg!(windows) { ".exe" } else { "" };
            let p = dir.join(format!("duckle-runner{suffix}"));
            write_embedded_if_changed(&p, EMBEDDED_RUNNER)?;
            (p, if cfg!(windows) { "Windows" } else { "this machine" }, true)
        }
        other => return Err(format!("unknown runner target '{other}'")),
    };

    // The whole command, with real paths, because the point is that it can be pasted. An
    // instruction naming `duckle-runner` when the file is somewhere else entirely is not an
    // instruction, it is a riddle.
    //
    // Two lines - change directory, then run `.\name` - rather than one quoted absolute
    // path, because a quoted path at the start of a line means different things in the two
    // shells a Windows user might have open. PowerShell, which is what Windows 11 opens by
    // default, answers `Unexpected token 'serve'`. This form works in PowerShell, in
    // cmd.exe, and in a POSIX shell unchanged.
    let command = if runnable {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "duckle-runner".into());
        let here = if cfg!(windows) { ".\\" } else { "./" };
        let ws = workspace_path
            .filter(|w| !w.trim().is_empty())
            .map(|w| format!(" --workspace {}", shell_quote(&w)))
            .unwrap_or_default();
        format!(
            "cd {}\n{here}{name} serve{ws} --host 0.0.0.0 --port 8090",
            shell_quote(&dir.to_string_lossy()),
        )
    } else {
        String::new()
    };

    Ok(StagedRunner {
        path: path.to_string_lossy().into_owned(),
        platform: platform.to_string(),
        folder: dir.to_string_lossy().into_owned(),
        command,
    })
}

// ---- Plans --------------------------------------------------------------
//
// A plan is several pipelines in ordered steps, stored in `<workspace>/plans.json`.
// The workspace arrives as an argument rather than being read from the scheduler,
// matching `run_history` and the catalog commands: the frontend already knows which
// workspace it is showing, and a command that silently uses a different one is the
// kind of bug nobody sees until two workspaces are open.

#[tauri::command]
fn plans_list(workspace_path: String) -> Result<Vec<plans::Plan>, String> {
    // Propagated rather than flattened to an empty list, for the same reason
    // `schedule_list` propagates: a plans.json that will not parse must never be
    // shown as "you have no plans" while the plans are still sitting on disk.
    plans::load(std::path::Path::new(&workspace_path))
}

/// Add a plan or replace the one with the same id, and answer with the whole store.
///
/// Validated here rather than only in the UI, because this is also what an agent or a
/// second window reaches. Returning the full list means the caller redraws from what was
/// actually written instead of from what it hoped was written.
#[tauri::command]
fn plans_save(workspace_path: String, plan: plans::Plan) -> Result<Vec<plans::Plan>, String> {
    let problems = plan.problems();
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    plans::update(std::path::Path::new(&workspace_path), move |list| {
        match list.iter().position(|p| p.id == plan.id) {
            Some(i) => list[i] = plan,
            None => list.push(plan),
        }
    })
}

#[tauri::command]
fn plans_delete(workspace_path: String, id: String) -> Result<Vec<plans::Plan>, String> {
    plans::update(std::path::Path::new(&workspace_path), move |list| {
        list.retain(|p| p.id != id)
    })
}

/// Run a plan now and answer with what became of each pipeline in it.
///
/// The same path a scheduled plan takes: each pipeline takes its own run lock, lands in
/// its own run history and raises its own alerts, so a plan run and a scheduled run of the
/// same pipelines are indistinguishable afterwards except for the trigger recorded.
#[tauri::command]
async fn plans_run(workspace_path: String, id: String) -> Result<plans::PlanRun, String> {
    scheduler()?.run_plan_now(std::path::Path::new(&workspace_path), &id).await
}

// ---- Workspace catalog --------------------------------------------------

/// The whole catalog view for a workspace: graph, ownership, annotations,
/// freshness, and whether the saved graph is still current.
///
/// Reads the saved graph rather than rebuilding, so opening the screen never
/// silently costs a full workspace rescan; `stale` says when a rebuild is due
/// and `workspace_catalog_rebuild` is the deliberate act.
#[tauri::command]
fn workspace_catalog(
    workspace: String,
) -> Result<duckle_duckdb_engine::catalog::CatalogView, String> {
    duckle_duckdb_engine::catalog::view(std::path::Path::new(&workspace))
}

#[tauri::command]
fn workspace_catalog_rebuild(
    workspace: String,
) -> Result<duckle_duckdb_engine::catalog::CatalogView, String> {
    let ws = std::path::Path::new(&workspace);
    duckle_duckdb_engine::catalog::build_and_save(ws)?;
    duckle_duckdb_engine::catalog::view(ws)
}

/// Set the human metadata for one asset or pipeline, in owners.json.
///
/// Fields left null are left alone, so writing a description cannot clear an
/// owner somebody else authored.
#[allow(clippy::too_many_arguments)]
#[tauri::command]
fn workspace_catalog_annotate(
    workspace: String,
    pipelines: bool,
    name: String,
    owner: Option<String>,
    contact: Option<String>,
    description: Option<String>,
    tags: Option<Vec<String>>,
) -> Result<duckle_duckdb_engine::catalog::CatalogView, String> {
    let ws = std::path::Path::new(&workspace);
    duckle_duckdb_engine::catalog::annotate(
        ws,
        pipelines,
        &name,
        owner,
        contact,
        description,
        tags,
    )?;
    duckle_duckdb_engine::catalog::view(ws)
}

/// Read an asset's LIVE schema, on demand.
///
/// Deliberately not part of building the graph: this opens the source and
/// therefore needs credentials, a network and time, none of which should be
/// spent because somebody opened a catalog screen. It answers for one asset,
/// when asked, by finding a node that touches it and inspecting through that
/// node's own configuration - so it authenticates exactly the way the pipeline
/// does rather than inventing a second way to connect.
#[tauri::command]
fn workspace_catalog_inspect(workspace: String, asset: String) -> Result<Vec<String>, String> {
    use duckle_duckdb_engine::catalog;
    let ws = std::path::Path::new(&workspace);
    let cat = catalog::load(ws)?.ok_or("no catalog has been built for this workspace yet")?;
    let touch = cat
        .touches
        .iter()
        .find(|t| t.asset == asset && t.component_id.starts_with("src."))
        .ok_or_else(|| {
            format!("nothing in this workspace READS {asset}, so there is no node to inspect it through")
        })?;

    // Re-read the pipeline for that node's live properties: the catalog keeps
    // names, not configuration, and inspecting needs the connection details.
    let path = catalog::discover_pipeline_files(ws)
        .into_iter()
        .find(|p| p.file_stem().map(|s| s.to_string_lossy() == touch.pipeline_id.as_str()).unwrap_or(false))
        .ok_or_else(|| format!("pipeline {} is no longer in this workspace", touch.pipeline_id))?;
    let text = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let doc: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let node = doc
        .get("nodes")
        .and_then(|n| n.as_array())
        .and_then(|nodes| {
            nodes.iter().find(|n| n.get("id").and_then(|i| i.as_str()) == Some(&touch.node_id))
        })
        .ok_or_else(|| format!("node {} is no longer in {}", touch.node_id, touch.pipeline_id))?;
    let props = node.pointer("/data/properties").cloned().unwrap_or(serde_json::Value::Null);
    let format = touch.component_id.strip_prefix("src.").unwrap_or(&touch.component_id);

    let engine = engine()?;
    let inspection = engine.inspect(format, props).map_err(|e| e.to_string())?;
    Ok(inspection.schema.iter().map(|c| c.name.clone()).collect())
}

// ---- Engine install (first-run guided setup) ---------------------------

/// Which engines are present in the app-data dir, and whether each can
/// be installed on this platform.
#[tauri::command]
fn engine_status(app: tauri::AppHandle) -> Result<Vec<EngineStatus>, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    Ok(engine_manager::status(&dir))
}

/// The chat models offered at install time, smallest first, for the setup
/// picker. Curated and size-checked rather than a live Hugging Face search, so
/// the list can never offer a file that 404s partway through the download.
#[tauri::command]
fn llama_models() -> Vec<engine_manager::LlamaModel> {
    engine_manager::LLAMA_MODELS.to_vec()
}

/// The id installed by default when the user does not choose.
#[tauri::command]
fn llama_default_model() -> &'static str {
    engine_manager::DEFAULT_LLAMA_MODEL_ID
}

/// Download + install an engine (duckdb / slothdb / llamacpp) into
/// app-data, streaming progress.
///
/// `model_id` selects which GGUF the chat assistant downloads (see
/// `engine_manager::LLAMA_MODELS`). Other engines ignore it, and omitting it
/// installs the default model.
#[tauri::command]
async fn engine_install(
    app: tauri::AppHandle,
    engine: String,
    model_id: Option<String>,
    on_progress: Channel<InstallProgress>,
) -> Result<String, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    let result = tokio::task::spawn_blocking(move || {
        engine_manager::install(&dir, &engine, model_id.as_deref(), |p| {
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

/// Whether the Pixeltable Python is already provisioned (#223), so the UI can
/// say "will download ~1 GB on first run" instead of appearing to hang.
#[tauri::command]
async fn pixeltable_status(app: tauri::AppHandle) -> Result<bool, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    Ok(pixeltable_engine::is_installed(&dir))
}

/// Provision the Pixeltable Python on demand and return its path (#223).
///
/// Not done at startup: it is a large dependency tree for a connector most
/// workspaces never touch, so it is fetched when a Pixeltable node first needs
/// it. Idempotent - returns instantly once installed.
#[tauri::command]
async fn pixeltable_install(app: tauri::AppHandle) -> Result<String, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || pixeltable_engine::ensure(&dir))
        .await
        .map_err(|e| e.to_string())?
        .map(|p| p.to_string_lossy().into_owned())
}

/// Seed a brand-new / empty workspace with the bundled sample pipelines and
/// generate their input data locally (via the provisioned DuckDB). No-op if the
/// workspace already has a duckle.json. Returns true when it actually seeded, so
/// the frontend knows to re-hydrate from the new files.
#[tauri::command]
async fn seed_sample_workspace(app: tauri::AppHandle, workspace: String) -> Result<bool, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    // Pass the engine path through even if it is not present yet: seed() lays the
    // sample pipelines down regardless and only treats data generation (which
    // needs DuckDB) as best effort, so a not-yet-installed engine no longer
    // blocks seeding (which would leave the new workspace on the blank default).
    let duckdb = resolve_duckdb_bin(&dir);
    let ws = std::path::PathBuf::from(&workspace);
    tokio::task::spawn_blocking(move || samples::seed(&ws, &duckdb))
        .await
        .map_err(|e| e.to_string())?
}

// ---- Legacy job import -------------------------------------------------

/// What one job file turned into. The counts are reported alongside the
/// pipeline so the dialog can say what the file actually contained, not only
/// what survived translation.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobImport {
    /// `{ name, nodes, edges }` - the same shape the canvas and runner read.
    pipeline: JsonValue,
    /// Everything translation could not settle, already phrased for a human.
    warnings: Vec<String>,
    /// Source component name -> how many of them the file held.
    components: Vec<(String, usize)>,
    node_count: usize,
    /// How many of those nodes came through with a real Duckle component
    /// behind them. The remainder are placeholders on the canvas that still
    /// have to be replaced by hand, so the two numbers are reported side by
    /// side rather than letting a node count imply a working pipeline.
    translated: usize,
}

/// Read a legacy visual-ETL `.item` job file and translate it into a Duckle
/// pipeline.
///
/// Nothing is written to the workspace here. The translated pipeline is handed
/// back for the frontend to open as an unsaved tab, so a job that imports badly
/// costs the user nothing but a closed tab.
#[tauri::command]
fn import_job_file(path: String) -> Result<JobImport, String> {
    let path = PathBuf::from(path);
    let xml = std::fs::read_to_string(&path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    // The file stem is the job name, matching how the jobs are stored.
    let job_name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("imported_job");
    let import = duckle_duckdb_engine::talend::import_item(&xml, job_name)?;
    Ok(JobImport {
        node_count: import.nodes.len(),
        translated: import.nodes.iter().filter(|n| n.data.component_id.is_some()).count(),
        pipeline: import.to_pipeline_json(),
        warnings: import.warnings.iter().map(|w| w.to_string()).collect(),
        components: import.components.iter().map(|(k, v)| (k.clone(), *v)).collect(),
    })
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

/// Where this workspace can be deployed. Names and URLs only: the API key that
/// authenticates to each server never crosses into the front end, because a token that
/// reaches the browser layer is a token in a log, a crash report and a screenshot.
#[tauri::command]
fn deploy_targets(workspace_path: String) -> Result<Vec<deploy::TargetInfo>, String> {
    deploy::list_targets(&ws_path(&workspace_path))
}

/// Save a server and the API key minted for this machine by that server
/// (`duckle-runner console key-add <label> --role admin`). The key is encrypted with the
/// workspace key before it touches the disk, the same protection a saved connection gets.
#[tauri::command]
fn deploy_target_save(
    workspace_path: String,
    name: String,
    url: String,
    api_key: String,
) -> Result<(), String> {
    deploy::save_target(&ws_path(&workspace_path), &name, &url, &api_key)
}

/// What a server at this address is, before anything is saved: waiting to be set up, or
/// already set up and wanting a key.
#[tauri::command]
fn deploy_target_probe(url: String) -> Result<String, String> {
    deploy::probe(&url)
}

/// Finish setting up a server from here, and keep the key it hands back.
#[tauri::command]
fn deploy_target_claim(
    workspace_path: String,
    name: String,
    url: String,
    admin_label: String,
) -> Result<String, String> {
    deploy::claim(&ws_path(&workspace_path), &name, &url, &admin_label)
}

#[tauri::command]
fn deploy_target_remove(workspace_path: String, name: String) -> Result<bool, String> {
    deploy::remove_target(&ws_path(&workspace_path), &name)
}

/// Ask a target who we are. The only way to find out that the URL is right, the server is
/// up and the key still works, before trusting all three during a deploy.
#[tauri::command]
fn deploy_target_check(workspace_path: String, name: String) -> Result<serde_json::Value, String> {
    deploy::check_target(&ws_path(&workspace_path), &name)
}

/// Send a pipeline to a target. Any schedule travels with it and arrives switched off, so
/// a cadence set while testing here cannot start firing there the moment it lands.
#[tauri::command]
fn deploy_pipeline(
    workspace_path: String,
    target: String,
    name: String,
    pipeline: serde_json::Value,
    schedule: Option<serde_json::Value>,
) -> Result<serde_json::Value, String> {
    deploy::deploy(
        &ws_path(&workspace_path),
        &target,
        &name,
        &pipeline,
        schedule.as_ref(),
    )
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

/// Write the embedded LanceDB sidecar bytes to a temp stub + return its path, so
/// the engine can shell out to it for src.lancedb / snk.lancedb. Caller checks
/// EMBEDDED_LANCE is non-empty first.
fn staged_lance() -> Result<PathBuf, String> {
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    stage_stub_bytes(EMBEDDED_LANCE, suffix, "lance-")
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

/// Inflate a zstd-compressed embedded sidecar (runner / mcp / lance). An absent
/// sidecar is embedded as 0 bytes by build.rs, which stays empty here so the
/// `EMBEDDED_*.is_empty()` "not bundled" gates keep working without inflating.
fn inflate_embedded(compressed: &[u8]) -> Result<Vec<u8>, String> {
    if compressed.is_empty() {
        return Ok(Vec::new());
    }
    zstd::decode_all(compressed).map_err(|e| format!("decompress bundled binary: {}", e))
}

/// Like write_if_changed, but the source is a zstd-compressed embedded sidecar.
/// A tiny per-build stamp file lets an already-staged binary skip inflation
/// entirely (the common case once a feature has been used), so the ~0.1-0.3s
/// inflate is paid at most once per app version.
fn write_embedded_if_changed(dest: &std::path::Path, compressed: &[u8]) -> Result<(), String> {
    let stamp = dest.with_extension("stamp");
    let want = env!("DUCKLE_BUILD_EPOCH");
    if dest.exists() {
        if let Ok(have) = std::fs::read_to_string(&stamp) {
            if have.trim() == want {
                return Ok(());
            }
        }
    }
    let raw = inflate_embedded(compressed)?;
    write_if_changed(dest, &raw)?;
    let _ = std::fs::write(&stamp, want);
    Ok(())
}

/// Stage a zstd-compressed embedded sidecar to a temp file keyed by `tag` +
/// COMPRESSED length (unique per build), returning the path. An already-staged
/// (and AV-scanned) stub with that name is reused as-is, so inflation is paid at
/// most once per build. Writes to a unique sibling then renames into place so a
/// concurrent build never sees a half-written stub.
/// The per-user application-data base, derived without a Tauri handle.
///
/// `stage_stub_bytes` is a free function reached from paths that have no `app`,
/// so Tauri's own resolver is unavailable and the three platform rules are
/// applied here instead.
fn user_app_data_base() -> Option<PathBuf> {
    let home = || std::env::var_os("HOME").map(PathBuf::from);
    let base = if cfg!(windows) {
        std::env::var_os("APPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        home().map(|h| h.join("Library").join("Application Support"))
    } else {
        std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| home().map(|h| h.join(".local").join("share")))
    };
    base.map(|b| b.join("io.duckle.app"))
}

/// Where extracted sidecars are staged before being executed.
///
/// NOT the shared temp directory, which this used to be, for two reasons. On a
/// multi-user machine the old path was predictable (`duckle-stub-<tag><len>`) and
/// world-writable, and the caller returned it on existence alone, so another local
/// user could put their own executable there first and have Duckle run it. And on a
/// hardened Linux host `/tmp` is routinely mounted `noexec`, which made the exec
/// fail with a bare "Permission denied" and no explanation - on exactly the managed
/// fleets this product is aimed at.
///
/// Created 0700 at creation time on unix, so there is no window in which it is
/// group- or world-writable. Falls back to temp only when the environment does not
/// say where the user's data lives.
fn staging_dir() -> PathBuf {
    let dir = user_app_data_base()
        .unwrap_or_else(std::env::temp_dir)
        .join("staging");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let _ = std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir);
    }
    #[cfg(not(unix))]
    {
        let _ = std::fs::create_dir_all(&dir);
    }
    dir
}

fn stage_stub_bytes(bytes: &[u8], suffix: &str, tag: &str) -> Result<PathBuf, String> {
    let dir = staging_dir();
    let path = dir.join(format!("duckle-stub-{}{}{}", tag, bytes.len(), suffix));
    if path.exists() {
        return Ok(path);
    }
    let real = inflate_embedded(bytes)?;
    let tmp = dir.join(format!(
        "duckle-stub-{}{}-{}{}",
        tag,
        bytes.len(),
        std::process::id(),
        suffix
    ));
    std::fs::write(&tmp, &real).map_err(|e| format!("write stub: {}", e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("chmod stub: {}", e))?;
    }
    match std::fs::rename(&tmp, &path) {
        Ok(()) => Ok(path),
        // Windows rename fails if the destination exists; if another build
        // already staged this stub, use it.
        Err(_) if path.exists() => {
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
    write_embedded_if_changed(&mcp, EMBEDDED_MCP)?;
    let runner = dir.join(format!("duckle-runner{suffix}"));
    write_embedded_if_changed(&runner, EMBEDDED_RUNNER)?;
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

/// `duckle serve [...]`, `duckle run [...]` or `duckle validate [...]` -
/// delegate to the embedded headless runner without launching the GUI. Returns
/// false when argv[1] is none of those; on a headless path this never returns.
///
/// `validate` is here so a local pre-commit check can use the installed app.
/// It needs no DuckDB, no credentials and no network, and its exit codes
/// (0 ok / 1 finding / 2 usage) are stable. Note this only works where the GUI
/// libraries are present: on Linux the binary links WebKitGTK at load time, so
/// CI should use the standalone `duckle-runner` instead.
pub fn run_headless_cli() -> bool {
    let mut it = std::env::args();
    let _exe = it.next();
    let (label, temp_dir, runner_subcommand) = match it.next().as_deref() {
        Some("serve") => ("duckle serve", "duckle-serve", Some("serve")),
        Some("run") => ("duckle run", "duckle-run", None),
        Some("validate") => ("duckle validate", "duckle-validate", Some("validate")),
        _ => return false,
    };
    let rest: Vec<String> = it.collect();
    run_embedded_runner(label, temp_dir, runner_subcommand, &rest);
}

/// Stage the embedded runner to a temp dir and exec it with optional subcommand.
fn run_embedded_runner(
    label: &str,
    temp_dir: &str,
    subcommand: Option<&str>,
    rest: &[String],
) -> ! {
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
        eprintln!("{label}: this build does not bundle the runner");
        std::process::exit(1);
    }
    // Same reasoning as `staging_dir`: not shared temp, which is predictable on a
    // multi-user machine and frequently mounted noexec on a hardened Linux host.
    let dir = staging_dir().join(temp_dir);
    let _ = std::fs::create_dir_all(&dir);
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let runner = dir.join(format!("duckle-runner{suffix}"));
    if let Err(e) = write_embedded_if_changed(&runner, EMBEDDED_RUNNER) {
        eprintln!("{label}: {e}");
        std::process::exit(1);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755));
    }
    let mut cmd = std::process::Command::new(&runner);
    if let Some(sub) = subcommand {
        cmd.arg(sub);
    }
    let code = cmd
        .args(rest)
        .status()
        .map(|s| s.code().unwrap_or(0))
        .unwrap_or_else(|e| {
            eprintln!("{label}: failed to start: {e}");
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
    write_embedded_if_changed(&runner, EMBEDDED_RUNNER)?;
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
    // Reuse a still-running panel - but only if it is actually accepting
    // connections. A child that is alive yet not listening (still starting,
    // wedged, or its port taken) would otherwise hand the browser a dead URL
    // and surface as ERR_CONNECTION_REFUSED. Re-probe the port before trusting it.
    if let Some(p) = guard.as_mut() {
        let alive = matches!(p.child.try_wait(), Ok(None));
        let listening = std::net::TcpStream::connect(("127.0.0.1", p.port)).is_ok();
        if alive && listening {
            return Ok(format!("http://127.0.0.1:{}", p.port));
        }
        let _ = p.child.kill(); // exited or not listening; start a fresh one
        *guard = None;
    }

    let app_data = app.path().app_data_dir().map_err(|e| e.to_string())?;
    let duckdb = resolve_duckdb_bin(&app_data);
    let runner = stage_panel_runner(&app_data)?;
    let port = pick_panel_port();

    // Capture the runner's stderr to a log file so a failed `serve` start is
    // diagnosable instead of vanishing into Stdio::null (root-cause of silent
    // "did not start in time" reports).
    let log_path = app_data.join("engines").join("panel").join("serve.log");
    let stderr_sink = std::fs::File::create(&log_path)
        .map(std::process::Stdio::from)
        .unwrap_or_else(|_| std::process::Stdio::null());

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
        .stderr(stderr_sink);
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
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        let tail: String = log
            .trim()
            .chars()
            .rev()
            .take(400)
            .collect::<Vec<char>>()
            .into_iter()
            .rev()
            .collect();
        return Err(if tail.is_empty() {
            "web panel did not start in time".to_string()
        } else {
            format!("web panel did not start in time. serve log:\n{}", tail)
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Sidecars are extracted and then EXECUTED, so where they are staged is a
    /// security boundary. It used to be the shared temp directory under a name
    /// derived only from a tag and a length, and the caller returned that path on
    /// existence alone - so another local user could place their own executable
    /// there first. Shared temp is also mounted `noexec` on hardened Linux hosts,
    /// which turned the exec into a bare "Permission denied".
    #[test]
    fn sidecars_are_not_staged_in_shared_temp() {
        let dir = staging_dir();
        assert_ne!(
            dir,
            std::env::temp_dir(),
            "sidecars are still staged straight into shared temp"
        );
        assert!(
            dir.ends_with("staging"),
            "unexpected staging directory: {}",
            dir.display()
        );

        // On unix it must be private from the moment it exists, not chmod'ed after.
        #[cfg(unix)]
        if dir.exists() {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "staging dir is group/other accessible: {mode:o}");
        }
    }
    use std::ffi::OsString;
    use std::path::Path;

    // Issue #179: an externally-set DUCKLE_DUCKDB_BIN must win over the bundled
    // engine path, and an empty value must be ignored (fall back to bundled).
    #[test]
    fn external_duckdb_bin_wins_over_bundled_179() {
        let app_data = Path::new("/tmp/duckle-app-data");
        let bundled = engine_manager::duckdb_path(app_data);

        // A non-empty override is used verbatim.
        assert_eq!(
            pick_duckdb_bin(Some(OsString::from("/usr/local/bin/duckdb")), app_data),
            PathBuf::from("/usr/local/bin/duckdb")
        );
        // An empty override is ignored -> bundled path.
        assert_eq!(pick_duckdb_bin(Some(OsString::new()), app_data), bundled);
        // No override -> bundled path (unchanged default behavior).
        assert_eq!(pick_duckdb_bin(None, app_data), bundled);
    }

    /// The Plans editor's whole backend, driven the way the modal drives it.
    ///
    /// These commands are thin, which is the point: what is worth pinning is that they
    /// round-trip through the same `plans.json` the console and the scheduler read, in the
    /// same spelling, and that a plan which cannot work is refused where it was written.
    #[test]
    fn the_plans_commands_round_trip_through_the_shared_store() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_string_lossy().to_string();

        assert!(plans_list(ws.clone()).unwrap().is_empty(), "a fresh workspace has no plans");

        let plan = plans::Plan {
            id: "nightly".into(),
            name: "Nightly load".into(),
            stop_on_failure: true,
            steps: vec![
                plans::Step {
                    name: "Extract".into(),
                    // The console's spelling, because the editor writes it that way too.
                    pipelines: vec!["pipelines/orders.json".into()],
                    continue_on_failure: None,
                },
                plans::Step { name: "Publish".into(), pipelines: vec!["pipelines/export.json".into()], continue_on_failure: None },
            ],
        };
        let saved = plans_save(ws.clone(), plan.clone()).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].steps.len(), 2);

        // Saving the same id again replaces it rather than adding a second.
        let mut edited = plan.clone();
        edited.name = "Nightly load v2".into();
        let after = plans_save(ws.clone(), edited).unwrap();
        assert_eq!(after.len(), 1, "the same id must not produce two plans");
        assert_eq!(after[0].name, "Nightly load v2");

        // Refused at the point somebody wrote it, not at three in the morning.
        let broken = plans::Plan {
            id: "broken".into(),
            name: String::new(),
            stop_on_failure: true,
            steps: vec![plans::Step { name: "Empty".into(), pipelines: vec![], continue_on_failure: None }],
        };
        let err = plans_save(ws.clone(), broken).expect_err("an empty step is not a plan");
        assert!(err.contains("no pipelines"), "unhelpful refusal: {err}");
        assert_eq!(plans_list(ws.clone()).unwrap().len(), 1, "the refused plan was written anyway");

        // The store the scheduler and the console read is the one that changed.
        let shared = plans::load(std::path::Path::new(&ws)).unwrap();
        assert_eq!(shared[0].id, "nightly");

        assert!(plans_delete(ws.clone(), "nightly".into()).unwrap().is_empty());
        assert!(plans_list(ws).unwrap().is_empty());
    }
}
