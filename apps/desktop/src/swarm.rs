//! The agent swarm, run by the duckle-mcp server bundled with the app.
//!
//! `duckle-mcp swarm` does the work and prints one event per line; this relays
//! each line to the swarm view over a channel. One swarm runs at a time. Stop
//! asks it to finish at its next step, and ends the process if it has not
//! within a few seconds (a pipeline run in progress finishes first).

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use tauri::ipc::Channel;
use tauri::Manager;

struct Running {
    child: Arc<Mutex<Child>>,
    stdin: Option<ChildStdin>,
}

static RUNNING: Mutex<Option<Running>> = Mutex::new(None);

/// The bundled server, staged where the app keeps it. A development build
/// bundles none and uses the one cargo built beside the app.
fn server(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    if !crate::EMBEDDED_MCP.is_empty() {
        let app_data = app
            .path()
            .app_data_dir()
            .map_err(|e| format!("app data dir: {e}"))?;
        return crate::stage_mcp(&app_data).map(|(mcp, _)| mcp);
    }
    let name = if cfg!(windows) {
        "duckle-mcp.exe"
    } else {
        "duckle-mcp"
    };
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join(name)))
        .filter(|p| p.is_file())
        .ok_or_else(|| {
            "This build has no duckle-mcp server. Build it with `cargo build -p duckle-mcp`."
                .to_string()
        })
}

fn command(server: &Path) -> Command {
    let mut cmd = Command::new(server);
    cmd.arg("swarm");
    // A proxy set in Settings lives in this process; the server is another.
    if let Some(proxy) = duckle_duckdb_engine::tls::current_proxy() {
        cmd.env("HTTPS_PROXY", &proxy).env("HTTP_PROXY", &proxy);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd
}

/// Which ways of reaching a model this computer offers: the Anthropic sign-in,
/// and which API keys the environment holds.
#[tauri::command]
pub async fn swarm_status(app: tauri::AppHandle) -> Result<Value, String> {
    let server = server(&app)?;
    tauri::async_runtime::spawn_blocking(move || {
        let out = command(&server)
            .arg("--status")
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .map_err(|e| format!("starting the swarm server: {e}"))?;
        serde_json::from_slice(&out.stdout)
            .map_err(|e| format!("reading the swarm server's answer: {e}"))
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Run one swarm. Every event goes to `on_event`; the `finished` one is also
/// returned.
#[tauri::command]
pub async fn swarm_run(
    app: tauri::AppHandle,
    request: Value,
    on_event: Channel<Value>,
) -> Result<Value, String> {
    let server = server(&app)?;
    let mut request = request;
    // The engine this app manages, so the agents' runs match the editor's.
    if let Some(bin) = crate::DUCKDB_BIN.get() {
        request["duckdb"] = json!(bin.to_string_lossy());
    }
    tauri::async_runtime::spawn_blocking(move || run(&server, &request, &on_event))
        .await
        .map_err(|e| e.to_string())?
}

fn run(server: &Path, request: &Value, on_event: &Channel<Value>) -> Result<Value, String> {
    let child = {
        let mut slot = RUNNING.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err("A swarm is already running.".into());
        }
        let mut child = command(server)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("starting the swarm server: {e}"))?;
        let mut stdin = child.stdin.take().ok_or("the swarm server has no input")?;
        writeln!(stdin, "{request}")
            .and_then(|_| stdin.flush())
            .map_err(|e| format!("starting the swarm: {e}"))?;
        let child = Arc::new(Mutex::new(child));
        *slot = Some(Running {
            child: Arc::clone(&child),
            stdin: Some(stdin),
        });
        child
    };
    let stdout = child
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .stdout
        .take();
    let mut finished = None;
    for line in stdout.into_iter().flat_map(|s| BufReader::new(s).lines()) {
        let Ok(line) = line else { break };
        let Ok(event) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if event.get("type").and_then(Value::as_str) == Some("finished") {
            finished = Some(event.clone());
        }
        let _ = on_event.send(event);
    }
    let _ = child.lock().unwrap_or_else(|p| p.into_inner()).wait();
    *RUNNING.lock().unwrap_or_else(|p| p.into_inner()) = None;
    Ok(finished.unwrap_or_else(|| {
        let done = json!({ "type": "finished", "ok": false, "stopped": true, "summary": "The swarm ended without finishing." });
        let _ = on_event.send(done.clone());
        done
    }))
}

/// Stop the running swarm, if there is one.
#[tauri::command]
pub fn swarm_stop() {
    let mut slot = RUNNING.lock().unwrap_or_else(|p| p.into_inner());
    let Some(running) = slot.as_mut() else { return };
    if let Some(mut stdin) = running.stdin.take() {
        // Dropped after: a closed input means stop too.
        let _ = writeln!(stdin, "stop");
    }
    let child = Arc::clone(&running.child);
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(8));
        let mut child = child.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(child.try_wait(), Ok(None)) {
            let _ = child.kill();
        }
    });
}
