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
//! pipeline whose interval has elapsed. Reaching it means being able to run
//! any pipeline in the workspace, so it is open only on loopback and refuses
//! to start on any other host without a credential: see console_auth.

use duckle_duckdb_engine::{append_run_record, load_run_history, DuckdbEngine, PipelineDoc, RunRecord};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const PANEL_HTML: &str = include_str!("panel.html");
const SIGNIN_HTML: &str = include_str!("signin.html");
const SETUP_HTML: &str = include_str!("setup.html");

/// The page that claims an unclaimed console, and the request behind it.
const SETUP_PATH: &str = "/setup";
const SETUP_CLAIM_PATH: &str = "/api/setup/claim";

use crate::audit;
use crate::console_auth;

struct ServeArgs {
    host: String,
    port: u16,
    workspace: PathBuf,
    duckdb: Option<PathBuf>,
    tick_interval: Duration,
    /// Console credential. Prefer DUCKLE_CONSOLE_TOKEN: an argument is visible
    /// to anyone who can list processes on the host.
    token: Option<String>,
}

fn parse_serve_args() -> Result<ServeArgs, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8080;
    let mut workspace: Option<PathBuf> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut tick_secs: Option<u64> = None;
    let mut token: Option<String> = None;
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
            "--token" => token = Some(take("--token")?),
            "--tick-interval" => {
                tick_secs = Some(
                    take("--tick-interval")?
                        .parse()
                        .map_err(|_| "--tick-interval must be a number (seconds)".to_string())?,
                )
            }
            "-h" | "--help" => {
                println!(
                    "duckle-runner serve - web management console\n\n\
                     USAGE:\n    duckle-runner serve [--host <ip>] [--port <n>] [--workspace <dir>] [--duckdb <path>] [--tick-interval <secs>]\n\n\
                     OPTIONS:\n    \
                     --host <ip>            Bind address (default 127.0.0.1; use 0.0.0.0 for remote access)\n    \
                     --port <n>             Port (default 8080)\n    \
                     --workspace <dir>      Workspace root holding pipelines, runs/, logs/ (default: current dir)\n    \
                     --duckdb <path>        DuckDB CLI (default: DUCKLE_DUCKDB_BIN, sibling bin/duckdb, or PATH)\n    \
                     --tick-interval <secs> Scheduler poll cadence in seconds (default 15; also DUCKLE_TICK_INTERVAL)\n    \
                     --token <secret>       Shared sign-in token (also DUCKLE_CONSOLE_TOKEN)\n\n\
                     On 127.0.0.1 with no accounts the console is open, because reaching it\n\
                     means already being on the machine. On any other --host with no credential\n\
                     it starts UNCLAIMED: for 15 minutes anyone who can reach it can claim\n\
                     it and become its administrator. Supply --token, DUCKLE_CONSOLE_TOKEN,\n\
                     or `duckle-runner console add-user <name> --role ...` to skip that.\n\
                     Put it behind a reverse proxy if you need TLS."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown serve argument: {}", other)),
        }
    }
    let workspace = workspace.unwrap_or_else(|| PathBuf::from("."));
    // Poll cadence: --tick-interval flag > DUCKLE_TICK_INTERVAL env > 15s default.
    let tick_interval = Duration::from_secs(
        tick_secs
            .or_else(|| {
                std::env::var("DUCKLE_TICK_INTERVAL")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .filter(|n| *n > 0)
            .unwrap_or(15),
    );
    Ok(ServeArgs { host, port, workspace, duckdb, tick_interval, token })
}

/// Bounds how many pipelines execute at once.
///
/// Runs used to be serialized outright. The stated reason was that the
/// workspace env vars are shared, but those are set once at startup and do not
/// vary per run, so the real constraint is resources: each concurrent run gets
/// its own DUCKLE_MEMORY_LIMIT and its own DuckDB child, so N at once needs
/// roughly N times the memory and spawns N times the threads.
///
/// So the default stays 1, byte-for-byte the old behaviour, and a power user
/// with cores and memory to spare raises it. Independent DuckDB queries were
/// measured scaling about 3.8x across 8 concurrent processes on a 20-core box,
/// which is where the headroom is - not in splitting one query, which measured
/// slower.
struct RunGate {
    /// Permits currently free. Guarded by the mutex, waited on via the condvar.
    free: Mutex<usize>,
    /// How many there were to begin with, so `free` can be read as saturation
    /// rather than as a bare number (#300).
    total: usize,
    ready: Condvar,
}

impl RunGate {
    fn new(permits: usize) -> Self {
        let permits = permits.max(1);
        RunGate { free: Mutex::new(permits), total: permits, ready: Condvar::new() }
    }

    /// Permits free right now, and how many there are in total (#300).
    ///
    /// A gauge, read without blocking: an operator alerting on "runs are
    /// queueing" needs to see saturation while it is happening, and a metrics
    /// scrape that waited for a permit would be the one thing guaranteed to
    /// make it worse.
    fn permits(&self) -> (usize, usize) {
        let free = *self.free.lock().unwrap_or_else(|p| p.into_inner());
        (free, self.total)
    }

    /// Block until a permit is free, then hold it until the guard drops.
    fn acquire(&self) -> RunPermit<'_> {
        let mut free = self.free.lock().unwrap_or_else(|p| p.into_inner());
        while *free == 0 {
            free = self.ready.wait(free).unwrap_or_else(|p| p.into_inner());
        }
        *free -= 1;
        RunPermit { gate: self }
    }
}

struct RunPermit<'a> {
    gate: &'a RunGate,
}

impl Drop for RunPermit<'_> {
    fn drop(&mut self) {
        let mut free = self.gate.free.lock().unwrap_or_else(|p| p.into_inner());
        *free += 1;
        // One permit freed wakes one waiter.
        self.gate.ready.notify_one();
    }
}

/// How many pipelines may run concurrently. 1 (the default) serializes them.
fn max_concurrent_runs() -> usize {
    std::env::var("DUCKLE_MAX_CONCURRENT_RUNS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

// #289/#295: the gate lives in the engine's `pools` module now, so the console
// and a backfill admit runs through one implementation rather than two that
// drift.
use duckle_duckdb_engine::pools::Gates;

struct State {
    workspace: PathBuf,
    duckdb: PathBuf,
    /// Bounds concurrent pipeline execution. Defaults to one at a time; raise
    /// with DUCKLE_MAX_CONCURRENT_RUNS. See [`RunGate`].
    run_lock: Gates,
    /// Pipeline ids currently executing, so the console can show a live
    /// "Running" status (discussion #155). Populated for the duration of a run.
    running: Mutex<std::collections::HashSet<String>>,
    /// Who may call this console and what they may do. Decided once at
    /// startup, because a bind that cannot be authenticated must not serve at
    /// all rather than serve and warn.
    console: console_auth::Console,
    /// Bind host, for the cross-origin / DNS-rebind guard on state-changing
    /// routes. The web editor has had this since it shipped; the console did
    /// not, which left the default loopback console drivable by any page the
    /// operator happened to visit.
    host: String,
    /// Scheduler poll cadence (issue #135). Default 15s; overridable via
    /// --tick-interval or DUCKLE_TICK_INTERVAL.
    tick_interval: Duration,
    /// #310: OIDC login, when this deployment configured one. `None` leaves
    /// every route below exactly as it was.
    oidc: Option<crate::oidc::Config>,
    /// Discovered once and reused. Behind a lock rather than resolved at
    /// startup so a provider that is briefly down delays a login instead of
    /// stopping the server from starting.
    oidc_endpoints: Mutex<Option<crate::oidc::Endpoints>>,
    /// Logins between the redirect and the callback.
    oidc_logins: Mutex<crate::oidc::PendingLogins>,
    /// #259: runs accepted through POST /api/run/async, by run id. Holds the
    /// engine handle so the run can be cancelled, and the result once it is
    /// done. `running` above is pipeline ids only and cannot answer "how is
    /// run X going" - a different question.
    runs: Mutex<std::collections::HashMap<String, LiveRun>>,
}

/// #259: one asynchronous run. `finished` is None while it is queued or
/// executing, and carries the same summary POST /api/run returns once it ends.
struct LiveRun {
    pipeline_id: String,
    started_at: String,
    /// The handle the run executes on, kept so DELETE /api/run can cancel it.
    /// Cancellation is polled at every stage boundary and kills the active
    /// DuckDB child, so even a long query stops promptly.
    engine: DuckdbEngine,
    finished: Option<Value>,
}

/// How many finished runs to remember in memory. The durable answer is the run
/// history on disk, which every record now carries a run id in; this only keeps
/// the common case - polling straight after a run ends - off the filesystem.
const MAX_REMEMBERED_RUNS: usize = 200;

/// #259: mint an id for an accepted run. The counter distinguishes two runs of
/// the same pipeline inside one millisecond.
fn new_run_id(pipeline_id: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let safe: String = pipeline_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!(
        "run-{}-{}-{}",
        safe,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3f"),
        n
    )
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
    apply_workspace_memory_limit(&workspace);

    // #259: anything still marked `running` was not finished by a process that
    // is alive now, because this one is starting. Say so, rather than leaving a
    // receipt claiming a run is in progress forever. `interrupted` is
    // deliberately distinct from `error`: the run did not fail, it stopped
    // being observed, and a caller that conflates them retries work that may
    // well have completed.
    let reclaimed =
        duckle_duckdb_engine::retry::reconcile(&workspace, &|pid| pid == std::process::id());
    if !reclaimed.is_empty() {
        eprintln!(
            "duckle: {} run(s) were still marked running and are now interrupted: {}",
            reclaimed.len(),
            reclaimed.join(", ")
        );
    }
    // #295: and the same for a backfill's slices, which had the reconciler but
    // no caller. A slice left `running` by a killed process is not claimable
    // (only `requested` is) and `retry` only moves `failed` and `interrupted`,
    // so nothing could ever pick it up again and the backfill was stuck for
    // good. This is the one call that makes it recoverable.
    let slices =
        duckle_duckdb_engine::backfill::reconcile(&workspace, &|pid| pid == std::process::id());
    if !slices.is_empty() {
        eprintln!(
            "duckle: {} backfill(s) had slices still marked running and are now interrupted: {}",
            slices.len(),
            slices.join(", ")
        );
    }

    // Decide who may use this console before binding anything. An exposed bind
    // with no credential does not refuse to start any more - it comes up
    // unclaimed, so setup can be finished in a browser - but it then spends
    // fifteen minutes accepting an administrator claim from anyone who reaches
    // it. That is a deliberate trade, and it is only sound when "no credential"
    // means the operator chose not to supply one.
    //
    // An empty value is NOT that. `DUCKLE_CONSOLE_TOKEN=` is what an unresolved
    // secret reference looks like: the deployment believes it passed a
    // credential, the variable exists, and its value is the empty string. Folding
    // that into "no credential given" silently opened the claim window on a
    // server whose operator had every reason to think it was locked. It is a
    // misconfiguration, so it is refused rather than downgraded.
    let supplied = args.token.clone().or_else(|| std::env::var("DUCKLE_CONSOLE_TOKEN").ok());
    if supplied.as_deref().is_some_and(|t| t.trim().is_empty()) {
        return Err(
            "a console credential was supplied but is empty. Set DUCKLE_CONSOLE_TOKEN (or              --token) to a real value, or remove it entirely to set the server up from a browser. Refusing to start rather than opening an administrator claim window."
                .to_string(),
        );
    }
    let token = supplied;
    let console = console_auth::Console::configure(&workspace, &args.host, token.as_deref())?;
    let console_open = console.is_open();

    let state = Arc::new(State {
        workspace: workspace.clone(),
        duckdb: duckdb.clone(),
        run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::load(&workspace)),
        running: Mutex::new(std::collections::HashSet::new()),
        runs: Mutex::new(std::collections::HashMap::new()),
        console,
        host: args.host.clone(),
        tick_interval: args.tick_interval,
        // #310: absent leaves every route exactly as it was. A config that
        // exists and cannot be read stops the server rather than quietly
        // falling back to local accounts while an operator believes SSO is on.
        oidc: match crate::oidc::load(&workspace) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                std::process::exit(2);
            }
        },
        oidc_endpoints: Mutex::new(None),
        oidc_logins: Mutex::new(Default::default()),
    });

    // Fold any pre-unification console store into schedules.json before the
    // scheduler reads it, so an existing install keeps firing across the change.
    migrate_legacy_schedules(&workspace);

    spawn_scheduler(state.clone());

    let addr = format!("{}:{}", args.host, args.port);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    runtime.block_on(async move {
        // Bound once, by the runtime that will serve it. Binding with std first to keep
        // the error message and then dropping it leaves a window where the port can be
        // taken between the two, and reports a confusing failure when it is.
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("bind {}: {}", addr, e))?;
        eprintln!("duckle-runner: management console on http://{}", addr);
        eprintln!("duckle-runner: workspace {}", workspace.display());
        eprintln!("duckle-runner: DuckDB {}", duckdb.display());
        if console_open {
            eprintln!("duckle-runner: no token set; reachable only from this machine");
        } else {
            eprintln!("duckle-runner: sign-in required");
        }
        axum::serve(listener, console_router(state).into_make_service())
            .await
            .map_err(|e| format!("serve: {e}"))
    })
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
    /// Editor credential. Prefer DUCKLE_CONSOLE_TOKEN over an argument, which
    /// anyone who can list processes on the host can read.
    token: Option<String>,
}

fn parse_web_args() -> Result<WebArgs, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8090;
    let mut workspace: Option<PathBuf> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut dist: Option<PathBuf> = None;
    let mut token: Option<String> = None;
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
            "--token" => token = Some(take("--token")?),
            "-h" | "--help" => {
                println!(
                    "duckle-runner web - serve the Duckle editor as a web app (spike)\n\n\
                     USAGE:\n    duckle-runner web --dist <dir> [--host <ip>] [--port <n>] [--workspace <dir>] [--token <secret>]\n\n\
                     Same accounts and roles as `duckle-runner serve`: open on 127.0.0.1\n\
                     with no accounts, and UNCLAIMED on any other --host without a credential,\n\
                     claimable by anyone who reaches it for 15 minutes. Supply --token,\n\
                     DUCKLE_CONSOLE_TOKEN, or `console add-user` to skip that window)."
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
        token,
    })
}

struct WebState {
    workspace: PathBuf,
    duckdb: PathBuf,
    dist: PathBuf,
    /// Bind host, for the cross-origin / DNS-rebind guard on POST routes.
    host: String,
    /// Bounds concurrent runs from the browser. One at a time by default; raise
    /// with DUCKLE_MAX_CONCURRENT_RUNS. See [`RunGate`].
    run_lock: Gates,
    /// Who may use this editor. Same policy object the console uses, so one
    /// set of accounts covers both.
    console: console_auth::Console,
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
    apply_workspace_memory_limit(&workspace);

    // #259: anything still marked `running` was not finished by a process that
    // is alive now, because this one is starting. Say so, rather than leaving a
    // receipt claiming a run is in progress forever. `interrupted` is
    // deliberately distinct from `error`: the run did not fail, it stopped
    // being observed, and a caller that conflates them retries work that may
    // well have completed.
    let reclaimed =
        duckle_duckdb_engine::retry::reconcile(&workspace, &|pid| pid == std::process::id());
    if !reclaimed.is_empty() {
        eprintln!(
            "duckle: {} run(s) were still marked running and are now interrupted: {}",
            reclaimed.len(),
            reclaimed.join(", ")
        );
    }
    // #295: and the same for a backfill's slices, which had the reconciler but
    // no caller. A slice left `running` by a killed process is not claimable
    // (only `requested` is) and `retry` only moves `failed` and `interrupted`,
    // so nothing could ever pick it up again and the backfill was stuck for
    // good. This is the one call that makes it recoverable.
    let slices =
        duckle_duckdb_engine::backfill::reconcile(&workspace, &|pid| pid == std::process::id());
    if !slices.is_empty() {
        eprintln!(
            "duckle: {} backfill(s) had slices still marked running and are now interrupted: {}",
            slices.len(),
            slices.join(", ")
        );
    }
    // The editor writes files, edits connections and runs pipelines, so it is
    // at least as powerful as the console and gets the same rule: loopback is
    // open, anything else needs a credential before the socket is bound.
    let token = args
        .token
        .clone()
        .or_else(|| std::env::var("DUCKLE_CONSOLE_TOKEN").ok())
        .filter(|t| !t.trim().is_empty());
    let console = console_auth::Console::configure(&workspace, &args.host, token.as_deref())?;
    let console_open = console.is_open();

    let state = Arc::new(WebState {
        workspace: workspace.clone(),
        duckdb: duckdb.clone(),
        dist: dist.clone(),
        host: args.host.clone(),
        run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::load(&workspace)),
        console,
    });
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).map_err(|e| format!("bind {}: {}", addr, e))?;
    eprintln!("duckle-runner: web editor on http://{}", addr);
    eprintln!("duckle-runner: workspace {}", workspace.display());
    eprintln!("duckle-runner: serving {}", dist.display());
    if console_open {
        eprintln!("duckle-runner: no token set; reachable only from this machine");
    } else {
        eprintln!("duckle-runner: sign-in required");
    }
    // A store that will not open is not a store with nothing in it. Saying nothing here
    // would be the exact failure this notice exists to prevent, so an unreadable one is
    // reported rather than counted as zero.
    match duckle_duckdb_engine::schedules::load(&workspace) {
        Ok(schedules) => {
            eprintln!("{}", scheduler_notice(schedules.iter().filter(|s| s.enabled).count()))
        }
        Err(e) => eprintln!(
            "duckle-runner: WARNING: the schedule store could not be read ({e}). \
             Whatever is in it will not run here either; the editor does not run schedules."
        ),
    }
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

/// Exchange a token for a session cookie, for the editor.
fn web_sign_in(state: &WebState, req: &Request) -> Reply {
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
    let token = body.get("token").and_then(|v| v.as_str()).unwrap_or("");
    match state.console.sign_in(token) {
        Some((sid, who)) => {
            audit::record(&state.workspace, Some(&who), "session.sign_in", "editor", audit::Outcome::Allowed);
            respond_json(&json!({ "label": who.label, "role": who.role.as_str() }))
                .with_header(session_cookie_header(&sid, req.forwarded_proto.as_deref()))
        }
        None => {
            audit::record(&state.workspace, None, "session.sign_in", "editor", audit::Outcome::Unauthenticated);
            respond_err("401 Unauthorized", "that token was not accepted")
        }
    }
}

/// The editor's gate: cross-origin, then identity, then role.
///
/// Extracted so the streaming route is held to the SAME checks as every other
/// route. It used to be reachable without any of them: `handle_web` matched
/// `/api/run_stream` and returned BEFORE calling `route_web`, which is where the
/// cross-origin guard, the sign-in check and the role check all live. Since
/// `run_stream` executes a pipeline taken from the request body, and resolves
/// this workspace's saved connections into it, that route accepted an
/// unauthenticated request and ran it with the workspace's credentials.
///
/// The reason for the early return was real - Server-Sent Events keep the socket
/// open, so there is no finished `Reply` to hand back - but the answer is to run
/// the checks first, not to skip them.
fn web_gate(
    req: &Request,
    state: &WebState,
    needed: console_auth::Role,
    action: &str,
) -> Result<console_auth::Identity, Reply> {
    if req.method == "POST" && req.path.starts_with("/api/") && !guard_local(req, &state.host) {
        return Err(respond_403("blocked: cross-origin or non-local request"));
    }
    let Some(who) = state.console.identify(req.authorization.as_deref(), req.cookie.as_deref())
    else {
        audit::record(&state.workspace, None, action, &req.path, audit::Outcome::Unauthenticated);
        return Err(respond_err("401 Unauthorized", "sign in to use the editor"));
    };
    if !who.role.allows(needed) {
        audit::record(&state.workspace, Some(&who), action, &req.path, audit::Outcome::Denied);
        return Err(respond_403(&format!(
            "this needs the {} role; you have {}",
            needed.as_str(),
            who.role.as_str()
        )));
    }
    Ok(who)
}

fn handle_web(mut stream: TcpStream, state: &WebState) -> Result<(), String> {
    let req = read_request(&mut stream)?;
    // Server-Sent Events keep the socket: there is no finished response to hand
    // back, so this route is dispatched here rather than through `route_web`.
    // It is gated FIRST, with the same checks `route_web` applies - running a
    // caller-supplied pipeline needs at least the operator role.
    if req.method == "POST" && req.path == "/api/run_stream" {
        let who = match web_gate(&req, state, console_auth::Role::Operator, "editor.api") {
            Ok(w) => w,
            Err(reply) => return write_reply(&mut stream, &reply),
        };
        audit::record(
            &state.workspace,
            Some(&who),
            "editor.api",
            &req.path,
            audit::Outcome::Allowed,
        );
        let body = req.body.clone();
        return run_stream(&mut stream, state, &body);
    }
    let reply = route_web(&req, state);
    write_reply(&mut stream, &reply)
}

/// Decide what the editor answers, without touching a socket.
fn route_web(req: &Request, state: &WebState) -> Reply {
    // Block cross-origin / non-local state-changing POSTs (CSRF + DNS-rebind).
    if req.method == "POST" && req.path.starts_with("/api/") && !guard_local(&req, &state.host) {
        return respond_403("blocked: cross-origin or non-local request");
    }
    if is_public_route(&req.method, &req.path) {
        if let Some(reply) = probe_reply(&req.path, &state.workspace) {
            return reply;
        }
        return web_sign_in(state, &req);
    }
    // Parse the route ONCE, here, and let both the gate and the dispatcher use
    // the result. They used to parse the path separately - the gate with
    // `starts_with("/api/cmd/connection")` and the dispatcher with
    // `trim_start_matches("/api/cmd/")` - and `trim_start_matches` strips its
    // prefix REPEATEDLY. So `/api/cmd//api/cmd/connection_decrypt_payload` was
    // not "a connection command" to the gate, which asked only for operator,
    // and was exactly `connection_decrypt_payload` to the dispatcher, which
    // decrypted the workspace's stored credentials. Two parsers over one string
    // is the bug; one parser is the fix.
    let cmd = req.path.strip_prefix("/api/cmd/");
    let fs_op = req.path.strip_prefix("/api/fs/");

    // The editor has no read-only mode: opening it means loading a workspace to
    // change it, so the whole surface needs operator. Anything touching
    // connections, which is to say credentials, needs admin.
    let needed = match cmd {
        Some(c) if c.starts_with("connection") => console_auth::Role::Admin,
        _ => console_auth::Role::Operator,
    };
    let action = if req.path.starts_with("/api/") { "editor.api" } else { "editor.open" };
    let who = state.console.identify(req.authorization.as_deref(), req.cookie.as_deref());
    let Some(who) = who else {
        audit::record(&state.workspace, None, action, &req.path, audit::Outcome::Unauthenticated);
        if req.method == "GET" && !req.path.starts_with("/api/") {
            return respond("401 Unauthorized", "text/html; charset=utf-8", SIGNIN_HTML.as_bytes());
        }
        return respond_err("401 Unauthorized", "sign in to use the editor");
    };
    if !who.role.allows(needed) {
        audit::record(&state.workspace, Some(&who), action, &req.path, audit::Outcome::Denied);
        return respond_403(&format!("this needs the {} role; you have {}", needed.as_str(), who.role.as_str()),
        );
    }
    if req.method == "POST" {
        audit::record(&state.workspace, Some(&who), action, &req.path, audit::Outcome::Allowed);
    }
    if req.method == "POST" {
        if let Some(cmd) = cmd {
            let cmd = cmd.to_string();
            // A panic inside a command (e.g. a source that misbehaves during a
            // live drift read) would otherwise unwind this connection's thread
            // and drop the socket, which the browser can only report as an
            // opaque "Failed to fetch". Catch it and answer with a real 500 the
            // editor can show.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                dispatch_cmd(state, &cmd, &req.body)
            }));
            return match outcome {
                Ok(r) => r,
                Err(_) => respond_err("500 Internal Server Error",
                    &format!("command '{cmd}' failed unexpectedly"),
                ),
            };
        }
        if let Some(op) = fs_op {
            return dispatch_fs(state, &op.to_string(), &req.body);
        }
    }
    if req.method == "POST" && req.path == "/api/inspect" {
        return inspect_schema(state, &req.body);
    }
    // Static frontend: map the URL path into the dist dir; unknown non-asset
    // paths fall back to index.html (SPA routing).
    serve_static(state, &req.path)
}

/// Server-side filesystem bridge for the web editor. The browser cannot touch
/// the server's disk, so the frontend's workspace file ops (read/write/list)
/// route here. Every path is confined to the workspace dir (no traversal out).
fn dispatch_fs(state: &WebState, op: &str, body: &[u8]) -> Reply {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let path_arg = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let target = match confine_to_workspace(&state.workspace, path_arg) {
        Ok(p) => p,
        Err(e) => return respond_err("400 Bad Request", &e),
    };
    match op {
        "exists" => respond_json(&serde_json::json!({ "exists": target.exists() })),
        "read" => match std::fs::read_to_string(&target) {
            Ok(content) => respond_json(&serde_json::json!({ "content": content })),
            Err(e) => respond_err("404 Not Found", &e.to_string()),
        },
        "write" => {
            let content = args.get("content").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(parent) = target.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&target, content) {
                Ok(()) => respond_json(&serde_json::json!({ "ok": true })),
                Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
            }
        }
        "mkdir" => match std::fs::create_dir_all(&target) {
            Ok(()) => respond_json(&serde_json::json!({ "ok": true })),
            Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
        },
        "remove" => {
            let r = if target.is_dir() { std::fs::remove_dir_all(&target) } else { std::fs::remove_file(&target) };
            match r {
                Ok(()) => respond_json(&serde_json::json!({ "ok": true })),
                Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
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
            respond_json(&Value::Array(entries))
        }
        _ => respond_err("404 Not Found", &format!("unknown fs op: {}", op)),
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
    // A plain prefix test lets a SIBLING through: with a workspace of `/srv/duckle`,
    // the string `/srv/duckle-backup/x` starts with it. Require the boundary to fall
    // on a separator, or the paths to be equal.
    let (n, w) = (norm(&normalized), norm(workspace));
    if !(n == w || n.starts_with(&format!("{w}/"))) {
        return Err("path escapes the workspace".into());
    }
    // The file bridge is reachable at operator level, while the commands that touch
    // credentials require admin. Without this, an operator reads the very things that
    // gate protects by asking for them as files: the AES key that decrypts every
    // stored secret, and the cached git token. Nothing legitimate fetches these over
    // HTTP, so they are refused for every role rather than raised to admin.
    let rel = n.strip_prefix(&w).unwrap_or("").trim_start_matches('/');
    if rel == ".duckle/keys"
        || rel.starts_with(".duckle/keys/")
        || rel == ".duckle/secrets"
        || rel.starts_with(".duckle/secrets/")
    {
        return Err("that path holds key material and is not reachable through the file API".into());
    }
    Ok(normalized)
}

/// The body of the two connection-secret commands, split out from the socket
/// so a test can drive the real path instead of re-implementing it. Encrypting
/// is strict: a failure must surface, never fall through to writing plaintext.
fn connection_secret_cmd(workspace: &Path, cmd: &str, body: &[u8]) -> Result<String, String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let payload = args.get("payloadJson").and_then(|v| v.as_str()).unwrap_or("null");
    // The id binds each ciphertext to the connection it belongs to, so a blob
    // cannot be transplanted into another connection and decrypted there.
    let connection_id = args
        .get("connectionId")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if cmd == "connection_encrypt_payload" {
        duckle_secrets::encrypt_payload_json(workspace, connection_id, payload)
    } else {
        duckle_secrets::decrypt_payload_json(workspace, connection_id, payload)
    }
}

fn dispatch_cmd(state: &WebState, cmd: &str, body: &[u8]) -> Reply {
    match cmd {
        // Drives the editor's runtime indicator offline -> ready.
        "ping" => respond_json(&Value::String("pong".into())),
        // Connection secrets, encrypted at rest with the same AES-256-GCM
        // primitives and the same per-workspace key the desktop app uses, so a
        // workspace stays readable whichever edition wrote it.
        //
        // These two used to echo the payload back unchanged, which meant the
        // self-hosted web edition wrote passwords to connections/*.json in
        // clear text while the desktop encrypted them - the same product
        // quietly downgrading its own security depending on how it was
        // launched. Encrypt is strict, because failing to encrypt must never
        // fall through to writing plaintext. Decrypt is lenient by design:
        // when there is no key yet, or a field is still plain, the payload is
        // returned as-is so connections saved before this change keep opening.
        "connection_encrypt_payload" | "connection_decrypt_payload" => {
            match connection_secret_cmd(&state.workspace, cmd, body) {
                Ok(out) => respond_json(&Value::String(out)),
                Err(e) => respond_err("500 Internal Server Error",
                    &format!("connection secrets: {e}"),
                ),
            }
        }
        // Execute a pipeline on the server engine and return the RunResult (the
        // same shape the desktop returns). The frontend reads the final result
        // from this response; live per-stage events (the Channel) are not
        // streamed in the MVP. Concurrency is bounded by run_lock (1 by default).
        "run_pipeline" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err("400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            // Saved Salesforce connection refs resolve server-side against this
            // workspace (#166 stage 2) - the browser never sees the secret.
            if let Err(e) = duckle_secrets::resolve_connection_refs(&state.workspace, &mut doc.nodes) {
                return respond_err("400 Bad Request", &e);
            }
            // Same placeholder resolution as /api/run (execute_one) and the
            // desktop: expand ${ENV:KEY} secrets - so a connection field stored as
            // ${ENV:...} still resolves after ref injection (#166 stage 2) - and the
            // ${date}/${datetime} builtins, before the workspace-context pass.
            let env_file = state.workspace.join("secrets.env");
            if let Err(e) = crate::apply_env_pass(&mut doc, &state.workspace, &env_file) {
                return respond_err("400 Bad Request", &e);
            }
            duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            let name = args.get("pipelineName").and_then(|v| v.as_str()).unwrap_or("web").to_string();
            let (_guard, pool, queued_ms) = state.run_lock.acquire(&doc.resource_pool);
            let engine = DuckdbEngine::new(state.duckdb.clone());
            let receipt =
                begin_editor_run(&state.workspace, &doc, &name, "web", Some((pool, queued_ms)));
            let result = engine.execute_pipeline_named(&doc, &name);
            duckle_duckdb_engine::retry::finish(
                &state.workspace,
                receipt,
                &result.status,
                duckle_duckdb_engine::retry::nodes_of(&result),
            );
            match serde_json::to_value(&result) {
                Ok(v) => respond_json(&v),
                Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
            }
        }
        // Plans: several pipelines in ordered steps. Authoring only - these are the store
        // operations, mirroring the desktop's plans_* commands so the same editor works in
        // both. RUNNING a plan is not here: that needs the run lock, per-pipeline run
        // history and alerting the console's `/api/plans/run` already does, and a second
        // implementation of it is exactly how the two schedulers came to disagree about
        // what a schedule meant. The editor says so rather than running it differently.
        //
        // The workspace the desktop sends is ignored: this process knows the only workspace
        // it serves, and a browser must not be able to name a directory on the server.
        // Backfill, for the shared editor's Backfill panel. Without these the
        // panel is dead in the web edition while working on the desktop - the
        // parity gap #75 exists to prevent. The argument names match the Tauri
        // commands, because the same React code calls both.
        //
        // The workspace is ALWAYS this server's, never the one in the payload:
        // a browser must not be able to point a state edit at another folder.
        "watermark_list" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            match args.get("pipelineName").and_then(|v| v.as_str()) {
                Some(name) => {
                    let entries = duckle_duckdb_engine::watermark::list(&state.workspace, name);
                    respond_json(&serde_json::to_value(&entries).unwrap_or(json!([])))
                }
                None => respond_err("400 Bad Request", "missing pipelineName"),
            }
        }
        "watermark_set" => {
            use duckle_duckdb_engine::watermark as wm;
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let name = args.get("pipelineName").and_then(|v| v.as_str());
            let node = args.get("nodeId").and_then(|v| v.as_str());
            let (name, node) = match (name, node) {
                (Some(n), Some(d)) => (n, d),
                _ => return respond_err("400 Bad Request", "missing pipelineName or nodeId"),
            };
            // Same engine guard as every other surface: a write that would
            // replace a different kind of state is refused, not applied.
            let done = if args.get("kind").and_then(|v| v.as_str()) == Some("snapshot") {
                match args.get("value").and_then(|v| v.as_str()).and_then(|v| v.parse::<u64>().ok()) {
                    Some(id) => wm::set_snapshot(&state.workspace, name, node, id),
                    None => return respond_err("400 Bad Request", "snapshot value must be a number"),
                }
            } else {
                match args.get("value").and_then(|v| v.as_str()) {
                    Some(v) => wm::set_incremental(
                        &state.workspace,
                        name,
                        node,
                        v,
                        args.get("valueType").and_then(|t| t.as_str()),
                    ),
                    None => return respond_err("400 Bad Request", "missing value"),
                }
            };
            match done {
                Ok(()) => respond_json(&json!({ "ok": true })),
                Err(e) => respond_err("400 Bad Request", &e.to_string()),
            }
        }
        "watermark_clear" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            match (
                args.get("pipelineName").and_then(|v| v.as_str()),
                args.get("nodeId").and_then(|v| v.as_str()),
            ) {
                (Some(name), Some(node)) => {
                    match duckle_duckdb_engine::watermark::clear(&state.workspace, name, node) {
                        Ok(()) => respond_json(&json!({ "ok": true })),
                        Err(e) => respond_err("400 Bad Request", &e.to_string()),
                    }
                }
                _ => respond_err("400 Bad Request", "missing pipelineName or nodeId"),
            }
        }
        "plans_list" => match duckle_duckdb_engine::plans::load(&state.workspace) {
            Ok(list) => respond_json(&serde_json::to_value(&list).unwrap_or(json!([]))),
            Err(e) => respond_err("500 Internal Server Error", &e),
        },
        "plans_save" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let plan: duckle_duckdb_engine::plans::Plan =
                match serde_json::from_value(args.get("plan").cloned().unwrap_or(Value::Null)) {
                    Ok(p) => p,
                    Err(e) => return respond_err("400 Bad Request", &format!("that is not a plan: {e}")),
                };
            let problems = plan.problems();
            if !problems.is_empty() {
                return respond_err("400 Bad Request", &problems.join("; "));
            }
            match duckle_duckdb_engine::plans::update(&state.workspace, move |list| {
                list.retain(|p| p.id != plan.id);
                list.push(plan);
                list.sort_by(|a, b| a.id.cmp(&b.id));
            }) {
                Ok(list) => respond_json(&serde_json::to_value(&list).unwrap_or(json!([]))),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        "plans_delete" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let id = args.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            match duckle_duckdb_engine::plans::update(&state.workspace, move |list| {
                list.retain(|p| p.id != id)
            }) {
                Ok(list) => respond_json(&serde_json::to_value(&list).unwrap_or(json!([]))),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        // Compile to per-stage SQL for the Plan tab.
        "compile_pipeline" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err("400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            match duckle_duckdb_engine::compile_pipeline_sql(&doc) {
                Ok(stages) => match serde_json::to_value(&stages) {
                    Ok(v) => respond_json(&v),
                    Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
                },
                Err(e) => respond_err("400 Bad Request", &e.to_string()),
            }
        }
        // #314. The web editor had none of the binder primitive at all: an
        // unknown command 404s and the web shim turns a 404 into a null no-op,
        // so the whole feature was silently dead here while the desktop had it.
        // `apply_workspace_context` first, like every other command on this
        // path and unlike the desktop one - without it a `${workspace}` in SQL
        // binds differently on the two surfaces.
        "analyze_node_sql" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err("400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            let engine = DuckdbEngine::new(state.duckdb.clone());
            // One node, with the upstream columns the EDITOR resolved - the
            // same call and the same arguments the desktop command makes.
            // Deriving them here instead would answer a subtly different
            // question on the two surfaces, which is the divergence #75 exists
            // to stop.
            let node_id = args
                .get("nodeId")
                .or_else(|| args.get("node"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let inputs: Vec<(String, Vec<duckle_duckdb_engine::Column>)> =
                serde_json::from_value(args.get("inputs").cloned().unwrap_or(json!([])))
                    .unwrap_or_default();
            match engine.analyze_node_sql(&doc, &node_id, &inputs) {
                Ok(analysis) => match serde_json::to_value(&analysis) {
                    Ok(v) => respond_json(&v),
                    Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
                },
                Err(e) => respond_err("400 Bad Request", &e.to_string()),
            }
        }
        // #307: the same list the desktop gets, so an external component appears
        // in the web editor's palette too. Read from manifests; nothing runs.
        "external_components" => {
            let (found, problems) = duckle_duckdb_engine::plugin::discover(&state.workspace);
            respond_json(&json!({
                "components": found
                    .iter()
                    .map(duckle_duckdb_engine::plugin::as_catalog_entry)
                    .collect::<Vec<_>>(),
                "problems": problems,
            }))
        }
        // #314: the same completion the desktop gets, so the web editor is not
        // a lesser place to write SQL.
        "complete_node_sql" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err("400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            let node_id = args.get("nodeId").and_then(Value::as_str).unwrap_or_default().to_string();
            let inputs: Vec<(String, Vec<duckle_duckdb_engine::Column>)> =
                serde_json::from_value(args.get("inputs").cloned().unwrap_or(json!([])))
                    .unwrap_or_default();
            let cursor = args.get("cursor").and_then(Value::as_u64).unwrap_or(0) as usize;
            let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(12) as usize;
            let engine = DuckdbEngine::new(state.duckdb.clone());
            match engine.complete_node_sql(&doc, &node_id, &inputs, cursor, limit) {
                Ok(items) => respond_json(&serde_json::to_value(&items).unwrap_or_default()),
                Err(e) => respond_err("400 Bad Request", &e.to_string()),
            }
        }
        "pipeline_column_lineage" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err("400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            let engine = DuckdbEngine::new(state.duckdb.clone());
            match engine.pipeline_column_lineage(&doc) {
                Ok(result) => match serde_json::to_value(&result) {
                    Ok(v) => respond_json(&v),
                    Err(e) => respond_err("500 Internal Server Error", &e.to_string()),
                },
                Err(e) => respond_err("400 Bad Request", &e.to_string()),
            }
        }
        // Trust scorecard for the open pipeline (compile + structural risks +
        // ungoverned PII). Static by default; with checkDrift it also reads each
        // source's live schema (resolving ${workspace} against this server's
        // workspace first). Matches the desktop command and the MCP tool.
        "pipeline_trust_report" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let pipeline = args.get("pipeline").cloned().unwrap_or(Value::Null);
            let check_drift = args.get("checkDrift").and_then(|v| v.as_bool()).unwrap_or(false);
            if check_drift {
                if let Ok(mut doc) = serde_json::from_value::<PipelineDoc>(pipeline.clone()) {
                    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
                    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
                    let resolved = match serde_json::to_value(&doc) {
                        Ok(v) => v,
                        Err(e) => return respond_err("500 Internal Server Error", &e.to_string()),
                    };
                    let engine = DuckdbEngine::new(state.duckdb.clone());
                    let report = duckle_duckdb_engine::trust::trust_report(&resolved, Some(&engine));
                    return respond_json(&report);
                }
            }
            let report = duckle_duckdb_engine::trust::trust_report(&pipeline, None);
            respond_json(&report)
        }
        // Tells the browser editor which server workspace it is editing, so it
        // can auto-load it (there is no native folder picker on the web).
        "web_bootstrap" => respond_json(&serde_json::json!({ "workspace": state.workspace.to_string_lossy() }),
        ),
        // The browser build skips the engine-setup gate, but answer truthfully.
        "engine_status" => respond_json(&serde_json::json!([{
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
        // Genuinely unknown commands get a real 404 (correct HTTP semantics for
        // typos and for non-browser callers like curl/tools). Desktop-only
        // commands the shared frontend still invokes on the web build are kept
        // graceful by the web shim, which maps a 404 to a null no-op so the
        // editor keeps booting.
        _ => respond_err("404 Not Found", &format!("unknown command: {}", cmd)),
    }
}

/// Run a pipeline and STREAM its progress to the browser as Server-Sent Events:
/// each engine PipelineEvent is a `data:` line; the final RunResult is an

/// #259: record a run the web editor started.
///
/// The editor's Run button does not go through `execute_one_with` - it streams,
/// and run-to-here needs a target - so without this the two most-used console
/// run paths recorded nothing addressable. Returns the receipt to finish with.
fn begin_editor_run(
    workspace: &std::path::Path,
    doc: &PipelineDoc,
    name: &str,
    trigger: &str,
    // #289: recorded on the receipt so an editor or streaming run answers the
    // same "which pool, and how long did it wait" as a scheduled one.
    admission: Option<(String, u64)>,
) -> duckle_duckdb_engine::retry::RunReceipt {
    let hash = duckle_duckdb_engine::retry::pipeline_hash(doc);
    let run_id = duckle_duckdb_engine::retry::new_run_id(name, trigger);
    let receipt = duckle_duckdb_engine::retry::begin(
        workspace,
        &run_id,
        trigger,
        name,
        &workspace.join("pipelines").join(format!("{name}.json")).display().to_string(),
        &hash,
        None,
    );
    let receipt = duckle_duckdb_engine::retry::RunReceipt {
        components: duckle_duckdb_engine::plugin::used_by(
            workspace,
            &serde_json::to_value(doc).unwrap_or_default(),
        ),
        ..receipt
    };
    match admission {
        None => {
            let _ = duckle_duckdb_engine::retry::write(workspace, &receipt);
            receipt
        }
        Some((pool, queue_ms)) => {
            let receipt = duckle_duckdb_engine::retry::RunReceipt {
                resource_pool: Some(pool),
                queue_ms: Some(queue_ms),
                ..receipt
            };
            let _ = duckle_duckdb_engine::retry::write(workspace, &receipt);
            receipt
        }
    }
}

/// `event: result` line. The frontend turns these back into the same live
/// per-node animation the desktop gets from the Tauri Channel.
fn run_stream(stream: &mut TcpStream, state: &WebState, body: &[u8]) -> Result<(), String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
        Ok(d) => d,
        Err(e) => {
            let msg = format!("bad pipeline: {}", e);
            return write_reply(stream, &respond_err("400 Bad Request", &msg));
        }
    };
    // Saved Salesforce connection refs resolve server-side against this
    // workspace (#166 stage 2) - the browser never sees the secret.
    if let Err(e) = duckle_secrets::resolve_connection_refs(&state.workspace, &mut doc.nodes) {
        return write_reply(stream, &respond_err("400 Bad Request", &e));
    }
    // Same placeholder resolution as /api/run (execute_one) and the desktop:
    // expand ${ENV:KEY} secrets - so a connection field stored as ${ENV:...}
    // still resolves after ref injection (#166 stage 2) - and the
    // ${date}/${datetime} builtins, before the workspace-context pass.
    let env_file = state.workspace.join("secrets.env");
    if let Err(e) = crate::apply_env_pass(&mut doc, &state.workspace, &env_file) {
        return write_reply(stream, &respond_err("400 Bad Request", &e));
    }
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
    let name = args.get("pipelineName").and_then(|v| v.as_str()).unwrap_or("web").to_string();
    // Optional run-to-here target: when set, the engine runs only the subgraph
    // up to and including this node (partial run).
    let target = args
        .get("targetNodeId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // SSE response head (no Content-Length; we stream until the run ends).
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;

    let (_guard, pool, queued_ms) = state.run_lock.acquire(&doc.resource_pool);
    // A second handle to the same socket for the event callback (the run is
    // synchronous, so events stream first, the result line follows).
    let mut ev = stream.try_clone().map_err(|e| e.to_string())?;
    let engine = DuckdbEngine::new(state.duckdb.clone());
    // Run-to-here is still a run, and the one an operator is most likely to
    // want to find again.
    let receipt = begin_editor_run(
        &state.workspace,
        &doc,
        &name,
        if target.is_some() { "web-partial" } else { "web" },
        Some((pool, queued_ms)),
    );
    let result = engine.execute_pipeline_with_events(&doc, target.as_deref(), Some(&name), |evt| {
        if let Ok(j) = serde_json::to_string(&evt) {
            let _ = ev.write_all(format!("data: {}\n\n", j).as_bytes());
            let _ = ev.flush();
        }
    });
    duckle_duckdb_engine::retry::finish(
        &state.workspace,
        receipt,
        &result.status,
        duckle_duckdb_engine::retry::nodes_of(&result),
    );
    let rj = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
    stream
        .write_all(format!("event: result\ndata: {}\n\n", rj).as_bytes())
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Web-editor autodetect (issue #148). The browser cannot read the server's
/// sources, so schema inspection routes here and drives the SAME engine.inspect
/// the desktop `autodetect_schema` command uses: real driver reads, ${ENV:...}
/// resolved engine-side, and honest errors. Without this the web editor could
/// only fall back to a fabricated col_1/col_2/col_3 schema. The response shape
/// ({ columns, sampleRows }) matches the desktop InspectionPayload exactly.
fn inspect_schema(state: &WebState, body: &[u8]) -> Reply {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let format = args.get("format").and_then(|v| v.as_str()).unwrap_or("");
    if format.is_empty() {
        return respond_err("400 Bad Request", "inspect: missing format");
    }
    let options = args
        .get("options")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let engine = DuckdbEngine::new(state.duckdb.clone());
    match engine.inspect(format, options) {
        Ok(insp) => respond_json(&serde_json::json!({ "columns": insp.schema, "sampleRows": insp.sample_rows }),
        ),
        Err(e) => respond_err("422 Unprocessable Entity", &e.to_string()),
    }
}

fn serve_static(state: &WebState, url_path: &str) -> Reply {
    let rel = url_path.trim_start_matches('/');
    let candidate = if rel.is_empty() { state.dist.join("index.html") } else { state.dist.join(rel) };
    // Confine to the dist dir, and SPA-fallback to index.html for non-asset paths.
    let file = match candidate.canonicalize() {
        Ok(p) if p.starts_with(&state.dist) && p.is_file() => p,
        _ => state.dist.join("index.html"),
    };
    match std::fs::read(&file) {
        Ok(bytes) => respond("200 OK", web_content_type(&file), &bytes),
        Err(e) => respond_err("404 Not Found", &format!("{}: {}", file.display(), e)),
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
    origin: Option<String>,
    host: Option<String>,
    /// `Authorization: Bearer <token>`, for API clients.
    authorization: Option<String>,
    /// Raw `Cookie` header, carrying the console's session id for browsers.
    cookie: Option<String>,
    /// `X-Forwarded-Proto` from a terminating proxy, so the session cookie can be marked
    /// Secure exactly when the browser's leg of the connection actually is.
    forwarded_proto: Option<String>,
    body: Vec<u8>,
}

/// How long a single read may stall before the connection is abandoned.
///
/// Generous per read, not per request, so a slow client on a bad link is fine.
/// Without any deadline a caller could open a socket, send one byte and park
/// the thread serving it forever - and since every connection gets its own
/// `std::thread::spawn` with no ceiling, a handful of those is the whole
/// server. It matters because this runs before anyone is identified: it is the
/// one part of the console an unauthenticated caller always reaches.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest request body that will be buffered.
///
/// `Content-Length` is the caller's own claim and was believed without limit,
/// so a declared and delivered 4 GiB was read into memory before anything
/// looked at who was asking. Pipeline documents and file writes are far below
/// this; anything above it is not a console request.
const MAX_BODY: usize = 32 << 20;

fn read_request(stream: &mut TcpStream) -> Result<Request, String> {
    // Both directions: a client that stops reading must not pin the thread on
    // a blocked write either.
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let _ = stream.set_write_timeout(Some(READ_TIMEOUT));
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
    let mut origin = None;
    let mut host = None;
    let mut authorization = None;
    let mut forwarded_proto = None;
    let mut cookie = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim();
            if key.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            } else if key.eq_ignore_ascii_case("origin") {
                origin = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("host") {
                host = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("x-forwarded-proto") {
                forwarded_proto = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("cookie") {
                cookie = Some(v.trim().to_string());
            }
        }
    }
    if content_length > MAX_BODY {
        return Err(format!("request body too large ({content_length} bytes)"));
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
    Ok(Request { method, path, query, origin, host, authorization, cookie, forwarded_proto, body })
}

/// Host part of an Origin/Host header value (drop scheme, port, path, ipv6 []).
fn header_host(s: &str) -> &str {
    let s = s.trim();
    let s = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .unwrap_or(s);
    let s = s.split('/').next().unwrap_or(s);
    if let Some(rest) = s.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    s.rsplit_once(':').map(|(h, _)| h).unwrap_or(s)
}

/// What to tell an operator starting the editor, about schedules it will not run.
///
/// The scheduler lives in `serve`, not in the editor. That is easy to miss and expensive
/// when missed: the published image's entrypoint is the editor, so the ordinary way to
/// deploy Duckle in a container produces a console where schedules sit looking armed, and
/// nothing ever fires them. There is no error, because nothing failed.
///
/// So say it at startup, and say it louder when the workspace already has schedules that
/// are about to be silently ignored.
fn scheduler_notice(enabled_schedules: usize) -> String {
    if enabled_schedules == 0 {
        "duckle-runner: note: the editor does not run schedules; `duckle-runner serve` does"
            .to_string()
    } else {
        format!(
            "duckle-runner: WARNING: {} enabled schedule(s) in this workspace will NOT run. \
             The editor does not run the scheduler. Restart with `duckle-runner serve` to \
             run them on a cron.",
            enabled_schedules
        )
    }
}

/// The attributes to put on the session cookie, given what the browser's leg of the
/// connection actually is.
///
/// `Secure` cannot simply always be set: the console serves plain HTTP, and on a bare
/// `http://host:8080` a Secure cookie is dropped by the browser, which would lock everyone
/// out of the ordinary local case. Behind a proxy that terminates TLS the browser IS on
/// https, and there the flag should be set, or a session cookie can be sent in clear to
/// anything that reaches the console directly.
///
/// Trusting the header is safe in this direction: the worst a client can do by lying is
/// ask for a stricter cookie than it needs.
fn cookie_attributes(forwarded_proto: Option<&str>) -> &'static str {
    const BASE: &str = "; HttpOnly; SameSite=Strict; Path=/";
    const SECURE: &str = "; HttpOnly; SameSite=Strict; Path=/; Secure";
    // A chain of proxies appends, so the browser's own hop is the first entry.
    let browser_on_https = forwarded_proto
        .and_then(|p| p.split(',').next())
        .is_some_and(|p| p.trim().eq_ignore_ascii_case("https"));
    if browser_on_https {
        SECURE
    } else {
        BASE
    }
}

/// The liveness route. Unauthenticated by design, and it answers with two bytes.
pub const HEALTH_PATH: &str = "/healthz";

/// The readiness route (#300).
///
/// Separate from liveness because they fail differently and an orchestrator
/// acts differently on each: a process that is alive but not ready should stop
/// receiving traffic, not be restarted. Readiness here means the state store is
/// usable and the server can accept a run. It deliberately checks nothing
/// external - a source being down is not this server being unready, and probing
/// sources on every scrape would turn a health check into load.
pub const READY_PATH: &str = "/readyz";

/// The routes an unauthenticated caller may reach, in one place so the console and the
/// editor cannot drift apart on it.
///
/// It stays this short deliberately. Signing in cannot require being signed in, and an
/// orchestrator has to be able to ask whether the process is alive without holding a
/// credential: every route needing one means a Kubernetes HTTP probe gets 401 and the pod
/// is marked unhealthy forever. `/healthz` answers `ok` and nothing else, so it can say the
/// process is up without telling an anonymous caller anything about what is in it.
/// Where a browser starts an OIDC login, and where the provider sends it back.
///
/// Public because signing in cannot require being signed in - the same reason
/// `POST /api/session` is. Neither route trusts anything it is handed: the
/// callback is worthless without a `state` this process issued and is still
/// holding.
pub const OIDC_LOGIN_PATH: &str = "/auth/oidc/login";
pub const OIDC_CALLBACK_PATH: &str = "/auth/oidc/callback";

pub const PUBLIC_ROUTES: [(&str, &str); 7] = [
    ("POST", "/api/session"),
    ("GET", HEALTH_PATH),
    ("GET", READY_PATH),
    ("GET", SETUP_PATH),
    ("POST", SETUP_CLAIM_PATH),
    ("GET", OIDC_LOGIN_PATH),
    ("GET", OIDC_CALLBACK_PATH),
];

pub fn is_public_route(method: &str, path: &str) -> bool {
    PUBLIC_ROUTES.contains(&(method, path))
}

fn is_loopback_host(h: &str) -> bool {
    matches!(h, "127.0.0.1" | "localhost" | "::1")
}

/// Whether a state-changing POST is allowed. Closes the no-auth CSRF /
/// DNS-rebinding gap that the web server otherwise has: a cross-origin Origin
/// (a random website's JS hitting localhost) is rejected, and when bound to
/// loopback the Host must be loopback too, so a DNS name rebound to 127.0.0.1
/// cannot drive the local server. A loopback bind (the default) is fully
/// guarded; a 0.0.0.0 / explicit-IP bind is an opted-in remote exposure (the
/// startup banner already warns "no authentication"), so only the cross-origin
/// check applies there.
fn guard_local(req: &Request, bind_host: &str) -> bool {
    let bound_loopback = is_loopback_host(bind_host);
    if bound_loopback {
        if let Some(h) = req.host.as_deref() {
            if !is_loopback_host(header_host(h)) {
                return false;
            }
        }
    }
    if let Some(o) = req.origin.as_deref() {
        let oh = header_host(o);
        let same_as_host = req.host.as_deref().map(header_host) == Some(oh);
        if !(is_loopback_host(oh) || oh == bind_host || same_as_host) {
            return false;
        }
    }
    true
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

/// One HTTP response, built rather than written.
///
/// Handlers return this instead of writing to a socket. It is what lets the router be
/// exercised without opening a port, and it is the shape a framework wants: a request goes
/// in, a value comes out, and exactly one function knows how bytes reach a wire.
#[derive(Debug, Clone)]
pub struct Reply {
    pub status: String,
    pub content_type: String,
    pub body: Vec<u8>,
    /// Whole header lines, for the rare response needing one. Set-Cookie is the only user
    /// today, which is why this is a list of lines rather than a map.
    pub headers: Vec<String>,
}

impl Reply {
    pub fn with_header(mut self, line: String) -> Reply {
        self.headers.push(line);
        self
    }

    /// The numeric status, for tests and for the eventual framework mapping.
    pub fn code(&self) -> u16 {
        self.status.split_whitespace().next().and_then(|c| c.parse().ok()).unwrap_or(500)
    }
}

/// The only function that still knows about a socket.
fn write_reply(stream: &mut TcpStream, reply: &Reply) -> Result<(), String> {
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
        reply.status,
        reply.content_type,
        reply.body.len()
    );
    for line in &reply.headers {
        head.push_str(line);
        head.push_str("\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(&reply.body).map_err(|e| e.to_string())?;
    Ok(())
}

/// Whether this server could accept a run right now (#300).
///
/// One write and one delete under `.duckle/`, which is where run state lives.
/// Cheap enough to run on every probe, and it fails for the reasons that
/// actually stop runs being recorded: a read-only mount, a full disk, a
/// workspace that has gone away underneath the process.
fn probe_ready(workspace: &Path) -> Result<(), String> {
    let dir = workspace.join(".duckle");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{} is not writable: {e}", dir.display()))?;
    let probe = dir.join(".readyz");
    std::fs::write(&probe, b"ok").map_err(|e| format!("{} is not writable: {e}", dir.display()))?;
    let _ = std::fs::remove_file(&probe);
    Ok(())
}

/// The Prometheus exposition body (#300).
///
/// The run-history half is rendered by the engine, so the endpoint and the
/// textfile exporter cannot come to disagree about what a series means. What is
/// added here is the half a file cannot carry: what this process is doing right
/// now.
fn metrics_body(state: &State) -> String {
    let mut out = duckle_duckdb_engine::history::render_metrics(&state.workspace)
        .unwrap_or_else(|_| {
            // A workspace with no runs yet has no `runs/` directory. That is a
            // server with nothing to report, not a broken scrape - returning an
            // error here would make a fresh deployment look down.
            String::new()
        });
    let per_pool = state.run_lock.permits();
    // Permits in use, not distinct pipeline ids. `state.running` is a set of
    // ids for the console's "Running" badge, so two runs of one pipeline count
    // once and the first to finish removes the id while the other is still
    // going. Saturation is what an operator alerts on, and the gate knows it
    // exactly.
    let in_flight: usize = per_pool.iter().map(|(_, free, total)| total.saturating_sub(*free)).sum();
    let named = state.running.lock().map(|s| s.len()).unwrap_or(0);
    let accepted = state.runs.lock().map(|r| r.len()).unwrap_or(0);
    // Written line by line rather than as one continued string literal: a `\`
    // continuation keeps the indentation of the following source line, which
    // put nine spaces in front of every `# TYPE` and left the document invalid.
    // Prometheus requires each line to start in column zero.
    for (name, help, value) in [
        ("duckle_runs_in_flight", "Run slots in use right now.", in_flight),
        (
            "duckle_pipelines_running",
            "Distinct pipelines with at least one run in flight.",
            named,
        ),
        (
            "duckle_run_permits_total",
            "Run slots across every pool.",
            per_pool.iter().map(|(_, _, total)| *total).sum::<usize>(),
        ),
        (
            "duckle_run_permits_free",
            "Slots available. Zero for any length of time means runs are queueing.",
            per_pool.iter().map(|(_, free, _)| *free).sum::<usize>(),
        ),
        (
            "duckle_runs_tracked",
            "Async runs this process is still holding, finished or not.",
            accepted,
        ),
    ] {
        out.push_str(&format!("# HELP {name} {help}
# TYPE {name} gauge
{name} {value}
"));
    }
    // #289: per-pool, labelled. The totals above cannot answer "which pool is
    // saturated", which is the only question worth asking once there is more
    // than one - a network pool at 8/8 and a heavy pool idle sum to something
    // that looks half busy.
    out.push_str("# HELP duckle_pool_permits_total Run slots in this pool.
# TYPE duckle_pool_permits_total gauge
");
    for (pool, _, total) in &per_pool {
        out.push_str(&format!("duckle_pool_permits_total{{pool=\"{pool}\"}} {total}
"));
    }
    out.push_str("# HELP duckle_pool_permits_free Slots free in this pool. Zero for any length of time means runs are queueing here.
# TYPE duckle_pool_permits_free gauge
");
    for (pool, free, _) in &per_pool {
        out.push_str(&format!("duckle_pool_permits_free{{pool=\"{pool}\"}} {free}
"));
    }
    out
}

/// #295: create, retry or cancel a backfill over the API.
///
/// Create and retry EXECUTE, which can take hours, so they are accepted and run
/// on a thread while the response returns the plan id immediately - the same
/// shape as an asynchronous run, for the same reason: an operator should get
/// something addressable rather than a held-open connection.
fn api_backfill_action(state: &Arc<State>, req: &Request) -> Reply {
    use duckle_duckdb_engine::backfill;
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
    let action = body.get("action").and_then(Value::as_str).unwrap_or_default();
    let ws = state.workspace.clone();
    let duckdb = state.duckdb.clone();

    match action {
        "create" => {
            let Some(file) = body.get("pipeline").and_then(Value::as_str) else {
                return respond_err("400 Bad Request", "create needs a pipeline path");
            };
            let path = match resolve_in_workspace(&ws, file) {
                Ok(p) => p,
                Err(e) => return respond_err("400 Bad Request", &e),
            };
            let plan = match duckle_duckdb_engine::backfill_exec::plan_for(
                &ws,
                &path,
                body.get("from").and_then(Value::as_str).unwrap_or_default(),
                body.get("to").and_then(Value::as_str).unwrap_or_default(),
                body.get("maxConcurrent").and_then(Value::as_u64).unwrap_or(4) as usize,
                body.get("occurrence").and_then(Value::as_str),
            ) {
                Ok(p) => p,
                Err(e) => return respond_err("400 Bad Request", &e),
            };
            // A dry run writes nothing. "What would this queue" must not be a
            // question that queues anything.
            if body.get("dryRun").and_then(Value::as_bool).unwrap_or(false) {
                return respond_json(&json!({
                    "dryRun": true,
                    "count": plan.partitions.len(),
                    "partitions": plan.partitions.iter().map(|p| &p.key).collect::<Vec<_>>(),
                }));
            }
            if let Err(e) = backfill::save(&ws, &plan) {
                return respond_err("500 Internal Server Error", &e);
            }
            let id = plan.id.clone();
            let logged = id.clone();
            let count = plan.partitions.len();
            let force = body.get("force").and_then(Value::as_bool).unwrap_or(false);
            std::thread::spawn(move || {
                if let Err(e) =
                    duckle_duckdb_engine::backfill_exec::execute_ledger(&ws, &duckdb, plan, force, &|_| {})
                {
                    // Accepted-then-failed is still a failure, and a background
                    // thread that swallows it leaves the ledger as the only
                    // evidence.
                    eprintln!("backfill {logged}: {e}");
                }
            });
            respond_json(&json!({ "accepted": true, "id": id, "partitions": count }))
        }
        "retry" => {
            let Some(id) = body.get("id").and_then(Value::as_str) else {
                return respond_err("400 Bad Request", "retry needs an id");
            };
            let mut plan = match backfill::load(&ws, id) {
                Ok(p) => p,
                Err(e) => return respond_err("404 Not Found", &e),
            };
            let only = body
                .get("partition")
                .and_then(Value::as_str)
                .map(|k| vec![k.to_string()]);
            let n = plan.retry_open(only.as_deref());
            if n == 0 {
                return respond_json(&json!({ "retried": 0, "id": id }));
            }
            plan.pid = Some(std::process::id());
            if let Err(e) = backfill::save(&ws, &plan) {
                return respond_err("500 Internal Server Error", &e);
            }
            let id = id.to_string();
            let logged = id.clone();
            std::thread::spawn(move || {
                // A retry is explicit: the operator decided this slice should
                // run, so it is not skipped as already-done.
                if let Err(e) =
                    duckle_duckdb_engine::backfill_exec::execute_ledger(&ws, &duckdb, plan, true, &|_| {})
                {
                    // Accepted-then-failed is still a failure, and a background
                    // thread that swallows it leaves the ledger as the only
                    // evidence.
                    eprintln!("backfill {logged}: {e}");
                }
            });
            respond_json(&json!({ "accepted": true, "id": id, "retried": n }))
        }
        // Cancel is immediate: it only marks the open slices, so there is
        // nothing to wait for and an operator gets the answer rather than an
        // acceptance.
        "cancel" => {
            let Some(id) = body.get("id").and_then(Value::as_str) else {
                return respond_err("400 Bad Request", "cancel needs an id");
            };
            let mut plan = match backfill::load(&ws, id) {
                Ok(p) => p,
                Err(e) => return respond_err("404 Not Found", &e),
            };
            let n = plan.cancel();
            plan.pid = None;
            if let Err(e) = backfill::save(&ws, &plan) {
                return respond_err("500 Internal Server Error", &e);
            }
            respond_json(&json!({ "cancelled": n, "id": id }))
        }
        other => respond_err("400 Bad Request", &format!("unknown action {other:?}")),
    }
}

fn respond(status: &str, content_type: &str, body: &[u8]) -> Reply {
    Reply {
        status: status.to_string(),
        content_type: content_type.to_string(),
        body: body.to_vec(),
        headers: Vec::new(),
    }
}

fn respond_json(value: &Value) -> Reply {
    respond("200 OK", "application/json", value.to_string().as_bytes())
}

fn respond_err(status: &str, msg: &str) -> Reply {
    respond(status, "application/json", json!({ "error": msg }).to_string().as_bytes())
}

fn respond_403(msg: &str) -> Reply {
    respond("403 Forbidden", "text/plain", msg.as_bytes())
}

#[allow(dead_code)]
fn handle(mut stream: TcpStream, state: &Arc<State>) -> Result<(), String> {
    let req = read_request(&mut stream)?;
    let reply = route_console(&req, state);
    write_reply(&mut stream, &reply)
}

/// Decide what the console answers, without touching a socket.
///
/// Every authorisation decision lives here, which is what makes it testable: a request
/// goes in and a response comes out, so the 401 and 403 paths can be exercised without
/// standing up a server.
/// What a request is allowed to do, decided once.
///
/// Either the caller, or the response to send them instead. The extractor and the router
/// both call this, because a second copy of an authorisation rule is a second thing to
/// forget when the rule changes.
///
/// A public route yields no caller and no refusal: it is handled before anything needs an
/// identity, which is why sign-in can work without being signed in.
pub enum Access {
    /// Reachable without a credential. Nothing has been decided about a caller.
    Public,
    /// Who this is. Already checked against what the route requires.
    Caller(console_auth::Identity),
    /// Send this instead, and do not dispatch.
    Refused(Reply),
}

fn authorize(req: &Request, state: &State) -> Access {
    // Nobody administers this console yet, so there is no identity to establish and
    // nothing to authorise against. Exactly two things answer, and everything else says
    // what is actually wrong: 401 would claim the caller needs to sign in, when what they
    // need is for somebody to finish setting the server up.
    if state.console.mode() == console_auth::Mode::Unclaimed {
        // Liveness answers throughout. A probe that fails during setup gets the pod
        // killed, which restarts it, which re-opens the window and loses whatever was
        // typed: setup would be impossible on anything that health-checks.
        let setting_up = (req.method == "GET" && req.path == SETUP_PATH)
            || (req.method == "POST" && req.path == SETUP_CLAIM_PATH)
            || (req.method == "GET" && req.path == HEALTH_PATH);
        if !setting_up {
            return Access::Refused(respond_err(
                "503 Service Unavailable",
                "this server has not been set up yet. Open /setup to claim it",
            ));
        }
        return Access::Public;
    }

    // A loopback console with no token treats every caller as a local admin, on the
    // reasoning that reaching the socket means already being on the machine. A browser
    // breaks that reasoning: any page the operator visits can POST to 127.0.0.1 from their
    // machine. The editor has blocked cross-origin state changes since it shipped and the
    // console did not, so `fetch('http://127.0.0.1:8080/api/run', ...)` from a random site
    // ran a workspace pipeline. Same guard, same place in the request.
    if req.method != "GET" && req.path.starts_with("/api/") && !guard_local(req, &state.host) {
        return Access::Refused(respond_403("blocked: cross-origin or non-local request"));
    }

    if is_public_route(&req.method, &req.path) {
        return Access::Public;
    }

    // Everything else is identified and authorised before it is dispatched, so a route
    // cannot be reached by forgetting to check it at the call site.
    let (needed, action) = audit::requirement(&req.method, &req.path);
    let target = audit_target(req);
    let who = state.console.identify(req.authorization.as_deref(), req.cookie.as_deref());
    let Some(who) = who else {
        audit::record(&state.workspace, None, action, &target, audit::Outcome::Unauthenticated);
        // A browser asking for the page gets the sign-in form; an API client gets a 401 it
        // can act on.
        if req.method == "GET" && (req.path == "/" || req.path == "/index.html") {
            return Access::Refused(respond(
                "401 Unauthorized",
                "text/html; charset=utf-8",
                SIGNIN_HTML.as_bytes(),
            ));
        }
        return Access::Refused(respond_err("401 Unauthorized", "sign in to use the console"));
    };
    if !who.role.allows(needed) {
        audit::record(&state.workspace, Some(&who), action, &target, audit::Outcome::Denied);
        return Access::Refused(respond_err(
            "403 Forbidden",
            &format!("this needs the {} role; you have {}", needed.as_str(), who.role.as_str()),
        ));
    }
    // Reads are not recorded: they would bury the events worth seeing under a dashboard
    // that polls every few seconds. Anything that changes something, and every refusal
    // above, is.
    if req.method != "GET" {
        audit::record(&state.workspace, Some(&who), action, &target, audit::Outcome::Allowed);
    }
    Access::Caller(who)
}

/// The handful of things reachable without a credential.
///
/// Kept in one function so the socket path and the framework path answer identically, and
/// so the list of what needs no credential is somewhere a person can read in one go.
/// The OIDC login and callback (#310).
///
/// Both halves in one function because they are two steps of one exchange and
/// splitting them would put the state handling in two places.
fn oidc_route(req: &Request, state: &State) -> Reply {
    let Some(cfg) = state.oidc.as_ref() else {
        // Not configured is 404 and not 401: there is no such login here, and
        // saying "unauthorised" would suggest there is one to get past.
        return respond_err("404 Not Found", "this server has no OIDC login configured");
    };
    // Discovered lazily and cached, so a provider that is briefly unavailable
    // delays a login rather than preventing the server from starting.
    let endpoints = {
        let mut slot = state.oidc_endpoints.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            match crate::oidc::discover(&cfg.issuer) {
                Ok(e) => *slot = Some(e),
                Err(e) => return respond_err("502 Bad Gateway", &e),
            }
        }
        slot.clone().expect("just discovered")
    };

    if req.path == OIDC_LOGIN_PATH {
        let (url, pending) = crate::oidc::begin(cfg, &endpoints);
        let browser = pending.browser.clone();
        state
            .oidc_logins
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(pending);
        return Reply {
            status: "302 Found".into(),
            content_type: "text/plain; charset=utf-8".into(),
            body: Vec::new(),
            headers: vec![
                format!("Location: {url}"),
                // Ties the callback to THIS browser. Without it the state is a
                // bearer token: an attacker starts a login, takes the callback
                // URL for their own identity, and gets a victim to visit it -
                // the victim is then signed in as the attacker.
                format!(
                    "Set-Cookie: {}={browser}{}",
                    crate::oidc::LOGIN_COOKIE,
                    cookie_attributes(req.forwarded_proto.as_deref())
                ),
            ],
        };
    }

    // The callback. Everything here is attacker-supplied until proven
    // otherwise, which is why the state is checked before anything is fetched.
    let (Some(code), Some(returned_state)) =
        (req.query.get("code"), req.query.get("state"))
    else {
        // A provider error comes back as ?error=..., and echoing it verbatim
        // would put attacker text on the page.
        return respond_err("400 Bad Request", "the OIDC callback carried no code");
    };
    let browser = req
        .cookie
        .as_deref()
        .and_then(|c| console_auth::cookie_value(c, crate::oidc::LOGIN_COOKIE));
    let Some(pending) = state
        .oidc_logins
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take(returned_state, browser.as_deref())
    else {
        return respond_err(
            "400 Bad Request",
            "this callback does not match a login this server started, or it took too long",
        );
    };

    let id_token = match crate::oidc::exchange(cfg, &endpoints, code, &pending.verifier) {
        Ok(t) => t,
        Err(e) => return respond_err("502 Bad Gateway", &e),
    };
    let now = chrono::Utc::now().timestamp();
    let identity = match crate::oidc::verify(cfg, &endpoints, &id_token, &pending.nonce, now) {
        Ok(i) => i,
        // 403 and not 502: the provider answered, and the answer was that this
        // person does not get in.
        Err(e) => {
            audit::record(&state.workspace, None, "oidc.refused", &e, audit::Outcome::Denied);
            return respond_err("403 Forbidden", &e);
        }
    };

    // Audit carries the provider's STABLE subject, not the display name: a
    // name or an email can be reassigned to a different person, and an audit
    // trail that follows the label rather than the identity is worse than none.
    audit::record(
        &state.workspace,
        None,
        "oidc.signin",
        &format!("{} as {}", identity.actor(), identity.role.as_str()),
        audit::Outcome::Allowed,
    );
    let sid = state.console.sign_in_external(&identity.actor(), identity.role);
    Reply {
        status: "302 Found".into(),
        content_type: "text/plain; charset=utf-8".into(),
        body: Vec::new(),
        headers: vec![
            "Location: /".to_string(),
            session_cookie_header(&sid, req.forwarded_proto.as_deref()),
            // And the login cookie is spent.
            format!(
                "Set-Cookie: {}=; Max-Age=0{}",
                crate::oidc::LOGIN_COOKIE,
                cookie_attributes(req.forwarded_proto.as_deref())
            ),
        ],
    }
}

/// Liveness and readiness, for whichever server is asking.
///
/// Shared by the console and the editor because both are processes an
/// orchestrator probes, and a probe that answers on one and redirects to a
/// sign-in page on the other is worse than not having it.
fn probe_reply(path: &str, workspace: &Path) -> Option<Reply> {
    if path == HEALTH_PATH {
        return Some(respond("200 OK", "text/plain; charset=utf-8", b"ok"));
    }
    if path == READY_PATH {
        // Writable, not merely present: a workspace mounted read-only, or one
        // whose disk has filled, answers every read fine and cannot record a
        // single run. That is exactly the state readiness exists to catch.
        return Some(match probe_ready(workspace) {
            Ok(()) => respond("200 OK", "text/plain; charset=utf-8", b"ready"),
            // 503, so a load balancer takes it out of rotation rather than
            // restarting it: the process is fine, its storage is not.
            Err(why) => respond(
                "503 Service Unavailable",
                "text/plain; charset=utf-8",
                format!("not ready: {why}
").as_bytes(),
            ),
        });
    }
    None
}

fn public_route(req: &Request, state: &State) -> Reply {
    if let Some(reply) = probe_reply(&req.path, &state.workspace) {
        return reply;
    }
    // Before the sign-in fallthrough below. A path in PUBLIC_ROUTES with no
    // branch here does not 404 - it is treated as a token sign-in attempt and
    // answered 401, which for a login route is a bug that looks like a
    // misconfigured provider.
    if req.path == OIDC_LOGIN_PATH || req.path == OIDC_CALLBACK_PATH {
        return oidc_route(req, state);
    }
    if req.method == "GET" && req.path == SETUP_PATH {
        // A console that is already set up has no setup page, and saying so is friendlier
        // than serving a form that will refuse whatever is typed into it.
        if state.console.mode() != console_auth::Mode::Unclaimed {
            return respond_err("410 Gone", "this server has already been set up");
        }
        return respond("200 OK", "text/html; charset=utf-8", SETUP_HTML.as_bytes());
    }
    if req.method == "POST" && req.path == SETUP_CLAIM_PATH {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
        let label = body.get("label").and_then(|v| v.as_str()).unwrap_or("");
        return match state.console.claim(label) {
            Ok(token) => {
                // Worth an audit line of its own: this is the moment the server acquired
                // an owner, and it is the one event with nobody to attribute it to yet.
                audit::record(&state.workspace, None, "console.claimed", label, audit::Outcome::Allowed);
                eprintln!("duckle-runner: claimed by '{label}'; setup is closed");
                respond_json(&json!({ "token": token, "label": label, "role": "admin" }))
            }
            Err(e) => respond_err("409 Conflict", &e),
        };
    }
    sign_in(state, req)
}

#[allow(dead_code)]
fn route_console(req: &Request, state: &Arc<State>) -> Reply {
    match authorize(req, state) {
        Access::Refused(reply) => reply,
        Access::Public => public_route(req, state),
        Access::Caller(who) => dispatch_console(req, state, who),
    }
}

/// Answer a request whose caller has already been authorised.
///
/// Split from [`route_console`] so the axum path does not authorise a second time:
/// authorising twice would write two audit entries for one request.
fn dispatch_console(req: &Request, state: &Arc<State>, who: console_auth::Identity) -> Reply {
    let route = (req.method.as_str(), req.path.as_str());

    if route == ("DELETE", "/api/session") {
        state.console.sign_out(req.cookie.as_deref());
        return respond_json(&json!({ "ok": true }));
    }
    if route == ("GET", "/api/whoami") {
        return respond_json(&json!({ "label": who.label, "role": who.role.as_str(), "open": state.console.is_open() }),
        );
    }

    match route {
        ("GET", "/") | ("GET", "/index.html") => {
            respond("200 OK", "text/html; charset=utf-8", PANEL_HTML.as_bytes())
        }
        // #300. Authenticated, unlike /healthz and /readyz: pipeline names are
        // the shape of someone's business, and a probe saying "this process is
        // up" gives an anonymous caller nothing. A scraper sends the same
        // bearer token any other API client does.
        ("GET", "/metrics") => respond(
            "200 OK",
            // The version Prometheus negotiates for the text exposition format;
            // without it some scrapers fall back to guessing.
            "text/plain; version=0.0.4; charset=utf-8",
            metrics_body(state).as_bytes(),
        ),
        ("GET", "/api/summary") => respond_json(&api_summary(state)),
        ("GET", "/api/pipelines") => respond_json(&api_pipelines(state)),
        ("GET", "/api/pipeline") => match req.query.get("file") {
            Some(f) => match read_pipeline_file(state, f) {
                Ok(v) => respond_json(&v),
                Err(e) => respond_err("404 Not Found", &e),
            },
            None => respond_err("400 Bad Request", "missing file"),
        },
        ("GET", "/api/runs") => respond_json(&api_runs(state, req.query.get("id").map(|s| s.as_str()))),
        // #295: the persisted backfill plan, addressable over the server rather
        // than only by an in-process CLI invocation.
        ("GET", "/api/backfills") => match req.query.get("id") {
            Some(id) => match duckle_duckdb_engine::backfill::load(&state.workspace, id) {
                Ok(b) => respond_json(&serde_json::to_value(&b).unwrap_or_default()),
                Err(e) => respond_err("404 Not Found", &e),
            },
            None => respond_json(
                &json!({ "backfills": duckle_duckdb_engine::backfill::list(&state.workspace) }),
            ),
        },
        ("POST", "/api/backfills") => api_backfill_action(state, req),
        ("GET", "/api/log") => respond_json(&api_log(state, &req.query)),
        ("GET", "/api/catalog") => respond_json(&api_catalog(state)),
        ("GET", "/api/batches") => respond_json(&json!({ "batches": duckle_duckdb_engine::batch::statuses(&state.workspace) }),
        ),
        ("GET", "/api/batch") => match req.query.get("id") {
            Some(id) => {
                let status = duckle_duckdb_engine::batch::status(&state.workspace, id);
                let ledger = duckle_duckdb_engine::batch::ledger(&state.workspace, id);
                // Newest first: an operator opening a batch is looking for what
                // just went wrong, not for how it started.
                let mut recent: Vec<_> = ledger.into_iter().collect();
                recent.reverse();
                recent.truncate(200);
                respond_json(&json!({ "status": status, "recent": recent }))
            }
            None => respond_err("400 Bad Request", "missing id"),
        },
        ("POST", "/api/batch/redrive") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            match body.get("id").and_then(|v| v.as_str()) {
                Some(id) => match duckle_duckdb_engine::batch::redrive(&state.workspace, id) {
                    Ok(cleared) => respond_json(&json!({ "ok": true, "cleared": cleared,
                                 "status": duckle_duckdb_engine::batch::status(&state.workspace, id) }),
                    ),
                    Err(e) => respond_err("400 Bad Request", &e.to_string()),
                },
                None => respond_err("400 Bad Request", "missing id"),
            }
        }
        // Backfill: the saved state a pipeline resumes from. Same operations
        // the desktop Backfill panel and `duckle-runner backfill` use, so the
        // three surfaces cannot disagree about what a run will read.
        //
        // The pipeline is named by ?file=, resolved inside the workspace and
        // reduced to its file stem - exactly what execute_one hands to
        // execute_pipeline_named - so what this reports is what a run reads.
        ("GET", "/api/watermarks") => match watermark_target(state, req) {
            Ok((ws, name)) => {
                let entries = duckle_duckdb_engine::watermark::list(&ws, &name);
                respond_json(&json!({ "pipeline": name, "entries": entries }))
            }
            Err(e) => respond_err("400 Bad Request", &e),
        },
        ("POST", "/api/watermarks") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            match watermark_target(state, req) {
                Err(e) => respond_err("400 Bad Request", &e),
                Ok((ws, name)) => match body.get("node").and_then(|v| v.as_str()) {
                    None => respond_err("400 Bad Request", "missing node"),
                    Some(node) => {
                        use duckle_duckdb_engine::watermark as wm;
                        // A wrong-kind write is refused by the engine, not here:
                        // one guard, so this route cannot drift from the CLI.
                        let done = match (
                            body.get("value").and_then(|v| v.as_str()),
                            body.get("snapshot_id").and_then(|v| v.as_u64()),
                        ) {
                            (Some(v), None) => wm::set_incremental(
                                &ws,
                                &name,
                                node,
                                v,
                                body.get("type").and_then(|t| t.as_str()),
                            ),
                            (None, Some(id)) => wm::set_snapshot(&ws, &name, node, id),
                            (Some(_), Some(_)) => {
                                return respond_err(
                                    "400 Bad Request",
                                    "give value or snapshot_id, not both",
                                )
                            }
                            (None, None) => {
                                return respond_err(
                                    "400 Bad Request",
                                    "missing value or snapshot_id",
                                )
                            }
                        };
                        match done {
                            Ok(()) => respond_json(&json!({ "ok": true, "node": node })),
                            Err(e) => respond_err("400 Bad Request", &e.to_string()),
                        }
                    }
                },
            }
        }
        ("DELETE", "/api/watermarks") => match watermark_target(state, req) {
            Ok((ws, name)) => match req.query.get("node") {
                Some(node) => match duckle_duckdb_engine::watermark::clear(&ws, &name, node) {
                    Ok(()) => respond_json(&json!({ "ok": true, "cleared": node })),
                    Err(e) => respond_err("400 Bad Request", &e.to_string()),
                },
                None => respond_err("400 Bad Request", "missing node"),
            },
            Err(e) => respond_err("400 Bad Request", &e),
        },
        ("GET", "/api/audit") => {
            let filter = audit::Filter {
                actor: req.query.get("actor").cloned(),
                outcome: req.query.get("outcome").cloned(),
                action: req.query.get("action").cloned(),
                // A page, not the file. The console polls, and an unbounded
                // read would grow with the log until the poll was the most
                // expensive thing the server did.
                limit: req
                    .query
                    .get("limit")
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(200)
                    .clamp(1, 1000),
            };
            match audit::read(&state.workspace, &filter) {
                Ok(page) => respond_json(&json!(page)),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        ("POST", "/api/catalog") => {
            match duckle_duckdb_engine::catalog::build_and_save(&state.workspace) {
                Ok(_) => respond_json(&api_catalog(state)),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        ("GET", "/api/schedules") => match load_schedules(state) {
            Ok(v) => respond_json(&v),
            Err(e) => respond_err("500 Internal Server Error", &e),
        },
        ("POST", "/api/schedules") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            match save_schedule(state, &body) {
                Ok(v) => respond_json(&v),
                Err(e) => respond_err("400 Bad Request", &e),
            }
        }
        ("GET", "/api/plans") => match duckle_duckdb_engine::plans::load(&state.workspace) {
            Ok(plans) => respond_json(&json!({ "plans": plans })),
            Err(e) => respond_err("500 Internal Server Error", &e),
        },
        ("POST", "/api/plans") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let plan: duckle_duckdb_engine::plans::Plan = match serde_json::from_value(body) {
                Ok(p) => p,
                Err(e) => return respond_err("400 Bad Request", &format!("that is not a plan: {e}")),
            };
            // Refused where it was written rather than at three in the morning.
            let problems = plan.problems();
            if !problems.is_empty() {
                return respond_err("400 Bad Request", &problems.join("; "));
            }
            let id = plan.id.clone();
            match duckle_duckdb_engine::plans::update(&state.workspace, |list| {
                list.retain(|p| p.id != id);
                list.push(plan);
                list.sort_by(|a, b| a.id.cmp(&b.id));
            }) {
                Ok(_) => respond_json(&json!({ "saved": id })),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        ("DELETE", "/api/plans") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            match duckle_duckdb_engine::plans::update(&state.workspace, |list| {
                list.retain(|p| p.id != id)
            }) {
                Ok(_) => respond_json(&json!({ "removed": id })),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        ("POST", "/api/plans/run") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let plan = match duckle_duckdb_engine::plans::load(&state.workspace) {
                Ok(list) => list.into_iter().find(|p| p.id == id),
                Err(e) => return respond_err("500 Internal Server Error", &e),
            };
            let Some(plan) = plan else {
                return respond_err("404 Not Found", "no plan with that id");
            };
            let params = parse_run_params(body.get("params"));
            // Each pipeline goes through the ordinary run path, so every one of them lands
            // in run history under its own name. A plan that produced a single opaque run
            // would answer "the nightly load failed" without answering which part.
            let outcome = duckle_duckdb_engine::plans::execute(&plan, |pipeline| {
                plan_step_outcome(execute_one(
                    state,
                    // A step may be spelled as a bare id by the desktop editor; execute_one
                    // takes a workspace-relative file. Normalised so one plans.json means
                    // the same thing in both products.
                    &duckle_duckdb_engine::plans::step_pipeline_file(pipeline),
                    "plan",
                    &params,
                ))
            });
            respond_json(&serde_json::to_value(&outcome).unwrap_or(json!({})))
        }
        ("GET", "/api/admin/users") => match state.console.list_accounts() {
            Ok(list) => respond_json(&json!({
                "users": list.iter().map(|(label, role)| json!({
                    "label": label, "role": role.as_str()
                })).collect::<Vec<_>>()
            })),
            Err(e) => respond_err("500 Internal Server Error", &e),
        },
        ("POST", "/api/admin/users") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let label = body.get("label").and_then(|v| v.as_str()).unwrap_or("");
            let role = body.get("role").and_then(|v| v.as_str()).unwrap_or("viewer");
            match console_auth::Console::role_from(role) {
                None => respond_err("400 Bad Request", "role must be viewer, operator or admin"),
                Some(role) => match state.console.create_account(label, role) {
                    // Shown once. There is no route that gives it back, by design.
                    Ok(token) => respond_json(&json!({ "label": label, "role": role.as_str(), "token": token })),
                    Err(e) => respond_err("400 Bad Request", &e),
                },
            }
        }
        ("DELETE", "/api/admin/users") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let label = body.get("label").and_then(|v| v.as_str()).unwrap_or("");
            // Removing the last administrator would leave a console nobody can administer,
            // and no amount of interface recovers from that. It only applies to removing an
            // administrator: an earlier version counted the survivors without checking what
            // was being removed, so deleting an operator was refused for leaving too few
            // admins, which it had nothing to do with.
            let accounts = state.console.list_accounts().unwrap_or_default();
            let removing_an_admin = accounts
                .iter()
                .any(|(n, r)| n == label && *r == console_auth::Role::Admin);
            let admins_left = accounts
                .iter()
                .filter(|(n, r)| *r == console_auth::Role::Admin && n != label)
                .count();
            if removing_an_admin && admins_left == 0 {
                return respond_err(
                    "409 Conflict",
                    "that is the last administrator; make someone else an admin first",
                );
            }
            match state.console.remove_account(label) {
                Ok(true) => respond_json(&json!({ "removed": label })),
                Ok(false) => respond_err("404 Not Found", "no account with that name"),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        ("GET", "/api/admin/keys") => match state.console.list_keys() {
            Ok(keys) => {
                let now = crate::auth_store::now_secs();
                respond_json(&json!({
                    "keys": keys.iter().map(|k| json!({
                        "label": k.label,
                        "role": k.role.as_str(),
                        "state": if k.revoked { "revoked" }
                                 else if k.expires_at.is_some_and(|e| e <= now) { "expired" }
                                 else { "live" },
                        "createdAt": k.created_at,
                        "expiresAt": k.expires_at,
                        "lastUsedAt": k.last_used_at,
                    })).collect::<Vec<_>>()
                }))
            }
            Err(e) => respond_err("500 Internal Server Error", &e),
        },
        ("POST", "/api/admin/keys") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let label = body.get("label").and_then(|v| v.as_str()).unwrap_or("");
            let role = body.get("role").and_then(|v| v.as_str()).unwrap_or("viewer");
            let days = body.get("expiresDays").and_then(|v| v.as_i64()).filter(|d| *d > 0);
            match console_auth::Console::role_from(role) {
                None => respond_err("400 Bad Request", "role must be viewer, operator or admin"),
                Some(role) => match state.console.create_key(label, role, days) {
                    Ok(key) => respond_json(&json!({ "label": label, "role": role.as_str(), "key": key })),
                    Err(e) => respond_err("400 Bad Request", &e),
                },
            }
        }
        ("POST", "/api/admin/keys/revoke") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let label = body.get("label").and_then(|v| v.as_str()).unwrap_or("");
            match state.console.revoke_key(label) {
                Ok(0) => respond_err("404 Not Found", "no live key with that name"),
                Ok(n) => respond_json(&json!({ "revoked": label, "count": n })),
                Err(e) => respond_err("500 Internal Server Error", &e),
            }
        }
        ("POST", "/api/deploy") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            match deploy_into(&state.workspace, &body) {
                Ok(v) => respond_json(&v),
                Err(e) => respond_err("400 Bad Request", &e),
            }
        }
        ("GET", "/api/params") => match req.query.get("file") {
            Some(f) => match discover_pipeline_params(state, f) {
                Ok(names) => respond_json(&json!({ "params": names })),
                Err(e) => respond_err("404 Not Found", &e),
            },
            None => respond_err("400 Bad Request", "missing file"),
        },
        ("POST", "/api/run") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let file = match body.get("file").and_then(|v| v.as_str()) {
                Some(f) => f.to_string(),
                None => return respond_err("400 Bad Request", "missing file"),
            };
            let params = parse_run_params(body.get("params"));
            match execute_one(state, &file, "manual", &params) {
                Ok(v) => respond_json(&v),
                Err(e) => respond_err("400 Bad Request", &e),
            }
        }
        // #259: the same run, but the HTTP request does not wait for it. A
        // backfill can outlive any reverse proxy's idle timeout, and a client
        // that gives up mid-run should not take the run down with it.
        ("POST", "/api/run/async") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let file = match body.get("file").and_then(|v| v.as_str()) {
                Some(f) => f.to_string(),
                None => return respond_err("400 Bad Request", "missing file"),
            };
            let params = parse_run_params(body.get("params"));
            // Resolve before accepting: a 202 for a run that can never start is
            // worse than a 400, because the caller then polls for a run that
            // will never appear.
            let path = match resolve_in_workspace(&state.workspace, &file) {
                Ok(p) => p,
                Err(e) => return respond_err("400 Bad Request", &e),
            };
            let pipeline_id = path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "pipeline".into());
            let run_id = new_run_id(&pipeline_id);
            let engine = DuckdbEngine::new(state.duckdb.clone());
            if let Ok(mut runs) = state.runs.lock() {
                runs.insert(
                    run_id.clone(),
                    LiveRun {
                        pipeline_id: pipeline_id.clone(),
                        started_at: chrono::Utc::now().to_rfc3339(),
                        engine: engine.clone(),
                        finished: None,
                    },
                );
            }
            let bg = Arc::clone(state);
            let rid = run_id.clone();
            std::thread::spawn(move || {
                let outcome =
                    execute_one_with(&bg, &file, "manual", &params, Some(engine), Some(&rid));
                if let Ok(mut runs) = bg.runs.lock() {
                    let pid = runs.get(&rid).map(|r| r.pipeline_id.clone()).unwrap_or_default();
                    if let Some(live) = runs.get_mut(&rid) {
                        live.finished = Some(match outcome {
                            Ok(v) => v,
                            // execute_one answers Err only when the run could
                            // not START, which the poller still has to see.
                            Err(e) => json!({ "id": pid, "status": "error", "error": e }),
                        });
                    }
                    forget_oldest_finished_runs(&mut runs);
                }
            });
            respond(
                "202 Accepted",
                "application/json",
                json!({
                    "runId": run_id,
                    "pipelineId": pipeline_id,
                    "status": "queued",
                })
                .to_string()
                .as_bytes(),
            )
        }
        ("GET", "/api/run/status") => match req.query.get("runId") {
            Some(rid) => match run_status(state, rid) {
                Some(v) => respond_json(&v),
                None => respond_err("404 Not Found", "no such run"),
            },
            None => respond_err("400 Bad Request", "missing runId"),
        },
        // Cancellation is polled at every stage boundary and kills the active
        // DuckDB child, so this answers at once and the run stops shortly after
        // rather than at the end of whatever it was doing.
        ("DELETE", "/api/run") => match req.query.get("runId") {
            Some(rid) => {
                let hit = state.runs.lock().ok().and_then(|runs| {
                    runs.get(rid).map(|live| {
                        live.engine.request_cancel();
                        (live.pipeline_id.clone(), live.finished.is_some())
                    })
                });
                match hit {
                    Some((pid, done)) => respond_json(&json!({
                        "runId": rid,
                        "pipelineId": pid,
                        // Reporting "cancelling" for a run that already ended
                        // would be a lie the caller acts on.
                        "cancelling": !done,
                    })),
                    None => respond_err("404 Not Found", "no such run"),
                }
            }
            None => respond_err("400 Bad Request", "missing runId"),
        },
        _ => respond_err("404 Not Found", "not found"),
    }
}

/// What the request was aimed at, for the audit log. Never the body, which can
/// hold run parameters, and never the query string wholesale.
fn audit_target(req: &Request) -> String {
    if let Some(f) = req.query.get("file").or_else(|| req.query.get("id")) {
        return f.clone();
    }
    if req.method != "GET" {
        if let Ok(body) = serde_json::from_slice::<Value>(&req.body) {
            if let Some(t) = body.get("file").or_else(|| body.get("id")).and_then(|v| v.as_str()) {
                return t.to_string();
            }
        }
    }
    req.path.clone()
}

/// Exchange a token for a session cookie.
///
/// The token arrives in the body, never in the URL: a query string reaches the
/// server log, the browser history and any proxy in between.
/// The `Set-Cookie` line that hands a browser its session (#310).
///
/// One constructor for all three login paths - the console token, the editor
/// token and OIDC. There were three copies of this and the third spelled the
/// cookie name as a literal that did not match the one the console reads, so a
/// completed SSO login authenticated nobody: the session row was written, the
/// browser held a cookie under a name nothing looks at, and the audit log said
/// the user had signed in.
fn session_cookie_header(sid: &str, forwarded_proto: Option<&str>) -> String {
    // HttpOnly so page scripts cannot read it, SameSite=Strict so another site
    // cannot ride it, and Secure exactly when the browser's own hop is https.
    // Always setting it would stop the cookie being stored at all on plain-HTTP
    // local use; never setting it lets a session cookie travel in clear behind
    // a proxy that does terminate TLS.
    format!(
        "Set-Cookie: {}={sid}{}",
        console_auth::SESSION_COOKIE,
        cookie_attributes(forwarded_proto)
    )
}

fn sign_in(state: &State, req: &Request) -> Reply {
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
    let token = body.get("token").and_then(|v| v.as_str()).unwrap_or("");
    match state.console.sign_in(token) {
        Some((sid, who)) => {
            audit::record(&state.workspace, Some(&who), "session.sign_in", "-", audit::Outcome::Allowed);
            respond_json(&json!({ "label": who.label, "role": who.role.as_str() }))
                .with_header(session_cookie_header(&sid, req.forwarded_proto.as_deref()))
        }
        None => {
            audit::record(
                &state.workspace,
                None,
                "session.sign_in",
                "-",
                audit::Outcome::Unauthenticated,
            );
            respond_err("401 Unauthorized", "that token was not accepted")
        }
    }
}

// ── Pipeline discovery ──

/// Scan the workspace for pipeline files (a `.json` with a top-level `nodes`
/// array), skipping bookkeeping folders. Returns (absolute path, id, value).
fn discover_pipelines(workspace: &Path) -> Vec<(PathBuf, String, Value)> {
    let mut out = Vec::new();
    // One walk, shared with the catalog. Each keeping its own copy of the
    // folders to skip is how the two came to disagree: the console could open
    // a pipeline in a subfolder that the workspace graph could not see, so the
    // blast radius quietly omitted it.
    for path in duckle_duckdb_engine::catalog::discover_pipeline_files(workspace) {
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
    out.sort_by(|a, b| a.1.to_lowercase().cmp(&b.1.to_lowercase()));
    out
}

/// Map of repo item id -> human name from <workspace>/repository.json. Workspace
/// pipeline files are saved as pipelines/<id>.json with no `name` field, so the
/// dashboard must resolve the friendly name here instead of showing the internal
/// id (#108). Best-effort: a missing / unreadable repository.json yields an empty
/// map and callers fall back to the id.
fn repo_names(workspace: &Path) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let text = match std::fs::read_to_string(workspace.join("repository.json")) {
        Ok(t) => t,
        Err(_) => return map,
    };
    let items: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
    for it in items {
        if let (Some(id), Some(name)) = (
            it.get("id").and_then(|x| x.as_str()),
            it.get("name").and_then(|x| x.as_str()),
        ) {
            if !name.trim().is_empty() {
                map.insert(id.to_string(), name.to_string());
            }
        }
    }
    map
}

/// #102: apply the workspace's saved memory cap (.duckle/settings.json
/// memory_limit_mb, set from the desktop Settings UI) as DUCKLE_MEMORY_LIMIT so
/// web-editor runs honor the same per-workspace limit. An explicit
/// DUCKLE_MEMORY_LIMIT already in the launch environment wins.
fn apply_workspace_memory_limit(workspace: &Path) {
    if std::env::var("DUCKLE_MEMORY_LIMIT").map(|v| !v.is_empty()).unwrap_or(false) {
        return;
    }
    let text = match std::fs::read_to_string(workspace.join(".duckle").join("settings.json")) {
        Ok(t) => t,
        Err(_) => return,
    };
    let v: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return,
    };
    if let Some(mb) = v.get("memory_limit_mb").and_then(|x| x.as_u64()).filter(|m| *m > 0) {
        std::env::set_var("DUCKLE_MEMORY_LIMIT", format!("{}MB", mb));
    }
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
    // A broken store must not take the pipeline list down with it; the
    // Schedules view reports the reason on its own.
    let scheds = load_schedules(state).unwrap_or_else(|_| json!({}));
    let names = repo_names(&state.workspace);
    let items: Vec<Value> = discover_pipelines(&state.workspace)
        .into_iter()
        .map(|(path, id, v)| {
            let last = last_run(&state.workspace, &id);
            let sched = scheds
                .get(&id)
                .cloned()
                .unwrap_or(json!({ "enabled": false, "intervalSeconds": 0, "intervalMinutes": 0 }));
            let running = state.running.lock().map(|s| s.contains(&id)).unwrap_or(false);
            let next_at = next_run_at(&sched, last.as_ref().map(|r| r.at.as_str()));
            let name = names
                .get(&id)
                .cloned()
                .or_else(|| {
                    v.get("name").and_then(|x| x.as_str()).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
                })
                .unwrap_or_else(|| id.clone());
            json!({
                "file": rel(&state.workspace, &path),
                "id": id,
                "name": name,
                "nodeCount": v.get("nodes").and_then(|n| n.as_array()).map(|a| a.len()).unwrap_or(0),
                "edgeCount": v.get("edges").and_then(|e| e.as_array()).map(|a| a.len()).unwrap_or(0),
                "lastStatus": last.as_ref().map(|r| r.status.clone()),
                "lastAt": last.as_ref().map(|r| r.at.clone()),
                "lastDurationMs": last.as_ref().map(|r| r.duration_ms),
                "lastRows": last.as_ref().map(|r| r.rows),
                "schedule": sched,
                "running": running,
                "nextRunAt": next_at,
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
/// #259: what is known about one run id.
///
/// In memory while the run is live and for a while after it ends; then from the
/// run history on disk, which is what lets an answer survive a console restart.
/// None means no run was ever accepted under that id.
fn run_status(state: &State, run_id: &str) -> Option<Value> {
    // Copy out from under the lock rather than holding it while also touching
    // `running`: two locks held at once is how deadlocks start.
    let live = state.runs.lock().ok().and_then(|runs| {
        runs.get(run_id)
            .map(|r| (r.pipeline_id.clone(), r.started_at.clone(), r.finished.clone()))
    });
    if let Some((pipeline_id, started_at, finished)) = live {
        if let Some(done) = finished {
            return Some(json!({
                "runId": run_id,
                "pipelineId": pipeline_id,
                "state": "finished",
                "startedAt": started_at,
                // The pipeline's own status, NOT whether the call succeeded: a
                // pipeline that ran and failed still finished.
                "status": done.get("status").cloned().unwrap_or(json!("unknown")),
                "result": done,
            }));
        }
        // The run gate defaults to one at a time, so an accepted run is often
        // waiting rather than executing. `running` gains the pipeline id the
        // moment the gate is acquired, which is exactly that distinction.
        let executing = state
            .running
            .lock()
            .map(|set| set.contains(&pipeline_id))
            .unwrap_or(false);
        return Some(json!({
            "runId": run_id,
            "pipelineId": pipeline_id,
            "state": if executing { "running" } else { "queued" },
            "startedAt": started_at,
        }));
    }
    for (_path, id, _v) in discover_pipelines(&state.workspace) {
        for r in load_run_history(&state.workspace, &id) {
            if r.run_id.as_deref() == Some(run_id) {
                return Some(json!({
                    "runId": run_id,
                    "pipelineId": id,
                    "state": "finished",
                    "startedAt": r.at,
                    "status": r.status,
                    "durationMs": r.duration_ms,
                    "rows": r.rows,
                    "error": r.error,
                }));
            }
        }
    }
    None
}

/// Keep the in-memory run map bounded. Only FINISHED runs are dropped: a live
/// run has to stay, or its cancel handle goes with it.
fn forget_oldest_finished_runs(runs: &mut std::collections::HashMap<String, LiveRun>) {
    let mut done: Vec<(String, String)> = runs
        .iter()
        .filter(|(_, r)| r.finished.is_some())
        .map(|(id, r)| (r.started_at.clone(), id.clone()))
        .collect();
    if done.len() <= MAX_REMEMBERED_RUNS {
        return;
    }
    // started_at is RFC3339 UTC, so a string sort orders by time; oldest first.
    done.sort();
    let excess = done.len() - MAX_REMEMBERED_RUNS;
    for (_, id) in done.into_iter().take(excess) {
        runs.remove(&id);
    }
}

fn api_runs(state: &State, only: Option<&str>) -> Value {
    let mut rows: Vec<Value> = Vec::new();
    let names = repo_names(&state.workspace);
    for (path, id, v) in discover_pipelines(&state.workspace) {
        if let Some(want) = only {
            if want != id {
                continue;
            }
        }
        let name = names
            .get(&id)
            .cloned()
            .or_else(|| {
                v.get("name").and_then(|x| x.as_str()).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
            })
            .unwrap_or_else(|| id.clone());
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
    let doc: Value = serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;
    // Confining to the workspace is not enough on its own. This route is rated
    // for viewers, and the workspace also holds `.duckle/console-users.json`
    // and `connections/*.json`, so "any JSON inside the workspace" handed the
    // lowest role the account hashes and the stored connection payloads. A
    // pipeline is the thing with a `nodes` array; anything else is refused
    // whatever its path.
    if doc.get("nodes").and_then(Value::as_array).is_none() {
        return Err(format!("{file} is not a pipeline"));
    }
    Ok(doc)
}

/// Where a deployed pipeline is allowed to land.
///
/// [`resolve_in_workspace`] cannot be used here: it canonicalises, which needs the file to
/// exist, and the whole point of a deploy is that it may not. So the check is lexical, and
/// it is strict on purpose. This takes a name off the network and turns it into a path
/// that gets written, which is the shape of every directory-traversal bug ever filed.
fn deploy_target(workspace: &Path, name: &str) -> Result<PathBuf, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("a deployment needs a name".into());
    }
    // A drive letter or a leading separator is an absolute path in disguise.
    if name.starts_with('/') || name.starts_with('\\') || name.chars().nth(1) == Some(':') {
        return Err(format!("{name} is not a name inside the workspace"));
    }
    let mut path = workspace.to_path_buf();
    for part in name.split(['/', '\\']) {
        if part.is_empty() || part == "." || part == ".." {
            return Err(format!("{name} is not a name inside the workspace"));
        }
        path.push(part);
    }
    if path.extension().and_then(|e| e.to_str()) != Some("json") {
        path.set_extension("json");
    }
    Ok(path)
}

/// Land a pipeline an author sent, and the schedule it should eventually run on.
///
/// Split out from the handler so a test drives the same code the route runs.
fn deploy_into(workspace: &Path, body: &Value) -> Result<Value, String> {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .ok_or("a deployment needs a name")?
        .trim()
        .to_string();
    let pipeline = body.get("pipeline").ok_or("a deployment needs a pipeline")?;
    // The same rule the reader uses, for the same reason: the workspace also holds account
    // hashes and connection payloads, and a deploy writes a file into it. A pipeline is the
    // thing with a `nodes` array; anything else is refused whatever it is called.
    if pipeline.get("nodes").and_then(Value::as_array).is_none() {
        return Err(format!("{name} is not a pipeline"));
    }

    let target = deploy_target(workspace, &name)?;
    let replaced = target.exists();
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(pipeline).map_err(|e| e.to_string())?;
    // Through a temporary file in the same directory, then rename over: a scheduler tick
    // landing mid-write must never read half a pipeline.
    let tmp = target.with_extension("json.deploying");
    std::fs::write(&tmp, text).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &target).map_err(|e| format!("install {}: {e}", target.display()))?;

    // A schedule travels with the pipeline but arrives switched off. A cadence set while
    // testing on a laptop must not start firing the moment it reaches production; turning
    // it on is a separate act, and a deliberate one, needing only the operator role while
    // deploying the code needs admin.
    let scheduled = match body.get("schedule") {
        Some(sched) if !sched.is_null() => {
            let mut s = sched.clone();
            s["id"] = json!(name);
            s["enabled"] = json!(false);
            save_schedule_at(workspace, &s)?;
            json!({ "saved": true, "enabled": false })
        }
        _ => Value::Null,
    };

    Ok(json!({ "deployed": name, "replaced": replaced, "schedule": scheduled }))
}

/// Resolve a workspace-relative path and refuse anything that escapes the
/// workspace (no `..` traversal beyond the root).
/// Workspace + pipeline NAME for a backfill request, from `?file=`.
///
/// The name has to be derived exactly the way a run derives it, or the API
/// reports one folder's state while runs read another. `execute_one` resolves
/// the file inside the workspace and hands `execute_pipeline_named` the file
/// stem; this does the same, and goes through `resolve_in_workspace` so a
/// `?file=../../etc` cannot read state outside the workspace.
fn watermark_target(state: &Arc<State>, req: &Request) -> Result<(PathBuf, String), String> {
    let file = req
        .query
        .get("file")
        .ok_or_else(|| "missing file".to_string())?;
    let path = resolve_in_workspace(&state.workspace, file)?;
    let name = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or_else(|| "cannot read a pipeline name from that path".to_string())?;
    Ok((state.workspace.clone(), name))
}

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

/// The workspace graph, for the console's Catalog tab.
///
/// Reads the saved graph rather than rescanning every pipeline, because the
/// dashboard polls every few seconds and a rebuild reads every pipeline file.
/// A POST to the same path rebuilds it deliberately.
fn api_catalog(state: &State) -> Value {
    // The SAME assembly the desktop screen uses, so the two surfaces cannot
    // drift into two slightly different catalogs. It reads the saved graph and
    // reports `stale`; rebuilding stays the Operator-rated POST, because this
    // route is Viewer-rated and a viewer opening a tab must not write a file.
    match duckle_duckdb_engine::catalog::view(&state.workspace) {
        Ok(view) => json!(view),
        Err(e) => json!({ "error": e }),
    }
}

/// Where the console's own store used to live, before both products moved to
/// the workspace `schedules.json` the desktop app already used. Only read now,
/// and only to carry an existing install's schedules across once.
fn legacy_schedules_path(workspace: &Path) -> PathBuf {
    workspace.join("panel-schedules.json")
}

/// The console's view of the shared store, one entry per pipeline id:
/// `{ "enabled": bool, "intervalSeconds": n, "intervalMinutes": n, "cron": "<expr>" }`.
/// A non-empty `cron` takes precedence over the interval (#132).
///
/// `intervalSeconds` is the real stored value. `intervalMinutes` is derived and
/// kept only so an older console page still renders something sensible; it is
/// rounded and must not be written back as if it were exact, because the
/// desktop editor offers seconds as a unit and a 30-second schedule saved from
/// a minutes-only view comes back as a minute.
///
/// The store can hold several schedules for one pipeline, and file-watch
/// schedules the console cannot express at all. Those are left strictly alone:
/// this view shows the first schedule the console can represent, and a save
/// edits that same record by id rather than replacing the pipeline's entry.
/// The schedule store, or why it could not be read.
///
/// The failure is returned rather than flattened to an empty map. An empty map
/// renders as "nothing is scheduled", which is the same sentence a healthy
/// workspace with no schedules produces - so a file that would not parse was
/// indistinguishable from one that said there was nothing to do.
fn load_schedules(state: &State) -> Result<Value, String> {
    let list = duckle_duckdb_engine::schedules::load(&state.workspace).inspect_err(|e| {
        eprintln!("duckle-runner: {e}");
    })?;
    let mut out = serde_json::Map::new();
    for s in &list {
        if out.contains_key(&s.pipeline_id) {
            continue;
        }
        let (seconds, cron) = match &s.kind {
            duckle_duckdb_engine::schedules::ScheduleKind::Cron { expr } => (0, expr.clone()),
            duckle_duckdb_engine::schedules::ScheduleKind::Interval { seconds } => {
                (*seconds, String::new())
            }
            // Not expressible here; leave the pipeline looking unscheduled to
            // the console rather than misrepresenting a watch as an interval.
            duckle_duckdb_engine::schedules::ScheduleKind::FileWatch { .. } => continue,
        };
        out.insert(
            s.pipeline_id.clone(),
            json!({
                "id": s.id,
                "enabled": s.enabled,
                "intervalSeconds": seconds,
                "intervalMinutes": seconds / 60,
                "cron": cron,
                // The scheduler reads this projection rather than the store, so anything
                // it needs has to appear here. Leaving this out meant a schedule that
                // named a plan fired the pipeline it was keyed by instead, and failed
                // looking for a file nobody had written.
                "planId": s.plan_id,
            }),
        );
    }
    Ok(Value::Object(out))
}

/// Carry a pre-unification `panel-schedules.json` into the shared store.
///
/// Runs once at startup. Only pipelines with no schedule already in the shared
/// store are imported, so a workspace where the desktop app already scheduled
/// the same pipeline keeps the desktop's record rather than gaining a second
/// one. The old file is left on disk untouched: it costs nothing, and deleting
/// a user's data to tidy up is not this function's call to make.
fn migrate_legacy_schedules(workspace: &Path) {
    let legacy = legacy_schedules_path(workspace);
    let Ok(text) = std::fs::read_to_string(&legacy) else {
        return;
    };
    let Ok(Value::Object(entries)) = serde_json::from_str::<Value>(&text) else {
        return;
    };
    if entries.is_empty() {
        return;
    }
    let outcome = duckle_duckdb_engine::schedules::update(workspace, |list| {
        for (pipeline_id, cfg) in &entries {
            if list.iter().any(|s| &s.pipeline_id == pipeline_id) {
                continue;
            }
            let cron = cfg.get("cron").and_then(Value::as_str).unwrap_or("").trim();
            let minutes = cfg.get("intervalMinutes").and_then(Value::as_u64).unwrap_or(0);
            let kind = if !cron.is_empty() {
                duckle_duckdb_engine::schedules::ScheduleKind::Cron { expr: cron.to_string() }
            } else if minutes > 0 {
                duckle_duckdb_engine::schedules::ScheduleKind::Interval { seconds: minutes * 60 }
            } else {
                // Neither a cron nor a usable interval: nothing to carry over.
                continue;
            };
            list.push(duckle_duckdb_engine::schedules::Schedule {
                id: format!("panel-{pipeline_id}"),
                pipeline_id: pipeline_id.clone(),
                name: pipeline_id.clone(),
                plan_id: None,
                enabled: cfg.get("enabled").and_then(Value::as_bool).unwrap_or(false),
                kind,
                timezone: cfg
                    .get("timezone")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(str::to_string),
                exclude: cfg
                    .get("exclude")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default(),
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            });
        }
    });
    match outcome {
        Ok(_) => eprintln!(
            "duckle-runner: imported schedules from {} into schedules.json",
            legacy.display()
        ),
        Err(e) => eprintln!("duckle-runner: could not import {}: {e}", legacy.display()),
    }
}

/// The `cron` crate expects a 6- or 7-field expression (seconds first). Accept a
/// standard 5-field cron ("min hour dom mon dow") by prepending a "0 " seconds
/// field; pass a 6/7-field expression through. Returns None for any other field
/// count so a malformed expression is rejected rather than silently ignored.
fn normalize_cron(expr: &str) -> Option<String> {
    match expr.split_whitespace().count() {
        5 => Some(format!("0 {}", expr)),
        6 | 7 => Some(expr.to_string()),
        _ => None,
    }
}

/// The next time an enabled schedule is expected to fire, as an RFC3339 string
/// for the console to display beside "last run" (discussion #155). Cron uses the
/// exact next occurrence in local time; interval mode estimates from the last
/// run (or now) rolled forward by whole intervals. Returns None when the
/// schedule is disabled or has neither a cron nor a positive interval.
fn next_run_at(sched: &Value, last_at: Option<&str>) -> Option<String> {
    if !sched.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    let cron = sched.get("cron").and_then(Value::as_str).unwrap_or("").trim();
    if !cron.is_empty() {
        // #318: through the shared evaluator, so this console and the embedded
        // scheduler read the same expression as the same instant. They did not
        // once (#194), and the way that showed up was a job firing at two
        // different times depending on which surface owned it.
        let tz = sched.get("timezone").and_then(Value::as_str);
        let zone = duckle_duckdb_engine::cronzone::resolve_zone(tz).ok()?;
        let exclude: duckle_duckdb_engine::cronzone::Exclusions = sched
            .get("exclude")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let (occ, _skipped) = duckle_duckdb_engine::cronzone::next_after_excluding(
            cron,
            &zone,
            &exclude,
            chrono::Utc::now(),
        )
        .ok()?;
        return occ.map(|o| o.at.to_rfc3339());
    }
    let interval = sched.get("intervalSeconds").and_then(Value::as_u64).unwrap_or(0);
    if interval == 0 {
        return None;
    }
    let step = chrono::Duration::seconds(interval as i64);
    let now = chrono::Utc::now();
    let mut next = last_at
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc) + step)
        .unwrap_or(now + step);
    // A steady interval schedule fires every `interval`; roll past any missed
    // slots so the shown time is the next one still in the future.
    while next <= now {
        next += step;
    }
    Some(next.to_rfc3339())
}

fn save_schedule(state: &State, body: &Value) -> Result<Value, String> {
    save_schedule_at(&state.workspace, body)
}

/// The store half of saving a schedule, split out so a test can drive the same
/// code the handler runs rather than a copy of its logic.
fn save_schedule_at(workspace: &Path, body: &Value) -> Result<Value, String> {
    let id = body.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
    let enabled = body.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let interval = body.get("intervalMinutes").and_then(|v| v.as_u64()).unwrap_or(0);
    let cron = body.get("cron").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
    // A schedule fires a plan when it names one, and a single pipeline otherwise, which is
    // what every schedule saved before plans existed means.
    //
    // Three answers, not two. No `planId` key means the caller is not talking about plans -
    // an older client, the desktop app, the Schedules tab toggling one on - and whatever the
    // schedule already runs is left alone. An empty one means "a pipeline, not a plan", and
    // is the only way to take a plan off a schedule.
    let plan_id: Option<Option<String>> = body.get("planId").map(|v| {
        v.as_str().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
    });
    // Validate a supplied cron expression up front so a bad one is rejected with
    // a clear message instead of silently never firing (#132).
    if !cron.is_empty()
        && normalize_cron(&cron).and_then(|e| e.parse::<cron::Schedule>().ok()).is_none()
    {
        return Err("Invalid cron expression (use 5 fields, e.g. `0 9 * * 1`)".to_string());
    }
    // #318: a zone typo is refused here, in front of whoever typed it, rather
    // than becoming a job that quietly runs on the container's UTC clock.
    if let Some(tz) = body.get("timezone").and_then(|v| v.as_str()) {
        duckle_duckdb_engine::cronzone::resolve_zone(Some(tz))?;
    }
    let timezone: Option<String> = body
        .get("timezone")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    // #296: a maintenance calendar is checked where it is written. A misspelled
    // weekday excludes nothing, which looks exactly like no exclusion at all
    // until the day it was supposed to cover arrives.
    let exclude: duckle_duckdb_engine::cronzone::Exclusions = match body.get("exclude").cloned() {
        Some(v) => serde_json::from_value(v)
            .map_err(|e| format!("Invalid exclude calendar: {e}"))?,
        None => Default::default(),
    };
    exclude.validate()?;
    // Seconds are what the store holds. A console that sends only minutes is
    // still honoured, but one that echoes back the intervalSeconds it was given
    // keeps a sub-minute schedule exactly as the desktop editor set it.
    let seconds = match body.get("intervalSeconds").and_then(|v| v.as_u64()) {
        Some(s) => s,
        None => interval.saturating_mul(60),
    };
    // An enabled schedule with neither a cron nor a positive interval is not a
    // schedule. The runner skips it, but the desktop scheduler computes
    // `now + 0s` as its next run and fires it on every tick, forever. Refusing
    // it here is better than either behaviour, and better than the console's
    // empty interval box quietly becoming "run continuously".
    if enabled && cron.is_empty() && seconds == 0 {
        return Err(
            "An enabled schedule needs a cron expression or an interval greater than zero".into(),
        );
    }
    let kind = if !cron.is_empty() {
        duckle_duckdb_engine::schedules::ScheduleKind::Cron { expr: cron }
    } else {
        duckle_duckdb_engine::schedules::ScheduleKind::Interval { seconds }
    };
    let pipeline_id = id.to_string();
    duckle_duckdb_engine::schedules::update(workspace, move |list| {
        // Edit the record this pipeline already has rather than adding another,
        // so saving from the console does not quietly double a schedule the
        // desktop app created. A file-watch record is not one the console can
        // edit, so it is skipped and a new record is added alongside it.
        let existing = list.iter_mut().find(|s| {
            s.pipeline_id == pipeline_id
                && !matches!(s.kind, duckle_duckdb_engine::schedules::ScheduleKind::FileWatch { .. })
        });
        match existing {
            Some(s) => {
                s.enabled = enabled;
                s.kind = kind;
                if let Some(wanted) = plan_id.clone() {
                    s.plan_id = wanted;
                }
                // A changed trigger invalidates the time this process armed.
                s.next_run_at = None;
            }
            None => list.push(duckle_duckdb_engine::schedules::Schedule {
                id: format!("panel-{pipeline_id}"),
                pipeline_id: pipeline_id.clone(),
                name: pipeline_id.clone(),
                enabled,
                plan_id: plan_id.clone().flatten(),
                kind,
                timezone: timezone.clone(),
                exclude: exclude.clone(),
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            }),
        }
    })
    .map_err(|e| format!("write schedules: {}", e))?;
    Ok(json!({ "ok": true }))
}

// ── Execution ──

/// Parse the optional `params` object from a run request into a {name: value}
/// map, keeping only non-empty string-ish values (a blank field means "use the
/// context default", so it is dropped rather than overriding with an empty value).
fn parse_run_params(v: Option<&Value>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(Value::Object(m)) = v {
        for (k, val) in m {
            let s = match val {
                Value::String(s) => s.clone(),
                Value::Null => continue,
                other => other.to_string(),
            };
            if !s.is_empty() {
                out.insert(k.clone(), s);
            }
        }
    }
    out
}

/// List the `${...}` parameters a pipeline file exposes, for the dashboard's
/// run-parameters form. Reads the file and delegates to the engine's discovery.
fn discover_pipeline_params(state: &State, file: &str) -> Result<Vec<String>, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let doc: PipelineDoc =
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;
    Ok(duckle_duckdb_engine::context::discover_parameters(&doc))
}

/// Run one pipeline by its workspace-relative file path, end to end: resolve
/// env/time placeholders (as the runner does), execute through the engine,
/// append a run-history record, and return a result summary. Serialized by the
/// run lock so a scheduled run never overlaps a manual one.
/// Removes a pipeline id from the running set when the run ends, no matter how
/// (normal return, `?` error, or panic). Paired with the insert in execute_one.
struct RunningGuard<'a> {
    set: &'a Mutex<std::collections::HashSet<String>>,
    id: String,
}
impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.id);
        }
    }
}

/// Fire a scheduled pipeline, unless another Duckle process already is.
///
/// The in-memory `last_fired` / `cron_next` maps below only stop THIS process
/// double-firing. A desktop app open on the same workspace runs its own
/// scheduler and knows nothing about this one, so the guard has to live on
/// disk. Skipping is the right response to a clash: the next tick comes round
/// anyway, and two runs of one pipeline race on the sink and on the
/// `xf.incremental` watermark, which is how a load quietly skips rows.
fn run_scheduled(state: &State, id: &str, file: &str) {
    let _lock = match duckle_duckdb_engine::runlock::try_acquire(&state.workspace, id) {
        Some(l) => l,
        None => {
            eprintln!("duckle-runner: scheduled {id} already running elsewhere, skipped");
            return;
        }
    };
    match execute_one(state, file, "scheduled", &HashMap::new()) {
        Ok(v) => {
            let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("?");
            eprintln!("duckle-runner: scheduled {} -> {}", id, status);
            record_schedule_outcome(
                state,
                id,
                status,
                v.get("durationMs").and_then(|d| d.as_u64()).unwrap_or(0),
                v.get("error").and_then(|e| e.as_str()).map(str::to_string),
            );
        }
        Err(e) => {
            eprintln!("duckle-runner: scheduled {} failed: {}", id, e);
            // A run that could not even start is still an outcome, and leaving
            // it out is what makes a schedule look like it never fired.
            record_schedule_outcome(state, id, "error", 0, Some(e.clone()));
            alerts_notify(state, id, "error", 0, Some(e));
        }
    }
}

/// Write a scheduled run's outcome back to the shared schedule store.
///
/// Only the desktop app used to do this, and once both products moved to one
/// `schedules.json` that left a runner-only deployment showing "never run"
/// forever while it was in fact running fine every hour - and made any
/// staleness check built on `lastRunAt` useless.
fn record_schedule_outcome(
    state: &State,
    pipeline_id: &str,
    status: &str,
    duration_ms: u64,
    error: Option<String>,
) {
    let (pipeline_id, status) = (pipeline_id.to_string(), status.to_string());
    let outcome = duckle_duckdb_engine::schedules::update(&state.workspace, move |list| {
        for s in list.iter_mut().filter(|s| s.pipeline_id == pipeline_id) {
            s.last_run_at = Some(chrono::Utc::now());
            s.last_run_status = Some(status.clone());
            s.last_run_duration_ms = Some(duration_ms);
            s.last_run_error = error.clone();
        }
    });
    if let Err(e) = outcome {
        eprintln!("duckle-runner: could not record the run against its schedule: {e}");
    }
}

/// Raise an alert for something that happened outside `execute_one`, which is
/// where the ordinary path already reports from.
fn alerts_notify(state: &State, pipeline_id: &str, status: &str, duration_ms: u64, error: Option<String>) {
    let result = duckle_duckdb_engine::RunResult {
        cache_keys: Default::default(),
        status: status.to_string(),
        // Synthesised for an alert about something outside a run.
        unchanged: false,
        incomplete: false,
        incomplete_reason: None,
        artifacts: Vec::new(),
        artifacts_truncated: false,
        duration_ms,
        nodes: Default::default(),
        preview: Vec::new(),
        category: error.as_deref().map(duckle_duckdb_engine::error_category::categorize_error)
            .map(str::to_string),
        error,
    };
    duckle_duckdb_engine::alerts::notify(&state.workspace, pipeline_id, &result);
}

/// A schedule came due and its pipeline is not there.
///
/// This used to do nothing at all: the fire site was `if let Some(path) =
/// pipes.get(id)`, so renaming or deleting a pipeline turned its schedule into
/// a no-op that reported nothing, forever. That is the worst shape a scheduler
/// failure can take, because everything looks healthy while the data quietly
/// stops arriving.
fn report_missing_pipeline(state: &State, id: &str) {
    let msg = format!(
        "scheduled pipeline '{id}' has no pipeline file in the workspace; \
         it was probably renamed, moved or deleted"
    );
    eprintln!("duckle-runner: {msg}");
    record_schedule_outcome(state, id, "error", 0, Some(msg.clone()));
    alerts_notify(state, id, "error", 0, Some(msg));
}

fn execute_one(
    state: &State,
    file: &str,
    trigger: &str,
    params: &HashMap<String, String>,
) -> Result<Value, String> {
    execute_one_with(state, file, trigger, params, None, None)
}

/// #259: the body of `execute_one`, with the engine handle and the run id
/// supplied by the caller.
///
/// An asynchronous run registers its engine handle BEFORE the run starts, or a
/// cancel arriving early has nothing to cancel, and it records the id it was
/// accepted under so the run can still be found after a restart. Both callers
/// otherwise take exactly the same path.
fn execute_one_with(
    state: &State,
    file: &str,
    trigger: &str,
    params: &HashMap<String, String>,
    engine: Option<DuckdbEngine>,
    run_id: Option<&str>,
) -> Result<Value, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;

    let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "pipeline".into());


    // Mark this pipeline as running for the duration of the execution so the
    // console can show a live "Running" status (discussion #155). The guard
    // clears it on every exit path, including the `?` early returns below.
    if let Ok(mut set) = state.running.lock() {
        set.insert(id.clone());
    }
    let _running = RunningGuard { set: &state.running, id: id.clone() };

    // Same placeholder resolution as `duckle-runner run`: saved Salesforce
    // connection refs first (#166 stage 2, so a connection field stored as
    // ${ENV:...} still expands), then ${ENV:KEY} secrets, then the dynamic
    // ${date}/${datetime}/... builtins.
    duckle_secrets::resolve_connection_refs(&state.workspace, &mut doc.nodes)?;
    let env_file = state.workspace.join("secrets.env");
    crate::apply_env_pass(&mut doc, &state.workspace, &env_file)?;
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    // Per-run input parameters from the dashboard (issue #127) override the
    // static workspace context for this run; applied before the context pass so a
    // supplied value wins and any unset ${KEY} still resolves from the context.
    // Refused rather than substituted when a parameter would inject shell syntax
    // into an executed property: /api/run needs only operator, and quietly allowing
    // that would hand an operator the execution /api/deploy reserves for admin.
    // #309: what the run was actually given, secrets already replaced. Taken
    // from the substitution boundary rather than from `params` here, because
    // that is the only place that knows the effective set (declared defaults
    // included) and which of them the pipeline declared secret.
    // #317: named, so the receipt can later say where a value came from. There
    // is one source on this path today, which is why nothing here ever reports
    // an override - but a schedule that binds parameters is the obvious second,
    // and the difference between a deliberate override and an accidental
    // double binding has to be recoverable the first time it happens, not
    // after someone notices it did not get recorded.
    let supplied: Vec<duckle_duckdb_engine::params::Supplied> = params
        .iter()
        .map(|(name, value)| duckle_duckdb_engine::params::Supplied {
            name: name.clone(),
            value: value.clone(),
            source: "run input".to_string(),
        })
        .collect();
    let (recorded_params, parameter_sources) =
        duckle_duckdb_engine::context::apply_params_from(&mut doc, &supplied)?;
    // Match the web cmd paths and headless `duckle-runner --pipeline`: resolve
    // ${workspace}/${projectroot} and workspace-relative file paths before run,
    // so file-loaded pipelines (manual /api/run + scheduled runs) work too.
    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);

    let engine = engine.unwrap_or_else(|| DuckdbEngine::new(state.duckdb.clone()));
    // #259: every console execution is addressable, not only the async one.
    // `execute_one` passed None, so a synchronous run, a scheduled step and a
    // plan step each recorded no id at all - which is what made "which run was
    // that?" unanswerable for most of the ways a run actually starts. Minted
    // here because all of them come through this function.
    let owned_id = run_id
        .map(str::to_string)
        .unwrap_or_else(|| duckle_duckdb_engine::retry::new_run_id(&id, trigger));
    let hash = duckle_duckdb_engine::retry::pipeline_hash(&doc);
    let receipt = duckle_duckdb_engine::retry::begin(
        &state.workspace,
        &owned_id,
        trigger,
        &id,
        &path.display().to_string(),
        &hash,
        None,
    );
    // Written again straight away rather than only at `finish`: a run that is
    // killed still leaves a receipt, and "what was that run given?" is exactly
    // the question asked of a run that did not come back.
    let receipt = duckle_duckdb_engine::retry::RunReceipt {
        parameters: recorded_params,
        parameter_sources,
        // #307: the external components this pipeline names, with their hashes.
        components: duckle_duckdb_engine::plugin::used_by(
            &state.workspace,
            &serde_json::to_value(&doc).unwrap_or_default(),
        ),
        ..receipt
    };
    let _ = duckle_duckdb_engine::retry::write(&state.workspace, &receipt);
    // #259: log lines carry the id the receipt was written with.
    // #289: the receipt exists BEFORE the wait, so an API caller has a durable
    // id to inspect or cancel while capacity is unavailable rather than holding
    // the request open. `queued` is a real state - nothing has started, so
    // there is nothing to undo if it is cancelled here.
    let mut receipt = receipt;
    let asked = state.run_lock.pool_for(&doc.resource_pool);
    if state.run_lock.is_saturated(&asked) {
        duckle_duckdb_engine::retry::enqueue(
            &state.workspace,
            &mut receipt,
            &asked,
            "resource_pool_capacity",
        );
    }
    let (_guard, pool, queued_ms) = state.run_lock.acquire(&doc.resource_pool);
    duckle_duckdb_engine::retry::admitted(&state.workspace, &mut receipt, queued_ms);
    let receipt = duckle_duckdb_engine::retry::RunReceipt { resource_pool: Some(pool), ..receipt };

    let result = engine.with_run_id(&receipt.run_id).execute_pipeline_named(&doc, &id);
    duckle_duckdb_engine::retry::finish(
        &state.workspace,
        receipt,
        &result.status,
        duckle_duckdb_engine::retry::nodes_of(&result),
    );

    let mut record = RunRecord::from_result_in(&state.workspace, &id, &result, trigger);
    // #259: stamp the id the caller was handed, so a finished async run is
    // still answerable once it has left memory.
    record.run_id = Some(owned_id);
    let _ = append_run_record(&state.workspace, &id, record);
    // After the run is recorded, so an unreachable channel can never cost a
    // run its history entry, and never changes the outcome reported below.
    duckle_duckdb_engine::alerts::notify(&state.workspace, &id, &result);

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

/// Turn what `execute_one` returns into what a plan needs to know.
///
/// The two disagree about what a `Result` means, and the disagreement is easy to miss.
/// `execute_one` answers `Err` only when the run could not be started at all; a pipeline
/// that ran and failed comes back as `Ok` carrying `"status": "error"`. A plan asks a
/// simpler question - did this pipeline work - so the status has to be read, not just the
/// `Result`. Reading only the `Result` reported a plan of three failed pipelines as `ok` and
/// let every later step run against data that was never produced.
fn plan_step_outcome(result: Result<Value, String>) -> Result<(), String> {
    let value = result?;
    if value.get("status").and_then(|v| v.as_str()) == Some("error") {
        return Err(value
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("the pipeline failed")
            .to_string());
    }
    Ok(())
}

// ── Scheduler ──

/// Background loop: every 30s, run any enabled pipeline whose schedule is due.
/// Interval schedules are tracked in-memory from process start (first run fires
/// one interval after boot). Cron schedules are evaluated in LOCAL time so
/// "0 9 * * *" means 9am local, matching how the dashboard displays run times
/// (#132). Both keep next-run state in-memory, so a restart re-arms from the
/// next occurrence with no surprise burst of catch-up runs.
/// What this tick should do with a cron schedule, and what to arm next.
///
/// Returned rather than performed, so the decision can be tested without a
/// thread, a workspace and a wall clock.
///
/// The armed occurrence is remembered together with the expression it came
/// from. Keyed by schedule id alone, an edited cron expression did nothing
/// until the OLD occurrence came round: a schedule moved from 03:00 to 09:00
/// skipped 09:00 entirely and then fired at 03:00 the next morning, at the one
/// time it had just been moved away from.
fn cron_decision(
    armed: Option<&(String, chrono::DateTime<chrono::Local>)>,
    expr: &str,
    sched: &cron::Schedule,
    now: chrono::DateTime<chrono::Local>,
) -> (bool, Option<(String, chrono::DateTime<chrono::Local>)>) {
    let next_after_now = || sched.after(&now).next().map(|t| (expr.to_string(), t));
    match armed {
        // Armed from this very expression, and its moment has come.
        Some((e, at)) if e == expr && now >= *at => (true, next_after_now()),
        // Armed from this expression, not due yet. Left exactly as it is:
        // re-arming here would push the occurrence away on every tick.
        Some((e, at)) if e == expr => (false, Some((e.clone(), *at))),
        // Never seen before, or the expression changed underneath us. Arm what
        // it says NOW, and do not fire: the edit is not itself an occurrence.
        _ => (false, next_after_now()),
    }
}

/// Fire one schedule that has come due.
///
/// Both triggers, cron and interval, arrive here, and both used to carry their own copy of
/// the lookup. One copy means a schedule that names a plan is handled once rather than in
/// two places that can drift.
fn fire_schedule(state: &State, id: &str, cfg: &Value, pipes: &HashMap<String, PathBuf>) {
    let plan_id = cfg
        .get("planId")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(plan_id) = plan_id {
        fire_plan(state, plan_id);
        return;
    }
    match pipes.get(id) {
        Some(path) => {
            let file = rel(&state.workspace, path);
            run_scheduled(state, id, &file);
        }
        None => report_missing_pipeline(state, id),
    }
}

/// Run a whole plan because its schedule came due.
///
/// Each pipeline still goes through the ordinary run path, so run history shows them
/// individually and a failure at three in the morning names the pipeline rather than the
/// plan. What is logged here is the shape of the attempt, which run history cannot show.
fn fire_plan(state: &State, plan_id: &str) {
    let plan = match duckle_duckdb_engine::plans::load(&state.workspace) {
        Ok(list) => list.into_iter().find(|p| p.id == plan_id),
        Err(e) => {
            eprintln!("duckle-runner: scheduled plan '{plan_id}': {e}");
            return;
        }
    };
    let Some(plan) = plan else {
        // The same shape of problem as a schedule pointing at a deleted pipeline: said
        // once, on the tick, rather than silently doing nothing forever.
        eprintln!("duckle-runner: schedule fires plan '{plan_id}', which does not exist");
        return;
    };
    let params = HashMap::new();
    let outcome = duckle_duckdb_engine::plans::execute(&plan, |pipeline| {
        plan_step_outcome(execute_one(
            state,
            &duckle_duckdb_engine::plans::step_pipeline_file(pipeline),
            "schedule",
            &params,
        ))
    });
    let ran = outcome
        .steps
        .iter()
        .flat_map(|s| s.pipelines.iter())
        .filter(|p| p.status != "skipped")
        .count();
    eprintln!(
        "duckle-runner: plan '{}' {} ({} of {} pipelines ran)",
        plan.id,
        outcome.status,
        ran,
        outcome.steps.iter().map(|s| s.pipelines.len()).sum::<usize>()
    );
}

/// How often freshness is judged (#304).
///
/// Its own cadence rather than the scheduler's: an asset's age does not change
/// meaningfully between two scheduler ticks, and evaluating reads run history
/// for every asset, so doing it every tick would spend real work to learn
/// nothing. A minute is far finer than any freshness limit anyone declares -
/// they are written in hours - and coarse enough to be free.
const FRESHNESS_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

fn spawn_scheduler(state: Arc<State>) {
    std::thread::spawn(move || {
        let mut last_fired: HashMap<String, Instant> = HashMap::new();
        // The armed occurrence AND the expression it came from. See cron_decision.
        let mut cron_next: HashMap<String, (String, chrono::DateTime<chrono::Local>)> =
            HashMap::new();
        // #304: checked on a clock, because the failures freshness exists for
        // produce no run at all - a schedule switched off, a server that was
        // down, a source that stopped publishing. Nothing here can be reached
        // from the end of a run.
        let mut last_freshness: Option<Instant> = None;
        let freshness_running = Arc::new(std::sync::atomic::AtomicBool::new(false));
        loop {
            std::thread::sleep(state.tick_interval);
            if last_freshness.is_none_or(|t| t.elapsed() >= FRESHNESS_EVERY) {
                last_freshness = Some(Instant::now());
                // Off the scheduler's thread, so a slow evaluation delays no
                // schedule, and guarded so two can never overlap on a workspace
                // where it takes longer than the interval.
                let busy = Arc::clone(&freshness_running);
                if !busy.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    let ws = state.workspace.clone();
                    std::thread::spawn(move || {
                        let (assets, sent) = duckle_duckdb_engine::sla::check_and_alert(
                            &ws,
                            chrono::Utc::now(),
                        );
                        let stale: Vec<&str> = assets
                            .iter()
                            .filter(|a| a.state == duckle_duckdb_engine::sla::State::Stale)
                            .map(|a| a.asset.as_str())
                            .collect();
                        if !stale.is_empty() {
                            eprintln!(
                                "duckle: {} asset(s) past their freshness limit ({} alert(s) sent): {}",
                                stale.len(),
                                sent,
                                stale.join(", ")
                            );
                        }
                        busy.store(false, std::sync::atomic::Ordering::SeqCst);
                    });
                }
            }
            let scheds = match load_schedules(&state) {
                Ok(v) => v,
                // Already reported to stderr by load_schedules. Firing nothing
                // is the safe answer to a store that will not parse.
                Err(_) => continue,
            };
            let obj = match scheds.as_object() {
                Some(o) => o,
                None => continue,
            };
            // Map id -> its file path for the enabled, due ones.
            let pipes: HashMap<String, PathBuf> =
                discover_pipelines(&state.workspace).into_iter().map(|(p, id, _)| (id, p)).collect();
            for (id, cfg) in obj {
                // Cron schedule (local time) takes precedence over interval when
                // set (#132). Kept separate so the interval path below is unchanged.
                {
                    let enabled = cfg.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                    let cron = cfg.get("cron").and_then(|v| v.as_str()).unwrap_or("").trim();
                    if enabled && !cron.is_empty() {
                        last_fired.remove(id);
                        match normalize_cron(cron).and_then(|e| e.parse::<cron::Schedule>().ok()) {
                            None => {
                                cron_next.remove(id);
                            }
                            Some(sched) => {
                                let (fire, rearm) = cron_decision(
                                    cron_next.get(id),
                                    cron,
                                    &sched,
                                    chrono::Local::now(),
                                );
                                if fire {
                                    fire_schedule(&state, id, cfg, &pipes);
                                }
                                match rearm {
                                    Some(next) => {
                                        cron_next.insert(id.clone(), next);
                                    }
                                    // No future occurrence at all. Forgetting
                                    // it leaves the schedule armed by nothing
                                    // rather than due every tick.
                                    None => {
                                        cron_next.remove(id);
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    // Not a cron schedule: drop any stale cron state and fall
                    // through to the interval logic below.
                    cron_next.remove(id);
                }
                let enabled = cfg.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                // Seconds, not minutes: the shared store keeps the exact value
                // the desktop editor set, and rounding it here would turn a
                // 30-second schedule into one that never fires.
                let seconds = cfg.get("intervalSeconds").and_then(|v| v.as_u64()).unwrap_or(0);
                if !enabled || seconds == 0 {
                    last_fired.remove(id);
                    continue;
                }
                let interval = Duration::from_secs(seconds);
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
                    // The clock is re-armed whether or not the pipeline is
                    // there. It used to be advanced only inside the match, so a
                    // missing pipeline left this schedule permanently due and it
                    // re-evaluated on every tick, silently, for as long as the
                    // process lived.
                    last_fired.insert(id.clone(), now);
                    fire_schedule(&state, id, cfg, &pipes);
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------------
// HTTP transport for the console.
// ---------------------------------------------------------------------------------

use axum::response::IntoResponse as _;

impl axum::response::IntoResponse for Reply {
    fn into_response(self) -> axum::response::Response {
        let code = axum::http::StatusCode::from_u16(self.code())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        let mut builder = axum::response::Response::builder()
            .status(code)
            .header(axum::http::header::CONTENT_TYPE, self.content_type.clone());
        for line in &self.headers {
            if let Some((name, value)) = line.split_once(": ") {
                builder = builder.header(name, value);
            }
        }
        builder
            .body(axum::body::Body::from(self.body))
            .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

/// A caller who has already been authorised for the route they asked for.
///
/// This is the whole reason for the framework. A handler that takes a `Caller` cannot run
/// for a request that was refused, because the refusal happens before the handler is
/// entered. The permission table still decides what each route needs; what changes is that
/// there is no longer a call site at which to forget to consult it.
pub struct Caller(pub console_auth::Identity);

// axum 0.8 dropped its async_trait re-export: the trait now uses a native
// async fn, so the attribute is not only unnecessary but absent.
impl axum::extract::FromRequestParts<Arc<State>> for Caller {
    type Rejection = axum::response::Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        let req = request_from_parts(parts, Vec::new());
        match authorize(&req, state) {
            Access::Caller(who) => Ok(Caller(who)),
            Access::Refused(reply) => Err(reply.into_response()),
            // A public route is served by its own handler, which takes no Caller. Reaching
            // here would mean one was wired to an authorised handler by mistake.
            Access::Public => Err(
                respond_err("500 Internal Server Error", "public route reached an authorised handler")
                    .into_response(),
            ),
        }
    }
}

/// Rebuild the request shape the router already understands from axum's parts.
///
/// One conversion, so the handlers and every test keep speaking the same language.
fn request_from_parts(parts: &axum::http::request::Parts, body: Vec<u8>) -> Request {
    let uri = &parts.uri;
    let mut query = HashMap::new();
    if let Some(q) = uri.query() {
        for pair in q.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            query.insert(url_decode(k), url_decode(v));
        }
    }
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    Request {
        method: parts.method.as_str().to_string(),
        path: uri.path().to_string(),
        query,
        origin: header("origin"),
        host: header("host"),
        authorization: header("authorization"),
        cookie: header("cookie"),
        forwarded_proto: header("x-forwarded-proto"),
        body,
    }
}

/// Everything that needs a credential. The `Caller` argument is the enforcement.
async fn console_authed(
    caller: Caller,
    axum::extract::State(state): axum::extract::State<Arc<State>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, MAX_BODY).await.unwrap_or_default();
    let req = request_from_parts(&parts, bytes.to_vec());
    // A handler can run a pipeline, which blocks for as long as the pipeline takes, so it
    // does not belong on a thread the runtime needs for accepting connections.
    match tokio::task::spawn_blocking(move || dispatch_console(&req, &state, caller.0)).await {
        Ok(reply) => reply.into_response(),
        Err(e) => respond_err("500 Internal Server Error", &format!("handler panicked: {e}"))
            .into_response(),
    }
}

/// The routes reachable without a credential, which take no `Caller` and so cannot
/// accidentally be treated as authorised.
async fn console_public(
    axum::extract::State(state): axum::extract::State<Arc<State>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, MAX_BODY).await.unwrap_or_default();
    let req = request_from_parts(&parts, bytes.to_vec());
    match authorize(&req, &state) {
        Access::Refused(reply) => reply.into_response(),
        _ => match tokio::task::spawn_blocking(move || public_route(&req, &state)).await {
            Ok(reply) => reply.into_response(),
            Err(_) => {
                respond_err("500 Internal Server Error", "request failed").into_response()
            }
        },
    }
}

fn console_router(state: Arc<State>) -> axum::Router {
    // Built FROM `PUBLIC_ROUTES` rather than beside it. Listing them twice is
    // how `/readyz` came to be public to the authoriser and unknown to the
    // router: the fallback then ran the authorised handler, whose extractor
    // sees a public route and can only answer 500. Every hand-rolled test
    // passed, because those go through `route_console` and never touch this.
    //
    // Setup is reachable without a credential because there is not yet a
    // credential to have. `claim` refuses once the console has an owner, so it
    // cannot be replayed.
    let mut router = axum::Router::new();
    for (method, path) in PUBLIC_ROUTES {
        let handler = match method {
            "POST" => axum::routing::post(console_public),
            _ => axum::routing::get(console_public),
        };
        // `.fallback` on the METHOD router, not just on the Router. A path
        // registered for one method answers 405 to every other method itself,
        // and the Router-level fallback never runs - which took `DELETE
        // /api/session` out of service and made the console's Sign out button
        // a no-op, because `/api/session` is public for POST and authorised
        // for DELETE.
        router = router.route(path, handler.fallback(console_authed));
    }
    router
        // Everything else goes through the extractor, so a route added later is
        // authorised by existing, rather than by someone remembering to ask.
        .fallback(console_authed)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::{
        connection_secret_cmd, console_auth, cookie_attributes, cron_decision, deploy_into,
        deploy_target, is_public_route, load_schedules, plan_step_outcome, route_console,
        scheduler_notice, Request,
        migrate_legacy_schedules, normalize_cron, read_pipeline_file, read_request,
        save_schedule_at, web_gate, confine_to_workspace, RunGate, State, WebState, HEALTH_PATH, MAX_BODY,
        OIDC_CALLBACK_PATH, OIDC_LOGIN_PATH,
        Gates,
        route_web,
    };
    use std::sync::Mutex;
    use duckle_duckdb_engine::schedules::{self, ScheduleKind};

    fn pipeline() -> serde_json::Value {
        serde_json::json!({ "name": "Orders", "nodes": [], "edges": [] })
    }

    /// The point of the whole feature: a pipeline authored somewhere else arrives and is
    /// there afterwards.
    #[test]
    fn a_deployed_pipeline_lands_in_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let out = deploy_into(ws, &serde_json::json!({
            "name": "orders-load",
            "pipeline": pipeline(),
        }))
        .expect("deploys");

        assert_eq!(out["replaced"], false, "nothing was there before");
        let landed: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(ws.join("orders-load.json")).expect("written"),
        )
        .unwrap();
        assert_eq!(landed["name"], "Orders");
    }

    /// Sourav's call: a schedule travels with the pipeline but arrives switched off, so a
    /// cadence someone set while testing on a laptop cannot start firing in production the
    /// moment it lands. Turning it on is a separate, deliberate act.
    #[test]
    fn a_deployed_schedule_arrives_disabled_even_when_it_was_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        deploy_into(ws, &serde_json::json!({
            "name": "orders-load",
            "pipeline": pipeline(),
            "schedule": { "enabled": true, "intervalMinutes": 30 },
        }))
        .expect("deploys");

        let saved = schedules::load(ws).expect("store readable");
        assert_eq!(saved.len(), 1, "the schedule should travel");
        assert!(!saved[0].enabled, "it must arrive switched off");
    }

    /// Deploying again is an update, and saying so is the difference between a deploy and
    /// an accident.
    #[test]
    fn deploying_over_an_existing_pipeline_says_it_replaced_one() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let body = serde_json::json!({ "name": "orders-load", "pipeline": pipeline() });

        deploy_into(ws, &body).expect("first");
        let second = deploy_into(ws, &body).expect("second");
        assert_eq!(second["replaced"], true);
    }

    /// This route takes a name off the network and writes a file at it, which is the shape
    /// of every directory traversal bug ever filed.
    #[test]
    fn a_deployment_cannot_be_written_outside_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        for name in [
            "../escaped",
            "../../etc/cron",
            "nested/../../out",
            "/abs/path",
            r"\abs\path",
            r"C:\Windows\evil",
            "",
            "   ",
        ] {
            assert!(
                deploy_target(ws, name).is_err(),
                "{name:?} was accepted as a deployment target"
            );
        }
        // A plain name and a nested one are both fine.
        assert!(deploy_target(ws, "orders").is_ok());
        assert!(deploy_target(ws, "team/orders.json").is_ok());
    }

    /// A deploy writes into the workspace, which also holds account hashes and connection
    /// payloads. Only something that is actually a pipeline may be written.
    #[test]
    fn a_body_that_is_not_a_pipeline_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let err = deploy_into(ws, &serde_json::json!({
            "name": "console-users",
            "pipeline": { "label": "not a pipeline" },
        }))
        .expect_err("must refuse");
        assert!(err.contains("not a pipeline"), "unhelpful refusal: {err}");
        assert!(!ws.join("console-users.json").exists(), "it was written anyway");
    }

    /// The scheduler reads a projection of the schedule store, not the store, so anything
    /// it needs has to be in the projection. Leaving the plan out meant a schedule that
    /// named a plan fired the pipeline it happened to be keyed by, and failed looking for a
    /// file nobody had written.
    #[test]
    fn a_schedule_that_names_a_plan_tells_the_scheduler_so() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        save_schedule_at(
            &ws,
            &serde_json::json!({
                "id": "nightly",
                "planId": "nightly",
                "enabled": true,
                "intervalSeconds": 60
            }),
        )
        .expect("saves");

        let state = guarded_state(&ws);
        let seen = load_schedules(&state).expect("projection reads");
        let entry = seen.get("nightly").expect("the schedule is there");
        assert_eq!(
            entry.get("planId").and_then(|v| v.as_str()),
            Some("nightly"),
            "the scheduler cannot fire a plan it is never told about"
        );
    }

    /// A pipeline that fails must fail its step, and stop the steps after it.
    ///
    /// `execute_one` answers `Ok` for a run that happened and `Err` only for one that could
    /// not be started, so a failed pipeline comes back as `Ok(status: "error")`. Reading
    /// only the `Result` therefore reported a plan whose every pipeline failed as a plan
    /// that worked, and - much worse - let every later step run against data the failed step
    /// was supposed to produce, which is the one thing a plan exists to prevent.
    ///
    /// Seen for real: three pipelines failed with "DuckDB engine isn't installed yet" and
    /// the plan said `ok (3 of 3 pipelines ran)`.
    #[test]
    fn a_pipeline_that_fails_fails_its_plan_and_stops_the_next_step() {
        let plan = duckle_duckdb_engine::plans::Plan {
            id: "nightly".into(),
            name: String::new(),
            stop_on_failure: true,
            steps: vec![
                duckle_duckdb_engine::plans::Step {
                    name: "Extract".into(),
                    pipelines: vec!["orders.json".into()],
                    continue_on_failure: None,
                },
                duckle_duckdb_engine::plans::Step {
                    name: "Publish".into(),
                    pipelines: vec!["export.json".into()],
                    continue_on_failure: None,
                },
            ],
        };

        // Exactly what execute_one hands back for a run that started and failed.
        let failed_but_ok = Ok(serde_json::json!({
            "id": "orders",
            "status": "error",
            "error": "DuckDB engine isn't installed yet. Open Setup to install it.",
        }));

        let mut attempted = Vec::new();
        let outcome = duckle_duckdb_engine::plans::execute(&plan, |pipeline| {
            attempted.push(pipeline.to_string());
            plan_step_outcome(failed_but_ok.clone())
        });

        assert_eq!(outcome.status, "failed", "a plan of failed pipelines is not an ok plan");
        assert_eq!(
            attempted,
            ["orders.json"],
            "the second step ran against data the first step never produced"
        );
        let first = &outcome.steps[0].pipelines[0];
        assert_eq!(first.status, "failed");
        assert!(
            first.error.as_deref().unwrap_or("").contains("DuckDB engine"),
            "the reason the pipeline failed was thrown away: {:?}",
            first.error
        );
    }

    /// Every schedule save that predates plans - the Schedules tab toggling one on, an older
    /// client, the desktop app - sends no `planId` at all. Reading that as "no plan" would
    /// leave the schedule pointed at the label in its pipeline_id, which is not a file, so
    /// switching a nightly plan off and on again would be enough to stop it running.
    ///
    /// Saying `"planId": ""` is different: that is somebody asking for a pipeline instead.
    #[test]
    fn a_save_that_says_nothing_about_a_plan_does_not_remove_one() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let save = |body: serde_json::Value| save_schedule_at(&ws, &body).expect("saves");
        let plan_of = || {
            duckle_duckdb_engine::schedules::load(&ws).unwrap()[0].plan_id.clone()
        };

        save(serde_json::json!({ "id": "nightly", "planId": "nightly", "enabled": true, "intervalSeconds": 60 }));
        assert_eq!(plan_of().as_deref(), Some("nightly"));

        // The Schedules tab turning it off and on again, which sends no planId.
        save(serde_json::json!({ "id": "nightly", "enabled": false, "intervalSeconds": 60 }));
        assert_eq!(plan_of().as_deref(), Some("nightly"), "toggling it off dropped the plan");

        // Asked for explicitly, it goes.
        save(serde_json::json!({ "id": "nightly", "planId": "", "enabled": true, "intervalSeconds": 60 }));
        assert_eq!(plan_of(), None, "an explicit empty planId means 'a pipeline, not a plan'");
    }

    /// Removing the last administrator leaves a console nobody can administer. Removing
    /// anyone else has nothing to do with that, and an earlier version counted the
    /// surviving admins without checking who was being removed, so deleting an operator
    /// was refused for leaving too few of them.
    #[test]
    fn only_the_last_administrator_is_protected_from_removal() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let state = guarded_state(&ws);
        state.console.create_account("alice", console_auth::Role::Operator).unwrap();
        state.console.create_account("boss", console_auth::Role::Admin).unwrap();

        let remove = |label: &str| {
            let mut req = request("DELETE", "/api/admin/users", Some("Bearer s3cret"));
            req.body = serde_json::to_vec(&serde_json::json!({ "label": label })).unwrap();
            req.origin = None;
            route_console(&req, &state).code()
        };

        assert_eq!(remove("alice"), 200, "an operator is not an administrator");
        assert_eq!(remove("boss"), 409, "the last administrator must be protected");

        state.console.create_account("boss2", console_auth::Role::Admin).unwrap();
        assert_eq!(remove("boss"), 200, "with a second admin, removing one is fine");
    }

    /// Behind a proxy that terminates TLS the browser is on https, and a session cookie
    /// that is not marked Secure can be sent in clear to anything that reaches the console
    /// directly. That is the deployment shape the guide recommends, so it is the one that
    /// has to be right.
    #[test]
    fn a_session_cookie_is_secure_when_the_browser_is_on_https() {
        assert!(cookie_attributes(Some("https")).contains("Secure"));
        assert!(cookie_attributes(Some("HTTPS")).contains("Secure"), "the header is not case sensitive");
        assert!(
            cookie_attributes(Some("https, http")).contains("Secure"),
            "a chain of proxies leaves a list, and the first hop is the browser's"
        );
    }

    /// The other direction matters more: the console serves plain HTTP, and a browser
    /// silently discards a Secure cookie on http, which would lock everyone out of the
    /// ordinary local case rather than fail visibly.
    #[test]
    fn a_session_cookie_is_not_secure_on_plain_http() {
        for proto in [None, Some("http"), Some("HTTP")] {
            assert!(
                !cookie_attributes(proto).contains("Secure"),
                "Secure on {proto:?} would stop the cookie being stored at all"
            );
        }
    }

    /// Whatever else changes, these two do not: script cannot read it, another site cannot
    /// ride it.
    #[test]
    fn a_session_cookie_is_always_httponly_and_samesite() {
        for proto in [None, Some("http"), Some("https")] {
            let a = cookie_attributes(proto);
            assert!(a.contains("HttpOnly"), "{proto:?}");
            assert!(a.contains("SameSite=Strict"), "{proto:?}");
        }
    }

    fn request(method: &str, path: &str, auth: Option<&str>) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            query: std::collections::HashMap::new(),
            origin: None,
            host: Some("127.0.0.1".into()),
            authorization: auth.map(String::from),
            cookie: None,
            forwarded_proto: None,
            body: Vec::new(),
        }
    }

    fn guarded_state(ws: &std::path::Path) -> std::sync::Arc<State> {
        std::sync::Arc::new(State {
            workspace: ws.to_path_buf(),
            duckdb: std::path::PathBuf::from("duckdb"),
            run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(Default::default())),
            running: Mutex::new(std::collections::HashSet::new()),
            runs: Mutex::new(std::collections::HashMap::new()),
            console: console_auth::Console::configure(ws, "0.0.0.0", Some("s3cret")).unwrap(),
            host: "0.0.0.0".into(),
            tick_interval: std::time::Duration::from_secs(15),
            oidc: None,
            oidc_endpoints: Mutex::new(None),
            oidc_logins: Mutex::new(Default::default()),
        })
    }

    /// #300: an operator can alert on failed and queued runs without the UI.
    #[test]
    fn metrics_serves_run_history_and_what_this_process_is_doing() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join("runs")).unwrap();
        std::fs::write(
            ws.join("runs").join("nightly.json"),
            // snake_case, which is what append_run_record writes: RunRecord
            // carries no rename attribute.
            r#"[{"at":"2026-09-01T00:00:00Z","status":"error","duration_ms":1200,"rows":0,
                 "node_count":2,"trigger":"scheduled"}]"#,
        )
        .unwrap();
        let state = guarded_state(&ws);
        let reply = route_console(&request("GET", "/metrics", Some("Bearer s3cret")), &state);
        assert_eq!(reply.code(), 200);
        assert!(
            reply.content_type.starts_with("text/plain; version=0.0.4"),
            "a scraper negotiates on this: {}",
            reply.content_type
        );
        let body = String::from_utf8_lossy(&reply.body).into_owned();
        // The failed run, from history.
        assert!(
            body.contains("duckle_run_last_status{pipeline=\"nightly\"} 0"),
            "no failed-run series: {body}"
        );
        // And saturation, which no textfile can carry.
        assert!(body.contains("duckle_run_permits_total 1"), "{body}");
        assert!(body.contains("duckle_run_permits_free 1"), "{body}");
        assert!(body.contains("duckle_runs_in_flight 0"), "{body}");
    }

    /// Through the REAL axum Router, not the hand-rolled path.
    ///
    /// The regression this pins lived only in the router: `/api/session` is
    /// public for POST and authorised for DELETE, and registering the path for
    /// POST alone made axum's own method router answer 405 to DELETE, so the
    /// Router fallback never ran and the console's Sign out did nothing. Every
    /// existing test went through `route_console` and stayed green.
    #[tokio::test]
    async fn the_router_serves_every_method_a_public_path_also_has() {
        use tower::ServiceExt;
        let tmp = tempfile::tempdir().unwrap();
        let router = super::console_router(guarded_state(tmp.path()));
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .method("DELETE")
                    .uri("/api/session")
                    .header("authorization", "Bearer s3cret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            response.status(),
            axum::http::StatusCode::METHOD_NOT_ALLOWED,
            "the method router answered 405 and the fallback never ran"
        );
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[test]
    fn a_scraper_does_not_need_an_admin_token() {
        // Handing a monitoring agent a credential that can also add users and
        // deploy pipelines is a worse trade than not scraping.
        let (role, action) = crate::audit::requirement("GET", "/metrics");
        assert_eq!(action, "metrics.read", "left to the fallback, which demands admin");
        assert!(
            crate::console_auth::Role::Viewer.allows(role),
            "a viewer cannot scrape; requirement is {role:?}"
        );
    }

    /// #289: a pool of one admits one run at a time, and a bigger pool admits
    /// more - measured on the real gate, not asserted from the config.
    #[test]
    fn a_pool_bounds_what_runs_at_once() {
        use std::collections::BTreeMap;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mut limits = BTreeMap::new();
        limits.insert("heavy".to_string(), 1usize);
        limits.insert("network".to_string(), 4usize);
        let gates = std::sync::Arc::new(super::Gates::new(
            duckle_duckdb_engine::pools::Pools::from_limits(limits),
        ));

        for (pool, cap) in [("heavy", 1usize), ("network", 4usize)] {
            let peak = std::sync::Arc::new(AtomicUsize::new(0));
            let live = std::sync::Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::new();
            for _ in 0..8 {
                let gates = gates.clone();
                let peak = peak.clone();
                let live = live.clone();
                handles.push(std::thread::spawn(move || {
                    let (_permit, got, _waited) = gates.acquire(pool);
                    assert_eq!(got, pool);
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    live.fetch_sub(1, Ordering::SeqCst);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(
                peak.load(Ordering::SeqCst),
                cap,
                "pool {pool} admitted the wrong number at once"
            );
        }
    }

    /// A pipeline naming a pool nobody defined is admitted to the default
    /// rather than to a new unbounded one.
    #[test]
    fn an_invented_pool_does_not_escape_admission_control() {
        use std::collections::BTreeMap;
        let mut limits = BTreeMap::new();
        limits.insert("heavy".to_string(), 1usize);
        let gates = super::Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(limits));
        let (_permit, got, _) = gates.acquire("i-made-this-up");
        assert_eq!(got, duckle_duckdb_engine::pools::DEFAULT);
    }

    /// Every entry point that starts a run goes through a pool.
    ///
    /// The issue's own warning: a pool applied to scheduled runs but not to the
    /// API would let an agent bypass exactly the protection it exists for. This
    /// reads the source rather than trusting a review, because the failure is
    /// an acquire site that was never edited.
    #[test]
    fn no_run_path_acquires_without_naming_a_pool() {
        let src = include_str!("serve.rs");
        for (n, line) in src.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            // `let ... = state.run_lock.acquire(...)`, so the test does not
            // match its own assertion text below.
            if trimmed.starts_with("let ") && line.contains("run_lock.acquire(") {
                assert!(
                    line.contains("resource_pool"),
                    "serve.rs:{}: a run is admitted without naming a pool: {}",
                    n + 1,
                    line.trim()
                );
            }
        }
    }

    /// Whatever a login hands the browser, the console must find it.
    ///
    /// Reads the header the SERVER builds rather than one the test writes -
    /// which is the difference that matters: the bug that shipped was a login
    /// path spelling the cookie name itself, and a test that spells it too
    /// agrees with the bug.
    #[test]
    fn the_cookie_a_login_sets_is_the_cookie_the_console_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let console =
            console_auth::Console::configure(tmp.path(), "0.0.0.0", Some("s3cret")).unwrap();
        let sid = console.sign_in_external("user-123 (Ada)", console_auth::Role::Operator);

        let header = super::session_cookie_header(&sid, None);
        let sent_back = header
            .strip_prefix("Set-Cookie: ")
            .and_then(|c| c.split(';').next())
            .expect("a cookie");
        let who = console
            .identify(None, Some(sent_back))
            .expect("the console must find the session its own login just set");
        assert_eq!(who.role, console_auth::Role::Operator);
        assert!(header.contains("HttpOnly") && header.contains("SameSite=Strict"), "{header}");
    }

    /// The session an OIDC login mints must be one the console can actually
    /// find.
    ///
    /// This is the bug that shipped: the callback set `Set-Cookie: duckle_sid=`
    /// while the console reads `duckle_console`, so a completed SSO login
    /// authenticated nobody - the row was written, the browser held a cookie
    /// under a name nothing looks at, and the user landed back on the sign-in
    /// form while the audit log said they had signed in. Every existing test
    /// stopped at the 302 or the 400 and never reached the success path.
    #[test]
    fn a_session_minted_for_an_external_identity_is_one_the_console_finds() {
        let tmp = tempfile::tempdir().unwrap();
        let console =
            console_auth::Console::configure(tmp.path(), "0.0.0.0", Some("s3cret")).unwrap();
        let sid = console.sign_in_external("user-123 (Ada)", console_auth::Role::Operator);

        // Exactly the header the browser will send back, built from the same
        // constant the callback must use.
        let cookie = format!("{}={sid}", console_auth::SESSION_COOKIE);
        let who = console
            .identify(None, Some(&cookie))
            .expect("the console must find the session its own login minted");
        assert_eq!(who.role, console_auth::Role::Operator);
        assert_eq!(who.label, "user-123 (Ada)", "the audited actor must survive");

        // And the name that did not work finds nothing, which is what makes
        // this test about the name rather than about sessions in general.
        assert!(console.identify(None, Some(&format!("duckle_sid={sid}"))).is_none());
    }

    /// #314: the web editor can complete SQL, the same way the desktop does.
    #[test]
    fn the_web_editor_completes_a_nodes_sql() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let state = WebState {
            workspace: ws.clone(),
            duckdb: std::path::PathBuf::from("duckdb"),
            dist: ws.clone(),
            host: "0.0.0.0".into(),
            run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(Default::default())),
            console: console_auth::Console::configure(&ws, "0.0.0.0", Some("s3cret")).unwrap(),
        };
        let mut req = request("POST", "/api/cmd/complete_node_sql", Some("Bearer s3cret"));
        req.body = serde_json::to_vec(&serde_json::json!({
            "pipeline": { "nodes": [
                { "id": "q", "type": "transform", "position": {"x":0,"y":0},
                  "data": { "label": "sql", "componentId": "code.sql",
                            // A statement where the answer DEPENDS on the
                            // cursor: a column belongs at the start and a
                            // relation after FROM, so a request that ignored
                            // the position could not pass both halves.
                            "properties": { "sql": "SELECT region FROM " } } }
            ], "edges": [] },
            "nodeId": "q",
            "inputs": [["src", [{ "name": "region", "type": "string" }]]],
            "cursor": 19
        }))
        .unwrap();
        let reply = route_web(&req, &state);
        assert_eq!(reply.code(), 200, "{}", String::from_utf8_lossy(&reply.body));
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        let items = body.as_array().expect("a list");
        // After FROM, only relations belong.
        assert_eq!(items[0]["kind"], "relation", "{body}");
        assert!(
            items.iter().all(|i| i["kind"] == "relation"),
            "a column was offered where a relation belongs: {body}"
        );

        // The same request at the start of the statement answers differently,
        // which is what makes the cursor load-bearing rather than decorative.
        let mut payload: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        payload["cursor"] = serde_json::json!(7);
        let mut at_start = request("POST", "/api/cmd/complete_node_sql", Some("Bearer s3cret"));
        at_start.body = serde_json::to_vec(&payload).unwrap();
        let early = route_web(&at_start, &state);
        let early: serde_json::Value = serde_json::from_slice(&early.body).unwrap();
        assert_eq!(early[0]["kind"], "column", "{early}");
    }

    /// #295: the backfill plan is addressable over the API, and a dry run
    /// queues nothing.
    #[test]
    fn a_backfill_dry_run_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        // Canonical, because resolve_in_workspace compares canonical paths and
        // a temp dir is a symlink on some platforms.
        let ws = &tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines/daily.json"),
            r#"{"formatVersion":1,"name":"daily",
                "partition":{"type":"time","cadence":"day","timezone":"UTC"},
                "nodes":[{"id":"s","type":"source","position":{"x":0,"y":0},
                  "data":{"label":"In","componentId":"src.csv",
                          "properties":{"path":"in.csv","hasHeader":true}}}],
                "edges":[]}"#,
        )
        .unwrap();
        let state = guarded_state(ws);
        let mut req = request("POST", "/api/backfills", Some("Bearer s3cret"));
        req.body = serde_json::to_vec(&serde_json::json!({
            "action": "create", "pipeline": "pipelines/daily.json",
            "from": "2020-01-01", "to": "2020-01-03", "dryRun": true
        }))
        .unwrap();
        let reply = route_console(&req, &state);
        assert_eq!(reply.code(), 200, "{}", String::from_utf8_lossy(&reply.body));
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body["count"], 3);
        assert_eq!(body["partitions"][0], "2020-01-01");
        // "What would this queue" must not be a question that queues anything.
        assert!(
            duckle_duckdb_engine::backfill::list(ws).is_empty(),
            "a dry run persisted a plan"
        );
    }

    /// Reading a backfill is a viewer's business; changing one is an
    /// operator's. Left to the catch-all both would need admin, and every
    /// operator would get a 403 with the action logged as "unknown".
    #[test]
    fn backfill_routes_ask_for_the_right_role() {
        let (read, read_action) = crate::audit::requirement("GET", "/api/backfills");
        assert_eq!(read_action, "backfills.read");
        assert!(console_auth::Role::Viewer.allows(read));
        let (write, write_action) = crate::audit::requirement("POST", "/api/backfills");
        assert_eq!(write_action, "backfill.write");
        assert!(console_auth::Role::Operator.allows(write));
        assert!(!console_auth::Role::Viewer.allows(write), "a viewer must not start a backfill");
    }

    /// #314: the web editor gets the same analysis shape the desktop does.
    ///
    /// One object for one node, using the upstream columns the EDITOR resolved
    /// - not an array, and not inputs this side derived. The two surfaces
    /// answering subtly different questions is the divergence #75 exists to
    /// stop, and it is invisible until someone compares them.
    #[test]
    fn the_web_editor_analyses_one_node_the_way_the_desktop_does() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let state = WebState {
            workspace: ws.clone(),
            duckdb: std::path::PathBuf::from("duckdb"),
            dist: ws.clone(),
            host: "0.0.0.0".into(),
            run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(Default::default())),
            console: console_auth::Console::configure(&ws, "0.0.0.0", Some("s3cret")).unwrap(),
        };
        let mut req = request("POST", "/api/cmd/analyze_node_sql", Some("Bearer s3cret"));
        req.body = serde_json::to_vec(&serde_json::json!({
            "pipeline": { "nodes": [
                { "id": "s", "type": "source", "position": {"x":0,"y":0},
                  "data": { "label": "in", "componentId": "src.postgres",
                            "properties": { "mode": "sql", "sql": "SELECT 1" } } }
            ], "edges": [] },
            "nodeId": "s",
            "inputs": []
        }))
        .unwrap();
        let reply = route_web(&req, &state);
        assert_eq!(reply.code(), 200, "{}", String::from_utf8_lossy(&reply.body));
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert!(body.is_object(), "one node's analysis, not an array: {body}");
        assert_eq!(body["nodeId"], "s");
        // A source's SQL is remote, so it is reported as not validated rather
        // than checked against DuckDB - the same answer the desktop gives.
        assert_eq!(body["dialect"], "remote");
        assert_eq!(body["validated"], false);
    }

    /// #307: the web editor gets the same external components the desktop
    /// does, so a workspace's palette is not different in the two editors.
    #[test]
    fn the_web_console_serves_the_external_component_list() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let d = ws.join("components").join("upper");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(
            d.join("duckle-component.json"),
            r#"{"id":"ext.upper","version":"1.0.0","label":"Uppercase",
                "inputs":[{"name":"main"}],"outputs":[{"name":"main"}],
                "runtime":{"command":["python","run.py"]}}"#,
        )
        .unwrap();
        // The EDITOR server, which is where the palette lives and where the
        // /api/cmd/ dispatcher is - the console has its own routes.
        let ws = ws.canonicalize().unwrap();
        let state = WebState {
            workspace: ws.clone(),
            duckdb: std::path::PathBuf::from("duckdb"),
            dist: ws.clone(),
            host: "0.0.0.0".into(),
            run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(Default::default())),
            console: console_auth::Console::configure(&ws, "0.0.0.0", Some("s3cret")).unwrap(),
        };
        let mut req = request("POST", "/api/cmd/external_components", Some("Bearer s3cret"));
        req.body = b"{}".to_vec();
        let reply = route_web(&req, &state);
        assert_eq!(reply.code(), 200, "{}", String::from_utf8_lossy(&reply.body));
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        let items = body["components"].as_array().expect("components");
        assert_eq!(items.len(), 1, "{body}");
        assert_eq!(items[0]["id"], "ext.upper");
        assert_eq!(items[0]["kind"], "transform", "derived from its ports");
        assert_eq!(items[0]["external"], true);
        // The form travels with it, or the tile cannot be configured.
        assert!(items[0]["manifest"].get("sections").is_some(), "{}", items[0]);
    }

    /// #310: the login routes are public, and a server with no OIDC config
    /// answers 404 rather than 401.
    #[test]
    fn the_oidc_routes_are_public_and_absent_when_unconfigured() {
        assert!(is_public_route("GET", OIDC_LOGIN_PATH), "signing in cannot require being signed in");
        assert!(is_public_route("GET", OIDC_CALLBACK_PATH));
        let tmp = tempfile::tempdir().unwrap();
        let state = guarded_state(tmp.path());
        for path in [OIDC_LOGIN_PATH, OIDC_CALLBACK_PATH] {
            let reply = route_console(&request("GET", path, None), &state);
            // 404 and not 401: there is no such login here, and "unauthorised"
            // would suggest there is one to get past. And NOT 401 by accident
            // either - a public path with no branch falls through to the token
            // sign-in and answers 401, which for a login route is a bug that
            // looks like a misconfigured provider.
            assert_eq!(reply.code(), 404, "{path} answered {}", reply.code());
        }
    }

    /// A callback with no login behind it is refused before anything is
    /// fetched, so an attacker cannot make this server talk to a provider.
    #[test]
    fn a_callback_without_a_matching_state_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join(".duckle")).unwrap();
        std::fs::write(
            crate::oidc::config_path(&ws),
            r#"{"issuer":"https://idp.invalid","clientId":"c",
                "redirectUri":"https://console.invalid/auth/oidc/callback"}"#,
        )
        .unwrap();
        let mut state = guarded_state(&ws);
        std::sync::Arc::get_mut(&mut state).unwrap().oidc =
            crate::oidc::load(&ws).unwrap();
        // Endpoints pre-seeded so the test never touches the network: the point
        // is that the state check happens BEFORE the exchange.
        *std::sync::Arc::get_mut(&mut state).unwrap().oidc_endpoints.get_mut().unwrap() =
            Some(crate::oidc::Endpoints {
                authorization: "https://idp.invalid/authorize".into(),
                token: "https://idp.invalid/token".into(),
                jwks: "https://idp.invalid/jwks".into(),
                issuer: "https://idp.invalid".into(),
            });
        let state = state;

        let mut req = request("GET", OIDC_CALLBACK_PATH, None);
        req.query.insert("code".into(), "stolen".into());
        req.query.insert("state".into(), "never-issued".into());
        let reply = route_console(&req, &state);
        assert_eq!(reply.code(), 400);
        let body = String::from_utf8_lossy(&reply.body);
        assert!(body.contains("does not match a login"), "{body}");
    }

    /// The redirect carries a state this process is holding, and never the
    /// PKCE verifier.
    #[test]
    fn a_login_issues_a_state_and_keeps_the_verifier() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join(".duckle")).unwrap();
        std::fs::write(
            crate::oidc::config_path(&ws),
            r#"{"issuer":"https://idp.invalid","clientId":"c",
                "redirectUri":"https://console.invalid/auth/oidc/callback"}"#,
        )
        .unwrap();
        let mut state = guarded_state(&ws);
        std::sync::Arc::get_mut(&mut state).unwrap().oidc = crate::oidc::load(&ws).unwrap();
        *std::sync::Arc::get_mut(&mut state).unwrap().oidc_endpoints.get_mut().unwrap() =
            Some(crate::oidc::Endpoints {
                authorization: "https://idp.invalid/authorize".into(),
                token: "https://idp.invalid/token".into(),
                jwks: "https://idp.invalid/jwks".into(),
                issuer: "https://idp.invalid".into(),
            });
        let state = state;

        let reply = route_console(&request("GET", OIDC_LOGIN_PATH, None), &state);
        assert_eq!(reply.code(), 302);
        let location = reply
            .headers
            .iter()
            .find(|h| h.starts_with("Location: "))
            .expect("a redirect");
        assert!(location.contains("code_challenge_method=S256"), "{location}");
        assert!(location.contains("state="), "{location}");
        assert_eq!(
            state.oidc_logins.lock().unwrap().len(),
            1,
            "the login must be held, or the callback has nothing to match"
        );
    }

    #[test]
    fn metrics_is_not_public() {
        // Pipeline names are the shape of someone's business. /healthz says the
        // process is up and tells an anonymous caller nothing else; this would
        // tell them everything.
        assert!(!is_public_route("GET", "/metrics"));
        let tmp = tempfile::tempdir().unwrap();
        let state = guarded_state(tmp.path());
        let reply = route_console(&request("GET", "/metrics", None), &state);
        assert_ne!(reply.code(), 200, "served metrics to an unauthenticated caller");
    }

    #[test]
    fn metrics_on_a_workspace_that_has_never_run_is_not_an_error() {
        // A fresh deployment has no runs/ directory. Reporting a broken scrape
        // for it would make every new install look down.
        let tmp = tempfile::tempdir().unwrap();
        let state = guarded_state(tmp.path());
        let reply = route_console(&request("GET", "/metrics", Some("Bearer s3cret")), &state);
        assert_eq!(reply.code(), 200);
        assert!(String::from_utf8_lossy(&reply.body).contains("duckle_run_permits_total"));
    }

    /// #300: liveness and readiness fail differently and are acted on
    /// differently, so they are separate routes.
    #[test]
    fn readyz_is_public_and_reports_a_workspace_it_cannot_write() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        assert!(is_public_route("GET", "/readyz"), "an orchestrator probe holds no credential");
        let state = guarded_state(&ws);
        let ok = route_console(&request("GET", "/readyz", None), &state);
        assert_eq!(ok.code(), 200);
        assert_eq!(String::from_utf8_lossy(&ok.body), "ready");

        // Liveness is a different question and must not move with it.
        assert_eq!(route_console(&request("GET", "/healthz", None), &state).code(), 200);
    }

    #[test]
    fn readiness_fails_on_a_workspace_that_cannot_be_written() {
        // The probe itself rather than the route, because a State cannot be
        // built on an unwritable workspace at all - configuring the console is
        // the first thing that writes there. The route maps Err to 503 in three
        // lines above; what needs proving is that the probe says Err for a
        // workspace that genuinely cannot record a run.
        let tmp = tempfile::tempdir().unwrap();
        let not_a_directory = tmp.path().join("workspace-is-a-file");
        std::fs::write(&not_a_directory, b"x").unwrap();
        let why = crate::serve::probe_ready(&not_a_directory).unwrap_err();
        assert!(why.contains("not writable"), "{why}");

        // And a workspace that is fine says so, so the check is not simply
        // always failing.
        assert!(crate::serve::probe_ready(tmp.path()).is_ok());
    }

    /// #259: the whole asynchronous contract, end to end without a socket.
    ///
    /// Accept a run, get an id back straight away, poll it to completion, and
    /// find it in the durable run history under the same id. The last part is
    /// what makes the id worth handing out: a console that restarts mid-run can
    /// still answer for it.
    #[test]
    fn an_async_run_is_accepted_polled_and_recorded_under_its_id() {
        let duckdb = match std::env::var("DUCKLE_DUCKDB_BIN") {
            Ok(v) if !v.trim().is_empty() => std::path::PathBuf::from(v),
            _ => return, // no engine binary here; the engine suite skips the same way
        };
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(ws.join("in.csv"), "id,name\n1,alice\n2,bob\n").unwrap();
        let doc = serde_json::json!({
            "nodes": [
                { "id": "s", "position": {"x":0,"y":0}, "data": { "label": "in", "componentId": "src.csv",
                  "properties": { "path": ws.join("in.csv").to_string_lossy(), "hasHeader": true } } },
                { "id": "k", "position": {"x":200,"y":0}, "data": { "label": "out", "componentId": "snk.csv",
                  "properties": { "path": ws.join("out.csv").to_string_lossy(), "hasHeader": true } } }
            ],
            "edges": [ { "id": "e1", "source": "s", "target": "k", "data": { "connectionType": "main" } } ]
        });
        std::fs::write(
            ws.join("pipelines").join("async_demo.json"),
            serde_json::to_string(&doc).unwrap(),
        )
        .unwrap();

        let mut state = guarded_state(&ws);
        // guarded_state points at a placeholder binary; this test actually runs.
        std::sync::Arc::get_mut(&mut state).unwrap().duckdb = duckdb;
        let state = state;

        let mut accept = request("POST", "/api/run/async", Some("Bearer s3cret"));
        accept.body = serde_json::json!({ "file": "pipelines/async_demo.json" })
            .to_string()
            .into_bytes();
        let reply = route_console(&accept, &state);
        assert_eq!(reply.code(), 202, "an async run must be ACCEPTED, not awaited");
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        let run_id = body.get("runId").and_then(|v| v.as_str()).unwrap().to_string();
        assert!(!run_id.is_empty());
        assert_eq!(body.get("pipelineId").and_then(|v| v.as_str()), Some("async_demo"));

        // Poll the way a client would.
        let mut status = serde_json::Value::Null;
        for _ in 0..200 {
            let mut q = request("GET", "/api/run/status", Some("Bearer s3cret"));
            q.query.insert("runId".into(), run_id.clone());
            let r = route_console(&q, &state);
            assert_eq!(r.code(), 200, "status must answer for an accepted run");
            status = serde_json::from_slice(&r.body).unwrap();
            if status.get("state").and_then(|v| v.as_str()) == Some("finished") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(
            status.get("state").and_then(|v| v.as_str()),
            Some("finished"),
            "run did not finish in time: {status}"
        );
        // The pipeline's own status, not merely that the call worked.
        assert_eq!(
            status.get("status").and_then(|v| v.as_str()),
            Some("ok"),
            "run reported: {status}"
        );
        assert!(ws.join("out.csv").is_file(), "the run should have written its sink");

        // Durable: the history record carries the id the 202 handed out, so the
        // answer survives losing the in-memory registry.
        let recorded = duckle_duckdb_engine::history::load_run_history(&ws, "async_demo");
        assert!(
            recorded.iter().any(|r| r.run_id.as_deref() == Some(run_id.as_str())),
            "no run record carried run_id {run_id}"
        );

        // An id nobody was given is a 404, not an empty success.
        let mut unknown = request("GET", "/api/run/status", Some("Bearer s3cret"));
        unknown.query.insert("runId".into(), "run-nope-0".into());
        assert_eq!(route_console(&unknown, &state).code(), 404);
    }

    /// #259: cancelling an id that is not live is a 404 rather than a cheerful
    /// "cancelling" for a run that does not exist.
    #[test]
    fn cancelling_an_unknown_run_is_not_reported_as_cancelling() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let state = guarded_state(&ws);
        let mut req = request("DELETE", "/api/run", Some("Bearer s3cret"));
        req.query.insert("runId".into(), "run-missing-0".into());
        assert_eq!(route_console(&req, &state).code(), 404);
        // And with no id at all it is a bad request, not a 404.
        let bare = request("DELETE", "/api/run", Some("Bearer s3cret"));
        assert_eq!(route_console(&bare, &state).code(), 400);
    }

    /// The point of routing to a value rather than a socket: the authorisation decisions
    /// can be exercised directly. Before this they needed a listening server, so in
    /// practice they were only ever checked by hand.
    #[test]
    fn the_console_decides_who_gets_in_without_a_socket() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let state = guarded_state(&ws);

        assert_eq!(route_console(&request("GET", "/healthz", None), &state).code(), 200);
        assert_eq!(route_console(&request("GET", "/api/catalog", None), &state).code(), 401);
        assert_eq!(
            route_console(&request("GET", "/api/catalog", Some("Bearer wrong")), &state).code(),
            401
        );
        assert_eq!(
            route_console(&request("GET", "/api/catalog", Some("Bearer s3cret")), &state).code(),
            200
        );
    }

    /// A --token caller is an admin, so this proves the role gate rather than the token
    /// gate: an unknown route falls to admin and a viewer must not reach it.
    #[test]

    /// `/api/run_stream` executes a pipeline supplied in the request body, and
    /// resolves this workspace's saved connections into it before running. It was
    /// dispatched by `handle_web` before `route_web` was ever called, so it ran
    /// with no cross-origin guard, no sign-in and no role check: an unauthenticated
    /// POST executed arbitrary work with the workspace's credentials, on an image
    /// whose entrypoint is `duckle-runner web`.
    ///
    /// The gate is asserted here rather than through `handle_web`, which owns a

    /// The file bridge sits at operator level while the connection commands require
    /// admin. That gate is worth nothing if the same operator can ask for the key as
    /// a file, so the key and token directories are refused outright.
    #[test]
    fn the_file_api_cannot_reach_key_material() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        for path in [
            ".duckle/keys/secret.key",
            ".duckle/keys",
            ".duckle/secrets/git.json",
            "pipelines/../.duckle/keys/secret.key",
        ] {
            assert!(
                confine_to_workspace(&ws, path).is_err(),
                "the file API served key material at {path}"
            );
        }
        // Ordinary workspace files still resolve.
        assert!(confine_to_workspace(&ws, "pipelines/orders.json").is_ok());
    }

    /// A prefix test on strings treats a sibling directory as inside the workspace.
    #[test]
    fn a_sibling_directory_is_not_inside_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let sibling = format!("{}-backup/steal.json", ws.to_string_lossy());
        assert!(
            confine_to_workspace(&ws, &sibling).is_err(),
            "a sibling directory sharing the workspace name prefix was accepted"
        );
    }

    /// socket. Reverting `web_gate`'s identity check turns this red.
    #[test]
    fn the_streaming_run_route_is_not_reachable_without_credentials() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        let state = WebState {
            workspace: ws.clone(),
            duckdb: std::path::PathBuf::from("duckdb"),
            dist: ws.clone(),
            host: "0.0.0.0".into(),
            run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(Default::default())),
            console: console_auth::Console::configure(&ws, "0.0.0.0", Some("s3cret")).unwrap(),
        };

        let anonymous = request("POST", "/api/run_stream", None);
        let refused = web_gate(&anonymous, &state, console_auth::Role::Operator, "editor.api")
            .expect_err("an unauthenticated streaming run must be refused");
        assert_eq!(
            refused.code(),
            401,
            "unauthenticated /api/run_stream answered {} instead of 401",
            refused.code()
        );

        let bearer = format!("Bearer {}", "s3cret");
        let signed_in = request("POST", "/api/run_stream", Some(&bearer));
        web_gate(&signed_in, &state, console_auth::Role::Operator, "editor.api")
            .expect("a credentialed operator must still be allowed to run");
    }

    fn a_role_that_is_not_enough_is_refused_not_admitted() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().canonicalize().unwrap();
        {
            let store = crate::auth_store::AuthStore::open(&ws).unwrap();
            store.create_api_key("dashboard", console_auth::Role::Viewer, None).unwrap();
        }
        let state = guarded_state(&ws);
        let key = {
            let store = crate::auth_store::AuthStore::open(&ws).unwrap();
            store.create_api_key("dash2", console_auth::Role::Viewer, None).unwrap()
        };

        let viewer = format!("Bearer {key}");
        assert_eq!(
            route_console(&request("GET", "/api/catalog", Some(&viewer)), &state).code(),
            200,
            "a viewer may read the catalog"
        );
        let refused = route_console(&request("POST", "/api/deploy", Some(&viewer)), &state);
        assert_eq!(refused.code(), 403, "a viewer must not deploy");

        // The shape of the refusal, not just its code. `docs/current/ci-and-orchestration.md`
        // tells people writing a CI step that a mis-scoped key comes back as
        // `{"error": "..."}`, and a pipeline that reads it with `jq -r .error` breaks
        // silently if this ever becomes plain text. The console has both kinds of refusal
        // in it - the cross-origin guard answers text/plain - so which one this route uses
        // is worth holding still.
        assert_eq!(
            refused.content_type, "application/json",
            "a documented JSON refusal became {}",
            refused.content_type
        );
        let body: serde_json::Value =
            serde_json::from_slice(&refused.body).expect("a 403 body must parse as JSON");
        assert_eq!(
            body["error"], "this needs the admin role; you have viewer",
            "the refusal must say which role is needed and which one the caller has"
        );
    }

    /// An orchestrator has to be able to ask whether the process is alive without holding
    /// a credential. Every route requiring one meant a Kubernetes HTTP probe got 401 and
    /// the pod was reported unhealthy for as long as it ran.
    #[test]
    fn liveness_can_be_checked_without_signing_in() {
        assert!(is_public_route("GET", HEALTH_PATH));
    }

    /// The other half, and the more important one: opening a hole for probes must not open
    /// one for anything else.
    #[test]
    fn nothing_else_is_reachable_without_signing_in() {
        for (method, path) in [
            ("GET", "/"),
            ("GET", "/api/pipeline"),
            ("GET", "/api/catalog"),
            ("GET", "/api/audit"),
            ("POST", "/api/run"),
            ("POST", "/api/schedule"),
            ("DELETE", "/api/session"),
            ("GET", "/healthz/../api/audit"),
            ("POST", HEALTH_PATH),
        ] {
            assert!(
                !is_public_route(method, path),
                "{method} {path} must require a credential"
            );
        }
    }

    /// Deployed from the published image the entrypoint is the editor, so an operator who
    /// has armed schedules and sees nothing fire gets no error to explain it. Naming the
    /// count is the difference between a note and a warning worth acting on.
    #[test]
    fn armed_schedules_are_named_when_the_editor_will_not_run_them() {
        let n = scheduler_notice(3);
        assert!(n.contains("3"), "the count must appear: {n}");
        assert!(n.contains("NOT run"), "it must say they will not run: {n}");
        assert!(n.contains("serve"), "it must say what to run instead: {n}");
    }

    /// With nothing armed there is nothing to alarm anyone about, but the difference
    /// between the two modes is still worth stating once.
    #[test]
    fn an_empty_workspace_gets_a_note_rather_than_a_warning() {
        let n = scheduler_notice(0);
        assert!(!n.contains("WARNING"), "nothing is at risk yet: {n}");
        assert!(n.contains("serve"), "it must still say which mode schedules: {n}");
    }

    /// The console and the desktop app now keep one store, so a schedule saved
    /// here has to be a record the desktop reads, in the file the desktop reads.
    /// Before this, the console wrote `panel-schedules.json` and the desktop
    /// never saw it, which is why the same pipeline could end up scheduled twice.
    #[test]
    fn a_console_save_lands_in_the_store_the_desktop_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": true, "intervalSeconds": 90 }),
        )
        .expect("save");

        let list = schedules::load(ws).expect("the desktop can read it");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pipeline_id, "nightly-load");
        assert!(list[0].enabled);
        // 90 seconds, not "1 minute" or "0 minutes": the console works in the
        // same units the desktop editor offers, so an interval survives a save
        // from either side unchanged.
        assert!(
            matches!(list[0].kind, ScheduleKind::Interval { seconds: 90 }),
            "interval was not stored exactly: {:?}",
            list[0].kind
        );

        // Saving again edits that record rather than adding a second one, so a
        // pipeline cannot accumulate duplicate schedules by being saved twice.
        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": false, "cron": "0 9 * * 1" }),
        )
        .expect("second save");
        let list = schedules::load(ws).expect("still readable");
        assert_eq!(list.len(), 1, "a second save duplicated the schedule");
        assert!(!list[0].enabled);
        assert!(matches!(&list[0].kind, ScheduleKind::Cron { expr } if expr == "0 9 * * 1"));
    }

    #[test]
    fn an_enabled_schedule_with_no_cron_and_no_interval_is_refused() {
        // The console's interval box left empty posts intervalSeconds: 0. The
        // runner skips such a schedule, but the desktop scheduler computes
        // `now + 0s` as the next run and fires it on every tick, forever.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let err = save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": true, "intervalSeconds": 0 }),
        )
        .expect_err("an enabled schedule with no trigger must be refused");
        assert!(err.contains("greater than zero"), "unhelpful message: {err}");

        // Disabling it is fine - that is how the console turns a schedule off.
        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": false, "intervalSeconds": 0 }),
        )
        .expect("a disabled schedule needs no trigger");
    }

    #[test]
    fn the_pipeline_reader_refuses_anything_that_is_not_a_pipeline() {
        // This route is rated for viewers, and the workspace also holds the
        // console account hashes and the connection files. Confining to the
        // workspace was the only check, so "any JSON under the workspace" was
        // readable by the lowest role there is.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        // A saved connection: same workspace, same "it is JSON" shape, and it
        // holds an encrypted credential payload. (The account file makes the
        // same point but Console::configure rightly refuses to start against a
        // fabricated Argon2 hash, so this is the cleaner fixture.)
        std::fs::create_dir_all(ws.join("connections")).unwrap();
        std::fs::write(
            ws.join("connections").join("prod-db.json"),
            serde_json::json!({ "name": "prod", "payload": "<ciphertext>" }).to_string(),
        )
        .unwrap();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines").join("real.json"),
            serde_json::json!({ "name": "real", "nodes": [], "edges": [] }).to_string(),
        )
        .unwrap();

        // The server canonicalises the workspace at startup, and
        // resolve_in_workspace compares canonical paths - on Windows that means
        // a \?\ prefix on both sides or neither. A test holding the raw
        // temp path would fail every lookup for the wrong reason.
        let ws_canon = ws.canonicalize().unwrap();
        let state = State {
            workspace: ws_canon.clone(),
            duckdb: std::path::PathBuf::from("duckdb"),
            run_lock: Gates::new(duckle_duckdb_engine::pools::Pools::from_limits(Default::default())),
            running: Mutex::new(std::collections::HashSet::new()),
            runs: Mutex::new(std::collections::HashMap::new()),
            console: console_auth::Console::configure(&ws_canon, "127.0.0.1", None).unwrap(),
            host: "127.0.0.1".into(),
            tick_interval: std::time::Duration::from_secs(15),
            oidc: None,
            oidc_endpoints: Mutex::new(None),
            oidc_logins: Mutex::new(Default::default()),
        };

        let leaked = read_pipeline_file(&state, "connections/prod-db.json");
        assert!(leaked.is_err(), "a stored connection was readable through the pipeline route");
        assert!(read_pipeline_file(&state, "pipelines/real.json").is_ok(), "a real pipeline still reads");
    }

    /// An install that already had console schedules must keep firing across
    /// the move to the shared store, and must not gain a duplicate for a
    /// pipeline the desktop app had already scheduled.
    #[test]
    fn the_old_console_store_is_carried_over_without_duplicating() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The desktop already schedules one of these two pipelines.
        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "already-known", "enabled": true, "intervalSeconds": 600 }),
        )
        .unwrap();

        std::fs::write(
            ws.join("panel-schedules.json"),
            serde_json::json!({
                "already-known": { "enabled": true, "intervalMinutes": 5, "cron": "" },
                "console-only": { "enabled": true, "intervalMinutes": 15, "cron": "" },
                "never-configured": { "enabled": false, "intervalMinutes": 0, "cron": "" },
            })
            .to_string(),
        )
        .unwrap();

        migrate_legacy_schedules(ws);

        let list = schedules::load(ws).unwrap();
        let ids: std::collections::HashSet<&str> =
            list.iter().map(|s| s.pipeline_id.as_str()).collect();
        assert!(ids.contains("console-only"), "a console schedule was lost");
        assert!(
            !ids.contains("never-configured"),
            "an entry with no cron and no interval was imported as a schedule"
        );
        assert_eq!(
            list.iter().filter(|s| s.pipeline_id == "already-known").count(),
            1,
            "the pipeline the desktop already scheduled gained a duplicate"
        );
        // ...and it kept the desktop's value rather than the console's 5 minutes.
        let known = list.iter().find(|s| s.pipeline_id == "already-known").unwrap();
        assert!(matches!(known.kind, ScheduleKind::Interval { seconds: 600 }));

        // Running again is a no-op, so a restart does not re-import.
        migrate_legacy_schedules(ws);
        assert_eq!(schedules::load(ws).unwrap().len(), list.len(), "re-imported on restart");
    }

    #[test]
    fn normalize_cron_pads_five_fields_and_validates() {
        // A standard 5-field cron gets a "0 " seconds field prepended so the
        // `cron` crate (which wants 6/7 fields) accepts it, and the result parses.
        let five = normalize_cron("0 9 * * 1").expect("5-field accepted");
        assert_eq!(five, "0 0 9 * * 1");
        assert!(five.parse::<cron::Schedule>().is_ok(), "padded expr parses");
        // A 6-field expression passes through unchanged and parses.
        let six = normalize_cron("*/30 * * * * *").expect("6-field accepted");
        assert_eq!(six, "*/30 * * * * *");
        assert!(six.parse::<cron::Schedule>().is_ok());
        // Garbage / wrong field counts are rejected (never fire silently).
        assert!(normalize_cron("not a cron").is_none());
        assert!(normalize_cron("* * *").is_none());
        assert!(normalize_cron("").is_none());
    }

    /// The web editor must encrypt connection secrets exactly like the desktop.
    ///
    /// This drives `connection_secret_cmd`, the same function the HTTP handler
    /// calls, so reverting that handler to the old echo-the-payload-back
    /// behaviour fails here. The assertion that matters is the negative one:
    /// the stored form must NOT contain the password. A round-trip assertion
    /// alone would have passed against the broken pass-through, because
    /// echoing a payload round-trips perfectly.
    #[test]
    fn web_editor_encrypts_connection_secrets() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let body = serde_json::to_vec(&serde_json::json!({
            "payloadJson": r#"{"host":"db.internal","user":"reporting","password":"hunter2"}"#
        }))
        .unwrap();

        let stored = connection_secret_cmd(ws, "connection_encrypt_payload", &body).expect("encrypts");
        assert!(
            !stored.contains("hunter2"),
            "password reached disk in clear text: {stored}"
        );
        // Version-agnostic: the test cares THAT the value is sealed, not which
        // format version sealed it.
        assert!(
            duckle_secrets::is_encrypted(
                stored.split('"').find(|s| duckle_secrets::is_encrypted(s)).unwrap_or("")
            ),
            "no ciphertext marker: {stored}"
        );
        // Non-secret fields stay readable so the connection list still renders.
        assert!(stored.contains("db.internal"), "host should not be encrypted");

        let back_body =
            serde_json::to_vec(&serde_json::json!({ "payloadJson": stored })).unwrap();
        let back = connection_secret_cmd(ws, "connection_decrypt_payload", &back_body)
            .expect("decrypts");
        assert!(back.contains("hunter2"), "did not survive the round trip");
    }

    /// A workspace written before this fix holds plaintext. Opening it must
    /// keep working rather than erroring, which is why the decrypt side is
    /// deliberately lenient.
    #[test]
    fn web_editor_still_opens_legacy_plaintext_connections() {
        let tmp = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "payloadJson": r#"{"host":"db.internal","password":"hunter2"}"#
        }))
        .unwrap();
        let back = connection_secret_cmd(tmp.path(), "connection_decrypt_payload", &body)
            .expect("lenient");
        assert!(back.contains("hunter2"), "legacy plaintext must still load");
    }

    /// Editing a cron expression has to take effect before the old one fires.
    ///
    /// The armed occurrence was keyed by schedule id alone, so an edit changed
    /// nothing until the OLD occurrence came round. A schedule moved from
    /// 03:00 to 09:00 skipped 09:00 entirely and then fired at 03:00 the next
    /// morning: not merely late, but firing at the one time it had just been
    /// moved away from.
    #[test]
    fn an_edited_cron_expression_is_armed_from_the_new_one() {
        use chrono::{Datelike, TimeZone, Timelike};
        let parse = |e: &str| {
            normalize_cron(e).and_then(|x| x.parse::<cron::Schedule>().ok()).expect("bad cron")
        };
        let at = |h: u32, m: u32| {
            chrono::Local.with_ymd_and_hms(2026, 8, 15, h, m, 0).single().expect("ambiguous local time")
        };

        // 03:00 daily, first seen at 08:00: arm tomorrow, do not fire.
        let daily_3am = parse("0 3 * * *");
        let (fire, armed) = cron_decision(None, "0 3 * * *", &daily_3am, at(8, 0));
        assert!(!fire, "a schedule fired the moment it was first seen");
        let armed = armed.expect("nothing was armed");
        assert_eq!(armed.0, "0 3 * * *");
        assert_eq!(armed.1.hour(), 3, "armed at the wrong hour");

        // Now it is edited to 09:00. The next tick must re-arm from the NEW
        // expression rather than keep waiting for tomorrow's 03:00.
        let daily_9am = parse("0 9 * * *");
        let (fire, rearmed) = cron_decision(Some(&armed), "0 9 * * *", &daily_9am, at(8, 0));
        assert!(!fire, "the edit itself fired a run");
        let rearmed = rearmed.expect("nothing was armed after the edit");
        assert_eq!(rearmed.0, "0 9 * * *", "still armed by the old expression");
        assert_eq!(rearmed.1.hour(), 9, "did not re-arm from the edited expression");
        assert_eq!(rearmed.1.day(), 15, "the edit was pushed to tomorrow");

        // And at 09:00 it fires, then arms the following day.
        let (fire, next) = cron_decision(Some(&rearmed), "0 9 * * *", &daily_9am, at(9, 0));
        assert!(fire, "the edited schedule did not fire at its new time");
        let next = next.expect("nothing was armed after firing");
        assert_eq!(next.1.day(), 16, "re-armed on the same day, so it would fire twice");

        // An unchanged expression that is not due yet is left exactly alone,
        // or the occurrence would be pushed away on every tick and never come.
        let (fire, held) = cron_decision(Some(&next), "0 9 * * *", &daily_9am, at(9, 1));
        assert!(!fire);
        assert_eq!(held.unwrap().1, next.1, "an armed occurrence was moved by a tick");
    }

    /// The first thing an unauthenticated caller reaches must be bounded.
    ///
    /// `read_request` runs before anyone is identified, on a thread spawned per
    /// connection with no ceiling. It had no read deadline, so one byte and
    /// silence parked that thread for the life of the process, and it believed
    /// whatever Content-Length it was handed, so a declared body was buffered
    /// whole before anything looked at who was asking.
    #[test]
    fn an_unidentified_caller_cannot_park_a_thread_or_name_its_own_body_size() {
        use std::io::Write;
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // An outsized Content-Length is refused before a byte of it is read.
        let sender = std::thread::spawn(move || {
            let mut c = TcpStream::connect(addr).unwrap();
            let _ = write!(
                c,
                "POST /api/run HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
                MAX_BODY + 1
            );
            // Deliberately never sends the body: the refusal must not depend
            // on the caller actually delivering what it claimed.
            std::thread::sleep(std::time::Duration::from_millis(200));
        });
        let (mut server, _) = listener.accept().unwrap();
        let err = match read_request(&mut server) {
            Err(e) => e,
            Ok(_) => panic!("an unbounded body was accepted"),
        };
        assert!(err.contains("too large"), "wrong refusal: {err}");
        sender.join().unwrap();

        // And an ordinary request leaves the socket with a deadline on it, so
        // no later read on this connection can block forever either.
        let sender = std::thread::spawn(move || {
            let mut c = TcpStream::connect(addr).unwrap();
            let _ = write!(c, "GET /api/summary HTTP/1.1\r\nHost: x\r\n\r\n");
            std::thread::sleep(std::time::Duration::from_millis(200));
        });
        let (mut server, _) = listener.accept().unwrap();
        let req = read_request(&mut server).expect("an ordinary request was refused");
        assert_eq!(req.path, "/api/summary");
        assert!(
            server.read_timeout().unwrap().is_some(),
            "the connection has no read deadline, so a stalled caller pins the thread"
        );
        sender.join().unwrap();
    }

    /// #295: a killed process must not strand a backfill forever.
    ///
    /// A slice left `running` is not claimable - only `requested` is - and
    /// `retry` moves only `failed` and `interrupted`, so nothing could ever
    /// pick it up and the backfill was stuck for good. `backfill::reconcile`
    /// existed for exactly this and had NO production caller anywhere in the
    /// repo; its twin for run receipts was called here twice.
    ///
    /// Read from the source because the symptom is invisible until someone
    /// kills a backfill and tries to resume it days later. The needle is built
    /// from pieces so it cannot match itself in this file.
    #[test]
    fn a_killed_backfill_is_reconciled_when_the_server_starts() {
        let src = include_str!("serve.rs");
        let needle = format!("backfill::{}(&workspace", "reconcile");
        let calls = src
            .lines()
            .map(str::trim_start)
            .filter(|l| l.contains(&needle))
            .count();
        assert!(
            calls >= 2,
            "backfill slices are reconciled at {calls} of the two server start paths, so a              backfill killed mid-run stays stuck in `running` and can never be retried"
        );
        // And the run-receipt twin is still there, so this test cannot pass by
        // one having replaced the other.
        assert!(
            src.contains(&format!("retry::{}(&workspace", "reconcile")),
            "run receipts are no longer reconciled at startup"
        );
    }

}

#[cfg(test)]
mod freshness_tick {
    /// #304 asks for freshness to be evaluated "periodically in the
    /// server/scheduler, not only at the end of a run", and the module that
    /// does the evaluating had exactly one caller in the whole repo: a CLI
    /// subcommand. So the feature existed as a computation nobody performed -
    /// an SLA that only holds while an operator remembers to ask about it is
    /// not one.
    ///
    /// Read from the source because the symptom is silence: nothing fails, no
    /// alert arrives, and the asset that went stale looks exactly like the ones
    /// that did not. Needles are built from pieces so they cannot match
    /// themselves in this file.
    #[test]
    fn the_server_judges_freshness_on_a_clock() {
        let src = include_str!("serve.rs");
        let call = format!("sla::{}(", "check_and_alert");
        assert!(
            src.contains(&call),
            "the server never evaluates freshness, so a declared SLA is only checked when \
             someone runs the CLI by hand"
        );
        // And on its own cadence rather than every scheduler tick, which would
        // read run history for every asset several times a minute to learn
        // nothing.
        assert!(
            src.contains("FRESHNESS_EVERY"),
            "freshness is evaluated without a cadence of its own"
        );
        // Alerting is what makes it visible; evaluating and discarding would be
        // the same silence with more CPU.
        assert!(
            src.contains("past their freshness limit"),
            "a stale asset is found and then not reported anywhere"
        );
    }
}
