//! duckle-runner: headless execution of a Duckle pipeline file.
//!
//! Runs a pipeline standalone on a server with no desktop app. It also serves
//! as the clean stub at the front of a "Build Pipeline" single-file artifact:
//! when this executable carries a self-extracting payload trailer it extracts
//! and runs the embedded pipeline directly (see selfextract + run_artifact),
//! so the artifact is invoked by double-click or `./<pipeline>` with no
//! wrapper script. The embedded pipeline JSON is already resolved at build
//! time (context variables substituted, routines inlined), so the runner
//! stays a thin wrapper around the engine.
//!
//! Usage:
//!   duckle-runner --pipeline <file.json> [--workspace <dir>]
//!                 [--duckdb <path>] [--log-dir <dir>] [--name <label>]
//!
//! Exit code: 0 on success, 1 on pipeline error, 2 on usage/IO error.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

mod affected_cmd;
mod migrate_cmd;
mod runsdiff_cmd;
mod sql_cmd;
mod capabilities;
mod contracts_cmd;
mod report;
mod retention;
mod audit;
mod backfill;
mod baseline;
mod cache;
mod checkpoint;
mod auth_store;
mod catalog_cmd;
mod branch;
mod build;
mod console_auth;
use duckle_duckdb_engine::context;
mod drift;
mod follow;
mod listen;
mod import;
mod manifest;
mod pipetest;
mod python;
mod selfextract;
mod work;
mod serve;

const USAGE: &str = "\
duckle-runner - run a Duckle pipeline headlessly

USAGE:
    duckle-runner --pipeline <file.json> [options]
    duckle-runner validate [<file.json> ...] [--json]
    duckle-runner quickstart [--force]
    duckle-runner mcp                      (stdio MCP server for AI agents)
    duckle-runner test [<file.test.json> ...]
    duckle-runner cache <list|clear>       (stage outputs kept for reuse)
    duckle-runner python <check|prepare>   (the workspace's Python environment)

TEST:
    Run a pipeline against a fixed input and assert the rows out of one node.
    validate catches what will not compile; this catches a transform that
    compiles and computes the wrong thing.

    A case names the node it asserts on, so the run STOPS there: nothing
    downstream executes and no sink writes. `given` maps a source node id to
    the text it should read, so the case exercises the real reader.

    With no path, every *.test.json under ./tests. Exit 1 on a failed
    assertion, the same code a failed run uses.

EXIT CODES (stable, safe to gate CI on):
    0    success
    1    the work ran and reported failure (a pipeline failed, or a
         validated pipeline did not compile). A real finding.
    2    the runner could not start the work: bad usage, unreadable
         file, missing engine. Not a finding about your data.

VALIDATE:
    Compiles pipelines to SQL without opening a source or writing a
    sink, so it needs no DuckDB binary, no credentials and no network.
    With no path it checks every .json under ./pipelines.

    It catches: malformed JSON, unknown or preview-only components,
    missing wiring (a transform with no input), and anything that fails
    to compile.
    It does NOT yet catch every missing required property value, so a
    clean validate is not proof that a run will succeed.

OPTIONS:
    --pipeline <path>    Pipeline JSON to execute (required)
    --workspace <dir>    Workspace root (default: pipeline file's parent).
                         Exposed as DUCKLE_WORKSPACE for child-job and
                         incremental-state resolution.
    --duckdb <path>      DuckDB CLI binary. Resolution order if omitted:
                         DUCKLE_DUCKDB_BIN, then bin/duckdb next to this
                         runner, then 'duckdb' on PATH.
    --log-dir <dir>      Run-log directory (default: <workspace>/logs)

  RESOURCE BUDGET (a shared machine should not be at the mercy of one job)
    --memory-limit <sz>  e.g. 24GB. Above this DuckDB spills to disk rather
                         than growing until the OS kills it.
    --threads <n>        CPU threads. Default is every core, which starves
                         anything else running alongside.
    --temp-dir <dir>     Where spill goes. Each run gets its own subdirectory.
    --max-temp-size <sz> e.g. 300GB. DuckDB's own default is 90% of the disk,
                         so without this one large join can fill the volume
                         the OS is on.
    --no-cache           Ignore any reused stage output for this run (see
                         `cache`). Nothing is read from or written to it, so a
                         run taken to check the cache does not overwrite it.
    --name <label>       Run-log + state folder name (default: pipeline file stem)
    --target <node>      Run only as far as this node, then stop and print its rows
                         (tab-separated, header first). Nothing downstream runs, so
                         no sink past it writes. The same run-from-here the desktop
                         preview uses - useful for checking one step without running
                         the rest of the pipeline.
    --manifest           After a successful run, write a signed .ducklock
                         provenance manifest under <workspace>/manifests/
                         (also enabled by the DUCKLE_MANIFEST env var).
    --verify-manifest <path>
                         Verify a .ducklock manifest signature and exit.

BACKFILL (manage xf.incremental / src.ducklake.changes saved state, then exit
without running). Resolve the state folder from --name or the pipeline stem,
under --workspace (or the pipeline's parent):
    --list-watermarks            Print saved watermarks/snapshots and exit
    --set-watermark <node=value> Set an incremental watermark; repeatable
    --watermark-type <SQLTYPE>   SQL type for the next --set-watermark (default VARCHAR)
    --set-snapshot <node=id>     Set a DuckLake CDC snapshot id; repeatable
    --clear-watermark <node>     Delete a node's saved state (forces full reload); repeatable

    -h, --help           Print this help";

struct Args {
    pipeline: Option<PathBuf>,
    workspace: Option<PathBuf>,
    duckdb: Option<PathBuf>,
    log_dir: Option<PathBuf>,
    name: Option<String>,
    /// Run only as far as this node, then stop and show what it produced.
    target: Option<String>,
    list_watermarks: bool,
    // (node, value, sql_type) incremental sets, in order.
    set_watermarks: Vec<(String, String, String)>,
    // (node, snapshot_id) CDC sets.
    set_snapshots: Vec<(String, u64)>,
    clear_watermarks: Vec<String>,
    manifest: bool,
    verify_manifest: Option<PathBuf>,
    /// #305: the run this one is retrying, recorded on the receipt so the two
    /// are linked. `None` for an ordinary run.
    retry_of: Option<String>,
}

impl Args {
    /// True when any backfill flag was given - run() does the state op and exits.
    fn is_backfill(&self) -> bool {
        self.list_watermarks
            || !self.set_watermarks.is_empty()
            || !self.set_snapshots.is_empty()
            || !self.clear_watermarks.is_empty()
    }
}

/// Render the usage text under the name the binary was actually invoked as.
///
/// The same executable ships as `duckle-runner` (embedded in the desktop app,
/// and as a release asset) and as `duckle` (both the pip wheel and
/// scripts/install.sh place it under that name). Help that always said
/// "duckle-runner" would tell a pip user to type a command they do not have.
fn usage_for_invocation() -> String {
    let prog = std::env::args()
        .next()
        .map(std::path::PathBuf::from)
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_default();
    // Only swap for a name we recognise, so an oddly renamed copy still shows
    // the canonical help rather than something confusing.
    if prog == "duckle" {
        USAGE.replace("duckle-runner", "duckle")
    } else {
        USAGE.to_string()
    }
}

fn parse_args() -> Result<Args, String> {
    let mut pipeline = None;
    let mut workspace = None;
    let mut duckdb = None;
    let mut log_dir = None;
    let mut name = None;
    let mut target: Option<String> = None;
    let mut list_watermarks = false;
    let mut set_watermarks = Vec::new();
    let mut set_snapshots = Vec::new();
    let mut clear_watermarks = Vec::new();
    let mut manifest = false;
    let mut verify_manifest = None;
    // SQL type applied to the NEXT --set-watermark (so it can precede it).
    let mut pending_type = String::from("VARCHAR");
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut take = |label: &str| {
            it.next()
                .ok_or_else(|| format!("{} needs a value", label))
        };
        match arg.as_str() {
            "--pipeline" => pipeline = Some(PathBuf::from(take("--pipeline")?)),
            "--workspace" => workspace = Some(PathBuf::from(take("--workspace")?)),
            "--duckdb" => duckdb = Some(PathBuf::from(take("--duckdb")?)),
            "--log-dir" => log_dir = Some(PathBuf::from(take("--log-dir")?)),
            // Resource budget for this run. These set the same environment
            // variables the engine already reads, rather than a second
            // mechanism, so a flag, a workspace-wide export and a per-stage
            // setting all end up in one place. A flag is what makes them
            // usable on a shared server: capping one pipeline should not mean
            // exporting a variable that every other process on the box sees.
            "--memory-limit" => std::env::set_var("DUCKLE_MEMORY_LIMIT", take("--memory-limit")?),
            "--threads" => std::env::set_var("DUCKLE_THREADS", take("--threads")?),
            "--temp-dir" => std::env::set_var("DUCKLE_TEMP_DIR", take("--temp-dir")?),
            "--max-temp-size" => {
                std::env::set_var("DUCKLE_MAX_TEMP_DIR_SIZE", take("--max-temp-size")?)
            }
            // Distrust the reuse cache for this run without editing the
            // pipeline or dropping what is stored. Neither reads nor writes,
            // so a run taken to settle whether the cache is lying does not
            // then overwrite the evidence.
            "--no-cache" => std::env::set_var("DUCKLE_NO_CACHE", "1"),
            "--name" => name = Some(take("--name")?),
            "--target" => target = Some(take("--target")?),
            "--list-watermarks" => list_watermarks = true,
            "--watermark-type" => pending_type = take("--watermark-type")?,
            "--set-watermark" => {
                let spec = take("--set-watermark")?;
                let (node, value) = spec
                    .split_once('=')
                    .ok_or_else(|| format!("--set-watermark expects node=value, got '{}'", spec))?;
                set_watermarks.push((node.to_string(), value.to_string(), pending_type.clone()));
                pending_type = String::from("VARCHAR");
            }
            "--set-snapshot" => {
                let spec = take("--set-snapshot")?;
                let (node, id) = spec
                    .split_once('=')
                    .ok_or_else(|| format!("--set-snapshot expects node=id, got '{}'", spec))?;
                let id: u64 = id
                    .trim()
                    .parse()
                    .map_err(|_| format!("--set-snapshot id must be a number, got '{}'", id))?;
                set_snapshots.push((node.to_string(), id));
            }
            "--clear-watermark" => clear_watermarks.push(take("--clear-watermark")?),
            "--manifest" => manifest = true,
            "--verify-manifest" => {
                verify_manifest = Some(PathBuf::from(take("--verify-manifest")?))
            }
            "-h" | "--help" => {
                println!("{}", usage_for_invocation());
                std::process::exit(0);
            }
            // Allow a bare pipeline path as the first positional argument.
            other if pipeline.is_none() && !other.starts_with('-') => {
                pipeline = Some(PathBuf::from(other));
            }
            other => return Err(format!("unknown argument: {}", other)),
        }
    }
    Ok(Args {
        retry_of: None,
        target,
        pipeline,
        workspace,
        duckdb,
        log_dir,
        name,
        list_watermarks,
        set_watermarks,
        set_snapshots,
        clear_watermarks,
        manifest,
        verify_manifest,
    })
}

/// Find the DuckDB CLI: explicit flag, then env, then a sibling bin/duckdb
/// (how the build bundle ships it), then PATH.
pub(crate) fn resolve_duckdb(flag: Option<PathBuf>) -> Result<PathBuf, String> {
    if let Some(p) = flag {
        if p.exists() {
            return Ok(p);
        }
        // A bundle's run.sh passes bin/duckdb without an extension; on a
        // Windows bundle the file is duckdb.exe. Try the .exe sibling before
        // giving up so the POSIX launcher works under git-bash / WSL too.
        if p.extension().is_none() {
            let exe = p.with_extension("exe");
            if exe.exists() {
                return Ok(exe);
            }
        }
        return Err(format!("--duckdb path does not exist: {}", p.display()));
    }
    if let Ok(env) = std::env::var("DUCKLE_DUCKDB_BIN") {
        let p = PathBuf::from(env);
        if p.exists() {
            return Ok(p);
        }
    }
    // bin/duckdb(.exe) next to this runner (the bundle layout).
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            for cand in ["duckdb", "duckdb.exe"] {
                let p = dir.join(cand);
                if p.exists() {
                    return Ok(p);
                }
            }
        }
    }
    // Fall back to PATH; the engine spawns it by name.
    Ok(PathBuf::from("duckdb"))
}

/// Resolve the run/state folder name: --name, else the pipeline file stem.
fn resolve_name(args: &Args) -> Result<String, String> {
    if let Some(n) = &args.name {
        return Ok(n.clone());
    }
    args.pipeline
        .as_ref()
        .and_then(|p| p.file_stem())
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or_else(|| "need --name or --pipeline to resolve the state folder".to_string())
}

/// Resolve the workspace root: --workspace, else the pipeline file's parent.
fn resolve_workspace(args: &Args) -> PathBuf {
    args.workspace
        .clone()
        .or_else(|| args.pipeline.as_ref().and_then(|p| p.parent().map(Path::to_path_buf)))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Apply the backfill flags (set/clear/list watermarks) and return without
/// running the pipeline. Resolves the state folder from --name / pipeline stem
/// under the workspace, the same layout a real run reads.
fn run_backfill(args: &Args) -> Result<bool, String> {
    use duckle_duckdb_engine::watermark;
    let workspace = resolve_workspace(args);
    let name = resolve_name(args)?;

    for (node, value, ty) in &args.set_watermarks {
        watermark::set_incremental(&workspace, &name, node, value, Some(ty))
            .map_err(|e| format!("set watermark {}: {}", node, e))?;
        println!("set watermark  {} = {} ({})", node, value, ty);
    }
    for (node, id) in &args.set_snapshots {
        watermark::set_snapshot(&workspace, &name, node, *id)
            .map_err(|e| format!("set snapshot {}: {}", node, e))?;
        println!("set snapshot   {} = {}", node, id);
    }
    for node in &args.clear_watermarks {
        watermark::clear(&workspace, &name, node)
            .map_err(|e| format!("clear watermark {}: {}", node, e))?;
        println!("cleared        {}", node);
    }
    if args.list_watermarks {
        let entries = watermark::list(&workspace, &name);
        if entries.is_empty() {
            println!("(no saved watermarks for '{}' under {})", name, workspace.display());
        } else {
            println!("saved watermarks for '{}':", name);
            for e in entries {
                match e.value_type {
                    Some(t) => println!("  {:24} {} = {} ({})", e.node_id, e.kind, e.value, t),
                    None => println!("  {:24} {} = {}", e.node_id, e.kind, e.value),
                }
            }
        }
    }
    Ok(true)
}

fn run() -> Result<bool, String> {
    run_with(parse_args()?)
}

/// The run itself, given already-parsed arguments. Split out so `retry` can
/// drive the same path with arguments it built from a receipt rather than from
/// the command line (#305).
fn run_with(args: Args) -> Result<bool, String> {

    // Backfill flags short-circuit: manage saved watermark/snapshot state and
    // exit without running the pipeline.
    if args.is_backfill() {
        return run_backfill(&args);
    }

    // Verify a manifest and exit, without running anything.
    if let Some(p) = &args.verify_manifest {
        let ok = manifest::verify_manifest(p)?;
        println!("manifest : {}", if ok { "valid" } else { "INVALID" });
        return Ok(ok);
    }

    let pipeline = args
        .pipeline
        .clone()
        .ok_or_else(|| "--pipeline is required".to_string())?;
    if !pipeline.exists() {
        return Err(format!("pipeline file not found: {}", pipeline.display()));
    }
    let text = std::fs::read_to_string(&pipeline)
        .map_err(|e| format!("read {}: {}", pipeline.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text)
        .map_err(|e| format!("parse {}: {}", pipeline.display(), e))?;
    // #305: taken HERE, before the resolution passes below. apply_time_builtins
    // stamps a fresh date into the document on every run, so a hash taken after
    // it would differ daily and call an unchanged pipeline changed.
    let pipeline_hash = duckle_duckdb_engine::retry::pipeline_hash(&doc);

    // Workspace defaults to the pipeline file's directory. Pre-fetched
    // DuckDB extensions and incremental state live relative to it.
    let workspace = args
        .workspace
        .clone()
        .or_else(|| pipeline.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));

    // Expand saved Salesforce connection refs into node auth props (#166
    // stage 2) BEFORE the env pass, decrypting connections/<id>.json with the
    // workspace key - same shared crate the desktop app uses, so a
    // connection field stored as ${ENV:...} still resolves below.
    duckle_secrets::resolve_connection_refs(&workspace, &mut doc.nodes)?;
    // Runtime ${ENV:KEY} substitution. A built bundle ships ${ENV:KEY}
    // placeholders in place of secrets; resolve them now from the
    // environment, then secrets.env, then a decrypted secrets.enc.
    let env_file = workspace.join("secrets.env");
    apply_env_pass(&mut doc, &workspace, &env_file)?;
    // Stamp the dynamic date/time builtins (${date}/${datetime}/...) at run
    // time. A built bundle deliberately ships these unresolved so each run
    // (e.g. a daily cron of the same artifact) writes a fresh-dated path.
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    // Resolve ${workspace}/${projectroot} + workspace context vars on the parent
    // (a file-loaded pipeline doesn't go through the by-id resolver, so these
    // would otherwise pass through literally; foreach children already resolve
    // them). Makes ${workspace}-relative pipelines portable in headless runs.
    context::apply_workspace_context(&mut doc, &workspace);
    let log_dir = args.log_dir.clone().unwrap_or_else(|| workspace.join("logs"));
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", &log_dir);

    let duckdb = resolve_duckdb(args.duckdb)?;
    let name = args.name.clone().unwrap_or_else(|| {
        pipeline
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "pipeline".into())
    });

    eprintln!("duckle-runner: {} (workspace {})", pipeline.display(), workspace.display());
    // No canvas here, so per-node preview rows have nobody to show them to:
    // a headless run reads them off the wire only to drop them.
    // Run only as far as one node, and SHOW what it produced. Without the rows there is
    // nothing to look at, so this is the one case where a headless run keeps its
    // previews: stopping early is only useful if you can see where you stopped.
    let target = args.target.clone();
    let engine = match target.is_some() {
        true => DuckdbEngine::new(duckdb),
        false => DuckdbEngine::new(duckdb).without_previews(),
    };
    // #259: identity before work. A run killed here still exists to be found,
    // and `reconcile` can later tell it apart from one that finished.
    let trigger = if args.retry_of.is_some() { "retry" } else { "manual" };
    let run_id = duckle_duckdb_engine::retry::new_run_id(&name, trigger);
    println!("run id   : {run_id}");
    let receipt = duckle_duckdb_engine::retry::begin(
        &workspace,
        &run_id,
        trigger,
        &name,
        &pipeline.display().to_string(),
        &pipeline_hash,
        args.retry_of.clone(),
    );

    // #259: the engine logs under the id the receipt was written with, so a
    // run's log lines join to its receipt and its history record.
    let engine = engine.with_run_id(&receipt.run_id);
    // #259: the engine logs under the id the receipt was written with, so a
    // run's log lines join to its receipt and its history record.
    let engine = engine.with_run_id(&receipt.run_id);
    let result = match target.as_deref() {
        Some(t) => engine.execute_pipeline_with_events(&doc, Some(t), Some(&name), |_| {}),
        None => engine.execute_pipeline_named(&doc, &name),
    };

    // #259: the run is recorded BEFORE the result is printed, and the id is
    // minted before the work above ran - see where `receipt` is created.
    let run_id = receipt.run_id.clone();
    duckle_duckdb_engine::retry::finish(
        &workspace,
        receipt,
        &result.status,
        duckle_duckdb_engine::retry::nodes_of(&result),
    );

    // #309: the console, the scheduler and the desktop all append a run-history
    // record; the bare CLI was the only run surface that did not, so a run
    // started here was invisible to the Runs tab, to alerting, to asset
    // freshness and to `runs diff`. Found by comparing two CLI runs and getting
    // "at least one run has no history record" for both of them.
    let mut record = duckle_duckdb_engine::RunRecord::from_result_in(
        &workspace,
        &name,
        &result,
        if target.is_some() { "partial" } else { "manual" },
    );
    record.run_id = Some(run_id);
    duckle_duckdb_engine::append_run_record(&workspace, &name, record);

    println!("status   : {}", result.status);
    println!("duration : {} ms", result.duration_ms);
    if let Some(err) = &result.error {
        println!("error    : {err}");
    }
    // #258: a run that stopped at a ceiling is not a failure, and must not read
    // like a clean success either. Everything downstream was skipped, so the
    // sinks hold what they held before.
    if result.incomplete {
        println!(
            "incomplete: {} - the rows produced are correct and are not all of them; nothing downstream ran",
            result.incomplete_reason.as_deref().unwrap_or("stopped early")
        );
    }
    for (id, st) in &result.nodes {
        let rows = st.rows.map(|r| format!(" ({r} rows)")).unwrap_or_default();
        // What the stage said about itself - which page it stopped at, that it
        // found nothing to do, that it reused a cached output. Headless is
        // where this matters most: with no panel to open, a run that skipped
        // the work would otherwise look exactly like one that did it.
        let note = st
            .note
            .as_deref()
            .filter(|n| !n.trim().is_empty())
            .map(|n| format!(" - {n}"))
            .unwrap_or_default();
        println!("  {:20} {}{}{}", id, st.status, rows, note);
    }

    // Stopping at a node is only useful if you can see what it produced, so its rows go
    // out here. Tab-separated, header first: enough for a person to read and for a
    // script to cut on, without pretending to be a data format.
    if let Some(t) = target.as_deref() {
        match result.preview.iter().find(|p| p.node_id == t) {
            Some(p) => {
                println!();
                println!("{}", p.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join("	"));
                for row in &p.rows {
                    let cells: Vec<String> = p
                        .columns
                        .iter()
                        .map(|c| match row.get(&c.name) {
                            Some(serde_json::Value::String(s)) => s.clone(),
                            Some(serde_json::Value::Null) | None => String::new(),
                            Some(v) => v.to_string(),
                        })
                        .collect();
                    println!("{}", cells.join("	"));
                }
            }
            None if result.status == "ok" => {
                println!();
                println!("(no rows to show for {t}: it produces no output relation)");
            }
            None => {}
        }
    }

    // Emit a signed provenance manifest for a successful run when asked.
    if result.status == "ok" && (args.manifest || std::env::var_os("DUCKLE_MANIFEST").is_some()) {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        // Best-effort column lineage to embed in the signed artifact; a
        // resolution failure just omits it rather than failing the manifest.
        let lineage = engine.pipeline_column_lineage(&doc).ok();
        // Per-node run outcome (rows + status) so the manifest attests to what
        // the run produced. result.nodes is a BTreeMap, so this is deterministic.
        let outputs: Vec<manifest::NodeOutcome> = result
            .nodes
            .iter()
            .map(|(id, st)| manifest::NodeOutcome {
                node: id.clone(),
                status: st.status.clone(),
                rows: st.rows,
            })
            .collect();
        // Fingerprint the source files this run read, so the signed artifact
        // pins its inputs as well as its plan and outputs.
        let inputs = collect_input_fingerprints(&doc);
        match manifest::write_manifest(
            &workspace,
            &name,
            &doc,
            &result.status,
            result.duration_ms,
            stamp,
            lineage,
            &outputs,
            &inputs,
            &result.artifacts,
            result.artifacts_truncated,
        ) {
            Ok(path) => println!("manifest : {}", path.display()),
            Err(e) => eprintln!("manifest : skipped ({e})"),
        }
    }

    Ok(result.status == "ok")
}

/// Parse a KEY=VALUE file (secrets.env shape) into a map. Skips empty and
/// `#`-comment lines; splits on the FIRST `=`; trims the KEY; trims only a
/// trailing CR off the VALUE (handles CRLF).
fn parse_env_file(text: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim().to_string();
            if key.is_empty() {
                continue;
            }
            let val = v.strip_suffix('\r').unwrap_or(v).to_string();
            out.insert(key, val);
        }
    }
    out
}

/// Decrypt `<workspace>/secrets.enc` under DUCKLE_BUNDLE_PASSPHRASE and
/// parse it into a KEY=VALUE map. Hard-fails (exit 2) when the file is
/// present but the passphrase is unset, the blob is corrupt, or the tag
/// fails - never silently falls through to unresolved placeholders.
fn load_secrets_enc(workspace: &Path) -> Result<Option<HashMap<String, String>>, String> {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
    use base64::Engine as _;
    use sha2::{Digest, Sha256};

    let path = workspace.join("secrets.enc");
    if !path.exists() {
        return Ok(None);
    }
    let passphrase = std::env::var("DUCKLE_BUNDLE_PASSPHRASE")
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| "secrets.enc present but DUCKLE_BUNDLE_PASSPHRASE is not set".to_string())?;

    let b64 = std::fs::read_to_string(&path)
        .map_err(|e| format!("read {}: {}", path.display(), e))?;
    let payload = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| format!("decode secrets.enc: {}", e))?;
    // Two formats. A payload tagged with the magic carries a per-bundle salt and was
    // keyed with Argon2id. An untagged one is the original scheme, whose key was an
    // unsalted single SHA-256 of the passphrase; it is still read so bundles built
    // before the change keep working, and it is the reason the magic exists at all.
    let magic = crate::build::BUNDLE_MAGIC;
    let salt_len = crate::build::BUNDLE_SALT_LEN;
    let (key, nonce_bytes, ciphertext): (Vec<u8>, &[u8], &[u8]) = if payload.starts_with(magic) {
        let rest = &payload[magic.len()..];
        if rest.len() < salt_len + 12 + 16 {
            return Err("secrets.enc is corrupt (too short)".to_string());
        }
        let (salt, rest) = rest.split_at(salt_len);
        let (nonce_bytes, ciphertext) = rest.split_at(12);
        let key = crate::build::derive_bundle_key(&passphrase, salt)?;
        (key.to_vec(), nonce_bytes, ciphertext)
    } else {
        if payload.len() < 12 + 16 {
            return Err("secrets.enc is corrupt (too short)".to_string());
        }
        let (nonce_bytes, ciphertext) = payload.split_at(12);
        (Sha256::digest(passphrase.as_bytes()).to_vec(), nonce_bytes, ciphertext)
    };
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| format!("cipher init: {}", e))?;
    let nonce = Nonce::try_from(nonce_bytes).map_err(|e| format!("nonce: {}", e))?;
    let plain = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| "wrong DUCKLE_BUNDLE_PASSPHRASE or corrupt secrets.enc".to_string())?;
    let text = String::from_utf8(plain).map_err(|e| format!("secrets.enc not UTF-8: {}", e))?;
    Ok(Some(parse_env_file(&text)))
}

/// Substitute `${ENV:NAME}` placeholders across every node's properties.
/// Precedence per NAME: real process env, then secrets.env (read from
/// `env_path`), then a decrypted `<workspace>/secrets.enc`. A miss leaves the
/// literal placeholder and warns once per distinct missing NAME.
///
/// `env_path` is passed explicitly (rather than derived from `workspace`) so
/// the artifact path can point it at an operator-supplied secrets.env sitting
/// next to the exe / in CWD WITHOUT copying that plaintext file into the
/// shared, persistent extraction cache.
pub(crate) fn apply_env_pass(doc: &mut PipelineDoc, workspace: &Path, env_path: &Path) -> Result<(), String> {
    // Secrets held in an external vault are fetched first, so a value that
    // came from the vault is in place before anything reads the properties.
    duckle_duckdb_engine::context::apply_vault(doc);
    // File/enc map: secrets.env first, secrets.enc overlaying. Real env is
    // checked first at lookup time so it always wins.
    let mut file_map: HashMap<String, String> = HashMap::new();
    if let Ok(text) = std::fs::read_to_string(env_path) {
        file_map = parse_env_file(&text);
    }
    if let Some(enc) = load_secrets_enc(workspace)? {
        for (k, v) in enc {
            file_map.insert(k, v);
        }
    }

    let re = regex::Regex::new(r"\$\{ENV:([^}]+)\}").map_err(|e| e.to_string())?;
    // RefCell so the (shared) closure can record warnings without becoming
    // FnMut (substitute_deep takes &impl Fn).
    let warned = std::cell::RefCell::new(std::collections::HashSet::<String>::new());
    let lookup = |name: &str| -> Option<String> {
        if let Ok(v) = std::env::var(name) {
            return Some(v);
        }
        file_map.get(name).cloned()
    };
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| {
            let name = caps[1].trim();
            match lookup(name) {
                Some(v) => v,
                None => {
                    if warned.borrow_mut().insert(name.to_string()) {
                        eprintln!("duckle-runner: ${{ENV:{}}} is unresolved (set it in the environment or secrets.env)", name);
                    }
                    caps[0].to_string()
                }
            }
        })
        .into_owned()
    };

    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            context::substitute_deep(props, &replace);
        }
    }
    Ok(())
}

/// Execute an embedded pipeline payload (the artifact case): extract the
/// payload to a per-artifact temp cache, point DuckDB at the bundled binary
/// + extensions, resolve `${ENV:KEY}` placeholders, run the pipeline, and
/// return its status as the process exit code (0 ok, 1 pipeline error, 2
/// setup/IO error).
fn run_artifact(payload: Vec<u8>) -> ExitCode {
    let root = match selfextract::extract_to_cache(&payload) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("duckle-runner: {e}");
            return ExitCode::from(2);
        }
    };

    // Point at the embedded duckdb + its extensions. The engine spawns
    // duckdb without env_clear, so DUCKLE_DUCKDB_BIN and HOME/USERPROFILE
    // set here are inherited by the spawned child, which resolves extensions
    // under <home>/.duckdb/extensions.
    let duckdb_name = if cfg!(windows) { "duckdb.exe" } else { "duckdb" };
    let duckdb = root.join("bin").join(duckdb_name);
    std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
    let binhome = root.join("bin");
    if cfg!(windows) {
        std::env::set_var("USERPROFILE", &binhome);
    } else {
        std::env::set_var("HOME", &binhome);
    }

    // Locate the single pipeline json under root/pipeline/.
    let pipeline = match find_pipeline_json(&root) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle-runner: {e}");
            return ExitCode::from(2);
        }
    };
    let name = pipeline
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pipeline".into());

    // #145: the run-host workspace. Honor an operator-supplied DUCKLE_WORKSPACE
    // so ${workspace}-relative paths in a portable artifact point at the real
    // data dir on this machine; fall back to the extraction root for a fully
    // self-contained bundle. Mirror run()'s env wiring off the same root.
    let ws_root = std::env::var_os("DUCKLE_WORKSPACE")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| root.clone());
    std::env::set_var("DUCKLE_WORKSPACE", &ws_root);
    std::env::set_var("DUCKLE_LOG_DIR", ws_root.join("logs"));

    // Resolve the operator-supplied secrets.env PER INVOCATION: next to the
    // artifact exe first, then CWD. It is read at its real location and never
    // copied into the shared, hash-keyed extraction cache - copying it there
    // would (1) bake plaintext secrets into a persistent temp dir shared by
    // every run of this artifact, and (2) make a later run from a different
    // directory silently reuse the first run's secrets. Real process env still
    // wins over the file at lookup time.
    let mut env_file = PathBuf::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let cand = dir.join("secrets.env");
            if cand.exists() {
                env_file = cand;
            }
        }
    }
    if env_file.as_os_str().is_empty() {
        if let Ok(cwd) = std::env::current_dir() {
            let cand = cwd.join("secrets.env");
            if cand.exists() {
                env_file = cand;
            }
        }
    }

    let text = match std::fs::read_to_string(&pipeline) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("duckle-runner: read {}: {}", pipeline.display(), e);
            return ExitCode::from(2);
        }
    };
    let mut doc: PipelineDoc = match serde_json::from_str(&text) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner: parse {}: {}", pipeline.display(), e);
            return ExitCode::from(2);
        }
    };
    // Saved Salesforce connection refs resolve against the run-host workspace
    // (#166 stage 2); artifacts normally ship ${ENV:} placeholders instead,
    // but a ref-only pipeline run with DUCKLE_WORKSPACE pointed at a real
    // workspace works, and an unresolvable ref fails with a clear error here
    // rather than a downstream auth failure.
    if let Err(e) = duckle_secrets::resolve_connection_refs(&ws_root, &mut doc.nodes) {
        eprintln!("duckle-runner: {e}");
        return ExitCode::from(2);
    }
    if let Err(e) = apply_env_pass(&mut doc, &root, &env_file) {
        eprintln!("duckle-runner: {e}");
        return ExitCode::from(2);
    }
    // #145: the build ships ${workspace} / ${projectroot} / ${date} as
    // placeholders (resolve_workspace_portable). Re-resolve them here against the
    // run-host workspace, exactly as run() does for file-loaded pipelines, so a
    // cross-OS artifact resolves correct paths instead of the build host's.
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    context::apply_workspace_context(&mut doc, &ws_root);

    eprintln!("duckle-runner: {} (artifact, workspace {})", pipeline.display(), ws_root.display());
    // No canvas here, so per-node preview rows have nobody to show them to:
    // a headless run reads them off the wire only to drop them.
    let engine = DuckdbEngine::new(duckdb).without_previews();
    let result = engine.execute_pipeline_named(&doc, &name);

    println!("status   : {}", result.status);
    println!("duration : {} ms", result.duration_ms);
    if let Some(err) = &result.error {
        println!("error    : {err}");
    }
    // #258: a run that stopped at a ceiling is not a failure, and must not read
    // like a clean success either. Everything downstream was skipped, so the
    // sinks hold what they held before.
    if result.incomplete {
        println!(
            "incomplete: {} - the rows produced are correct and are not all of them; nothing downstream ran",
            result.incomplete_reason.as_deref().unwrap_or("stopped early")
        );
    }
    for (id, st) in &result.nodes {
        let rows = st.rows.map(|r| format!(" ({r} rows)")).unwrap_or_default();
        // What the stage said about itself - which page it stopped at, that it
        // found nothing to do, that it reused a cached output. Headless is
        // where this matters most: with no panel to open, a run that skipped
        // the work would otherwise look exactly like one that did it.
        let note = st
            .note
            .as_deref()
            .filter(|n| !n.trim().is_empty())
            .map(|n| format!(" - {n}"))
            .unwrap_or_default();
        println!("  {:20} {}{}{}", id, st.status, rows, note);
    }
    ExitCode::from(if result.status == "ok" { 0 } else { 1 })
}

/// Find the single `*.json` pipeline file under `<root>/pipeline/`.
fn find_pipeline_json(root: &Path) -> Result<PathBuf, String> {
    let dir = root.join("pipeline");
    let entries =
        std::fs::read_dir(&dir).map_err(|e| format!("read_dir {}: {}", dir.display(), e))?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            return Ok(path);
        }
    }
    Err(format!("no pipeline json found under {}", dir.display()))
}

const REVIEW_USAGE: &str = "\
duckle-runner review - review a pipeline change before merging it

USAGE:
    duckle-runner review --before <old.json> --after <new.json> [options]

OPTIONS:
    --json                 Emit the full report as JSON.
    --data                 Also run both versions and diff the data (per-node
                           row counts). Sinks are stripped before running, so no
                           destination is written; sources are read and
                           transforms run. Needs a DuckDB binary.
    --drift                Also check the --after version's sources for schema
                           drift: read each source's live schema and compare it
                           to the declared one. Needs a DuckDB binary.
    --duckdb <path>        DuckDB CLI for --data/--drift (else DUCKLE_DUCKDB_BIN
                           / PATH).
    --workspace <dir>      Workspace root for --data/--drift placeholder/secret
                           resolution (default: the --before file's directory).

Without --data/--drift the review is static and read-only (nothing is executed,
no DuckDB binary needed): it reports nodes added/removed/changed, edges, whether
the compiled SQL changed, and whether each version still compiles.

Exit code: 0 reviewed, 1 the --after version fails to compile (or, with --data,
fails to run; with --drift, a source drifted in a breaking way), 2 usage/IO
error.";

/// Fingerprint each local-file source the run read, for the provenance manifest.
/// Files at or under the cap are content-hashed; larger ones record size only.
/// Non-file sources (databases, cloud, globs) are skipped.
fn collect_input_fingerprints(doc: &PipelineDoc) -> Vec<manifest::InputFingerprint> {
    const HASH_CAP: u64 = 256 * 1024 * 1024; // 256 MiB
    let mut out = Vec::new();
    for n in &doc.nodes {
        if !n.data.component_id.as_deref().unwrap_or("").starts_with("src.") {
            continue;
        }
        let path = match n.data.properties.as_ref().and_then(|p| p.get("path")).and_then(|v| v.as_str())
        {
            Some(p) if !p.is_empty() => p,
            _ => continue,
        };
        let meta = match std::fs::metadata(path) {
            Ok(m) if m.is_file() => m,
            _ => continue,
        };
        let bytes = meta.len();
        let sha256 = if bytes <= HASH_CAP {
            std::fs::read(path).ok().map(|b| manifest::sha256_hex(&b))
        } else {
            None
        };
        out.push(manifest::InputFingerprint { node: n.id.clone(), path: path.to_string(), bytes, sha256 });
    }
    out
}

/// Run one side of a `review --data` comparison sink-safely: every sink node is
/// removed before execution, so sources are read and transforms run but no
/// destination is ever written. Returns each surviving node's row count.
/// `mcp` - hand off to the MCP server sitting next to this binary.
///
/// Exists so the agent entry point is a plain `uvx duckle mcp` rather than
/// `uvx --from duckle duckle-mcp`. uvx maps its first argument to both the
/// package and the command, so a command whose name differs from the package
/// needs --from; routing through the `duckle` command avoids that entirely.
/// The MCP server speaks JSON-RPC on stdio, so this must exec rather than
/// wrap: nothing may be written to stdout here.
fn run_mcp() -> ExitCode {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle mcp: locating this executable: {e}");
            return ExitCode::from(2);
        }
    };
    let name = if cfg!(windows) { "duckle-mcp.exe" } else { "duckle-mcp" };
    let server = match exe.parent().map(|d| d.join(name)) {
        Some(p) if p.exists() => p,
        _ => {
            eprintln!(
                "duckle mcp: {name} not found next to {}.\n\
                 The pip package ships it; a source build needs \
                 `cargo build -p duckle-mcp` first.",
                exe.display()
            );
            return ExitCode::from(2);
        }
    };
    let mut cmd = std::process::Command::new(&server);
    cmd.args(std::env::args_os().skip(2));
    match cmd.status() {
        Ok(s) => ExitCode::from(s.code().unwrap_or(1) as u8),
        Err(e) => {
            eprintln!("duckle mcp: starting {}: {e}", server.display());
            ExitCode::from(2)
        }
    }
}

/// `quickstart` - scaffold a working pipeline, run it, and show the rows.
///
/// Deliberately goes all the way to a result. The comparable onboarding
/// commands (create-next-app, dlthub-start, npm create astro) scaffold a
/// folder and then hand you a second command to run; Duckle can finish the
/// job in one because the engine ships in the same install. Someone typing
/// `uvx duckle@latest quickstart` should see real rows, not a TODO.
fn run_quickstart() -> ExitCode {
    let mut force = false;
    for arg in std::env::args().skip(2) {
        match arg.as_str() {
            "--force" | "-f" => force = true,
            other => {
                eprintln!("duckle quickstart: unknown flag {other}");
                return ExitCode::from(2);
            }
        }
    }

    let csv = Path::new("orders.csv");
    let pipeline = Path::new("pipelines").join("quickstart.json");
    let out = Path::new("out.csv");

    if !force {
        for p in [csv, pipeline.as_path()] {
            if p.exists() {
                eprintln!(
                    "duckle quickstart: {} already exists. Re-run with --force to overwrite.",
                    p.display()
                );
                return ExitCode::from(2);
            }
        }
    }

    println!("\nDuckle quickstart\n");

    let sample = "id,region,customer,amount\n\
                  1,EU,Acme,10\n\
                  2,EU,Globex,25\n\
                  3,US,Initech,40\n\
                  4,UK,Umbrella,30\n\
                  5,US,Hooli,15\n\
                  6,EU,Soylent,55\n\
                  7,UK,Vehement,22\n\
                  8,APAC,Massive,8\n";
    if let Err(e) = std::fs::write(csv, sample) {
        eprintln!("duckle quickstart: writing {}: {e}", csv.display());
        return ExitCode::from(2);
    }
    println!("  created  {}  (8 rows of sample data)", csv.display());

    // The same JSON the desktop canvas reads, so this file opens visually.
    let doc = serde_json::json!({
        "name": "quickstart",
        "nodes": [
            { "id": "csv", "type": "source", "position": {"x": 0, "y": 0},
              "data": { "label": "Orders CSV", "componentId": "src.csv",
                        "properties": { "path": "orders.csv" } } },
            { "id": "filter", "type": "transform", "position": {"x": 220, "y": 0},
              "data": { "label": "Amount >= 20", "componentId": "xf.filter",
                        "properties": { "predicate": { "mode": "python",
                                                       "expr": "amount >= 20" } } } },
            { "id": "derive", "type": "transform", "position": {"x": 440, "y": 0},
              "data": { "label": "Add total", "componentId": "xf.pyexpr",
                        "properties": { "columns": [
                            { "name": "total", "expr": "round(amount * 1.2, 2)" },
                            { "name": "tag", "expr": "f'{region}-{customer}'" }
                        ] } } },
            { "id": "out", "type": "sink", "position": {"x": 660, "y": 0},
              "data": { "label": "Result CSV", "componentId": "snk.csv",
                        "properties": { "path": "out.csv" } } }
        ],
        "edges": [
            { "id": "e1", "source": "csv", "target": "filter", "sourceHandle": "main",
              "targetHandle": "main", "data": { "connectionType": "main" } },
            { "id": "e2", "source": "filter", "target": "derive", "sourceHandle": "main",
              "targetHandle": "main", "data": { "connectionType": "main" } },
            { "id": "e3", "source": "derive", "target": "out", "sourceHandle": "main",
              "targetHandle": "main", "data": { "connectionType": "main" } }
        ]
    });
    if let Err(e) = std::fs::create_dir_all("pipelines")
        .and_then(|_| std::fs::write(&pipeline, serde_json::to_string_pretty(&doc).unwrap_or_default()))
    {
        eprintln!("duckle quickstart: writing {}: {e}", pipeline.display());
        return ExitCode::from(2);
    }
    println!("  created  {}", pipeline.display());

    // Run it by re-invoking this same executable, so what happens here is
    // exactly what the user gets when they run the command themselves.
    println!("\nRunning {} ...\n", pipeline.display());
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle quickstart: locating this executable: {e}");
            return ExitCode::from(2);
        }
    };
    // Anchor the workspace to the current directory. Without it the workspace
    // defaults to the pipeline file's parent, so a first run would tuck its
    // logs away under pipelines/logs/ rather than beside the output.
    //
    // Output is captured rather than streamed so the child's banner line can
    // be dropped: it names the binary as "duckle-runner", which is not a
    // command a pip user has. The run takes well under a second, so nothing
    // is lost by not streaming.
    let out_res = std::process::Command::new(exe)
        .arg("--pipeline")
        .arg(&pipeline)
        .arg("--workspace")
        .arg(".")
        .output();
    let output = match out_res {
        Ok(o) => o,
        Err(e) => {
            eprintln!("duckle quickstart: running the pipeline: {e}");
            return ExitCode::from(2);
        }
    };
    for stream in [&output.stdout, &output.stderr] {
        for line in String::from_utf8_lossy(stream).lines() {
            if line.starts_with("duckle-runner: ") && line.contains("(workspace") {
                continue;
            }
            println!("{line}");
        }
    }
    if !output.status.success() {
        eprintln!(
            "\nduckle quickstart: the pipeline did not complete. If the DuckDB engine is \
             missing, install it with `pip install duckdb-cli` or set DUCKLE_DUCKDB_BIN."
        );
        return ExitCode::from(1);
    }

    // Show the actual rows. Reading the file back keeps this dependency-free
    // and proves the pipeline really wrote something.
    if let Ok(text) = std::fs::read_to_string(out) {
        println!("\n{}:\n", out.display());
        for line in text.lines().take(8) {
            println!("  {line}");
        }
    }

    println!(
        "\nNext:\n\
         \x20 duckle validate            compile-check every pipeline (no engine needed)\n\
         \x20 duckle --pipeline {}\n\
         \x20 open {} in the Duckle studio to edit it visually\n\
         \n\
         Docs: https://duckle.org   Components: duckle-mcp exposes them to AI agents\n",
        pipeline.display(),
        pipeline.display()
    );
    ExitCode::from(0)
}

/// `validate` - compile every pipeline without touching a source or a sink.
///
/// This is the CI gate: it needs no DuckDB binary, no credentials and no
/// network, because compiling only turns the graph into SQL. Exits 0 when all
/// pipelines compile, 1 when any fails to compile (a real finding, distinct
/// from the runner being misused), and 2 for a usage error.
/// `follow <pipeline> [flags]` - parse the follower's own arguments and hand
/// off to the loop. Kept separate from `parse_args` because the flags are
/// disjoint: a follower has no `--target`, and none of the backfill flags mean
/// anything mid-stream.
fn run_follow() -> Result<(), String> {
    let argv: Vec<String> = std::env::args().skip(2).collect();
    if argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", FOLLOW_HELP);
        return Ok(());
    }
    let mut o = follow::FollowOptions::default();
    let mut i = 0;
    let mut positional: Option<String> = None;
    while i < argv.len() {
        let a = argv[i].as_str();
        let mut take = |what: &str| -> Result<String, String> {
            i += 1;
            argv.get(i)
                .cloned()
                .ok_or_else(|| format!("{} needs a value", what))
        };
        match a {
            "--pipeline" => o.pipeline = std::path::PathBuf::from(take("--pipeline")?),
            "--workspace" => o.workspace = Some(std::path::PathBuf::from(take("--workspace")?)),
            "--duckdb" => o.duckdb = Some(std::path::PathBuf::from(take("--duckdb")?)),
            "--log-dir" => o.log_dir = Some(std::path::PathBuf::from(take("--log-dir")?)),
            "--name" => o.name = Some(take("--name")?),
            "--idle-ms" => {
                let v = take("--idle-ms")?;
                o.idle_ms = v.parse().map_err(|_| format!("--idle-ms wants a number, got {v}"))?;
            }
            "--max-batches" => {
                let v = take("--max-batches")?;
                let n: u64 = v
                    .parse()
                    .map_err(|_| format!("--max-batches wants a number, got {v}"))?;
                if n == 0 {
                    return Err("--max-batches 0 would do nothing; omit it to run until stopped".into());
                }
                o.max_batches = Some(n);
            }
            "--on-error" => {
                o.on_error = match take("--on-error")?.as_str() {
                    "stop" => follow::OnError::Stop,
                    "continue" => follow::OnError::Continue,
                    other => return Err(format!("--on-error takes stop or continue, got {other}")),
                }
            }
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other => positional = Some(other.to_string()),
        }
        i += 1;
    }
    if o.pipeline.as_os_str().is_empty() {
        match positional {
            Some(p) => o.pipeline = std::path::PathBuf::from(p),
            None => return Err("a pipeline path is required (see --help)".into()),
        }
    }
    follow::run(o).map(|_| ())
}

const FOLLOW_HELP: &str = "duckle-runner follow <pipeline.json> [flags]

Run one pipeline continuously instead of once, keeping the process warm
between batches. Each pass is a micro-batch.

Sources that track their position (src.kafka with trackOffset, xf.incremental)
resume where the last SUCCESSFUL batch stopped. A batch that fails anywhere -
transform, quality gate or sink - does not advance that position, so the next
pass re-reads exactly the records that did not land. Killing the process is
safe for the same reason.

  --idle-ms N        wait N ms after a pass that read nothing (default 1000)
  --max-batches N    stop after N passes (default: run until stopped)
  --on-error MODE    stop (default) or continue
  --workspace DIR    default: the pipeline file's directory
  --name NAME        run name in logs and state (default: the file stem)
  --duckdb PATH      DuckDB binary to use
  --log-dir DIR      default: <workspace>/logs
";

/// `listen --port N --spool FILE [flags]`
fn run_listen() -> Result<(), String> {
    let argv: Vec<String> = std::env::args().skip(2).collect();
    if argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", LISTEN_HELP);
        return Ok(());
    }
    let mut o = listen::ListenOptions::default();
    let mut i = 0;
    while i < argv.len() {
        let a = argv[i].as_str();
        let mut take = |what: &str| -> Result<String, String> {
            i += 1;
            argv.get(i).cloned().ok_or_else(|| format!("{} needs a value", what))
        };
        match a {
            "--port" => {
                let v = take("--port")?;
                o.port = v.parse().map_err(|_| format!("--port wants a number, got {v}"))?;
            }
            "--spool" => o.spool = std::path::PathBuf::from(take("--spool")?),
            "--path-filter" => o.path_filter = Some(take("--path-filter")?),
            "--bind" => o.bind = take("--bind")?,
            "--max-messages" => {
                let v = take("--max-messages")?;
                o.max_messages = Some(
                    v.parse().map_err(|_| format!("--max-messages wants a number, got {v}"))?,
                );
            }
            other => return Err(format!("unknown flag {other}")),
        }
        i += 1;
    }
    if o.port == 0 {
        return Err("--port is required".into());
    }
    if o.spool.as_os_str().is_empty() {
        return Err("--spool is required (the file src.spool will read)".into());
    }
    listen::run(o).map(|_| ())
}

const LISTEN_HELP: &str = "duckle-runner listen --port N --spool FILE [flags]

Keep an HTTP listener up and append what arrives to an append-only NDJSON
spool. Read the spool with src.spool, which resumes from where the last
SUCCESSFUL run stopped.

This exists because src.webhook collects inside a pipeline run: between runs
its port is closed and arriving requests are refused. Spooling decouples
arrival from processing, so a slow batch, a failed batch or a restart costs
nothing that already arrived.

  --port N           port to bind (required)
  --spool FILE       the NDJSON file to append to (required)
  --path-filter P    only spool requests whose path starts with P
  --bind ADDR        default 127.0.0.1; loopback unless you say otherwise
  --max-messages N   stop after N records

A record is {received_at, method, path, headers, json|body}. A JSON body is
embedded under `json` so the pipeline can address its fields; anything else is
kept verbatim under `body`.
";

fn run_validate() -> ExitCode {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut json_out = false;
    let mut with_sql = false;
    // #308: validate only what a change reaches. The selection comes from the
    // same function `affected` prints, so the two can never disagree about
    // which pipelines a change touches - a gate that selects differently from
    // the command people read is worse than having neither.
    let mut affected_base: Option<String> = None;
    let mut affected_head = String::new();
    let mut affected_workspace = PathBuf::from(".");
    let mut include_uncertain = false;
    // #312: CI reads a format, not console text. `--json` stays exactly as it
    // was and is the same document as `--format json`, so nothing that already
    // parses it breaks.
    let mut format = String::new();
    let mut it = std::env::args().skip(2); // skip the exe and the "validate" verb
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--json" => json_out = true,
            "--format" => match it.next().as_deref() {
                Some(f @ ("json" | "junit" | "sarif")) => format = f.to_string(),
                Some(other) => {
                    eprintln!(
                        "duckle-runner validate: unknown --format {other}. Use json, junit or sarif."
                    );
                    return ExitCode::from(2);
                }
                None => {
                    eprintln!("duckle-runner validate: --format needs json, junit or sarif");
                    return ExitCode::from(2);
                }
            },
            // Emit the compiled SQL per stage. This is the whole point of a
            // compile-to-SQL engine being inspectable: you can read exactly
            // what will run before it runs.
            "--sql" => with_sql = true,
            // `--affected` only turns the mode on. It must not overwrite a
            // revision `--base` has already parsed, or the two flags fight and
            // whichever came last wins.
            "--affected" => affected_base = affected_base.or(Some(String::new())),
            "--base" => affected_base = Some(it.next().unwrap_or_default()),
            "--head" => affected_head = it.next().unwrap_or_default(),
            "--workspace" => {
                affected_workspace = it.next().map(PathBuf::from).unwrap_or(affected_workspace)
            }
            "--include-uncertain" => include_uncertain = true,
            "--pipeline" => match it.next() {
                Some(p) => paths.push(PathBuf::from(p)),
                None => {
                    eprintln!("duckle-runner validate: --pipeline needs a path");
                    return ExitCode::from(2);
                }
            },
            other if other.starts_with('-') => {
                eprintln!("duckle-runner validate: unknown flag {other}");
                return ExitCode::from(2);
            }
            other => paths.push(PathBuf::from(other)),
        }
    }
    // #308: `--affected --base <rev>` replaces the path list with the pipelines
    // that change reaches. Nothing affected means nothing to validate, and that
    // is a pass - reporting "no pipelines given" for it would fail every clean
    // pull request.
    if let Some(base) = affected_base {
        if base.trim().is_empty() {
            eprintln!("duckle-runner validate --affected: --base <rev> is required");
            return ExitCode::from(2);
        }
        let selection = affected_cmd::select(
            &affected_workspace,
            &base,
            &affected_head,
            include_uncertain,
        );
        let affected = match selection {
            Ok(a) => a,
            Err(e) => {
                eprintln!("duckle-runner validate --affected: {e}");
                return ExitCode::from(2);
            }
        };
        // In the order the selection gives, so reading the output follows the
        // dependency chain rather than the alphabet.
        let mut order = affected.selection.order.clone();
        for entry in &affected.selection.selected {
            if !order.contains(&entry.pipeline) {
                order.push(entry.pipeline.clone());
            }
        }
        // The path comes from the same walk that found the pipeline. Guessing
        // it back from the id silently dropped anything in a nested folder, and
        // a gate that drops what it cannot find reports a clean run.
        let mut unresolved: Vec<String> = Vec::new();
        for id in &order {
            match affected.paths.get(id) {
                Some(path) => paths.push(path.clone()),
                None => unresolved.push(id.clone()),
            }
        }
        if !unresolved.is_empty() {
            eprintln!(
                "duckle-runner validate --affected: selected but could not be located: {}. \
Refusing rather than reporting a clean run.",
                unresolved.join(", ")
            );
            return ExitCode::from(2);
        }
        if paths.is_empty() {
            println!("nothing affected against {base}");
            return ExitCode::from(0);
        }
    }
    // No explicit paths: validate every pipeline in ./pipelines, which is the
    // workspace layout the editor writes.
    if paths.is_empty() {
        let dir = PathBuf::from("pipelines");
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().and_then(|x| x.to_str()) == Some("json") {
                    paths.push(p);
                }
            }
            paths.sort();
        }
        if paths.is_empty() {
            eprintln!(
                "duckle-runner validate: no pipeline given and no .json files under ./pipelines"
            );
            return ExitCode::from(2);
        }
    }

    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut findings: Vec<report::Finding> = Vec::new();
    let mut failed = 0usize;
    let machine = !format.is_empty();
    for path in &paths {
        let label = path.display().to_string();
        let outcome = std::fs::read_to_string(path)
            .map_err(|e| format!("read: {e}"))
            .and_then(|text| {
                serde_json::from_str::<PipelineDoc>(&text).map_err(|e| format!("parse: {e}"))
            })
            .and_then(|doc| {
                // #298: a dead property is not a compile error - the pipeline
                // compiles perfectly and does the wrong thing. Checked here so
                // the one surface whose whole job is to say "this is fine"
                // cannot say it about a property nothing reads.
                let dead = duckle_duckdb_engine::props::check(&doc);
                duckle_duckdb_engine::compile_pipeline_sql(&doc)
                    .map_err(|e| e.to_string())
                    .map(|stages| (stages, dead))
            });
        match outcome {
            Ok((stages, dead)) => {
                let n = stages.len();
                findings.push(report::Finding::pass(&label, "compile", format!("{n} stages")));
                // #298: strict here, warn at execution. A lint that cannot fail
                // is one people stop reading, and validate is where a typo
                // should be caught - not three hours into a run whose output
                // looks plausible.
                let refused = dead.iter().filter(|f| f.fails).count();
                for f in &dead {
                    let detail = format!("{}: {}", f.node, f.message);
                    findings.push(match f.fails {
                        true => report::Finding::fail(&label, &f.code, detail),
                        false => report::Finding::pass(&label, &f.code, detail),
                    });
                }
                if refused > 0 {
                    failed += 1;
                }
                if json_out || machine {
                    let mut entry = serde_json::json!({
                        "pipeline": label, "ok": refused == 0, "stages": n
                    });
                    if !dead.is_empty() {
                        entry["properties"] =
                            serde_json::to_value(&dead).unwrap_or_else(|_| serde_json::json!([]));
                    }
                    if with_sql {
                        entry["sql"] =
                            serde_json::to_value(&stages).unwrap_or_else(|_| serde_json::json!([]));
                    }
                    results.push(entry);
                } else {
                    match refused {
                        0 => println!("ok    {label}  ({n} stages)"),
                        _ => println!("FAIL  {label}  ({n} stages, {refused} dead propert\
ies)"),
                    }
                    for f in &dead {
                        println!("      {} {}", f.code, f.message);
                    }
                    if with_sql {
                        for s in &stages {
                            match serde_json::to_value(s) {
                                Ok(v) => {
                                    let sql = v
                                        .get("sql")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("")
                                        .trim()
                                        .to_string();
                                    let name = v
                                        .get("name")
                                        .or_else(|| v.get("node_id"))
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("stage");
                                    if !sql.is_empty() {
                                        println!("      -- {name}\n      {sql}");
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                failed += 1;
                findings.push(report::Finding::fail(&label, "compile", e.clone()));
                if json_out || machine {
                    results.push(serde_json::json!({ "pipeline": label, "ok": false, "error": e }));
                } else {
                    println!("FAIL  {label}");
                    println!("      {e}");
                }
            }
        }
    }
    if json_out || machine {
        match format.as_str() {
            "junit" => println!("{}", report::junit("validate", &findings)),
            "sarif" => println!("{}", report::sarif("validate", &findings)),
            // The versioned envelope carries `results` as well, so the shape
            // `--json` has always emitted is still there: an existing consumer
            // reads ok/checked/failed/results, a new one reads schemaVersion
            // and findings, and neither has to know about the other.
            _ => println!(
                "{}",
                report::json("validate", &findings, serde_json::json!({ "results": results }))
            ),
        }
    } else {
        println!(
            "\n{} pipeline(s) checked, {} failed",
            paths.len(),
            failed
        );
    }
    ExitCode::from(if failed == 0 { 0 } else { 1 })
}

fn run_side_for_review(
    path: &Path,
    workspace: &Path,
    engine: &DuckdbEngine,
) -> Result<std::collections::BTreeMap<String, Option<u64>>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut doc: PipelineDoc =
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))?;
    // Sink-safety: drop every sink node (and any edge touching it) so the run
    // cannot write to a destination.
    let sink_ids: std::collections::HashSet<String> = doc
        .nodes
        .iter()
        .filter(|n| n.data.component_id.as_deref().unwrap_or("").starts_with("snk."))
        .map(|n| n.id.clone())
        .collect();
    doc.nodes.retain(|n| !sink_ids.contains(&n.id));
    doc.edges.retain(|e| !sink_ids.contains(&e.source) && !sink_ids.contains(&e.target));
    // Resolve placeholders the same way a normal headless run does.
    let env_file = workspace.join("secrets.env");
    apply_env_pass(&mut doc, workspace, &env_file)?;
    context::apply_time_builtins(&mut doc);
    context::apply_workspace_context(&mut doc, workspace);
    std::env::set_var("DUCKLE_WORKSPACE", workspace);
    let res = engine.execute_pipeline(&doc);
    if res.status != "ok" {
        return Err(res.error.unwrap_or_else(|| "run failed".to_string()));
    }
    Ok(res.nodes.iter().map(|(k, s)| (k.clone(), s.rows)).collect())
}

/// Run schema-drift detection on the AFTER pipeline for `review --drift`:
/// resolve placeholders the same way a run does, then read each source's live
/// schema and diff it against the declared one. Returns the engine drift report.
fn drift_after(
    av: &serde_json::Value,
    ws: &Path,
    engine: &DuckdbEngine,
) -> Result<serde_json::Value, String> {
    let mut adoc: PipelineDoc =
        serde_json::from_value(av.clone()).map_err(|e| format!("invalid pipeline: {e}"))?;
    let env_file = ws.join("secrets.env");
    apply_env_pass(&mut adoc, ws, &env_file)?;
    context::apply_time_builtins(&mut adoc);
    context::apply_workspace_context(&mut adoc, ws);
    std::env::set_var("DUCKLE_WORKSPACE", ws);
    Ok(duckle_duckdb_engine::drift::schema_drift(engine, &adoc))
}

/// `duckle-runner review`: static review of a pipeline change. Compares two
/// versions and reports the diff plus each side's compile status. Returns the
/// process exit code.
fn run_review() -> Result<i32, String> {
    let mut before: Option<PathBuf> = None;
    let mut after: Option<PathBuf> = None;
    let mut as_json = false;
    let mut as_data = false;
    let mut as_drift = false;
    let mut duckdb_arg: Option<PathBuf> = None;
    let mut workspace_arg: Option<PathBuf> = None;
    let mut it = std::env::args().skip(2); // skip the exe and the "review" verb
    while let Some(a) = it.next() {
        match a.as_str() {
            "--before" => before = Some(PathBuf::from(it.next().ok_or("--before needs a value")?)),
            "--after" => after = Some(PathBuf::from(it.next().ok_or("--after needs a value")?)),
            "--json" => as_json = true,
            "--data" => as_data = true,
            "--drift" => as_drift = true,
            "--duckdb" => duckdb_arg = Some(PathBuf::from(it.next().ok_or("--duckdb needs a value")?)),
            "--workspace" => {
                workspace_arg = Some(PathBuf::from(it.next().ok_or("--workspace needs a value")?))
            }
            "-h" | "--help" => {
                println!("{REVIEW_USAGE}");
                return Ok(0);
            }
            other if before.is_none() && !other.starts_with('-') => {
                before = Some(PathBuf::from(other))
            }
            other if after.is_none() && !other.starts_with('-') => {
                after = Some(PathBuf::from(other))
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let before = before.ok_or("--before <file> is required")?;
    let after = after.ok_or("--after <file> is required")?;

    let load = |p: &Path| -> Result<serde_json::Value, String> {
        let text = std::fs::read_to_string(p).map_err(|e| format!("read {}: {e}", p.display()))?;
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", p.display()))
    };
    let bv = load(&before)?;
    let av = load(&after)?;

    // Compile status of each side. A change that breaks compilation is the gate.
    let compiles = |v: &serde_json::Value| -> Result<(), String> {
        let doc: PipelineDoc =
            serde_json::from_value(v.clone()).map_err(|e| format!("invalid pipeline: {e}"))?;
        duckle_duckdb_engine::compile_pipeline_sql(&doc).map(|_| ()).map_err(|e| e.to_string())
    };
    let before_compiles = compiles(&bv);
    let after_compiles = compiles(&av);

    let report = duckle_duckdb_engine::review::diff_pipelines(&bv, &av);

    // Optional live checks (need a DuckDB binary): --data diffs per-node row
    // counts by running both versions sink-safe; --drift reads the AFTER
    // version's source schemas and flags drift from their declared schemas.
    let mut data_section: Option<serde_json::Value> = None;
    let mut after_run_failed = false;
    let mut drift_section: Option<serde_json::Value> = None;
    let mut after_drift_breaking = false;
    let mut after_drift_failed = false;
    if as_data || as_drift {
        let duckdb = resolve_duckdb(duckdb_arg)?;
        std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
        let ws = workspace_arg
            .clone()
            .or_else(|| before.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("."));
        let engine = DuckdbEngine::new(duckdb);
        if as_data {
            let br = run_side_for_review(&before, &ws, &engine);
            let ar = run_side_for_review(&after, &ws, &engine);
            after_run_failed = ar.is_err();
            let mut changed_rows: Vec<serde_json::Value> = Vec::new();
            if let (Ok(b), Ok(a)) = (&br, &ar) {
                let mut ids: std::collections::BTreeSet<String> = b.keys().cloned().collect();
                ids.extend(a.keys().cloned());
                for id in ids {
                    let brows = b.get(&id).copied().flatten();
                    let arows = a.get(&id).copied().flatten();
                    if brows != arows {
                        let delta = match (brows, arows) {
                            (Some(x), Some(y)) => Some(y as i64 - x as i64),
                            _ => None,
                        };
                        changed_rows.push(serde_json::json!({
                            "node": id, "beforeRows": brows, "afterRows": arows, "delta": delta
                        }));
                    }
                }
            }
            let before_side = match &br {
                Ok(_) => serde_json::json!({ "ok": true }),
                Err(e) => serde_json::json!({ "ok": false, "error": e }),
            };
            let after_side = match &ar {
                Ok(_) => serde_json::json!({ "ok": true }),
                Err(e) => serde_json::json!({ "ok": false, "error": e }),
            };
            data_section = Some(serde_json::json!({
                "before": before_side,
                "after": after_side,
                "changedRows": changed_rows,
                "note": "sinks skipped (no destination written); sources read and transforms run",
            }));
        }
        if as_drift {
            match drift_after(&av, &ws, &engine) {
                Ok(report) => {
                    after_drift_breaking = report["hasBreaking"] == serde_json::json!(true);
                    drift_section = Some(report);
                }
                Err(e) => {
                    after_drift_failed = true;
                    drift_section = Some(serde_json::json!({ "ok": false, "error": e }));
                }
            }
        }
    }

    if as_json {
        let out = serde_json::json!({
            "before": { "path": before.display().to_string(),
                "compiles": before_compiles.is_ok(),
                "error": before_compiles.as_ref().err() },
            "after": { "path": after.display().to_string(),
                "compiles": after_compiles.is_ok(),
                "error": after_compiles.as_ref().err() },
            "diff": report,
            "dataDiff": data_section,
            "schemaDrift": drift_section,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
    } else {
        let yn = |r: &Result<(), String>| if r.is_ok() { "yes" } else { "no" };
        println!("review: {} -> {}", before.display(), after.display());
        println!("  before compiles : {}", yn(&before_compiles));
        println!("  after compiles  : {}", yn(&after_compiles));
        if let Err(e) = &after_compiles {
            println!("    after error   : {e}");
        }
        let s = &report["summary"];
        let n = |k: &str| s[k].as_u64().unwrap_or(0);
        println!(
            "  nodes: +{} added  -{} removed  ~{} changed",
            n("nodesAdded"),
            n("nodesRemoved"),
            n("nodesChanged")
        );
        println!("  edges: +{} added  -{} removed", n("edgesAdded"), n("edgesRemoved"));
        println!("  plan changed: {}", if s["planChanged"] == serde_json::json!(true) { "yes" } else { "no" });
        let arr = |k: &str| report["nodes"][k].as_array().cloned().unwrap_or_default();
        for node in arr("added") {
            println!("    + {} {}", node["node"].as_str().unwrap_or(""), node["componentId"].as_str().unwrap_or(""));
        }
        for node in arr("removed") {
            println!("    - {} {}", node["node"].as_str().unwrap_or(""), node["componentId"].as_str().unwrap_or(""));
        }
        for node in arr("changed") {
            let mut tags = Vec::new();
            if !node["componentChanged"].is_null() {
                tags.push(format!(
                    "component {}->{}",
                    node["componentChanged"]["from"].as_str().unwrap_or(""),
                    node["componentChanged"]["to"].as_str().unwrap_or("")
                ));
            }
            if node["propertiesChanged"] == serde_json::json!(true) {
                tags.push("properties".to_string());
            }
            if node["planChanged"] == serde_json::json!(true) {
                tags.push("plan".to_string());
            }
            println!(
                "    ~ {} ({}) [{}]",
                node["node"].as_str().unwrap_or(""),
                node["label"].as_str().unwrap_or(""),
                tags.join(", ")
            );
        }
        if let Some(d) = &data_section {
            let side = |k: &str| {
                if d[k]["ok"] == serde_json::json!(true) {
                    "ok".to_string()
                } else {
                    format!("failed ({})", d[k]["error"].as_str().unwrap_or(""))
                }
            };
            println!("  data diff (sinks skipped, sources read):");
            println!("    before run : {}", side("before"));
            println!("    after run  : {}", side("after"));
            let rows = d["changedRows"].as_array().cloned().unwrap_or_default();
            if rows.is_empty() && d["before"]["ok"] == serde_json::json!(true) && d["after"]["ok"] == serde_json::json!(true) {
                println!("    no per-node row-count changes");
            }
            let cell = |v: &serde_json::Value| v.as_u64().map(|n| n.to_string()).unwrap_or_else(|| "-".to_string());
            for r in rows {
                let delta = r["delta"].as_i64().map(|d| format!("  ({d:+})")).unwrap_or_default();
                println!(
                    "    ~ {}: {} -> {}{}",
                    r["node"].as_str().unwrap_or(""),
                    cell(&r["beforeRows"]),
                    cell(&r["afterRows"]),
                    delta
                );
            }
        }
        if let Some(d) = &drift_section {
            println!("  schema drift (after sources):");
            if d["ok"] == serde_json::json!(false) {
                println!("    failed ({})", d["error"].as_str().unwrap_or(""));
            } else {
                let srcs = d["sources"].as_array().cloned().unwrap_or_default();
                let drifted: Vec<&serde_json::Value> =
                    srcs.iter().filter(|s| s["status"] == serde_json::json!("drift")).collect();
                if drifted.is_empty() {
                    println!("    no source drift");
                }
                for s in drifted {
                    let cols = |k: &str| {
                        s[k].as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
                            .unwrap_or_default()
                    };
                    println!("    ~ {} [{}]", s["nodeId"].as_str().unwrap_or(""), s["componentId"].as_str().unwrap_or(""));
                    let missing = cols("missingColumns");
                    let added = cols("addedColumns");
                    if !missing.is_empty() {
                        println!("        - missing: {missing}");
                    }
                    if !added.is_empty() {
                        println!("        + added:   {added}");
                    }
                    for c in s["typeChanges"].as_array().cloned().unwrap_or_default() {
                        println!(
                            "        ~ type:    {} {} -> {}",
                            c["column"].as_str().unwrap_or(""),
                            c["declared"].as_str().unwrap_or(""),
                            c["live"].as_str().unwrap_or("")
                        );
                    }
                }
            }
        }
    }

    // Fail the gate when the proposed (after) version no longer compiles, or
    // (with --data) fails to run, or (with --drift) a source drifted in a
    // breaking way or the drift check could not be completed.
    Ok(
        if after_compiles.is_err() || after_run_failed || after_drift_breaking || after_drift_failed {
            1
        } else {
            0
        },
    )
}

fn main() -> ExitCode {
    // Artifact probe FIRST: if this executable carries a self-extracting
    // payload trailer, run the embedded pipeline and exit. A plain runner
    // (no trailer) falls through to the unchanged CLI dispatch below.
    if let Ok(exe) = std::env::current_exe() {
        match selfextract::detect(&exe) {
            Ok(Some(payload)) => return run_artifact(payload),
            Ok(None) => {}
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                return ExitCode::from(2);
            }
        }
    }

    // Subcommand dispatch: `build` -> the bundle builder; anything else
    // (a bare pipeline path or --pipeline) -> the run path.
    if std::env::args().nth(1).as_deref() == Some("build") {
        return match build::run() {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `follow` -> run one pipeline continuously instead of once, keeping the
    // process warm between batches. Safe to kill: the saved source position
    // only advances when a batch reaches "ok".
    if std::env::args().nth(1).as_deref() == Some("follow") {
        return match run_follow() {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("duckle-runner follow: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `backfill` -> inspect and edit the state a pipeline resumes from, without
    // the desktop app. Must sit above the fallthrough run path, or the verb is
    // parsed as a bare pipeline path.
    if std::env::args().nth(1).as_deref() == Some("backfill") {
        return match backfill::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner backfill: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `baseline` -> see and re-base what qa.baseline treats as normal, so a
    // source that legitimately changed shape does not force the check off.
    if std::env::args().nth(1).as_deref() == Some("baseline") {
        return match baseline::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner baseline: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `cache` -> see and drop the stage outputs kept for reuse. Separate from
    // `checkpoint` because the two hold different things: a cached output can
    // be recomputed, a checkpointed item was paid for.
    if std::env::args().nth(1).as_deref() == Some("cache") {
        return match cache::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner cache: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `python` -> prepare and inspect the workspace's Python environment.
    // Separate from a run on purpose: resolving dependencies mid-pipeline would
    // turn a missing package into a download, which an air-gapped or scheduled
    // run cannot have.
    if std::env::args().nth(1).as_deref() == Some("python") {
        return match python::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner python: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `checkpoint` -> see and bound the results a stage has already paid for.
    if std::env::args().nth(1).as_deref() == Some("checkpoint") {
        return match checkpoint::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner checkpoint: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `listen` -> keep a push source up and spool what arrives, so nothing is
    // lost between pipeline runs. Read the spool with src.spool.
    if std::env::args().nth(1).as_deref() == Some("listen") {
        return match run_listen() {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("duckle-runner listen: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `mcp` -> hand off to the MCP server, so agents use `uvx duckle mcp`.
    // `test` -> run pipelines against fixed inputs and assert what comes out.
    if std::env::args().nth(1).as_deref() == Some("test") {
        return match resolve_duckdb(None) {
            Ok(d) => pipetest::run(d),
            Err(e) => {
                eprintln!("duckle-runner test: {e}");
                ExitCode::from(2)
            }
        };
    }
    if std::env::args().nth(1).as_deref() == Some("mcp") {
        return run_mcp();
    }
    // `quickstart` -> scaffold a working pipeline, run it, show the rows.
    if std::env::args().nth(1).as_deref() == Some("quickstart") {
        return run_quickstart();
    }
    // `components schema` -> the accepted property names, per component, so an
    // agent or editor does not have to scrape source to avoid #198 (#298).
    if std::env::args().nth(1).as_deref() == Some("components") {
        if std::env::args().nth(2).as_deref() != Some("schema") {
            eprintln!("usage: duckle-runner components schema [--json]");
            return ExitCode::from(2);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&duckle_duckdb_engine::props::schema_json())
                .unwrap_or_default()
        );
        return ExitCode::from(0);
    }
    // `sql check` -> bind every node's SQL without running it (#314).
    if std::env::args().nth(1).as_deref() == Some("sql") {
        return sql_cmd::run();
    }
    // `runs diff` -> what was different about these two runs (#309).
    if std::env::args().nth(1).as_deref() == Some("runs") {
        return runsdiff_cmd::run();
    }
    // `migrate` -> bring a workspace up to the current format, deliberately
    // and never on sight (#299).
    if std::env::args().nth(1).as_deref() == Some("migrate") {
        return migrate_cmd::run();
    }
    // `affected` -> which pipelines a change reaches, and why (#308).
    if std::env::args().nth(1).as_deref() == Some("affected") {
        return affected_cmd::run();
    }
    // `contracts` -> will this change break something downstream? (#302)
    if std::env::args().nth(1).as_deref() == Some("contracts") {
        return contracts_cmd::run();
    }
    // `freshness` -> which assets are past the age they declared (#304).
    if std::env::args().nth(1).as_deref() == Some("freshness") {
        return run_freshness();
    }
    // `capabilities` -> the connector matrix, generated from the manifests (#313).
    if std::env::args().nth(1).as_deref() == Some("capabilities") {
        return capabilities::run();
    }
    // `retention` -> report and bound what the workspace accumulates (#303).
    if std::env::args().nth(1).as_deref() == Some("retention") {
        return run_retention();
    }
    // `retry` -> plan and, when it is safe, repeat a failed run (#305).
    if std::env::args().nth(1).as_deref() == Some("retry") {
        return run_retry();
    }
    // `validate` -> compile-only CI gate. No engine binary, no credentials,
    // no network: it never opens a source or writes a sink.
    if std::env::args().nth(1).as_deref() == Some("validate") {
        return run_validate();
    }
    // `serve` -> the web management console (HTTP server + embedded panel).
    if std::env::args().nth(1).as_deref() == Some("serve") {
        return match serve::run() {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `web` -> serve the full Duckle editor as a web app (#75 phase 2 spike).
    if std::env::args().nth(1).as_deref() == Some("web") {
        return match serve::run_web() {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }

    // `review` -> static review of a pipeline change (diff + compile gate).
    if std::env::args().nth(1).as_deref() == Some("review") {
        return match run_review() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `catalog` -> what the workspace reads and writes, across all pipelines.
    if std::env::args().nth(1).as_deref() == Some("catalog") {
        return match catalog_cmd::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `audit` -> read back who did what through the management console.
    if std::env::args().nth(1).as_deref() == Some("audit") {
        return match audit::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `import` -> convert a folder of legacy job files into pipelines.
    if std::env::args().nth(1).as_deref() == Some("import") {
        return match import::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `work` -> run items queued by a For Each set to "Queue for workers".
    if std::env::args().nth(1).as_deref() == Some("work") {
        return match work::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `console` -> manage who may sign in to the management console.
    if std::env::args().nth(1).as_deref() == Some("console") {
        return match console_auth::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `branch` -> data branches over a DuckDB database file.
    if std::env::args().nth(1).as_deref() == Some("branch") {
        return match branch::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    // `drift` -> detect schema drift in a pipeline's sources.
    if std::env::args().nth(1).as_deref() == Some("drift") {
        return match drift::run() {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                eprintln!("duckle-runner: {e}");
                ExitCode::from(2)
            }
        };
    }
    match run() {
        Ok(true) => ExitCode::from(0),
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("duckle-runner: {e}");
            ExitCode::from(2)
        }
    }
}

/// `duckle-runner retry <run_id>` - repeat a failed run without repeating what
/// is already known-good, and without repeating a write nobody asked to repeat.
///
/// Prints the plan first, always. A retry that quietly re-writes three sinks is
/// the failure this exists to prevent, so the plan is the product and the
/// execution is what happens once somebody has read it.
fn run_retry() -> ExitCode {
    use duckle_duckdb_engine::retry;

    let mut it = std::env::args().skip(2);
    let mut run_id: Option<String> = None;
    let mut workspace: Option<PathBuf> = None;
    let (mut dry_run, mut allow_changed, mut rerun_sinks, mut json_out) = (false, false, false, false);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(PathBuf::from),
            "--dry-run" => dry_run = true,
            "--allow-changed" => allow_changed = true,
            "--rerun-sinks" => rerun_sinks = true,
            "--json" => json_out = true,
            "-h" | "--help" => {
                println!(
                    "usage: duckle-runner retry <run_id> [--workspace DIR] [--dry-run] \
                     [--allow-changed] [--rerun-sinks] [--json]"
                );
                return ExitCode::from(0);
            }
            other if !other.starts_with('-') && run_id.is_none() => run_id = Some(other.to_string()),
            other => {
                eprintln!("duckle-runner retry: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(run_id) = run_id else {
        eprintln!("duckle-runner retry: a run id is required. It is printed as `run id` by the run you want to retry.");
        return ExitCode::from(2);
    };
    let workspace = workspace.unwrap_or_else(|| PathBuf::from("."));

    // The receipt names the pipeline, so a retry does not ask the operator to
    // remember which file a run came from.
    let prior = match retry::load(&workspace, &run_id) {
        Ok(r) => r,
        Err(retry::LoadError::NotFound) => {
            eprintln!(
                "duckle-runner retry: no receipt for run {run_id} under {}. Only a run started by \
                 `duckle-runner --pipeline` writes one.",
                workspace.display()
            );
            return ExitCode::from(2);
        }
        Err(retry::LoadError::Unreadable(e)) => {
            eprintln!("duckle-runner retry: the receipt for {run_id} could not be read ({e}).");
            return ExitCode::from(2);
        }
    };

    let pipeline = PathBuf::from(&prior.pipeline_path);
    let doc: duckle_duckdb_engine::PipelineDoc = match std::fs::read_to_string(&pipeline)
        .map_err(|e| e.to_string())
        .and_then(|t| serde_json::from_str(&t).map_err(|e| e.to_string()))
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "duckle-runner retry: cannot read the pipeline this run used ({}): {e}",
                pipeline.display()
            );
            return ExitCode::from(2);
        }
    };

    // Whether a recorded output is still there is answered by looking, not by
    // trusting the receipt. A run that succeeded months ago may have had its
    // cache pruned since.
    let ws = workspace.clone();
    let pname = prior.pipeline_name.clone();
    let cache_hit = move |node: &str, key: &str| -> Option<String> {
        let f = duckle_duckdb_engine::outcache::dir(&ws, &pname, node)
            .join(format!("{key}.parquet"));
        f.is_file().then(|| f.display().to_string())
    };

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    let new_id = format!("retry-{stamp}");
    let plan = retry::plan(
        &workspace,
        &run_id,
        &doc,
        &new_id,
        allow_changed,
        rerun_sinks,
        &cache_hit,
    );

    if json_out {
        println!("{}", serde_json::to_string_pretty(&plan).unwrap_or_default());
    } else {
        println!("retry of : {run_id}");
        println!("pipeline : {}", prior.pipeline_path);
        if let Some(r) = &plan.refusal {
            println!("refused  : {}", r.code);
            println!("           {}", r.message);
        } else {
            for d in &plan.decisions {
                let (what, why) = match &d.action {
                    retry::Action::Reuse { evidence } => ("reuse ", evidence.clone()),
                    retry::Action::ReExecute { reason } => ("run   ", reason.clone()),
                    retry::Action::RewriteSink { reason } => ("WRITE ", reason.clone()),
                };
                println!("  {what} {:<24} {why}", d.node_id);
            }
        }
    }
    if plan.refusal.is_some() {
        return ExitCode::from(2);
    }
    if dry_run {
        println!("dry run  : nothing was executed");
        return ExitCode::from(0);
    }

    // Reuse itself is the engine's decision, made per stage from a content key
    // computed at run time. The plan above says what that is expected to come
    // to; running is what makes it so.
    let args = Args {
        pipeline: Some(pipeline),
        workspace: Some(workspace),
        duckdb: None,
        log_dir: None,
        name: Some(prior.pipeline_name.clone()),
        target: None,
        list_watermarks: false,
        set_watermarks: Vec::new(),
        set_snapshots: Vec::new(),
        clear_watermarks: Vec::new(),
        manifest: false,
        verify_manifest: None,
        retry_of: Some(run_id),
    };
    match run_with(args) {
        Ok(true) => ExitCode::from(0),
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("duckle-runner retry: {e}");
            ExitCode::from(2)
        }
    }
}

/// `duckle-runner retention status|prune` - bound what Duckle accumulates (#303).
///
/// Retention is opt-in per category: a bare `prune` with no limits removes
/// nothing. Housekeeping that deletes by default is how a workspace loses
/// something nobody meant to lose.
fn run_retention() -> ExitCode {
    let mut it = std::env::args().skip(2);
    let sub = it.next().unwrap_or_default();
    let mut workspace = PathBuf::from(".");
    let mut json_out = false;
    let mut dry_run = false;
    let mut policy = retention::Policy::default();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(PathBuf::from).unwrap_or(workspace),
            "--json" => json_out = true,
            "--dry-run" => dry_run = true,
            "--cache-days" => policy.cache_days = it.next().and_then(|v| v.parse().ok()),
            "--logs-days" => policy.logs_days = it.next().and_then(|v| v.parse().ok()),
            "--receipts-keep" => policy.receipts_keep = it.next().and_then(|v| v.parse().ok()),
            other => {
                eprintln!("duckle-runner retention: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    match sub.as_str() {
        "status" => {
            let use_ = retention::survey(&workspace);
            if json_out {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "schemaVersion": report::SCHEMA_VERSION,
                        "command": "retention.status",
                        "categories": use_,
                    }))
                    .unwrap_or_default()
                );
            } else {
                println!("{:<12} {:>10} {:>12}  oldest", "category", "files", "bytes");
                for c in &use_ {
                    let oldest = c
                        .oldest_days
                        .map(|d| format!("{d}d"))
                        .unwrap_or_else(|| "-".into());
                    println!("{:<12} {:>10} {:>12}  {oldest}", c.category, c.files, c.bytes);
                }
            }
            ExitCode::from(0)
        }
        "prune" => {
            let plan = retention::plan(&workspace, &policy);
            let bytes: u64 = plan.iter().map(|r| r.bytes).sum();
            if json_out {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "schemaVersion": report::SCHEMA_VERSION,
                        "command": "retention.prune",
                        "dryRun": dry_run,
                        "files": plan.len(),
                        "bytes": bytes,
                        "removals": plan,
                    }))
                    .unwrap_or_default()
                );
            } else {
                for r in &plan {
                    println!("{:<10} {:>10}  {}  ({})", r.category, r.bytes, r.path, r.reason);
                }
                println!("
{} file(s), {bytes} bytes", plan.len());
            }
            if dry_run {
                if !json_out {
                    println!("dry run: nothing was deleted");
                }
                return ExitCode::from(0);
            }
            let (n, freed) = retention::apply(&workspace, &plan);
            if !json_out {
                println!("removed {n} file(s), {freed} bytes");
            }
            ExitCode::from(0)
        }
        _ => {
            eprintln!(
                "usage: duckle-runner retention status|prune [--workspace DIR] [--json]                  [--dry-run] [--cache-days N] [--logs-days N] [--receipts-keep N]"
            );
            ExitCode::from(2)
        }
    }
}

/// `duckle-runner freshness` - which assets are older than they said they would
/// be (#304).
///
/// Evaluated on a clock rather than at the end of a run, because the ways an
/// asset goes stale mostly produce no failed run at all: a schedule switched
/// off, a server down, a source that stopped publishing.
fn run_freshness() -> ExitCode {
    let mut workspace = PathBuf::from(".");
    let mut json_out = false;
    let mut stale_only = false;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(PathBuf::from).unwrap_or(workspace),
            "--json" => json_out = true,
            "--stale" => stale_only = true,
            other => {
                eprintln!("duckle-runner freshness: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    let mut all = duckle_duckdb_engine::sla::evaluate(&workspace, chrono::Utc::now());
    if stale_only {
        all.retain(|a| a.state == duckle_duckdb_engine::sla::State::Stale);
    }
    let stale = all
        .iter()
        .filter(|a| a.state == duckle_duckdb_engine::sla::State::Stale)
        .count();
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schemaVersion": report::SCHEMA_VERSION,
                "command": "freshness",
                "stale": stale,
                "assets": all,
            }))
            .unwrap_or_default()
        );
    } else {
        println!("{:<40} {:<8} {:<10} {:<10} owner", "asset", "state", "age", "limit");
        for a in &all {
            let age = a
                .age_seconds
                .map(|s| format!("{}h", s / 3600))
                .unwrap_or_else(|| "never".into());
            println!(
                "{:<40} {:<8} {:<10} {:<10} {}",
                a.asset,
                format!("{:?}", a.state).to_lowercase(),
                age,
                a.maximum_age.clone().unwrap_or_else(|| "-".into()),
                a.owner.clone().unwrap_or_else(|| "-".into())
            );
        }
        println!("
{} asset(s), {stale} stale", all.len());
    }
    // Stale is a finding about the data, which is exit 1 - the same code a
    // failed check uses, so a monitoring job gates on it without special-casing.
    if stale > 0 { ExitCode::from(1) } else { ExitCode::from(0) }
}
