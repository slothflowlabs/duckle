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

mod build;
use duckle_duckdb_engine::context;
mod manifest;
mod selfextract;
mod serve;

const USAGE: &str = "\
duckle-runner - run a Duckle pipeline headlessly

USAGE:
    duckle-runner --pipeline <file.json> [options]

OPTIONS:
    --pipeline <path>    Pipeline JSON to execute (required)
    --workspace <dir>    Workspace root (default: pipeline file's parent).
                         Exposed as DUCKLE_WORKSPACE for child-job and
                         incremental-state resolution.
    --duckdb <path>      DuckDB CLI binary. Resolution order if omitted:
                         DUCKLE_DUCKDB_BIN, then bin/duckdb next to this
                         runner, then 'duckdb' on PATH.
    --log-dir <dir>      Run-log directory (default: <workspace>/logs)
    --name <label>       Run-log + state folder name (default: pipeline file stem)
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
    list_watermarks: bool,
    // (node, value, sql_type) incremental sets, in order.
    set_watermarks: Vec<(String, String, String)>,
    // (node, snapshot_id) CDC sets.
    set_snapshots: Vec<(String, u64)>,
    clear_watermarks: Vec<String>,
    manifest: bool,
    verify_manifest: Option<PathBuf>,
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

fn parse_args() -> Result<Args, String> {
    let mut pipeline = None;
    let mut workspace = None;
    let mut duckdb = None;
    let mut log_dir = None;
    let mut name = None;
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
            "--name" => name = Some(take("--name")?),
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
                println!("{USAGE}");
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
fn resolve_duckdb(flag: Option<PathBuf>) -> Result<PathBuf, String> {
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
    let args = parse_args()?;

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

    // Workspace defaults to the pipeline file's directory. Pre-fetched
    // DuckDB extensions and incremental state live relative to it.
    let workspace = args
        .workspace
        .clone()
        .or_else(|| pipeline.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));

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
    let engine = DuckdbEngine::new(duckdb);
    let result = engine.execute_pipeline_named(&doc, &name);

    println!("status   : {}", result.status);
    println!("duration : {} ms", result.duration_ms);
    if let Some(err) = &result.error {
        println!("error    : {err}");
    }
    for (id, st) in &result.nodes {
        let rows = st.rows.map(|r| format!(" ({r} rows)")).unwrap_or_default();
        println!("  {:20} {}{}", id, st.status, rows);
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
        match manifest::write_manifest(
            &workspace,
            &name,
            &doc,
            &result.status,
            result.duration_ms,
            stamp,
            lineage,
            &outputs,
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
    if payload.len() < 12 + 16 {
        return Err("secrets.enc is corrupt (too short)".to_string());
    }
    let (nonce_bytes, ciphertext) = payload.split_at(12);
    let key = Sha256::digest(passphrase.as_bytes());
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| format!("cipher init: {}", e))?;
    let nonce = Nonce::from_slice(nonce_bytes);
    let plain = cipher
        .decrypt(nonce, ciphertext)
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
fn apply_env_pass(doc: &mut PipelineDoc, workspace: &Path, env_path: &Path) -> Result<(), String> {
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

    // Workspace = the extraction root; mirror run()'s env wiring.
    std::env::set_var("DUCKLE_WORKSPACE", &root);
    std::env::set_var("DUCKLE_LOG_DIR", root.join("logs"));

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
    if let Err(e) = apply_env_pass(&mut doc, &root, &env_file) {
        eprintln!("duckle-runner: {e}");
        return ExitCode::from(2);
    }

    eprintln!("duckle-runner: {} (artifact, workspace {})", pipeline.display(), root.display());
    let engine = DuckdbEngine::new(duckdb);
    let result = engine.execute_pipeline_named(&doc, &name);

    println!("status   : {}", result.status);
    println!("duration : {} ms", result.duration_ms);
    if let Some(err) = &result.error {
        println!("error    : {err}");
    }
    for (id, st) in &result.nodes {
        let rows = st.rows.map(|r| format!(" ({r} rows)")).unwrap_or_default();
        println!("  {:20} {}{}", id, st.status, rows);
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
    --duckdb <path>        DuckDB CLI for --data (else DUCKLE_DUCKDB_BIN / PATH).
    --workspace <dir>      Workspace root for --data placeholder/secret
                           resolution (default: the --before file's directory).

Without --data the review is static and read-only (nothing is executed, no
DuckDB binary needed): it reports nodes added/removed/changed, edges, whether
the compiled SQL changed, and whether each version still compiles.

Exit code: 0 reviewed, 1 the --after version fails to compile (or, with --data,
fails to run), 2 usage/IO error.";

/// Run one side of a `review --data` comparison sink-safely: every sink node is
/// removed before execution, so sources are read and transforms run but no
/// destination is ever written. Returns each surviving node's row count.
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

/// `duckle-runner review`: static review of a pipeline change. Compares two
/// versions and reports the diff plus each side's compile status. Returns the
/// process exit code.
fn run_review() -> Result<i32, String> {
    let mut before: Option<PathBuf> = None;
    let mut after: Option<PathBuf> = None;
    let mut as_json = false;
    let mut as_data = false;
    let mut duckdb_arg: Option<PathBuf> = None;
    let mut workspace_arg: Option<PathBuf> = None;
    let mut it = std::env::args().skip(2); // skip the exe and the "review" verb
    while let Some(a) = it.next() {
        match a.as_str() {
            "--before" => before = Some(PathBuf::from(it.next().ok_or("--before needs a value")?)),
            "--after" => after = Some(PathBuf::from(it.next().ok_or("--after needs a value")?)),
            "--json" => as_json = true,
            "--data" => as_data = true,
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

    // Optional live data comparison: run both versions sink-safe and diff
    // per-node row counts. Opt-in via --data; needs a DuckDB binary.
    let mut data_section: Option<serde_json::Value> = None;
    let mut after_run_failed = false;
    if as_data {
        let duckdb = resolve_duckdb(duckdb_arg)?;
        std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
        let ws = workspace_arg
            .clone()
            .or_else(|| before.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("."));
        let engine = DuckdbEngine::new(duckdb);
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
    }

    // Fail the gate when the proposed (after) version no longer compiles, or
    // (with --data) fails to run.
    Ok(if after_compiles.is_err() || after_run_failed { 1 } else { 0 })
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
    match run() {
        Ok(true) => ExitCode::from(0),
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("duckle-runner: {e}");
            ExitCode::from(2)
        }
    }
}
