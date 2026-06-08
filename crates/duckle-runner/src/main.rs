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
mod context;
mod selfextract;

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
    --name <label>       Run-log folder name (default: pipeline file stem)
    -h, --help           Print this help";

struct Args {
    pipeline: PathBuf,
    workspace: Option<PathBuf>,
    duckdb: Option<PathBuf>,
    log_dir: Option<PathBuf>,
    name: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut pipeline = None;
    let mut workspace = None;
    let mut duckdb = None;
    let mut log_dir = None;
    let mut name = None;
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
    let pipeline = pipeline.ok_or_else(|| "--pipeline is required".to_string())?;
    Ok(Args { pipeline, workspace, duckdb, log_dir, name })
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

fn run() -> Result<bool, String> {
    let args = parse_args()?;
    if !args.pipeline.exists() {
        return Err(format!("pipeline file not found: {}", args.pipeline.display()));
    }
    let text = std::fs::read_to_string(&args.pipeline)
        .map_err(|e| format!("read {}: {}", args.pipeline.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text)
        .map_err(|e| format!("parse {}: {}", args.pipeline.display(), e))?;

    // Workspace defaults to the pipeline file's directory. Pre-fetched
    // DuckDB extensions and incremental state live relative to it.
    let workspace = args
        .workspace
        .clone()
        .or_else(|| args.pipeline.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));

    // Runtime ${ENV:KEY} substitution. A built bundle ships ${ENV:KEY}
    // placeholders in place of secrets; resolve them now from the
    // environment, then secrets.env, then a decrypted secrets.enc.
    let env_file = workspace.join("secrets.env");
    apply_env_pass(&mut doc, &workspace, &env_file)?;
    let log_dir = args.log_dir.clone().unwrap_or_else(|| workspace.join("logs"));
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", &log_dir);

    let duckdb = resolve_duckdb(args.duckdb)?;
    let name = args.name.clone().unwrap_or_else(|| {
        args.pipeline
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "pipeline".into())
    });

    eprintln!("duckle-runner: {} (workspace {})", args.pipeline.display(), workspace.display());
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
    match run() {
        Ok(true) => ExitCode::from(0),
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("duckle-runner: {e}");
            ExitCode::from(2)
        }
    }
}
