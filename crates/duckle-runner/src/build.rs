//! The `build` subcommand: emit a self-contained, single-file artifact from
//! a workspace pipeline (the "Build Job" equivalent).
//!
//! Resolves the pipeline (context vars, routine inlining, child-pipeline
//! paths), redacts secrets to `${ENV:KEY}` placeholders, gathers duckdb +
//! extensions + the resolved pipeline + scrubbed contexts + secret files
//! into a staging dir, then packs it into a ZIP payload appended after a
//! clean runner stub to produce ONE executable named at --out. That single
//! file self-extracts and runs offline under cron / systemd / Task Scheduler.
//!
//! Exit codes (mapped by main): 0 ok, 2 any fatal build error (usage, IO,
//! or a tripped leak guard). The build never runs a pipeline, so the "1
//! pipeline error" code is unused here.

use duckle_duckdb_engine::context::{self, substitute_deep};
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine as _;
use duckle_duckdb_engine::{is_secret_prop_key, PipelineDoc};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Default DuckDB binary location used when neither --duckdb nor
/// DUCKLE_DUCKDB_BIN is given.
const DEFAULT_DUCKDB: &str =
    "C:/Users/Sourav Roy/AppData/Roaming/io.duckle.app/engines/duckdb/duckdb.exe";

/// Minimum length for a string to be treated as a secret. Below this we
/// risk corrupting structural tokens (a 1-3 char "secret" could be a
/// delimiter, "true", a digit, etc).
const SECRET_MIN_LEN: usize = 4;

struct BuildArgs {
    workspace: PathBuf,
    pipeline_id: String,
    out: PathBuf,
    context: Option<String>,
    secrets: SecretMode,
    stub: Option<PathBuf>,
    duckdb: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq)]
enum SecretMode {
    Env,
    Passphrase,
}

/// Minimal repo item view, just to map a pipeline id to a display name.
#[derive(Deserialize)]
struct RepoItem {
    id: String,
    name: String,
    #[serde(rename = "type")]
    kind: String,
}

fn parse_build_args() -> Result<BuildArgs, String> {
    let mut workspace = None;
    let mut pipeline_id = None;
    let mut out = None;
    let mut context = None;
    let mut secrets = SecretMode::Env;
    let mut stub = None;
    let mut duckdb = None;

    // Skip "duckle-runner" and "build".
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        let mut take = |label: &str| it.next().ok_or_else(|| format!("{} needs a value", label));
        match arg.as_str() {
            "--workspace" => workspace = Some(PathBuf::from(take("--workspace")?)),
            "--pipeline-id" => pipeline_id = Some(take("--pipeline-id")?),
            "--out" => out = Some(PathBuf::from(take("--out")?)),
            "--context" => context = Some(take("--context")?),
            "--secrets" => {
                secrets = match take("--secrets")?.as_str() {
                    "env" => SecretMode::Env,
                    "passphrase" => SecretMode::Passphrase,
                    other => return Err(format!("--secrets must be env|passphrase, got {}", other)),
                }
            }
            "--stub" => stub = Some(PathBuf::from(take("--stub")?)),
            "--duckdb" => duckdb = Some(PathBuf::from(take("--duckdb")?)),
            other => return Err(format!("unknown build argument: {}", other)),
        }
    }

    Ok(BuildArgs {
        workspace: workspace.ok_or_else(|| "build: --workspace is required".to_string())?,
        pipeline_id: pipeline_id.ok_or_else(|| "build: --pipeline-id is required".to_string())?,
        out: out.ok_or_else(|| "build: --out is required".to_string())?,
        context,
        secrets,
        stub,
        duckdb,
    })
}

/// Turn a prop/var key into an UPPER_SNAKE env KEY. Mirrors the engine's
/// secret_placeholder normalization (camelCase split, non-alnum -> `_`)
/// but emits the bare uppercased name (no `${...}` wrapper, no DUCKLE_
/// prefix). e.g. apiKey -> API_KEY, client_secret -> CLIENT_SECRET.
fn key_namer(key: &str) -> String {
    let mut out = String::new();
    let mut prev_lower = false;
    for ch in key.chars() {
        if ch.is_ascii_uppercase() && prev_lower {
            out.push('_');
        }
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
        prev_lower = ch.is_ascii_lowercase() || ch.is_ascii_digit();
    }
    out.trim_end_matches('_').to_string()
}

/// Sanitize a pipeline display name for use as a filename + folder segment.
fn sanitize_name(name: &str) -> String {
    let mut out = String::new();
    let mut prev_us = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch);
            prev_us = false;
        } else if !prev_us {
            out.push('_');
            prev_us = true;
        }
    }
    let trimmed = out.trim_matches(|c| c == '_' || c == '.');
    if trimmed.is_empty() {
        "pipeline".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Whether a prop key holds a credential. Gates on the engine's
/// single-sourced keyword list, but refines the two short, ambiguous
/// needles `pat` and `sas`: as loose substrings they false-positive on
/// ordinary keys (e.g. `pat` matches `path`, `compatible`; `sas` matches
/// `sasl`). When the engine helper matches ONLY because of those needles,
/// require them to appear as a delimited word, so a `path` prop (the most
/// common file prop) is not wrongly redacted into `${ENV:PATH}` and its
/// resolved value lost. Genuine keys like `pat`, `client_pat`, `sasUrl`
/// still match.
fn is_secret_key(key: &str) -> bool {
    if !is_secret_prop_key(key) {
        return false;
    }
    let k = key.to_ascii_lowercase();
    // The unambiguous needles (everything except pat/sas). If any matches,
    // the key is genuinely a credential key regardless of pat/sas.
    const STRONG: [&str; 14] = [
        "password", "passwd", "secret", "token", "apikey", "api_key",
        "privatekey", "private_key", "accesskey", "access_key",
        "clientsecret", "client_secret", "connectionstring", "connection_string",
    ];
    if k.contains("credential") || STRONG.iter().any(|n| k.contains(n)) {
        return true;
    }
    // Only pat/sas could have matched: require a delimited word so `path`
    // / `compatible` / `sasl` do not trip.
    is_delimited_word(&k, "pat") || is_delimited_word(&k, "sas")
}

/// True if `needle` appears in `hay` bounded by non-alphanumeric chars (or
/// string ends) on both sides, i.e. as a standalone word/token.
fn is_delimited_word(hay: &str, needle: &str) -> bool {
    let bytes = hay.as_bytes();
    let nlen = needle.len();
    let mut start = 0;
    while let Some(pos) = hay[start..].find(needle) {
        let i = start + pos;
        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        let after = i + nlen;
        let after_ok = after == bytes.len() || !bytes[after].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        start = i + 1;
    }
    false
}

/// A detected secret: its raw plaintext value, the env KEY that replaces
/// it, and the owning node id (for collision disambiguation).
struct Detected {
    value: String,
    key: String,
    node_id: String,
}

/// Recurse a props JSON value collecting secrets. `parent_key` is the
/// nearest enclosing object key (used to name array/standalone secrets).
fn collect_node_secrets(
    value: &JsonValue,
    parent_key: Option<&str>,
    node_id: &str,
    secret_values: &[String],
    out: &mut Vec<Detected>,
) {
    match value {
        JsonValue::Object(map) => {
            for (k, v) in map {
                // S1: a secret-named key with a non-empty string value. The
                // key is unambiguously a credential field, so redact (and
                // leak-guard) its value regardless of length - the
                // SECRET_MIN_LEN floor is only for the ambiguous S2 path.
                if is_secret_key(k) {
                    if let Some(s) = v.as_str() {
                        if !s.is_empty() {
                            out.push(Detected {
                                value: s.to_string(),
                                key: key_namer(k),
                                node_id: node_id.to_string(),
                            });
                        }
                    }
                }
                collect_node_secrets(v, Some(k), node_id, secret_values, out);
            }
        }
        JsonValue::Array(arr) => {
            for v in arr {
                collect_node_secrets(v, parent_key, node_id, secret_values, out);
            }
        }
        JsonValue::String(s) => {
            // S2: any string value equal to a captured secret context var.
            if s.len() >= SECRET_MIN_LEN && secret_values.iter().any(|sv| sv == s) {
                let key = parent_key.map(key_namer).filter(|k| !k.is_empty()).unwrap_or_else(|| "SECRET".to_string());
                out.push(Detected {
                    value: s.clone(),
                    key,
                    node_id: node_id.to_string(),
                });
            }
        }
        _ => {}
    }
}

/// Build the KEY -> plaintext value map from the resolved doc, handling
/// collisions (same key, different value -> disambiguate; never overwrite).
/// Returns the map plus the de-duplicated set of (value, key) replacements
/// sorted longest-value-first for safe substring replacement.
fn build_key_map(
    doc: &PipelineDoc,
    secret_values: &[String],
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    // Collect every detection across nodes.
    let mut detected: Vec<Detected> = Vec::new();
    for node in &doc.nodes {
        if let Some(props) = node.data.properties.as_ref() {
            collect_node_secrets(props, None, &node.id, secret_values, &mut detected);
        }
    }

    // key_map: KEY -> value, with collision disambiguation.
    let mut key_map: Vec<(String, String)> = Vec::new();
    // value -> resolved KEY (so the same value always maps to one KEY).
    let mut value_to_key: HashMap<String, String> = HashMap::new();

    for d in &detected {
        if value_to_key.contains_key(&d.value) {
            continue; // value already has a KEY assigned.
        }
        let mut key = d.key.clone();
        if key.is_empty() {
            key = "SECRET".to_string();
        }
        // Resolve collisions: same KEY, different value.
        if let Some((_, existing_val)) = key_map.iter().find(|(k, _)| *k == key) {
            if existing_val != &d.value {
                key = format!("{}__{}", key, key_namer(&d.node_id));
                let mut counter = 2;
                while key_map.iter().any(|(k, v)| *k == key && v != &d.value) {
                    key = format!("{}__{}__{}", d.key, key_namer(&d.node_id), counter);
                    counter += 1;
                }
            } else {
                // same KEY same value -> dedupe.
                value_to_key.insert(d.value.clone(), key);
                continue;
            }
        }
        key_map.push((key.clone(), d.value.clone()));
        value_to_key.insert(d.value.clone(), key);
    }

    // Replacement list (value, KEY) sorted longest value first.
    let mut replacements: Vec<(String, String)> = key_map
        .iter()
        .map(|(k, v)| (v.clone(), k.clone()))
        .collect();
    replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    (key_map, replacements)
}

/// Replace every detected secret value in the doc with `${ENV:KEY}`.
fn redact_doc(doc: &mut PipelineDoc, replacements: &[(String, String)]) {
    let replace = |s: &str| -> String {
        let mut out = s.to_string();
        for (value, key) in replacements {
            if out.contains(value.as_str()) {
                out = out.replace(value.as_str(), &format!("${{ENV:{}}}", key));
            }
        }
        out
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
        // Strip non-runtime preview fields. sampleRows can carry live
        // secret data from a source preview and the engine does not read
        // it to run; schema IS read by the planner so it is kept.
        node.data.sample_rows = None;
    }
}

/// Assert no raw secret appears as a substring in `bytes`. The set scanned
/// = key_map values UNION secret_values. Every value in both lists comes
/// from an explicit secret source (a secret-keyed prop or a secret:true
/// context var), so the SECRET_MIN_LEN floor (which only guards detection
/// against structural-token collisions) is NOT applied here: a short secret
/// must abort the build rather than silently ship in plaintext. Fails the
/// build on a hit.
fn leak_guard(
    bytes: &str,
    file_label: &str,
    key_map: &[(String, String)],
    secret_values: &[String],
) -> Result<(), String> {
    let mut all: Vec<&String> = key_map.iter().map(|(_, v)| v).collect();
    all.extend(secret_values.iter());
    for v in all {
        if !v.is_empty() && bytes.contains(v.as_str()) {
            return Err(format!(
                "leak guard: a secret value appears in plaintext in {} (build aborted)",
                file_label
            ));
        }
    }
    Ok(())
}

fn copy_file(src: &Path, dst: &Path) -> Result<(), String> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
    }
    std::fs::copy(src, dst)
        .map(|_| ())
        .map_err(|e| format!("copy {} -> {}: {}", src.display(), dst.display(), e))
}

/// Set the unix exec bit (0o755). No-op on windows.
#[cfg(unix)]
fn set_exec(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)
        .map_err(|e| format!("stat {}: {}", path.display(), e))?
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)
        .map_err(|e| format!("chmod {}: {}", path.display(), e))
}

#[cfg(not(unix))]
fn set_exec(_path: &Path) -> Result<(), String> {
    Ok(())
}

/// Resolve the duckdb source binary: --duckdb > DUCKLE_DUCKDB_BIN > default.
fn resolve_duckdb_src(flag: &Option<PathBuf>) -> Option<PathBuf> {
    if let Some(p) = flag {
        return if p.exists() { Some(p.clone()) } else { None };
    }
    if let Ok(env) = std::env::var("DUCKLE_DUCKDB_BIN") {
        let p = PathBuf::from(env);
        if p.exists() {
            return Some(p);
        }
    }
    let p = PathBuf::from(DEFAULT_DUCKDB);
    if p.exists() {
        Some(p)
    } else {
        None
    }
}

/// Windows: suppress the console window that pops up when a windowless parent
/// (the desktop spawns the runner headless) shells out to a console child.
/// No-op on other platforms.
fn no_window(cmd: &mut std::process::Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let _ = cmd;
}

/// Run `<duckdb> --version` and parse the `vX.Y.Z` token.
fn duckdb_version(bin: &Path) -> Option<String> {
    let mut cmd = std::process::Command::new(bin);
    no_window(&mut cmd);
    let out = cmd.arg("--version").output().ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.split_whitespace()
        .find(|t| t.starts_with('v') && t[1..].chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
        .map(|t| t.to_string())
}

/// Run `<duckdb> -c "PRAGMA platform;"` and read the single value.
fn duckdb_platform(bin: &Path) -> Option<String> {
    let mut cmd = std::process::Command::new(bin);
    no_window(&mut cmd);
    let out = cmd
        .arg("-noheader")
        .arg("-list")
        .arg("-c")
        .arg("PRAGMA platform;")
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(|l| l.to_string())
}

pub fn run() -> Result<(), String> {
    let args = parse_build_args()?;

    // 1. Resolve the pipeline.
    let resolved = context::resolve_workspace(
        &args.workspace,
        &args.pipeline_id,
        args.context.as_deref(),
    )?;
    let secret_values = resolved.secret_values.clone();
    let mut doc = resolved.doc;

    // 2. Pipeline display name from repository.json.
    let repo: Vec<RepoItem> = {
        let path = args.workspace.join("repository.json");
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("read {}: {}", path.display(), e))?;
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?
    };
    let display = repo
        .iter()
        .find(|i| i.id == args.pipeline_id)
        .map(|i| i.name.clone())
        .unwrap_or_else(|| args.pipeline_id.clone());
    let name = sanitize_name(&display);

    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    // Stage the artifact contents in a temp dir, then pack them into the
    // single-file payload. --out is the FINAL artifact path (written exactly).
    let staging = std::env::temp_dir().join(format!(
        "duckle-build-{}-{}",
        sanitize_name(&name),
        std::process::id()
    ));
    if staging.exists() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    std::fs::create_dir_all(&staging).map_err(|e| format!("mkdir {}: {}", staging.display(), e))?;
    let root = &staging;

    // 3. Redaction (resolve FIRST, redact SECOND).
    let (mut key_map, replacements) = build_key_map(&doc, &secret_values);
    redact_doc(&mut doc, &replacements);

    // value -> KEY, so the scrubbed contexts/ file uses the SAME env KEY the
    // pipeline + secrets.env.example use for a given secret value.
    let value_to_key: HashMap<String, String> = key_map
        .iter()
        .map(|(k, v)| (v.clone(), k.clone()))
        .collect();
    // KEYs for secret context vars whose value never appeared in the pipeline;
    // merged into key_map below so secrets.env.example / secrets.enc list them.
    let mut extra_keys: Vec<(String, String)> = Vec::new();

    // Track relative file paths for the manifest.
    let mut files: Vec<String> = Vec::new();
    let record = |files: &mut Vec<String>, rel: &str| files.push(rel.to_string());

    // --- bin/duckdb (best-effort) ---
    let duckdb_src = resolve_duckdb_src(&args.duckdb);
    let mut duckdb_ver: Option<String> = None;
    let mut duckdb_plat: Option<String> = None;
    if let Some(src) = &duckdb_src {
        let duckdb_name = if os == "windows" { "duckdb.exe" } else { "duckdb" };
        let duckdb_dst = root.join("bin").join(duckdb_name);
        copy_file(src, &duckdb_dst)?;
        set_exec(&duckdb_dst)?;
        record(&mut files, &format!("bin/{}", duckdb_name));

        // Derive version + platform from the resolved binary.
        duckdb_ver = duckdb_version(src);
        duckdb_plat = duckdb_platform(src);

        // --- extensions (best-effort) ---
        if let (Some(ver), Some(plat)) = (&duckdb_ver, &duckdb_plat) {
            let home = if os == "windows" {
                std::env::var("USERPROFILE").ok()
            } else {
                std::env::var("HOME").ok()
            };
            if let Some(home) = home {
                let ext_src = PathBuf::from(&home)
                    .join(".duckdb")
                    .join("extensions")
                    .join(ver)
                    .join(plat);
                let needed = needed_extensions(&doc);
                if ext_src.is_dir() && !needed.is_empty() {
                    let ext_dst = root
                        .join("bin")
                        .join(".duckdb")
                        .join("extensions")
                        .join(ver)
                        .join(plat);
                    copy_extensions(&ext_src, &ext_dst, ver, plat, &needed, &mut files)?;
                } else if needed.is_empty() {
                    // Pure file pipeline (csv / parquet / json over local paths):
                    // DuckDB needs no loadable extensions, so embed none. Keeps
                    // the artifact small instead of shipping the whole ~600 MB
                    // extension set.
                } else {
                    eprintln!(
                        "duckle-runner build: extension dir not found ({}); connectors needing extensions will fail offline",
                        ext_src.display()
                    );
                }
            } else {
                eprintln!("duckle-runner build: cannot resolve user home; skipping extensions");
            }
        }
    } else {
        eprintln!("duckle-runner build: no duckdb binary found; bundle will need a duckdb on PATH");
    }

    // --- pipeline/<name>.json (redacted, leak-guarded) ---
    let pipe_json = serde_json::to_string_pretty(&doc)
        .map_err(|e| format!("serialize pipeline: {}", e))?;
    let pipe_rel = format!("pipeline/{}.json", name);
    leak_guard(&pipe_json, &pipe_rel, &key_map, &secret_values)?;
    let pipe_dst = root.join("pipeline").join(format!("{}.json", name));
    write_file(&pipe_dst, pipe_json.as_bytes())?;
    record(&mut files, &pipe_rel);

    // --- contexts/ (scrubbed in BOTH modes) ---
    for item in repo.iter().filter(|i| i.kind == "context") {
        if let Some(want) = args.context.as_deref() {
            if item.name != want {
                continue;
            }
        }
        let src = args.workspace.join("contexts").join(format!("{}.json", item.id));
        if !src.exists() {
            continue;
        }
        let text = std::fs::read_to_string(&src)
            .map_err(|e| format!("read {}: {}", src.display(), e))?;
        let scrubbed = scrub_context_file(&text, &value_to_key, &mut extra_keys)?;
        let rel = format!("contexts/{}.json", item.id);
        leak_guard(&scrubbed, &rel, &key_map, &secret_values)?;
        let dst = root.join("contexts").join(format!("{}.json", item.id));
        write_file(&dst, scrubbed.as_bytes())?;
        record(&mut files, &rel);
    }

    // routines/ are intentionally NOT shipped: resolve_workspace already
    // inlines SQL routine bodies into the pipeline doc at build time, and
    // run_artifact only ever reads pipeline/<name>.json. Copying routine
    // files verbatim would be dead payload AND an unguarded leak surface (a
    // routine whose SQL embeds a literal secret would bypass the leak guard
    // that scrubs pipeline/ and contexts/).

    // Merge context-only secret KEYs (values not present in the pipeline)
    // into key_map so the operator-facing secret list is complete. Skip a
    // KEY already present for the same value; on a same-KEY/different-value
    // collision, suffix the KEY so neither value is lost.
    for (k, v) in extra_keys {
        if key_map.iter().any(|(ek, ev)| *ek == k && *ev == v) {
            continue;
        }
        let mut key = k.clone();
        let mut counter = 2;
        while key_map.iter().any(|(ek, ev)| *ek == key && *ev != v) {
            key = format!("{}__{}", k, counter);
            counter += 1;
        }
        key_map.push((key, v));
    }

    // --- secret-delivery files ---
    match args.secrets {
        SecretMode::Env => {
            let example = secrets_env_example(&key_map);
            let rel = "secrets.env.example";
            write_file(&root.join(rel), example.as_bytes())?;
            record(&mut files, rel);
        }
        SecretMode::Passphrase => {
            let blob = encrypt_secrets(&key_map)?;
            let rel = "secrets.enc";
            write_file(&root.join(rel), blob.as_bytes())?;
            record(&mut files, rel);
        }
    }

    // --- manifest.json (leak-guarded; paths only, never values) ---
    let manifest = render_manifest(
        &name,
        &args.pipeline_id,
        args.context.as_deref(),
        os,
        arch,
        duckdb_ver.as_deref(),
        duckdb_plat.as_deref(),
        &files,
    );
    leak_guard(&manifest, "manifest.json", &key_map, &secret_values)?;
    write_file(&root.join("manifest.json"), manifest.as_bytes())?;

    // Pack the staging tree into the payload and write the single-file
    // artifact = [stub runner][zip payload][16-byte trailer].
    let payload = crate::selfextract::pack(root)?;
    let stub = resolve_stub(&args.stub)?;
    let out_file = &args.out;
    if let Some(parent) = out_file.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
    }
    crate::selfextract::write_artifact(&stub, &payload, out_file)?;
    let _ = std::fs::remove_dir_all(root);

    eprintln!("duckle-runner build: wrote {}", out_file.display());
    Ok(())
}

/// Resolve the stub runner bytes to prepend to the artifact. With --stub,
/// read that file. Without it, reuse the current exe ONLY when the current
/// exe is itself a clean runner (no trailer); if the current exe is already
/// an artifact, error and ask for --stub.
fn resolve_stub(flag: &Option<PathBuf>) -> Result<Vec<u8>, String> {
    match flag {
        Some(p) => std::fs::read(p).map_err(|e| format!("read stub {}: {}", p.display(), e)),
        None => {
            let exe = std::env::current_exe().map_err(|e| format!("current_exe: {}", e))?;
            if crate::selfextract::has_trailer(&exe)? {
                return Err(
                    "this duckle-runner is itself a built artifact; pass --stub <clean-runner> to build"
                        .to_string(),
                );
            }
            std::fs::read(&exe).map_err(|e| format!("read stub {}: {}", exe.display(), e))
        }
    }
}

/// Write a file, creating parent dirs. Uses the exact bytes given.
fn write_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
    }
    std::fs::write(path, bytes).map_err(|e| format!("write {}: {}", path.display(), e))
}

/// Copy `.duckdb_extension` (+ `.info` sidecars) files from the source ext
/// dir into the bundle; skip `*.tmp-*`. Records relative paths.
/// DuckDB extensions a pipeline actually needs, derived from its component
/// ids. Pure file pipelines (csv / tsv / parquet / json over local paths)
/// need none, so the artifact embeds none instead of the whole ~600 MB set.
fn needed_extensions(doc: &PipelineDoc) -> std::collections::BTreeSet<String> {
    let mut set = std::collections::BTreeSet::new();
    let v = match serde_json::to_value(doc) {
        Ok(v) => v,
        Err(_) => return set,
    };
    let nodes = match v.get("nodes").and_then(|n| n.as_array()) {
        Some(n) => n,
        None => return set,
    };
    for node in nodes {
        let cid = node
            .get("data")
            .and_then(|d| d.get("componentId"))
            .and_then(|c| c.as_str())
            .unwrap_or("");
        // The extension base name matches the .duckdb_extension file stem.
        let ext: &[&str] = match cid {
            "src.excel" | "snk.excel" => &["excel"],
            "src.avro" | "snk.avro" => &["avro"],
            "src.iceberg" | "snk.iceberg" => &["iceberg"],
            "src.delta" => &["delta"],
            "src.spatial" | "snk.spatial" | "xf.join.spatial" | "xf.geo.distance"
            | "xf.geo.buffer" | "xf.geo.intersects" => &["spatial"],
            "xf.ai.vector_search" => &["vss"],
            "xf.ai.text_search" => &["fts"],
            "xf.ip.parse" => &["inet"],
            "src.sqlite" | "snk.sqlite" => &["sqlite_scanner"],
            "src.ducklake" | "snk.ducklake" => &["ducklake"],
            "src.quack" | "snk.quack" => &["quack"],
            "src.json" | "snk.json" | "src.jsonl" | "snk.jsonl" => &["json"],
            "src.parquet" | "snk.parquet" => &["parquet"],
            _ => {
                let fam = cid
                    .strip_prefix("src.")
                    .or_else(|| cid.strip_prefix("snk."))
                    .unwrap_or(cid);
                match fam {
                    "postgres" | "cockroach" | "redshift" | "pgvector" => &["postgres_scanner"],
                    "mysql" | "mariadb" => &["mysql_scanner"],
                    "s3" | "gcs" | "http" | "https" | "minio" | "r2" | "b2" => &["httpfs"],
                    // azureblob also wants the azure extension, but that is not
                    // in the prefetch set; ship httpfs and let azure autoload.
                    "azureblob" => &["httpfs"],
                    _ => &[],
                }
            }
        };
        for e in ext {
            set.insert((*e).to_string());
        }
    }
    set
}

fn copy_extensions(
    src: &Path,
    dst: &Path,
    ver: &str,
    plat: &str,
    needed: &std::collections::BTreeSet<String>,
    files: &mut Vec<String>,
) -> Result<(), String> {
    let mut found: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let entries = std::fs::read_dir(src).map_err(|e| format!("read_dir {}: {}", src.display(), e))?;
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        if fname.contains(".tmp-") {
            continue;
        }
        // Stem before .duckdb_extension(.info); copy only extensions the
        // pipeline needs so we do not embed the entire extension set.
        let stem = fname
            .strip_suffix(".duckdb_extension.info")
            .or_else(|| fname.strip_suffix(".duckdb_extension"));
        if let Some(stem) = stem {
            if !needed.contains(stem) {
                continue;
            }
            found.insert(stem.to_string());
            copy_file(&entry.path(), &dst.join(&*fname))?;
            files.push(format!(
                "bin/.duckdb/extensions/{}/{}/{}",
                ver, plat, fname
            ));
        }
    }
    for want in needed.iter() {
        if !found.contains(want) {
            eprintln!(
                "duckle-runner build: needed extension '{}' not found in {}; it will autoload at run time if the target has network",
                want, src.display()
            );
        }
    }
    Ok(())
}

/// Scrub a contexts/<id>.json file: replace every secret:true var value
/// with `${ENV:KEY}`. Preserves object key order via serde_json's
/// preserve_order feature.
///
/// To keep the env KEY consistent with the pipeline and secrets.env.example,
/// the placeholder is keyed by the secret VALUE: when the same value was
/// detected in the pipeline (`value_to_key`), reuse that KEY. When a secret
/// var's value never appeared in the pipeline, fall back to naming the KEY
/// from the var's own key, and push the (KEY, value) pair into `extra` so the
/// caller can add it to secrets.env.example / secrets.enc.
fn scrub_context_file(
    text: &str,
    value_to_key: &HashMap<String, String>,
    extra: &mut Vec<(String, String)>,
) -> Result<String, String> {
    let mut v: JsonValue =
        serde_json::from_str(text).map_err(|e| format!("parse context file: {}", e))?;
    if let Some(vars) = v.get_mut("variables").and_then(JsonValue::as_array_mut) {
        for var in vars.iter_mut() {
            let is_secret = var
                .get("secret")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);
            if !is_secret {
                continue;
            }
            let value = var
                .get("value")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .to_string();
            let key = match value_to_key.get(&value) {
                // The value was redacted in the pipeline; reuse that KEY.
                Some(k) => k.clone(),
                // Not seen in the pipeline: name from the var key and ensure
                // the operator-facing KEY list still carries it.
                None => {
                    let k = var
                        .get("key")
                        .and_then(JsonValue::as_str)
                        .map(key_namer)
                        .unwrap_or_default();
                    if !value.is_empty()
                        && !extra.iter().any(|(ek, _)| *ek == k)
                    {
                        extra.push((k.clone(), value.clone()));
                    }
                    k
                }
            };
            if let Some(obj) = var.as_object_mut() {
                obj.insert(
                    "value".to_string(),
                    JsonValue::String(format!("${{ENV:{}}}", key)),
                );
            }
        }
    }
    serde_json::to_string_pretty(&v).map_err(|e| format!("serialize context file: {}", e))
}

/// secrets.env.example: sorted KEY= lines (empty values), trailing newline.
fn secrets_env_example(key_map: &[(String, String)]) -> String {
    let mut keys: Vec<&String> = key_map.iter().map(|(k, _)| k).collect();
    keys.sort();
    keys.dedup();
    let mut out = String::new();
    for k in keys {
        out.push_str(k);
        out.push_str("=\n");
    }
    out
}

/// Build the plaintext KEY=VALUE blob (sorted by KEY, trailing newline),
/// then AES-256-GCM encrypt under SHA-256(DUCKLE_BUNDLE_PASSPHRASE) with a
/// fresh random nonce prepended; return base64.
fn encrypt_secrets(key_map: &[(String, String)]) -> Result<String, String> {
    let passphrase = std::env::var("DUCKLE_BUNDLE_PASSPHRASE")
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| {
            "--secrets passphrase requires DUCKLE_BUNDLE_PASSPHRASE in the environment".to_string()
        })?;

    let mut pairs: Vec<&(String, String)> = key_map.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut plain = String::new();
    for (k, v) in pairs {
        plain.push_str(k);
        plain.push('=');
        plain.push_str(v);
        plain.push('\n');
    }

    let key = Sha256::digest(passphrase.as_bytes());
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| format!("cipher init: {}", e))?;
    let mut nonce_bytes = [0u8; 12];
    getrandom::getrandom(&mut nonce_bytes).map_err(|e| format!("nonce: {}", e))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plain.as_bytes())
        .map_err(|e| format!("encrypt: {}", e))?;

    let mut payload = Vec::with_capacity(12 + ciphertext.len());
    payload.extend_from_slice(&nonce_bytes);
    payload.extend_from_slice(&ciphertext);
    Ok(base64::engine::general_purpose::STANDARD.encode(payload))
}

#[allow(clippy::too_many_arguments)]
fn render_manifest(
    name: &str,
    pipeline_id: &str,
    context: Option<&str>,
    os: &str,
    arch: &str,
    duckdb_ver: Option<&str>,
    duckdb_plat: Option<&str>,
    files: &[String],
) -> String {
    let mut m = serde_json::Map::new();
    m.insert("name".into(), JsonValue::String(name.to_string()));
    m.insert("pipelineId".into(), JsonValue::String(pipeline_id.to_string()));
    m.insert("format".into(), JsonValue::String("self-extracting".to_string()));
    m.insert(
        "note".into(),
        JsonValue::String(
            "Single-file artifact: a clean duckle-runner stub with a zip payload + 16-byte trailer appended. The payload self-extracts at run time. The artifact is unsigned; appending the payload invalidates any code signature, so do not codesign/Authenticode-sign it.".to_string(),
        ),
    );
    m.insert(
        "context".into(),
        match context {
            Some(c) => JsonValue::String(c.to_string()),
            None => JsonValue::Null,
        },
    );
    m.insert(
        "builtOsArch".into(),
        JsonValue::String(format!("{}-{}", os, arch)),
    );
    m.insert(
        "duckdbVersion".into(),
        duckdb_ver.map(|s| JsonValue::String(s.to_string())).unwrap_or(JsonValue::Null),
    );
    m.insert(
        "duckdbPlatform".into(),
        duckdb_plat.map(|s| JsonValue::String(s.to_string())).unwrap_or(JsonValue::Null),
    );
    let mut sorted = files.to_vec();
    sorted.sort();
    m.insert(
        "files".into(),
        JsonValue::Array(sorted.into_iter().map(JsonValue::String).collect()),
    );
    serde_json::to_string_pretty(&JsonValue::Object(m)).unwrap_or_else(|_| "{}".to_string())
}
