//! #307: components Duckle did not write.
//!
//! An iXBRL parser, an OCR adapter, a country-specific registry reader - none
//! of these belong in the Rust core, and none of them should have to be an
//! escape hatch either. An external component declares itself in a manifest,
//! appears in the catalog like any other, is gated by policy like any other,
//! and exchanges bulk data as Parquet rather than row-by-row JSON.
//!
//! ## The interchange is Parquet, not row JSON
//!
//! #307 is explicit that bulk tabular data must not be row-by-row JSON. Parquet
//! is what this uses: DuckDB reads and writes it natively at both ends, it is
//! typed, and the issue names it as the acceptable interchange for tools that
//! cannot stream Arrow IPC. Control messages - properties, errors, progress -
//! are JSON, because they are small and structured.
//!
//! ## Declared, not discovered by running
//!
//! A component's ports, properties and version come from its manifest, read
//! without executing anything. That is what lets the catalog, the palette, MCP
//! and `validate` know about a component before it has ever run, and what stops
//! "what components exist here" from being a question that runs third-party
//! code.
//!
//! ## Policy decides, not the manifest
//!
//! A manifest is written by whoever wrote the component. It says what the
//! component is; it does not get to say whether this workspace will run it.
//! That is [`crate::policy`]'s job, and an unapproved component is refused with
//! its id named.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Where a workspace keeps external components.
pub const DIR: &str = "components";
/// The file that declares one.
pub const MANIFEST: &str = "duckle-component.json";

/// A port the component reads or writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Port {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

/// How the host starts the component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Runtime {
    /// argv, run with the workspace as its working directory. Never a shell
    /// string: a string would be split by a shell, and a component path with a
    /// space in it would become two arguments.
    pub command: Vec<String>,
    /// Seconds before the host gives up and kills it.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Which tabular interchange formats the component can handle, best first
    /// (#307).
    ///
    /// `arrow` is the Arrow IPC **stream** format (`.arrows`), which #307
    /// prefers; `parquet` is the fallback it allows. Stream rather than file
    /// because that is what DuckDB writes - a component should reach for
    /// `open_stream`, not `open_file`. Defaults to parquet only, so a component written before this
    /// existed keeps getting exactly what it got - declaring a format you
    /// cannot read is the one mistake here that produces an unreadable file
    /// rather than an error.
    #[serde(default = "default_interchange")]
    pub interchange: Vec<String>,
    /// The lock file pinning the component's dependencies, relative to the
    /// component directory. Hashed into the run manifest so a run records what
    /// it actually ran against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock: Option<String>,
}

fn default_timeout() -> u64 {
    300
}

fn default_format() -> String {
    PARQUET.to_string()
}

fn default_interchange() -> Vec<String> {
    vec![PARQUET.to_string()]
}

pub const ARROW: &str = "arrow";
pub const PARQUET: &str = "parquet";

/// The format the host and the component will use.
///
/// The component's preference order decides, and the host's ability vetoes:
/// Arrow IPC needs a DuckDB extension that may not be installable on a machine
/// with no network, and failing a run over an interchange preference would be
/// absurd when a perfectly good fallback exists.
pub fn choose_interchange(component: &[String], host_has_arrow: bool) -> &'static str {
    for want in component {
        match want.trim().to_ascii_lowercase().as_str() {
            ARROW if host_has_arrow => return ARROW,
            PARQUET => return PARQUET,
            _ => continue,
        }
    }
    PARQUET
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// `ext.` plus a name. The prefix is required so an external component can
    /// never shadow a built-in one - a component called `xf.filter` that quietly
    /// replaced the real one would be the worst possible failure here.
    pub id: String,
    pub version: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub inputs: Vec<Port>,
    #[serde(default)]
    pub outputs: Vec<Port>,
    /// The property form, in the same shape a built-in component's manifest
    /// uses, so the palette and MCP need no special case.
    #[serde(default)]
    pub properties: serde_json::Value,
    pub runtime: Runtime,
}

/// A manifest plus where it was found and what it hashes to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Installed {
    #[serde(flatten)]
    pub manifest: Manifest,
    /// Directory holding the manifest.
    pub dir: String,
    /// sha256 of the manifest bytes, and of the lock file when there is one.
    pub manifest_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_hash: Option<String>,
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Validate a manifest before it is allowed to describe anything.
///
/// Every rule here exists because breaking it would let a component be
/// something other than what the catalog says it is.
pub fn validate(m: &Manifest) -> Result<(), String> {
    if !m.id.starts_with("ext.") {
        return Err(format!(
            "component id {:?} must start with `ext.` so an external component cannot shadow a \
             built-in one",
            m.id
        ));
    }
    let name = &m.id["ext.".len()..];
    if name.is_empty()
        || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(format!("component id {:?} is not a plain identifier", m.id));
    }
    if m.version.trim().is_empty() {
        return Err(format!("{}: version is required", m.id));
    }
    if m.runtime.command.is_empty() {
        return Err(format!("{}: runtime.command is required", m.id));
    }
    if m.runtime.command.iter().any(|a| a.trim().is_empty()) {
        return Err(format!("{}: runtime.command has an empty argument", m.id));
    }
    Ok(())
}

pub fn dir(workspace: &Path) -> PathBuf {
    workspace.join(DIR)
}

/// Every external component this workspace declares.
///
/// A manifest that does not parse or does not validate is REPORTED, not
/// skipped: a component silently missing from the palette is a bug report about
/// the wrong thing, and the author needs to know which file and why.
pub fn discover(workspace: &Path) -> (Vec<Installed>, Vec<String>) {
    let mut found = Vec::new();
    let mut problems = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir(workspace)) else { return (found, problems) };
    let mut dirs: Vec<PathBuf> = entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()).collect();
    dirs.sort();
    for d in dirs {
        let path = d.join(MANIFEST);
        if !path.exists() {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                problems.push(format!("{}: {e}", path.display()));
                continue;
            }
        };
        let manifest: Manifest = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                problems.push(format!("{}: {e}", path.display()));
                continue;
            }
        };
        if let Err(e) = validate(&manifest) {
            problems.push(format!("{}: {e}", path.display()));
            continue;
        }
        let lock_hash = manifest
            .runtime
            .lock
            .as_ref()
            .and_then(|rel| std::fs::read(d.join(rel)).ok())
            .map(|b| sha256(&b));
        found.push(Installed {
            manifest,
            dir: d.display().to_string(),
            manifest_hash: sha256(&bytes),
            lock_hash,
        });
    }
    (found, problems)
}

/// The one this id names, if the workspace has it.
pub fn find(workspace: &Path, component_id: &str) -> Option<Installed> {
    discover(workspace).0.into_iter().find(|i| i.manifest.id == component_id)
}

/// Whether this workspace is allowed to run this component.
///
/// Reuses the component allowlist policy already applies to built-ins, so an
/// operator does not learn a second mechanism and a server-side policy covers
/// external components by construction rather than by remembering to.
pub fn check_allowed(workspace: Option<&Path>, component_id: &str) -> Result<(), String> {
    let policy = crate::policy::load(workspace).map_err(|e| e.to_string())?;
    match policy.allows_component(component_id) {
        true => Ok(()),
        false => Err(format!(
            "policy does not allow the component {component_id}. An external component runs code \
             this workspace did not write, so it is refused unless it is named."
        )),
    }
}

/// The control message handed to a component on stdin.
///
/// Secrets are deliberately absent: #307 asks that raw secrets not travel in
/// pipeline JSON or command arguments, and the same reasoning applies to a
/// message a subprocess could log. A component that needs a credential is given
/// the name of one to resolve from its own environment.
/// Which part of the lifecycle this invocation is (#307).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Check the configuration and report readiness. The component must not do
    /// the work, write the output, or touch anything outside itself: this runs
    /// during `validate`, where a side effect would be a surprise.
    Initialize,
    #[default]
    Execute,
}

/// A line a component may emit while it works.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// 0.0 to 1.0, when the component knows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[serde(default)]
    pub phase: Phase,
    pub protocol: u32,
    pub component: String,
    pub version: String,
    /// The node's properties, already substituted.
    pub properties: serde_json::Value,
    /// `arrow` or `parquet`: what the files named below are, and what the
    /// component must write. Negotiated from what it said it can handle.
    #[serde(default = "default_format")]
    pub format: String,
    /// The tables the component should read, by input port name. Absent for a
    /// source, which has none.
    #[serde(default)]
    pub inputs: BTreeMap<String, String>,
    /// Parquet the component must write.
    pub output: String,
    /// A directory the component MAY write files into (#307).
    ///
    /// Files, models and documents are exchanged by reference rather than
    /// streamed as rows: the component writes the file and names it back, and
    /// the host records where it is and what it hashes to. Given by the host so
    /// a component does not invent a path, and so everything one run produced
    /// is in one place.
    pub artifact_dir: String,
    /// Parquet the component MAY write with the rows it could not handle
    /// (#307). Optional by design: a component with no reject semantics writes
    /// nothing here and the host makes an empty relation, so a wired reject
    /// port is always safe to read.
    pub reject: String,
    pub run_id: String,
}

/// What a component answers.
/// A file a component produced, referenced rather than streamed.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    /// Where it is. A path, or any URI the workspace can resolve.
    pub uri: String,
    /// sha256 of the bytes. The component may declare it - a component that
    /// fetched a document usually knows it already - and the host verifies
    /// rather than trusts. Absent, the host computes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Free text: `model`, `report`, `page-3`. For a person reading a receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Bytes, recorded so a receipt says how big the thing was without
    /// needing the file to still exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    #[serde(default)]
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// Files this run produced (#307).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
}

/// Ask a component whether its configuration is usable, without running it.
///
/// The initialize phase: no input, no output path it should write, and the
/// component is expected to answer and exit. Used by `validate` and by the
/// conformance kit, so a misconfigured component is caught before a pipeline
/// depends on it rather than at midnight.
///
/// A component that does not implement the phase answers as it would for an
/// execute and that is accepted: the phase is optional, and refusing every
/// component written before it existed would be a worse outcome than a check
/// that some components answer trivially.
pub fn initialize(
    installed: &Installed,
    properties: &serde_json::Value,
) -> Result<Response, String> {
    let request = Request {
        phase: Phase::Initialize,
        // Nothing is exchanged during an initialize, so the format is only
        // there for a component that echoes the request back.
        format: PARQUET.to_string(),
        protocol: PROTOCOL,
        component: installed.manifest.id.clone(),
        version: installed.manifest.version.clone(),
        properties: properties.clone(),
        inputs: Default::default(),
        // Named but not to be written. A component that writes here during
        // initialize is doing work it was asked not to do, and the kit checks
        // exactly that.
        output: String::new(),
        reject: String::new(),
        artifact_dir: String::new(),
        run_id: String::new(),
    };
    invoke(installed, &request)
}

/// Check what a component said about the files it produced, and fill in what it
/// did not say.
///
/// A declared hash is VERIFIED rather than trusted: a hash a component asserts
/// about its own output is worth nothing if nobody checks it, and the whole
/// reason to record one is to be able to tell later that the file changed.
pub fn verify_artifacts(dir: &Path, declared: &[Artifact]) -> Result<Vec<Artifact>, String> {
    let mut out = Vec::new();
    for a in declared {
        let raw = a.uri.trim();
        if raw.is_empty() {
            return Err("an artifact was declared with no uri".to_string());
        }
        // A relative uri is resolved inside the directory the host provided, so
        // a component cannot name a file outside the run's own artifacts by
        // accident. An absolute path it wrote itself is taken as given.
        let path = match Path::new(raw).is_absolute() {
            true => PathBuf::from(raw),
            false => dir.join(raw),
        };
        let bytes = std::fs::read(&path)
            .map_err(|e| format!("artifact {raw:?} was declared and cannot be read: {e}"))?;
        let actual = sha256(&bytes);
        if let Some(claimed) = a.hash.as_deref().map(str::trim).filter(|h| !h.is_empty()) {
            if !claimed.eq_ignore_ascii_case(&actual) {
                return Err(format!(
                    "artifact {raw:?} was declared with hash {claimed} and hashes to {actual}"
                ));
            }
        }
        out.push(Artifact {
            // Forward slashes throughout, so a receipt reads the same on every
            // platform and a mixed C:/a path does not appear.
            uri: path.display().to_string().replace(char::from(92), "/"),
            hash: Some(actual),
            media_type: a.media_type.clone(),
            role: a.role.clone(),
            bytes: Some(bytes.len() as u64),
        });
    }
    Ok(out)
}

pub const PROTOCOL: u32 = 1;

/// Run a component once and return what it said.
///
/// The whole protocol, in one place: argv from the manifest, the request as
/// JSON on stdin, a JSON reply on stdout, a kill at the declared timeout. The
/// engine and the conformance kit both call this rather than each implementing
/// the protocol, because two implementations would eventually disagree about
/// what conforming means - and the kit exists to answer exactly that.
pub fn invoke(installed: &Installed, request: &Request) -> Result<Response, String> {
    invoke_with(installed, request, &mut |_| {}, &|| false)
}

/// The same, reporting progress and observing cancellation (#307).
///
/// stdout is read as it arrives rather than collected at the end, which is what
/// makes progress possible at all - and the pipes are drained on their own
/// threads, because a component that writes more than a pipe buffer while the
/// host is not reading deadlocks, which this repository has been bitten by
/// before with DuckDB.
///
/// Cancellation is termination. A portable in-band cancel would need every
/// component to read stdin as a live stream while working, which the simple
/// case - `json.load(sys.stdin)` - cannot do. So the host kills the process and
/// cleans up its own files deterministically; what a component cannot do is
/// clean up in-process, and pretending otherwise would be worse than saying so.
pub fn invoke_with(
    installed: &Installed,
    request: &Request,
    on_progress: &mut dyn FnMut(Progress),
    cancelled: &dyn Fn() -> bool,
) -> Result<Response, String> {
    use std::io::{BufRead, BufReader, Write};
    let body = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    let cmd = &installed.manifest.runtime.command;
    let mut c = std::process::Command::new(&cmd[0]);
    c.args(&cmd[1..])
        .current_dir(&installed.dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        c.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let mut child = c.spawn().map_err(|e| format!("cannot start {:?}: {e}", cmd[0]))?;
    {
        // Written and then CLOSED: a component doing `json.load(sys.stdin)`
        // blocks until EOF, so holding the handle open would hang every simple
        // component ever written against this protocol.
        let mut stdin = child.stdin.take().ok_or("no stdin")?;
        stdin.write_all(&body).map_err(|e| format!("writing the request: {e}"))?;
    }

    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let out_pipe = child.stdout.take().ok_or("no stdout")?;
    std::thread::spawn(move || {
        for line in BufReader::new(out_pipe).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let err_pipe = child.stderr.take().ok_or("no stderr")?;
    let errors = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let sink = errors.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(err_pipe).lines().map_while(Result::ok) {
            if let Ok(mut held) = sink.lock() {
                held.push_str(&line);
                held.push(char::from(10));
            }
        }
    });

    let deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(installed.manifest.runtime.timeout_secs.max(1));
    let mut result: Option<Response> = None;
    let stop = |child: &mut std::process::Child| {
        let _ = child.kill();
        let _ = child.wait();
    };
    loop {
        // Drain whatever has arrived before deciding anything, so a result line
        // written just before the deadline is not thrown away for being late.
        while let Ok(line) = rx.try_recv() {
            match classify(&line) {
                Some(Line::Progress(p)) => on_progress(p),
                Some(Line::Result(r)) => result = Some(r),
                None => {}
            }
        }
        if cancelled() {
            stop(&mut child);
            return Err("cancelled".to_string());
        }
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if std::time::Instant::now() >= deadline => {
                stop(&mut child);
                return Err(format!(
                    "did not finish within {}s",
                    installed.manifest.runtime.timeout_secs
                ));
            }
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => return Err(e.to_string()),
        }
    }
    // The reader thread may still hold buffered lines after the process exits.
    while let Ok(line) = rx.recv_timeout(std::time::Duration::from_millis(200)) {
        match classify(&line) {
            Some(Line::Progress(p)) => on_progress(p),
            Some(Line::Result(r)) => result = Some(r),
            None => {}
        }
    }
    let status = child.wait().map_err(|e| e.to_string())?;
    let stderr = errors.lock().map(|e| e.trim().to_string()).unwrap_or_default();
    if !status.success() {
        return Err(match stderr.is_empty() {
            true => "exited non-zero with no message".to_string(),
            false => stderr,
        });
    }
    result.ok_or_else(|| {
        format!("did not answer with a control message. stderr: {stderr}")
    })
}

enum Line {
    Progress(Progress),
    Result(Response),
}

/// What one line of a component's stdout was.
///
/// A bare object carrying `ok` is a result, which is what every component
/// written against the first version of this protocol emits - so they keep
/// working without knowing progress exists.
fn classify(line: &str) -> Option<Line> {
    let v: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    match v.get("type").and_then(|t| t.as_str()) {
        Some("progress") => serde_json::from_value(v).ok().map(Line::Progress),
        Some("result") => serde_json::from_value(v).ok().map(Line::Result),
        _ if v.get("ok").is_some() => serde_json::from_value(v).ok().map(Line::Result),
        _ => None,
    }
}

/// One external component, in the shape the built-in catalog uses.
///
/// The same conversion for every consumer - MCP discovery, the palette, the
/// properties form - so an external component is described identically
/// everywhere rather than three times with three sets of defaults. `kind` is
/// derived from the ports because that is what the palette groups by: no
/// inputs is a source, no outputs is a sink, both is a transform.
pub fn as_catalog_entry(installed: &Installed) -> serde_json::Value {
    let m = &installed.manifest;
    let kind = match (m.inputs.is_empty(), m.outputs.is_empty()) {
        (true, false) => "source",
        (false, true) => "sink",
        _ => "transform",
    };
    let label = match m.label.trim().is_empty() {
        true => m.id.clone(),
        false => m.label.clone(),
    };
    serde_json::json!({
        "id": m.id,
        "label": label,
        "kind": kind,
        "availability": "available",
        "summary": m.description,
        "version": m.version,
        // Marked, so a surface can say where a component came from rather than
        // presenting third-party code as though Duckle shipped it.
        "external": true,
        "manifestHash": installed.manifest_hash,
        "lockHash": installed.lock_hash,
        "manifest": {
            "id": m.id,
            "kind": kind,
            "label": label,
            "description": m.description,
            "schemaSource": "upstream",
            "sections": m.properties.get("sections").cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        },
    })
}

/// What a run used, recorded on its receipt (#307 criterion 4).
///
/// The hashes are of the manifest and the lock file, so "what exactly did this
/// run execute" is answerable afterwards. A version alone would not be: a
/// component edited in place keeps its version, and the whole point of
/// recording this is the case where somebody changed something.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Used {
    pub id: String,
    pub version: String,
    pub manifest_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_hash: Option<String>,
    /// True when the pipeline names a component this workspace does not have.
    /// Recorded rather than omitted: a receipt that simply lacks an entry is
    /// indistinguishable from a run that used no external components at all.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
}

/// The external components a pipeline document names, with their hashes.
///
/// Derived from the document rather than from what happened to execute, so a
/// component on a branch that did not run this time is still recorded as part
/// of what the pipeline was.
pub fn used_by(workspace: &Path, doc: &serde_json::Value) -> Vec<Used> {
    let mut ids: std::collections::BTreeSet<String> = Default::default();
    if let Some(nodes) = doc.get("nodes").and_then(|n| n.as_array()) {
        for n in nodes {
            if let Some(id) = n
                .get("data")
                .and_then(|d| d.get("componentId"))
                .and_then(|v| v.as_str())
            {
                if id.starts_with("ext.") {
                    ids.insert(id.to_string());
                }
            }
        }
    }
    let installed = discover(workspace).0;
    ids.into_iter()
        .map(|id| match installed.iter().find(|i| i.manifest.id == id) {
            Some(i) => Used {
                id,
                version: i.manifest.version.clone(),
                manifest_hash: i.manifest_hash.clone(),
                lock_hash: i.lock_hash.clone(),
                missing: false,
            },
            None => Used {
                id,
                version: String::new(),
                manifest_hash: String::new(),
                lock_hash: None,
                missing: true,
            },
        })
        .collect()
}

/// Every external component this workspace declares, catalog-shaped.
pub fn catalog_entries(workspace: &Path) -> Vec<serde_json::Value> {
    discover(workspace).0.iter().map(as_catalog_entry).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(id: &str) -> Manifest {
        Manifest {
            id: id.into(),
            version: "1.0.0".into(),
            label: "Test".into(),
            description: String::new(),
            inputs: vec![Port { name: "main".into(), description: String::new() }],
            outputs: vec![Port { name: "main".into(), description: String::new() }],
            properties: serde_json::json!({}),
            runtime: Runtime {
                command: vec!["python".into(), "run.py".into()],
                timeout_secs: 30,
                interchange: default_interchange(),
                lock: None,
            },
        }
    }

    fn install(ws: &Path, name: &str, body: &str) {
        let d = dir(ws).join(name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(MANIFEST), body).unwrap();
    }

    #[test]
    fn an_external_component_cannot_shadow_a_built_in_one() {
        // The worst possible failure here: a component called xf.filter that
        // quietly replaced the real one.
        for id in ["xf.filter", "src.postgres", "filter", "code.python"] {
            let e = validate(&manifest(id)).unwrap_err();
            assert!(e.contains("ext."), "{id} was accepted: {e}");
        }
        assert!(validate(&manifest("ext.ixbrl")).is_ok());
    }

    #[test]
    fn an_id_must_be_a_plain_identifier() {
        for id in ["ext.", "ext.a b", "ext.a/b", "ext.a;rm -rf"] {
            assert!(validate(&manifest(id)).is_err(), "{id} was accepted");
        }
    }

    #[test]
    fn a_component_with_no_command_is_refused() {
        let mut m = manifest("ext.x");
        m.runtime.command = vec![];
        assert!(validate(&m).is_err());
        m.runtime.command = vec!["python".into(), "  ".into()];
        assert!(validate(&m).is_err(), "an empty argument is not a command");
    }

    #[test]
    fn discovery_reports_a_broken_manifest_rather_than_skipping_it() {
        // A component silently missing from the palette is a bug report about
        // the wrong thing.
        let tmp = tempfile::tempdir().unwrap();
        install(tmp.path(), "good", &serde_json::to_string(&manifest("ext.good")).unwrap());
        install(tmp.path(), "broken", "{ not json");
        install(
            tmp.path(),
            "shadow",
            &serde_json::to_string(&manifest("xf.filter")).unwrap(),
        );
        let (found, problems) = discover(tmp.path());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].manifest.id, "ext.good");
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems.iter().any(|p| p.contains("broken")), "{problems:?}");
        assert!(problems.iter().any(|p| p.contains("ext.")), "{problems:?}");
    }

    #[test]
    fn a_component_is_hashed_so_a_run_can_record_what_it_ran() {
        let tmp = tempfile::tempdir().unwrap();
        let mut m = manifest("ext.hashed");
        m.runtime.lock = Some("uv.lock".into());
        install(tmp.path(), "hashed", &serde_json::to_string(&m).unwrap());
        std::fs::write(dir(tmp.path()).join("hashed").join("uv.lock"), "pinned==1.0").unwrap();

        let found = find(tmp.path(), "ext.hashed").expect("discovered");
        assert_eq!(found.manifest_hash.len(), 64);
        assert_eq!(found.lock_hash.as_ref().map(String::len), Some(64));

        // Editing the lock changes the hash, which is the whole point.
        std::fs::write(dir(tmp.path()).join("hashed").join("uv.lock"), "pinned==2.0").unwrap();
        let again = find(tmp.path(), "ext.hashed").unwrap();
        assert_ne!(again.lock_hash, found.lock_hash);
        assert_eq!(again.manifest_hash, found.manifest_hash, "the manifest did not change");
    }

    #[test]
    fn a_workspace_with_no_components_directory_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let (found, problems) = discover(tmp.path());
        assert!(found.is_empty() && problems.is_empty());
    }

    #[test]
    fn a_run_records_the_components_it_used_with_their_hashes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut m = manifest("ext.upper");
        m.runtime.lock = Some("requirements.txt".into());
        install(tmp.path(), "upper", &serde_json::to_string(&m).unwrap());
        std::fs::write(dir(tmp.path()).join("upper").join("requirements.txt"), "duckdb==1.5.0")
            .unwrap();

        let doc = serde_json::json!({ "nodes": [
            { "id": "s", "data": { "componentId": "src.csv" } },
            { "id": "u", "data": { "componentId": "ext.upper" } },
            { "id": "v", "data": { "componentId": "ext.upper" } }
        ]});
        let used = used_by(tmp.path(), &doc);
        assert_eq!(used.len(), 1, "one entry per component, not per node: {used:?}");
        assert_eq!(used[0].id, "ext.upper");
        assert_eq!(used[0].version, "1.0.0");
        assert_eq!(used[0].manifest_hash.len(), 64);
        assert_eq!(used[0].lock_hash.as_ref().map(String::len), Some(64));
        assert!(!used[0].missing);

        // The hash is what makes this worth recording: a component edited in
        // place keeps its version, and that is exactly the case being guarded.
        std::fs::write(dir(tmp.path()).join("upper").join("requirements.txt"), "duckdb==9.9.9")
            .unwrap();
        assert_ne!(used_by(tmp.path(), &doc)[0].lock_hash, used[0].lock_hash);
    }

    #[test]
    fn a_component_the_workspace_does_not_have_is_recorded_as_missing() {
        // Omitted, it would be indistinguishable from a run that used no
        // external components at all.
        let tmp = tempfile::tempdir().unwrap();
        let doc = serde_json::json!({ "nodes": [
            { "id": "x", "data": { "componentId": "ext.absent" } }
        ]});
        let used = used_by(tmp.path(), &doc);
        assert_eq!(used.len(), 1);
        assert!(used[0].missing);
        assert_eq!(used[0].id, "ext.absent");
    }

    #[test]
    fn a_pipeline_with_no_external_components_records_none() {
        let tmp = tempfile::tempdir().unwrap();
        let doc = serde_json::json!({ "nodes": [
            { "id": "s", "data": { "componentId": "src.csv" } }
        ]});
        assert!(used_by(tmp.path(), &doc).is_empty());
    }

    #[test]
    fn the_component_prefers_and_the_host_vetoes() {
        // The component's order decides, and the host's ability vetoes:
        // failing a run over an interchange preference would be absurd when a
        // perfectly good fallback exists.
        let both = vec!["arrow".to_string(), "parquet".to_string()];
        assert_eq!(choose_interchange(&both, true), ARROW);
        assert_eq!(choose_interchange(&both, false), PARQUET, "no extension, no arrow");

        // Order is the component's preference, not ours.
        let prefers_parquet = vec!["parquet".to_string(), "arrow".to_string()];
        assert_eq!(choose_interchange(&prefers_parquet, true), PARQUET);
    }

    #[test]
    fn an_arrow_only_component_still_gets_parquet_rather_than_nothing() {
        // It will probably fail to read it, and that is a clear error from the
        // component. Handing it no format at all, or failing before it runs,
        // tells the author less.
        assert_eq!(choose_interchange(&["arrow".to_string()], false), PARQUET);
        // And a format nobody knows is ignored rather than passed through.
        assert_eq!(choose_interchange(&["feather".to_string()], true), PARQUET);
        assert_eq!(choose_interchange(&[], true), PARQUET, "declaring none means the default");
    }

    #[test]
    fn a_component_that_never_mentions_interchange_gets_parquet() {
        // Every component written before this existed keeps getting exactly
        // what it got.
        let m: Manifest = serde_json::from_str(
            r#"{"id":"ext.old","version":"1","runtime":{"command":["x"]}}"#,
        )
        .unwrap();
        assert_eq!(m.runtime.interchange, vec!["parquet".to_string()]);
        assert_eq!(choose_interchange(&m.runtime.interchange, true), PARQUET);
    }

    #[test]
    fn a_component_written_before_progress_existed_still_works() {
        // A bare object carrying `ok` is a result. Every component written
        // against the first version of this protocol emits exactly that, and
        // they must keep working without knowing progress exists.
        assert!(matches!(classify(r#"{"ok":true,"rows":3}"#), Some(Line::Result(_))));
        let Some(Line::Result(r)) = classify(r#"{"ok":false,"error":"boom"}"#) else {
            panic!("not a result")
        };
        assert!(!r.ok);
        assert_eq!(r.error.as_deref(), Some("boom"));
    }

    #[test]
    fn progress_and_result_lines_are_told_apart() {
        let Some(Line::Progress(p)) =
            classify(r#"{"type":"progress","rows":10,"fraction":0.5,"message":"half"}"#)
        else {
            panic!("not progress")
        };
        assert_eq!(p.rows, Some(10));
        assert_eq!(p.fraction, Some(0.5));
        assert_eq!(p.message.as_deref(), Some("half"));
        assert!(matches!(classify(r#"{"type":"result","ok":true}"#), Some(Line::Result(_))));
    }

    #[test]
    fn a_stray_line_does_not_break_the_protocol() {
        // Components print things. A log line, a warning from a library, a
        // blank line - none of them are control messages, and treating one as
        // a malformed result would fail runs that are fine.
        for noise in [
            "starting up",
            "",
            "   ",
            "[]",
            r#"{"unrelated":1}"#,
            "Traceback (most recent call last):",
        ] {
            assert!(classify(noise).is_none(), "{noise:?} was taken for a control message");
        }
    }

    #[test]
    fn an_initialize_asks_for_no_work_to_be_done() {
        // The phase exists so validate can check a configuration without side
        // effects, so it names no input and no output to write.
        let m = manifest("ext.x");
        let i = Installed {
            manifest: m,
            dir: ".".into(),
            manifest_hash: "h".into(),
            lock_hash: None,
        };
        // Build the request the same way `initialize` does, without spawning.
        let req = Request {
            phase: Phase::Initialize,
            format: PARQUET.into(),
            protocol: PROTOCOL,
            component: i.manifest.id.clone(),
            version: i.manifest.version.clone(),
            properties: serde_json::json!({}),
            inputs: Default::default(),
            output: String::new(),
            reject: String::new(),
            artifact_dir: String::new(),
            run_id: String::new(),
        };
        assert!(req.inputs.is_empty(), "initialize reads nothing");
        assert!(req.output.is_empty(), "and is given nowhere to write");
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains(r#""phase":"initialize""#), "{json}");
    }

    #[test]
    fn a_declared_hash_is_verified_rather_than_trusted() {
        // A hash a component asserts about its own output is worth nothing if
        // nobody checks it, and being able to tell later that the file changed
        // is the entire reason to record one.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("doc.md"), b"hello").unwrap();
        let real = sha256(b"hello");

        let ok = verify_artifacts(
            tmp.path(),
            &[Artifact { uri: "doc.md".into(), hash: Some(real.clone()), ..Default::default() }],
        )
        .expect("a correct hash");
        assert_eq!(ok[0].hash.as_deref(), Some(real.as_str()));
        assert_eq!(ok[0].bytes, Some(5));

        let e = verify_artifacts(
            tmp.path(),
            &[Artifact { uri: "doc.md".into(), hash: Some("0".repeat(64)), ..Default::default() }],
        )
        .unwrap_err();
        assert!(e.contains("hashes to"), "{e}");
    }

    #[test]
    fn an_artifact_that_was_declared_and_not_written_is_an_error() {
        // A run that says it produced a document and did not is a run whose
        // provenance is a lie.
        let tmp = tempfile::tempdir().unwrap();
        let e = verify_artifacts(
            tmp.path(),
            &[Artifact { uri: "absent.pdf".into(), ..Default::default() }],
        )
        .unwrap_err();
        assert!(e.contains("cannot be read"), "{e}");
        // An empty uri is refused BY NAME rather than by happening to fail when
        // the directory is read as a file - the message is what tells the
        // author their manifest is wrong rather than their disk.
        let e = verify_artifacts(tmp.path(), &[Artifact::default()]).unwrap_err();
        assert!(e.contains("no uri"), "{e}");
    }

    #[test]
    fn the_host_computes_a_hash_the_component_did_not_declare() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("m.bin"), b"model").unwrap();
        let out = verify_artifacts(
            tmp.path(),
            &[Artifact { uri: "m.bin".into(), hash: None, ..Default::default() }],
        )
        .unwrap();
        assert_eq!(out[0].hash.as_deref(), Some(sha256(b"model").as_str()));
        // A relative uri resolves inside the directory the host provided, so a
        // component cannot name a file outside the run's artifacts by accident.
        assert!(out[0].uri.ends_with("m.bin"));
        assert!(out[0].uri.contains(&tmp.path().display().to_string().replace(char::from(92), "/")));
    }

    #[test]
    fn a_catalog_entry_takes_its_kind_from_the_ports() {
        // The palette groups by kind, and a component with no inputs is a
        // source however its author described it.
        let mut m = manifest("ext.reader");
        m.inputs.clear();
        let e = as_catalog_entry(&Installed {
            manifest: m.clone(),
            dir: "d".into(),
            manifest_hash: "h".into(),
            lock_hash: None,
        });
        assert_eq!(e["kind"], "source");
        assert_eq!(e["external"], true, "third-party code must not look built-in");

        let mut w = manifest("ext.writer");
        w.outputs.clear();
        let e = as_catalog_entry(&Installed {
            manifest: w,
            dir: "d".into(),
            manifest_hash: "h".into(),
            lock_hash: None,
        });
        assert_eq!(e["kind"], "sink");
        assert_eq!(as_catalog_entry(&Installed {
            manifest: manifest("ext.both"),
            dir: "d".into(),
            manifest_hash: "h".into(),
            lock_hash: None,
        })["kind"], "transform");
    }

    #[test]
    fn a_component_with_no_label_falls_back_to_its_id() {
        // An unlabelled tile in the palette is worse than a technical one.
        let mut m = manifest("ext.unlabelled");
        m.label = String::new();
        let e = as_catalog_entry(&Installed {
            manifest: m,
            dir: "d".into(),
            manifest_hash: "h".into(),
            lock_hash: None,
        });
        assert_eq!(e["label"], "ext.unlabelled");
    }

    #[test]
    fn the_request_carries_no_secrets() {
        // #307: raw secrets must not travel in pipeline JSON or command
        // arguments, and a subprocess could log its own stdin.
        let r = Request {
            phase: Phase::Execute,
            format: PARQUET.into(),
            protocol: PROTOCOL,
            component: "ext.x".into(),
            version: "1".into(),
            properties: serde_json::json!({ "url": "https://x", "tokenRef": "MY_TOKEN" }),
            inputs: BTreeMap::from([("main".into(), "in.parquet".into())]),
            output: "out.parquet".into(),
            reject: "rej.parquet".into(),
            artifact_dir: "artifacts".into(),
            run_id: "run-1".into(),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("tokenRef"), "a reference is fine");
        assert!(!json.contains("password"), "{json}");
        // The shape a component reads back.
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.inputs.get("main").map(String::as_str), Some("in.parquet"));
    }
}
