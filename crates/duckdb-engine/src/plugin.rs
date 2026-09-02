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
    /// The lock file pinning the component's dependencies, relative to the
    /// component directory. Hashed into the run manifest so a run records what
    /// it actually ran against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock: Option<String>,
}

fn default_timeout() -> u64 {
    300
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub protocol: u32,
    pub component: String,
    pub version: String,
    /// The node's properties, already substituted.
    pub properties: serde_json::Value,
    /// Parquet the component should read, by input port name. Absent for a
    /// source, which has none.
    #[serde(default)]
    pub inputs: BTreeMap<String, String>,
    /// Parquet the component must write.
    pub output: String,
    pub run_id: String,
}

/// What a component answers.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    #[serde(default)]
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

pub const PROTOCOL: u32 = 1;

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
    fn the_request_carries_no_secrets() {
        // #307: raw secrets must not travel in pipeline JSON or command
        // arguments, and a subprocess could log its own stdin.
        let r = Request {
            protocol: PROTOCOL,
            component: "ext.x".into(),
            version: "1".into(),
            properties: serde_json::json!({ "url": "https://x", "tokenRef": "MY_TOKEN" }),
            inputs: BTreeMap::from([("main".into(), "in.parquet".into())]),
            output: "out.parquet".into(),
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
