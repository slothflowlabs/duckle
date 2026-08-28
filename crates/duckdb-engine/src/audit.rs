//! The audit log's shape, and appends from inside the engine.
//!
//! The console writes this file for HTTP requests. The engine writes to it too,
//! because some changes an operator makes never go through the console: the
//! CLI, MCP and the desktop panel all reach the state mutators directly. An
//! audit that only sees one of four doors is not an audit.
//!
//! The record type lives here rather than in the console so there is exactly
//! one of it. A log written by one shape and read by another drifts the first
//! time a field is renamed, and the symptom is a reader quietly showing blanks.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub fn audit_path(workspace: &Path) -> PathBuf {
    workspace.join("logs").join("audit.ndjson")
}

/// One recorded attempt.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Entry {
    /// RFC3339, when the attempt was made.
    #[serde(default)]
    pub at: String,
    /// Who, or "-" for a caller that never identified itself.
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub role: String,
    pub action: String,
    #[serde(default)]
    pub target: String,
    pub outcome: String,
    /// What actually changed, when the action changed something.
    ///
    /// "cleared the baseline" is not enough to review afterwards; the value it
    /// held is the thing an operator needs to see. Optional because a request
    /// that was merely refused has nothing to describe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Append one event. Never fails the operation it describes.
///
/// A run that aborted because its audit file was unwritable would turn a full
/// disk into an outage, so a write failure goes to stderr and the caller
/// continues. That is a deliberate trade, and the reason this is not the only
/// record: the state file itself is still the truth about what is stored.
pub fn append_entry(workspace: &Path, entry: &Entry) {
    let line = match serde_json::to_string(entry) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("duckle: could not encode an audit entry: {e}");
            return;
        }
    };
    let path = audit_path(workspace);
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!("duckle: could not create {}: {e}", parent.display());
            return;
        }
    }
    use std::io::Write;
    match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut f) => {
            if let Err(e) = writeln!(f, "{line}") {
                eprintln!("duckle: could not write the audit log: {e}");
            }
        }
        Err(e) => eprintln!("duckle: could not open the audit log: {e}"),
    }
}

/// A change made from inside the engine, by whoever is running it.
///
/// `DUCKLE_ACTOR` is how a surface says who is asking - the console sets it
/// from the authenticated identity, and a person at a terminal is "-" rather
/// than a name invented on their behalf.
pub fn note(workspace: &Path, action: &str, target: &str, detail: Option<String>) {
    append_entry(
        workspace,
        &Entry {
            at: chrono::Utc::now().to_rfc3339(),
            actor: std::env::var("DUCKLE_ACTOR")
                .ok()
                .filter(|a| !a.trim().is_empty())
                .unwrap_or_else(|| "-".into()),
            role: std::env::var("DUCKLE_ACTOR_ROLE")
                .ok()
                .filter(|a| !a.trim().is_empty())
                .unwrap_or_else(|| "-".into()),
            action: action.to_string(),
            target: target.to_string(),
            outcome: "allowed".into(),
            detail,
        },
    );
}
