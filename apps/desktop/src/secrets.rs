//! Encryption at rest for saved connection secrets and the cached git PAT.
//!
//! The AES-256-GCM implementation lives in the shared `duckle-secrets` crate
//! (#166 stage 2) so the headless runner resolves saved connections through
//! the exact same decrypt path; this module keeps the desktop-facing Tauri
//! commands and re-exports the primitives `workspace_git.rs` uses.

use std::path::Path;

pub(crate) use duckle_secrets::{decrypt_value, encrypt_value, is_encrypted, workspace_key};

/// Encrypt the sensitive fields of a connection payload JSON before it is
/// written to disk.
#[tauri::command]
pub fn connection_encrypt_payload(workspace: String, payload_json: String) -> Result<String, String> {
    duckle_secrets::encrypt_payload_json(Path::new(&workspace), &payload_json)
}

/// Decrypt the sensitive fields of a connection payload JSON after it is read
/// from disk. If the workspace key is missing, the payload is returned
/// unchanged so plaintext / legacy values still load.
#[tauri::command]
pub fn connection_decrypt_payload(workspace: String, payload_json: String) -> Result<String, String> {
    duckle_secrets::decrypt_payload_json(Path::new(&workspace), &payload_json)
}
