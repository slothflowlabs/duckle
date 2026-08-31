//! Encryption at rest for saved connection secrets, plus run-time resolution
//! of saved-connection references (#166 stage 2).
//!
//! Sensitive connection fields (passwords, tokens, keys) are encrypted with a
//! per-workspace AES-256-GCM key kept at `<workspace>/.duckle/keys/secret.key`
//! (owner-only on unix, excluded from version control). The connection JSON in
//! `<workspace>/connections/` therefore holds ciphertext for those fields, so
//! the folder is safe to commit or share as long as `.duckle/keys/` is not.
//! `${...}` placeholders are never encrypted - they resolve from the
//! environment at run time.
//!
//! This crate is the single decrypt path shared by the desktop app and the
//! headless runner (#166): both call [`resolve_connection_refs`] before their
//! `${ENV:...}` pass so a node that stores only a `connectionRef` gets its
//! auth fields injected in memory - the engine stays credential-agnostic and
//! secrets never land in the pipeline file.

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine;
use duckle_metadata::PipelineNode;
use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};

pub const ENC_PREFIX: &str = "enc:v1:";
/// Ciphertext bound to its field via AES-GCM associated data. See [`aad_for`].
pub const ENC_PREFIX_V2: &str = "enc:v2:";

/// Connection-payload fields (by name) that hold a secret and get encrypted.
pub const SENSITIVE_KEYS: &[&str] = &[
    "password",
    "secretKey",
    "accessKey",
    "accountKey",
    "sessionToken",
    "pat",
    "token",
    "apiKey",
    "passphrase",
    "secret",
    // Salesforce OAuth client-credentials + bearer token (#166 stage 2).
    "clientSecret",
    "accessToken",
    // The bearer/API token on a saved REST connection.
    "authToken",
];

fn key_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("keys").join("secret.key")
}

/// Load the workspace key. With `create`, a fresh random 32-byte key is
/// generated and persisted on first use; without it, a missing key is an
/// error (so a workspace shared without the key decrypts to nothing rather
/// than minting a wrong key).
pub fn workspace_key(workspace: &Path, create: bool) -> Result<[u8; 32], String> {
    let path = key_path(workspace);
    if let Ok(bytes) = std::fs::read(&path) {
        if bytes.len() == 32 {
            let mut k = [0u8; 32];
            k.copy_from_slice(&bytes);
            return Ok(k);
        }
    }
    if !create {
        return Err("no workspace key".into());
    }
    let mut k = [0u8; 32];
    getrandom::fill(&mut k).map_err(|e| format!("key rng: {}", e))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create keys dir: {}", e))?;
    }
    // Create the key file owner-only from the start; writing first and
    // chmod'ing after left a brief world-readable window (TOCTOU).
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .map_err(|e| format!("create key: {}", e))?;
        f.write_all(&k).map_err(|e| format!("write key: {}", e))?;
    }
    #[cfg(not(unix))]
    std::fs::write(&path, k).map_err(|e| format!("write key: {}", e))?;
    Ok(k)
}

pub fn is_encrypted(s: &str) -> bool {
    s.starts_with(ENC_PREFIX) || s.starts_with(ENC_PREFIX_V2)
}

/// Bind a ciphertext to the place it belongs.
///
/// AES-GCM authenticates its associated data, so a value sealed under one context
/// fails to open under another. Without it every ciphertext under a workspace key
/// is interchangeable: the same blob decrypts in ANY field of ANY connection, so
/// anyone able to write a connection file could move a production password into a
/// connection pointing at a host they control and have the engine send it there.
///
/// The unit separator is not valid inside an id or a field name, so `a` + `b.c`
/// and `a.b` + `c` cannot produce the same associated data.
pub fn aad_for(context: &str, field: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(context.len() + field.len() + 1);
    v.extend_from_slice(context.as_bytes());
    v.push(0x1f);
    v.extend_from_slice(field.as_bytes());
    v
}

/// Encrypt plaintext into an `enc:v2:<base64(nonce || ciphertext)>` token, bound to
/// `aad` (see [`aad_for`]). The same value sealed for a different field, or a
/// different connection, will not decrypt here.
pub fn encrypt_value(key: &[u8; 32], aad: &[u8], plaintext: &str) -> Result<String, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| format!("cipher init: {}", e))?;
    let mut nonce_bytes = [0u8; 12];
    getrandom::fill(&mut nonce_bytes).map_err(|e| format!("nonce rng: {}", e))?;
    let ciphertext = cipher
        .encrypt(
            &Nonce::from(nonce_bytes),
            Payload { msg: plaintext.as_bytes(), aad },
        )
        .map_err(|e| format!("encrypt: {}", e))?;
    let mut payload = Vec::with_capacity(12 + ciphertext.len());
    payload.extend_from_slice(&nonce_bytes);
    payload.extend_from_slice(&ciphertext);
    Ok(format!(
        "{}{}",
        ENC_PREFIX_V2,
        base64::engine::general_purpose::STANDARD.encode(payload)
    ))
}

/// Decrypt an `enc:v2:` token, checking it was sealed for this `aad`.
///
/// An `enc:v1:` token predates the binding and carries no associated data, so it is
/// decrypted without one. That path exists purely so secrets stored before the
/// change keep opening; it is the weakness v2 closes, and a value is upgraded to v2
/// the next time its connection is saved.
pub fn decrypt_value(key: &[u8; 32], aad: &[u8], blob: &str) -> Result<String, String> {
    let (b64, bound) = match blob.strip_prefix(ENC_PREFIX_V2) {
        Some(rest) => (rest, true),
        None => (blob.strip_prefix(ENC_PREFIX).ok_or("not an encrypted value")?, false),
    };
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("base64: {}", e))?;
    if raw.len() < 12 {
        return Err("ciphertext too short".into());
    }
    let (nonce_bytes, ciphertext) = raw.split_at(12);
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| format!("cipher init: {}", e))?;
    let nonce = Nonce::try_from(nonce_bytes).map_err(|e| format!("nonce: {}", e))?;
    let plain = if bound {
        cipher
            .decrypt(&nonce, Payload { msg: ciphertext, aad })
            .map_err(|_| SEALED_ELSEWHERE.to_string())?
    } else {
        cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|e| format!("decrypt: {}", e))?
    };
    String::from_utf8(plain).map_err(|e| format!("utf8: {}", e))
}

const SEALED_ELSEWHERE: &str =
    "decrypt: this value was not sealed for this field, so it cannot be moved between fields or between connections";

/// Walk a JSON value, encrypting or decrypting the sensitive string fields in
/// place. Already-encrypted values and `${...}` placeholders are left alone.
fn transform(
    value: &mut JsonValue,
    key: &[u8; 32],
    context: &str,
    encrypting: bool,
) -> Result<(), String> {
    match value {
        JsonValue::Object(map) => {
            for (k, v) in map.iter_mut() {
                if let Some(s) = v.as_str() {
                    if encrypting {
                        if SENSITIVE_KEYS.contains(&k.as_str())
                            && !s.is_empty()
                            && !is_encrypted(s)
                            && !s.starts_with("${")
                        {
                            // Propagate: never silently leave a secret in
                            // plaintext (the file is meant to hold ciphertext).
                            let enc = encrypt_value(key, &aad_for(context, k), s)?;
                            *v = JsonValue::String(enc);
                        }
                    } else if is_encrypted(s) {
                        // Decrypt stays lenient: a missing/legacy value loads
                        // unchanged rather than failing the read.
                        if let Ok(dec) = decrypt_value(key, &aad_for(context, k), s) {
                            *v = JsonValue::String(dec);
                        }
                    }
                } else {
                    transform(v, key, context, encrypting)?;
                }
            }
        }
        JsonValue::Array(arr) => {
            for v in arr.iter_mut() {
                transform(v, key, context, encrypting)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Encrypt the sensitive fields of a connection payload JSON before it is
/// written to disk. Creates the workspace key on first use.
pub fn encrypt_payload_json(
    workspace: &Path,
    connection_id: &str,
    payload_json: &str,
) -> Result<String, String> {
    let key = workspace_key(workspace, true)?;
    let mut v: JsonValue =
        serde_json::from_str(payload_json).map_err(|e| format!("json: {}", e))?;
    transform(&mut v, &key, connection_id, true)?;
    serde_json::to_string(&v).map_err(|e| format!("json: {}", e))
}

/// Decrypt the sensitive fields of a connection payload JSON after it is read
/// from disk. If the workspace key is missing, the payload is returned
/// unchanged so plaintext / legacy values still load. (Editor-facing and
/// deliberately LENIENT - the run-time path is [`load_connection`], which is
/// strict, because executing with `enc:v1:` ciphertext as a credential is a
/// confusing downstream auth failure.)
pub fn decrypt_payload_json(
    workspace: &Path,
    connection_id: &str,
    payload_json: &str,
) -> Result<String, String> {
    let key = match workspace_key(workspace, false) {
        Ok(k) => k,
        Err(_) => return Ok(payload_json.to_string()),
    };
    let mut v: JsonValue =
        serde_json::from_str(payload_json).map_err(|e| format!("json: {}", e))?;
    transform(&mut v, &key, connection_id, false)?;
    serde_json::to_string(&v).map_err(|e| format!("json: {}", e))
}

/// Any string field anywhere in the value still carrying the `enc:v1:` prefix?
fn any_encrypted(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(s) => is_encrypted(s),
        JsonValue::Object(map) => map.values().any(any_encrypted),
        JsonValue::Array(arr) => arr.iter().any(any_encrypted),
        _ => false,
    }
}

/// Run-time load of `<workspace>/connections/<id>.json`, decrypted. STRICT:
/// a missing file, or an `enc:v1:` field that cannot be decrypted (missing or
/// wrong workspace key), is an error - unlike the lenient editor-facing
/// [`decrypt_payload_json`].
pub fn load_connection(workspace: &Path, id: &str) -> Result<JsonValue, String> {
    let path = workspace.join("connections").join(format!("{}.json", id));
    let txt = std::fs::read_to_string(&path).map_err(|e| {
        format!(
            "connection '{}' not found under {} ({})",
            id,
            workspace.display(),
            e
        )
    })?;
    let mut v: JsonValue = serde_json::from_str(&txt)
        .map_err(|e| format!("connection '{}': invalid JSON: {}", id, e))?;
    if any_encrypted(&v) {
        let key = workspace_key(workspace, false).map_err(|_| {
            format!(
                "connection '{}' holds encrypted fields but {} is missing - \
                 copy the workspace key or re-enter the secrets",
                id,
                key_path(workspace).display()
            )
        })?;
        transform(&mut v, &key, id, false)?;
        if any_encrypted(&v) {
            return Err(format!(
                "connection '{}' could not be decrypted with the workspace key \
                 (wrong key?)",
                id
            ));
        }
    }
    Ok(v)
}

/// The payload holds a connection JSON object; read a non-empty string field.
fn conn_str<'a>(conn: &'a JsonValue, key: &str) -> Option<&'a str> {
    conn.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

/// Expand a `connectionRef` on a node's properties into the fields the engine
/// reads. Salesforce nodes (#166 stage 2) get their auth fields; every other
/// kind merges its credential/config fields (#185). No-op when no ref is set.
/// The connection WINS over node-level auth props - "ref set => the saved
/// connection defines auth" keeps rotation in one place and avoids half-states
/// mixing stale node fields with connection credentials.
pub fn resolve_connection_ref_props(
    workspace: &Path,
    component_id: &str,
    props: &mut JsonValue,
) -> Result<(), String> {
    // #256: a transport connection is resolved FIRST and independently. It has
    // to be its own key, because `connectionRef` on these nodes is already spent
    // on the REST auth connection, and it has to be resolved before the early
    // return below, or a node carrying a transport but no auth connection would
    // silently keep neither.
    resolve_transport_ref(workspace, props)?;
    let Some(ref_id) = props
        .get("connectionRef")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
    else {
        return Ok(());
    };
    // src.salesforce rides the generic REST source: its form keys the mode as
    // `authType`, the token as `authToken`, and its `url` is user-authored so no
    // instanceUrl is injected. Every other Salesforce node - the Collections sink
    // and both Bulk API 2.0 nodes - owns its own endpoint and uses the sink's
    // `authMode` / `instanceUrl` / `accessToken` keys.
    let is_rest_source = component_id == "src.salesforce";
    let is_salesforce = is_rest_source
        || matches!(
            component_id,
            "snk.salesforce" | "snk.salesforce.bulk" | "src.salesforce.bulk"
        );
    // Salesforce demands its connection (auth is the whole node); every other
    // kind falls back to the node's inline props if the connection can no longer
    // be loaded, so a since-removed connection never hard-fails a pipeline that
    // still carries usable credentials.
    let conn = match load_connection(workspace, &ref_id) {
        Ok(c) => c,
        Err(e) => return if is_salesforce { Err(e) } else { Ok(()) },
    };
    let kind = conn.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if !is_salesforce {
        // Generic connection (S3 / Postgres / GCS / Azure / ...): merge its
        // credential and config fields onto the node, exactly as the desktop
        // connection picker does, so headless / scheduled / web runs and
        // ref-only pipelines resolve credentials the same way (#185).
        return merge_generic_connection(component_id, kind, &conn, props);
    }
    if kind != "salesforce" {
        return Err(format!(
            "{}: connection '{}' is kind '{}', expected a Salesforce connection",
            component_id, ref_id, kind
        ));
    }
    // Same aliases the engine's salesforce_oauth_from_props accepts.
    let client_credentials = matches!(
        conn.get("authMode")
            .and_then(|v| v.as_str())
            .unwrap_or("bearer"),
        "clientCredentials" | "client_credentials" | "oauth" | "oauth_client_credentials"
    );
    let map = props
        .as_object_mut()
        .ok_or_else(|| format!("{}: node properties are not an object", component_id))?;
    // The sink and Bulk forms key the mode as `authMode`; the REST-shaped source
    // form keys it as `authType` (stage 1, 11af9fb).
    if !is_rest_source {
        map.insert(
            "authMode".into(),
            JsonValue::String(
                if client_credentials {
                    "clientCredentials"
                } else {
                    "bearer"
                }
                .into(),
            ),
        );
    } else {
        map.insert(
            "authType".into(),
            JsonValue::String(
                if client_credentials {
                    "oauth_client_credentials"
                } else {
                    "bearer"
                }
                .into(),
            ),
        );
    }
    for (conn_key, prop_key) in [
        ("loginUrl", "loginUrl"),
        ("clientId", "clientId"),
        ("clientSecret", "clientSecret"),
    ] {
        if let Some(v) = conn_str(&conn, conn_key) {
            map.insert(prop_key.into(), JsonValue::String(v.into()));
        }
    }
    // instanceUrl feeds the sink and Bulk endpoints, which build their own URLs;
    // the REST source's `url` is user-authored (full query URL), so it is never
    // injected there.
    if !is_rest_source {
        if let Some(v) = conn_str(&conn, "instanceUrl") {
            map.insert("instanceUrl".into(), JsonValue::String(v.into()));
        }
    }
    // Bearer-mode saved connection: the sink and Bulk nodes read `accessToken`,
    // the REST-shaped source reads `authToken` (push_rest_auth).
    if let Some(v) = conn_str(&conn, "accessToken") {
        map.insert(
            if is_rest_source { "authToken" } else { "accessToken" }.into(),
            JsonValue::String(v.into()),
        );
    }
    Ok(())
}

/// Merge a saved connection's credential/config fields onto a node's props for
/// any non-Salesforce component (S3, Postgres, GCS, Azure, Snowflake, ...). The
/// connection field names already match what each engine connector reads, so
/// this is the run-time equivalent of the desktop UI's "pick a connection"
/// action - and, unlike that, it also covers headless / scheduled / web runs
/// and pipelines that carry only a `connectionRef`. The connection wins over any
/// stale inline value so credential rotation lives in one place. Runs before the
/// `${ENV:}` pass, so a field stored as `${ENV:...}` still resolves afterwards.
/// Apply a saved REST connection to a node, auth-first.
///
/// The point of a REST connection is the thing two people asked for on the same
/// day: the vendor's auth in one place, so rotating a key is one edit rather
/// than one per node. That gives two rules, and neither is the generic
/// "whatever the connection says wins":
///
/// - **Headers merge per key.** The connection carries `X-Access-Key`; the node
///   still gets to add its own `Content-Type`. A key present on the node wins,
///   because the specific thing should be able to say something about itself.
/// - **`url` only fills a node that has none.** Copying it over would point
///   every node using the connection at the same endpoint, which is the
///   opposite of "one datasource, a different query per node".
///
/// Auth fields fill in the same way: only where the node left a blank.
/// #256: flatten a saved `http` transport connection onto a node.
///
/// Proxies, timeouts and a User-Agent are transport, not credentials, and every
/// HTTP-backed component wants the same four. Keeping them on a saved connection
/// means setting a corporate proxy once rather than on every node, and the node
/// still wins where it set a value itself - the same policy a saved REST
/// connection already uses for url and auth.
///
/// A connection that has since been deleted is not fatal: the node keeps
/// whatever it carries inline, exactly like every non-Salesforce kind.
fn resolve_transport_ref(workspace: &Path, props: &mut JsonValue) -> Result<(), String> {
    let Some(ref_id) = props
        .get("transportRef")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
    else {
        return Ok(());
    };
    let Ok(conn) = load_connection(workspace, &ref_id) else {
        return Ok(());
    };
    let kind = conn.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if kind != "http" {
        return Err(format!(
            "transportRef '{}' is kind '{}', expected an HTTP transport connection",
            ref_id, kind
        ));
    }
    let Some(map) = props.as_object_mut() else {
        return Ok(());
    };
    // Connection field -> the flat prop the engine reads.
    for (from, to) in [
        ("proxy", "httpProxy"),
        ("readTimeoutSecs", "httpReadTimeoutSecs"),
        ("connectTimeoutSecs", "httpConnectTimeoutSecs"),
        ("userAgent", "httpUserAgent"),
    ] {
        let filled = |v: Option<&JsonValue>| -> bool {
            !matches!(v, None | Some(JsonValue::Null)) && v.and_then(|x| x.as_str()) != Some("")
        };
        if filled(map.get(to)) {
            continue; // the node said something specific; leave it alone
        }
        if let Some(v) = conn.get(from) {
            if filled(Some(v)) {
                map.insert(to.to_string(), v.clone());
            }
        }
    }
    Ok(())
}

fn merge_rest_connection(conn: &JsonValue, map: &mut serde_json::Map<String, JsonValue>) {
    let filled = |v: Option<&JsonValue>| -> bool {
        !matches!(v, None | Some(JsonValue::Null)) && v.and_then(|x| x.as_str()) != Some("")
    };

    // Headers, merged. Both shapes the engine accepts are normalised to an
    // object here: headers_from_props reads either, and an object is the one
    // that can be merged by key.
    let as_pairs = |v: Option<&JsonValue>| -> Vec<(String, JsonValue)> {
        match v {
            Some(JsonValue::Object(o)) => o.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            Some(JsonValue::Array(a)) => a
                .iter()
                .filter_map(|it| {
                    let k = it.get("key")?.as_str()?.to_string();
                    let v = it.get("value").cloned().unwrap_or(JsonValue::Null);
                    Some((k, v))
                })
                .collect(),
            _ => Vec::new(),
        }
    };
    let from_conn = as_pairs(conn.get("headers"));
    if !from_conn.is_empty() {
        let mut merged = serde_json::Map::new();
        for (k, v) in from_conn {
            merged.insert(k, v);
        }
        // The node last, so it overrides the connection on a shared key.
        for (k, v) in as_pairs(map.get("headers")) {
            if !matches!(v, JsonValue::Null) && v.as_str() != Some("") {
                merged.insert(k, v);
            }
        }
        map.insert("headers".into(), JsonValue::Object(merged));
    }

    // Everything else fills a blank and never overwrites.
    for key in ["url", "authType", "authToken", "authHeader", "tokenUrl", "clientId", "clientSecret"]
    {
        let Some(v) = conn.get(key) else { continue };
        if v.is_null() || v.as_str() == Some("") {
            continue;
        }
        if filled(map.get(key)) {
            continue;
        }
        map.insert(key.to_string(), v.clone());
    }
}

fn merge_generic_connection(
    component_id: &str,
    kind: &str,
    conn: &JsonValue,
    props: &mut JsonValue,
) -> Result<(), String> {
    // The same fields the desktop connection picker copies (PropertiesPanel
    // onPickConnection). Node-specific props (path, format, object, ...) are
    // left untouched.
    const KEYS: &[&str] = &[
        "host",
        "port",
        "database",
        "username",
        "password",
        "bucket",
        "region",
        "accessKey",
        "secretKey",
        "sessionToken",
        "accountName",
        "accountKey",
        "brokers",
        "url",
        "endpoint",
        "urlStyle",
        "useSsl",
        "sslmode",
        "sslrootcert",
        "sslcert",
        "sslkey",
        "connectTimeout",
        "options",
        "connParams",
    ];
    let map = props
        .as_object_mut()
        .ok_or_else(|| format!("{}: node properties are not an object", component_id))?;

    // A REST connection exists so that auth lives in ONE place while each node
    // keeps its own request. The generic "connection value wins" rule is wrong
    // for both of its interesting fields, so it gets its own policy.
    if kind == "rest" {
        merge_rest_connection(conn, map);
        return Ok(());
    }

    for key in KEYS {
        let Some(v) = conn.get(*key) else {
            continue;
        };
        // Skip nulls and empty strings so a blank connection field never
        // clobbers a node default.
        if v.is_null() || v.as_str() == Some("") {
            continue;
        }
        if *key == "urlStyle" {
            // Normalize legacy free-text URL styles to DuckDB's canonical
            // 'path' / 'vhost' (matches the UI picker); leave the node default
            // for an unrecognized value.
            if let Some(s) = v.as_str() {
                let low = s.to_lowercase();
                let canon = if low.starts_with("path") {
                    "path"
                } else if low.starts_with("vhost") || low.contains("virtual") {
                    "vhost"
                } else {
                    continue;
                };
                map.insert("urlStyle".into(), JsonValue::String(canon.into()));
                continue;
            }
        }
        map.insert((*key).to_string(), v.clone());
    }
    // Snowflake keys the account identifier as `account`, but the connection
    // stores it in `host` (matches the UI picker).
    if kind == "snowflake" {
        if let Some(h) = conn_str(conn, "host") {
            map.insert("account".into(), JsonValue::String(h.into()));
        }
    }
    Ok(())
}

/// Resolve saved-connection references on every node in a pipeline document, in
/// place. Call BEFORE the `${ENV:...}` pass so a connection field stored as a
/// placeholder still expands afterwards.
pub fn resolve_connection_refs(workspace: &Path, nodes: &mut [PipelineNode]) -> Result<(), String> {
    for node in nodes.iter_mut() {
        let Some(component_id) = node.data.component_id.clone() else {
            continue;
        };
        if let Some(props) = node.data.properties.as_mut() {
            resolve_connection_ref_props(workspace, &component_id, props)?;
        }
    }
    Ok(())
}

/// Does any node in the document carry a non-empty `connectionRef`? Lets a host
/// that has no workspace path fail with a clear message instead of silently
/// running with unresolved credentials.
pub fn has_connection_refs(nodes: &[PipelineNode]) -> bool {
    nodes.iter().any(|node| {
        node.data
            .properties
            .as_ref()
            .and_then(|p| p.get("connectionRef"))
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_ws(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("duckle_sec_{}_{}", tag, std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }


    /// The property the binding exists for.
    ///
    /// Under one workspace key every ciphertext used to be interchangeable, so the
    /// same blob decrypted in ANY field of ANY connection. Anyone able to write a
    /// connection file could copy a production password into a connection pointing
    /// at a host they control and have the engine send it there. Binding the
    /// ciphertext to (connection, field) makes that fail to authenticate.
    #[test]
    fn a_secret_cannot_be_moved_between_fields_or_connections() {
        let key = [7u8; 32];
        let sealed = encrypt_value(&key, &aad_for("prod-db", "password"), "hunter2").unwrap();

        assert_eq!(
            decrypt_value(&key, &aad_for("prod-db", "password"), &sealed).unwrap(),
            "hunter2",
            "it must still open where it belongs"
        );

        let moved_connection = decrypt_value(&key, &aad_for("attacker-db", "password"), &sealed);
        assert!(
            moved_connection.is_err(),
            "a password was transplanted into another connection and decrypted"
        );

        let moved_field = decrypt_value(&key, &aad_for("prod-db", "accessKey"), &sealed);
        assert!(
            moved_field.is_err(),
            "a password was transplanted into another field and decrypted"
        );
    }

    /// Secrets written before the binding must keep opening, or upgrading Duckle
    /// would lock people out of their own connections.
    #[test]
    fn a_secret_encrypted_by_an_older_build_still_decrypts() {
        // The round-trip test below encrypts and decrypts with the SAME library,
        // so it pins the blob layout but cannot notice if the cipher's own output
        // ever changed - which is exactly what a crypto dependency bump risks,
        // and it would silently strand every secret already on disk.
        //
        // This blob is therefore a fixed known answer, produced OUTSIDE this
        // codebase (Python's cryptography / OpenSSL) for key = 32 bytes of 0x09,
        // nonce = 12 bytes of 0x03, plaintext "legacy", no AAD. If a future
        // aes-gcm release changes anything observable, this stops decrypting.
        let key = [9u8; 32];
        let blob = format!("{}{}", ENC_PREFIX, "AwMDAwMDAwMDAwMDvfGMEzbF51+cyZ2Z34DTOdLhyPAUOw==");
        assert_eq!(
            decrypt_value(&key, &aad_for("anything", "at-all"), &blob).unwrap(),
            "legacy",
            "a secret written by an older build no longer decrypts"
        );
    }

    #[test]
    fn a_v1_value_still_decrypts() {
        use aes_gcm::aead::Aead;
        let key = [9u8; 32];
        let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
        let nonce = [3u8; 12];
        let ct = cipher
            .encrypt(Nonce::from_slice(&nonce), "legacy".as_bytes())
            .unwrap();
        let mut payload = nonce.to_vec();
        payload.extend_from_slice(&ct);
        let blob = format!(
            "{}{}",
            ENC_PREFIX,
            base64::engine::general_purpose::STANDARD.encode(payload)
        );

        // The aad is ignored for a v1 blob, which is exactly the weakness v2 closes.
        assert_eq!(
            decrypt_value(&key, &aad_for("anything", "at-all"), &blob).unwrap(),
            "legacy"
        );
        assert!(is_encrypted(&blob), "v1 must still be recognised as encrypted");
    }

    fn write_connection(ws: &Path, id: &str, payload: &str) {
        let dir = ws.join("connections");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join(format!("{}.json", id)), payload).unwrap();
    }

    fn sf_node(component_id: &str, props: serde_json::Value) -> PipelineNode {
        serde_json::from_value(serde_json::json!({
            "id": "n1",
            "position": {"x": 0.0, "y": 0.0},
            "data": {
                "label": "sf",
                "componentId": component_id,
                "properties": props,
            }
        }))
        .unwrap()
    }

    #[test]
    fn round_trip_encrypts_only_sensitive_fields() {
        let ws = temp_ws("rt");

        let payload = r#"{"kind":"postgres","host":"db.local","username":"u","password":"s3cr3t","port":5432}"#;
        let enc = encrypt_payload_json(&ws, "test-conn", payload).unwrap();
        // Non-secret fields stay readable; the password becomes ciphertext.
        assert!(
            enc.contains("\"host\":\"db.local\""),
            "host should be plaintext: {}",
            enc
        );
        assert!(
            enc.contains("\"username\":\"u\""),
            "username should be plaintext: {}",
            enc
        );
        assert!(
            (enc.contains(ENC_PREFIX) || enc.contains(ENC_PREFIX_V2)),
            "password should be encrypted: {}",
            enc
        );
        assert!(!enc.contains("s3cr3t"), "plaintext secret leaked: {}", enc);

        let dec = decrypt_payload_json(&ws, "test-conn", &enc).unwrap();
        let v: JsonValue = serde_json::from_str(&dec).unwrap();
        assert_eq!(v["password"], "s3cr3t");
        assert_eq!(v["host"], "db.local");
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn env_placeholders_are_not_encrypted() {
        let ws = temp_ws("env");
        let payload = r#"{"password":"${ENV:PGPASSWORD}"}"#;
        let enc = encrypt_payload_json(&ws, "sf-prod", payload).unwrap();
        assert!(
            enc.contains("${ENV:PGPASSWORD}"),
            "placeholder must survive: {}",
            enc
        );
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn salesforce_secret_fields_are_encrypted() {
        let ws = temp_ws("sfenc");
        let payload = r#"{"kind":"salesforce","authMode":"clientCredentials","clientId":"cid","clientSecret":"csecret","accessToken":"atok"}"#;
        let enc = encrypt_payload_json(&ws, "sf-prod", payload).unwrap();
        assert!(!enc.contains("csecret"), "clientSecret leaked: {}", enc);
        assert!(!enc.contains("atok"), "accessToken leaked: {}", enc);
        assert!(
            enc.contains("\"clientId\":\"cid\""),
            "clientId is not a secret: {}",
            enc
        );
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn resolve_client_credentials_on_source() {
        let ws = temp_ws("cc_src");
        let enc = encrypt_payload_json(
            &ws, "sf-prod",
            r#"{"kind":"salesforce","authMode":"clientCredentials","loginUrl":"https://acme.my.salesforce.com","clientId":"cid","clientSecret":"csecret"}"#,
        )
        .unwrap();
        write_connection(&ws, "sf-prod", &enc);

        let mut node = sf_node(
            "src.salesforce",
            serde_json::json!({"connectionRef": "sf-prod", "authType": "bearer", "url": "https://x/services/data/v60.0/query"}),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        let props = node.data.properties.unwrap();
        // Connection wins over the stale node-level bearer mode.
        assert_eq!(props["authType"], "oauth_client_credentials");
        assert_eq!(props["loginUrl"], "https://acme.my.salesforce.com");
        assert_eq!(props["clientId"], "cid");
        assert_eq!(props["clientSecret"], "csecret");
        // The user-authored query URL is untouched.
        assert_eq!(props["url"], "https://x/services/data/v60.0/query");
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn resolve_bearer_on_sink_and_source() {
        let ws = temp_ws("bearer");
        let enc = encrypt_payload_json(
            &ws, "sf-b",
            r#"{"kind":"salesforce","authMode":"bearer","instanceUrl":"https://acme.my.salesforce.com","accessToken":"tok123"}"#,
        )
        .unwrap();
        write_connection(&ws, "sf-b", &enc);

        let mut sink = sf_node(
            "snk.salesforce",
            serde_json::json!({"connectionRef": "sf-b"}),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut sink)).unwrap();
        let props = sink.data.properties.unwrap();
        assert_eq!(props["authMode"], "bearer");
        assert_eq!(props["instanceUrl"], "https://acme.my.salesforce.com");
        assert_eq!(props["accessToken"], "tok123");

        let mut src = sf_node(
            "src.salesforce",
            serde_json::json!({"connectionRef": "sf-b"}),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut src)).unwrap();
        let props = src.data.properties.unwrap();
        assert_eq!(props["authType"], "bearer");
        // The REST-shaped source reads the token as authToken.
        assert_eq!(props["authToken"], "tok123");
        assert!(
            props.get("instanceUrl").is_none(),
            "source url is user-authored"
        );
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn resolve_bulk_nodes_use_the_sink_key_shape() {
        let ws = temp_ws("bulk");
        let enc = encrypt_payload_json(
            &ws, "sf-bulk",
            r#"{"kind":"salesforce","authMode":"clientCredentials","instanceUrl":"https://acme.my.salesforce.com","loginUrl":"https://acme.my.salesforce.com","clientId":"cid","clientSecret":"csecret"}"#,
        )
        .unwrap();
        write_connection(&ws, "sf-bulk", &enc);

        // Both Bulk nodes own their endpoint, so both take the sink's key shape
        // - unlike src.salesforce, which rides the REST form. Without the
        // component-id gate these would fall through to merge_generic_connection
        // and silently resolve with no authMode at all.
        for id in ["snk.salesforce.bulk", "src.salesforce.bulk"] {
            let mut node = sf_node(id, serde_json::json!({"connectionRef": "sf-bulk"}));
            resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
            let props = node.data.properties.unwrap();
            assert_eq!(props["authMode"], "clientCredentials", "{}", id);
            assert_eq!(props["clientId"], "cid", "{}", id);
            assert_eq!(props["clientSecret"], "csecret", "{}", id);
            assert_eq!(props["instanceUrl"], "https://acme.my.salesforce.com", "{}", id);
            assert!(
                props.get("authType").is_none(),
                "{}: authType is the REST source's key",
                id
            );
        }
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn bulk_node_rejects_a_non_salesforce_connection() {
        let ws = temp_ws("bulk_kind");
        let enc =
            encrypt_payload_json(&ws, "s3-conn", r#"{"kind":"s3","accessKey":"ak","secretKey":"sk"}"#).unwrap();
        write_connection(&ws, "s3-conn", &enc);
        let mut node = sf_node(
            "snk.salesforce.bulk",
            serde_json::json!({"connectionRef": "s3-conn"}),
        );
        let err = resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap_err();
        assert!(err.contains("expected a Salesforce connection"), "{}", err);
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn env_placeholder_in_connection_survives_resolution() {
        let ws = temp_ws("cc_env");
        let enc = encrypt_payload_json(
            &ws, "sf-env",
            r#"{"kind":"salesforce","authMode":"clientCredentials","loginUrl":"https://a.my.salesforce.com","clientId":"cid","clientSecret":"${ENV:SF_CLIENT_SECRET}"}"#,
        )
        .unwrap();
        write_connection(&ws, "sf-env", &enc);
        let mut node = sf_node(
            "snk.salesforce",
            serde_json::json!({"connectionRef": "sf-env"}),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        let props = node.data.properties.unwrap();
        // Injected verbatim; the host's ${ENV:} pass runs after resolution.
        assert_eq!(props["clientSecret"], "${ENV:SF_CLIENT_SECRET}");
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn missing_connection_errors_with_id() {
        let ws = temp_ws("missing");
        let mut node = sf_node(
            "snk.salesforce",
            serde_json::json!({"connectionRef": "nope"}),
        );
        let err = resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap_err();
        assert!(
            err.contains("'nope'"),
            "error should name the connection: {}",
            err
        );
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn missing_key_is_strict_at_run_time_but_lenient_in_editor() {
        let ws = temp_ws("strict");
        let enc = encrypt_payload_json(
            &ws, "sf-s",
            r#"{"kind":"salesforce","authMode":"clientCredentials","clientId":"cid","clientSecret":"csecret"}"#,
        )
        .unwrap();
        write_connection(&ws, "sf-s", &enc);
        // Simulate a workspace copied without .duckle/keys/.
        std::fs::remove_file(ws.join(".duckle").join("keys").join("secret.key")).unwrap();

        let err = load_connection(&ws, "sf-s").unwrap_err();
        assert!(
            err.contains("secret.key"),
            "run-time load must be strict: {}",
            err
        );
        // Editor load stays lenient: ciphertext passes through unchanged.
        let out = decrypt_payload_json(&ws, "sf-s", &enc).unwrap();
        assert!(
            (out.contains(ENC_PREFIX) || out.contains(ENC_PREFIX_V2)),
            "editor load stays lenient: {}",
            out
        );
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn refless_nodes_are_untouched() {
        let ws = temp_ws("noop");
        // No connectionRef -> nothing to resolve, both SF and non-SF.
        let mut pg = sf_node("src.postgres", serde_json::json!({"host": "inline"}));
        resolve_connection_refs(&ws, std::slice::from_mut(&mut pg)).unwrap();
        assert_eq!(pg.data.properties.unwrap(), serde_json::json!({"host": "inline"}));

        let mut sf = sf_node("snk.salesforce", serde_json::json!({"object": "Account"}));
        resolve_connection_refs(&ws, std::slice::from_mut(&mut sf)).unwrap();
        assert_eq!(sf.data.properties.unwrap(), serde_json::json!({"object": "Account"}));
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn missing_generic_connection_falls_back_to_inline_props() {
        // A non-SF node whose ref points at a since-removed connection keeps its
        // inline props rather than hard-failing the run (#185).
        let ws = temp_ws("miss");
        let mut node = sf_node(
            "src.s3",
            serde_json::json!({"connectionRef": "gone", "accessKey": "AKINLINE"}),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        assert_eq!(node.data.properties.unwrap()["accessKey"], "AKINLINE");
        let _ = std::fs::remove_dir_all(&ws);
    }

    /// The whole point of a REST connection: auth in one place, query per node.
    ///
    /// Two users asked for this on the same day - "let's say auth changes, then
    /// you update in one place". That only works if the connection supplies the
    /// headers WITHOUT taking over the request, so the rules are the opposite of
    /// the generic merge: headers merge per key, and the node's own url survives.
    #[test]
    fn a_rest_connection_supplies_auth_without_taking_over_the_request() {
        let ws = temp_ws("rest");
        write_connection(
            &ws,
            "crazyvendor",
            r#"{"kind":"rest","url":"https://vendor.example.com/sql",
                "headers":[{"key":"X-Client-Number","value":"42"},
                           {"key":"X-Access-Key","value":"${ENV:XP_KEY}"},
                           {"key":"Content-Type","value":"application/json"}],
                "authType":"bearer","authToken":"t0ken"}"#,
        );

        // A node that keeps its own query and overrides one header.
        let mut node = sf_node(
            "src.rest",
            serde_json::json!({
                "connectionRef": "crazyvendor",
                "url": "https://vendor.example.com/sql/v2",
                "body": "{\"queryString\":\"SELECT * FROM custom.Contact\"}",
                "headers": [{"key": "Content-Type", "value": "application/xml"}]
            }),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        let p = node.data.properties.unwrap();

        // The node's own request is untouched. Copying the connection's url
        // over it would point every node at one endpoint, which is exactly the
        // thing being asked for and exactly what must not happen.
        assert_eq!(p["url"], "https://vendor.example.com/sql/v2");
        assert!(p["body"].as_str().unwrap().contains("custom.Contact"));

        // Auth arrives from the connection, so rotating the key is one edit.
        let headers = p["headers"].as_object().expect("headers merged to an object");
        assert_eq!(headers["X-Client-Number"], "42");
        assert_eq!(headers["X-Access-Key"], "${ENV:XP_KEY}"); // env pass resolves later
        assert_eq!(p["authType"], "bearer");
        assert_eq!(p["authToken"], "t0ken");

        // ...and the node still wins on a header it set itself.
        assert_eq!(headers["Content-Type"], "application/xml");
        let _ = std::fs::remove_dir_all(&ws);
    }

    /// A node with no url of its own takes the connection's.
    #[test]
    fn a_rest_node_with_no_url_of_its_own_inherits_the_connection_one() {
        let ws = temp_ws("rest2");
        write_connection(
            &ws,
            "vendor",
            r#"{"kind":"rest","url":"https://vendor.example.com/sql","headers":[{"key":"X-Access-Key","value":"k"}]}"#,
        );
        let mut node = sf_node("src.rest", serde_json::json!({ "connectionRef": "vendor" }));
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        let p = node.data.properties.unwrap();
        assert_eq!(p["url"], "https://vendor.example.com/sql");
        assert_eq!(p["headers"].as_object().unwrap()["X-Access-Key"], "k");
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn s3_connection_ref_merges_credentials() {
        // #185: an S3 node carrying only a connectionRef gets its credentials
        // merged from the saved connection, urlStyle normalized, and node-only
        // props (path) preserved. A ${ENV:} value survives for the later pass.
        let ws = temp_ws("s3");
        write_connection(
            &ws,
            "minio",
            r#"{"kind":"s3","accessKey":"AKIA123","secretKey":"${ENV:MASSIVE_SECRET}","region":"eu-west-1","endpoint":"minio.local:9000","urlStyle":"Path (MinIO / B2)","bucket":"flatfiles"}"#,
        );
        let mut node = sf_node(
            "src.s3",
            serde_json::json!({"connectionRef": "minio", "path": "s3://flatfiles/a.csv"}),
        );
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        let p = node.data.properties.unwrap();
        assert_eq!(p["accessKey"], "AKIA123");
        assert_eq!(p["secretKey"], "${ENV:MASSIVE_SECRET}"); // resolved by the env pass later
        assert_eq!(p["region"], "eu-west-1");
        assert_eq!(p["endpoint"], "minio.local:9000");
        assert_eq!(p["urlStyle"], "path"); // legacy label normalized
        assert_eq!(p["path"], "s3://flatfiles/a.csv"); // node-only prop preserved
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn snowflake_connection_ref_maps_host_to_account() {
        let ws = temp_ws("snow");
        write_connection(
            &ws,
            "sf",
            r#"{"kind":"snowflake","host":"acme-xy12345","username":"u","password":"p"}"#,
        );
        let mut node = sf_node("src.snowflake", serde_json::json!({"connectionRef": "sf"}));
        resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap();
        let p = node.data.properties.unwrap();
        assert_eq!(p["account"], "acme-xy12345");
        assert_eq!(p["username"], "u");
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn wrong_kind_connection_errors() {
        let ws = temp_ws("kind");
        write_connection(&ws, "pg", r#"{"kind":"postgres","host":"db"}"#);
        let mut node = sf_node("snk.salesforce", serde_json::json!({"connectionRef": "pg"}));
        let err = resolve_connection_refs(&ws, std::slice::from_mut(&mut node)).unwrap_err();
        assert!(
            err.contains("kind 'postgres'"),
            "error should name the kind: {}",
            err
        );
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[test]
    fn has_connection_refs_detects_any_ref() {
        let sf = sf_node("snk.salesforce", serde_json::json!({"connectionRef": "x"}));
        let s3 = sf_node("src.s3", serde_json::json!({"connectionRef": "y"}));
        let bare = sf_node("snk.salesforce", serde_json::json!({"object": "Account"}));
        assert!(has_connection_refs(&[sf]));
        assert!(has_connection_refs(&[s3])); // #185: any kind of ref now counts
        assert!(!has_connection_refs(&[bare]));
    }
}
