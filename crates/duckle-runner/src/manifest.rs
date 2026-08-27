//! Signed reproducible run manifest (".ducklock"). After a successful run the
//! runner can emit a small JSON manifest pinning the pipeline and compiled-plan
//! hashes plus the engine versions, signed with a per-workspace Ed25519 key, so
//! an auditor can verify offline that a given run came from a given pipeline.
//!
//! The signing key is generated on first use under `<workspace>/.duckle/keys/`
//! (the same place the secret-encryption key lives). The manifest embeds its own
//! public key so verification needs nothing but the file; compare the embedded
//! key with `<workspace>/.duckle/keys/manifest.pub` to also establish trust.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use duckle_duckdb_engine::lineage::RootColumn;
use duckle_duckdb_engine::{compile_pipeline_sql, PipelineDoc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Per-node column lineage as the engine resolves it: node id -> list of
/// (output column, its root source columns).
type Lineage = HashMap<String, Vec<(String, Vec<RootColumn>)>>;

const SCHEMA_VERSION: u32 = 1;
const DUCKDB_VERSION: &str = "1.5.4";

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// A source input file pinned in the manifest: its path, size, and content hash
/// (omitted when the file is too large to hash). Lets an auditor confirm the run
/// read the exact same input bytes.
pub struct InputFingerprint {
    pub node: String,
    pub path: String,
    pub bytes: u64,
    pub sha256: Option<String>,
}

/// Load the workspace Ed25519 signing key, generating one on first use.
fn signing_key(workspace: &Path) -> Result<SigningKey, String> {
    let dir = workspace.join(".duckle").join("keys");
    let key_path = dir.join("manifest.key");
    if key_path.exists() {
        let raw = std::fs::read(&key_path).map_err(|e| format!("read signing key: {e}"))?;
        let seed: [u8; 32] = raw
            .try_into()
            .map_err(|_| "signing key file is not 32 bytes".to_string())?;
        return Ok(SigningKey::from_bytes(&seed));
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("create keys dir: {e}"))?;
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).map_err(|e| format!("generate key: {e}"))?;
    std::fs::write(&key_path, seed).map_err(|e| format!("write signing key: {e}"))?;
    let sk = SigningKey::from_bytes(&seed);
    let _ = std::fs::write(dir.join("manifest.pub"), B64.encode(sk.verifying_key().to_bytes()));
    Ok(sk)
}

/// The exact bytes we sign and verify: the manifest body as JSON. The same
/// in-memory body is embedded in the file, so a parse + re-serialize round-trip
/// reproduces these bytes regardless of serde_json key-ordering.
fn body_bytes(body: &Value) -> Vec<u8> {
    serde_json::to_vec(body).unwrap_or_default()
}

/// Reshape the engine's lineage map into a readable, deterministic node ->
/// [{ column, roots }] object for the manifest body (serde_json::Map sorts its
/// keys, so the signed bytes are stable across runs of the same pipeline).
fn lineage_to_json(lineage: Lineage) -> Value {
    let mut out = serde_json::Map::new();
    for (node_id, cols) in lineage {
        let arr: Vec<Value> = cols
            .into_iter()
            .map(|(column, roots)| {
                json!({
                    "column": column,
                    "roots": serde_json::to_value(&roots).unwrap_or_else(|_| json!([])),
                })
            })
            .collect();
        out.insert(node_id, Value::Array(arr));
    }
    Value::Object(out)
}

/// A node's run outcome to record in the manifest: the rows it produced and
/// whether it succeeded. Built from the engine's per-node run status.
pub struct NodeOutcome {
    pub node: String,
    pub status: String,
    pub rows: Option<u64>,
}

/// Build, sign and write a manifest for a completed run. Returns the file path.
/// `lineage` is the engine's resolved column lineage when available; it is
/// embedded so the signed artifact records where each output column came from.
/// `outputs` records each node's row count and status, so the signed artifact
/// attests to what the run produced, not just the pipeline definition.
pub fn write_manifest(
    workspace: &Path,
    name: &str,
    doc: &PipelineDoc,
    status: &str,
    duration_ms: u64,
    stamp_ms: u128,
    lineage: Option<Lineage>,
    outputs: &[NodeOutcome],
    inputs: &[InputFingerprint],
    artifacts: &[duckle_duckdb_engine::ArtifactRef],
    artifacts_truncated: bool,
) -> Result<PathBuf, String> {
    let pipeline_hash = sha256_hex(&serde_json::to_vec(doc).unwrap_or_default());
    let compiled_hash = match compile_pipeline_sql(doc) {
        Ok(stages) => sha256_hex(&serde_json::to_vec(&stages).unwrap_or_default()),
        Err(e) => return Err(format!("compile for manifest: {e}")),
    };
    let lineage_json = lineage.map(lineage_to_json);
    let outputs_json: Vec<Value> = outputs
        .iter()
        .map(|o| {
            let mut m = json!({ "node": o.node, "status": o.status });
            if let Some(r) = o.rows {
                m["rows"] = json!(r);
            }
            m
        })
        .collect();
    let inputs_json: Vec<Value> = inputs
        .iter()
        .map(|i| {
            let mut m = json!({ "node": i.node, "path": i.path, "bytes": i.bytes });
            if let Some(h) = &i.sha256 {
                m["sha256"] = json!(h);
            }
            m
        })
        .collect();
    // #247: remote objects the run read or wrote. A local file input is pinned
    // by path above; an s3:// or https:// object has no path, so without this
    // the boundary that matters most in a raw-zone pipeline - where the bytes
    // came from - was the one thing the signed manifest did not record.
    let artifacts_json: Vec<Value> = artifacts
        .iter()
        .map(|a| serde_json::to_value(a).unwrap_or(Value::Null))
        .collect();
    // #278: the limits the run actually ran under, so two runs that spilled
    // differently can be told apart from two runs given different budgets.
    let limits = duckle_duckdb_engine::effective_resource_limits();
    let mut body = json!({
        "schemaVersion": SCHEMA_VERSION,
        "pipeline": name,
        "atEpochMs": stamp_ms.to_string(),
        "status": status,
        "durationMs": duration_ms,
        "nodeCount": doc.nodes.len(),
        "inputs": inputs_json,
        "outputs": outputs_json,
        "pipelineHash": pipeline_hash,
        "compiledPlanHash": compiled_hash,
        "duckleVersion": env!("CARGO_PKG_VERSION"),
        "duckdbVersion": DUCKDB_VERSION,
        "lineageResolved": lineage_json.is_some(),
        "artifacts": artifacts_json,
        "artifactsTruncated": artifacts_truncated,
        "resourceLimits": limits,
    });
    if let (Some(l), Some(obj)) = (lineage_json, body.as_object_mut()) {
        obj.insert("lineage".to_string(), l);
    }

    let sk = signing_key(workspace)?;
    let signature = sk.sign(&body_bytes(&body));
    let manifest = json!({
        "alg": "ed25519",
        "publicKey": B64.encode(sk.verifying_key().to_bytes()),
        "signature": B64.encode(signature.to_bytes()),
        "body": body,
    });

    let dir = workspace.join("manifests");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create manifests dir: {e}"))?;
    let safe: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let path = dir.join(format!("{safe}-{stamp_ms}.ducklock"));
    std::fs::write(&path, serde_json::to_vec_pretty(&manifest).unwrap_or_default())
        .map_err(|e| format!("write manifest: {e}"))?;
    Ok(path)
}

/// Verify a manifest's signature over its embedded body. Ok(true) if intact.
pub fn verify_manifest(path: &Path) -> Result<bool, String> {
    let raw = std::fs::read(path).map_err(|e| format!("read manifest: {e}"))?;
    let m: Value = serde_json::from_slice(&raw).map_err(|e| format!("parse manifest: {e}"))?;
    let body = m.get("body").ok_or("manifest has no body")?;
    let pk_b64 = m
        .get("publicKey")
        .and_then(|v| v.as_str())
        .ok_or("manifest has no publicKey")?;
    let sig_b64 = m
        .get("signature")
        .and_then(|v| v.as_str())
        .ok_or("manifest has no signature")?;
    let pk_bytes: [u8; 32] = B64
        .decode(pk_b64)
        .map_err(|e| format!("publicKey base64: {e}"))?
        .try_into()
        .map_err(|_| "publicKey is not 32 bytes".to_string())?;
    let sig_bytes: [u8; 64] = B64
        .decode(sig_b64)
        .map_err(|e| format!("signature base64: {e}"))?
        .try_into()
        .map_err(|_| "signature is not 64 bytes".to_string())?;
    let vk = VerifyingKey::from_bytes(&pk_bytes).map_err(|e| format!("publicKey invalid: {e}"))?;
    let signature = Signature::from_bytes(&sig_bytes);
    Ok(vk.verify(&body_bytes(body), &signature).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_then_verify_roundtrips_and_detects_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        let doc: PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"A","componentId":"src.csv","properties":{"path":"a.csv"}}}],"edges":[]}"#,
        )
        .unwrap();
        let outputs = vec![NodeOutcome { node: "s".into(), status: "ok".into(), rows: Some(3) }];
        let inputs = vec![InputFingerprint {
            node: "s".into(),
            path: "a.csv".into(),
            bytes: 12,
            sha256: Some("abc123".into()),
        }];
        let path = write_manifest(ws, "demo", &doc, "ok", 12, 1_700_000_000_000, None, &outputs, &inputs, &[], false)
            .unwrap();
        assert!(verify_manifest(&path).unwrap(), "fresh manifest should verify");

        // The run outcome and inputs are recorded.
        let m: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(m["body"]["outputs"][0]["node"], json!("s"));
        assert_eq!(m["body"]["outputs"][0]["rows"], json!(3));
        assert_eq!(m["body"]["inputs"][0]["sha256"], json!("abc123"));
        assert_eq!(m["body"]["inputs"][0]["bytes"], json!(12));

        // Tamper with a recorded row count: verification must fail.
        let mut m: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        m["body"]["outputs"][0]["rows"] = json!(99999);
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(!verify_manifest(&path).unwrap(), "tampered manifest must fail");
    }

    #[test]
    fn embedded_lineage_is_recorded_and_signed() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        let doc: PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"A","componentId":"src.csv","properties":{"path":"a.csv"}}}],"edges":[]}"#,
        )
        .unwrap();
        let mut lineage: Lineage = HashMap::new();
        lineage.insert(
            "s".to_string(),
            vec![(
                "email".to_string(),
                vec![RootColumn { node: "s".to_string(), column: "email".to_string() }],
            )],
        );
        let path = write_manifest(ws, "demo", &doc, "ok", 5, 1_700_000_000_001, Some(lineage), &[], &[], &[], false)
            .unwrap();
        assert!(verify_manifest(&path).unwrap(), "manifest with lineage should verify");

        let m: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(m["body"]["lineageResolved"], json!(true));
        assert_eq!(m["body"]["lineage"]["s"][0]["column"], json!("email"));
        assert_eq!(m["body"]["lineage"]["s"][0]["roots"][0]["node"], json!("s"));
    }
    /// #247: a remote object has no path, so the manifest recorded nothing at
    /// all about the boundary that matters most in a raw-zone pipeline - which
    /// bytes were pulled in, and where they landed.
    #[test]
    fn remote_artifacts_are_recorded_and_signed() {
        use duckle_duckdb_engine::ArtifactRef;
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        let doc: PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"c","position":{"x":0,"y":0},"data":{"label":"A","componentId":"src.changed","properties":{"uri":"s3://raw/feed.zip"}}}],"edges":[]}"#,
        )
        .unwrap();
        let artifacts = vec![
            // Observed but not read: an ETag and a size, and honestly no hash.
            ArtifactRef {
                node: "c".into(),
                role: "input".into(),
                uri: "s3://raw/feed.zip".into(),
                name: Some("feed.zip".into()),
                media_type: None,
                size_bytes: Some(4096),
                sha256: None,
                etag: Some("abc123".into()),
                modified_at: Some("2026-01-01T00:00:00Z".into()),
            },
            // Copied, so the bytes went through this run and the hash is real.
            ArtifactRef {
                node: "cp".into(),
                role: "output".into(),
                uri: "s3://lake/raw/feed.zip".into(),
                name: Some("feed.zip".into()),
                media_type: Some("application/zip".into()),
                size_bytes: Some(4096),
                sha256: Some("deadbeef".into()),
                etag: None,
                modified_at: None,
            },
        ];
        let path = write_manifest(
            ws, "demo", &doc, "ok", 5, 1_700_000_000_002, None, &[], &[], &artifacts, true,
        )
        .unwrap();
        assert!(verify_manifest(&path).unwrap(), "a manifest with artifacts must verify");

        let m: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let a = &m["body"]["artifacts"];
        assert_eq!(a[0]["uri"], json!("s3://raw/feed.zip"));
        assert_eq!(a[0]["etag"], json!("abc123"));
        assert!(a[0].get("sha256").is_none(), "an object that was only OBSERVED has no hash");
        assert_eq!(a[1]["role"], json!("output"));
        assert_eq!(a[1]["sha256"], json!("deadbeef"));
        assert_eq!(a[1]["mediaType"], json!("application/zip"));
        assert_eq!(
            m["body"]["artifactsTruncated"],
            json!(true),
            "a partial list must never read as a complete one"
        );
    }

    /// #278: the budget a run was given is part of what makes it reproducible.
    /// Only what was SET is recorded - inventing a value for an unset limit
    /// would claim a setting nobody chose.
    #[test]
    fn the_resource_limits_a_run_used_are_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        let doc: PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"A","componentId":"src.csv","properties":{"path":"a.csv"}}}],"edges":[]}"#,
        )
        .unwrap();
        std::env::set_var("DUCKLE_MEMORY_LIMIT", "4GB");
        std::env::set_var("DUCKLE_THREADS", "2");
        std::env::remove_var("DUCKLE_TEMP_DIR");
        let path =
            write_manifest(ws, "demo", &doc, "ok", 5, 1_700_000_000_003, None, &[], &[], &[], false)
                .unwrap();
        std::env::remove_var("DUCKLE_MEMORY_LIMIT");
        std::env::remove_var("DUCKLE_THREADS");

        let m: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(m["body"]["resourceLimits"]["memoryLimit"], json!("4GB"));
        assert_eq!(m["body"]["resourceLimits"]["threads"], json!("2"));
        assert!(
            m["body"]["resourceLimits"].get("tempDirectory").is_none(),
            "an unset limit is DuckDB's default, not a value we chose"
        );
        assert!(verify_manifest(&path).unwrap());
    }
}