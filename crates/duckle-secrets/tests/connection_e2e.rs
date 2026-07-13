//! End-to-end test of #166 stage 2: a pipeline whose Salesforce node holds
//! ONLY a `connectionRef` runs against a mock org - the saved connection is
//! encrypted at rest with the workspace key, resolved + decrypted by the
//! host-side pass, and the engine mints a fresh client-credentials token from
//! the injected fields. Exercises the exact chain the desktop app and the
//! headless runner share; the engine itself never touches the keystore.
//!
//! Skips (like the engine's own execution tests) unless `DUCKLE_DUCKDB_BIN`
//! points at a DuckDB CLI - CI's rust matrix sets it.

use std::io::Write;
use std::path::Path;

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use serde_json::{json, Value};

fn engine() -> Option<DuckdbEngine> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok()?;
    let p = std::path::PathBuf::from(bin);
    p.exists().then(|| DuckdbEngine::new(p))
}

fn write_file(dir: &Path, name: &str, content: &str) -> String {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.flush().unwrap();
    path.to_string_lossy().replace('\\', "/")
}

fn doc(nodes: Value, edges: Value) -> PipelineDoc {
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({
        "id": id,
        "position": { "x": 0, "y": 0 },
        "data": { "label": id, "componentId": component, "properties": props }
    })
}

fn main_edge(id: &str, source: &str, target: &str) -> Value {
    json!({ "id": id, "source": source, "target": target, "data": { "connectionType": "main" } })
}

/// Two-request mock org: the token mint, then the sObject Collections POST.
/// Same shape as the engine tests' `sf_mock_server_oauth`.
fn sf_mock_server(
    data_resp: &'static str,
) -> (
    u16,
    std::sync::mpsc::Receiver<Vec<u8>>,
    std::thread::JoinHandle<()>,
) {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind sf mock");
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            stream
                .set_read_timeout(Some(Duration::from_millis(250)))
                .ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let request_line = String::from_utf8_lossy(&buf)
                .lines()
                .next()
                .unwrap_or("")
                .to_string();
            let _ = tx.send(buf);
            let body = if request_line.contains("oauth2/token") {
                format!(
                    r#"{{"access_token":"minted-e2e","instance_url":"http://127.0.0.1:{}","token_type":"Bearer"}}"#,
                    port
                )
            } else {
                data_resp.to_string()
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    (port, rx, handle)
}

#[test]
fn connection_ref_resolves_decrypts_and_engine_mints() {
    use std::time::Duration;
    let Some(engine) = engine() else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a duckdb CLI to run");
        return;
    };
    let (port, rx, handle) =
        sf_mock_server(r#"[{"id":"001000000000001","success":true,"errors":[]}]"#);

    // A workspace holding an ENCRYPTED client-credentials connection.
    let ws = tempfile::tempdir().unwrap();
    let login = format!("http://127.0.0.1:{}", port);
    let enc = duckle_secrets::encrypt_payload_json(
        ws.path(),
        &json!({
            "kind": "salesforce",
            "authMode": "clientCredentials",
            "loginUrl": login,
            "clientId": "e2e-client-id",
            "clientSecret": "e2e-s3cret",
        })
        .to_string(),
    )
    .unwrap();
    assert!(
        enc.contains("enc:v1:") && !enc.contains("e2e-s3cret"),
        "clientSecret must be ciphertext at rest: {}",
        enc
    );
    std::fs::create_dir_all(ws.path().join("connections")).unwrap();
    std::fs::write(ws.path().join("connections").join("sf-e2e.json"), &enc).unwrap();

    // The node carries ONLY the ref - no credential, no URL.
    let csv = write_file(ws.path(), "in.csv", "Name\nAcme\n");
    let mut pipeline = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "f",
                "snk.salesforce",
                json!({
                    "connectionRef": "sf-e2e",
                    "apiVersion": "v60.0",
                    "object": "Account",
                    "operation": "insert"
                }),
            ),
        ]),
        json!([main_edge("e", "s", "f")]),
    );

    // Host-side resolution - the same call the desktop app, the scheduler and
    // the runner make before their env pass.
    duckle_secrets::resolve_connection_refs(ws.path(), &mut pipeline.nodes).unwrap();

    let r = engine.execute_pipeline_named(&pipeline, "connref-e2e");
    assert_eq!(r.status, "ok", "ref-only pipeline failed: {:?}", r.error);

    // First request: the client-credentials mint carrying the DECRYPTED secret.
    let tok_req = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("expected token request");
    let tok_raw = String::from_utf8_lossy(&tok_req);
    assert!(
        tok_raw
            .lines()
            .next()
            .unwrap_or("")
            .contains("/services/oauth2/token"),
        "first request should hit the token endpoint"
    );
    assert!(
        tok_raw.contains("client_id=e2e-client-id") && tok_raw.contains("client_secret=e2e-s3cret"),
        "mint should carry the decrypted connection credentials"
    );

    // Second request: the Collections POST authed with the minted token.
    let data_req = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("expected collections request");
    let _ = handle.join();
    let data_raw = String::from_utf8_lossy(&data_req);
    assert!(
        data_raw.contains("Authorization: Bearer minted-e2e"),
        "collections POST should use the minted token"
    );
    assert!(
        data_raw.contains(r#""Name":"Acme""#),
        "collections POST should carry the csv row"
    );
}
