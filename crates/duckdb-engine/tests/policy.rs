//! #285 - policy as a boundary rather than guidance.
//!
//! These live in their own test binary on purpose. The policy is configured
//! through `DUCKLE_POLICY_FILE`, which is process-wide, and Rust runs the tests
//! in one binary in PARALLEL threads - so setting it here would apply to every
//! other test that happened to be running at the time. Serialising with a mutex
//! only helps for tests that take the same mutex; a separate binary is a
//! separate process, which is the only version that actually isolates.
//!
//! Found the hard way: one leaked variable turned forty unrelated tests red.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use serde_json::{json, Value};
use std::path::PathBuf;

fn engine() -> Option<DuckdbEngine> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty())?;
    Some(DuckdbEngine::new(PathBuf::from(bin)))
}

macro_rules! engine_or_skip {
    () => {
        match engine() {
            Some(e) => e,
            None => {
                eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a duckdb CLI to run");
                return;
            }
        }
    };
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
    json!({
        "id": id,
        "source": source,
        "target": target,
        "data": { "connectionType": "main" }
    })
}

fn out_path(dir: &std::path::Path, name: &str) -> String {
    dir.join(name).to_string_lossy().replace('\\', "/")
}

/// One at a time inside this binary too.
///
/// A separate binary isolates these from every OTHER test, but Rust still runs
/// the tests within it in parallel, and both the policy path and the workspace
/// are process-wide. The guard below removes the variable on drop; this stops a
/// second test from setting its own while the first is still running.
static POLICY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serialised() -> std::sync::MutexGuard<'static, ()> {
    POLICY_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Sets `DUCKLE_POLICY_FILE` and removes it on drop, so a failing assertion
/// cannot leave it set for the next test in this binary.
struct PolicyEnv;

impl PolicyEnv {
    fn set(path: &std::path::Path) -> Self {
        std::env::set_var("DUCKLE_POLICY_FILE", path);
        PolicyEnv
    }
}

impl Drop for PolicyEnv {
    fn drop(&mut self) {
        std::env::remove_var("DUCKLE_POLICY_FILE");
    }
}

/// A workspace with a server policy pointing at it.
fn setup(policy_yaml: &str) -> (tempfile::TempDir, PolicyEnv) {
    let tmp = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", tmp.path());
    let policy = tmp.path().join("server-policy.yaml");
    std::fs::write(&policy, policy_yaml).unwrap();
    let guard = PolicyEnv::set(&policy);
    (tmp, guard)
}

/// The headline case: an agent writes a pipeline that would write outside the
/// approved area, and the run does not happen. Nothing reaches the disk.
#[test]
fn a_denied_write_never_reaches_the_disk() {
    let _serial = serialised();
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", tmp.path());
    let allowed_dir = out_path(tmp.path(), "dev");
    let policy = tmp.path().join("server-policy.yaml");
    std::fs::write(
        &policy,
        format!("mode: enforce\nfilesystem:\n  allowedPaths:\n    - {}\n", allowed_dir),
    )
    .unwrap();
    let _env = PolicyEnv::set(&policy);

    let forbidden = out_path(tmp.path(), "prod/out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "code.sql", json!({ "sql": "SELECT 1 AS id" })),
            node("k", "snk.csv", json!({ "path": forbidden, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "k")]),
    ));

    assert_eq!(r.status, "error", "a denied write must not run");
    let err = r.error.unwrap_or_default();
    assert!(err.contains("policy"), "the refusal says it is policy: {err}");
    assert!(err.contains("server-policy.yaml"), "and names the file that refused: {err}");
    // The point of enforcing at plan time: no side effect happened at all.
    assert!(
        !std::path::Path::new(&forbidden).exists(),
        "the denied file was written anyway"
    );
}

/// The same pipeline inside the allowance runs normally, or the policy is just
/// an outage with extra steps.
#[test]
fn an_allowed_write_still_runs() {
    let _serial = serialised();
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", tmp.path());
    let allowed_dir = out_path(tmp.path(), "dev");
    let policy = tmp.path().join("server-policy.yaml");
    std::fs::write(
        &policy,
        format!("mode: enforce\nfilesystem:\n  allowedPaths:\n    - {}\n", allowed_dir),
    )
    .unwrap();
    let _env = PolicyEnv::set(&policy);

    let ok = format!("{}/out.csv", allowed_dir);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "code.sql", json!({ "sql": "SELECT 1 AS id" })),
            node("k", "snk.csv", json!({ "path": ok, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "k")]),
    ));
    assert_eq!(r.status, "ok", "an allowed write must still run: {:?}", r.error);
    assert!(std::path::Path::new(&ok).exists());
}

/// The property the whole design turns on, through a real run: a workspace file
/// - which whatever writes the pipelines can also write - cannot switch the
/// boundary off or hand itself back a denied component.
#[test]
fn a_workspace_policy_cannot_widen_the_server_policy() {
    let _serial = serialised();
    let engine = engine_or_skip!();
    let (tmp, _env) = setup("mode: enforce\ncomponents:\n  deny:\n    - code.sql\n");

    // The agent writes itself permission, twice over: it removes the deny and
    // switches the mode to report.
    std::fs::create_dir_all(tmp.path().join(".duckle")).unwrap();
    std::fs::write(
        tmp.path().join(".duckle").join("policy.yaml"),
        "mode: report\ncomponents:\n  deny: []\n",
    )
    .unwrap();

    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "code.sql", json!({ "sql": "SELECT 1 AS id" })),
            node("k", "snk.csv", json!({ "path": out_path(tmp.path(), "x.csv") })),
        ]),
        json!([main_edge("e1", "s", "k")]),
    ));
    assert_eq!(r.status, "error", "a workspace file wrote itself permission and got it");
}

/// Adding a restriction the server did not have is the legitimate direction,
/// and it has to work or the second layer is pointless.
#[test]
fn a_workspace_policy_can_add_a_restriction() {
    let _serial = serialised();
    let engine = engine_or_skip!();
    let (tmp, _env) = setup("mode: enforce\n");
    std::fs::create_dir_all(tmp.path().join(".duckle")).unwrap();
    std::fs::write(
        tmp.path().join(".duckle").join("policy.yaml"),
        "components:\n  deny:\n    - code.sql\n",
    )
    .unwrap();

    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "code.sql", json!({ "sql": "SELECT 1 AS id" })),
            node("k", "snk.csv", json!({ "path": out_path(tmp.path(), "y.csv") })),
        ]),
        json!([main_edge("e1", "s", "k")]),
    ));
    assert_eq!(r.status, "error", "a workspace may narrow, and that has to bind");
}

/// A policy file that is named and cannot be read must refuse the run. Falling
/// back to "no policy" would mean a typo in the environment silently removes
/// the boundary, which is the worst failure mode this feature has.
#[test]
fn an_unreadable_policy_refuses_rather_than_defaulting_to_open() {
    let _serial = serialised();
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", tmp.path());
    let _env = PolicyEnv::set(&tmp.path().join("does-not-exist.yaml"));

    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "code.sql", json!({ "sql": "SELECT 1 AS id" })),
            node("k", "snk.csv", json!({ "path": out_path(tmp.path(), "z.csv") })),
        ]),
        json!([main_edge("e1", "s", "k")]),
    ));
    assert_eq!(r.status, "error");
    let err = r.error.unwrap_or_default();
    assert!(
        err.contains("could not be read") && err.contains("Refusing"),
        "a missing policy is a refusal, not an open door: {err}"
    );
}

/// With no policy configured at all, nothing changes - the feature has to be
/// opt-in or every existing workspace breaks on upgrade.
#[test]
fn no_policy_configured_changes_nothing() {
    let _serial = serialised();
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", tmp.path());
    std::env::remove_var("DUCKLE_POLICY_FILE");
    let out = out_path(tmp.path(), "free.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "code.sql", json!({ "sql": "SELECT 1 AS id" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "k")]),
    ));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(std::path::Path::new(&out).exists());
}

/// The control plane is not a way around the policy.
///
/// `state.allowMutation: false` refuses a PIPELINE that advances a watermark.
/// It is worth nothing if `duckle-runner backfill --clear`, the HTTP API, MCP
/// and the desktop panel can clear the same file, and all four arrive at these
/// three functions directly rather than through a compiled pipeline.
#[test]
fn the_control_plane_cannot_change_state_the_policy_withholds() {
    use duckle_duckdb_engine::watermark as wm;
    let _lock = serialised();
    let (tmp, _env) = setup("mode: enforce\nstate:\n  allowMutation: false\n");
    let ws = tmp.path();

    // Seed a watermark by hand, so clearing it has something to destroy. This
    // writes the file directly rather than through the gated API.
    let dir = ws.join("state").join("orders");
    std::fs::create_dir_all(&dir).unwrap();
    let mark = dir.join("inc.json");
    std::fs::write(&mark, r#"{"value":"2026-08-01","type":"DATE"}"#).unwrap();

    let set = wm::set_incremental(ws, "orders", "inc", "2020-01-01", Some("DATE"));
    assert!(set.is_err(), "backfill --set walked past state.allowMutation");
    let snap = wm::set_snapshot(ws, "orders", "cdc", 1);
    assert!(snap.is_err(), "backfill --set-snapshot walked past state.allowMutation");
    let cleared = wm::clear(ws, "orders", "inc");
    assert!(cleared.is_err(), "backfill --clear walked past state.allowMutation");

    // The refusal has to be real: the file is untouched, not just reported on.
    assert_eq!(
        std::fs::read_to_string(&mark).unwrap(),
        r#"{"value":"2026-08-01","type":"DATE"}"#,
        "the watermark was changed despite the refusal"
    );
    // And it says which policy said no, not just 'denied'.
    let msg = set.unwrap_err().to_string();
    assert!(msg.contains("server-policy.yaml"), "names the policy: {msg}");

    // Reading is NOT gated - an operator has to see what they cannot change.
    let entries = wm::list(ws, "orders");
    assert_eq!(entries.len(), 1, "listing state must still work: {entries:?}");
}

/// The same surfaces work normally when the policy permits it, so the guard
/// cannot be a blanket refusal that happens to pass the test above.
#[test]
fn the_control_plane_still_works_when_mutation_is_permitted() {
    use duckle_duckdb_engine::watermark as wm;
    let _lock = serialised();
    let (tmp, _env) = setup("mode: enforce\nstate:\n  allowMutation: true\n");
    let ws = tmp.path();

    wm::set_incremental(ws, "orders", "inc", "2026-01-01", Some("DATE"))
        .expect("an allowed set must go through");
    assert_eq!(wm::list(ws, "orders").len(), 1);
    wm::clear(ws, "orders", "inc").expect("an allowed clear must go through");
    assert!(wm::list(ws, "orders").is_empty());
}

/// A tiny HTTP server. `redirect_to` makes it answer 302 instead of 200, which
/// is how the redirect hop gets tested without the network.
fn stub_http(redirect_to: Option<String>) -> (u16, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = std::sync::Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            stream.set_read_timeout(Some(std::time::Duration::from_millis(400))).ok();
            // Drain the request before answering it, or the client sees a
            // connection reset instead of the status line.
            let mut buf = [0u8; 4096];
            loop {
                match stream.read(&mut buf) {
                    Ok(n) if n == buf.len() => continue,
                    _ => break,
                }
            }
            seen.fetch_add(1, Ordering::SeqCst);
            let resp = match &redirect_to {
                Some(to) => format!(
                    "HTTP/1.1 302 Found\r\nLocation: {to}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                ),
                None => "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi"
                    .to_string(),
            };
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });
    (port, hits)
}

/// The compile-time domain check reads a URL out of a node's properties. The
/// request is made somewhere else entirely, so the boundary has to exist there
/// too - this is the run-time half.
#[test]
fn a_request_to_a_host_outside_the_allowlist_is_never_sent() {
    let _lock = serialised();
    let (_tmp, _env) = setup("mode: enforce\nnetwork:\n  allowedDomains:\n    - example.com\n");
    let (port, hits) = stub_http(None);

    let err = duckle_duckdb_engine::tls::http_agent()
        .get(&format!("http://127.0.0.1:{port}/data"))
        .call()
        .expect_err("a host outside the allowlist must not be reachable");

    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the request reached the server, so nothing was actually enforced"
    );
    let msg = err.to_string();
    assert!(msg.contains("127.0.0.1"), "names the host it refused: {msg}");
}

/// The case the compile-time check structurally cannot see: the configured URL
/// is allowed, and the server answers 302 to somewhere that is not.
#[test]
fn a_redirect_out_of_the_allowlist_is_refused_at_the_hop() {
    let _lock = serialised();
    // `localhost` and `127.0.0.1` are the same machine and DIFFERENT host
    // strings, which is exactly the shape of a redirect that leaves the
    // allowed domain.
    let (elsewhere, offsite) = stub_http(None);
    let (port, first) = stub_http(Some(format!("http://127.0.0.1:{elsewhere}/next")));
    let (_tmp, _env) = setup("mode: enforce\nnetwork:\n  allowedDomains:\n    - localhost\n");

    let err = duckle_duckdb_engine::tls::http_agent()
        .get(&format!("http://localhost:{port}/start"))
        .call()
        .expect_err("the redirect target is outside the allowlist");

    assert_eq!(
        first.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the allowed first hop should still have happened"
    );
    assert_eq!(
        offsite.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the redirect was followed off the allowlist"
    );
    let msg = err.to_string();
    assert!(msg.contains("127.0.0.1"), "names the host it refused: {msg}");
}

/// The gate is a boundary, not a blanket refusal: an allowed host still works.
#[test]
fn an_allowed_host_is_still_reachable() {
    let _lock = serialised();
    let (port, hits) = stub_http(None);
    let (_tmp, _env) = setup("mode: enforce\nnetwork:\n  allowedDomains:\n    - localhost\n");

    let resp = duckle_duckdb_engine::tls::http_agent()
        .get(&format!("http://localhost:{port}/ok"))
        .call()
        .expect("an allowed host must still be reachable");
    assert_eq!(resp.status(), 200);
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
}
