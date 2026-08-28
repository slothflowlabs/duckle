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
