//! #312: `duckle-runner policy check`, run as the real binary.

use std::path::{Path, PathBuf};

fn node(id: &str, label: &str, component: &str, props: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "id": id, "type": "transform", "position": { "x": 0, "y": 0 },
        "data": { "label": label, "componentId": component, "properties": props }
    })
}

fn edge(a: &str, b: &str) -> serde_json::Value {
    serde_json::json!({ "id": format!("{a}_{b}"), "source": a, "target": b })
}

fn pipeline(ws: &Path, id: &str, nodes: serde_json::Value, edges: serde_json::Value) -> PathBuf {
    let path = ws.join("pipelines").join(format!("{id}.json"));
    std::fs::write(&path, serde_json::json!({ "nodes": nodes, "edges": edges }).to_string()).unwrap();
    path
}

/// A workspace whose policy denies code.shell and allows one domain, with a
/// clean pipeline, one breaking the policy twice, and one calling a sub-flow
/// that breaks it.
fn workspace(ws: &Path) {
    std::fs::create_dir_all(ws.join(".duckle")).unwrap();
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::write(
        ws.join(".duckle").join("policy.yaml"),
        "components:\n  deny: [code.shell]\nnetwork:\n  allowedDomains: [api.example.com]\n",
    )
    .unwrap();
    pipeline(
        ws,
        "p_ok",
        serde_json::json!([
            node("s", "Orders", "src.csv", serde_json::json!({ "path": "rows.csv", "hasHeader": true })),
            node("k", "Out", "snk.csv", serde_json::json!({ "path": "out.csv", "hasHeader": true })),
        ]),
        serde_json::json!([edge("s", "k")]),
    );
    pipeline(
        ws,
        "p_shell",
        serde_json::json!([
            node("api", "Feed", "src.rest", serde_json::json!({ "url": "https://evil.example.org/feed" })),
            node("sh", "Cleanup", "code.shell", serde_json::json!({ "command": "echo hi" })),
        ]),
        serde_json::json!([edge("api", "sh")]),
    );
    pipeline(
        ws,
        "p_sub",
        serde_json::json!([
            node("in", "In", "ctl.subflow.in", serde_json::json!({})),
            node("run", "Run", "code.shell", serde_json::json!({ "command": "echo hi" })),
            node("out", "Out", "ctl.subflow.out", serde_json::json!({})),
        ]),
        serde_json::json!([edge("in", "run"), edge("run", "out")]),
    );
    pipeline(
        ws,
        "p_caller",
        serde_json::json!([
            node("s", "Orders", "src.csv", serde_json::json!({ "path": "rows.csv", "hasHeader": true })),
            node("call", "Sub-flow", "ctl.subflow", serde_json::json!({ "subflowRef": "p_sub" })),
            node("k", "Out", "snk.csv", serde_json::json!({ "path": "out.csv", "hasHeader": true })),
        ]),
        serde_json::json!([edge("s", "call"), edge("call", "k")]),
    );
}

/// (exit code, stdout, stderr)
fn policy(ws: &Path, args: &[&str]) -> (i32, String, String) {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["policy", "check", "--workspace"])
        .arg(ws)
        .args(args)
        .current_dir(ws)
        .env_remove("DUCKLE_POLICY_FILE")
        .env_remove("DUCKLE_WORKSPACE")
        .output()
        .expect("the runner starts");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn every_violation_is_named_and_fails_the_check() {
    let tmp = tempfile::tempdir().unwrap();
    workspace(tmp.path());
    let (code, out, err) = policy(tmp.path(), &["pipelines/p_ok.json", "pipelines/p_shell.json"]);
    assert_eq!(code, 1, "a violation fails the check:\n{out}\n{err}");
    assert!(out.contains("code.shell") && out.contains("evil.example.org"), "both violations named:\n{out}");
    assert!(out.contains("p_ok.json"), "the permitted pipeline is listed too:\n{out}");
}

#[test]
fn a_subflow_step_is_checked_as_a_run_checks_it() {
    // A run expands a Sub-flow node before the policy sees the pipeline, so a
    // step inside a sub-flow is as forbidden as one written in place.
    let tmp = tempfile::tempdir().unwrap();
    workspace(tmp.path());
    let (code, out, err) = policy(tmp.path(), &["--format", "json", "pipelines/p_caller.json"]);
    assert_eq!(code, 1, "{out}\n{err}");
    let report: serde_json::Value = serde_json::from_str(&out).expect("json");
    let nodes: Vec<&str> = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|f| f["ok"] == false)
        .filter_map(|f| f["node"].as_str())
        .collect();
    assert_eq!(nodes, vec!["call__run"], "{report}");
}

#[test]
fn the_check_writes_the_ci_formats() {
    let tmp = tempfile::tempdir().unwrap();
    workspace(tmp.path());
    let shell = ["pipelines/p_ok.json", "pipelines/p_shell.json"];

    let (code, out, _) = policy(tmp.path(), &[&["--format", "json"][..], &shell[..]].concat());
    assert_eq!(code, 1);
    let json: serde_json::Value = serde_json::from_str(&out).expect("json");
    assert_eq!(json["schemaVersion"], 1, "{json}");
    assert_eq!(json["command"], "policy", "{json}");
    assert_eq!((json["checked"].as_u64(), json["failed"].as_u64()), (Some(3), Some(2)), "{json}");

    let (code, out, _) = policy(tmp.path(), &[&["--format", "sarif"][..], &shell[..]].concat());
    assert_eq!(code, 1);
    let sarif: serde_json::Value = serde_json::from_str(&out).expect("sarif");
    assert_eq!(sarif["runs"][0]["results"].as_array().map(Vec::len), Some(2), "{sarif}");

    let (code, out, _) = policy(tmp.path(), &[&["--format", "junit"][..], &shell[..]].concat());
    assert_eq!(code, 1);
    assert!(out.contains("<testsuite") && out.contains("failures=\"2\""), "{out}");
}

#[test]
fn with_no_policy_the_check_cannot_pass() {
    // A gate that passes because there was nothing to check is how a broken
    // gate goes unnoticed.
    let tmp = tempfile::tempdir().unwrap();
    workspace(tmp.path());
    std::fs::remove_file(tmp.path().join(".duckle").join("policy.yaml")).unwrap();
    let (code, out, err) = policy(tmp.path(), &["pipelines/p_shell.json"]);
    assert_eq!(code, 2, "no policy is not a pass:\n{out}\n{err}");
    assert!(err.contains("no policy"), "say why: {err}");
}

#[test]
fn with_no_paths_every_pipeline_in_the_workspace_is_checked() {
    let tmp = tempfile::tempdir().unwrap();
    workspace(tmp.path());
    let (code, out, _) = policy(tmp.path(), &["--format", "json"]);
    assert_eq!(code, 1);
    let json: serde_json::Value = serde_json::from_str(&out).expect("json");
    let mut files: Vec<String> = json["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["file"].as_str())
        .map(|f| Path::new(f).file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    files.sort();
    files.dedup();
    assert_eq!(files, ["p_caller.json", "p_ok.json", "p_shell.json", "p_sub.json"], "{json}");
}

#[test]
fn a_host_a_saved_connection_supplies_is_checked() {
    // The node names only its connection; the host is in the connection file,
    // where a run finds it too, so the domain rule reaches it.
    let tmp = tempfile::tempdir().unwrap();
    workspace(tmp.path());
    std::fs::create_dir_all(tmp.path().join("connections")).unwrap();
    std::fs::write(
        tmp.path().join("connections").join("warehouse.json"),
        r#"{"kind":"postgres","host":"db.evil.example.org","port":5432,"database":"d","username":"u","password":"p"}"#,
    )
    .unwrap();
    pipeline(
        tmp.path(),
        "p_conn",
        serde_json::json!([
            node("s", "Orders", "src.csv", serde_json::json!({ "path": "rows.csv", "hasHeader": true })),
            node("pg", "Warehouse", "snk.postgres", serde_json::json!({ "connectionRef": "warehouse", "table": "t" })),
        ]),
        serde_json::json!([edge("s", "pg")]),
    );
    let (code, out, err) = policy(tmp.path(), &["pipelines/p_conn.json"]);
    assert_eq!(code, 1, "{out}
{err}");
    assert!(out.contains("db.evil.example.org"), "the connection's host is checked:
{out}");
}
