//! `duckle-runner test --affected`, run as the real binary against a real git
//! workspace (#308). `affected` reports and `validate --affected` gates already
//! shared the selection; this is the third consumer of it - the suites it runs
//! must be exactly the pipelines the change reaches.

use std::path::Path;
use std::process::Command;

fn git(ws: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(ws)
        .args(args)
        .output()
        .expect("git runs");
    assert!(out.status.success(), "git {:?}: {}", args, String::from_utf8_lossy(&out.stderr));
}

fn write_pipeline(dir: &Path, name: &str) {
    std::fs::write(
        dir.join(format!("pipelines/{name}.pipeline.json")),
        format!(
            r#"{{"formatVersion":1,"name":"{n}","nodes":[
  {{"id":"s","type":"source","position":{{"x":0,"y":0}},"data":{{"label":"in","componentId":"src.csv","properties":{{"path":"in.csv","hasHeader":true}}}}}},
  {{"id":"flt","type":"transform","position":{{"x":200,"y":0}},"data":{{"label":"f","componentId":"xf.filter","properties":{{"predicate":"id > 0"}}}}}},
  {{"id":"k","type":"sink","position":{{"x":400,"y":0}},"data":{{"label":"out","componentId":"snk.csv","properties":{{"path":"{n}.csv","writeHeader":true}}}}}}
],"edges":[
  {{"id":"e1","source":"s","target":"flt","sourceHandle":"main","targetHandle":"main","data":{{"connectionType":"main"}}}},
  {{"id":"e2","source":"flt","target":"k","sourceHandle":"main","targetHandle":"main","data":{{"connectionType":"main"}}}}
]}}"#,
            n = name
        ),
    )
    .unwrap();
}

fn write_test(dir: &Path, name: &str) {
    std::fs::write(
        dir.join(format!("tests/{name}.test.json")),
        format!(
            r#"{{"pipeline":"../pipelines/{n}.pipeline.json","cases":[{{"name":"passes","given":{{"s":"id,v\n1,a\n2,b\n"}},"expect":{{"node":"flt","rowCount":2}}}}]}}"#,
            n = name
        ),
    )
    .unwrap();
}

#[test]
fn test_affected_runs_only_the_suites_whose_pipeline_changed() {
    let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
        return;
    };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: no git");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::create_dir_all(ws.join("tests")).unwrap();
    write_pipeline(ws, "a");
    write_pipeline(ws, "b");
    write_test(ws, "a");
    write_test(ws, "b");
    git(ws, &["init", "-q"]);
    git(ws, &["add", "-A"]);
    git(ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"]);

    let run = |extra: &[&str]| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_duckle-runner"));
        c.arg("test")
            .args(extra)
            .current_dir(ws)
            .env("DUCKLE_DUCKDB_BIN", &bin)
            .env("DUCKLE_WORKSPACE", ws);
        c.output().expect("the runner starts")
    };

    // A clean tree affects nothing - and that is a pass, not an error, or
    // every clean pull request would fail its own gate.
    let out = run(&["--affected", "--base", "HEAD", "--workspace", &ws.display().to_string()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("nothing affected"),
        "clean tree: {stdout} {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Edit one pipeline: its suite runs, the other's is skipped.
    let a = ws.join("pipelines/a.pipeline.json");
    let text = std::fs::read_to_string(&a).unwrap().replace("id > 0", "id >= 0");
    std::fs::write(&a, text).unwrap();
    let out = run(&["--affected", "--base", "HEAD", "--workspace", &ws.display().to_string()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "a ran and passed: {stdout} {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("b.test.json"), "b skipped by name: {stdout}");
    assert!(!stdout.contains("a.test.json  (pipeline not affected)"), "a ran: {stdout}");
}

/// The case the feature is sold on: a shared input the model covers
/// (duckle.json) marks every pipeline affected, so every suite runs.
#[test]
fn test_affected_runs_every_suite_when_a_shared_input_changes() {
    let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
        return;
    };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: no git");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::create_dir_all(ws.join("tests")).unwrap();
    write_pipeline(ws, "a");
    write_pipeline(ws, "b");
    write_test(ws, "a");
    write_test(ws, "b");
    std::fs::write(ws.join("duckle.json"), r#"{"name":"ws","version":1}"#).unwrap();
    git(ws, &["init", "-q"]);
    git(ws, &["add", "-A"]);
    git(ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"]);

    std::fs::write(ws.join("duckle.json"), r#"{"name":"ws","version":2}"#).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .arg("test")
        .args(["--affected", "--base", "HEAD", "--workspace", &ws.display().to_string()])
        .current_dir(ws)
        .env("DUCKLE_DUCKDB_BIN", &bin)
        .env("DUCKLE_WORKSPACE", ws)
        .output()
        .expect("the runner starts");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "both suites ran and passed: {stdout} {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !stdout.contains("(pipeline not affected)"),
        "a shared input reaches both pipelines: {stdout}"
    );
}

/// A suite whose own file changed must run even when no pipeline did - it is
/// the one change whose test cannot be skipped. And a change the model does
/// not cover at all cannot read as "nothing affected": the gate fails open
/// and runs everything.
#[test]
fn test_affected_runs_a_changed_suite_and_fails_open_on_unmodelled_changes() {
    let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
        return;
    };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: no git");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::create_dir_all(ws.join("tests")).unwrap();
    write_pipeline(ws, "a");
    write_pipeline(ws, "b");
    write_test(ws, "a");
    write_test(ws, "b");
    git(ws, &["init", "-q"]);
    git(ws, &["add", "-A"]);
    git(ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"]);

    let run = || {
        Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
            .arg("test")
            .args(["--affected", "--base", "HEAD", "--workspace", &ws.display().to_string()])
            .current_dir(ws)
            .env("DUCKLE_DUCKDB_BIN", &bin)
            .env("DUCKLE_WORKSPACE", ws)
            .output()
            .expect("the runner starts")
    };

    // Touch only suite b's file: b runs, a is skipped.
    let b = ws.join("tests/b.test.json");
    let text = std::fs::read_to_string(&b).unwrap().replace("passes", "still passes");
    std::fs::write(&b, text).unwrap();
    let out = run();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "b ran: {stdout}");
    assert!(stdout.contains("a.test.json"), "a skipped by name: {stdout}");
    assert!(
        !stdout.contains("b.test.json  (pipeline not affected)"),
        "a changed suite is never skipped: {stdout}"
    );

    // Commit that, then change a file the model does not cover at all: every
    // suite runs, because the selection cannot say the change reaches nothing.
    git(ws, &["add", "-A"]);
    git(ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "rename"]);
    std::fs::write(ws.join("notes.txt"), "unmodelled\n").unwrap();
    let out = run();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not modelled"),
        "the unclassified files are reported: {stderr}"
    );
    assert!(
        !stdout.contains("nothing affected"),
        "an unmodelled change is not 'nothing affected': {stdout}"
    );

    // But an unmodelled change NEXT TO a selected pipeline does not fail
    // open: the selection produced an answer, so the normal retain applies
    // and only the affected suite runs. Failing open here would switch the
    // feature off for the ordinary pull request that touches a doc file.
    git(ws, &["add", "-A"]);
    git(ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "unmodelled"]);
    let a = ws.join("pipelines/a.pipeline.json");
    let text = std::fs::read_to_string(&a).unwrap().replace("id > 0", "id >= 0");
    std::fs::write(&a, text).unwrap();
    std::fs::write(ws.join("README.md"), "docs\n").unwrap();
    let out = run();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "a ran: {stdout} {stderr}");
    assert!(stdout.contains("ok    "), "the affected suite ran: {stdout}");
    assert!(
        stdout.contains("b.test.json"),
        "b is still skipped when the selection produced an answer: {stdout}"
    );
    assert!(stderr.contains("README.md"), "the unmodelled file is still reported: {stderr}");
}

/// A relative --workspace used to double-join the path prefix and drop every
/// suite: the located paths already carry the workspace root.
#[test]
fn test_affected_accepts_a_relative_workspace() {
    let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
        return;
    };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: no git");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path().join("ws");
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::create_dir_all(ws.join("tests")).unwrap();
    write_pipeline(&ws, "a");
    write_test(&ws, "a");
    git(&ws, &["init", "-q"]);
    git(&ws, &["add", "-A"]);
    git(&ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"]);

    let a = ws.join("pipelines/a.pipeline.json");
    let text = std::fs::read_to_string(&a).unwrap().replace("id > 0", "id >= 0");
    std::fs::write(&a, text).unwrap();

    // Run from the PARENT with a relative --workspace, the shape that
    // double-joined the prefix. The suite is named explicitly: discovered
    // suites are read from ./tests relative to the working directory, and
    // asserting only that "nothing affected" is absent would pass on the
    // usage error the empty discovery exits with - leaving the fix unpinned.
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .arg("test")
        .args(["ws/tests/a.test.json", "--affected", "--base", "HEAD", "--workspace", "ws"])
        .current_dir(tmp.path())
        .env("DUCKLE_DUCKDB_BIN", &bin)
        .env("DUCKLE_WORKSPACE", &ws)
        .output()
        .expect("the runner starts");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "a changed pipeline under a relative workspace runs its suite: {stdout} {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("ok    passes"),
        "the suite actually ran: {stdout}"
    );
}

/// --head names a committed revision, which has no files to run suites
/// against - refusing up front beats "nothing affected" on a real change.
/// A suite whose pipeline path no longer resolves is kept and reported by
/// the ordinary path, not hidden behind "not affected".
#[test]
fn test_affected_rejects_head_and_keeps_a_suite_whose_pipeline_moved() {
    let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
        return;
    };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: no git");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::create_dir_all(ws.join("tests")).unwrap();
    write_pipeline(ws, "a");
    write_test(ws, "a");
    // A suite naming a pipeline that does not exist.
    std::fs::write(
        ws.join("tests/moved.test.json"),
        r#"{"pipeline":"../pipelines/gone.pipeline.json","cases":[{"name":"x","given":{},"expect":{"node":"flt","rowCount":0}}]}"#,
    )
    .unwrap();
    git(ws, &["init", "-q"]);
    git(ws, &["add", "-A"]);
    git(ws, &["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"]);

    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .arg("test")
        .args(["--affected", "--base", "HEAD~0", "--head", "HEAD", "--workspace", &ws.display().to_string()])
        .current_dir(ws)
        .env("DUCKLE_DUCKDB_BIN", &bin)
        .env("DUCKLE_WORKSPACE", ws)
        .output()
        .expect("the runner starts");
    assert_eq!(out.status.code(), Some(2), "--head is refused up front");

    let a = ws.join("pipelines/a.pipeline.json");
    let text = std::fs::read_to_string(&a).unwrap().replace("id > 0", "id >= 0");
    std::fs::write(&a, text).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .arg("test")
        .args(["--affected", "--base", "HEAD", "--workspace", &ws.display().to_string()])
        .current_dir(ws)
        .env("DUCKLE_DUCKDB_BIN", &bin)
        .env("DUCKLE_WORKSPACE", ws)
        .output()
        .expect("the runner starts");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("moved.test.json  (pipeline not affected)"),
        "an unresolvable pipeline is not 'not affected': {stdout}"
    );
}
