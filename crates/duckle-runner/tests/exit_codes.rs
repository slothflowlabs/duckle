//! The run's exit codes as the README states them: `0` ok, `1` the pipeline
//! failed or did not compile, `2` the runner could not start.

use std::process::Command;

/// One inline row written to `out.csv`: enough to tell a run that happened from one that did not.
fn write_pipeline(dir: &std::path::Path) {
    std::fs::write(
        dir.join("p.json"),
        r#"{"nodes":[
             {"id":"n","position":{"x":0,"y":0},
              "data":{"label":"n","componentId":"src.inline",
                      "properties":{"columns":[{"key":"a","value":"1"}]}}},
             {"id":"k","position":{"x":1,"y":0},
              "data":{"label":"k","componentId":"snk.csv","properties":{"path":"out.csv"}}}],
           "edges":[{"id":"e","source":"n","target":"k"}]}"#,
    )
    .unwrap();
}

#[test]
fn a_run_with_no_engine_anywhere_could_not_start() {
    // The runner fell back to `duckdb` on PATH without looking, so a missing
    // engine surfaced inside the run and exited 1, as if the pipeline were at
    // fault. A CI job reads 1 as "the data is wrong" and 2 as "the gate never
    // ran"; a machine with no engine is the second.
    let tmp = tempfile::tempdir().unwrap();
    write_pipeline(tmp.path());
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["--pipeline", "p.json", "--workspace", "."])
        .current_dir(tmp.path())
        .env("DUCKLE_DUCKDB_BIN", tmp.path().join("no-such-duckdb.exe"))
        .env("PATH", tmp.path())
        .output()
        .expect("the runner starts");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr: {stderr}");
    assert!(stderr.contains("DuckDB"), "the message names what is missing: {stderr}");
    assert!(!tmp.path().join("out.csv").exists(), "nothing ran");
}

#[test]
fn a_duckdb_found_only_on_path_runs_the_pipeline() {
    // The runner finds DuckDB on PATH and hands the engine the bare name to
    // spawn, but the engine checked that name as a file in the working
    // directory, so every run failed "DuckDB engine not found at duckdb ...
    // or install one on PATH": the advice was what the user had already done.
    let Some(bin) = std::env::var_os("DUCKLE_DUCKDB_BIN")
        .map(std::path::PathBuf::from)
        .filter(|b| b.is_file() && b.file_stem().is_some_and(|s| s == "duckdb"))
    else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a CLI named duckdb");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    write_pipeline(tmp.path());
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["--pipeline", "p.json", "--workspace", "."])
        .current_dir(tmp.path())
        .env_remove("DUCKLE_DUCKDB_BIN")
        .env("PATH", bin.parent().unwrap())
        .output()
        .expect("the runner starts");
    let said = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert_eq!(out.status.code(), Some(0), "{said}");
    assert!(tmp.path().join("out.csv").exists(), "the pipeline wrote nothing: {said}");
}

#[test]
fn a_pipeline_test_with_duckdb_only_on_path_runs_its_sql_check() {
    // `duckle test` answers a case's `sql` through the engine's query path,
    // which had its own copy of the same check: "DuckDB engine isn't
    // installed (expected at duckdb). Open Setup to install it."
    let Some(bin) = std::env::var_os("DUCKLE_DUCKDB_BIN")
        .map(std::path::PathBuf::from)
        .filter(|b| b.is_file() && b.file_stem().is_some_and(|s| s == "duckdb"))
    else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a CLI named duckdb");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    write_pipeline(tmp.path());
    std::fs::write(
        tmp.path().join("t.json"),
        r#"{"pipeline": "p.json", "cases": [
             {"name": "one row", "expect": {"node": "n", "sql": "SELECT count(*) = 1 FROM {rows}"}}]}"#,
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["test", "t.json"])
        .current_dir(tmp.path())
        .env_remove("DUCKLE_DUCKDB_BIN")
        .env("PATH", bin.parent().unwrap())
        .output()
        .expect("the runner starts");
    let said = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert_eq!(out.status.code(), Some(0), "{said}");
    assert!(said.contains("1 passed"), "{said}");
}
