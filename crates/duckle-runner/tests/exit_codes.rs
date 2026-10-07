//! The run's exit codes as the README states them: `0` ok, `1` the pipeline
//! failed or did not compile, `2` the runner could not start.

use std::process::Command;

#[test]
fn a_run_with_no_engine_anywhere_could_not_start() {
    // The runner fell back to `duckdb` on PATH without looking, so a missing
    // engine surfaced inside the run and exited 1, as if the pipeline were at
    // fault. A CI job reads 1 as "the data is wrong" and 2 as "the gate never
    // ran"; a machine with no engine is the second.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("p.json"),
        r#"{"nodes":[
             {"id":"n","position":{"x":0,"y":0},
              "data":{"label":"n","componentId":"src.inline",
                      "properties":{"columns":[{"key":"a","value":"1"}]}}},
             {"id":"k","position":{"x":1,"y":0},
              "data":{"label":"k","componentId":"snk.csv","properties":{"path":"out.csv"}}}],
           "edges":[{"id":"e","source":"n","target":"k"}]}"#,
    )
    .unwrap();
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
