//! A run's OpenLineage events name the datasets it read and wrote, whether or
//! not anyone has built the workspace catalog first.

use std::path::Path;
use std::process::Command;

fn duckdb() -> Option<String> {
    std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())
}

#[test]
fn lineage_events_name_their_datasets_without_a_catalog_build() {
    // The events took their datasets from the SAVED catalog only, so a
    // workspace where nobody had run `duckle-runner catalog build` emitted
    // inputs: [] and outputs: [] for every run, and a lineage tool drew
    // nothing.
    let Some(duck) = duckdb() else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a duckdb CLI to run");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    std::fs::write(ws.join("openlineage.json"), "{}").unwrap();
    std::fs::write(ws.join("in.csv"), "id\n1\n2\n").unwrap();
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::write(
        ws.join("pipelines/p.json"),
        r#"{"nodes":[
             {"id":"s","position":{"x":0,"y":0},
              "data":{"label":"s","componentId":"src.csv",
                      "properties":{"path":"${workspace}/in.csv","hasHeader":true}}},
             {"id":"k","position":{"x":1,"y":0},
              "data":{"label":"k","componentId":"snk.csv",
                      "properties":{"path":"${workspace}/out.csv"}}}],
           "edges":[{"id":"e","source":"s","target":"k"}]}"#,
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["--pipeline", "pipelines/p.json", "--workspace", "."])
        .current_dir(ws)
        .env("DUCKLE_DUCKDB_BIN", duck)
        .output()
        .expect("the runner starts");
    assert_eq!(out.status.code(), Some(0), "{}", String::from_utf8_lossy(&out.stderr));

    let events = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).expect("events written");
    let complete: serde_json::Value = events
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|e| e["eventType"] == "COMPLETE")
        .expect("a COMPLETE event");
    let names = |key: &str| -> Vec<String> {
        complete[key]
            .as_array()
            .map(|a| a.iter().filter_map(|d| d["name"].as_str().map(str::to_string)).collect())
            .unwrap_or_default()
    };
    assert!(names("inputs").iter().any(|n| n.ends_with("in.csv")), "inputs: {}", complete["inputs"]);
    assert!(names("outputs").iter().any(|n| n.ends_with("out.csv")), "outputs: {}", complete["outputs"]);
}
