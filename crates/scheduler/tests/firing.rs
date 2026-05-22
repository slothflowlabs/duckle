//! Integration test for the scheduled-run path: load a pipeline from
//! the workspace, execute it through the engine, write the output, and
//! record run history. This is everything the ticker does on a fire,
//! minus the wall-clock wait.

use duckle_duckdb_engine::DuckdbEngine;
use duckle_scheduler::{Schedule, ScheduleKind, Scheduler};
use serde_json::json;

fn norm(p: &std::path::Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

#[tokio::test]
async fn run_now_executes_pipeline_from_disk_and_records_history() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();

    // Input CSV.
    let csv = ws.join("in.csv");
    std::fs::write(&csv, "id,v\n1,a\n2,b\n3,c\n").unwrap();
    let out = ws.join("out.csv");

    // Pipeline file at <ws>/pipelines/pipe1.json (CSV -> CSV sink).
    let pipelines = ws.join("pipelines");
    std::fs::create_dir_all(&pipelines).unwrap();
    let pipeline = json!({
        "nodes": [
            { "id": "s1", "position": {"x":0,"y":0}, "data": {
                "label":"CSV","componentId":"src.csv",
                "properties": { "path": norm(&csv), "hasHeader": true } } },
            { "id": "k1", "position": {"x":0,"y":0}, "data": {
                "label":"Out","componentId":"snk.csv",
                "properties": { "path": norm(&out), "hasHeader": true } } }
        ],
        "edges": [
            { "id":"e1", "source":"s1", "target":"k1", "data": { "connectionType":"main" } }
        ]
    });
    std::fs::write(
        pipelines.join("pipe1.json"),
        serde_json::to_string_pretty(&pipeline).unwrap(),
    )
    .unwrap();

    // Drives the real DuckDB CLI; soft-skip if not provided.
    let engine = match std::env::var("DUCKLE_DUCKDB_BIN").ok() {
        Some(bin) if std::path::Path::new(&bin).exists() => {
            DuckdbEngine::new(std::path::PathBuf::from(bin))
        }
        _ => {
            eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a duckdb CLI to run");
            return;
        }
    };
    let sched = Scheduler::new(engine);
    sched.set_workspace(Some(ws.to_path_buf()));

    let s = sched
        .upsert(Schedule {
            id: String::new(),
            pipeline_id: "pipe1".into(),
            name: "nightly".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 3600 },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        })
        .unwrap();

    // Fire it the way the ticker would.
    let result = sched.run_now(&s.id).await.unwrap();
    assert_eq!(result.status, "ok", "scheduled run failed: {:?}", result.error);

    // The pipeline actually wrote its output.
    assert!(out.exists(), "scheduled run should have written out.csv");
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written.lines().count(), 4, "header + 3 rows");

    // History was recorded with the scheduled trigger.
    let hist = ws.join("runs").join("pipe1.json");
    assert!(hist.exists(), "run history file should exist");
    let hist_content = std::fs::read_to_string(&hist).unwrap();
    assert!(
        hist_content.contains("\"scheduled\""),
        "history should mark the run as scheduled"
    );

    // The schedule's last-run bookkeeping was updated.
    let after = sched.list();
    let updated = after.iter().find(|x| x.id == s.id).unwrap();
    assert_eq!(updated.last_run_status.as_deref(), Some("ok"));
    assert!(updated.last_run_at.is_some());
}
