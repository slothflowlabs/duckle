//! #309: `duckle-runner runs diff <run_a> <run_b>`.
//!
//! Joins each run's receipt to its history record and hands both to the
//! comparison in the engine. Neither run is opened for writing anywhere in
//! here: a comparison that could alter what it compares would be worthless the
//! second time it was run.

use duckle_duckdb_engine::history::{load_run_history, RunRecord};
use duckle_duckdb_engine::retry::{self, RunReceipt};
use duckle_duckdb_engine::rundiff::{self, Area};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// The history record for a run id, if one was kept.
///
/// History is per pipeline and capped, so an old run has a receipt long after
/// its record has been trimmed away. That is a normal, expected absence rather
/// than an error - and it is why the comparison reports what it could not see.
fn record_for(workspace: &Path, receipt: &RunReceipt) -> Option<RunRecord> {
    let pipeline_id = Path::new(&receipt.pipeline_path)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| receipt.pipeline_name.clone());
    load_run_history(workspace, &pipeline_id)
        .into_iter()
        .find(|r| r.run_id.as_deref() == Some(receipt.run_id.as_str()))
}

fn label(area: Area) -> &'static str {
    match area {
        Area::Code => "code",
        Area::Runtime => "runtime",
        Area::Invocation => "invocation",
        Area::Inputs => "inputs",
        Area::Execution => "execution",
        Area::Output => "output",
    }
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    if it.next().as_deref() != Some("diff") {
        eprintln!("usage: duckle-runner runs diff <run_a> <run_b> [--workspace DIR] [--json]");
        return ExitCode::from(2);
    }
    let mut ids: Vec<String> = Vec::new();
    let mut workspace = PathBuf::from(".");
    let mut json = false;
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(Into::into).unwrap_or(workspace),
            "--json" => json = true,
            other if other.starts_with('-') => {
                eprintln!("duckle-runner runs diff: unknown argument {other}");
                return ExitCode::from(2);
            }
            other => ids.push(other.to_string()),
        }
    }
    if ids.len() != 2 {
        eprintln!("duckle-runner runs diff: needs exactly two run ids, got {}", ids.len());
        return ExitCode::from(2);
    }

    let mut loaded = Vec::new();
    for id in &ids {
        match retry::load(&workspace, id) {
            Ok(r) => loaded.push(r),
            Err(retry::LoadError::NotFound) => {
                eprintln!(
                    "duckle-runner runs diff: no receipt for {id}. Runs started before receipts existed, and runs whose receipt has been pruned, cannot be compared."
                );
                return ExitCode::from(2);
            }
            Err(retry::LoadError::Unreadable(e)) => {
                eprintln!("duckle-runner runs diff: {id}: {e}");
                return ExitCode::from(2);
            }
        }
    }
    let (a, b) = (&loaded[0], &loaded[1]);
    let (ra, rb) = (record_for(&workspace, a), record_for(&workspace, b));
    let diff = rundiff::compare(a, b, ra.as_ref(), rb.as_ref());

    if json {
        println!("{}", serde_json::to_string_pretty(&diff).unwrap_or_default());
        return ExitCode::from(0);
    }

    println!("{}  {}  {}  {}", diff.a.run_id, diff.a.at, diff.a.status, diff.a.trigger);
    println!("{}  {}  {}  {}\n", diff.b.run_id, diff.b.at, diff.b.status, diff.b.trigger);
    if diff.differences.is_empty() {
        println!("nothing this build compares is different");
    }
    let mut current: Option<Area> = None;
    for d in &diff.differences {
        if current != Some(d.area) {
            println!("{}:", label(d.area));
            current = Some(d.area);
        }
        println!("  {:<38} {}  ->  {}", d.field, d.a, d.b);
    }
    if !diff.explanations.is_empty() {
        println!();
        for e in &diff.explanations {
            println!("* {e}");
        }
    }
    if !diff.not_compared.is_empty() {
        println!("\nnot compared:");
        for n in &diff.not_compared {
            println!("  {n}");
        }
    }
    ExitCode::from(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckle_duckdb_engine::history::append_run_record;

    fn workspace() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "duckle-runsdiff-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    fn receipt(id: &str) -> RunReceipt {
        retry::RunReceipt {
            run_id: id.into(),
            trigger: "manual".into(),
            state: "finished".into(),
            pid: None,
            parent_run_id: None,
            at: "2026-09-01T00:00:00Z".into(),
            status: "ok".into(),
            pipeline_name: "nightly".into(),
            pipeline_path: "pipelines/nightly.json".into(),
            pipeline_hash: "aaa".into(),
            engine_version: "1.5.4".into(),
            parameters: Default::default(),
            nodes: Default::default(),
        }
    }

    #[test]
    fn a_history_record_is_joined_to_its_receipt_by_run_id() {
        let ws = workspace();
        let r = receipt("run-1");
        retry::write(&ws, &r).unwrap();
        let mut rec = RunRecord {
            run_id: Some("run-1".into()),
            at: "2026-09-01T00:00:00Z".into(),
            status: "ok".into(),
            duration_ms: 1234,
            rows: 7,
            node_count: 1,
            trigger: "manual".into(),
            error: None,
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            category: None,
            assets: vec![],
        };
        append_run_record(&ws, "nightly", rec.clone());
        // A record for a DIFFERENT run must not be picked up.
        rec.run_id = Some("run-2".into());
        rec.duration_ms = 9999;
        append_run_record(&ws, "nightly", rec);

        let found = record_for(&ws, &r).expect("joined");
        assert_eq!(found.duration_ms, 1234, "matched the wrong run's record");
    }

    #[test]
    fn a_run_whose_history_was_trimmed_still_compares() {
        // History is capped per pipeline; an old run keeps its receipt long
        // after its record is gone. That must degrade, not fail.
        let ws = workspace();
        let a = receipt("run-a");
        let b = receipt("run-b");
        retry::write(&ws, &a).unwrap();
        retry::write(&ws, &b).unwrap();
        assert!(record_for(&ws, &a).is_none());
        let d = rundiff::compare(&a, &b, None, None);
        assert!(d.not_compared.iter().any(|n| n.contains("history record")));
    }
}
