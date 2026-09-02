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
    // History is keyed differently by different surfaces: the console and the
    // scheduler key by the pipeline id, the CLI by the run's NAME, which
    // `--name` can set to anything. Both are tried rather than assuming one,
    // because guessing wrong does not fail - it silently drops the record and
    // the comparison reports less than it could.
    let stem = Path::new(&receipt.pipeline_path)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    [stem, receipt.pipeline_name.clone()]
        .iter()
        .filter(|k| !k.is_empty())
        .flat_map(|key| load_run_history(workspace, key))
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
    match it.next().as_deref() {
        Some("diff") => {}
        // #259: the log lines for one historical run, by the durable id its
        // receipt was written with. The capability arrived when the engine
        // stopped minting its own log id; this is the command that saves
        // reaching for grep and knowing which pipeline's file to look in.
        Some("logs") => return logs(it),
        _ => {
            eprintln!(
                "usage: duckle-runner runs diff <run_a> <run_b> [--workspace DIR] [--json]
                        duckle-runner runs logs <run_id> [--workspace DIR]"
            );
            return ExitCode::from(2);
        }
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

    /// #259: one run's lines, and only that run's.
    #[test]
    fn logs_are_selected_by_the_run_id_field_not_by_the_text() {
        // A run id that appears inside another run's message - an error quoting
        // it, a retry naming its parent - must not be reported as that run's
        // log, or "show me what this run did" answers with someone else's work.
        let ours = r#"{"event":"stage_finished","run_id":"run-a-1","rows":3}"#;
        let theirs = r#"{"event":"error","run_id":"run-b-2","message":"retry of run-a-1"}"#;
        let pick = |line: &str, want: &str| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v.get("run_id").and_then(|r| r.as_str()).map(|r| r == want))
                .unwrap_or(false)
        };
        assert!(pick(ours, "run-a-1"));
        assert!(!pick(theirs, "run-a-1"), "matched another run that merely mentioned it");
        assert!(pick(theirs, "run-b-2"));
        // A line that is not JSON is not a log line of anyone's.
        assert!(!pick("starting up", "run-a-1"));
    }
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
            parameter_sources: Default::default(),
            release_id: None,
            components: Vec::new(),
            artifacts: Vec::new(),
            partition_key: None,
            resource_pool: None,
            queue_reason: None,
            queued_at: None,
            started_at: None,
            queue_ms: None,
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

/// `runs logs <run_id>` - what one run wrote, wherever it wrote it.
///
/// The pipeline comes from the run's own receipt rather than being asked for:
/// an operator holding a run id from an alert or an API response should not
/// also have to know which pipeline produced it to read its log.
fn logs(mut it: impl Iterator<Item = String>) -> ExitCode {
    let mut run_id: Option<String> = None;
    let mut workspace = PathBuf::from(".");
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(Into::into).unwrap_or(workspace),
            other if other.starts_with('-') => {
                eprintln!("duckle-runner runs logs: unknown flag {other}");
                return ExitCode::from(2);
            }
            other => run_id = Some(other.to_string()),
        }
    }
    let Some(run_id) = run_id else {
        eprintln!("usage: duckle-runner runs logs <run_id> [--workspace DIR]");
        return ExitCode::from(2);
    };
    let receipt = duckle_duckdb_engine::retry::load(&workspace, &run_id);
    let log_dir = std::env::var("DUCKLE_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("logs"));
    // The receipt names the pipeline, and therefore the file. Without one - a
    // run from another workspace, a receipt pruned by retention - every log is
    // searched rather than giving up, because the id is still in the lines.
    let files: Vec<PathBuf> = match &receipt {
        Ok(r) => vec![log_dir.join(&r.pipeline_name).join("runtime.log")],
        Err(_) => std::fs::read_dir(&log_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.path().join("runtime.log"))
                    .filter(|p| p.exists())
                    .collect()
            })
            .unwrap_or_default(),
    };
    let mut found = 0usize;
    for file in &files {
        let Ok(text) = std::fs::read_to_string(file) else { continue };
        for line in text.lines() {
            // Matched on the field rather than anywhere in the line, so a run
            // whose id appears inside someone else's message is not reported as
            // that run's log.
            let is_ours = serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v.get("run_id").and_then(|r| r.as_str()).map(|r| r == run_id))
                .unwrap_or(false);
            if is_ours {
                println!("{line}");
                found += 1;
            }
        }
    }
    if found == 0 {
        eprintln!(
            "no log lines for {run_id} in {}{}",
            log_dir.display(),
            match receipt {
                Ok(_) => "",
                Err(_) => " (and no receipt for it, so every pipeline's log was searched)",
            }
        );
        return ExitCode::from(1);
    }
    ExitCode::from(0)
}
