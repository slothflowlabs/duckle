//! `duckle-runner openlineage` - drain the lineage event buffer (#311).
//!
//! `emit` appends every event to `logs/openlineage.ndjson` before it tries
//! the collector, so a collector that is down costs one timeout and the event
//! is durable. Runs flush that backlog opportunistically when a POST lands;
//! this command is the same drain on demand, for an operator or a scheduler
//! that does not want to wait for a run.

use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "\
duckle-runner openlineage flush [--workspace DIR] [--json]

Send every buffered event in logs/openlineage.ndjson to the collector named
in openlineage.json, keeping what it refuses for the next pass. This is the
unbounded drain; a run's own pass is capped so a long outage cannot hold a
finished run open. Events the collector rejects outright (4xx) are quarantined
to logs/openlineage.rejected.ndjson rather than replayed forever.
";

pub fn run() -> ExitCode {
    let mut workspace = PathBuf::from(".");
    let mut json = false;
    let mut verb = None;
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--workspace" => match it.next() {
                Some(v) => workspace = PathBuf::from(v),
                None => {
                    eprintln!("duckle-runner openlineage: --workspace needs a value\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--json" => json = true,
            other if verb.is_none() && !other.starts_with('-') => verb = Some(other.to_string()),
            other => {
                eprintln!("duckle-runner openlineage: unknown argument {other}\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    if verb.as_deref() != Some("flush") {
        eprint!("{USAGE}");
        return ExitCode::from(2);
    }
    let Some(cfg) = duckle_duckdb_engine::openlineage::load(&workspace) else {
        eprintln!(
            "duckle-runner openlineage: no openlineage.json in {} - nothing configured, nothing to send",
            workspace.display()
        );
        return ExitCode::from(2);
    };
    let outcome = duckle_duckdb_engine::openlineage::flush(&workspace, &cfg);
    if json {
        println!(
            "{}",
            serde_json::json!({ "sent": outcome.sent, "kept": outcome.kept, "rejected": outcome.rejected })
        );
    } else {
        println!(
            "sent {} event(s), {} still buffered, {} rejected",
            outcome.sent, outcome.kept, outcome.rejected
        );
    }
    ExitCode::from(0)
}
