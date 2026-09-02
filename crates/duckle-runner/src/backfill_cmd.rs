//! #295: `duckle-runner backfill create | status | retry | cancel`.
//!
//! A backfill is a plan written to disk before anything runs, and each slice is
//! an ordinary durable run. Nothing here is a new execution path: a partition
//! run gets a receipt, a release id and a log line exactly as a manual run
//! does, and its parameters go through the same #317 boundary every other
//! caller uses - with `partition` as the source, so an operator can see that a
//! value came from the slice rather than from them.

use duckle_duckdb_engine::backfill::{self, Backfill, PartitionRun, State};
use duckle_duckdb_engine::{partition, PipelineDoc};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

pub fn usage() -> ExitCode {
    eprintln!(
        "usage: duckle-runner backfill <command>\n\
         \n\
         \x20 create <pipeline.json> --from YYYY-MM-DD --to YYYY-MM-DD [--max-concurrent N]\n\
         \x20                        [--dry-run] [--workspace DIR] [--json]\n\
         \x20 status [<backfill-id>] [--workspace DIR] [--json]\n\
         \x20 retry <backfill-id> [--partition KEY] [--verify] [--workspace DIR]\n\
         \x20 cancel <backfill-id> [--workspace DIR]\n\
         \n\
         \x20 list | set | clear   inspect and edit incremental state (unpartitioned)"
    );
    ExitCode::from(2)
}

/// The subcommands that belong to partitioned backfills rather than to
/// watermark editing, which owns `list`, `set` and `clear`.
pub fn is_partition_verb(verb: &str) -> bool {
    matches!(verb, "create" | "status" | "retry" | "cancel")
}

struct Args {
    workspace: PathBuf,
    from: String,
    to: String,
    max_concurrent: usize,
    partition: Option<String>,
    occurrence: Option<String>,
    force: bool,
    /// #306: re-hash every committed part before deciding what to retry.
    verify: bool,
    dry_run: bool,
    json: bool,
    positional: Vec<String>,
}

fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args {
        workspace: PathBuf::from("."),
        from: String::new(),
        to: String::new(),
        max_concurrent: 4,
        partition: None,
        occurrence: None,
        force: false,
        verify: false,
        dry_run: false,
        json: false,
        positional: Vec::new(),
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--workspace" => a.workspace = it.next().map(Into::into).unwrap_or(a.workspace),
            "--from" => a.from = it.next().unwrap_or_default(),
            "--to" => a.to = it.next().unwrap_or_default(),
            "--partition" => a.partition = it.next(),
            "--occurrence" => a.occurrence = it.next(),
            "--force" => a.force = true,
            "--verify" => a.verify = true,
            "--max-concurrent" => {
                a.max_concurrent = it
                    .next()
                    .and_then(|v| v.trim().parse().ok())
                    .filter(|n| *n > 0)
                    .unwrap_or(a.max_concurrent)
            }
            "--dry-run" => a.dry_run = true,
            "--json" => a.json = true,
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other => a.positional.push(other.to_string()),
        }
    }
    Ok(a)
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    let Some(command) = it.next() else { return usage() };
    let args = match parse(it) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("duckle-runner backfill: {e}");
            return ExitCode::from(2);
        }
    };
    match command.as_str() {
        "create" => create(&args),
        "status" => status(&args),
        "retry" => retry(&args),
        "cancel" => cancel(&args),
        _ => usage(),
    }
}

fn read_doc(path: &Path) -> Result<(serde_json::Value, PipelineDoc), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let raw: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let doc: PipelineDoc =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok((raw, doc))
}

fn create(args: &Args) -> ExitCode {
    let Some(path) = args.positional.first().map(PathBuf::from) else { return usage() };
    let (raw, _doc) = match read_doc(&path) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("duckle-runner backfill create: {e}");
            return ExitCode::from(2);
        }
    };
    let Some(def) = partition::of(&raw) else {
        eprintln!(
            "duckle-runner backfill create: {} declares no `partition`, so there is nothing to \n\
             slice it by. Add one, or use `backfill set` for an unpartitioned incremental pipeline.",
            path.display()
        );
        return ExitCode::from(2);
    };
    let partitions = match partition::generate(&def, &args.from, &args.to) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle-runner backfill create: {e}");
            return ExitCode::from(2);
        }
    };
    if partitions.is_empty() {
        eprintln!("duckle-runner backfill create: that range produces no partitions");
        return ExitCode::from(1);
    }

    // A dry run says exactly what would be queued, before anything is written.
    // "How many runs is this going to be" is the question worth answering
    // before starting a three-year backfill, not after.
    if args.dry_run {
        if args.json {
            println!("{}", serde_json::to_string_pretty(&partitions).unwrap_or_default());
        } else {
            for p in &partitions {
                println!("  {}", p.key);
            }
            println!("\n{} partition(s) would be queued", partitions.len());
        }
        return ExitCode::from(0);
    }

    // The same builder the console and MCP use, so a plan is identical
    // whoever asked for it - and the occurrence ids come out the same, which
    // is the whole point of them.
    let plan = match duckle_duckdb_engine::backfill_exec::plan_for(
        &args.workspace,
        &path,
        &args.from,
        &args.to,
        args.max_concurrent,
        args.occurrence.as_deref(),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle-runner backfill create: {e}");
            return ExitCode::from(2);
        }
    };
    // Written BEFORE anything executes. A backfill that started and was killed
    // before its plan reached disk is one nobody can resume or even name.
    if let Err(e) = duckle_duckdb_engine::backfill::save(&args.workspace, &plan) {
        eprintln!("duckle-runner backfill create: {e}");
        return ExitCode::from(2);
    }
    println!("backfill {}  {} partition(s)", plan.id, plan.partitions.len());
    execute(&args.workspace, plan, args.json, args.force)
}

fn status(args: &Args) -> ExitCode {
    match args.positional.first() {
        None => {
            let all = backfill::list(&args.workspace);
            if args.json {
                println!("{}", serde_json::to_string_pretty(&all).unwrap_or_default());
                return ExitCode::from(0);
            }
            if all.is_empty() {
                println!("no backfills in this workspace");
            }
            for b in &all {
                let counts = b.counts();
                let summary: Vec<String> =
                    counts.iter().map(|(k, v)| format!("{v} {k}")).collect();
                println!("  {}  {}  [{}]", b.id, b.pipeline, summary.join(", "));
            }
            ExitCode::from(0)
        }
        Some(id) => {
            let b = match backfill::load(&args.workspace, id) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("duckle-runner backfill status: {e}");
                    return ExitCode::from(2);
                }
            };
            if args.json {
                println!("{}", serde_json::to_string_pretty(&b).unwrap_or_default());
                return ExitCode::from(0);
            }
            println!("backfill {}  pipeline {}", b.id, b.pipeline);
            if let Some(r) = &b.release_id {
                println!("release  {r}");
            }
            for p in &b.partitions {
                let run = p.run_id.as_deref().unwrap_or("-");
                let err = p.error.as_deref().unwrap_or("");
                println!("  {:<12} {:<12} {run}  {err}", p.key, p.state.as_str());
            }
            let counts = b.counts();
            let summary: Vec<String> = counts.iter().map(|(k, v)| format!("{v} {k}")).collect();
            println!("\n{}", summary.join(", "));
            match b.partitions.iter().any(|p| p.state == State::Failed) {
                true => ExitCode::from(1),
                false => ExitCode::from(0),
            }
        }
    }
}

fn retry(args: &Args) -> ExitCode {
    let Some(id) = args.positional.first() else { return usage() };
    let mut b = match backfill::load(&args.workspace, id) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("duckle-runner backfill retry: {e}");
            return ExitCode::from(2);
        }
    };
    // #306: a chunked extract's parts are checked for existence and size on
    // every run. `--verify` re-READS them, which is the only thing that catches
    // one edited or corrupted in place - and costs a full pass over the extract
    // to do, which is why it is asked for rather than assumed.
    let redo = b.recheck_artifacts(args.verify);
    if !redo.is_empty() {
        println!(
            "{} part(s) no longer match what was committed: {}",
            redo.len(),
            redo.join(", ")
        );
    }
    let only = args.partition.clone().map(|k| vec![k]);
    // A slice the recheck reset is work to do, and counting only `retry_open`
    // here meant `retry` DETECTED a lost part, moved it back to requested, then
    // reported "nothing to retry" and returned - leaving the extract short with
    // the ledger saying it had just been checked.
    let n = redo.len() + b.retry_open(only.as_deref());
    if n == 0 {
        println!("nothing to retry in {id}");
        return ExitCode::from(0);
    }
    b.pid = Some(std::process::id());
    if let Err(e) = backfill::save(&args.workspace, &b) {
        eprintln!("duckle-runner backfill retry: {e}");
        return ExitCode::from(2);
    }
    println!("retrying {n} partition(s) of {id}");
    execute(&args.workspace, b, args.json, true)
}

fn cancel(args: &Args) -> ExitCode {
    let Some(id) = args.positional.first() else { return usage() };
    let mut b = match backfill::load(&args.workspace, id) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("duckle-runner backfill cancel: {e}");
            return ExitCode::from(2);
        }
    };
    let n = b.cancel();
    b.pid = None;
    if let Err(e) = backfill::save(&args.workspace, &b) {
        eprintln!("duckle-runner backfill cancel: {e}");
        return ExitCode::from(2);
    }
    println!("cancelled {n} partition(s) of {id}");
    ExitCode::from(0)
}

/// The CLI's shell around the shared executor.
///
/// Printing and an exit code; the orchestration itself lives in the engine so
/// the console and MCP run slices the same way this does.
fn execute(workspace: &Path, plan: Backfill, json: bool, force: bool) -> ExitCode {
    let duckdb = match crate::resolve_duckdb(None) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner backfill: {e}");
            return ExitCode::from(2);
        }
    };
    // #306: dispatched by the ledger's kind, so retrying a chunked extract
    // resumes the extract rather than running the whole pipeline per chunk.
    let plan = match duckle_duckdb_engine::backfill_exec::execute_ledger(
        workspace,
        &duckdb,
        plan,
        force,
        &|o| {
            eprintln!(
                "  {:<12} {}",
                o.key,
                match (&o.error, &o.reused_from) {
                    (Some(e), _) => format!("FAILED  {e}"),
                    (None, Some(b)) => format!("already done by {b}"),
                    (None, None) => "ok".to_string(),
                }
            );
        },
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle-runner backfill: {e}");
            return ExitCode::from(2);
        }
    };
    let counts = plan.counts();
    if json {
        println!("{}", serde_json::to_string_pretty(&plan).unwrap_or_default());
    } else {
        let summary: Vec<String> = counts.iter().map(|(k, v)| format!("{v} {k}")).collect();
        println!("
{}: {}", plan.id, summary.join(", "));
    }
    match counts.get("failed").copied().unwrap_or(0) {
        0 => ExitCode::from(0),
        _ => ExitCode::from(1),
    }
}
