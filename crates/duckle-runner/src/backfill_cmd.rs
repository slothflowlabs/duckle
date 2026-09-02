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
         \x20 retry <backfill-id> [--partition KEY] [--workspace DIR]\n\
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

    let name = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pipeline".into());
    let plan = Backfill {
        id: backfill::new_id(&name),
        pipeline: name.clone(),
        pipeline_path: path.display().to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        release_id: duckle_duckdb_engine::release::active(
            &args.workspace,
            &std::env::var("DUCKLE_ENVIRONMENT").unwrap_or_else(|_| "default".into()),
        ),
        max_concurrent: args.max_concurrent,
        pid: Some(std::process::id()),
        partitions: partitions
            .into_iter()
            .map(|p| PartitionRun {
                key: p.key,
                state: State::Requested,
                run_id: None,
                attempts: 0,
                error: None,
                finished_at: None,
                params: p.params,
            })
            .collect(),
    };
    // Written BEFORE anything executes. A backfill that started and was killed
    // before its plan reached disk is one nobody can resume or even name.
    if let Err(e) = backfill::save(&args.workspace, &plan) {
        eprintln!("duckle-runner backfill create: {e}");
        return ExitCode::from(2);
    }
    println!("backfill {}  {} partition(s)", plan.id, plan.partitions.len());
    execute(&args.workspace, plan, args.json)
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
    let only = args.partition.clone().map(|k| vec![k]);
    let n = b.retry_open(only.as_deref());
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
    execute(&args.workspace, b, args.json)
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

/// Run every open partition, `max_concurrent` at a time.
///
/// Each slice is an ordinary run: its own receipt, its own release id, its own
/// log lines. The plan is saved after every state change rather than at the end,
/// because the whole point is that a kill halfway through leaves a resumable
/// record rather than an unanswerable question.
fn execute(workspace: &Path, plan: Backfill, json: bool) -> ExitCode {
    let duckdb = match crate::resolve_duckdb(None) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner backfill: {e}");
            return ExitCode::from(2);
        }
    };
    let workers = plan
        .max_concurrent
        .min(plan.partitions.iter().filter(|p| p.state.is_claimable()).count())
        .max(1);
    // #295: a backfill's own bound is an ADDITIONAL ceiling, not a way around
    // the machine's. Each slice still acquires the pool its pipeline asks for,
    // so `--max-concurrent 4` over a pipeline in a pool of 1 runs one at a
    // time - the protection exists to stop two heavy jobs competing, and a
    // backfill is the most likely thing to try.
    let gates = std::sync::Arc::new(duckle_duckdb_engine::pools::Gates::load(workspace));
    let id = plan.id.clone();
    let path = PathBuf::from(&plan.pipeline_path);
    let pipeline = plan.pipeline.clone();
    let release = plan.release_id.clone();
    let shared = Arc::new(Mutex::new(plan));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let shared = Arc::clone(&shared);
            let workspace = workspace.to_path_buf();
            let duckdb = duckdb.clone();
            let path = path.clone();
            let pipeline = pipeline.clone();
            let release = release.clone();
            let id = id.clone();
            let gates = std::sync::Arc::clone(&gates);
            scope.spawn(move || loop {
                // Claim one slice under the lock and mark it running on disk
                // before starting, so a crash leaves it `running` for the next
                // start to reconcile rather than looking untouched.
                let claimed = {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let Some(idx) = plan.partitions.iter().position(|p| p.state.is_claimable())
                    else {
                        return;
                    };
                    plan.partitions[idx].state = State::Running;
                    plan.partitions[idx].attempts += 1;
                    let _ = backfill::save(&workspace, &plan);
                    (idx, plan.partitions[idx].clone())
                };
                let (idx, slice) = claimed;
                let outcome =
                    run_one(&workspace, &duckdb, &path, &pipeline, &id, &slice, &release, &gates);
                {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let p = &mut plan.partitions[idx];
                    p.finished_at = Some(chrono::Utc::now().to_rfc3339());
                    match &outcome {
                        Ok(run_id) => {
                            p.state = State::Succeeded;
                            p.run_id = Some(run_id.clone());
                            p.error = None;
                        }
                        Err((run_id, e)) => {
                            p.state = State::Failed;
                            p.run_id = run_id.clone();
                            p.error = Some(e.clone());
                        }
                    }
                    let _ = backfill::save(&workspace, &plan);
                    eprintln!(
                        "  {:<12} {}",
                        slice.key,
                        match &outcome {
                            Ok(_) => "ok".to_string(),
                            Err((_, e)) => format!("FAILED  {e}"),
                        }
                    );
                }
            });
        }
    });

    let mut plan = Arc::try_unwrap(shared)
        .map(|m| m.into_inner().unwrap_or_else(|p| p.into_inner()))
        .unwrap_or_else(|arc| arc.lock().unwrap_or_else(|p| p.into_inner()).clone());
    plan.pid = None;
    let _ = backfill::save(workspace, &plan);
    let counts = plan.counts();
    if json {
        println!("{}", serde_json::to_string_pretty(&plan).unwrap_or_default());
    } else {
        let summary: Vec<String> = counts.iter().map(|(k, v)| format!("{v} {k}")).collect();
        println!("\n{}: {}", plan.id, summary.join(", "));
    }
    match counts.get("failed").copied().unwrap_or(0) {
        0 => ExitCode::from(0),
        _ => ExitCode::from(1),
    }
}

/// One partition: an ordinary durable run with the slice's parameters bound.
fn run_one(
    workspace: &Path,
    duckdb: &Path,
    path: &Path,
    pipeline: &str,
    backfill_id: &str,
    slice: &PartitionRun,
    release: &Option<String>,
    gates: &duckle_duckdb_engine::pools::Gates,
) -> Result<String, (Option<String>, String)> {
    let (_, mut doc) = read_doc(path).map_err(|e| (None, e))?;
    // Held for the whole slice, like any other run.
    let (_permit, pool, queued_ms) = gates.acquire(&doc.resource_pool);
    // #317's boundary, with `partition` as the source - so an operator reading
    // the receipt can see that a value came from the slice rather than from
    // them, and a partition value colliding with one they passed is recorded as
    // an override rather than silently winning.
    let supplied: Vec<duckle_duckdb_engine::params::Supplied> = slice
        .params
        .iter()
        .map(|(name, value)| duckle_duckdb_engine::params::Supplied {
            name: name.clone(),
            value: value.clone(),
            source: "partition".to_string(),
        })
        .collect();
    let (recorded, sources) =
        duckle_duckdb_engine::context::apply_params_from(&mut doc, &supplied)
            .map_err(|e| (None, e))?;
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, workspace);

    let hash = duckle_duckdb_engine::retry::pipeline_hash(&doc);
    let run_id = duckle_duckdb_engine::retry::new_run_id(pipeline, "backfill");
    let receipt = duckle_duckdb_engine::retry::begin(
        workspace,
        &run_id,
        "backfill",
        pipeline,
        &path.display().to_string(),
        &hash,
        // The backfill is what caused this run, and naming it is what makes
        // "which slice produced this output" answerable from the receipt alone.
        Some(backfill_id.to_string()),
    );
    let receipt = duckle_duckdb_engine::retry::RunReceipt {
        parameters: recorded,
        parameter_sources: sources,
        partition_key: Some(slice.key.clone()),
        resource_pool: Some(pool),
        queue_ms: Some(queued_ms),
        components: duckle_duckdb_engine::plugin::used_by(
            workspace,
            &serde_json::to_value(&doc).unwrap_or_default(),
        ),
        release_id: release.clone().or(receipt.release_id.clone()),
        ..receipt
    };
    let _ = duckle_duckdb_engine::retry::write(workspace, &receipt);

    let engine = duckle_duckdb_engine::DuckdbEngine::new(duckdb.to_path_buf())
        .without_previews()
        .with_run_id(&run_id);
    let result = engine.execute_pipeline_named(&doc, pipeline);
    let status = result.status.clone();
    let error = result.error.clone();
    duckle_duckdb_engine::retry::finish(
        workspace,
        receipt,
        &status,
        duckle_duckdb_engine::retry::nodes_of(&result),
    );
    match status == "ok" {
        true => Ok(run_id),
        false => Err((Some(run_id), error.unwrap_or_else(|| "the run failed".into()))),
    }
}

#[cfg(test)]
mod tests {
    /// #295: a backfill slice is admitted by the machine's pool, not only by
    /// the backfill's own bound.
    ///
    /// Reads the source, because the failure is an executor that stops
    /// acquiring: `--max-concurrent 4` over a pipeline in a pool of one must
    /// still run one at a time, and a backfill is the most likely thing to try
    /// to get around that.
    #[test]
    fn every_partition_run_acquires_a_pool() {
        let src = include_str!("backfill_cmd.rs");
        // Built from pieces so the needle does not appear in this file as a
        // literal and match itself - which it did, and made this test pass
        // against an executor that had stopped acquiring anything.
        let needle = format!("gates.{}(&doc.resource_pool)", "acquire");
        let bound = format!("let (_permit, pool, queued_ms) = gates.{}", "acquire");
        let call = src
            .lines()
            .map(str::trim_start)
            // A real call site, not a comment and not a string in this test.
            .filter(|l| l.starts_with("let "))
            .find(|l| l.contains(&needle));
        let call = call.unwrap_or_else(|| {
            panic!("run_one no longer acquires a resource pool, so a backfill can outrun the machine")
        });
        // Bound for the life of the slice: a permit dropped at once admits
        // everything while still looking like it acquired.
        assert!(
            call.starts_with(&bound),
            "the permit is not held for the slice: {call}"
        );
    }
}
