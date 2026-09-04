//! #326: `duckle-runner sequence status|plan|apply` - ordered delta chains.
//!
//! The issue asks for a report that says where a chain has got to and, when it
//! cannot proceed, exactly which link is missing. That is `status`. `plan`
//! writes the ledger, `apply` runs it, and everything after that - retry,
//! cancel, restart reconciliation - is `backfill`, because a sequence is a
//! slice generator over the existing ledger rather than a second job system.

use duckle_duckdb_engine::backfill::{self, Backfill, Kind};
use duckle_duckdb_engine::sequence::{self, Status};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: duckle-runner sequence status|plan|apply <pipeline.json> [--workspace DIR] [--json]\n\
         \n\
         \x20 --force                apply a link even if an identical one already succeeded\n\
         \n\
         The pipeline declares a `sequence` block:\n\
         \n\
         \x20 \"sequence\": {{\n\
         \x20   \"uri\": \"s3://registry/deltas\",\n\
         \x20   \"pattern\": \"D{{date:YYYYMMDD}}.KBO.zip\",\n\
         \x20   \"order\": {{ \"type\": \"date\", \"cadence\": \"day\" }},\n\
         \x20   \"requireContinuity\": true,\n\
         \x20   \"baseline\": \"2026-08-31\"\n\
         \x20 }}\n\
         \n\
         status  where the chain is, and which link is missing. Changes nothing.\n\
         plan    write the ledger for the links that are published\n\
         apply   run it, one link at a time, in order. Resume with `backfill retry`.\n\
         \n\
         A link is only claimable once its predecessor has SUCCEEDED, so a hole\n\
         stops the chain instead of being stepped over. Exit 1 when the chain is\n\
         blocked, so a scheduled check can gate on it."
    );
    ExitCode::from(2)
}

struct Args {
    verb: String,
    path: PathBuf,
    workspace: PathBuf,
    json: bool,
    force: bool,
}

fn parse() -> Option<Args> {
    let mut it = std::env::args().skip(2);
    let verb = match it.next() {
        Some(v) if matches!(v.as_str(), "status" | "plan" | "apply") => v,
        _ => return None,
    };
    let mut path = None;
    let mut workspace = PathBuf::from(".");
    let (mut json, mut force) = (false, false);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--json" => json = true,
            "--force" => force = true,
            "--workspace" => workspace = it.next().map(PathBuf::from)?,
            other if other.starts_with('-') => {
                eprintln!("duckle-runner sequence: unknown flag {other}");
                return None;
            }
            other => path = Some(PathBuf::from(other)),
        }
    }
    Some(Args { verb, path: path?, workspace, json, force })
}

pub fn run() -> ExitCode {
    let Some(args) = parse() else { return usage() };
    let doc: serde_json::Value = match std::fs::read_to_string(&args.path)
        .map_err(|e| e.to_string())
        .and_then(|t| serde_json::from_str(&t).map_err(|e| e.to_string()))
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner sequence: {}: {e}", args.path.display());
            return ExitCode::from(2);
        }
    };
    let Some(def) = sequence::of(&doc) else {
        eprintln!(
            "duckle-runner sequence: {} declares no `sequence`, so its objects are independent \
             rather than a chain. That is the ordinary case and needs nothing here.",
            args.path.display()
        );
        return ExitCode::from(2);
    };
    let pipeline = args
        .path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pipeline".into());

    // Progression comes from the ledger, which is the only record of it.
    let states = sequence::ledger_states(&args.workspace, &pipeline, def.epoch.as_deref());
    let props = doc.get("sequence").cloned().unwrap_or(serde_json::Value::Null);
    let report = match sequence::report(&def, &props, |k| states.get(k).copied()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("duckle-runner sequence: {e}");
            return ExitCode::from(2);
        }
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report).unwrap_or_default());
    } else {
        print_report(&report);
    }

    // A malformed name is a finding whichever verb was asked for: a chain that
    // looks contiguous because an object was silently dropped is the failure
    // the totality contract exists to prevent, so it is never only a warning.
    let refused = !report.refusals.is_empty();
    let blocked = matches!(report.verdict.state, Status::Blocked { .. });

    if args.verb == "status" {
        return match refused || (blocked && report.require_continuity) {
            true => ExitCode::from(1),
            false => ExitCode::from(0),
        };
    }
    if refused {
        eprintln!(
            "\nrefusing to plan: {} object(s) in the sequence produced no key. Fix the names, or \
             narrow the collection so they are not selected.",
            report.refusals.len()
        );
        return ExitCode::from(1);
    }

    let release = duckle_duckdb_engine::release::active(
        &args.workspace,
        &std::env::var("DUCKLE_ENVIRONMENT").unwrap_or_else(|_| "default".into()),
    );
    let slices = sequence::slices(&def, &pipeline, release.as_deref(), &report.links);
    if slices.is_empty() {
        println!("nothing published yet: no links to plan");
        return ExitCode::from(0);
    }
    let plan = Backfill {
        id: backfill::new_id(&format!("{pipeline}-seq")),
        pipeline: pipeline.clone(),
        pipeline_path: args.path.display().to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        release_id: release,
        // A chain is serial by construction - the claim predicate sees to that -
        // so asking for more workers would only allocate threads that block.
        max_concurrent: 1,
        pid: Some(std::process::id()),
        kind: Kind::Sequence,
        chunk_node: None,
        staging: None,
        epoch: def.epoch.clone(),
        partitions: slices,
    };
    if let Err(e) = backfill::save(&args.workspace, &plan) {
        eprintln!("duckle-runner sequence {}: {e}", args.verb);
        return ExitCode::from(2);
    }
    println!("\nledger {} ({} link(s))", plan.id, plan.partitions.len());
    if args.verb == "plan" {
        println!("run it with: duckle-runner sequence apply {}", args.path.display());
        return ExitCode::from(0);
    }
    execute(&args.workspace, plan, args.force)
}

fn print_report(r: &sequence::Report) {
    let position = r.verdict.position.as_deref().unwrap_or("-");
    match &r.verdict.state {
        Status::Complete => {
            println!("sequence status: complete");
            println!("position:        {position}");
        }
        Status::WaitingForNext { expected, observed } => {
            println!("sequence status: waiting_for_next");
            println!("position:        {position}");
            println!("expected:        {expected}");
            println!(
                "                 {}",
                match observed {
                    // Two different problems that a position pointer reports
                    // identically, which is why they are printed apart.
                    true => "published, not yet applied",
                    false => "not published yet - whether that is LATE is a freshness question",
                }
            );
        }
        Status::Blocked { expected, next_observed, reason } => {
            println!("sequence status: blocked");
            println!("position:        {position}");
            println!("expected:        {expected}");
            if let Some(waiting) = next_observed {
                println!("next observed:   {waiting}");
            }
            println!("reason:          {}", reason.as_str());
            if !r.require_continuity {
                println!(
                    "                 reported only: this sequence does not set \
                     requireContinuity, so nothing is prevented"
                );
            }
        }
    }
    if let Some(epoch) = &r.epoch {
        println!("epoch:           {epoch}");
    }
    if let Some(baseline) = &r.baseline {
        println!("baseline:        {baseline}");
    }
    let missing: Vec<&str> = r.links.iter().filter(|l| l.is_missing()).map(|l| l.key.as_str()).collect();
    if !missing.is_empty() {
        println!("holes:           {}", missing.join(", "));
    }
    if !r.refusals.is_empty() {
        println!("\n{} object(s) produced no sequence key:", r.refusals.len());
        for f in &r.refusals {
            println!("  {:<24} {}", f.code.as_str(), f.uri);
            println!("  {:<24} {}", "", f.detail);
        }
        println!("\nexpected pattern: {}", r.refusals[0].expected_pattern);
    }
}

/// The same shell `backfill` uses, so a link runs exactly as any other slice.
fn execute(workspace: &Path, plan: Backfill, force: bool) -> ExitCode {
    let duckdb = match crate::resolve_duckdb(None) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner sequence apply: {e}");
            return ExitCode::from(2);
        }
    };
    let out = match duckle_duckdb_engine::backfill_exec::execute_ledger(
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
            eprintln!("duckle-runner sequence apply: {e}");
            return ExitCode::from(2);
        }
    };
    let counts = out.counts();
    println!(
        "\n{}",
        counts.iter().map(|(k, n)| format!("{n} {k}")).collect::<Vec<_>>().join(", ")
    );
    // A link left `requested` after a pass is not a failure, it is the chain
    // stopping where it should. Saying which predecessor it is waiting for is
    // the difference between that and a run that silently did less than asked.
    let mut stalled = false;
    for i in 0..out.partitions.len() {
        if let Some(why) = out.blocked_reason(i) {
            println!("  {:<12} {why}", out.partitions[i].key);
            stalled = true;
        }
    }
    match out.is_done() && !stalled {
        true => ExitCode::from(0),
        false => ExitCode::from(1),
    }
}
