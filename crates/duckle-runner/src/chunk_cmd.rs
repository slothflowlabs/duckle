//! #306: `duckle-runner source plan` - what chunked extraction would do.
//!
//! The issue asks that a plan show the selected strategy, the generated
//! predicates, the chunk count, the concurrency, the snapshot behaviour and the
//! fallback. This prints exactly that, and refuses where the connector cannot
//! give stable semantics, before anything touches a database.

use duckle_duckdb_engine::chunking::{self, Bounds, Dialect, Snapshot, Strategy};
use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: duckle-runner source plan <pipeline.json> --node ID [--json]\n\
         \n\
         \x20 --min N --max N        bounds of a range key, from your own probe\n\
         \x20 --from D --to D        bounds of a time key (YYYY-MM-DD)\n\
         \x20 --nulls N              NULLs in the key, as counted by the probe\n\
         \n\
         Prints the probe SQL when no bounds are given."
    );
    ExitCode::from(2)
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    if it.next().as_deref() != Some("plan") {
        return usage();
    }
    let mut path: Option<PathBuf> = None;
    let mut node: Option<String> = None;
    let (mut min, mut max, mut from, mut to) = (None::<i64>, None::<i64>, None, None);
    let mut nulls: u64 = 0;
    let mut json = false;
    while let Some(a) = it.next() {
        match a.as_str() {
            "--node" => node = it.next(),
            "--min" => min = it.next().and_then(|v| v.trim().parse().ok()),
            "--max" => max = it.next().and_then(|v| v.trim().parse().ok()),
            "--from" => from = it.next(),
            "--to" => to = it.next(),
            "--nulls" => nulls = it.next().and_then(|v| v.trim().parse().ok()).unwrap_or(0),
            "--json" => json = true,
            other if other.starts_with('-') => {
                eprintln!("duckle-runner source plan: unknown flag {other}");
                return ExitCode::from(2);
            }
            other => path = Some(PathBuf::from(other)),
        }
    }
    let (Some(path), Some(node)) = (path, node) else { return usage() };

    let doc: serde_json::Value = match std::fs::read_to_string(&path)
        .map_err(|e| e.to_string())
        .and_then(|t| serde_json::from_str(&t).map_err(|e| e.to_string()))
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner source plan: {}: {e}", path.display());
            return ExitCode::from(2);
        }
    };
    let Some(n) = doc
        .get("nodes")
        .and_then(|v| v.as_array())
        .and_then(|ns| ns.iter().find(|n| n.get("id").and_then(|v| v.as_str()) == Some(&node)))
    else {
        eprintln!("duckle-runner source plan: no node {node:?} in {}", path.display());
        return ExitCode::from(2);
    };
    let component = n
        .get("data")
        .and_then(|d| d.get("componentId"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let props = n.get("data").and_then(|d| d.get("properties"));
    let Some(spec) = props.and_then(|p| p.get("chunking")) else {
        eprintln!(
            "duckle-runner source plan: node {node} declares no `chunking`. Without it the \
             source is read with one query, which is the default and is fine until it is not."
        );
        return ExitCode::from(2);
    };
    let strategy: Strategy = match serde_json::from_value(spec.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("duckle-runner source plan: chunking on {node}: {e}");
            return ExitCode::from(2);
        }
    };

    // Capability first, before anything else is computed: telling someone how
    // their chunks would look and THEN that the connector cannot do it is the
    // wrong order.
    if let Err(e) = chunking::check_supported(&component, &strategy) {
        eprintln!("duckle-runner source plan: {e}");
        return ExitCode::from(1);
    }

    let table = props
        .and_then(|p| p.get("tableName").or_else(|| p.get("table")))
        .and_then(|v| v.as_str())
        .unwrap_or("<table>");
    let bounds = match (&strategy, min, max, &from, &to) {
        (Strategy::Hash { .. }, _, _, _, _) => Bounds::None,
        (_, Some(lo), Some(hi), _, _) => Bounds::Range { min: lo, max: hi },
        (_, _, _, Some(f), Some(t)) => Bounds::Time { from: f.clone(), to: t.clone() },
        _ => {
            // The bounds come from the source, and this command deliberately
            // does not connect to it. Printing the probe is more useful than
            // guessing, and it is the same SQL the executor will run.
            match chunking::probe_sql(&strategy, table) {
                Ok(sql) => {
                    println!("run this against the source and pass the result back:\n\n  {sql}\n");
                    println!("  duckle-runner source plan {} --node {node} \\", path.display());
                    println!("      --min <lo> --max <hi> --nulls <nulls>");
                    return ExitCode::from(0);
                }
                Err(e) => {
                    eprintln!("duckle-runner source plan: {e}");
                    return ExitCode::from(2);
                }
            }
        }
    };

    let dialect = match component.as_str() {
        "src.oracle" => Dialect::Oracle,
        "src.mssql" => Dialect::MsSql,
        "src.mysql" => Dialect::MySql,
        "src.duckdb" | "src.ducklake" => Dialect::Duckdb,
        _ => Dialect::Postgres,
    };
    let concurrency = props
        .and_then(|p| p.get("chunking"))
        .and_then(|c| c.get("concurrency"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;
    // Only Oracle can pin one today, through the SCN path the parallel reader
    // already uses. Everything else says best-effort rather than implying a
    // consistency it cannot provide.
    let snapshot = match component.as_str() {
        "src.oracle" => Snapshot::Pinned { id: "SCN, pinned at read time".into() },
        _ => Snapshot::BestEffort,
    };

    let plan = match chunking::plan(&strategy, &bounds, nulls, concurrency, snapshot, dialect) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle-runner source plan: {e}");
            return ExitCode::from(1);
        }
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&plan).unwrap_or_default());
        return ExitCode::from(0);
    }
    println!("strategy    {} on {}", plan.strategy, plan.column);
    println!("connector   {component}");
    println!("chunks      {}", plan.chunks.len());
    println!("concurrency {}", plan.concurrency);
    println!(
        "snapshot    {}",
        match &plan.snapshot {
            Snapshot::Pinned { id } => format!("one consistent state ({id})"),
            Snapshot::BestEffort => "best effort - each chunk reads when it runs".to_string(),
            Snapshot::Watermark { column, at } => format!("cut off at {column} <= {at}"),
        }
    );
    println!("fallback    one query, as today, if chunking is removed");
    for note in &plan.notes {
        println!("\nnote        {note}");
    }
    println!("\npredicates:");
    for c in plan.chunks.iter().take(6) {
        println!("  {:<16} {}", c.key, c.predicate);
    }
    if plan.chunks.len() > 6 {
        println!("  ... and {} more", plan.chunks.len() - 6);
    }
    println!(
        "\nNOT YET EXECUTED: this prints the plan. Chunked reads are not wired into the\n\
         source path, so a run today still issues one query."
    );
    ExitCode::from(0)
}
