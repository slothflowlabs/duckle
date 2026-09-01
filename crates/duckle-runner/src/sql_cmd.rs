//! #314: `duckle-runner sql check <pipeline> [--node X]`.
//!
//! Binds every SQL-bearing node against the columns its upstreams produce and
//! reports what DuckDB said, before anything runs. The same
//! `analyze_pipeline_sql` the editor and MCP call, so the three cannot come to
//! disagree about whether a pipeline is sound.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use std::path::PathBuf;
use std::process::ExitCode;

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    if it.next().as_deref() != Some("check") {
        eprintln!(
            "usage: duckle-runner sql check <pipeline.json> [--node ID] [--duckdb PATH]\n\
             \x20                          [--format json|junit|sarif]"
        );
        return ExitCode::from(2);
    }
    let mut path: Option<PathBuf> = None;
    let mut node: Option<String> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut format = String::new();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--node" => node = it.next(),
            "--duckdb" => duckdb = it.next().map(PathBuf::from),
            "--format" => match it.next().as_deref() {
                Some(f @ ("json" | "junit" | "sarif")) => format = f.to_string(),
                _ => {
                    eprintln!("duckle-runner sql check: --format needs json, junit or sarif");
                    return ExitCode::from(2);
                }
            },
            other if other.starts_with('-') => {
                eprintln!("duckle-runner sql check: unknown flag {other}");
                return ExitCode::from(2);
            }
            other => path = Some(PathBuf::from(other)),
        }
    }
    let Some(path) = path else {
        eprintln!("duckle-runner sql check: needs a pipeline file");
        return ExitCode::from(2);
    };

    let doc: PipelineDoc = match std::fs::read_to_string(&path)
        .map_err(|e| format!("read: {e}"))
        .and_then(|t| serde_json::from_str(&t).map_err(|e| format!("parse: {e}")))
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner sql check: {}: {e}", path.display());
            return ExitCode::from(2);
        }
    };

    let binary = duckdb
        .or_else(|| std::env::var("DUCKLE_DUCKDB_BIN").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("duckdb"));
    let engine = DuckdbEngine::new(binary);
    let analyses = match engine.analyze_pipeline_sql(&doc) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("duckle-runner sql check: {e}");
            return ExitCode::from(2);
        }
    };
    let analyses: Vec<_> = match &node {
        Some(id) => analyses.into_iter().filter(|a| a.node_id == *id).collect(),
        None => analyses,
    };
    if analyses.is_empty() {
        eprintln!("duckle-runner sql check: no such node");
        return ExitCode::from(2);
    }

    let label = path.display().to_string();
    let mut findings: Vec<crate::report::Finding> = Vec::new();
    let mut problems = 0usize;
    for a in &analyses {
        for d in &a.diagnostics {
            problems += 1;
            let mut f = crate::report::Finding::fail(&label, &d.kind, describe(a, d));
            f.node = Some(a.node_id.clone());
            f.line = d.line;
            f.column = d.column;
            findings.push(f);
        }
        if a.diagnostics.is_empty() {
            let note = match (&a.note, a.validated) {
                // Not checked is not the same as checked and clean, and a
                // report that showed them the same would be the one thing
                // worse than no report.
                (Some(why), _) => format!("not checked - {why}"),
                (None, true) => format!("{} column(s)", a.columns.len()),
                (None, false) => "not checked".to_string(),
            };
            let mut f = crate::report::Finding::pass(&label, "bind", note);
            f.node = Some(a.node_id.clone());
            findings.push(f);
        }
    }

    match format.as_str() {
        "junit" => println!("{}", crate::report::junit("sql-check", &findings)),
        "sarif" => println!("{}", crate::report::sarif("sql-check", &findings)),
        "json" => println!(
            "{}",
            crate::report::json(
                "sql-check",
                &findings,
                serde_json::json!({ "nodes": analyses })
            )
        ),
        _ => {
            for a in &analyses {
                match (&a.note, a.diagnostics.is_empty()) {
                    (Some(why), _) => println!("skip  {:<18} {why}", a.node_id),
                    (None, true) => println!(
                        "ok    {:<18} {} column(s)",
                        a.node_id,
                        a.columns.len()
                    ),
                    (None, false) => {
                        for d in &a.diagnostics {
                            println!("FAIL  {:<18} {}", a.node_id, describe(a, d));
                        }
                    }
                }
            }
            println!("\n{} node(s) checked, {problems} problem(s)", analyses.len());
        }
    }
    match problems {
        0 => ExitCode::from(0),
        _ => ExitCode::from(1),
    }
}

fn describe(a: &duckle_duckdb_engine::NodeAnalysis, d: &duckle_duckdb_engine::sqldiag::Diagnostic) -> String {
    let mut out = String::new();
    if let (Some(line), Some(column)) = (d.line, d.column) {
        out.push_str(&format!("{line}:{column}: "));
    }
    out.push_str(&d.message);
    if !d.candidates.is_empty() {
        out.push_str(&format!("  (did you mean {}?)", d.candidates.join(", ")));
    }
    let _ = a;
    out
}
