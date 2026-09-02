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
        // Not checked is not the same as checked and clean, and a report that
        // showed them the same would be the one thing worse than no report. A
        // node can be both - unvalidated AND carrying a hint - and dropping the
        // note in that case told a reader the node had been checked.
        let note = match (&a.note, a.validated) {
            (Some(why), _) => Some(format!("not checked - {why}")),
            (None, true) if a.diagnostics.is_empty() => {
                Some(format!("{} column(s)", a.columns.len()))
            }
            (None, false) => Some("not checked".to_string()),
            (None, true) => None,
        };
        if let Some(note) = note {
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
                for line in lines(a) {
                    println!("{line}");
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

/// One node's terminal lines.
///
/// Separate from `run` because the interesting case is easy to get wrong and
/// impossible to see: a node can be BOTH not-validated and carrying something
/// worth saying - a remote query whose dialect Duckle cannot check, but whose
/// quote is unclosed in every dialect. Printing only the note counted that hint
/// towards the exit code while showing nothing about it.
fn lines(a: &duckle_duckdb_engine::NodeAnalysis) -> Vec<String> {
    let mut out = Vec::new();
    match &a.note {
        Some(why) => out.push(format!("skip  {:<18} {why}", a.node_id)),
        None if a.diagnostics.is_empty() => out.push(format!(
            "ok    {:<18} {} column(s)",
            a.node_id,
            a.columns.len()
        )),
        None => {
            for d in &a.diagnostics {
                out.push(format!("FAIL  {:<18} {}", a.node_id, describe(a, d)));
            }
        }
    }
    // A node can be BOTH not-validated and worth saying something about.
    if a.note.is_some() {
        for d in &a.diagnostics {
            out.push(format!("hint  {:<18} {}", a.node_id, describe(a, d)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::lines;
    use duckle_duckdb_engine::sqldiag::Diagnostic;
    use duckle_duckdb_engine::NodeAnalysis;

    fn node(note: Option<&str>, diags: &[&str]) -> NodeAnalysis {
        NodeAnalysis {
            node_id: "pg".into(),
            component: "src.postgres".into(),
            dialect: match note {
                Some(_) => "remote".into(),
                None => "duckdb".into(),
            },
            columns: Vec::new(),
            diagnostics: diags
                .iter()
                .map(|m| Diagnostic {
                    kind: "hint".into(),
                    message: (*m).into(),
                    line: None,
                    column: None,
                    candidates: Vec::new(),
                })
                .collect(),
            validated: note.is_none(),
            note: note.map(str::to_string),
        }
    }

    #[test]
    fn a_skipped_node_still_shows_what_is_true_in_every_dialect() {
        // The regression this covers: hints on an unvalidated node counted
        // towards the exit code and were then never printed, so `sql check`
        // failed while saying only that it had not checked anything.
        let out = lines(&node(Some("remote dialect"), &["an unclosed single quote"]));
        assert!(out.iter().any(|l| l.starts_with("skip ")), "{out:?}");
        assert!(
            out.iter()
                .any(|l| l.starts_with("hint ") && l.contains("unclosed single quote")),
            "a hint on an unvalidated node must be visible: {out:?}"
        );
    }

    #[test]
    fn a_skipped_node_with_nothing_to_add_says_only_that() {
        let out = lines(&node(Some("remote dialect"), &[]));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("skip "), "{out:?}");
    }

    #[test]
    fn a_checked_node_reports_fail_not_hint() {
        // A node DuckDB did bind carries no "not checked" caveat, so its
        // problems are failures rather than things that might be fine.
        let out = lines(&node(None, &["column x does not exist"]));
        assert!(out.iter().all(|l| l.starts_with("FAIL ")), "{out:?}");
    }

    #[test]
    fn a_clean_node_is_ok() {
        assert!(lines(&node(None, &[]))[0].starts_with("ok    "));
    }
}

