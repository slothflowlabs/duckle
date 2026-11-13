//! #312: `duckle-runner policy check` - pipelines against the environment's
//! policy, in the formats the other checks write.
//!
//! The check a run makes when it compiles, on the document the run would
//! check: saved connections resolved and sub-flows expanded first, so a host a
//! connection supplies, or a step inside a sub-flow, is checked as if it were
//! written in the pipeline. Every violation is reported, not the first.
//!
//! `mode: report` decides what a run does with a violation (log and allow);
//! this check reports violations either way, so a policy can be tried against
//! CI before it is enforced. And with no policy at all it exits 2: a gate that
//! passes because there was nothing to check is how a broken gate goes
//! unnoticed.

use crate::report::{self, Finding};
use duckle_duckdb_engine::format::strip_bom;
use duckle_duckdb_engine::{policy, PipelineDoc};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const USAGE: &str =
    "usage: duckle-runner policy check [--workspace DIR] [--format json|junit|sarif] [pipeline.json ...]";

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    if it.next().as_deref() != Some("check") {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let mut workspace = PathBuf::from(".");
    let mut format = String::new();
    let mut paths: Vec<PathBuf> = Vec::new();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => match it.next() {
                Some(w) => workspace = w.into(),
                None => {
                    eprintln!("duckle-runner policy: --workspace needs a directory");
                    return ExitCode::from(2);
                }
            },
            "--format" => match it.next().as_deref() {
                Some(f @ ("json" | "junit" | "sarif")) => format = f.to_string(),
                _ => {
                    eprintln!("duckle-runner policy: --format needs json, junit or sarif");
                    return ExitCode::from(2);
                }
            },
            other if other.starts_with('-') => {
                eprintln!("duckle-runner policy: unknown flag {other}\n{USAGE}");
                return ExitCode::from(2);
            }
            other => paths.push(other.into()),
        }
    }

    // The workspace layer of the policy lives here, and a Sub-flow node's
    // sub-flow is found here by id, as a run finds it.
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    let policy = match policy::load(Some(&workspace)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("duckle-runner policy check: {e}");
            return ExitCode::from(2);
        }
    };
    if policy.is_empty() {
        eprintln!(
            "duckle-runner policy check: no policy to check against: DUCKLE_POLICY_FILE is not set \
             and {} does not exist. A check with nothing to check against would pass everything.",
            workspace.join(".duckle").join("policy.yaml").display()
        );
        return ExitCode::from(2);
    }
    if paths.is_empty() {
        paths = duckle_duckdb_engine::catalog::document_paths(&workspace)
            .into_iter()
            .map(|(_, path, _)| path)
            .collect();
        if paths.is_empty() {
            eprintln!("duckle-runner policy check: no pipelines in {}", workspace.display());
            return ExitCode::from(2);
        }
    }

    let mut findings: Vec<Finding> = Vec::new();
    for path in &paths {
        let file = path.display().to_string();
        let doc = match as_a_run_checks_it(path, &workspace) {
            Ok(doc) => doc,
            Err(e) => {
                eprintln!("duckle-runner policy check: {file}: {e}");
                return ExitCode::from(2);
            }
        };
        let violations = policy::check(&policy, &doc);
        if violations.is_empty() {
            findings.push(Finding::pass(&file, "policy", "permitted".into()));
        }
        for v in violations {
            let mut finding = Finding::fail(&file, "policy", v.detail);
            finding.node = Some(v.node);
            findings.push(finding);
        }
    }

    let failed = findings.iter().filter(|f| !f.ok).count();
    let from = policy.sources.join(", ");
    match format.as_str() {
        "json" => println!(
            "{}",
            report::json("policy", &findings, serde_json::json!({ "policy": policy.sources, "mode": policy.mode }))
        ),
        "junit" => println!("{}", report::junit("policy", &findings)),
        "sarif" => println!("{}", report::sarif("policy", &findings)),
        _ => {
            for path in &paths {
                let file = path.display().to_string();
                let denied: Vec<&Finding> = findings.iter().filter(|f| f.file == file && !f.ok).collect();
                if denied.is_empty() {
                    println!("ok    {file}");
                    continue;
                }
                println!("DENY  {file}");
                for f in denied {
                    println!("        {:<16} {}", f.node.as_deref().unwrap_or(""), f.message);
                }
            }
            let breaking = paths
                .iter()
                .filter(|p| findings.iter().any(|f| !f.ok && f.file == p.display().to_string()))
                .count();
            println!(
                "\n{breaking} of {} pipeline(s) break the policy from {from} ({failed} violation(s))",
                paths.len()
            );
        }
    }
    ExitCode::from(if failed > 0 { 1 } else { 0 })
}

/// The pipeline as a run checks it: saved connections resolved, sub-flows
/// expanded. Connections are best effort, as validate's are: one that cannot
/// be resolved leaves the node as written, and the run itself refuses it.
fn as_a_run_checks_it(path: &Path, workspace: &Path) -> Result<PipelineDoc, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
    let mut doc: PipelineDoc = serde_json::from_str(strip_bom(&text)).map_err(|e| format!("parse: {e}"))?;
    let _ = duckle_secrets::resolve_connection_refs(workspace, &mut doc.nodes);
    let expanded = duckle_duckdb_engine::subflow::expand(&doc, &Default::default()).map_err(|e| e.to_string())?;
    Ok(expanded.unwrap_or(doc))
}
