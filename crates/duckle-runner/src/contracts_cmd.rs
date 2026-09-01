//! #302: `duckle-runner contracts check --base <rev>`.
//!
//! A pipeline can validate perfectly on its own and still break another one.
//! This compares each produced asset's declared schema against a git revision,
//! asks the catalog who reads that asset, and reports what would break.
//!
//! ## Why a git revision rather than a stored contract
//!
//! Because the previous schema already exists, in the commit the change is
//! being proposed against. A separately stored contract would be a second copy
//! of the same fact that has to be kept in step, and the first time it drifted
//! the check would confidently compare against something nobody had shipped.

use duckle_duckdb_engine::catalog;
use duckle_duckdb_engine::contracts::{self, Severity};
use serde_json::Value;
use std::process::ExitCode;

/// The declared schema of the node that writes an asset, if it declares one.
fn declared_schema(doc: &Value, node_id: &str) -> Option<Vec<duckle_duckdb_engine::Column>> {
    doc.get("nodes")?
        .as_array()?
        .iter()
        .find(|n| n.get("id").and_then(Value::as_str) == Some(node_id))
        .and_then(|n| n.get("data")?.get("schema").cloned())
        .and_then(|s| serde_json::from_value(s).ok())
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    let sub = it.next().unwrap_or_default();
    if sub != "check" {
        eprintln!(
            "usage: duckle-runner contracts check --base <rev> [--workspace DIR] \\
             [--format json|junit|sarif] [--strict]"
        );
        return ExitCode::from(2);
    }
    let mut base = String::new();
    let mut workspace = std::path::PathBuf::from(".");
    let mut format = String::new();
    let mut strict = false;
    while let Some(a) = it.next() {
        match a.as_str() {
            "--base" => base = it.next().unwrap_or_default(),
            "--workspace" => workspace = it.next().map(Into::into).unwrap_or(workspace),
            "--strict" => strict = true,
            "--format" => match it.next().as_deref() {
                Some(f @ ("json" | "junit" | "sarif")) => format = f.to_string(),
                _ => {
                    eprintln!("duckle-runner contracts: --format needs json, junit or sarif");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("duckle-runner contracts: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    if base.trim().is_empty() {
        eprintln!("duckle-runner contracts check: --base <rev> is required (e.g. --base main)");
        return ExitCode::from(2);
    }

    let base_docs = match catalog::documents_at_revision(&workspace, &base) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner contracts check: cannot read {base}: {e}");
            return ExitCode::from(2);
        }
    };
    let head_docs = catalog::documents(&workspace);
    let head = catalog::build_from_documents(&head_docs);

    // Parsed once: the reference search runs per change per consumer, and
    // re-parsing every consumer each time turns a fast check into a slow one.
    let head_typed: Vec<(String, duckle_duckdb_engine::PipelineDoc)> = head_docs
        .iter()
        .filter_map(|(id, v)| serde_json::from_value(v.clone()).ok().map(|d| (id.clone(), d)))
        .collect();

    let mut findings: Vec<contracts::Finding> = Vec::new();
    for touch in head.touches.iter().filter(|t| t.direction == catalog::Direction::Write) {
        let Some((_, head_doc)) = head_docs.iter().find(|(id, _)| *id == touch.pipeline_id) else {
            continue;
        };
        let Some((_, base_doc)) = base_docs.iter().find(|(id, _)| *id == touch.pipeline_id) else {
            // A brand new pipeline breaks nothing that existed before it.
            continue;
        };
        // No declared contract on one side or the other means saying nothing:
        // inferring a schema here and comparing it to a declared one would
        // manufacture changes nobody made.
        let (before, after) = match (
            declared_schema(base_doc, &touch.node_id),
            declared_schema(head_doc, &touch.node_id),
        ) {
            (Some(b), Some(a)) => (b, a),
            _ => continue,
        };
        // Whoever reads this asset, other than the pipeline that writes it.
        let consumers: Vec<(String, duckle_duckdb_engine::PipelineDoc)> = head
            .consumers(&touch.asset)
            .iter()
            .map(|c| c.pipeline_id.clone())
            .filter(|id| *id != touch.pipeline_id)
            .filter_map(|id| head_typed.iter().find(|(hid, _)| *hid == id).cloned())
            .collect();
        findings.extend(contracts::check_asset(&touch.asset, &before, &after, &consumers));
    }

    // Compatible changes are real information, but they are not what a gate is
    // for; keeping them out of the report keeps the breaking ones readable.
    findings.retain(|f| f.severity != Severity::Compatible);
    let breaking = findings.iter().filter(|f| f.severity == Severity::Breaking).count();
    let potential = findings.len() - breaking;

    if !format.is_empty() {
        let rf: Vec<crate::report::Finding> = findings
            .iter()
            .map(|f| crate::report::Finding {
                file: format!("{}", f.asset),
                node: Some(f.change.column().to_string()),
                rule: match f.severity {
                    Severity::Breaking => "contract-breaking",
                    _ => "contract-potentially-breaking",
                }
                .to_string(),
                message: describe(f),
                ok: false,
                line: None,
                column: None,
            })
            .collect();
        match format.as_str() {
            "junit" => println!("{}", crate::report::junit("contracts", &rf)),
            "sarif" => println!("{}", crate::report::sarif("contracts", &rf)),
            _ => println!(
                "{}",
                crate::report::json(
                    "contracts",
                    &rf,
                    serde_json::json!({ "base": base, "breaking": breaking, "potentiallyBreaking": potential, "changes": findings }),
                )
            ),
        }
    } else if findings.is_empty() {
        println!("no breaking changes against {base}");
    } else {
        for f in &findings {
            println!(
                "{:<22} {}",
                match f.severity {
                    Severity::Breaking => "BREAKING",
                    _ => "possibly breaking",
                },
                describe(f)
            );
        }
        println!("\n{breaking} breaking, {potential} possibly breaking, against {base}");
    }

    // Breaking fails the gate. `--strict` also fails on the uncertain ones, for
    // a repository that would rather stop and look than find out later.
    if breaking > 0 || (strict && potential > 0) {
        ExitCode::from(1)
    } else {
        ExitCode::from(0)
    }
}

fn describe(f: &contracts::Finding) -> String {
    use contracts::Change::*;
    let what = match &f.change {
        Added { column, .. } => format!("adds {column}"),
        Removed { column } => format!("removes {column}"),
        TypeChanged { column, from, to, .. } => format!("changes {column} from {from} to {to}"),
        NullabilityRelaxed { column } => format!("lets {column} be null"),
    };
    if f.affected.is_empty() {
        format!("{} {what}, and nothing in this workspace reads it", f.asset)
    } else {
        format!("{} {what}, read by {}", f.asset, f.affected.join(", "))
    }
}
