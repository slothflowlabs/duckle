//! #308: `duckle-runner affected --base <rev>`.
//!
//! Answers, for CI and for release promotion: what changed, which pipelines are
//! affected, and why each one is in the list. The walk itself lives in the
//! engine ([`duckle_duckdb_engine::affected`]) so it can be tested without a
//! repository; this module is the part that needs one - reading revisions,
//! diffing the shared inputs, printing.

use duckle_duckdb_engine::affected::{self, Reason, SharedChange};
use duckle_duckdb_engine::catalog;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Files that are not pipelines but do change what a pipeline compiles to.
///
/// A deliberately short, named list rather than "every other changed file".
/// Treating an unknown file as a compile input would select the whole workspace
/// whenever anyone edited a README; treating it as harmless would miss the one
/// that mattered. Naming them means the list can be wrong in a way someone can
/// see and correct - and whatever else changed is reported under `unclassified`
/// rather than quietly assumed to be one or the other.
const SHARED_INPUTS: [&str; 2] = ["plans.json", "duckle.json"];

fn git(workspace: &Path, args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .output()
        .map_err(|e| format!("git: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// One file's content at a revision, or at the worktree when `rev` is empty.
fn read_at(workspace: &Path, rev: &str, rel: &str) -> Option<String> {
    if rev.is_empty() {
        return std::fs::read_to_string(workspace.join(rel)).ok();
    }
    git(workspace, &["show", &format!("{rev}:./{rel}")]).ok()
}

/// A context's `key -> hash(value)`.
///
/// The hash is the point: a context holds credentials, and #308 is explicit
/// that secret values must not be read out of git. Comparing hashes answers
/// "did this key change" without the value ever reaching an output, a log or
/// this function's return type. It is not a cryptographic claim - only a
/// same-or-different test on values that never leave the stack.
fn context_digest(text: &str) -> BTreeMap<String, u64> {
    use std::hash::{Hash, Hasher};
    let mut out = BTreeMap::new();
    let Ok(doc) = serde_json::from_str::<serde_json::Value>(text) else { return out };
    let Some(vars) = doc.get("variables").and_then(|v| v.as_array()) else { return out };
    for v in vars {
        let Some(key) = v.get("key").and_then(|k| k.as_str()) else { continue };
        let mut h = std::collections::hash_map::DefaultHasher::new();
        v.get("value").map(|x| x.to_string()).unwrap_or_default().hash(&mut h);
        out.insert(key.to_string(), h.finish());
    }
    out
}

fn shared_changes(workspace: &Path, base: &str, head: &str, changed: &[String]) -> Vec<SharedChange> {
    let mut out: Vec<SharedChange> = Vec::new();
    for rel in changed {
        let name = Path::new(rel).file_name().and_then(|n| n.to_str()).unwrap_or("");
        if SHARED_INPUTS.contains(&name) {
            out.push(SharedChange { input: rel.clone(), keys: Vec::new() });
            continue;
        }
        if !rel.starts_with("contexts/") || !rel.ends_with(".json") {
            continue;
        }
        let before = read_at(workspace, base, rel).map(|t| context_digest(&t)).unwrap_or_default();
        let after = read_at(workspace, head, rel).map(|t| context_digest(&t)).unwrap_or_default();
        let mut keys: Vec<String> = before
            .keys()
            .chain(after.keys())
            .filter(|k| before.get(*k) != after.get(*k))
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        if !keys.is_empty() {
            out.push(SharedChange { input: rel.clone(), keys });
        }
    }
    out
}

/// Everything that changed between the two sides, as repository-relative paths.
fn changed_files(workspace: &Path, base: &str, head: &str) -> Result<Vec<String>, String> {
    let listed = if head.is_empty() {
        git(workspace, &["diff", "--name-only", base, "--", "."])?
    } else {
        git(workspace, &["diff", "--name-only", base, head, "--", "."])?
    };
    let mut files: Vec<String> =
        listed.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_string).collect();
    // Against the working tree, a file that was never added is still a change
    // someone made. `git diff` only knows about tracked files, so a brand new
    // context or plan would otherwise be invisible - and invisible is the one
    // answer this command must not give.
    if head.is_empty() {
        if let Ok(untracked) = git(workspace, &["ls-files", "--others", "--exclude-standard"]) {
            files.extend(untracked.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_string));
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    let mut base = String::new();
    let mut head = String::new();
    let mut workspace = PathBuf::from(".");
    let mut json = false;
    let mut include_uncertain = false;
    while let Some(a) = it.next() {
        match a.as_str() {
            "--base" => base = it.next().unwrap_or_default(),
            "--head" => head = it.next().unwrap_or_default(),
            "--workspace" => workspace = it.next().map(Into::into).unwrap_or(workspace),
            "--include-uncertain" => include_uncertain = true,
            "--json" => json = true,
            "--format" => match it.next().as_deref() {
                Some("json") => json = true,
                _ => {
                    eprintln!("duckle-runner affected: --format only takes json");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("duckle-runner affected: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    if base.trim().is_empty() {
        eprintln!(
            "usage: duckle-runner affected --base <rev> [--head <rev>] [--workspace DIR]\n\
             \x20                          [--include-uncertain] [--json]\n\n\
             Without --head the comparison is against the working tree."
        );
        return ExitCode::from(2);
    }

    match select(&workspace, &base, &head, include_uncertain) {
        Ok((selection, unclassified)) => {
            if json {
                let mut doc = serde_json::to_value(&selection).unwrap_or_default();
                doc["unclassified"] = serde_json::json!(unclassified);
                println!("{}", serde_json::to_string_pretty(&doc).unwrap_or_default());
            } else {
                print_text(&selection, &unclassified);
            }
            ExitCode::from(0)
        }
        Err(e) => {
            eprintln!("duckle-runner affected: {e}");
            ExitCode::from(2)
        }
    }
}

/// The selection, plus the changed files this command did not model.
///
/// Shared with `validate --affected`, so the two cannot drift into disagreeing
/// about which pipelines a change reaches.
pub fn select(
    workspace: &Path,
    base: &str,
    head: &str,
    include_uncertain: bool,
) -> Result<(affected::Selection, Vec<String>), String> {
    let base_docs = catalog::documents_at_revision(workspace, base)
        .map_err(|e| format!("cannot read {base}: {e}"))?;
    let head_docs = if head.is_empty() {
        catalog::documents(workspace)
    } else {
        catalog::documents_at_revision(workspace, head)
            .map_err(|e| format!("cannot read {head}: {e}"))?
    };
    let graph = catalog::build_from_documents(&head_docs);
    let changed = changed_files(workspace, base, head)?;
    let shared = shared_changes(workspace, base, head, &changed);

    // What changed, was not a pipeline, and is not a shared input this command
    // knows how to model. Reported rather than dropped: a file nobody accounted
    // for is exactly the thing that makes a selection quietly incomplete.
    let modelled: Vec<&str> = shared.iter().map(|s| s.input.as_str()).collect();
    let unclassified: Vec<String> = changed
        .iter()
        .filter(|rel| !modelled.contains(&rel.as_str()))
        .filter(|rel| {
            let stem = Path::new(rel)
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            !base_docs.iter().any(|(id, _)| *id == stem)
                && !head_docs.iter().any(|(id, _)| *id == stem)
        })
        .cloned()
        .collect();

    let mut selection =
        affected::select(&base_docs, &head_docs, &graph, &shared, include_uncertain);
    selection.base = base.to_string();
    selection.head = if head.is_empty() { "working tree".into() } else { head.to_string() };
    Ok((selection, unclassified))
}

fn describe(reason: &Reason) -> String {
    match reason {
        Reason::Added => "added".into(),
        Reason::Changed => "changed".into(),
        Reason::Downstream { path } => path.join(" -> "),
        Reason::Child { path, node } => format!("{} (at {node})", path.join(" -> ")),
        Reason::Shared { input, key: Some(k) } => format!("{input}: {k} changed"),
        Reason::Shared { input, key: None } => format!("{input} changed"),
        Reason::Uncertain { node, why } => format!("uncertain: {node} - {why}"),
    }
}

fn print_text(s: &affected::Selection, unclassified: &[String]) {
    println!("affected against {} (head: {})\n", s.base, s.head);
    if s.selected.is_empty() {
        println!("  nothing");
    }
    for entry in &s.selected {
        for (i, reason) in entry.reasons.iter().enumerate() {
            let name = if i == 0 { entry.pipeline.as_str() } else { "" };
            println!("  {:<28} {}", name, describe(reason));
        }
    }
    if !s.removed.is_empty() {
        println!("\ndeleted (cannot be run): {}", s.removed.join(", "));
    }
    if !s.order.is_empty() {
        println!("\nrun order: {}", s.order.join(", "));
    }
    if !s.cycles.is_empty() {
        println!("no order possible, these depend on each other: {}", s.cycles.join(", "));
    }
    if !s.uncertain.is_empty() {
        let note = if s.selected.iter().any(|x| x.uncertain) {
            "included"
        } else {
            "not included; pass --include-uncertain"
        };
        println!("\nunresolved dependencies ({note}):");
        for u in &s.uncertain {
            println!("  {:<28} {} - {}", u.pipeline, u.node, u.why);
        }
    }
    if !unclassified.is_empty() {
        println!("\nchanged, and not modelled as an input to any pipeline:");
        for f in unclassified {
            println!("  {f}");
        }
    }
    println!(
        "\n{} selected, {} deleted, {} unresolved",
        s.selected.len(),
        s.removed.len(),
        s.uncertain.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_changed_context_value_is_detected_without_the_value_being_read_out() {
        let before = r#"{"variables":[{"key":"REGION","value":"eu"},
                                      {"key":"TOKEN","value":"hunter2"}]}"#;
        let after = r#"{"variables":[{"key":"REGION","value":"eu"},
                                     {"key":"TOKEN","value":"correct-horse"}]}"#;
        let b = context_digest(before);
        let a = context_digest(after);
        assert_eq!(b["REGION"], a["REGION"], "unchanged key must compare equal");
        assert_ne!(b["TOKEN"], a["TOKEN"], "changed key must compare different");
        // The whole point: the digest is the only thing that leaves, and it
        // cannot be turned back into the secret.
        let rendered = format!("{a:?}");
        assert!(!rendered.contains("correct-horse"), "a secret value escaped: {rendered}");
        assert!(!rendered.contains("hunter2"));
    }

    #[test]
    fn an_added_or_removed_context_key_counts_as_changed() {
        let b = context_digest(r#"{"variables":[{"key":"A","value":"1"}]}"#);
        let a = context_digest(r#"{"variables":[{"key":"B","value":"1"}]}"#);
        let differing: Vec<&String> =
            b.keys().chain(a.keys()).filter(|k| b.get(*k) != a.get(*k)).collect();
        assert_eq!(differing, vec!["A", "B"]);
    }

    #[test]
    fn a_context_that_is_not_json_yields_no_keys_rather_than_panicking() {
        assert!(context_digest("not json at all").is_empty());
        assert!(context_digest(r#"{"variables":"wrong shape"}"#).is_empty());
    }

    #[test]
    fn a_reason_renders_as_the_chain_that_reached_it() {
        assert_eq!(
            describe(&Reason::Downstream {
                path: vec!["normalize".into(), "canonical".into(), "serving".into()]
            }),
            "normalize -> canonical -> serving"
        );
        assert_eq!(
            describe(&Reason::Child {
                path: vec!["child".into(), "parent".into()],
                node: "fe".into()
            }),
            "child -> parent (at fe)"
        );
    }
}
