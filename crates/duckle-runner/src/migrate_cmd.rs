//! #299: `duckle-runner migrate`.
//!
//! Reads every pipeline in a workspace, works out what would change, and only
//! writes when told to. The default is to report, because a command that
//! rewrites a Git-managed production workspace on sight is one people run once
//! and then never trust again.

use duckle_duckdb_engine::format;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

struct Plan {
    path: PathBuf,
    changes: Vec<String>,
    text: String,
}

/// Pipeline files in the workspace.
///
/// `catalog::documents` walks by the same rules but returns parsed documents
/// keyed by id, and this needs the PATH to write back to. The skip list and the
/// "has a nodes array" test below are kept identical to it deliberately: a file
/// one of them counts and the other does not is a file that stays on the old
/// format forever.
fn pipeline_files(workspace: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let mut stack = vec![workspace.to_path_buf()];
    const SKIP: [&str; 6] = ["runs", "logs", "node_modules", ".duckle", ".git", "target"];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                if !SKIP.contains(&name.as_str()) {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn render(doc: &serde_json::Value) -> String {
    serde_json::to_string_pretty(doc).unwrap_or_default() + "
"
}

/// The new text for a file, preferring a one-line edit over a re-serialize.
///
/// Re-serializing expands every object the author had written inline, so
/// stamping a version turned a one-line change into a 60-line diff. In a
/// Git-managed workspace - the case #299 is actually about - that buries the
/// change under formatting nobody asked for, and the review is worthless.
///
/// The overwhelmingly common migration is the stamp alone, and that one can be
/// done as a text insertion after the opening brace. Anything else falls back
/// to re-serializing, which is correct but noisy.
///
/// The insertion is used only if re-parsing it yields exactly the document the
/// migration produced. That check is what makes string surgery on JSON
/// acceptable here: a clever edit that produced something subtly different
/// would be far worse than a noisy diff.
fn rewrite(original: &str, migrated: &serde_json::Value, changes: &[String]) -> String {
    let only_a_stamp = changes.len() == 1 && changes[0].starts_with("stamped formatVersion");
    if only_a_stamp {
        if let Some(brace) = original.find('{') {
            let candidate = format!(
                "{}{{
  \"formatVersion\": {},{}",
                &original[..brace],
                format::WRITABLE,
                &original[brace + 1..]
            );
            if serde_json::from_str::<serde_json::Value>(&candidate).ok().as_ref()
                == Some(migrated)
            {
                return candidate;
            }
        }
    }
    render(migrated)
}

fn survey(workspace: &Path) -> Result<(Vec<Plan>, Vec<String>), String> {
    // A path that is not there is not a workspace with nothing to do. Without
    // this the walk finds no files, reports no plans, and the caller prints
    // "every pipeline is already at format version 1" with exit 0 - a clean
    // bill of health for a directory that does not exist.
    if !workspace.is_dir() {
        return Err(format!("{} is not a directory", workspace.display()));
    }
    let mut plans = Vec::new();
    let mut refused = Vec::new();
    for path in pipeline_files(workspace) {
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        // The same test the catalog uses for "is this a pipeline".
        if doc.get("nodes").and_then(|n| n.as_array()).is_none() {
            continue;
        }
        match format::migrate(&doc) {
            Ok(m) if m.is_empty() => {}
            Ok(m) => {
                let text = rewrite(&text, &m.doc, &m.changes);
                plans.push(Plan { path, changes: m.changes, text })
            }
            Err(e) => refused.push(format!("{}: {e}", path.display())),
        }
    }
    Ok((plans, refused))
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    let mut workspace = PathBuf::from(".");
    let mut write = false;
    let mut json = false;
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(Into::into).unwrap_or(workspace),
            // --check and --dry-run are the same thing and both are the
            // default; they exist because CI scripts write one or the other.
            "--check" | "--dry-run" => write = false,
            "--write" => write = true,
            "--json" => json = true,
            other => {
                eprintln!("duckle-runner migrate: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }

    let (plans, refused) = match survey(&workspace) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("duckle-runner migrate: {e}");
            return ExitCode::from(2);
        }
    };

    let mut written: Vec<String> = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    if write {
        for plan in &plans {
            // The original beside the file, not in a temp directory: a workspace
            // under version control shows it in `git status`, and a workspace
            // that is not shows it in the folder the person is already looking
            // at. Written BEFORE the new content, so a crash between the two
            // leaves the original in two places rather than none.
            let backup = plan.path.with_extension("json.bak");
            let original = std::fs::read(&plan.path);
            let result = original
                .and_then(|bytes| std::fs::write(&backup, bytes))
                .and_then(|_| std::fs::write(&plan.path, plan.text.as_bytes()));
            match result {
                Ok(()) => written.push(plan.path.display().to_string()),
                Err(e) => failed.push(format!("{}: {e}", plan.path.display())),
            }
        }
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "writable": format::WRITABLE,
                "maxReadable": format::MAX_READABLE,
                "applied": write,
                "pending": plans.iter().map(|p| serde_json::json!({
                    "file": p.path.display().to_string(),
                    "changes": p.changes,
                })).collect::<Vec<_>>(),
                "refused": refused,
                "written": written,
                "failed": failed,
            }))
            .unwrap_or_default()
        );
    } else {
        if plans.is_empty() && refused.is_empty() {
            println!("every pipeline is already at format version {}", format::WRITABLE);
        }
        for plan in &plans {
            println!("{}", plan.path.display());
            for change in &plan.changes {
                println!("    {change}");
            }
        }
        if !refused.is_empty() {
            println!("\nwritten by a newer Duckle, left alone:");
            for r in &refused {
                println!("    {r}");
            }
        }
        if write {
            println!("\n{} file(s) written, originals kept as .json.bak", written.len());
        } else if !plans.is_empty() {
            println!("\nnothing written. Pass --write to apply.");
        }
        for f in &failed {
            eprintln!("FAILED {f}");
        }
    }

    // A file this build cannot read is a hard stop whether or not anything was
    // written: continuing to run a workspace half of which is unreadable is the
    // situation the version marker exists to prevent.
    if !failed.is_empty() || !refused.is_empty() {
        return ExitCode::from(1);
    }
    ExitCode::from(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace(files: &[(&str, &str)]) -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "duckle-migrate-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("pipelines")).unwrap();
        for (name, body) in files {
            std::fs::write(base.join("pipelines").join(name), body).unwrap();
        }
        base
    }

    const OLD: &str = r#"{"name":"p","nodes":[{"id":"out","type":"sink","position":{"x":0,"y":0},
        "data":{"label":"Out","componentId":"snk.csv","properties":{"path":"o.csv","hasHeader":false}}}],
        "edges":[]}"#;

    #[test]
    fn a_workspace_that_is_not_there_is_an_error_not_an_all_clear() {
        let missing = std::env::temp_dir().join("duckle-migrate-no-such-workspace-xyz");
        let _ = std::fs::remove_dir_all(&missing);
        assert!(survey(&missing).is_err(), "a missing workspace reported a clean bill of health");
    }

    #[test]
    fn a_survey_reports_without_touching_anything() {
        let ws = workspace(&[("p.json", OLD)]);
        let before = std::fs::read_to_string(ws.join("pipelines/p.json")).unwrap();
        let (plans, refused) = survey(&ws).unwrap();
        assert_eq!(plans.len(), 1);
        assert!(refused.is_empty());
        assert!(plans[0].changes.iter().any(|c| c.contains("renamed")), "{:?}", plans[0].changes);
        assert_eq!(
            std::fs::read_to_string(ws.join("pipelines/p.json")).unwrap(),
            before,
            "a survey must not write"
        );
    }

    #[test]
    fn a_migrated_workspace_surveys_clean_the_second_time() {
        let ws = workspace(&[("p.json", OLD)]);
        let (plans, _) = survey(&ws).unwrap();
        std::fs::write(&plans[0].path, &plans[0].text).unwrap();
        let (again, _) = survey(&ws).unwrap();
        assert!(again.is_empty(), "not idempotent: {:?}", again[0].changes);
    }

    #[test]
    fn stamping_a_version_is_a_one_line_diff() {
        // The whole point: an author's inline objects survive, so a review sees
        // the change rather than a reformat of the file around it.
        let compact = concat!(
            "{
",
            "  \"name\": \"p\",
",
            "  \"nodes\": [
",
            "    { \"id\": \"a\", \"type\": \"source\", \"position\": { \"x\": 0, \"y\": 0 },
",
            "      \"data\": { \"label\": \"A\", \"componentId\": \"src.csv\",
",
            "                 \"properties\": { \"path\": \"a.csv\" } } }
",
            "  ],
",
            "  \"edges\": []
",
            "}
"
        );
        let ws = workspace(&[("p.json", compact)]);
        let (plans, _) = survey(&ws).unwrap();
        assert_eq!(plans.len(), 1);
        let added: Vec<&str> = plans[0]
            .text
            .lines()
            .filter(|l| !compact.lines().any(|o| o == *l))
            .collect();
        assert_eq!(added, vec!["  \"formatVersion\": 1,"], "not a one-line diff: {added:?}");
        let reparsed: serde_json::Value = serde_json::from_str(&plans[0].text).unwrap();
        assert_eq!(reparsed["formatVersion"], 1);
        assert_eq!(reparsed["name"], "p");
    }

    #[test]
    fn a_rename_falls_back_to_re_serialising() {
        // Correctness first: a change the text edit cannot express must not be
        // attempted with one.
        let ws = workspace(&[("p.json", OLD)]);
        let (plans, _) = survey(&ws).unwrap();
        let reparsed: serde_json::Value = serde_json::from_str(&plans[0].text).unwrap();
        assert_eq!(reparsed["nodes"][0]["data"]["properties"]["writeHeader"], false);
        assert_eq!(reparsed["formatVersion"], 1);
    }

    #[test]
    fn a_file_from_a_newer_build_is_refused_not_rewritten() {
        let ws = workspace(&[(
            "future.json",
            r#"{"formatVersion":99,"nodes":[],"edges":[]}"#,
        )]);
        let (plans, refused) = survey(&ws).unwrap();
        assert!(plans.is_empty(), "must not plan to rewrite it");
        assert_eq!(refused.len(), 1);
        assert!(refused[0].contains("Upgrade Duckle"), "{refused:?}");
    }

    #[test]
    fn a_json_file_that_is_not_a_pipeline_is_left_alone() {
        let ws = workspace(&[("owners.json", r#"{"rules":[{"glob":"*","owner":"a"}]}"#)]);
        let (plans, refused) = survey(&ws).unwrap();
        assert!(plans.is_empty() && refused.is_empty());
    }

    #[test]
    fn unreadable_json_does_not_stop_the_survey() {
        let ws = workspace(&[("broken.json", "{ not json"), ("p.json", OLD)]);
        let (plans, _) = survey(&ws).unwrap();
        assert_eq!(plans.len(), 1, "the readable one is still planned");
    }
}
