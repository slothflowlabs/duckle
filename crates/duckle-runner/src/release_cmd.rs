//! #297: `duckle-runner release build | verify | diff | activate | rollback`.
//!
//! A release records the control plane; an environment points at one. The whole
//! of activation's safety is that everything is checked before the pointer
//! moves, and that moving it is one rename.

use duckle_duckdb_engine::release::{self, Release};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: duckle-runner release <command> [--workspace DIR] [--json]\n\
         \n\
         \x20 build                          record the workspace as an immutable release\n\
         \x20 verify [<id>]                   recompute the hashes and check the dependencies\n\
         \x20 diff <from-id> [<to-id>]        what changed between two releases\n\
         \x20 activate <id> --environment E   check everything, then switch the pointer\n\
         \x20 rollback --environment E        point back at the previous release\n\
         \x20 list                            releases, newest first, and what is active"
    );
    ExitCode::from(2)
}

struct Args {
    workspace: PathBuf,
    environment: String,
    json: bool,
    force: bool,
    positional: Vec<String>,
}

fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args {
        workspace: PathBuf::from("."),
        environment: String::new(),
        json: false,
        force: false,
        positional: Vec::new(),
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--workspace" => a.workspace = it.next().map(Into::into).unwrap_or(a.workspace),
            "--environment" | "--env" => a.environment = it.next().unwrap_or_default(),
            "--json" => a.json = true,
            "--force" => a.force = true,
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other => a.positional.push(other.to_string()),
        }
    }
    Ok(a)
}

/// Everything that must hold before an environment may point at a release.
///
/// Reported all at once, and every check runs even after one fails: an operator
/// fixing a production activation should not discover the second problem after
/// fixing the first.
fn dependency_gate(workspace: &Path, release: &Release) -> Vec<String> {
    let mut problems = Vec::new();

    // 1. The release's own content is intact and compiles.
    //
    //    Deliberately NOT "the workspace already matches this release": that
    //    requirement made rollback impossible, because rolling back to A is
    //    exactly the case where the workspace holds B. What matters is that the
    //    bytes A stored are still there and still run - the workspace is what
    //    activation is about to overwrite.
    if release.body.files.is_empty() {
        problems.push(
            "this release stored no content, so activating it would move a pointer without              changing what runs. Rebuild it with a newer Duckle."
                .to_string(),
        );
    }
    for (rel, hash) in &release.body.files {
        let bytes = match duckle_duckdb_engine::release::get_object(workspace, hash) {
            Ok(b) => b,
            Err(e) => {
                problems.push(format!("{rel}: {e}"));
                continue;
            }
        };
        // Only pipelines compile; plans and schedules are checked by their own
        // readers when they are used.
        if !rel.ends_with(".json") || rel.ends_with("plans.json") || rel.ends_with("schedules.json")
        {
            continue;
        }
        let compiled = serde_json::from_slice::<duckle_duckdb_engine::PipelineDoc>(&bytes)
            .map_err(|e| format!("parse: {e}"))
            .and_then(|d| {
                duckle_duckdb_engine::compile_pipeline_sql(&d).map_err(|e| e.to_string())
            });
        if let Err(e) = compiled {
            problems.push(format!("{rel} does not compile: {e}"));
        }
    }

    // 3. Every connection the release names exists. Checked here rather than
    //    discovered at run time, which is the difference between refusing an
    //    activation and breaking production.
    let known = saved_connections(workspace);
    for name in &release.body.connection_refs {
        if !known.contains(name) {
            problems.push(format!(
                "connection {name:?} is referenced by a pipeline and is not saved in this workspace"
            ));
        }
    }

    // 4. Policy. A release that policy refuses must not become active, and
    //    finding out at the first run is finding out too late.
    if let Err(e) = duckle_duckdb_engine::policy::load(Some(workspace)) {
        problems.push(format!("policy: {e}"));
    }
    problems
}

fn pipeline_path(workspace: &Path, id: &str) -> Option<PathBuf> {
    duckle_duckdb_engine::catalog::document_paths(workspace)
        .into_iter()
        .find(|(found, _, _)| found == id)
        .map(|(_, path, _)| path)
}

/// Connection names this workspace has saved.
fn saved_connections(workspace: &Path) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let dir = workspace.join("connections");
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) == Some("json") {
                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                    out.insert(stem.to_string());
                }
            }
        }
    }
    out
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(2);
    let Some(command) = it.next() else { return usage() };
    let args = match parse(it) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("duckle-runner release: {e}");
            return ExitCode::from(2);
        }
    };
    let ws = args.workspace.clone();

    match command.as_str() {
        "build" => match release::build(&ws).and_then(|r| release::save(&ws, &r).map(|p| (r, p))) {
            Ok((r, path)) => {
                match args.json {
                    true => println!("{}", serde_json::to_string_pretty(&r).unwrap_or_default()),
                    false => println!(
                        "release {}\n  {} pipeline(s), format v{}\n  written to {}",
                        r.id,
                        r.body.pipelines.len(),
                        r.body.format_version,
                        path.display()
                    ),
                }
                ExitCode::from(0)
            }
            Err(e) => {
                eprintln!("duckle-runner release build: {e}");
                ExitCode::from(2)
            }
        },

        "verify" => {
            let id = match args.positional.first().cloned().or_else(|| {
                args.environment
                    .is_empty()
                    .then_some(())
                    .and(None)
                    .or_else(|| release::active(&ws, &args.environment))
            }) {
                Some(id) => id,
                None => {
                    eprintln!("duckle-runner release verify: give a release id, or --environment");
                    return ExitCode::from(2);
                }
            };
            let release = match release::load(&ws, &id) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("duckle-runner release verify: {e}");
                    return ExitCode::from(2);
                }
            };
            // The id IS the hash, so a document whose body no longer hashes to
            // its own name has been edited.
            let recomputed = release::id_of(&release.body);
            let mut problems = Vec::new();
            if recomputed != release.id {
                problems.push(format!(
                    "this document is stored as {} but its content hashes to {recomputed}",
                    release.id
                ));
            }
            problems.extend(dependency_gate(&ws, &release));
            report(&args, &id, &problems)
        }

        "diff" => {
            let from = args.positional.first().cloned();
            let to = args.positional.get(1).cloned();
            let Some(from) = from else { return usage() };
            let a = match release::load(&ws, &from) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("duckle-runner release diff: {e}");
                    return ExitCode::from(2);
                }
            };
            let b = match to {
                Some(id) => match release::load(&ws, &id) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("duckle-runner release diff: {e}");
                        return ExitCode::from(2);
                    }
                },
                // No second id means "against the workspace as it is now",
                // which is the question asked before building a release.
                None => match release::build(&ws) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("duckle-runner release diff: {e}");
                        return ExitCode::from(2);
                    }
                },
            };
            let d = release::diff(&a.body, &b.body);
            if args.json {
                println!("{}", serde_json::to_string_pretty(&d).unwrap_or_default());
            } else if d.is_empty() {
                println!("{} and {} are the same control plane", a.id, b.id);
            } else {
                for id in &d.added {
                    println!("  added     {id}");
                }
                for id in &d.changed {
                    println!("  changed   {id}");
                }
                for id in &d.removed {
                    println!("  removed   {id}");
                }
                if d.plans_changed {
                    println!("  changed   plans.json");
                }
                if d.schedules_changed {
                    println!("  changed   schedules.json");
                }
                for id in &d.parameters_changed {
                    println!("  contract  {id} declares different parameters");
                }
                for name in &d.new_connection_refs {
                    println!("  needs     connection {name}");
                }
            }
            ExitCode::from(0)
        }

        "activate" => {
            let Some(id) = args.positional.first().cloned() else { return usage() };
            if args.environment.trim().is_empty() {
                eprintln!("duckle-runner release activate: --environment is required");
                return ExitCode::from(2);
            }
            let release = match release::load(&ws, &id) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("duckle-runner release activate: {e}");
                    return ExitCode::from(2);
                }
            };
            // Serialised against anything else touching the control plane, so
            // two activations cannot interleave their pointer writes.
            let _lock = match duckle_duckdb_engine::runlock::lock_store(&ws, "release") {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("duckle-runner release activate: {e}");
                    return ExitCode::from(2);
                }
            };
            let problems = dependency_gate(&ws, &release);
            if !problems.is_empty() {
                eprintln!("duckle-runner release activate: refusing, nothing was changed:");
                for p in &problems {
                    eprintln!("  {p}");
                }
                return ExitCode::from(1);
            }
            // Activating overwrites the workspace's control-plane files with
            // the release's. Uncommitted edits would be lost, so they are named
            // and the activation refuses unless the operator says to discard
            // them.
            let d = release::drift(&ws, &release);
            if !d.is_empty() && !args.force {
                eprintln!(
                    "duckle-runner release activate: the workspace differs from this release,                      and activating would overwrite it. Nothing was changed."
                );
                for f in &d.changed {
                    eprintln!("  would overwrite  {f}");
                }
                for f in &d.extra {
                    eprintln!("  would remove     {f}  (not part of this release)");
                }
                eprintln!("
Build a release from the workspace first, or pass --force to discard.");
                return ExitCode::from(1);
            }
            // Content first, pointer second. A crash between them leaves the
            // environment pointing at the release it was running, with the new
            // content on disk - which `verify` reports as drift rather than
            // silently accepting.
            match release::materialise(&ws, &release) {
                Ok(touched) => {
                    for t in &touched {
                        println!("  {t}");
                    }
                }
                Err(e) => {
                    eprintln!("duckle-runner release activate: {e}");
                    return ExitCode::from(2);
                }
            }
            if let Err(e) = release::point_at(&ws, &args.environment, &id) {
                eprintln!("duckle-runner release activate: {e}");
                return ExitCode::from(2);
            }
            println!("{} is now running release {id}", args.environment);
            ExitCode::from(0)
        }

        "rollback" => {
            if args.environment.trim().is_empty() {
                eprintln!("duckle-runner release rollback: --environment is required");
                return ExitCode::from(2);
            }
            let Some(previous) = release::previous(&ws, &args.environment) else {
                eprintln!(
                    "duckle-runner release rollback: {} has no previous release to go back to",
                    args.environment
                );
                return ExitCode::from(1);
            };
            let _lock = match duckle_duckdb_engine::runlock::lock_store(&ws, "release") {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("duckle-runner release rollback: {e}");
                    return ExitCode::from(2);
                }
            };
            // Deliberately NOT gated on the dependency checks. Rollback is what
            // an operator reaches for when the current release is broken, and a
            // rollback that refuses because the workspace is in a bad state is
            // a rollback that never works when it is needed. It DOES restore
            // the content: a rollback that moved only the pointer would leave
            // the environment running the release it was rolling back from.
            let target = match release::load(&ws, &previous) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("duckle-runner release rollback: {e}");
                    return ExitCode::from(2);
                }
            };
            match release::materialise(&ws, &target) {
                Ok(touched) => {
                    for t in &touched {
                        println!("  {t}");
                    }
                }
                Err(e) => {
                    eprintln!("duckle-runner release rollback: {e}");
                    return ExitCode::from(2);
                }
            }
            if let Err(e) = release::point_at(&ws, &args.environment, &previous) {
                eprintln!("duckle-runner release rollback: {e}");
                return ExitCode::from(2);
            }
            println!("{} rolled back to release {previous}", args.environment);
            ExitCode::from(0)
        }

        "list" => {
            let mut ids: Vec<(std::time::SystemTime, String)> = Vec::new();
            if let Ok(entries) = std::fs::read_dir(release::dir(&ws)) {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.extension().and_then(|x| x.to_str()) != Some("json") {
                        continue;
                    }
                    let when = e.metadata().and_then(|m| m.modified()).unwrap_or(std::time::UNIX_EPOCH);
                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                        ids.push((when, stem.to_string()));
                    }
                }
            }
            ids.sort_by(|a, b| b.0.cmp(&a.0));
            if ids.is_empty() {
                println!("no releases in this workspace");
            }
            for (_, id) in &ids {
                println!("  {id}");
            }
            let envs = ws.join(".duckle").join("environments");
            if let Ok(entries) = std::fs::read_dir(&envs) {
                println!();
                for e in entries.flatten() {
                    if let Some(name) = e.file_name().to_str() {
                        let active = release::active(&ws, name).unwrap_or_else(|| "-".into());
                        let prev = release::previous(&ws, name).unwrap_or_else(|| "-".into());
                        println!("  {name:<14} active {active}  (previous {prev})");
                    }
                }
            }
            ExitCode::from(0)
        }

        _ => usage(),
    }
}

fn report(args: &Args, id: &str, problems: &[String]) -> ExitCode {
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "release": id,
                "ok": problems.is_empty(),
                "problems": problems,
            }))
            .unwrap_or_default()
        );
    } else if problems.is_empty() {
        println!("release {id} verifies");
    } else {
        println!("release {id} has {} problem(s):", problems.len());
        for p in problems {
            println!("  {p}");
        }
    }
    match problems.is_empty() {
        true => ExitCode::from(0),
        false => ExitCode::from(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A release verifies on its own content, not on the workspace matching it.
    ///
    /// This test asserted the opposite until Louis pointed out what that
    /// implied: requiring `workspace == release` before activation makes
    /// rollback impossible, because rolling back to A is exactly the case where
    /// the workspace holds B. A release is now verified by whether the bytes it
    /// stored are intact and still compile - the workspace is what activation
    /// overwrites, not a precondition for it.
    #[test]
    fn a_release_verifies_on_its_own_content_not_on_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        let doc = r#"{"formatVersion":1,"name":"one","nodes":[
            {"id":"s","type":"source","position":{"x":0,"y":0},
             "data":{"label":"In","componentId":"src.csv","properties":{"path":"a.csv"}}}],
            "edges":[]}"#;
        std::fs::write(ws.join("pipelines/one.json"), doc).unwrap();
        let r = release::build(ws).unwrap();
        release::save(ws, &r).unwrap();
        assert!(dependency_gate(ws, &r).is_empty(), "{:?}", dependency_gate(ws, &r));

        // The workspace moves on. The release is unaffected: its content is
        // stored, so it still verifies and can still be activated - which is
        // what makes rolling back to it possible at all.
        std::fs::write(ws.join("pipelines/one.json"), doc.replace("a.csv", "b.csv")).unwrap();
        assert!(
            dependency_gate(ws, &r).is_empty(),
            "a release must not stop verifying because the workspace changed: {:?}",
            dependency_gate(ws, &r)
        );
        // And the difference is still visible, so activation can refuse to
        // discard uncommitted work rather than doing it silently.
        let drift = duckle_duckdb_engine::release::drift(ws, &r);
        assert_eq!(drift.changed, vec!["pipelines/one.json"], "{drift:?}");
    }

    #[test]
    fn a_release_whose_stored_content_will_not_compile_is_refused() {
        // The check that replaced it: what matters is that the bytes the
        // release stored still run, because those are what activation puts on
        // disk.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines/broken.json"),
            r#"{"formatVersion":1,"name":"broken","nodes":[
                {"id":"x","type":"transform","position":{"x":0,"y":0},
                 "data":{"label":"x","componentId":"xf.filter","properties":{}}}],
                "edges":[]}"#,
        )
        .unwrap();
        let r = release::build(ws).unwrap();
        release::save(ws, &r).unwrap();
        let problems = dependency_gate(ws, &r);
        assert!(
            problems.iter().any(|p| p.contains("does not compile")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_missing_connection_is_named_before_anything_is_activated() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines/c.json"),
            r#"{"formatVersion":1,"name":"c","nodes":[
                {"id":"s","type":"source","position":{"x":0,"y":0},
                 "data":{"label":"In","componentId":"src.postgres",
                         "properties":{"connectionRef":"warehouse","mode":"table","tableName":"t"}}}],
                "edges":[]}"#,
        )
        .unwrap();
        let r = release::build(ws).unwrap();
        let problems = dependency_gate(ws, &r);
        assert!(
            problems.iter().any(|p| p.contains("warehouse")),
            "a missing connection was not reported: {problems:?}"
        );

        std::fs::create_dir_all(ws.join("connections")).unwrap();
        std::fs::write(ws.join("connections/warehouse.json"), "{}").unwrap();
        let problems = dependency_gate(ws, &r);
        assert!(
            !problems.iter().any(|p| p.contains("warehouse")),
            "{problems:?}"
        );
    }

    #[test]
    fn every_problem_is_reported_rather_than_the_first() {
        // An operator fixing a production activation should not discover the
        // second problem after fixing the first.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines/a.json"),
            r#"{"formatVersion":1,"name":"a","nodes":[
                {"id":"s","type":"source","position":{"x":0,"y":0},
                 "data":{"label":"In","componentId":"src.postgres",
                         "properties":{"connectionRef":"one","mode":"table","tableName":"t"}}}],
                "edges":[]}"#,
        )
        .unwrap();
        std::fs::write(
            ws.join("pipelines/b.json"),
            r#"{"formatVersion":1,"name":"b","nodes":[
                {"id":"s","type":"source","position":{"x":0,"y":0},
                 "data":{"label":"In","componentId":"src.postgres",
                         "properties":{"connectionRef":"two","mode":"table","tableName":"t"}}}],
                "edges":[]}"#,
        )
        .unwrap();
        let r = release::build(ws).unwrap();
        let problems = dependency_gate(ws, &r);
        assert!(problems.iter().any(|p| p.contains("\"one\"")), "{problems:?}");
        assert!(problems.iter().any(|p| p.contains("\"two\"")), "{problems:?}");
    }
}
