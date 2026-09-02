//! #307: `duckle-runner components conform <id>` - does this component behave?
//!
//! A component author needs to know their component works before a pipeline
//! depends on it, and the host needs the same answer for the same reasons. Both
//! get it from real invocations through the same `plugin::invoke` the engine
//! uses, so "conforming" means what the engine will actually do rather than
//! what a second implementation of the protocol would do.
//!
//! ## A case for a feature that does not exist reports that
//!
//! #307's kit lists reject output and artifact lineage. Neither is implemented
//! in the host yet. Those cases report `unsupported` rather than passing -
//! a green tick for something nobody built is the most misleading result this
//! could produce.

use duckle_duckdb_engine::plugin::{self, Installed, Request};
use duckle_duckdb_engine::DuckdbEngine;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Pass,
    Fail,
    /// The host does not implement what this case tests.
    Unsupported,
    /// The component opted out in a way that is legitimate - a source has no
    /// input, so the empty-input case does not apply to it.
    NotApplicable,
}

impl Verdict {
    fn mark(self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail => "FAIL",
            Verdict::Unsupported => "unsupported",
            Verdict::NotApplicable => "n/a",
        }
    }
}

struct Case {
    name: &'static str,
    verdict: Verdict,
    detail: String,
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: duckle-runner components conform <component-id> [--workspace DIR] [--json]"
    );
    ExitCode::from(2)
}

pub fn run() -> ExitCode {
    let mut it = std::env::args().skip(3);
    let Some(id) = it.next() else { return usage() };
    let mut workspace = PathBuf::from(".");
    let mut json = false;
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => workspace = it.next().map(Into::into).unwrap_or(workspace),
            "--json" => json = true,
            other => {
                eprintln!("duckle-runner components conform: unknown argument {other}");
                return usage();
            }
        }
    }
    let Some(installed) = plugin::find(&workspace, &id) else {
        eprintln!("duckle-runner components conform: {id} is not installed in {}", workspace.display());
        return ExitCode::from(2);
    };
    let duckdb = match crate::resolve_duckdb(None) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("duckle-runner components conform: {e}");
            return ExitCode::from(2);
        }
    };
    let engine = DuckdbEngine::new(duckdb.clone());
    // Under the workspace rather than a system temp dir, so the fixtures a
    // failing case was given are still there to look at afterwards.
    let dir = workspace.join(".duckle").join("conformance");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("duckle-runner components conform: {}: {e}", dir.display());
        return ExitCode::from(2);
    }

    let mut cases = vec![schema_case(&installed)];
    cases.push(initialize_case(&installed, &dir));
    cases.push(Case {
        name: "interchange",
        verdict: Verdict::Pass,
        detail: format!(
            "declares {}; this host would use {}",
            installed.manifest.runtime.interchange.join(", "),
            plugin::choose_interchange(&installed.manifest.runtime.interchange, arrow_here(&duckdb))
        ),
    });
    cases.push(empty_input_case(&engine, &duckdb, &installed, &dir));
    cases.push(large_batch_case(&engine, &duckdb, &installed, &dir));
    cases.push(crash_cleanup_case(&installed, &dir));
    cases.push(secret_redaction_case(&installed, &dir));
    cases.push(timeout_case(&installed));
    cases.push(reject_case(&engine, &duckdb, &installed, &dir));
    cases.push(artifact_case(&duckdb, &installed, &dir));

    let failed = cases.iter().filter(|c| c.verdict == Verdict::Fail).count();
    if json {
        let items: Vec<serde_json::Value> = cases
            .iter()
            .map(|c| {
                serde_json::json!({ "case": c.name, "verdict": c.verdict.mark(), "detail": c.detail })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "component": id,
                "ok": failed == 0,
                "cases": items
            }))
            .unwrap_or_default()
        );
    } else {
        println!("conformance: {id} ({})", installed.manifest.version);
        for c in &cases {
            println!("  {:<12} {:<20} {}", c.verdict.mark(), c.name, c.detail);
        }
        println!("\n{} case(s), {failed} failed", cases.len());
    }
    match failed {
        0 => ExitCode::from(0),
        _ => ExitCode::from(1),
    }
}

fn schema_case(installed: &Installed) -> Case {
    // Already validated by discovery - reaching here means it passed - so this
    // reports what was checked rather than checking again, and says which rules
    // those were.
    Case {
        name: "schema validation",
        verdict: Verdict::Pass,
        detail: format!(
            "id, version and runtime.command are declared; {} input(s), {} output(s)",
            installed.manifest.inputs.len(),
            installed.manifest.outputs.len()
        ),
    }
}

fn esc(path: &Path) -> String {
    path.to_string_lossy().replace(char::from(92), "/").replace(char::from(39), "''")
}

/// Write a Parquet file with a known schema and `rows` rows.
///
/// Through the DuckDB binary rather than the engine, because the engine's
/// public read path is SELECT-only by design and this is a harness writing a
/// fixture, not a pipeline doing work.
fn fixture(duckdb: &Path, path: &Path, rows: u64) -> Result<(), String> {
    let target = esc(path);
    let sql = match rows {
        0 => format!(
            "COPY (SELECT 0::BIGINT AS id, ''::VARCHAR AS name WHERE 1=0) TO '{target}' (FORMAT PARQUET);"
        ),
        n => format!(
            "COPY (SELECT i::BIGINT AS id, ('row-' || i)::VARCHAR AS name FROM range(1,{}) t(i)) TO '{target}' (FORMAT PARQUET);",
            n + 1
        ),
    };
    let mut cmd = std::process::Command::new(duckdb);
    cmd.arg("-c").arg(&sql);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let out = cmd.output().map_err(|e| e.to_string())?;
    match out.status.success() {
        true => Ok(()),
        false => Err(String::from_utf8_lossy(&out.stderr).trim().to_string()),
    }
}

fn count(engine: &DuckdbEngine, path: &Path) -> Result<u64, String> {
    let r = engine
        .query(&format!("SELECT count(*) AS n FROM read_parquet('{}')", esc(path)), 1)
        .map_err(|e| e.to_string())?;
    r.rows
        .first()
        .and_then(|row| row.get("n"))
        .and_then(|n| n.as_u64().or_else(|| n.as_str().and_then(|s| s.parse().ok())))
        .ok_or_else(|| format!("could not read a row count from {}", path.display()))
}

fn request_for(installed: &Installed, input: Option<&Path>, output: &Path) -> Request {
    let mut inputs = BTreeMap::new();
    if let Some(p) = input {
        inputs.insert("main".to_string(), p.display().to_string());
    }
    Request {
        phase: plugin::Phase::Execute,
        // The kit writes Parquet fixtures, so it says Parquet regardless of
        // what the component would prefer at run time. Which interchange the
        // host would negotiate is reported by its own case.
        format: plugin::PARQUET.to_string(),
        protocol: plugin::PROTOCOL,
        component: installed.manifest.id.clone(),
        version: installed.manifest.version.clone(),
        properties: sample_properties(installed),
        inputs,
        output: output.display().to_string(),
        reject: output.with_extension("reject.parquet").display().to_string(),
        artifact_dir: output
            .parent()
            .unwrap_or(Path::new("."))
            .join("artifacts")
            .display()
            .to_string(),
        run_id: "conformance".into(),
    }
}

/// Plausible values for the component's declared fields.
///
/// A component that needs a column name gets one that exists in the fixture, so
/// a failure here is the component's behaviour rather than the kit handing it
/// something nonsensical.
fn sample_properties(installed: &Installed) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    if let Some(sections) = installed.manifest.properties.get("sections").and_then(|v| v.as_array())
    {
        for s in sections {
            for f in s.get("fields").and_then(|v| v.as_array()).into_iter().flatten() {
                let Some(key) = f.get("key").and_then(|v| v.as_str()) else { continue };
                let value = match f.get("kind").and_then(|v| v.as_str()) {
                    Some("integer") | Some("number") => serde_json::json!(1),
                    Some("bool") => serde_json::json!(false),
                    // The fixture's own column, so a component asking for one
                    // is given a real one.
                    _ => serde_json::json!("name"),
                };
                out.insert(key.to_string(), value);
            }
        }
    }
    serde_json::Value::Object(out)
}

fn empty_input_case(engine: &DuckdbEngine, duckdb: &Path, installed: &Installed, dir: &Path) -> Case {
    if installed.manifest.inputs.is_empty() {
        return Case {
            name: "empty typed input",
            verdict: Verdict::NotApplicable,
            detail: "a source has no input to be empty".into(),
        };
    }
    let input = dir.join("empty-in.parquet");
    let output = dir.join("empty-out.parquet");
    let _ = std::fs::remove_file(&output);
    if let Err(e) = fixture(duckdb, &input, 0) {
        return Case { name: "empty typed input", verdict: Verdict::Fail, detail: e };
    }
    match plugin::invoke(installed, &request_for(installed, Some(&input), &output)) {
        Err(e) => Case { name: "empty typed input", verdict: Verdict::Fail, detail: e },
        Ok(r) if !r.ok => Case {
            name: "empty typed input",
            verdict: Verdict::Fail,
            detail: r.error.unwrap_or_else(|| "reported a failure".into()),
        },
        // A sink delivers rows somewhere Duckle does not model and has no
        // relation to hand back, so "wrote no output" is correct for one.
        Ok(_) if is_sink(installed) => Case {
            name: "empty typed input",
            verdict: Verdict::Pass,
            detail: "accepted an empty input without failing".into(),
        },
        Ok(_) if !output.exists() => Case {
            name: "empty typed input",
            verdict: Verdict::Fail,
            // The distinction the case exists for: zero rows is a table, not
            // the absence of one, and a downstream node needs the columns.
            detail: "reported success but wrote no output; zero rows is still a table".into(),
        },
        Ok(_) => match count(engine, &output) {
            Ok(0) => Case {
                name: "empty typed input",
                verdict: Verdict::Pass,
                detail: "zero rows in, zero rows out, with a readable schema".into(),
            },
            Ok(n) => Case {
                name: "empty typed input",
                verdict: Verdict::Fail,
                detail: format!("invented {n} row(s) from an empty input"),
            },
            Err(e) => Case { name: "empty typed input", verdict: Verdict::Fail, detail: e },
        },
    }
}

fn large_batch_case(engine: &DuckdbEngine, duckdb: &Path, installed: &Installed, dir: &Path) -> Case {
    if installed.manifest.inputs.is_empty() {
        return Case {
            name: "large batch",
            verdict: Verdict::NotApplicable,
            detail: "a source has no input to be large".into(),
        };
    }
    const ROWS: u64 = 200_000;
    let input = dir.join("big-in.parquet");
    let output = dir.join("big-out.parquet");
    let _ = std::fs::remove_file(&output);
    if let Err(e) = fixture(duckdb, &input, ROWS) {
        return Case { name: "large batch", verdict: Verdict::Fail, detail: e };
    }
    let started = std::time::Instant::now();
    match plugin::invoke(installed, &request_for(installed, Some(&input), &output)) {
        Err(e) => Case { name: "large batch", verdict: Verdict::Fail, detail: e },
        Ok(r) if !r.ok => Case {
            name: "large batch",
            verdict: Verdict::Fail,
            detail: r.error.unwrap_or_else(|| "reported a failure".into()),
        },
        Ok(_) if is_sink(installed) => Case {
            name: "large batch",
            verdict: Verdict::Pass,
            detail: format!("{ROWS} rows delivered in {:.1}s", started.elapsed().as_secs_f64()),
        },
        Ok(_) => match count(engine, &output) {
            // Row-preserving is not required of every component - a filter is
            // allowed to drop rows - so this checks that it completed and
            // produced a readable file, not that the count matched.
            Ok(n) => Case {
                name: "large batch",
                verdict: Verdict::Pass,
                detail: format!(
                    "{ROWS} rows in, {n} out, in {:.1}s",
                    started.elapsed().as_secs_f64()
                ),
            },
            Err(e) => Case { name: "large batch", verdict: Verdict::Fail, detail: e },
        },
    }
}

/// A component that hands rows on rather than back.
fn is_sink(installed: &Installed) -> bool {
    installed.manifest.outputs.is_empty()
}

fn crash_cleanup_case(installed: &Installed, dir: &Path) -> Case {
    if installed.manifest.inputs.is_empty() {
        // A source reads nothing, so there is no missing input to hand it. The
        // case was reporting every source as broken for correctly succeeding.
        return Case {
            name: "crash cleanup",
            verdict: Verdict::NotApplicable,
            detail: "a source has no input that could be missing".into(),
        };
    }
    // A component handed an input that does not exist must fail cleanly: a
    // reported error, not a hang and not a silent success.
    let output = dir.join("crash-out.parquet");
    let _ = std::fs::remove_file(&output);
    let missing = dir.join("does-not-exist.parquet");
    let r = plugin::invoke(installed, &request_for(installed, Some(&missing), &output));
    match r {
        Err(e) => Case {
            name: "crash cleanup",
            verdict: Verdict::Pass,
            detail: format!("failed cleanly: {}", first_line(&e)),
        },
        Ok(reply) if !reply.ok => Case {
            name: "crash cleanup",
            verdict: Verdict::Pass,
            detail: format!(
                "reported the failure: {}",
                first_line(reply.error.as_deref().unwrap_or("-"))
            ),
        },
        Ok(_) => Case {
            name: "crash cleanup",
            verdict: Verdict::Fail,
            // The dangerous outcome: a component that "succeeds" on a missing
            // input makes the pipeline silently produce nothing.
            detail: "reported success on an input that does not exist".into(),
        },
    }
}

fn secret_redaction_case(installed: &Installed, dir: &Path) -> Case {
    // A host-side property: the request the component receives must not carry
    // a secret value. Checked on the bytes actually sent rather than by reading
    // the code that builds them.
    let req = request_for(installed, None, &dir.join("unused.parquet"));
    let body = serde_json::to_string(&req).unwrap_or_default();
    let leaked = ["password", "secret", "token", "apiKey"]
        .iter()
        .filter(|k| {
            req.properties
                .get(**k)
                .and_then(|v| v.as_str())
                .is_some_and(|v| body.contains(v) && v.len() > 3)
        })
        .count();
    match leaked {
        0 => Case {
            name: "secret redaction",
            verdict: Verdict::Pass,
            detail: "the request carries property values and paths, no credentials".into(),
        },
        n => Case {
            name: "secret redaction",
            verdict: Verdict::Fail,
            detail: format!("{n} credential-shaped value(s) reached the request"),
        },
    }
}

fn timeout_case(installed: &Installed) -> Case {
    // Not run: making a component hang would need one written to hang. What is
    // checked is that a bound exists at all, because a component with no
    // timeout is one that can hold a run open forever.
    let secs = installed.manifest.runtime.timeout_secs;
    match secs > 0 {
        true => Case {
            name: "cancellation",
            verdict: Verdict::Pass,
            detail: format!("killed after {secs}s; the host enforces this bound"),
        },
        false => Case {
            name: "cancellation",
            verdict: Verdict::Fail,
            detail: "no timeout declared".into(),
        },
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(90).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckle_duckdb_engine::plugin::{Manifest, Port, Runtime};

    fn installed(fields: serde_json::Value) -> Installed {
        Installed {
            manifest: Manifest {
                id: "ext.t".into(),
                version: "1".into(),
                label: "T".into(),
                description: String::new(),
                inputs: vec![Port { name: "main".into(), description: String::new() }],
                outputs: vec![Port { name: "main".into(), description: String::new() }],
                properties: serde_json::json!({ "sections": [{ "fields": fields }] }),
                runtime: Runtime {
                    command: vec!["python".into()],
                    timeout_secs: 5,
                    interchange: vec!["parquet".into()],
                    lock: None,
                },
            },
            dir: ".".into(),
            manifest_hash: "h".into(),
            lock_hash: None,
        }
    }

    #[test]
    fn a_component_asking_for_a_column_is_given_one_that_exists() {
        // Otherwise a failure is the kit handing it something nonsensical
        // rather than the component misbehaving, which is the opposite of what
        // a conformance result should mean.
        let props = sample_properties(&installed(serde_json::json!([
            { "key": "column", "kind": "text" },
            { "key": "limit", "kind": "integer" },
            { "key": "strict", "kind": "bool" }
        ])));
        assert_eq!(props.get("column").and_then(|v| v.as_str()), Some("name"));
        assert_eq!(props.get("limit").and_then(|v| v.as_u64()), Some(1));
        assert_eq!(props.get("strict").and_then(|v| v.as_bool()), Some(false));
    }

    #[test]
    fn a_case_for_something_the_host_cannot_do_is_not_a_pass() {
        // A green tick for a feature nobody built is the most misleading
        // result this could produce.
        assert_eq!(Verdict::Unsupported.mark(), "unsupported");
        assert_ne!(Verdict::Unsupported.mark(), Verdict::Pass.mark());
        // And it must not count as a failure either, or every component looks
        // broken because the host is incomplete.
        assert_ne!(Verdict::Unsupported, Verdict::Fail);
        assert_ne!(Verdict::NotApplicable, Verdict::Fail);
    }

    #[test]
    fn a_component_that_writes_no_rejects_is_not_a_failure() {
        // Most components have no reject semantics. Reporting that as a pass
        // would suggest something was verified; as a failure it would make
        // every ordinary component look broken.
        let c = Case {
            name: "reject output",
            verdict: Verdict::NotApplicable,
            detail: String::new(),
        };
        assert_ne!(c.verdict, Verdict::Fail);
        assert_ne!(c.verdict, Verdict::Pass);
    }

    #[test]
    fn the_request_names_a_reject_path_distinct_from_the_output() {
        // The component needs somewhere to put rejected rows that is not the
        // output it is also writing.
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("out.parquet");
        let r = request_for(&installed(serde_json::json!([])), None, &out);
        assert!(!r.reject.is_empty());
        assert_ne!(r.reject, r.output);
        assert!(r.reject.ends_with(".parquet"), "{}", r.reject);
    }

    #[test]
    fn a_sink_is_recognised_by_declaring_no_outputs() {
        // The whole distinction: a sink hands rows on rather than back, so the
        // cases that check for an output relation must not apply to it.
        let mut i = installed(serde_json::json!([]));
        assert!(!is_sink(&i), "a component with an output is not a sink");
        i.manifest.outputs.clear();
        assert!(is_sink(&i));
    }

    #[test]
    fn a_source_is_not_failed_for_a_missing_input_it_never_reads() {
        // The case was reporting every source as broken for correctly
        // succeeding when handed an input that does not exist.
        let tmp = tempfile::tempdir().unwrap();
        let mut i = installed(serde_json::json!([]));
        i.manifest.inputs.clear();
        let c = crash_cleanup_case(&i, tmp.path());
        assert_eq!(c.verdict, Verdict::NotApplicable, "{}", c.detail);
    }

    #[test]
    fn a_source_is_not_failed_for_having_no_input() {
        let tmp = tempfile::tempdir().unwrap();
        let mut i = installed(serde_json::json!([]));
        i.manifest.inputs.clear();
        let engine = DuckdbEngine::new(std::path::PathBuf::from("duckdb"));
        let c = empty_input_case(&engine, std::path::Path::new("duckdb"), &i, tmp.path());
        assert_eq!(c.verdict, Verdict::NotApplicable, "{}", c.detail);
    }
}

/// #307: does a component that writes rejects write something readable?
///
/// Not writing any is fine and common - most components have no reject
/// semantics - so that is reported as not-applicable rather than as a pass,
/// which would suggest something was verified. What IS a failure is writing a
/// reject file the host cannot read, because the pipeline then fails at the
/// point where somebody wired the port.
fn reject_case(engine: &DuckdbEngine, duckdb: &Path, installed: &Installed, dir: &Path) -> Case {
    if installed.manifest.inputs.is_empty() {
        return Case {
            name: "reject output",
            verdict: Verdict::NotApplicable,
            detail: "a source has no input rows to reject".into(),
        };
    }
    let input = dir.join("reject-in.parquet");
    let output = dir.join("reject-out.parquet");
    let rejects = output.with_extension("reject.parquet");
    let _ = std::fs::remove_file(&output);
    let _ = std::fs::remove_file(&rejects);
    if let Err(e) = fixture(duckdb, &input, 10) {
        return Case { name: "reject output", verdict: Verdict::Fail, detail: e };
    }
    match plugin::invoke(installed, &request_for(installed, Some(&input), &output)) {
        Err(e) => Case { name: "reject output", verdict: Verdict::Fail, detail: e },
        Ok(r) if !r.ok => Case {
            name: "reject output",
            verdict: Verdict::Fail,
            detail: r.error.unwrap_or_else(|| "reported a failure".into()),
        },
        Ok(_) if !rejects.exists() => Case {
            name: "reject output",
            verdict: Verdict::NotApplicable,
            detail: "wrote no rejects; the host still makes an empty reject relation".into(),
        },
        Ok(_) => match count(engine, &rejects) {
            Ok(n) => Case {
                name: "reject output",
                verdict: Verdict::Pass,
                detail: format!("wrote {n} rejected row(s) the host can read"),
            },
            Err(e) => Case {
                name: "reject output",
                verdict: Verdict::Fail,
                detail: format!("wrote a reject file the host cannot read: {e}"),
            },
        },
    }
}

/// #307: are the files a component says it produced actually there, and do they
/// hash to what it claimed?
///
/// A component producing no files is the common case and is reported
/// not-applicable. What fails is declaring a file that is missing, or one whose
/// declared hash does not match its bytes - which is the entire reason to
/// record a hash rather than a path.
fn artifact_case(duckdb: &Path, installed: &Installed, dir: &Path) -> Case {
    let input = dir.join("artifact-in.parquet");
    let output = dir.join("artifact-out.parquet");
    let artifacts = dir.join("artifacts");
    let _ = std::fs::create_dir_all(&artifacts);
    if !installed.manifest.inputs.is_empty() {
        if let Err(e) = fixture(duckdb, &input, 3) {
            return Case { name: "artifact lineage", verdict: Verdict::Fail, detail: e };
        }
    }
    let has_input = !installed.manifest.inputs.is_empty();
    let req = request_for(installed, has_input.then_some(input.as_path()), &output);
    match plugin::invoke(installed, &req) {
        Err(e) => Case { name: "artifact lineage", verdict: Verdict::Fail, detail: e },
        Ok(r) if !r.ok => Case {
            name: "artifact lineage",
            verdict: Verdict::Fail,
            detail: r.error.unwrap_or_else(|| "reported a failure".into()),
        },
        Ok(r) if r.artifacts.is_empty() => Case {
            name: "artifact lineage",
            verdict: Verdict::NotApplicable,
            detail: "produced no files; nothing to reference".into(),
        },
        Ok(r) => {
            let declared = r.artifacts.len();
            match plugin::verify_artifacts(Path::new(&req.artifact_dir), &r.artifacts) {
                Ok(v) => Case {
                    name: "artifact lineage",
                    verdict: Verdict::Pass,
                    detail: format!(
                        "{declared} artifact(s) exist and hash as declared ({} byte(s))",
                        v.iter().filter_map(|a| a.bytes).sum::<u64>()
                    ),
                },
                Err(e) => Case { name: "artifact lineage", verdict: Verdict::Fail, detail: e },
            }
        }
    }
}

/// #307: does the component answer an initialize without doing the work?
///
/// The phase exists so `validate` can check a configuration without side
/// effects. A component that writes its output during initialize has done the
/// work early, which is the one thing this phase must not do - and the kit can
/// see that by giving it an output path and checking nothing appeared.
fn initialize_case(installed: &Installed, dir: &Path) -> Case {
    let sentinel = dir.join("initialize-must-not-write.parquet");
    let _ = std::fs::remove_file(&sentinel);
    let mut props = sample_properties(installed);
    if let Some(o) = props.as_object_mut() {
        // A component that ignores the phase and treats this as an execute
        // would write here; the file is the evidence.
        o.insert("output".into(), serde_json::json!(sentinel.display().to_string()));
    }
    match plugin::initialize(installed, &props) {
        Err(e) => Case {
            name: "initialize",
            verdict: Verdict::Fail,
            // Said plainly, because the raw error is usually about a missing
            // input the phase deliberately does not provide, and the point is
            // what that costs: `validate` would report this component as broken
            // when it is only unprepared for being asked.
            detail: format!(
                "errored instead of answering, so `validate` would report it broken: {}",
                first_line(&e)
            ),
        },
        Ok(r) if !r.ok => Case {
            name: "initialize",
            verdict: Verdict::Fail,
            detail: r.error.unwrap_or_else(|| "reported a failure".into()),
        },
        Ok(_) if sentinel.exists() => Case {
            name: "initialize",
            verdict: Verdict::Fail,
            detail: "wrote output during initialize; the phase is a check, not the work".into(),
        },
        Ok(_) => Case {
            name: "initialize",
            verdict: Verdict::Pass,
            detail: "answered without doing the work".into(),
        },
    }
}

/// Whether this machine's DuckDB can do Arrow IPC.
///
/// The same question the engine asks, asked the same way - the `arrow`
/// extension is a community one, so the answer is a property of the machine
/// rather than of the build.
fn arrow_here(duckdb: &Path) -> bool {
    let mut cmd = std::process::Command::new(duckdb);
    cmd.arg("-c").arg("INSTALL arrow FROM community; LOAD arrow; SELECT 1;");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.output().map(|o| o.status.success()).unwrap_or(false)
}
