//! `duckle test` - run a pipeline against a fixed input and assert what comes out.
//!
//! `validate` compiles a pipeline without running it, which catches wiring and SQL that
//! will not bind. It cannot catch a transform that binds and computes the wrong thing.
//! The fastest way to catch THAT is the oldest one: a tiny known input, and the exact
//! rows expected out of one node.
//!
//! A case names the node it asserts on, so the run stops there - nothing downstream
//! executes and no sink writes. That is the same partial execution the desktop preview
//! uses, which is why this is small: the engine already knew how to stop at a node.
//!
//! A test file is JSON, because pipelines are:
//!
//! ```json
//! {
//!   "pipeline": "pipelines/orders.json",
//!   "cases": [
//!     {
//!       "name": "a row with no amount is dropped",
//!       "given": { "src_1": "id,amt\n1,5\n2,\n" },
//!       "expect": { "node": "filter_1", "rows": [{ "id": "1", "amt": "5" }] }
//!     }
//!   ]
//! }
//! ```
//!
//! `given` maps a source node id to the text it should read, or to a fixture FILE
//! beside the test. A source that reads a file has its `path` pointed at the fixture, so
//! the pipeline under test is the real one - its delimiter, its header setting, its
//! declared columns - rather than a copy that has drifted from it. A source that reads
//! no file (S3 behind a connection, REST, DuckLake) is REPLACED by a reader for the
//! fixture, because setting a path such a source ignores left the test reading
//! production while looking like it read the fixture.
//!
//! Comparison is strict: `5` and `"5"` are different, and so are `null`, a missing field
//! and `""`. A test that cannot tell those apart cannot catch a column that silently
//! became a string, or a join that produced NULL instead of a value. A case can opt back
//! into text comparison with `"compareAs": "text"` where the source really is all
//! VARCHAR.
//!
//! What is compared is the node's WHOLE output, written out and read back. It used to be
//! the run preview, which the engine caps at 100 rows - so a case expecting 100 rows
//! passed while the node produced 101.

use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};

#[derive(Debug)]
pub struct Case {
    pub name: String,
    pub given: Vec<(String, String)>,
    pub node: String,
    pub rows: Vec<JsonValue>,
    /// Compare by rendered text instead of by value AND type.
    ///
    /// Off by default, because `5` and `"5"` being equal hides exactly the bugs
    /// a test exists to catch. On for a case asserting against a source whose
    /// every column really is VARCHAR, where writing the expectation any other
    /// way would be a lie about the data.
    pub coerce: bool,
}

/// One failure, said in terms of the case rather than of the engine.
#[derive(Debug)]
pub struct Failure {
    pub case: String,
    pub why: String,
}

/// Read a test file into cases. The error names the file, since a suite usually has
/// several and "expected an object" on its own says nothing about which.
pub fn parse(path: &Path, text: &str) -> Result<(PathBuf, Vec<Case>), String> {
    let name = path.display();
    let doc: JsonValue =
        serde_json::from_str(text).map_err(|e| format!("{name}: not valid JSON: {e}"))?;
    let pipeline = doc
        .get("pipeline")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| format!("{name}: needs a \"pipeline\" naming the file under test"))?;
    // Relative to the TEST file, so a suite can sit beside the pipelines it covers and
    // still be run from anywhere.
    let base = path.parent().unwrap_or(Path::new("."));
    let pipeline_path = base.join(pipeline);

    let raw = doc
        .get("cases")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| format!("{name}: needs a \"cases\" array"))?;
    let mut cases = Vec::new();
    for (i, c) in raw.iter().enumerate() {
        let label = c
            .get("name")
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("case {}", i + 1));
        let expect = c
            .get("expect")
            .ok_or_else(|| format!("{name}: {label}: needs an \"expect\""))?;
        let node = expect
            .get("node")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| format!("{name}: {label}: \"expect\" needs a \"node\""))?
            .to_string();
        let rows = expect
            .get("rows")
            .and_then(JsonValue::as_array)
            .cloned()
            .ok_or_else(|| format!("{name}: {label}: \"expect\" needs a \"rows\" array"))?;
        let given = c
            .get("given")
            .and_then(JsonValue::as_object)
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        // Opt-in, per case: "compareAs": "text" on the expectation.
        let coerce = expect
            .get("compareAs")
            .and_then(JsonValue::as_str)
            .map(|m| m.eq_ignore_ascii_case("text"))
            .unwrap_or(false);
        cases.push(Case { name: label, given, node, rows, coerce });
    }
    Ok((pipeline_path, cases))
}

/// Point a source node at a file holding the case's text.
///
/// The node keeps every other property it has, so the fixture exercises the real
/// reader - its delimiter, its header setting, its declared columns - rather than a
/// simplified stand-in that would pass while the pipeline fails.
pub fn apply_given(doc: &mut JsonValue, node_id: &str, path: &str) -> Result<(), String> {
    let nodes = doc
        .get_mut("nodes")
        .and_then(JsonValue::as_array_mut)
        .ok_or("pipeline has no nodes")?;
    for n in nodes.iter_mut() {
        if n.get("id").and_then(JsonValue::as_str) != Some(node_id) {
            continue;
        }
        let data = n
            .get_mut("data")
            .and_then(JsonValue::as_object_mut)
            .ok_or_else(|| format!("node {node_id} has no data"))?;
        // Read the component BEFORE borrowing properties mutably.
        let component = data
            .get("componentId")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string();
        let props = data
            .entry("properties")
            .or_insert_with(|| JsonValue::Object(Default::default()));
        let props = props
            .as_object_mut()
            .ok_or_else(|| format!("node {node_id} properties are not an object"))?;
        // A source that reads a FILE keeps every other setting it has, so the
        // fixture exercises the real reader - its delimiter, its header
        // setting, its declared columns - rather than a copy of it.
        // An explicit list, not "does it have a path key". `src.ducklake` has
        // one and it means the CATALOG, so pointing it at a Parquet fixture
        // would ask DuckLake to attach a Parquet file as its metadata database.
        // Several other non-file sources carry a `path` that means something
        // else too.
        if reads_a_path(&component) {
            props.insert("path".into(), JsonValue::String(path.to_string()));
            return Ok(());
        }
        // A source that does NOT read a path - S3 behind a connection, REST,
        // DuckLake, a database - ignores one entirely. Setting it anyway left
        // the test reading PRODUCTION while looking like it read the fixture,
        // which is the quietest way a test suite can lie. Replace the whole
        // node with a reader for the fixture instead.
        let reader = reader_for(path).ok_or_else(|| {
            // Fall through to the message below rather than guessing: a
            // component this build does not know about, given a fixture it
            // cannot identify, has no safe substitution.
            format!(
                "{node_id} is a {} and does not read a file, so its input has to be given as a \
                 fixture FILE this can read in its place - name one ending .csv, .json, .jsonl, \
                 .ndjson or .parquet",
                if component.is_empty() { "source" } else { &component }
            )
        })?;
        data.insert("componentId".into(), JsonValue::String(reader.to_string()));
        let mut replacement = serde_json::Map::new();
        replacement.insert("path".into(), JsonValue::String(path.to_string()));
        if reader == "src.csv" {
            replacement.insert("hasHeader".into(), JsonValue::Bool(true));
        }
        data.insert("properties".into(), JsonValue::Object(replacement));
        return Ok(());
    }
    Err(format!("no node called {node_id} to give input to"))
}

/// Sources whose input IS a file path, so pointing that path at a fixture is
/// the whole substitution.
fn reads_a_path(component: &str) -> bool {
    matches!(
        component,
        "src.csv"
            | "src.json"
            | "src.jsonl"
            | "src.parquet"
            | "src.excel"
            | "src.xml"
            | "src.avro"
            | "src.orc"
            | "src.fixedwidth"
            | "src.artifact"
            | "src.text"
    )
}

/// The reader that can read this fixture, from its extension.
fn reader_for(path: &str) -> Option<&'static str> {
    let ext = path.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase())?;
    Some(match ext.as_str() {
        "csv" | "tsv" => "src.csv",
        "json" => "src.json",
        "jsonl" | "ndjson" => "src.jsonl",
        "parquet" => "src.parquet",
        _ => return None,
    })
}

/// How a cell is described when a comparison fails: the value AND its type,
/// because "expected [5], got [5]" is the least useful failure a typed
/// comparison can produce.
fn describe(v: Option<&JsonValue>) -> String {
    match v {
        None => "missing".into(),
        Some(JsonValue::Null) => "null".into(),
        Some(JsonValue::String(s)) => format!("\"{s}\" (string)"),
        Some(JsonValue::Bool(b)) => format!("{b} (boolean)"),
        Some(JsonValue::Number(n)) => format!("{n} (number)"),
        Some(JsonValue::Array(a)) => format!("{} (array of {})", JsonValue::Array(a.clone()), a.len()),
        Some(other) => format!("{other} (object)"),
    }
}

/// Are two cells the same, allowing for how a CSV fixture has to be written?
///
/// Strict by default: `5` and `"5"` are different, and so are `null`, a missing
/// field and `""`. A test that cannot tell those apart cannot catch the bugs
/// worth catching - a join that produced NULL instead of a value, or a parser
/// that produced the string "null".
///
/// `coerce` opts into comparing by rendered text, which is what a test asserting
/// on a source whose every column is VARCHAR wants.
fn same_cell(want: Option<&JsonValue>, got: Option<&JsonValue>, coerce: bool) -> bool {
    if !coerce {
        return match (want, got) {
            // A field the expectation does not mention is not asserted on at
            // all; a field it mentions as `null` must BE null, not absent.
            (Some(w), Some(g)) => w == g,
            (None, None) => true,
            _ => false,
        };
    }
    let text = |v: Option<&JsonValue>| match v {
        Some(JsonValue::String(s)) => s.clone(),
        Some(JsonValue::Null) | None => String::new(),
        Some(other) => other.to_string(),
    };
    text(want) == text(got)
}

pub fn compare(expected: &[JsonValue], actual: &[JsonValue]) -> Option<String> {
    compare_with(expected, actual, false)
}

pub fn compare_with(expected: &[JsonValue], actual: &[JsonValue], coerce: bool) -> Option<String> {
    if expected.len() != actual.len() {
        return Some(format!(
            "expected {} row(s), got {}",
            expected.len(),
            actual.len()
        ));
    }
    for (i, want) in expected.iter().enumerate() {
        let got = &actual[i];
        let obj = match want.as_object() {
            Some(o) => o,
            None => return Some(format!("row {}: expected an object", i + 1)),
        };
        for (k, wv) in obj {
            if !same_cell(Some(wv), got.get(k), coerce) {
                return Some(format!(
                    "row {}, {k}: expected {}, got {}",
                    i + 1,
                    describe(Some(wv)),
                    describe(got.get(k))
                ));
            }
        }
    }
    None
}

/// Run one case and say what went wrong, or nothing.
fn run_case(engine: &DuckdbEngine, pipeline: &Path, case: &Case, tmp: &Path) -> Option<String> {
    let text = match std::fs::read_to_string(pipeline) {
        Ok(t) => t,
        Err(e) => return Some(format!("cannot read {}: {e}", pipeline.display())),
    };
    let mut doc: JsonValue = match serde_json::from_str(&text) {
        Ok(d) => d,
        Err(e) => return Some(format!("{} is not valid JSON: {e}", pipeline.display())),
    };
    for (node, body) in &case.given {
        // A value naming a file that exists is the fixture itself - which is
        // how a Parquet or JSON fixture can stand in for a whole source. Any
        // other value is inline text, written out as before.
        let fixture = {
            let named = pipeline.parent().map(|d| d.join(body)).unwrap_or_else(|| PathBuf::from(body));
            if named.is_file() {
                named
            } else if Path::new(body).is_file() {
                PathBuf::from(body)
            } else {
                // Name the temp file by the SHAPE of the text, so inline JSON
                // standing in for a non-file source is read as JSON rather
                // than parsed as one very strange CSV column.
                let ext = match body.trim_start().chars().next() {
                    Some('{') | Some('[') => "json",
                    _ => "csv",
                };
                let f = tmp.join(format!("given_{node}.{ext}"));
                if let Err(e) = std::fs::write(&f, body) {
                    return Some(format!("cannot write the input for {node}: {e}"));
                }
                f
            }
        };
        let as_str = fixture.to_string_lossy().replace('\\', "/");
        if let Err(e) = apply_given(&mut doc, node, &as_str) {
            return Some(e);
        }
    }
    let mut doc_value = doc;
    // Compare the WHOLE relation, by writing it out and reading it back.
    //
    // This used to compare against the run PREVIEW, which is capped at 100
    // rows: a case expecting 100 rows passed while the node actually produced
    // 101, because only the first 100 ever reached the comparator. A test
    // framework that silently passes is worse than no test framework, so the
    // rows are materialised instead of sampled.
    //
    // A JSON sink downstream of the asserted node, with the sink as the run
    // target: everything upstream still runs exactly as it would, nothing else
    // downstream does, and JSON keeps the types that the typed comparison above
    // now depends on.
    let dump = tmp.join(format!("expect_{}.json", safe_name(&case.node)));
    let _ = std::fs::remove_file(&dump);
    let sink_id = "__duckle_test_capture";
    if let Err(e) = attach_capture_sink(&mut doc_value, &case.node, sink_id, &dump) {
        return Some(e);
    }
    let parsed: PipelineDoc = match serde_json::from_value(doc_value) {
        Ok(d) => d,
        Err(e) => return Some(format!("pipeline did not load: {e}")),
    };
    let result =
        engine.execute_pipeline_with_events(&parsed, Some(sink_id), Some("test"), |_| {});
    if result.status != "ok" {
        return Some(result.error.unwrap_or_else(|| "the run failed".into()));
    }
    let actual = match read_ndjson(&dump) {
        Ok(rows) => rows,
        // The node ran but wrote nothing readable. An empty expectation is
        // still a legitimate assertion, so only a non-empty one fails here.
        Err(e) if !case.rows.is_empty() => {
            return Some(format!("{} produced no rows to compare ({e})", case.node))
        }
        Err(_) => Vec::new(),
    };
    compare_with(&case.rows, &actual, case.coerce)
}

/// A file name that cannot escape the scratch folder or collide with a sibling.
fn safe_name(node: &str) -> String {
    node.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// Read the newline-delimited JSON a `snk.json` writes.
fn read_ndjson(path: &Path) -> Result<Vec<JsonValue>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(line).map_err(|e| format!("unreadable output row: {e}"))?);
    }
    Ok(out)
}

/// Add a JSON sink reading from `target`, so the run writes that node's whole
/// output somewhere this can read it back.
///
/// Additive and downstream-only: nothing upstream of the asserted node can see
/// it, so the pipeline under test still behaves exactly as it does in
/// production. Naming the target explicitly also gives a clear error when a
/// case asserts on a node that is not in the pipeline, which previously showed
/// up as "produced no rows".
fn attach_capture_sink(
    doc: &mut JsonValue,
    target: &str,
    sink_id: &str,
    out: &Path,
) -> Result<(), String> {
    let nodes = doc
        .get_mut("nodes")
        .and_then(JsonValue::as_array_mut)
        .ok_or_else(|| "pipeline has no nodes".to_string())?;
    if !nodes
        .iter()
        .any(|n| n.get("id").and_then(JsonValue::as_str) == Some(target))
    {
        return Err(format!("no node '{target}' in this pipeline"));
    }
    nodes.push(serde_json::json!({
        "id": sink_id,
        "position": { "x": 0, "y": 0 },
        "data": {
            "label": "test capture",
            "componentId": "snk.json",
            "properties": { "path": out.to_string_lossy().replace('\\', "/") }
        }
    }));
    let edges = doc
        .get_mut("edges")
        .and_then(JsonValue::as_array_mut)
        .ok_or_else(|| "pipeline has no edges".to_string())?;
    edges.push(serde_json::json!({
        "id": format!("{sink_id}_edge"),
        "source": target,
        "target": sink_id,
        "data": { "connectionType": "main" }
    }));
    Ok(())
}

/// `duckle test [<file.test.json> ...]`
pub fn run(duckdb: PathBuf) -> ExitCode {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut json_out = false;
    for arg in std::env::args().skip(2) {
        if arg == "--json" {
            json_out = true;
            continue;
        }
        if arg.starts_with('-') {
            eprintln!("duckle-runner test: unknown flag {arg}");
            return ExitCode::from(2);
        }
        paths.push(PathBuf::from(arg));
    }
    // Nothing named: every suite under ./tests, which is where a workspace keeps them.
    if paths.is_empty() {
        if let Ok(entries) = std::fs::read_dir("tests") {
            for e in entries.flatten() {
                let p = e.path();
                if p.to_string_lossy().ends_with(".test.json") {
                    paths.push(p);
                }
            }
            paths.sort();
        }
        if paths.is_empty() {
            eprintln!("duckle-runner test: nothing given and no *.test.json under ./tests");
            return ExitCode::from(2);
        }
    }

    let tmp = std::env::temp_dir().join(format!("duckle-test-{}", std::process::id()));
    if let Err(e) = std::fs::create_dir_all(&tmp) {
        eprintln!("duckle-runner test: cannot make a scratch folder: {e}");
        return ExitCode::from(2);
    }
    let engine = DuckdbEngine::new(duckdb);

    let (mut passed, mut failures) = (0usize, Vec::<Failure>::new());
    let mut results: Vec<JsonValue> = Vec::new();
    for path in &paths {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("duckle-runner test: cannot read {}: {e}", path.display());
                return ExitCode::from(2);
            }
        };
        let (pipeline, cases) = match parse(path, &text) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("duckle-runner test: {e}");
                return ExitCode::from(2);
            }
        };
        for case in &cases {
            let outcome = run_case(&engine, &pipeline, case, &tmp);
            match &outcome {
                None => {
                    passed += 1;
                    if !json_out {
                        println!("  ok    {}", case.name);
                    }
                }
                Some(why) => {
                    if !json_out {
                        println!("  FAIL  {}", case.name);
                        println!("        {why}");
                    }
                    failures.push(Failure { case: case.name.clone(), why: why.clone() });
                }
            }
            results.push(serde_json::json!({
                "suite": path.to_string_lossy(),
                "pipeline": pipeline.to_string_lossy(),
                "test": case.name,
                "node": case.node,
                "status": if outcome.is_none() { "pass" } else { "fail" },
                "assertion": outcome,
            }));
        }
    }
    let _ = std::fs::remove_dir_all(&tmp);

    if json_out {
        // One object, so an agent or a CI step reads a result rather than
        // scraping lines. The assertion text carries the value AND the type of
        // both sides, which is what makes a failure actionable without a rerun.
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "passed": passed,
                "failed": failures.len(),
                "results": results,
            }))
            .unwrap_or_default()
        );
        return if failures.is_empty() { ExitCode::from(0) } else { ExitCode::from(1) };
    }

    println!();
    println!("{passed} passed, {} failed", failures.len());
    // A failing assertion is a real finding about the pipeline, which is exit 1 - the
    // same code a failed run uses, so CI gates on it without special-casing.
    if failures.is_empty() { ExitCode::from(0) } else { ExitCode::from(1) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_case_compares_only_the_columns_it_names() {
        // A case that had to name every column would break the moment an unrelated one
        // was added upstream, so people would stop writing them.
        let want = vec![serde_json::json!({ "id": "1" })];
        let got = vec![serde_json::json!({ "id": "1", "extra": "ignored" })];
        assert_eq!(compare(&want, &got), None);
    }


    #[test]
    fn a_wrong_value_says_which_row_and_column() {
        let want = vec![serde_json::json!({ "id": "1", "amt": "5" })];
        let got = vec![serde_json::json!({ "id": "1", "amt": "6" })];
        let why = compare(&want, &got).expect("must fail");
        assert!(why.contains("row 1"), "{why}");
        assert!(why.contains("amt"), "{why}");
        // The failure names the TYPE as well as the value now: "expected [5],
        // got [5]" is the least useful thing a typed comparison can say.
        assert!(why.contains("\"5\"") && why.contains("\"6\""), "{why}");
        assert!(why.contains("string"), "the type belongs in the message: {why}");
    }

    #[test]
    fn a_different_number_of_rows_is_a_failure_on_its_own() {
        let want = vec![serde_json::json!({ "id": "1" })];
        assert!(compare(&want, &[]).unwrap().contains("expected 1 row(s), got 0"));
    }

    #[test]
    fn giving_a_node_input_keeps_its_other_settings() {
        // The fixture has to exercise the REAL reader - its delimiter, its header
        // setting - or a case passes while the pipeline it stands for fails.
        let mut doc = serde_json::json!({
            "nodes": [{ "id": "s", "data": { "componentId": "src.csv", "properties": {
                "path": "/old.csv", "delimiter": ";", "hasHeader": true } } }]
        });
        apply_given(&mut doc, "s", "/tmp/given").unwrap();
        let p = &doc["nodes"][0]["data"]["properties"];
        assert_eq!(p["path"], "/tmp/given");
        assert_eq!(p["delimiter"], ";", "the reader's own settings survive");
        assert_eq!(p["hasHeader"], true);
    }

    #[test]
    fn a_node_that_is_not_there_is_said_plainly() {
        let mut doc = serde_json::json!({ "nodes": [] });
        let e = apply_given(&mut doc, "nope", "/tmp/x").unwrap_err();
        assert!(e.contains("nope"), "{e}");
    }

    #[test]
    fn a_test_file_resolves_its_pipeline_beside_itself() {
        // So a suite can sit next to what it covers and still be run from anywhere.
        let text = r#"{"pipeline":"p.json","cases":[
            {"name":"c","expect":{"node":"n","rows":[]}}]}"#;
        let (p, cases) = parse(Path::new("suites/orders.test.json"), text).unwrap();
        assert_eq!(p, Path::new("suites").join("p.json"));
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].node, "n");
    }

    #[test]
    fn a_missing_piece_names_the_file_and_the_case() {
        let e = parse(Path::new("s/x.test.json"), r#"{"cases":[]}"#).unwrap_err();
        assert!(e.contains("x.test.json") && e.contains("pipeline"), "{e}");
        let e = parse(
            Path::new("s/x.test.json"),
            r#"{"pipeline":"p.json","cases":[{"name":"c","expect":{"rows":[]}}]}"#,
        )
        .unwrap_err();
        assert!(e.contains("c") && e.contains("node"), "{e}");
    }

    /// #250: `run_case` compared against the run PREVIEW, which the engine caps
    /// at 100 rows. A case expecting 100 rows therefore passed while the node
    /// actually produced 101, because only the first 100 ever reached the
    /// comparator. A test framework that silently passes is worse than none.
    #[test]
    fn a_result_longer_than_the_preview_cap_is_not_silently_truncated() {
        let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
            eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let pipeline = dir.path().join("p.json");
        // 101 rows out of one node - one more than the preview cap.
        std::fs::write(
            &pipeline,
            r#"{"nodes":[{"id":"g","position":{"x":0,"y":0},"data":{"label":"g",
               "componentId":"code.sql","properties":{
               "sql":"SELECT i AS id FROM range(101) t(i)"}}}],"edges":[]}"#,
        )
        .unwrap();

        let engine = DuckdbEngine::new(PathBuf::from(&bin));
        // The expectation names 100 rows, which is exactly what the capped
        // preview used to hand over.
        let rows: Vec<JsonValue> = (0..100).map(|i| serde_json::json!({ "id": i })).collect();
        let case = Case {
            name: "cap".into(),
            given: Vec::new(),
            node: "g".into(),
            rows,
            coerce: false,
        };
        let why = run_case(&engine, &pipeline, &case, dir.path());
        assert!(
            why.as_deref().map(|w| w.contains("101")).unwrap_or(false),
            "a 101-row result must not satisfy a 100-row expectation: {why:?}"
        );
    }

    /// The whole relation is compared, so a case CAN legitimately assert on more
    /// rows than the preview would ever have shown.
    #[test]
    fn a_case_can_assert_on_more_rows_than_the_preview_holds() {
        let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
            eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let pipeline = dir.path().join("p.json");
        std::fs::write(
            &pipeline,
            r#"{"nodes":[{"id":"g","position":{"x":0,"y":0},"data":{"label":"g",
               "componentId":"code.sql","properties":{
               "sql":"SELECT i AS id FROM range(150) t(i)"}}}],"edges":[]}"#,
        )
        .unwrap();
        let engine = DuckdbEngine::new(PathBuf::from(&bin));
        let rows: Vec<JsonValue> = (0..150).map(|i| serde_json::json!({ "id": i })).collect();
        let case = Case {
            name: "all".into(),
            given: Vec::new(),
            node: "g".into(),
            rows,
            coerce: false,
        };
        assert_eq!(
            run_case(&engine, &pipeline, &case, dir.path()),
            None,
            "150 rows should compare against 150 rows"
        );
    }

    /// A case naming a node that is not in the pipeline used to look like a node
    /// that produced nothing, which sent people looking at their data.
    #[test]
    fn a_case_naming_a_node_that_does_not_exist_says_so() {
        let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| !b.is_empty()) else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let pipeline = dir.path().join("p.json");
        std::fs::write(
            &pipeline,
            r#"{"nodes":[{"id":"g","position":{"x":0,"y":0},"data":{"label":"g",
               "componentId":"code.sql","properties":{"sql":"SELECT 1 AS id"}}}],"edges":[]}"#,
        )
        .unwrap();
        let engine = DuckdbEngine::new(PathBuf::from(&bin));
        let case = Case {
            name: "typo".into(),
            given: Vec::new(),
            node: "gg".into(),
            rows: vec![serde_json::json!({ "id": 1 })],
            coerce: false,
        };
        let why = run_case(&engine, &pipeline, &case, dir.path()).unwrap_or_default();
        assert!(why.contains("no node 'gg'"), "{why}");
    }

    /// Strict by default. A number and its text are DIFFERENT, because a test
    /// that cannot tell them apart cannot catch a column that silently became a
    /// string - which is one of the most common things to get wrong in ETL.
    #[test]
    fn a_number_and_its_text_are_different_assertions_by_default() {
        let want = vec![serde_json::json!({ "n": "5" })];
        let got = vec![serde_json::json!({ "n": 5 })];
        let why = compare(&want, &got).unwrap_or_default();
        assert!(why.contains("string") && why.contains("number"), "say which is which: {why}");

        // ...and the same assertion passes when the case opts into text.
        assert_eq!(compare_with(&want, &got, true), None);
    }

    /// NULL, a missing field and an empty string are three different outcomes.
    /// Collapsing them hides a failed join, a dropped column and a parser that
    /// wrote "" where it meant nothing.
    #[test]
    fn null_and_missing_and_empty_are_three_different_things() {
        let null_row = vec![serde_json::json!({ "v": null })];
        let empty_row = vec![serde_json::json!({ "v": "" })];
        let absent_row = vec![serde_json::json!({ "other": 1 })];

        assert!(compare(&null_row, &empty_row).is_some(), "null is not an empty string");
        assert!(compare(&null_row, &absent_row).is_some(), "null is not a missing field");
        assert!(compare(&empty_row, &absent_row).is_some(), "an empty string is not missing");
        // Each still matches itself.
        assert_eq!(compare(&null_row, &null_row), None);
        assert_eq!(compare(&empty_row, &empty_row), None);
    }


    /// A source that does not read a file - S3 behind a connection, REST,
    /// DuckLake - ignored the fixture path entirely, so the test read
    /// PRODUCTION while looking like it read the fixture. Replacing the node
    /// with a reader for the fixture is what makes a credential-free test of
    /// such a pipeline possible at all.
    #[test]
    fn a_source_that_reads_no_file_is_replaced_by_a_reader_for_the_fixture() {
        let mut doc: JsonValue = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"lake",
               "componentId":"src.ducklake","properties":{
               "path":"", "tableName":"orders", "connectionRef":"prod-lake"}}}],"edges":[]}"#,
        )
        .unwrap();
        apply_given(&mut doc, "s", "/fixtures/orders.parquet").unwrap();
        let data = &doc["nodes"][0]["data"];
        assert_eq!(data["componentId"], serde_json::json!("src.parquet"));
        assert_eq!(data["properties"]["path"], serde_json::json!("/fixtures/orders.parquet"));
        assert!(
            data["properties"].get("connectionRef").is_none(),
            "the production connection must not survive the substitution"
        );
    }

    /// A file-reading source keeps everything else it has, because the point of
    /// a fixture is to exercise the real reader.
    #[test]
    fn a_file_source_keeps_its_settings_when_given_a_fixture() {
        let mut doc: JsonValue = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"csv",
               "componentId":"src.csv","properties":{"path":"prod.csv","delimiter":";",
               "hasHeader":false}}}],"edges":[]}"#,
        )
        .unwrap();
        apply_given(&mut doc, "s", "/fixtures/o.csv").unwrap();
        let props = &doc["nodes"][0]["data"]["properties"];
        assert_eq!(props["path"], serde_json::json!("/fixtures/o.csv"));
        assert_eq!(props["delimiter"], serde_json::json!(";"), "the real reader is the point");
        assert_eq!(props["hasHeader"], serde_json::json!(false));
        assert_eq!(doc["nodes"][0]["data"]["componentId"], serde_json::json!("src.csv"));
    }

    /// Inline text against a source that reads no file cannot be honoured, and
    /// saying so beats setting a path the reader ignores.
    #[test]
    fn inline_text_for_a_non_file_source_is_refused_rather_than_ignored() {
        let mut doc: JsonValue = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"rest",
               "componentId":"src.rest","properties":{"url":"https://api.example.com/x"}}}],
               "edges":[]}"#,
        )
        .unwrap();
        // A temp file with no recognised extension is what inline text becomes
        // when the fixture cannot be named.
        let err = apply_given(&mut doc, "s", "/tmp/given_s").unwrap_err();
        assert!(err.contains("src.rest") && err.contains(".parquet"), "{err}");
    }

}
