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
    /// #250: expected column types, as `{"id": "BIGINT", "day": "DATE"}`.
    ///
    /// A rendered-value comparison cannot see a type regression: DATE and
    /// VARCHAR both serialise to a JSON string, BIGINT and DECIMAL both to a
    /// JSON number. Those are exactly the regressions this exists for.
    pub schema: Vec<(String, String)>,
    /// #250: sort both sides by these columns before comparing.
    ///
    /// SQL without an explicit ORDER BY has no guaranteed order, so a CORRECT
    /// result set can start failing after an execution-plan change. Naming the
    /// columns makes the case deterministic without asserting an order the
    /// pipeline never promised.
    pub order_by: Vec<String>,
    /// #250: compare as a bag - sort both sides by their whole content.
    ///
    /// The blunt version of `order_by`, for a result with no natural key.
    pub unordered: bool,
    /// #250: how many rows the node must produce.
    ///
    /// Separate from listing them: a case that only cares about the count
    /// should not have to write out every row, and one that lists rows gets
    /// the count checked anyway.
    pub row_count: Option<usize>,
    /// #250: columns whose values must be distinct across the whole result.
    pub unique: Vec<String>,
    /// #250: columns that must have no NULL and no missing value.
    pub not_null: Vec<String>,
    /// #250: a SQL predicate over the result, with `{rows}` standing for it.
    ///
    /// The escape hatch for anything the fixed assertions do not cover -
    /// `SELECT max(amount) < 100 FROM {rows}`. It must return one row whose
    /// first column is true.
    pub sql: Option<String>,
    /// #250: how far two numbers may differ and still count as equal.
    ///
    /// A computed float that lands on 0.30000000000000004 is not a regression,
    /// and a test that fails on it teaches people to stop writing tests. Off by
    /// default, because an exact comparison is the right one until it is not.
    pub tolerance: Option<f64>,
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
        // #250: rows are optional once the case asserts something else. A case
        // that only cares about the row count, or a uniqueness property, should
        // not have to write out every row - that is the whole reason those
        // assertions exist. With nothing else asserted, an expectation still
        // has to say what it expects.
        let asserts_structure = ["rowCount", "unique", "notNull", "sql", "schema"]
            .iter()
            .any(|k| expect.get(*k).is_some());
        let rows = match expect.get("rows").and_then(JsonValue::as_array) {
            Some(r) => r.clone(),
            None if asserts_structure => Vec::new(),
            None => {
                return Err(format!(
                    "{name}: {label}: \"expect\" needs \"rows\", or one of rowCount / unique / notNull / sql / schema"
                ))
            }
        };
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
        // #250: expected column types, on the expectation beside the rows.
        let schema: Vec<(String, String)> = expect
            .get("schema")
            .and_then(JsonValue::as_object)
            .map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let order_by: Vec<String> = expect
            .get("orderBy")
            .and_then(JsonValue::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        let unordered = expect
            .get("unordered")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let strings = |k: &str| -> Vec<String> {
            expect
                .get(k)
                .and_then(JsonValue::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default()
        };
        cases.push(Case {
            name: label,
            given,
            node,
            rows,
            coerce,
            schema,
            order_by,
            unordered,
            row_count: expect
                .get("rowCount")
                .and_then(JsonValue::as_u64)
                .map(|n| n as usize),
            unique: strings("unique"),
            not_null: strings("notNull"),
            sql: expect
                .get("sql")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            tolerance: expect.get("tolerance").and_then(JsonValue::as_f64),
        });
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
/// #250: where a `given` value naming a file is looked for.
///
/// Beside the TEST file first, which is what the docs describe and where a
/// suite's fixtures naturally live. The pipeline's own directory stays as a
/// fallback so suites written against the older behaviour keep working, and a
/// path that resolves as given is honoured last.
///
/// `None` means the value is not a file at all, and the caller treats it as
/// inline text.
fn resolve_fixture(suite_dir: &Path, pipeline: &Path, body: &str) -> Option<PathBuf> {
    let by_suite = suite_dir.join(body);
    if by_suite.is_file() {
        return Some(by_suite);
    }
    if let Some(by_pipeline) = pipeline.parent().map(|d| d.join(body)) {
        if by_pipeline.is_file() {
            return Some(by_pipeline);
        }
    }
    let literal = PathBuf::from(body);
    literal.is_file().then_some(literal)
}

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
            // #250: both take a local path and both have real parser
            // configuration worth testing. Leaving them out meant a PDF or
            // HTML pipeline could not be covered by a fixture at all - the
            // `given` was accepted and then quietly ignored, which is worse
            // than refusing it.
            | "src.pdf"
            | "src.html"
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
fn same_cell(
    want: Option<&JsonValue>,
    got: Option<&JsonValue>,
    coerce: bool,
    tolerance: Option<f64>,
) -> bool {
    // #250: two numbers within the declared tolerance are the same number. A
    // computed float landing on 0.30000000000000004 is not a regression, and a
    // test that fails on it teaches people to stop writing tests.
    if let (Some(t), Some(w), Some(g)) = (tolerance, want, got) {
        if let (Some(a), Some(b)) = (w.as_f64(), g.as_f64()) {
            return (a - b).abs() <= t;
        }
    }
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

/// Like [`compare_with`], allowing two numbers within `tolerance` to match.
pub fn compare_within(
    expected: &[JsonValue],
    actual: &[JsonValue],
    coerce: bool,
    tolerance: Option<f64>,
) -> Option<String> {
    compare_inner(expected, actual, coerce, tolerance)
}

pub fn compare_with(expected: &[JsonValue], actual: &[JsonValue], coerce: bool) -> Option<String> {
    compare_inner(expected, actual, coerce, None)
}

fn compare_inner(
    expected: &[JsonValue],
    actual: &[JsonValue],
    coerce: bool,
    tolerance: Option<f64>,
) -> Option<String> {
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
            if !same_cell(Some(wv), got.get(k), coerce, tolerance) {
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
fn run_case(
    engine: &DuckdbEngine,
    pipeline: &Path,
    suite_dir: &Path,
    case: &Case,
    tmp: &Path,
) -> Option<String> {
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
            // #250: beside the TEST file first, which is what the docs describe
            // and where a suite's fixtures naturally live. The pipeline's own
            // directory stays as a fallback so suites written against the older
            // behaviour keep working rather than failing on a missing file.
            if let Some(found) = resolve_fixture(suite_dir, pipeline, body) {
                found
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
    // #250: types first. A row comparison cannot see DATE becoming VARCHAR or
    // BIGINT becoming DECIMAL - both sides render the same - so checking the
    // schema before the values means the failure names the real regression
    // instead of a confusing value mismatch downstream of it.
    if let Some(why) = check_schema(
        &case.schema,
        result.preview.iter().find(|p| p.node_id == case.node),
    ) {
        return Some(why);
    }
    // #250: make the comparison deterministic before making it. Both sides are
    // sorted by the same rule, so the case does not depend on an order SQL
    // never promised - and the author does not have to hand-sort the
    // expectation to match whatever the planner happened to produce.
    let (want, got) = order_for_compare(&case.rows, &actual, case);
    // #250: the cheap structural assertions before the row-by-row one, so a
    // failure names the property that broke rather than the first cell that
    // happened to differ because of it.
    if let Some(why) = check_assertions(case, &got) {
        return Some(why);
    }
    if let Some(why) = check_sql(engine, case, &dump) {
        return Some(why);
    }
    // A case that listed no rows but DID assert structure has already been
    // checked. Falling through would compare against an empty list and so
    // demand an empty result, which is not what "I only asserted the count"
    // means. With no structural assertion either, an empty expectation still
    // asserts an empty result, exactly as it always did.
    if case.rows.is_empty() && has_structural(case) {
        return None;
    }
    compare_within(&want, &got, case.coerce, case.tolerance)
}

/// Does this case assert anything other than its rows?
fn has_structural(case: &Case) -> bool {
    case.row_count.is_some()
        || !case.unique.is_empty()
        || !case.not_null.is_empty()
        || case.sql.is_some()
        || !case.schema.is_empty()
}

/// #250: row count, uniqueness and not-null, over the whole result.
pub fn check_assertions(case: &Case, rows: &[JsonValue]) -> Option<String> {
    if let Some(want) = case.row_count {
        if rows.len() != want {
            return Some(format!("rowCount: expected {want}, got {}", rows.len()));
        }
    }
    for col in &case.not_null {
        for (i, row) in rows.iter().enumerate() {
            match row.get(col) {
                None => {
                    return Some(format!("notNull: row {} has no column {col:?}", i + 1))
                }
                Some(JsonValue::Null) => {
                    return Some(format!("notNull: {col:?} is null on row {}", i + 1))
                }
                Some(_) => {}
            }
        }
    }
    for col in &case.unique {
        let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            // A missing column is reported rather than skipped: "every value is
            // unique" is trivially true of a column that is not there, and a
            // silently true assertion is the failure mode this repo keeps
            // finding.
            let Some(v) = row.get(col) else {
                return Some(format!("unique: row {} has no column {col:?}", i + 1));
            };
            let k = v.to_string();
            if let Some(first) = seen.insert(k, i + 1) {
                return Some(format!(
                    "unique: {col:?} repeats {} on rows {first} and {}",
                    describe(Some(v)),
                    i + 1
                ));
            }
        }
    }
    None
}

/// #250: a SQL predicate over the captured result.
///
/// `{rows}` stands for the node's output. A placeholder rather than a magic
/// table name, because rewriting an identifier inside SQL means parsing SQL,
/// and getting that subtly wrong would change the assertion being made.
fn check_sql(engine: &DuckdbEngine, case: &Case, dump: &Path) -> Option<String> {
    let Some(sql) = case.sql.as_deref() else {
        return None;
    };
    if !sql.contains("{rows}") {
        return Some(format!(
            "sql: the assertion must say {{rows}} somewhere, so it is clear what it runs against: {sql}"
        ));
    }
    let src = format!(
        "read_json_auto('{}', format='newline_delimited')",
        dump.display().to_string().replace('\\', "/").replace('\'', "''")
    );
    let query = sql.replace("{rows}", &src);
    match engine.query(&query, 2) {
        Err(e) => Some(format!("sql: {e}")),
        Ok(res) => {
            let first = res
                .rows
                .first()
                .and_then(|r| r.as_object())
                .and_then(|o| o.values().next().cloned());
            match first {
                Some(JsonValue::Bool(true)) => None,
                Some(other) => Some(format!(
                    "sql: expected true, got {}: {sql}",
                    describe(Some(&other))
                )),
                None => Some(format!("sql: returned no rows: {sql}")),
            }
        }
    }
}

/// #250: put both sides in one canonical order.
///
/// `order_by` sorts by the named columns; `unordered` sorts by the whole row.
/// Neither is on by default, so a case that means to assert the pipeline's own
/// ORDER BY still does.
///
/// The key is a list of rendered values compared lexicographically - canonical
/// rather than numeric, so 10 sorts before 9. That is deliberate: the goal is
/// that both sides agree, not that the order reads naturally. A missing column
/// contributes an empty entry rather than being skipped, so two rows differing
/// only in whether a key column is present still sort apart.
pub fn order_for_compare(
    expected: &[JsonValue],
    actual: &[JsonValue],
    case: &Case,
) -> (Vec<JsonValue>, Vec<JsonValue>) {
    if case.order_by.is_empty() && !case.unordered {
        return (expected.to_vec(), actual.to_vec());
    }
    let key = |row: &JsonValue| -> Vec<String> {
        if case.order_by.is_empty() {
            return vec![row.to_string()];
        }
        case.order_by
            .iter()
            .map(|c| row.get(c).map(|v| v.to_string()).unwrap_or_default())
            .collect()
    };
    let mut want = expected.to_vec();
    let mut got = actual.to_vec();
    want.sort_by_key(&key);
    got.sort_by_key(&key);
    (want, got)
}

/// #250: does the node's schema match what the case declared?
///
/// Only the columns named are checked, like the row comparison: a test that had
/// to list every column would break on an unrelated addition and stop being
/// written.
///
/// Precision and scale are NOT compared. The types come from DuckDB's DESCRIBE
/// mapped to Duckle's own set, so `DECIMAL(18,3)` and `DECIMAL(10,2)` both read
/// as decimal. That is a real limit and it is stated rather than implied - the
/// regressions this catches are the ones that cross a type family, which is
/// what a rendered-value comparison is blind to.
pub fn check_schema(
    want: &[(String, String)],
    preview: Option<&duckle_duckdb_engine::NodePreview>,
) -> Option<String> {
    if want.is_empty() {
        return None;
    }
    let Some(preview) = preview else {
        return Some("the node reported no schema to check against".to_string());
    };
    for (col, declared) in want {
        // A type name nobody recognises is a mistake in the TEST, and saying so
        // beats asserting something that cannot fail.
        let Some(expected) = duckle_duckdb_engine::parse_type_name(declared) else {
            return Some(format!(
                "schema: {col:?} is declared as {declared:?}, which is not a type name I know"
            ));
        };
        let Some(actual) = preview.columns.iter().find(|c| &c.name == col) else {
            let have: Vec<&str> = preview.columns.iter().map(|c| c.name.as_str()).collect();
            return Some(format!(
                "schema: no column {col:?}. The node has: {}",
                have.join(", ")
            ));
        };
        if actual.data_type != expected {
            return Some(format!(
                "schema: {col:?} is {}, expected {} (declared {declared:?})",
                actual.data_type.name(),
                expected.name()
            ));
        }
    }
    None
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
            // Fixtures resolve beside the .test.json, which is this path.
            let suite_dir = path.parent().unwrap_or(Path::new(".")).to_path_buf();
            let outcome = run_case(&engine, &pipeline, &suite_dir, case, &tmp);
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

    /// #250: a fixture named in a case lives beside the .test.json, which is
    /// what the docs describe. It used to resolve against the PIPELINE's
    /// directory instead, so a suite kept next to its fixtures - the layout the
    /// docs show - could not find them, and the case fell through to treating
    /// the filename as inline text.
    #[test]
    fn a_fixture_resolves_beside_the_test_file() {
        let dir = tempfile::tempdir().unwrap();
        let suite = dir.path().join("suite");
        let pipes = dir.path().join("pipelines");
        std::fs::create_dir_all(&suite).unwrap();
        std::fs::create_dir_all(&pipes).unwrap();
        // The fixture sits beside the test file, NOT beside the pipeline.
        std::fs::write(suite.join("orders.csv"), "id,amt
1,5
").unwrap();
        let pipeline = pipes.join("p.json");
        std::fs::write(
            &pipeline,
            serde_json::json!({
                "nodes": [{ "id": "s", "type": "source", "position": {"x":0,"y":0},
                    "data": { "label": "s", "componentId": "src.csv",
                              "properties": { "path": "/nope.csv", "hasHeader": true } } }],
                "edges": []
            })
            .to_string(),
        )
        .unwrap();

        assert!(
            !pipeline.parent().unwrap().join("orders.csv").is_file(),
            "deliberately NOT beside the pipeline - that is the case that broke"
        );
        // The real resolver, not a restatement of it.
        let found = resolve_fixture(&suite, &pipeline, "orders.csv")
            .expect("a fixture beside the test file must be found");
        assert_eq!(found, suite.join("orders.csv"));

        // The pipeline's directory still works, so older suites keep running.
        std::fs::write(pipes.join("other.csv"), "id
1
").unwrap();
        assert_eq!(
            resolve_fixture(&suite, &pipeline, "other.csv").unwrap(),
            pipes.join("other.csv")
        );

        // And a value that names no file at all is inline text, not a path.
        assert!(resolve_fixture(&suite, &pipeline, "id,amt
1,5
").is_none());
    }

    /// #250: the regressions a rendered-value comparison is blind to.
    ///
    /// DATE and VARCHAR both serialise to a JSON string; BIGINT and DECIMAL
    /// both to a JSON number. A case comparing values passes through both.
    #[test]
    fn a_type_regression_a_value_comparison_cannot_see_is_caught() {
        use duckle_duckdb_engine::NodePreview;
        use duckle_duckdb_engine::{Column, DataType};
        let col = |name: &str, t: DataType| Column {
            name: name.to_string(),
            data_type: t,
            nullable: true,
            primary_key: None,
            format: None,
        };
        let preview = NodePreview {
            node_id: "x".into(),
            // The regression: a DATE that became text, and a BIGINT that
            // became a decimal.
            columns: vec![col("day", DataType::String), col("n", DataType::Decimal)],
            rows: vec![],
        };
        let want = vec![("day".to_string(), "DATE".to_string())];
        let why = check_schema(&want, Some(&preview)).expect("must catch DATE -> VARCHAR");
        assert!(why.contains("day"), "{why}");
        assert!(why.contains("date") && why.contains("string"), "both types: {why}");

        let want = vec![("n".to_string(), "BIGINT".to_string())];
        let why = check_schema(&want, Some(&preview)).expect("must catch BIGINT -> DECIMAL");
        assert!(why.contains("int64") && why.contains("decimal"), "{why}");

        // And it passes when the types are what was declared.
        let ok = vec![
            ("day".to_string(), "VARCHAR".to_string()),
            ("n".to_string(), "decimal".to_string()),
        ];
        assert_eq!(check_schema(&ok, Some(&preview)), None, "both vocabularies");
    }

    /// A type name nobody recognises is a mistake in the TEST. Mapping it to
    /// VARCHAR - which the engine's own mapper does by falling through - would
    /// make the assertion pass against any text column, and an assertion that
    /// cannot fail is worse than none.
    #[test]
    fn an_unknown_type_name_is_an_error_not_a_silent_varchar() {
        use duckle_duckdb_engine::NodePreview;
        use duckle_duckdb_engine::{Column, DataType};
        let preview = NodePreview {
            node_id: "x".into(),
            columns: vec![Column {
                name: "s".into(),
                data_type: DataType::String,
                nullable: true,
                primary_key: None,
                format: None,
            }],
            rows: vec![],
        };
        let want = vec![("s".to_string(), "VARCHARR".to_string())];
        let why = check_schema(&want, Some(&preview)).expect("a typo must not pass");
        assert!(why.contains("not a type name"), "{why}");
    }

    #[test]
    fn a_missing_column_names_what_the_node_does_have() {
        use duckle_duckdb_engine::NodePreview;
        use duckle_duckdb_engine::{Column, DataType};
        let preview = NodePreview {
            node_id: "x".into(),
            columns: vec![Column {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: true,
                primary_key: None,
                format: None,
            }],
            rows: vec![],
        };
        let want = vec![("nope".to_string(), "BIGINT".to_string())];
        let why = check_schema(&want, Some(&preview)).expect("must fail");
        assert!(why.contains("nope") && why.contains("id"), "{why}");
    }

    /// Only the columns named are checked, like the row comparison - otherwise
    /// an unrelated new column breaks every test and people stop writing them.
    #[test]
    fn a_schema_assertion_ignores_columns_it_does_not_name() {
        use duckle_duckdb_engine::NodePreview;
        use duckle_duckdb_engine::{Column, DataType};
        let preview = NodePreview {
            node_id: "x".into(),
            columns: vec![
                Column { name: "id".into(), data_type: DataType::Int64, nullable: true, primary_key: None, format: None },
                Column { name: "extra".into(), data_type: DataType::Json, nullable: true, primary_key: None, format: None },
            ],
            rows: vec![],
        };
        let want = vec![("id".to_string(), "BIGINT".to_string())];
        assert_eq!(check_schema(&want, Some(&preview)), None);
        // An empty declaration asserts nothing at all.
        assert_eq!(check_schema(&[], Some(&preview)), None);
    }

    fn case_with(order_by: &[&str], unordered: bool) -> Case {
        Case {
            name: "c".into(),
            given: Vec::new(),
            node: "n".into(),
            rows: Vec::new(),
            coerce: false,
            schema: Vec::new(),
            order_by: order_by.iter().map(|s| s.to_string()).collect(),
            unordered,
            row_count: None,
            unique: Vec::new(),
            not_null: Vec::new(),
            sql: None,
            tolerance: None,
        }
    }

    /// #250: SQL without an ORDER BY has no guaranteed order, so a CORRECT
    /// result can start failing after a plan change. Naming the key columns
    /// makes the case deterministic without asserting an order the pipeline
    /// never promised.
    #[test]
    fn order_by_makes_a_differently_ordered_result_compare_equal() {
        // Deliberately written in a DIFFERENT order from the result, and
        // neither side already sorted - so only sorting BOTH makes them agree.
        let want = vec![
            serde_json::json!({ "id": 2, "v": "b" }),
            serde_json::json!({ "id": 1, "v": "a" }),
        ];
        let got = vec![
            serde_json::json!({ "id": 1, "v": "a" }),
            serde_json::json!({ "id": 2, "v": "b" }),
        ];
        // Without it, the case fails purely on order.
        let plain = case_with(&[], false);
        let (w, g) = order_for_compare(&want, &got, &plain);
        assert!(compare_with(&w, &g, false).is_some(), "unordered compare must differ");

        let sorted = case_with(&["id"], false);
        let (w, g) = order_for_compare(&want, &got, &sorted);
        assert_eq!(compare_with(&w, &g, false), None, "orderBy must make it agree");
    }

    /// A genuinely different result must still fail - sorting is not a way of
    /// making any two sets equal.
    #[test]
    fn order_by_does_not_hide_a_real_difference() {
        let want = vec![serde_json::json!({ "id": 1, "v": "a" })];
        let got = vec![serde_json::json!({ "id": 1, "v": "CHANGED" })];
        let c = case_with(&["id"], false);
        let (w, g) = order_for_compare(&want, &got, &c);
        let why = compare_with(&w, &g, false).expect("a changed value must still fail");
        assert!(why.contains("v"), "{why}");
    }

    /// The blunt version, for a result with no natural key.
    #[test]
    fn unordered_compares_as_a_bag() {
        let want = vec![
            serde_json::json!({ "v": "b" }),
            serde_json::json!({ "v": "a" }),
        ];
        let got = vec![
            serde_json::json!({ "v": "a" }),
            serde_json::json!({ "v": "b" }),
        ];
        let c = case_with(&[], true);
        let (w, g) = order_for_compare(&want, &got, &c);
        assert_eq!(compare_with(&w, &g, false), None);

        // A missing row is still a failure, not merely a different order.
        let short = vec![serde_json::json!({ "v": "a" })];
        let (w, g) = order_for_compare(&want, &short, &c);
        assert!(compare_with(&w, &g, false).is_some());
    }

    /// Neither is on by default, so a case that means to assert the pipeline's
    /// own ORDER BY still does.
    #[test]
    fn ordering_is_off_unless_asked_for() {
        let want = vec![
            serde_json::json!({ "id": 2 }),
            serde_json::json!({ "id": 1 }),
        ];
        let got = vec![
            serde_json::json!({ "id": 1 }),
            serde_json::json!({ "id": 2 }),
        ];
        let c = case_with(&[], false);
        let (w, g) = order_for_compare(&want, &got, &c);
        assert_eq!(w, want, "untouched");
        assert_eq!(g, got, "untouched");
        assert!(compare_with(&w, &g, false).is_some(), "order still asserted");
    }

    fn case_asserting(f: impl FnOnce(&mut Case)) -> Case {
        let mut c = case_with(&[], false);
        f(&mut c);
        c
    }

    #[test]
    fn row_count_is_asserted_without_listing_every_row() {
        let rows = vec![
            serde_json::json!({ "id": 1 }),
            serde_json::json!({ "id": 2 }),
        ];
        let c = case_asserting(|c| c.row_count = Some(2));
        assert_eq!(check_assertions(&c, &rows), None);
        let c = case_asserting(|c| c.row_count = Some(3));
        let why = check_assertions(&c, &rows).expect("a wrong count must fail");
        assert!(why.contains("expected 3") && why.contains("got 2"), "{why}");
    }

    #[test]
    fn not_null_catches_a_null_and_a_missing_column() {
        let c = case_asserting(|c| c.not_null = vec!["v".into()]);
        assert_eq!(
            check_assertions(&c, &[serde_json::json!({ "v": 1 })]),
            None
        );
        let why = check_assertions(&c, &[serde_json::json!({ "v": JsonValue::Null })])
            .expect("a null must fail");
        assert!(why.contains("null") && why.contains("row 1"), "{why}");
        // Absent is not the same as null, and both fail this assertion.
        let why = check_assertions(&c, &[serde_json::json!({ "other": 1 })])
            .expect("a missing column must fail");
        assert!(why.contains("no column"), "{why}");
    }

    #[test]
    fn unique_names_both_rows_that_collide() {
        let c = case_asserting(|c| c.unique = vec!["id".into()]);
        let ok = vec![
            serde_json::json!({ "id": 1 }),
            serde_json::json!({ "id": 2 }),
        ];
        assert_eq!(check_assertions(&c, &ok), None);
        let dup = vec![
            serde_json::json!({ "id": 7 }),
            serde_json::json!({ "id": 9 }),
            serde_json::json!({ "id": 7 }),
        ];
        let why = check_assertions(&c, &dup).expect("a repeat must fail");
        assert!(why.contains("rows 1 and 3"), "it must say WHICH rows: {why}");
    }

    /// "Every value is unique" is trivially true of a column that is not
    /// there, and a silently true assertion is the failure mode this repo
    /// keeps finding.
    #[test]
    fn unique_on_a_column_that_is_not_there_is_an_error() {
        let c = case_asserting(|c| c.unique = vec!["nope".into()]);
        let why = check_assertions(&c, &[serde_json::json!({ "id": 1 })])
            .expect("must not pass vacuously");
        assert!(why.contains("no column"), "{why}");
    }

    /// #250: a computed float landing on 0.30000000000000004 is not a
    /// regression, and a test that fails on it teaches people to stop writing
    /// tests.
    #[test]
    fn tolerance_accepts_float_noise_and_still_rejects_a_real_change() {
        let want = vec![serde_json::json!({ "amt": 0.3 })];
        let got = vec![serde_json::json!({ "amt": 0.30000000000000004 })];
        assert!(
            compare_within(&want, &got, false, None).is_some(),
            "exact comparison is still the default"
        );
        assert_eq!(compare_within(&want, &got, false, Some(1e-9)), None);
        // A difference bigger than the tolerance is still a failure.
        let moved = vec![serde_json::json!({ "amt": 0.4 })];
        assert!(compare_within(&want, &moved, false, Some(1e-9)).is_some());
    }

    #[test]
    fn a_pdf_or_html_source_can_be_given_a_fixture() {
        // #250: both take a local path, so a `given` must be able to replace it.
        // Leaving them out accepted the given and then ignored it, which is
        // worse than refusing it.
        assert!(reads_a_path("src.pdf"), "src.pdf takes a local path");
        assert!(reads_a_path("src.html"), "src.html takes a local path");
        // A source that does NOT read a path must still be refused.
        assert!(!reads_a_path("src.postgres"));
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
            schema: Vec::new(),
            order_by: Vec::new(),
            unordered: false,
            row_count: None,
            unique: Vec::new(),
            not_null: Vec::new(),
            sql: None,
            tolerance: None,
        };
        let why = run_case(&engine, &pipeline, dir.path(), &case, dir.path());
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
            schema: Vec::new(),
            order_by: Vec::new(),
            unordered: false,
            row_count: None,
            unique: Vec::new(),
            not_null: Vec::new(),
            sql: None,
            tolerance: None,
        };
        assert_eq!(
            run_case(&engine, &pipeline, dir.path(), &case, dir.path()),
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
            schema: Vec::new(),
            order_by: Vec::new(),
            unordered: false,
            row_count: None,
            unique: Vec::new(),
            not_null: Vec::new(),
            sql: None,
            tolerance: None,
        };
        let why = run_case(&engine, &pipeline, dir.path(), &case, dir.path()).unwrap_or_default();
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
