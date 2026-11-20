//! The text sinks' quoting and escaping, the raw text sink, the SQL source and
//! sink, and what a default SQL node says about a statement that returns no
//! rows: the asks of someone writing raw text and running their own COPY.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::io::Read;
use std::path::Path;

fn engine() -> Option<DuckdbEngine> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok()?;
    let p = std::path::PathBuf::from(bin);
    p.exists().then(|| DuckdbEngine::new(p))
}

macro_rules! engine_or_skip {
    () => {
        match engine() {
            Some(e) => e,
            None => {
                eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a duckdb CLI to run");
                return;
            }
        }
    };
}

fn norm(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({"id": id, "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": id, "componentId": component, "properties": props}})
}

fn edge(id: &str, a: &str, b: &str) -> Value {
    json!({"id": id, "source": a, "target": b, "sourceHandle": "main", "targetHandle": "main", "data": {"connectionType": "main"}})
}

fn doc(nodes: Value, edges: Value) -> PipelineDoc {
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

/// Values that quoting exists for: a quote, a tab, a line break, and a NULL.
const TRICKY: &str = "SELECT t.* FROM input, (VALUES (1, 'plain'), (2, 'has \"quote\"'), (3, 'has' || chr(9) || 'tab'), \
    (4, 'two' || chr(10) || 'lines'), (5, NULL)) t(id, v) ORDER BY id";

/// What `sql` selects (over a one-row `input`), handed to `sink` with `props`.
fn pipeline(dir: &Path, sql: &str, sink: &str, props: Value) -> PipelineDoc {
    let kick = dir.join("kick.csv");
    std::fs::write(&kick, "k\n1\n").unwrap();
    doc(
        json!([
            node("s", "src.csv", json!({ "path": norm(&kick), "hasHeader": true })),
            node("q", "code.sql", json!({ "sql": sql })),
            node("w", sink, props),
        ]),
        json!([edge("e1", "s", "q"), edge("e2", "q", "w")]),
    )
}

/// The run as a pipeline of SQL stages takes it: batched, in one session.
fn write(engine: &DuckdbEngine, dir: &Path, sql: &str, sink: &str, props: Value) -> RunResult {
    engine.execute_pipeline(&pipeline(dir, sql, sink, props))
}

/// The same, stage by stage, which naming a target forces. The two paths count
/// a sink's rows and publish its file differently, so a sink is tried on both.
fn write_per_stage(engine: &DuckdbEngine, dir: &Path, sql: &str, sink: &str, props: Value) -> RunResult {
    engine.execute_pipeline_with_events(&pipeline(dir, sql, sink, props), Some("w"), None, |_| {})
}

type Run = fn(&DuckdbEngine, &Path, &str, &str, Value) -> RunResult;
const BOTH_PATHS: [Run; 2] = [write, write_per_stage];

fn text(path: &Path) -> String {
    String::from_utf8(std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn a_tsv_sink_writes_values_as_they_are_with_quoting_and_escaping_off() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("raw.tsv");
    let sql = TRICKY.replace("SELECT t.*", "SELECT t.v");
    for run in BOTH_PATHS {
        let _ = std::fs::remove_file(&out);
        let r = run(&engine, tmp.path(), &sql, "snk.tsv", json!({ "path": norm(&out), "hasHeader": false, "quoteChar": "", "escapeChar": "" }));
        assert_eq!(r.status, "ok", "snk.tsv failed: {:?}", r.error);
        assert_eq!(text(&out), "plain\nhas \"quote\"\nhas\ttab\ntwo\nlines\n\n", "no quote, no escape: each value as it is");
        assert_eq!(r.nodes.get("w").and_then(|n| n.rows), Some(5), "five rows, though the file has six line breaks");
    }
    // A TSV is tab-delimited whatever an older pipeline's delimiter says.
    let both = tmp.path().join("both.tsv");
    let r = write(&engine, tmp.path(), "SELECT 'a' AS x, 'b' AS y FROM input", "snk.tsv", json!({ "path": norm(&both), "hasHeader": false, "delimiter": ";" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(text(&both), "a\tb\n");
}

#[test]
fn a_csv_sink_quotes_and_escapes_with_the_characters_it_is_given() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let sql = "SELECT t.v FROM input, (VALUES (1, 'a,b'), (2, 'it''s'), (3, 'plain')) t(id, v) ORDER BY id";
    let single = tmp.path().join("single.csv");
    let r = write(&engine, tmp.path(), sql, "snk.csv", json!({ "path": norm(&single), "writeHeader": false, "quoteChar": "'", "escapeChar": "\\" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(text(&single), "'a,b'\n'it\\'s'\nplain\n");
    // With no escape named, a quote is escaped by itself: DuckDB's own default
    // would escape it with a double quote and write 'it"'s'.
    let repeated = tmp.path().join("repeated.csv");
    let r = write(&engine, tmp.path(), sql, "snk.csv", json!({ "path": norm(&repeated), "writeHeader": false, "quoteChar": "'" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(text(&repeated), "'a,b'\n'it''s'\nplain\n");
    // What the form writes by default, and what an older pipeline has: nothing.
    let usual = norm(&tmp.path().join("usual.csv"));
    for props in [
        json!({ "path": usual, "writeHeader": false, "quoteChar": "\"", "escapeChar": "" }),
        json!({ "path": usual, "writeHeader": false }),
    ] {
        let r = write(&engine, tmp.path(), "SELECT 'a\"b' AS v FROM input", "snk.csv", props.clone());
        assert_eq!(r.status, "ok", "{:?}", r.error);
        assert_eq!(text(Path::new(&usual)), "\"a\"\"b\"\n", "{props}: a double quote, doubled");
    }
}

#[test]
fn a_text_sink_writes_one_column_raw_one_line_a_row() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("lines.txt");
    for run in BOTH_PATHS {
        let _ = std::fs::remove_file(&out);
        let r = run(&engine, tmp.path(), TRICKY, "snk.text", json!({ "path": norm(&out), "column": "v" }));
        assert_eq!(r.status, "ok", "snk.text failed: {:?}", r.error);
        assert_eq!(text(&out), "plain\nhas \"quote\"\nhas\ttab\ntwo\nlines\n\n", "the column alone, as it is, a NULL as an empty line");
        assert_eq!(r.nodes.get("w").and_then(|n| n.rows), Some(5), "one row a value, the line break notwithstanding");
    }
    let gz = tmp.path().join("lines.txt.gz");
    let r = write(&engine, tmp.path(), TRICKY, "snk.text", json!({ "path": norm(&gz), "column": "id" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let mut unzipped = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(&gz).unwrap()).read_to_string(&mut unzipped).unwrap();
    assert_eq!(unzipped, "1\n2\n3\n4\n5\n", "a number as its text, gzip for a .gz name");
    let r = write(&engine, tmp.path(), TRICKY, "snk.text", json!({ "path": norm(&out) }));
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("column"), "{:?}", r.error);
}

#[test]
fn a_sql_sink_runs_statements_with_the_upstream_as_input() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("copied.txt");
    let copy = format!("COPY (SELECT v FROM input ORDER BY id) TO '{}' (FORMAT CSV, HEADER false, QUOTE '', ESCAPE '')", norm(&out));
    for run in BOTH_PATHS {
        let _ = std::fs::remove_file(&out);
        let r = run(&engine, tmp.path(), TRICKY, "snk.sql", json!({ "sql": copy }));
        assert_eq!(r.status, "ok", "snk.sql failed: {:?}", r.error);
        assert_eq!(text(&out), "plain\nhas \"quote\"\nhas\ttab\ntwo\nlines\n\n");
    }
    let counted = tmp.path().join("counted.csv");
    let two = format!(
        "CREATE TEMP TABLE n AS SELECT count(*) AS rows FROM input;\nCOPY n TO '{}' (FORMAT CSV, HEADER false);",
        norm(&counted)
    );
    let r = write(&engine, tmp.path(), TRICKY, "snk.sql", json!({ "sql": two }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(text(&counted), "5\n", "several statements, in order");
    let r = write(&engine, tmp.path(), TRICKY, "snk.sql", json!({ "sql": "  " }));
    assert_eq!(r.status, "error");
}

#[test]
fn a_sql_sink_leaves_no_input_behind_for_a_later_stage() {
    // The upstream is `input` only inside the SQL sink. A node called input (an
    // SQL name a source may well be given), read by a stage after it in the same
    // session, is still that node.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.csv");
    std::fs::write(&a, "n\n1\n2\n3\n").unwrap();
    let b = tmp.path().join("b.csv");
    std::fs::write(&b, "n\n9\n").unwrap();
    let copied = norm(&tmp.path().join("copied.csv"));
    let out = norm(&tmp.path().join("out.csv"));
    let r = engine.execute_pipeline(&doc(
        json!([
            node("input", "src.csv", json!({ "path": norm(&a), "hasHeader": true })),
            node("other", "src.csv", json!({ "path": norm(&b), "hasHeader": true })),
            node("k1", "snk.sql", json!({ "sql": format!("COPY input TO '{copied}' (FORMAT CSV, HEADER false)") })),
            node("k2", "snk.csv", json!({ "path": out, "writeHeader": false })),
        ]),
        json!([edge("e1", "other", "k1"), edge("e2", "input", "k2")]),
    ));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(text(Path::new(&copied)), "9\n", "inside the sink, input is its upstream");
    assert_eq!(text(Path::new(&out)), "1\n2\n3\n", "after it, input is the node of that name again");
}

#[test]
fn a_sql_source_reads_what_its_query_returns() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("people.csv");
    std::fs::write(&csv, "name,age\nada,36\ngrace,45\n").unwrap();
    let out = norm(&tmp.path().join("out.parquet"));
    let read = |sql: &str| {
        engine.execute_pipeline(&doc(
            json!([node("r", "src.sql", json!({ "sql": sql })), node("k", "snk.parquet", json!({ "path": out }))]),
            json!([edge("e1", "r", "k")]),
        ))
    };
    let r = read(&format!("SELECT name, age FROM read_csv('{}') ORDER BY age", norm(&csv)));
    assert_eq!(r.status, "ok", "src.sql failed: {:?}", r.error);
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").unwrap();
    let got = std::process::Command::new(bin)
        .args([":memory:", "-json", "-c", &format!("SELECT name, age FROM read_parquet('{out}')")])
        .output()
        .unwrap();
    let rows: Vec<Value> = serde_json::from_str(String::from_utf8_lossy(&got.stdout).trim()).unwrap();
    assert_eq!(rows, [json!({"name": "ada", "age": 36}), json!({"name": "grace", "age": 45})]);
    assert_eq!(read("").status, "error", "a source needs its query");
}

#[test]
fn a_default_sql_node_says_where_a_statement_that_returns_no_rows_belongs() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let sink = norm(&tmp.path().join("ignored.parquet"));
    let error_of = |props: Value| -> String {
        let r = engine.execute_pipeline(&doc(
            json!([node("q", "code.sql", props), node("k", "snk.parquet", json!({ "path": sink }))]),
            json!([edge("e1", "q", "k")]),
        ));
        assert_eq!(r.status, "error", "a statement that returns no rows cannot make a table");
        format!("{:?}", r.error)
    };
    let copy = error_of(json!({ "sql": "-- write the rows out\nCOPY (SELECT 1) TO 'x.csv'" }));
    assert!(copy.contains("SQL sink"), "{copy}");
    let install = error_of(json!({ "sql": "INSTALL h3 FROM community" }));
    assert!(install.contains("Load extensions"), "{install}");
    // Pure SQL runs a statement as it is, as it always has.
    let file = norm(&tmp.path().join("pure.csv"));
    let pure = engine.execute_pipeline(&doc(
        json!([node("q", "code.sql", json!({ "sql": format!("COPY (SELECT 1 AS x) TO '{file}' (FORMAT CSV)"), "pureSql": true }))]),
        json!([]),
    ));
    assert_eq!(pure.status, "ok", "{:?}", pure.error);
    assert!(Path::new(&file).exists());
}
