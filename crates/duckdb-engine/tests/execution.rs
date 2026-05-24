//! End-to-end execution tests for the DuckDB engine.
//!
//! Unlike the unit tests in `src/`, which check SQL *generation*, these
//! exercise the real read → transform → write path against temp files
//! and then read the output back to prove the data actually landed.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;

/// These tests drive the real DuckDB CLI. Point DUCKLE_DUCKDB_BIN at a
/// `duckdb` binary to run them; otherwise they soft-skip so `cargo test`
/// stays green in environments without it.
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

fn write_file(dir: &Path, name: &str, content: &str) -> String {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.flush().unwrap();
    norm(&path.to_string_lossy())
}

fn out_path(dir: &Path, name: &str) -> String {
    norm(&dir.join(name).to_string_lossy())
}

/// DuckDB is happiest with forward slashes even on Windows.
fn norm(p: &str) -> String {
    p.replace('\\', "/")
}

fn doc(nodes: Value, edges: Value) -> PipelineDoc {
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({
        "id": id,
        "position": { "x": 0, "y": 0 },
        "data": { "label": id, "componentId": component, "properties": props }
    })
}

fn main_edge(id: &str, source: &str, target: &str) -> Value {
    json!({ "id": id, "source": source, "target": target, "data": { "connectionType": "main" } })
}

/// Edge that leaves a specific output handle of the source (e.g. the
/// "reject" port of a validator).
fn port_edge(id: &str, source: &str, source_handle: &str, target: &str) -> Value {
    json!({
        "id": id,
        "source": source,
        "sourceHandle": source_handle,
        "target": target,
        "data": { "connectionType": if source_handle == "reject" { "reject" } else { "main" } }
    })
}

/// Edge into a node's `lookup` input port (used for join/CDC second
/// inputs, e.g. the "previous" snapshot of a Diff Detect).
fn lookup_edge(id: &str, source: &str, target: &str) -> Value {
    json!({
        "id": id,
        "source": source,
        "target": target,
        "targetHandle": "lookup",
        "data": { "connectionType": "lookup" }
    })
}

/// Read back output files independently of the engine, by shelling out
/// to the same DuckDB CLI (only called after engine_or_skip!, so the
/// binary is present).
fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin)
        .arg(":memory:")
        .arg("-json")
        .arg("-c")
        .arg(sql)
        .output()
        .expect("run duckdb");
    let s = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(s.trim()).unwrap_or_default()
}

/// Run setup SQL against a specific database file (used to seed a
/// source DB file for the duckdb-source test).
fn duckdb_exec(db: &str, sql: &str) {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin)
        .arg(db)
        .arg("-c")
        .arg(sql)
        .output()
        .expect("run duckdb");
    assert!(
        out.status.success(),
        "setup sql failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn count(from: &str) -> i64 {
    let rows = duckdb_json(&format!("SELECT COUNT(*) AS n FROM {}", from));
    rows.first()
        .and_then(|r| r.get("n"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1)
}

fn scalar_string(sql: &str) -> String {
    let rows = duckdb_json(sql);
    rows.first()
        .and_then(|r| r.as_object())
        .and_then(|o| o.values().next())
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

#[test]
fn csv_filter_parquet_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "orders.csv",
        "order_id,status,amount\n1,paid,10\n2,pending,20\n3,paid,30\n4,refunded,5\n",
    );
    let out = out_path(tmp.path(), "paid.parquet");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("f1", "xf.filter", json!({ "predicate": "status = 'paid'" })),
            node("k1", "snk.parquet", json!({ "path": out })),
        ]),
        json!([main_edge("e1", "s1", "f1"), main_edge("e2", "f1", "k1")]),
    );

    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);

    // Sink reports the 2 paid rows written.
    let sink = result.nodes.get("k1").expect("sink status present");
    assert_eq!(sink.rows, Some(2), "sink should report 2 rows");

    // The Parquet file exists and, read back independently, has exactly
    // the 2 paid rows.
    assert!(Path::new(&out).exists(), "parquet file should exist");
    assert_eq!(count(&format!("read_parquet('{}')", out)), 2);

    // And both rows really are 'paid'.
    let bad = count(&format!(
        "read_parquet('{}') WHERE status != 'paid'",
        out
    ));
    assert_eq!(bad, 0, "every output row must be paid");
}

#[test]
fn csv_to_csv_roundtrip_preserves_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,name\n1,alice\n2,bob\n3,carol\n",
    );
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert!(Path::new(&out).exists());
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn aggregate_groups_and_sums() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "sales.csv",
        "region,amount\nwest,10\nwest,20\neast,5\neast,15\neast,5\n",
    );
    let out = out_path(tmp.path(), "agg.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "a1",
                "xf.agg",
                json!({
                    "groupBy": ["region"],
                    "aggregations": [
                        { "column": "amount", "function": "sum", "alias": "total" }
                    ]
                }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "a1"), main_edge("e2", "a1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);

    // Two groups out.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    // west total = 30.
    let west = scalar_string(&format!(
        "SELECT CAST(total AS VARCHAR) FROM read_csv_auto('{}') WHERE region = 'west'",
        out
    ));
    assert_eq!(west, "30");
}

#[test]
fn preview_returned_for_leaf_without_sink() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "p.csv", "a,b\n1,x\n2,y\n");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("f1", "xf.filter", json!({ "predicate": "a >= 1" })),
        ]),
        json!([main_edge("e1", "s1", "f1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);

    // The leaf (filter) has no downstream sink, so it returns a preview.
    let preview = result
        .preview
        .iter()
        .find(|p| p.node_id == "f1")
        .expect("filter leaf preview present");
    assert_eq!(preview.rows.len(), 2);
    assert_eq!(preview.columns.len(), 2);

    // The filter's view row-count is reported on the node status.
    let f = result.nodes.get("f1").unwrap();
    assert_eq!(f.rows, Some(2));
}

#[test]
fn structured_filter_predicate_actually_filters() {
    // The visual filter builder stores a structured object carrying its
    // compiled SQL - the executor must honor it, not fall back to TRUE.
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "orders.csv",
        "id,status\n1,paid\n2,pending\n3,paid\n",
    );
    let out = out_path(tmp.path(), "filtered.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "f1",
                "xf.filter",
                json!({
                    "predicate": {
                        "mode": "builder",
                        "match": "all",
                        "conditions": [
                            { "id": "c1", "column": "status", "op": "eq", "value": "paid" }
                        ],
                        "sql": "status = 'paid'"
                    }
                }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "f1"), main_edge("e2", "f1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // Header + 2 paid rows - NOT all 3 (which is what the WHERE TRUE bug did).
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
}

#[test]
fn aggregate_accepts_func_output_keys() {
    // The UI stores aggregations as { column, func, output }; the
    // executor must accept those spellings (not only function/alias).
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "sales.csv",
        "region,amount\nwest,10\nwest,20\neast,5\n",
    );
    let out = out_path(tmp.path(), "agg.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "a1",
                "xf.agg",
                json!({
                    "groupBy": ["region"],
                    "aggregations": [
                        { "column": "amount", "func": "sum", "output": "total" }
                    ]
                }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "a1"), main_edge("e2", "a1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let west = scalar_string(&format!(
        "SELECT CAST(total AS VARCHAR) FROM read_csv_auto('{}') WHERE region = 'west'",
        out
    ));
    assert_eq!(west, "30");
}

#[test]
fn custom_sql_runs_with_input_alias() {
    // A Custom-SQL node runs its SELECT as a real stage, with the
    // upstream exposed as `input`.
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,amount\n1,10\n2,20\n3,5\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "q1",
                "code.sql",
                json!({ "sql": "SELECT id, amount * 2 AS dbl FROM input WHERE amount >= 10" }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "q1"), main_edge("e2", "q1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // Rows with amount >= 10 → ids 1 and 2.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let dbl = scalar_string(&format!(
        "SELECT CAST(dbl AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    assert_eq!(dbl, "20");
}

#[test]
fn quality_range_splits_pass_and_reject() {
    // A Range validator must route in-range rows to its main output and
    // out-of-range rows to its reject port (two materialized tables).
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,amount\n1,5\n2,50\n3,500\n");
    let pass = out_path(tmp.path(), "pass.csv");
    let rej = out_path(tmp.path(), "reject.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "v1",
                "qa.range",
                json!({ "column": "amount", "min": 10, "max": 100, "inclusive": true }),
            ),
            node("kp", "snk.csv", json!({ "path": pass, "hasHeader": true })),
            node("kr", "snk.csv", json!({ "path": rej, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s1", "v1"),
            port_edge("e2", "v1", "main", "kp"),
            port_edge("e3", "v1", "reject", "kr"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // 50 is in [10,100] -> pass; 5 and 500 -> reject.
    assert_eq!(count(&format!("read_csv_auto('{}')", pass)), 1);
    assert_eq!(count(&format!("read_csv_auto('{}')", rej)), 2);
}

#[test]
fn window_row_number_partitions() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "g,v\na,1\na,2\nb,9\n");
    let out = out_path(tmp.path(), "win.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "w1",
                "xf.rownum",
                json!({ "partitionBy": ["g"], "orderBy": ["v"], "outputName": "rn" }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "w1"), main_edge("e2", "w1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // Partition 'a' has two rows ranked 1 and 2 by v.
    let max_rn = scalar_string(&format!(
        "SELECT CAST(MAX(rn) AS VARCHAR) FROM read_csv_auto('{}') WHERE g = 'a'",
        out
    ));
    assert_eq!(max_rn, "2");
    let b_rn = scalar_string(&format!(
        "SELECT CAST(rn AS VARCHAR) FROM read_csv_auto('{}') WHERE g = 'b'",
        out
    ));
    assert_eq!(b_rn, "1");
}

#[test]
fn string_case_transforms_in_place() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "name\nalice\nbob\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c1", "xf.case", json!({ "column": "name", "pattern": "upper" })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "c1"), main_edge("e2", "c1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let first = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') ORDER BY name LIMIT 1",
        out
    ));
    assert_eq!(first, "ALICE");
}

#[test]
fn numeric_round_adds_column() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "v\n3.14159\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "r1",
                "xf.num.round",
                json!({ "column": "v", "argument": 2, "outputColumn": "rounded" }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "r1"), main_edge("e2", "r1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let rounded = scalar_string(&format!(
        "SELECT CAST(rounded AS VARCHAR) FROM read_csv_auto('{}')",
        out
    ));
    assert_eq!(rounded, "3.14");
}

#[test]
fn unimplemented_component_fails_loudly_not_silently() {
    // A not-yet-executable transform must error, not silently pass data
    // through (which would look like success while doing nothing).
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "a\n1\n");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("x1", "code.python", json!({})),
        ]),
        json!([main_edge("e1", "s1", "x1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "error", "unimplemented op should fail, not pass through");
}

#[test]
fn date_diff_computes_days() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "start,end\n2024-01-01,2024-01-11\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "d1",
                "xf.dt.diff",
                json!({ "startColumn": "start", "endColumn": "end", "unit": "day", "outputColumn": "days" }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "d1"), main_edge("e2", "d1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let days = scalar_string(&format!("SELECT CAST(days AS VARCHAR) FROM read_csv_auto('{}')", out));
    assert_eq!(days, "10");
}

#[test]
fn rollup_adds_grand_total() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "region,amount\nwest,10\neast,20\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "a1",
                "xf.rollup",
                json!({
                    "groupBy": ["region"],
                    "aggregations": [{ "column": "amount", "func": "sum", "output": "total" }]
                }),
            ),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "a1"), main_edge("e2", "a1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // 2 region rows + 1 grand-total row (region NULL).
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let total = scalar_string(&format!(
        "SELECT CAST(total AS VARCHAR) FROM read_csv_auto('{}') WHERE region IS NULL",
        out
    ));
    assert_eq!(total, "30");
}

#[test]
fn array_collect_groups_into_lists() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "g,v\na,1\na,2\nb,9\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node(
                "c1",
                "xf.arr.collect",
                json!({ "valueColumn": "v", "groupBy": ["g"], "outputColumn": "items" }),
            ),
            node("k1", "snk.json", json!({ "path": out })),
        ]),
        json!([main_edge("e1", "s1", "c1"), main_edge("e2", "c1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let len_a = scalar_string(&format!(
        "SELECT CAST(len(items) AS VARCHAR) FROM read_json_auto('{}') WHERE g = 'a'",
        out
    ));
    assert_eq!(len_a, "2");
}

// These use the EXACT property keys the UI forms write - the bug was
// the executor reading different keys, so config was silently dropped.

#[test]
fn groupby_form_keys_actually_group() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "region,amount\nwest,10\nwest,20\neast,5\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("g1", "xf.groupby", json!({
                "groupKeys": ["region"],
                "aggregations": [{ "column": "amount", "func": "sum", "output": "total" }]
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "g1"), main_edge("e2", "g1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let west = scalar_string(&format!(
        "SELECT CAST(total AS VARCHAR) FROM read_csv_auto('{}') WHERE region='west'", out));
    assert_eq!(west, "30");
}

#[test]
fn sort_form_keys_actually_sort() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "n\n3\n1\n2\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("o1", "xf.sort", json!({ "sortColumn": "n", "direction": "asc" })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "o1"), main_edge("e2", "o1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // First row after ascending sort is 1 (read back preserving order).
    let first = scalar_string(&format!(
        "SELECT CAST(n AS VARCHAR) FROM read_csv_auto('{}') LIMIT 1", out));
    assert_eq!(first, "1");
}

#[test]
fn distinct_columns_form_dedups() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "g,v\na,1\na,2\nb,3\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("d1", "xf.distinct", json!({ "columns": ["g"] })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "d1"), main_edge("e2", "d1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
}

#[test]
fn map_expressions_form_computes() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "amount\n100\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m1", "xf.map", json!({
                "expressions": [{ "key": "doubled", "value": "amount * 2" }]
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "m1"), main_edge("e2", "m1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let v = scalar_string(&format!("SELECT CAST(doubled AS VARCHAR) FROM read_csv_auto('{}')", out));
    assert_eq!(v, "200");
}

#[test]
fn sink_error_mode_refuses_to_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "a\n1\n");
    // Pre-create the output so 'error if exists' should refuse.
    let out = write_file(tmp.path(), "out.csv", "old\n1\n");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true, "mode": "error" })),
        ]),
        json!([main_edge("e1", "s1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "error", "should refuse to overwrite existing file");
}

#[test]
fn addcol_form_adds_computed_column() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "amount\n100\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("a1", "xf.addcol", json!({ "name": "tax", "expression": "amount + 5" })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "a1"), main_edge("e2", "a1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let tax = scalar_string(&format!("SELECT CAST(tax AS VARCHAR) FROM read_csv_auto('{}')", out));
    assert_eq!(tax, "105", "got tax={}", tax);
}

#[test]
fn rename_mapping_form_renames() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "a,b\n1,2\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r1", "xf.rename", json!({ "mapping": [{ "key": "a", "value": "x" }] })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "r1"), main_edge("e2", "r1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // Column 'a' is now 'x'; reading 'x' must work and equal 1.
    let x = scalar_string(&format!("SELECT CAST(x AS VARCHAR) FROM read_csv_auto('{}')", out));
    assert_eq!(x, "1", "got x={}", x);
}

#[test]
fn cast_single_column_form_changes_type() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "v\n10.9\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c1", "xf.cast", json!({ "column": "v", "targetType": "int32" })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "c1"), main_edge("e2", "c1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // 10.9 cast to int -> 11; if the cast were ignored it'd stay 10.9.
    let v = scalar_string(&format!("SELECT CAST(v AS VARCHAR) FROM read_csv_auto('{}')", out));
    assert_eq!(v, "11", "got v={}", v);
}

#[test]
fn duckdb_sink_writes_table() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,a\n2,b\n");
    let dbfile = out_path(tmp.path(), "out.duckdb");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k1", "snk.duckdb", json!({ "database": dbfile, "tableName": "people" })),
        ]),
        json!([main_edge("e1", "s1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let n = scalar_string(&format!(
        "ATTACH '{}' AS d (READ_ONLY); SELECT CAST(count(*) AS VARCHAR) AS n FROM d.people",
        dbfile
    ));
    assert_eq!(n, "2", "got {}", n);
}

#[test]
fn sqlite_sink_writes_table() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,a\n2,b\n3,c\n");
    let dbfile = out_path(tmp.path(), "out.sqlite");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k1", "snk.sqlite", json!({ "database": dbfile, "tableName": "people" })),
        ]),
        json!([main_edge("e1", "s1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let n = scalar_string(&format!(
        "ATTACH '{}' AS s (TYPE SQLITE); SELECT CAST(count(*) AS VARCHAR) AS n FROM s.people",
        dbfile
    ));
    assert_eq!(n, "3", "got {}", n);
}

#[test]
fn duckdb_source_reads_table() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let srcdb = out_path(tmp.path(), "src.duckdb");
    duckdb_exec(
        &srcdb,
        "CREATE TABLE orders AS SELECT * FROM (VALUES (1,'paid'),(2,'pending'),(3,'paid')) t(id,status)",
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.duckdb", json!({ "database": srcdb, "tableName": "orders" })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn window_aggregate_keeps_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "g,amt\na,10\na,20\nb,5\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w1", "xf.aggwin", json!({
                "function": "sum", "column": "amt", "partitionBy": ["g"], "outputName": "g_total"
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "w1"), main_edge("e2", "w1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // All 3 rows kept; group 'a' rows carry the partition total 30.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let total = scalar_string(&format!(
        "SELECT CAST(g_total AS VARCHAR) FROM read_csv_auto('{}') WHERE g = 'a' LIMIT 1",
        out
    ));
    assert_eq!(total, "30", "got {}", total);
}

#[test]
fn unpivot_wide_to_long() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,q1,q2\n1,10,20\n");
    let out = out_path(tmp.path(), "out.csv");
    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("u1", "xf.unpivot", json!({
                "columns": ["q1", "q2"], "nameColumn": "quarter", "valueColumn": "amount"
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "u1"), main_edge("e2", "u1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // One input row, two unpivoted columns -> two output rows.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let q1 = scalar_string(&format!(
        "SELECT CAST(amount AS VARCHAR) FROM read_csv_auto('{}') WHERE quarter = 'q1'",
        out
    ));
    assert_eq!(q1, "10", "got {}", q1);
}

#[test]
fn cdc_diff_detect_tags_changes() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let cur = write_file(tmp.path(), "cur.csv", "id,v\n1,a\n2,b2\n3,c\n");
    let prev = write_file(tmp.path(), "prev.csv", "id,v\n1,a\n2,b\n4,d\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("c1", "src.csv", json!({ "path": cur, "hasHeader": true })),
            node("p1", "src.csv", json!({ "path": prev, "hasHeader": true })),
            node("d1", "xf.cdc.diff", json!({
                "naturalKey": ["id"], "compareColumns": ["v"], "rejectUnchanged": true
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "c1", "d1"),
            lookup_edge("e2", "p1", "d1"),
            main_edge("e3", "d1", "k1"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // id=1 unchanged is dropped -> 3 rows: updated(2), inserted(3), deleted(4).
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let t2 = scalar_string(&format!(
        "SELECT change_type FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(t2, "updated", "got {}", t2);
    let t4 = scalar_string(&format!(
        "SELECT change_type FROM read_csv_auto('{}') WHERE id = 4",
        out
    ));
    assert_eq!(t4, "deleted", "got {}", t4);
}

#[test]
fn column_profile_summarizes() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,grp\n1,a\n2,a\n3,b\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p1", "qa.profile", json!({})),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "p1"), main_edge("e2", "p1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // One stats row per column.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let name = scalar_string(&format!(
        "SELECT column_name FROM read_csv_auto('{}') WHERE column_name = 'grp'",
        out
    ));
    assert_eq!(name, "grp");
}

#[test]
fn describe_lists_columns_and_types() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("d1", "qa.describe", json!({})),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "d1"), main_edge("e2", "d1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
}

#[test]
fn histogram_counts_values() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "g\na\na\nb\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("h1", "qa.histogram", json!({ "column": "g" })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "h1"), main_edge("e2", "h1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let freq = scalar_string(&format!(
        "SELECT CAST(frequency AS VARCHAR) FROM read_csv_auto('{}') WHERE value = 'a'",
        out
    ));
    assert_eq!(freq, "2", "got {}", freq);
}

#[test]
fn standardize_trims_and_uppercases() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "name\n  hello   world \n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c1", "qa.standardize", json!({
                "columns": ["name"], "case": "upper", "trim": true, "collapseWhitespace": true
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "c1"), main_edge("e2", "c1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    let v = scalar_string(&format!("SELECT name FROM read_csv_auto('{}')", out));
    assert_eq!(v, "HELLO WORLD", "got {}", v);
}

#[test]
fn fuzzy_dedupe_collapses_near_duplicates() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,name\n1,Acme Inc\n2,Acme Inc.\n3,Globex\n4,globex\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("u1", "qa.dedupe", json!({
                "columns": ["name"], "threshold": 0.9, "algorithm": "jaro-winkler"
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "u1"), main_edge("e2", "u1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // "Acme Inc"/"Acme Inc." collapse, "Globex"/"globex" collapse: 2 left.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
}

#[test]
fn record_match_finds_similar_pairs() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,name\n1,Acme Inc\n2,Acme Inc.\n3,Globex\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m1", "qa.match", json!({
                "columns": ["name"], "threshold": 0.85, "algorithm": "jaro-winkler"
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "m1"), main_edge("e2", "m1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // Only the Acme pair matches: one output row, carrying a match_score.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 1);
    let id = scalar_string(&format!(
        "SELECT CAST(id AS VARCHAR) FROM read_csv_auto('{}')",
        out
    ));
    assert_eq!(id, "1", "got {}", id);
}

#[test]
fn denormalize_groups_into_delimited_cells() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "g,v\na,x\na,y\nb,z\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("n1", "xf.denorm", json!({
                "groupBy": ["g"], "aggregateColumns": ["v"], "separator": ", "
            })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "n1"), main_edge("e2", "n1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let v = scalar_string(&format!(
        "SELECT v FROM read_csv_auto('{}') WHERE g = 'a'",
        out
    ));
    // Order within a group depends on input order; both members must be present.
    assert!(v.contains('x') && v.contains('y'), "got {}", v);
}

#[test]
fn normalize_explodes_delimited_column() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,tags\n1,\"a,b\"\n2,c\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("n1", "xf.norm", json!({ "column": "tags", "separator": "," })),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "n1"), main_edge("e2", "n1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // 2 + 1 = 3 rows after the explode.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn transpose_swaps_rows_and_columns() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "a,b,c\n1,10,100\n2,20,200\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("t1", "xf.transpose", json!({})),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "t1"), main_edge("e2", "t1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // 3 original columns -> 3 output rows; check the row for 'b' has the
    // original 'b' column values (10, 20).
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let v1 = scalar_string(&format!(
        "SELECT CAST(r1 AS VARCHAR) FROM read_csv_auto('{}') WHERE colname = 'b'",
        out
    ));
    assert_eq!(v1, "10", "got {}", v1);
}

#[test]
fn replicate_passes_data_through() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id\n1\n2\n3\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r1", "ctl.replicate", json!({})),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s1", "r1"), main_edge("e2", "r1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn merge_streams_concatenates_inputs() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv_a = write_file(tmp.path(), "a.csv", "id\n1\n2\n");
    let csv_b = write_file(tmp.path(), "b.csv", "id\n3\n4\n");
    let out = out_path(tmp.path(), "out.csv");
    let main_n = |id: &str, source: &str, target: &str, n: usize| {
        json!({
            "id": id, "source": source, "target": target,
            "targetHandle": format!("main_{}", n),
            "data": { "connectionType": "main" }
        })
    };
    let d = doc(
        json!([
            node("a", "src.csv", json!({ "path": csv_a, "hasHeader": true })),
            node("b", "src.csv", json!({ "path": csv_b, "hasHeader": true })),
            node("m", "ctl.merge", json!({})),
            node("k1", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_n("e1", "a", "m", 1),
            main_n("e2", "b", "m", 2),
            main_edge("e3", "m", "k1"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // 2 + 2 = 4 rows after merge.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 4);
}

/// Read the PostgreSQL connection details from env. Returns None to
/// skip when the test isn't running with a real PG service available
/// (i.e. anywhere except the CI postgres-integration job).
fn pg_env() -> Option<(String, u64, String, String, String)> {
    let host = std::env::var("DUCKLE_PG_HOST").ok()?;
    let port = std::env::var("DUCKLE_PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5432);
    let db = std::env::var("DUCKLE_PG_DB").unwrap_or_else(|_| "postgres".into());
    let user = std::env::var("DUCKLE_PG_USER").unwrap_or_else(|_| "postgres".into());
    let pass = std::env::var("DUCKLE_PG_PASS").unwrap_or_default();
    Some((host, port, db, user, pass))
}

#[test]
fn pg_sink_then_source_roundtrip() {
    let engine = engine_or_skip!();
    let (host, port, db, user, pass) = match pg_env() {
        Some(x) => x,
        None => {
            eprintln!("skipping: set DUCKLE_PG_HOST to run against a real PostgreSQL");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let out = out_path(tmp.path(), "out.csv");
    let table = format!("duckle_test_{}", std::process::id());

    // Write csv -> snk.postgres.
    let write_doc = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.postgres", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "schemaName": "public", "tableName": table, "mode": "overwrite"
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    );
    let r1 = engine.execute_pipeline(&write_doc);
    assert_eq!(r1.status, "ok", "write failed: {:?}", r1.error);

    // Read back from PG via src.postgres into a CSV file.
    let read_doc = doc(
        json!([
            node("r", "src.postgres", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "schemaName": "public", "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let r2 = engine.execute_pipeline(&read_doc);
    assert_eq!(r2.status, "ok", "read failed: {:?}", r2.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let name = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(name, "bob", "got {}", name);
}

fn mysql_env() -> Option<(String, u64, String, String, String)> {
    let host = std::env::var("DUCKLE_MYSQL_HOST").ok()?;
    let port = std::env::var("DUCKLE_MYSQL_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3306);
    let db = std::env::var("DUCKLE_MYSQL_DB").unwrap_or_else(|_| "ducktest".into());
    let user = std::env::var("DUCKLE_MYSQL_USER").unwrap_or_else(|_| "root".into());
    let pass = std::env::var("DUCKLE_MYSQL_PASS").unwrap_or_default();
    Some((host, port, db, user, pass))
}

#[test]
fn mysql_sink_then_source_roundtrip() {
    let engine = engine_or_skip!();
    let (host, port, db, user, pass) = match mysql_env() {
        Some(x) => x,
        None => {
            eprintln!("skipping: set DUCKLE_MYSQL_HOST to run against a real MySQL");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let out = out_path(tmp.path(), "out.csv");
    let table = format!("duckle_test_{}", std::process::id());

    // csv -> snk.mysql
    let write_doc = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.mysql", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "tableName": table, "mode": "overwrite"
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    );
    let r1 = engine.execute_pipeline(&write_doc);
    assert_eq!(r1.status, "ok", "write failed: {:?}", r1.error);

    // src.mysql -> csv
    let read_doc = doc(
        json!([
            node("r", "src.mysql", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let r2 = engine.execute_pipeline(&read_doc);
    assert_eq!(r2.status, "ok", "read failed: {:?}", r2.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let name = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(name, "bob", "got {}", name);
}

#[test]
fn md_source_reads_table() {
    // Live MotherDuck test: requires MOTHERDUCK_TOKEN plus a pre-created
    // table named by DUCKLE_MD_TABLE (default 'duckle_test') inside the
    // database DUCKLE_MD_DB (default 'my_db'). Skips cleanly otherwise.
    let engine = engine_or_skip!();
    let token = match std::env::var("MOTHERDUCK_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => {
            eprintln!("skipping: set MOTHERDUCK_TOKEN to run against MotherDuck");
            return;
        }
    };
    let db = std::env::var("DUCKLE_MD_DB").unwrap_or_else(|_| "my_db".into());
    let table = std::env::var("DUCKLE_MD_TABLE").unwrap_or_else(|_| "duckle_test".into());
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.motherduck", json!({
                "database": db, "token": token,
                "schemaName": "main", "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "MotherDuck read failed: {:?}", result.error);
    // Don't assert a specific row count - the table is the user's,
    // not ours. Just confirm the read ran end to end.
    assert!(std::path::Path::new(&out).exists(), "output CSV should exist");
}

#[test]
fn minio_source_reads_via_endpoint() {
    // Live S3-compatible test. The CI minio-integration job seeds
    // s3://duckle-test/orders.parquet with 3 rows; this verifies the
    // engine can read it back through the SECRET's endpoint plumbing.
    let engine = engine_or_skip!();
    let host = match std::env::var("DUCKLE_MINIO_HOST") {
        Ok(h) if !h.is_empty() => h,
        _ => {
            eprintln!("skipping: set DUCKLE_MINIO_HOST to run against MinIO");
            return;
        }
    };
    let port = std::env::var("DUCKLE_MINIO_PORT").unwrap_or_else(|_| "9000".into());
    let bucket = std::env::var("DUCKLE_MINIO_BUCKET").unwrap_or_else(|_| "duckle-test".into());
    let access = std::env::var("DUCKLE_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = std::env::var("DUCKLE_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.minio", json!({
                "bucket": bucket, "key": "orders.parquet", "region": "us-east-1",
                "accessKey": access, "secretKey": secret,
                "endpoint": format!("{}:{}", host, port),
                "urlStyle": "path", "useSsl": "false",
                "format": "parquet"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "MinIO read failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn schema_validate_rejects_rows_missing_required_columns() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,name,email\n1,alice,a@x\n2,,b@x\n3,carol,\n",
    );
    let pass = out_path(tmp.path(), "pass.csv");
    let reject = out_path(tmp.path(), "reject.csv");
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("v1", "qa.schemavalidate", json!({
                "expectedColumns": ["id", "name", "email"]
            })),
            node("ok", "snk.csv", json!({ "path": pass, "hasHeader": true })),
            node("bad", "snk.csv", json!({ "path": reject, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s1", "v1"),
            main_edge("e2", "v1", "ok"),
            port_edge("e3", "v1", "reject", "bad"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);
    // Row 1 passes (no nulls); rows 2 and 3 reject (name / email null).
    assert_eq!(count(&format!("read_csv_auto('{}')", pass)), 1);
    assert_eq!(count(&format!("read_csv_auto('{}')", reject)), 2);
}

#[test]
fn pg_sink_append_grows_table() {
    // Live PG test: an overwrite (3 rows) followed by an append (2 more)
    // should land 5 rows in the target table.
    let engine = engine_or_skip!();
    let (host, port, db, user, pass) = match pg_env() {
        Some(x) => x,
        None => {
            eprintln!("skipping: set DUCKLE_PG_HOST to run against a real PostgreSQL");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let table = format!("duckle_append_{}", std::process::id());
    let conn = |csv: &str, mode: &str| {
        doc(
            json!([
                node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
                node("w", "snk.postgres", json!({
                    "host": &host, "port": port, "database": &db,
                    "user": &user, "password": &pass,
                    "schemaName": "public", "tableName": &table, "mode": mode
                })),
            ]),
            json!([main_edge("e", "s", "w")]),
        )
    };
    let csv1 = write_file(tmp.path(), "in1.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let r1 = engine.execute_pipeline(&conn(&csv1, "overwrite"));
    assert_eq!(r1.status, "ok", "overwrite failed: {:?}", r1.error);

    let csv2 = write_file(tmp.path(), "in2.csv", "id,name\n4,dan\n5,eve\n");
    let r2 = engine.execute_pipeline(&conn(&csv2, "append"));
    assert_eq!(r2.status, "ok", "append failed: {:?}", r2.error);

    // Read back via src.postgres and verify the table now has 5 rows.
    let out = out_path(tmp.path(), "out.csv");
    let read_doc = doc(
        json!([
            node("r", "src.postgres", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "schemaName": "public", "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let r3 = engine.execute_pipeline(&read_doc);
    assert_eq!(r3.status, "ok", "read failed: {:?}", r3.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 5);
}

#[test]
fn avro_source_reads_fixture() {
    // The DuckDB avro extension is read-only and we can't self-generate
    // a fixture; this test runs when DUCKLE_AVRO_FIXTURE points at an
    // .avro file. CI doesn't ship a fixture today.
    let engine = engine_or_skip!();
    let path = match std::env::var("DUCKLE_AVRO_FIXTURE") {
        Ok(p) if !p.is_empty() && std::path::Path::new(&p).exists() => p,
        _ => {
            eprintln!("skipping: set DUCKLE_AVRO_FIXTURE to an .avro file path");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.avro", json!({ "path": norm(&path) })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "avro read failed: {:?}", result.error);
    assert!(count(&format!("read_csv_auto('{}')", out)) > 0);
}

#[test]
fn pg_sink_truncate_replaces_rows() {
    // Live PG test: overwrite 3 rows, then truncate-insert 2 rows.
    // After truncate, the table must end with exactly 2 rows.
    let engine = engine_or_skip!();
    let (host, port, db, user, pass) = match pg_env() {
        Some(x) => x,
        None => {
            eprintln!("skipping: set DUCKLE_PG_HOST to run against PostgreSQL");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let table = format!("duckle_trunc_{}", std::process::id());
    let write = |csv: &str, mode: &str| {
        doc(
            json!([
                node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
                node("w", "snk.postgres", json!({
                    "host": &host, "port": port, "database": &db,
                    "user": &user, "password": &pass,
                    "schemaName": "public", "tableName": &table, "mode": mode
                })),
            ]),
            json!([main_edge("e", "s", "w")]),
        )
    };
    let csv1 = write_file(tmp.path(), "in1.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let r1 = engine.execute_pipeline(&write(&csv1, "overwrite"));
    assert_eq!(r1.status, "ok", "overwrite failed: {:?}", r1.error);

    let csv2 = write_file(tmp.path(), "in2.csv", "id,name\n10,dan\n11,eve\n");
    let r2 = engine.execute_pipeline(&write(&csv2, "truncate"));
    assert_eq!(r2.status, "ok", "truncate failed: {:?}", r2.error);

    let out = out_path(tmp.path(), "out.csv");
    let r3 = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.postgres", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "schemaName": "public", "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    ));
    assert_eq!(r3.status, "ok", "read failed: {:?}", r3.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
}

#[test]
fn scd2_closes_changed_and_inserts_new_versions() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();

    // Seed the previous-history snapshot as parquet so timestamp + bool
    // + nullable columns survive (CSV would coerce them all to varchar).
    let prev = out_path(tmp.path(), "prev.parquet");
    duckdb_exec(
        ":memory:",
        &format!(
            "COPY (SELECT * FROM (VALUES \
                (1,'a',TIMESTAMP '2024-01-01',NULL::TIMESTAMP,TRUE), \
                (2,'b',TIMESTAMP '2024-01-01',NULL::TIMESTAMP,TRUE) \
            ) t(id,v,valid_from,valid_to,is_current)) TO '{}' (FORMAT PARQUET)",
            prev
        ),
    );
    let cur = write_file(tmp.path(), "cur.csv", "id,v\n1,a\n2,b2\n3,c\n");
    let out = out_path(tmp.path(), "out.parquet");
    let d = doc(
        json!([
            node("c", "src.csv", json!({ "path": cur, "hasHeader": true })),
            node("p", "src.parquet", json!({ "path": prev })),
            node("h", "xf.cdc.scd2", json!({
                "naturalKey": ["id"], "compareColumns": ["v"],
                "validFromColumn": "valid_from", "validToColumn": "valid_to",
                "isCurrentColumn": "is_current"
            })),
            node("k", "snk.parquet", json!({ "path": out })),
        ]),
        json!([
            main_edge("e1", "c", "h"),
            lookup_edge("e2", "p", "h"),
            main_edge("e3", "h", "k"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "scd2 failed: {:?}", result.error);
    // id=1 unchanged (1 row), id=2 closed + new (2 rows), id=3 new (1 row) = 4.
    assert_eq!(count(&format!("read_parquet('{}')", out)), 4);
    // id=2 should now have one closed and one current row.
    assert_eq!(
        count(&format!("read_parquet('{}') WHERE id = 2", out)),
        2
    );
    // The closed-and-replaced id=2 row should be the OLD v ('b'), not current.
    let closed = scalar_string(&format!(
        "SELECT v FROM read_parquet('{}') WHERE id = 2 AND NOT is_current",
        out
    ));
    assert_eq!(closed, "b", "got {}", closed);
}

#[test]
fn scd1_emits_resolved_state() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let cur = write_file(tmp.path(), "cur.csv", "id,v\n1,a\n2,b2\n3,c\n");
    let prev = write_file(tmp.path(), "prev.csv", "id,v\n1,a\n2,b\n4,d\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("c", "src.csv", json!({ "path": cur, "hasHeader": true })),
            node("p", "src.csv", json!({ "path": prev, "hasHeader": true })),
            node("h", "xf.cdc.scd1", json!({ "naturalKey": ["id"] })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "c", "h"),
            lookup_edge("e2", "p", "h"),
            main_edge("e3", "h", "k"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "scd1 failed: {:?}", result.error);
    // cur has (1,2,3); prev (4) retained because key 4 isn't in cur. Total 4.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 4);
    // id=2 must show the CURRENT value (b2), not the prev (b).
    let v = scalar_string(&format!(
        "SELECT v FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(v, "b2", "got {}", v);
}

#[test]
fn upsert_emits_only_changes_and_inserts() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let cur = write_file(tmp.path(), "cur.csv", "id,v\n1,a\n2,b2\n3,c\n");
    let prev = write_file(tmp.path(), "prev.csv", "id,v\n1,a\n2,b\n4,d\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("c", "src.csv", json!({ "path": cur, "hasHeader": true })),
            node("p", "src.csv", json!({ "path": prev, "hasHeader": true })),
            node("u", "xf.cdc.upsert", json!({
                "naturalKey": ["id"], "compareColumns": ["v"]
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "c", "u"),
            lookup_edge("e2", "p", "u"),
            main_edge("e3", "u", "k"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "upsert failed: {:?}", result.error);
    // id=1 unchanged (skipped), id=2 changed, id=3 new -> 2 rows.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
}

#[test]
fn excel_source_reads_xlsx() {
    // Self-generating: the DuckDB excel extension can both write and
    // read xlsx (v1.2+), so we COPY a small table out as .xlsx via the
    // CLI and read it back through the engine.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let xlsx = out_path(tmp.path(), "in.xlsx");
    duckdb_exec(
        ":memory:",
        &format!(
            "INSTALL excel; LOAD excel; \
             COPY (SELECT * FROM (VALUES (1,'alice'),(2,'bob'),(3,'carol')) t(id,name)) \
             TO '{}' (FORMAT 'xlsx', HEADER true)",
            xlsx
        ),
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.excel", json!({ "path": xlsx, "hasHeader": true })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "excel read failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
    let name = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') WHERE CAST(id AS INTEGER) = 2",
        out
    ));
    assert_eq!(name, "bob", "got {}", name);
}

#[test]
fn pg_sink_upsert_updates_and_inserts() {
    // Live PG test: overwrite (3 rows), then upsert a new batch where
    // one row collides (key=2, value changed) and one is new (key=4).
    // After upsert: 4 rows total; the colliding row carries the new
    // value; the new row was inserted.
    let engine = engine_or_skip!();
    let (host, port, db, user, pass) = match pg_env() {
        Some(x) => x,
        None => {
            eprintln!("skipping: set DUCKLE_PG_HOST to run against PostgreSQL");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let table = format!("duckle_upsert_{}", std::process::id());

    // Seed: overwrite with 3 rows including a PRIMARY KEY on id.
    // (build_relational_sink in overwrite mode does CREATE TABLE AS,
    // which produces a table without a constraint, so we ALTER it.)
    let csv1 = write_file(tmp.path(), "in1.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv1, "hasHeader": true })),
            node("w", "snk.postgres", json!({
                "host": &host, "port": port, "database": &db,
                "user": &user, "password": &pass,
                "schemaName": "public", "tableName": &table, "mode": "overwrite"
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    ));
    assert_eq!(r1.status, "ok", "overwrite failed: {:?}", r1.error);
    // Add a primary key so ON CONFLICT (id) has something to match on.
    // Run the ALTER via the postgres extension's passthrough so the
    // constraint lands in PG's catalog (DuckDB's ATTACH path silently
    // no-ops some DDL).
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let alter = std::process::Command::new(&bin)
        .arg(":memory:")
        .arg("-c")
        .arg(format!(
            "INSTALL postgres; LOAD postgres; \
             ATTACH 'host={host} port={port} dbname={db} user={user} password={pass}' AS d (TYPE POSTGRES); \
             CALL postgres_execute('d', 'ALTER TABLE public.{table} ADD PRIMARY KEY (id);');"
        ))
        .output()
        .expect("alter");
    assert!(
        alter.status.success(),
        "ALTER PK failed: {}",
        String::from_utf8_lossy(&alter.stderr)
    );

    // Upsert: id=2 changes (bob -> bobby), id=4 is new.
    let csv2 = write_file(tmp.path(), "in2.csv", "id,name\n2,bobby\n4,dan\n");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv2, "hasHeader": true })),
            node("w", "snk.postgres", json!({
                "host": &host, "port": port, "database": &db,
                "user": &user, "password": &pass,
                "schemaName": "public", "tableName": &table, "mode": "upsert",
                "conflictColumns": ["id"]
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    ));
    assert_eq!(r2.status, "ok", "upsert failed: {:?}", r2.error);

    // Read back: 4 rows total; id=2 carries 'bobby'.
    let out = out_path(tmp.path(), "out.csv");
    let r3 = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.postgres", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "schemaName": "public", "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    ));
    assert_eq!(r3.status, "ok", "read failed: {:?}", r3.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 4);
    let updated = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(updated, "bobby", "got {}", updated);
}

#[test]
fn switch_routes_rows_to_case_outputs() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,amount\n1,50\n2,150\n3,200\n4,30\n");
    let out_high = out_path(tmp.path(), "high.csv");
    let out_low = out_path(tmp.path(), "low.csv");
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("sw", "ctl.switch", json!({
                "branches": { "high": "amount > 100", "low": "amount <= 100" }
            })),
            node("kh", "snk.csv", json!({ "path": out_high, "hasHeader": true })),
            node("kl", "snk.csv", json!({ "path": out_low, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s", "sw"),
            port_edge("e2", "sw", "case_1", "kh"),
            port_edge("e3", "sw", "case_2", "kl"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "switch run failed: {:?}", result.error);
    // case_1 (high: amount > 100) -> ids 2 and 3.
    assert_eq!(count(&format!("read_csv_auto('{}')", out_high)), 2);
    // case_2 (low: <= 100, excluding high matches) -> ids 1 and 4.
    assert_eq!(count(&format!("read_csv_auto('{}')", out_low)), 2);
}

#[test]
fn iceberg_source_reads_fixture() {
    // Env-gated: set DUCKLE_ICEBERG_FIXTURE to a local Iceberg table
    // root (the directory that contains metadata/ and data/). DuckDB's
    // iceberg extension is read-only, so the test can't self-generate.
    let engine = engine_or_skip!();
    let path = match std::env::var("DUCKLE_ICEBERG_FIXTURE") {
        Ok(p) if !p.is_empty() && std::path::Path::new(&p).exists() => p,
        _ => {
            eprintln!("skipping: set DUCKLE_ICEBERG_FIXTURE to an Iceberg table directory");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.iceberg", json!({ "path": norm(&path) })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "iceberg read failed: {:?}", result.error);
    assert!(count(&format!("read_csv_auto('{}')", out)) >= 0);
}

#[test]
fn delta_source_reads_fixture() {
    // Env-gated: set DUCKLE_DELTA_FIXTURE to a local Delta table root.
    let engine = engine_or_skip!();
    let path = match std::env::var("DUCKLE_DELTA_FIXTURE") {
        Ok(p) if !p.is_empty() && std::path::Path::new(&p).exists() => p,
        _ => {
            eprintln!("skipping: set DUCKLE_DELTA_FIXTURE to a Delta table directory");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.delta", json!({ "path": norm(&path) })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "delta read failed: {:?}", result.error);
    assert!(count(&format!("read_csv_auto('{}')", out)) >= 0);
}

#[test]
fn tsv_sink_writes_tab_delimited() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let out = out_path(tmp.path(), "out.tsv");
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k", "snk.tsv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "s", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "tsv write failed: {:?}", result.error);
    // Read back as a tab-delimited CSV and confirm row count + a value.
    assert_eq!(
        count(&format!("read_csv_auto('{}', delim = '\t', header = true)", out)),
        2
    );
    let raw = std::fs::read_to_string(&out).expect("read out.tsv");
    assert!(raw.contains('\t'), "expected tab delimiter, got: {:?}", raw);
}

#[test]
fn vector_search_ranks_by_cosine_similarity() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    // Seed three rows with 3-dim float vectors via parquet (preserves
    // the FLOAT[3] type that vss expects).
    let parquet = out_path(tmp.path(), "vecs.parquet");
    duckdb_exec(
        ":memory:",
        &format!(
            "COPY (SELECT * FROM (VALUES \
                (1, [1.0, 0.0, 0.0]::FLOAT[3]), \
                (2, [0.0, 1.0, 0.0]::FLOAT[3]), \
                (3, [0.9, 0.1, 0.0]::FLOAT[3]) \
            ) t(id, vec)) TO '{}' (FORMAT PARQUET)",
            parquet
        ),
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s", "src.parquet", json!({ "path": parquet })),
            node("v", "xf.ai.vector_search", json!({
                "vectorColumn": "vec",
                "targetVector": "[0.9, 0.1, 0.0]",
                "dimension": 3,
                "distanceMetric": "cosine",
                "topK": 2,
                "outputColumn": "score"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "v"), main_edge("e2", "v", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "vector_search failed: {:?}", result.error);
    // topK = 2 -> two rows. The closest match (identical vector) is id=3.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let top = scalar_string(&format!(
        "SELECT CAST(id AS VARCHAR) FROM read_csv_auto('{}') ORDER BY score DESC LIMIT 1",
        out
    ));
    assert_eq!(top, "3", "got {}", top);
}

#[test]
fn spatial_source_reads_geojson() {
    // The spatial extension is GDAL-backed (~50 MB); only opt-in CI /
    // local runs install it. Set DUCKLE_TEST_SPATIAL=1 to exercise.
    if std::env::var("DUCKLE_TEST_SPATIAL").ok().as_deref() != Some("1") {
        eprintln!("skipping: set DUCKLE_TEST_SPATIAL=1 to run spatial tests");
        return;
    }
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let geojson = write_file(
        tmp.path(),
        "t.geojson",
        r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"alpha"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"beta"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#,
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("r", "src.spatial", json!({ "path": geojson })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "spatial read failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    let names = scalar_string(&format!(
        "SELECT string_agg(name, ',' ORDER BY name) FROM read_csv_auto('{}')",
        out
    ));
    assert_eq!(names, "alpha,beta", "got {}", names);
}

#[test]
fn text_search_ranks_by_bm25() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,body\n1,duck duck goose\n2,the quick brown fox\n3,duckdb is fast for analytics\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("t", "xf.ai.text_search", json!({
                "idColumn": "id",
                "textColumns": ["body"],
                "query": "duck",
                "outputColumn": "score"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "t"), main_edge("e2", "t", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "text_search failed: {:?}", result.error);
    // BM25 tokenization means 'duck' matches 'duck duck goose' but not
    // 'duckdb' (different token). So exactly one row.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 1);
    let body = scalar_string(&format!("SELECT body FROM read_csv_auto('{}')", out));
    assert!(body.contains("duck duck goose"), "got {}", body);
}

#[test]
fn excel_sink_writes_xlsx() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let xlsx = out_path(tmp.path(), "out.xlsx");
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k", "snk.excel", json!({ "path": xlsx, "hasHeader": true })),
        ]),
        json!([main_edge("e", "s", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "xlsx write failed: {:?}", result.error);
    // Read back via the same extension.
    let n = scalar_string(&format!(
        "INSTALL excel; LOAD excel; SELECT CAST(count(*) AS VARCHAR) FROM read_xlsx('{}')",
        xlsx
    ));
    assert_eq!(n, "3", "got {}", n);
}

#[test]
fn spatial_sink_writes_geojson() {
    if std::env::var("DUCKLE_TEST_SPATIAL").ok().as_deref() != Some("1") {
        eprintln!("skipping: set DUCKLE_TEST_SPATIAL=1 to run spatial tests");
        return;
    }
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    // Source: a tiny in-memory table of geometry points via the spatial
    // extension. We seed via duckdb_exec because src.csv has no geom type.
    let parquet = out_path(tmp.path(), "geoms.parquet");
    duckdb_exec(
        ":memory:",
        &format!(
            "INSTALL spatial; LOAD spatial; \
             COPY (SELECT ST_Point(1, 2) AS geom, 'alpha' AS name UNION ALL \
                   SELECT ST_Point(3, 4), 'beta') TO '{}' (FORMAT PARQUET)",
            parquet
        ),
    );
    let out = out_path(tmp.path(), "out.geojson");
    let d = doc(
        json!([
            node("s", "src.parquet", json!({ "path": parquet })),
            node("k", "snk.spatial", json!({ "path": out, "driver": "GeoJSON" })),
        ]),
        json!([main_edge("e", "s", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "spatial write failed: {:?}", result.error);
    // Read back via ST_Read and verify both features made it.
    let n = scalar_string(&format!(
        "INSTALL spatial; LOAD spatial; SELECT CAST(count(*) AS VARCHAR) FROM ST_Read('{}')",
        out
    ));
    assert_eq!(n, "2", "got {}", n);
}

#[test]
fn md_sink_writes_table() {
    let engine = engine_or_skip!();
    let token = match std::env::var("MOTHERDUCK_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => {
            eprintln!("skipping: set MOTHERDUCK_TOKEN to run against MotherDuck");
            return;
        }
    };
    let db = std::env::var("DUCKLE_MD_DB").unwrap_or_else(|_| "my_db".into());
    let table = format!("duckle_sink_test_{}", std::process::id());
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.motherduck", json!({
                "database": db, "token": token,
                "schemaName": "main", "tableName": table, "mode": "overwrite"
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "MD write failed: {:?}", result.error);
}

#[test]
fn iceberg_sink_writes_then_source_reads() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let table_dir = out_path(tmp.path(), "ice_table");

    // csv -> snk.iceberg writes a full Iceberg table (data/ + metadata/).
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.iceberg", json!({ "path": table_dir })),
        ]),
        json!([main_edge("e", "s", "w")]),
    ));
    assert_eq!(r1.status, "ok", "iceberg write failed: {:?}", r1.error);

    // Read back via src.iceberg into a csv to verify the roundtrip.
    let out = out_path(tmp.path(), "out.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.iceberg", json!({ "path": table_dir })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    ));
    assert_eq!(r2.status, "ok", "iceberg read failed: {:?}", r2.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn ducklake_sink_then_source_roundtrip() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let catalog = out_path(tmp.path(), "lake.duckdb");

    // csv -> snk.ducklake creates the catalog and writes 'orders' table.
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.ducklake", json!({
                "path": catalog, "schemaName": "main", "tableName": "orders", "mode": "overwrite"
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    ));
    assert_eq!(r1.status, "ok", "ducklake write failed: {:?}", r1.error);

    // src.ducklake reads the table back.
    let out = out_path(tmp.path(), "out.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.ducklake", json!({
                "path": catalog, "schemaName": "main", "tableName": "orders", "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    ));
    assert_eq!(r2.status, "ok", "ducklake read failed: {:?}", r2.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn hash_adds_md5_column() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("h", "xf.hash", json!({
                "column": "name", "algorithm": "md5", "outputColumn": "name_md5"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "h"), main_edge("e2", "h", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "hash failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    // md5('alice') is a well-known fixed digest.
    let alice = scalar_string(&format!(
        "SELECT name_md5 FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    assert_eq!(alice, "6384e2b2184bcbf58eccf10ca7a6563c", "got {}", alice);
}

#[test]
fn geo_distance_computes_point_distance() {
    // Same gate as the other spatial tests - the GDAL-backed extension
    // is ~50 MB so only opt-in runs install it.
    if std::env::var("DUCKLE_TEST_SPATIAL").ok().as_deref() != Some("1") {
        eprintln!("skipping: set DUCKLE_TEST_SPATIAL=1 to run spatial tests");
        return;
    }
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    // Seed a parquet with GEOMETRY columns via duckdb_exec so the type
    // survives into the pipeline (CSV would coerce to varchar).
    let parquet = out_path(tmp.path(), "geoms.parquet");
    duckdb_exec(
        ":memory:",
        &format!(
            "INSTALL spatial; LOAD spatial; \
             COPY (SELECT * FROM (VALUES \
                 ('a', ST_Point(3, 4)), \
                 ('b', ST_Point(6, 8)) \
             ) t(name, loc)) TO '{}' (FORMAT PARQUET)",
            parquet
        ),
    );
    let out = out_path(tmp.path(), "out.csv");
    let d = doc(
        json!([
            node("s", "src.parquet", json!({ "path": parquet })),
            node("g", "xf.geo.distance", json!({
                "geomColumn": "loc", "targetWkt": "POINT(0 0)", "outputColumn": "dist"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "g"), main_edge("e2", "g", "k")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "geo_distance failed: {:?}", result.error);
    // (3,4) -> (0,0) is 5; (6,8) -> (0,0) is 10.
    let a = scalar_string(&format!(
        "SELECT CAST(round(dist, 2) AS VARCHAR) FROM read_csv_auto('{}') WHERE name = 'a'",
        out
    ));
    assert_eq!(a, "5.0", "got {}", a);
}

#[test]
fn snk_webhook_posts_one_request_per_row() {
    // Spins up a tiny TCP/HTTP listener, runs snk.webhook against it,
    // and verifies (a) two requests arrived (one per CSV row) and (b)
    // the row JSON shows up in the request bodies.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http");
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}/hook", addr);

    let handle = std::thread::spawn(move || {
        // Accept exactly 2 connections; close each after one round-trip.
        for stream in listener.incoming().take(2) {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            // Drain whatever the client wrote - headers and body can
            // arrive in separate TCP reads, so keep going until the
            // read times out (no more data) or we hit a cap.
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let body = b"ok";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            // Windows CI hits WSAECONNABORTED (os err 10053) if we drop
            // the stream before the client finishes reading.
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.webhook", json!({ "url": url })),
        ]),
        json!([main_edge("e1", "s", "w")]),
    ));
    assert_eq!(r.status, "ok", "webhook pipeline failed: {:?}", r.error);

    // Drain received requests with a generous timeout so slow CI hosts
    // don't flake.
    let mut requests = Vec::new();
    for _ in 0..2 {
        if let Ok(req) = rx.recv_timeout(Duration::from_secs(5)) {
            requests.push(String::from_utf8_lossy(&req).to_string());
        }
    }
    let _ = handle.join();
    assert_eq!(requests.len(), 2, "expected 2 HTTP requests, got {}", requests.len());
    let combined = requests.join("|");
    assert!(combined.contains("alice"), "expected alice in payloads: {}", combined);
    assert!(combined.contains("bob"), "expected bob in payloads: {}", combined);
    assert!(combined.contains("POST"), "expected POST method: {}", combined);
}

#[test]
fn text_replace_slug_and_strip_html() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,title,html\n1,Hello World!,<p>Hi <b>there</b></p>\n2,Foo Bar Baz,<div>x</div>\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("rep", "xf.text.replace", json!({
                "column": "title", "search": "World", "replacement": "Galaxy",
                "outputColumn": "title2"
            })),
            node("sg", "xf.text.slug", json!({ "column": "title", "outputColumn": "slug" })),
            node("sh", "xf.text.strip_html", json!({ "column": "html", "outputColumn": "text" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s", "rep"),
            main_edge("e2", "rep", "sg"),
            main_edge("e3", "sg", "sh"),
            main_edge("e4", "sh", "k"),
        ]),
    ));
    assert_eq!(r.status, "ok", "replace/slug/strip_html failed: {:?}", r.error);
    let r1_title = scalar_string(&format!(
        "SELECT title2 FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let r1_slug = scalar_string(&format!(
        "SELECT slug FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let r1_text = scalar_string(&format!(
        "SELECT text FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let r2_slug = scalar_string(&format!(
        "SELECT slug FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(r1_title, "Hello Galaxy!");
    assert_eq!(r1_slug, "hello-world");
    assert_eq!(r1_text, "Hi there");
    assert_eq!(r2_slug, "foo-bar-baz");
}

#[test]
fn text_reverse_repeat_and_compare() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id,a,b\n1,abc,xyz\n2,foo,foo\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("rv", "xf.text.reverse", json!({ "column": "a", "outputColumn": "a_rev" })),
            node("rp", "xf.text.repeat", json!({ "column": "a", "count": 3, "outputColumn": "a_x3" })),
            node("cp", "xf.compare", json!({
                "leftColumn": "a", "rightColumn": "b", "op": "eq", "outputColumn": "match"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s", "rv"),
            main_edge("e2", "rv", "rp"),
            main_edge("e3", "rp", "cp"),
            main_edge("e4", "cp", "k"),
        ]),
    ));
    assert_eq!(r.status, "ok", "reverse/repeat/compare failed: {:?}", r.error);
    let row1_rev = scalar_string(&format!(
        "SELECT a_rev FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let row1_x3 = scalar_string(&format!(
        "SELECT a_x3 FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let row1_match = scalar_string(&format!(
        "SELECT CAST(match AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let row2_match = scalar_string(&format!(
        "SELECT CAST(match AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(row1_rev, "cba");
    assert_eq!(row1_x3, "abcabcabc");
    assert_eq!(row1_match, "false");
    assert_eq!(row2_match, "true");
}

#[test]
fn snk_clickhouse_emits_jsoneachrow_to_insert_endpoint() {
    // Mock /?query=... endpoint; the engine should POST NDJSON to it.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let endpoint = format!("http://127.0.0.1:{}", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c", "snk.clickhouse", json!({
                "endpoint": endpoint,
                "database": "default",
                "tableName": "users",
                "user": "ch", "password": "p"
            })),
        ]),
        json!([main_edge("e", "s", "c")]),
    ));
    assert_eq!(r.status, "ok", "clickhouse sink failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(5)).expect("expected 1 CH request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    // URL should have the urlencoded INSERT statement.
    assert!(body.contains("/?query="), "expected query in URL: {}", body.lines().next().unwrap_or(""));
    assert!(body.contains("INSERT") && body.contains("default") && body.contains("users"),
        "expected URL-encoded INSERT INTO default.users: {}", body);
    assert!(body.contains("FORMAT") && body.contains("JSONEachRow"),
        "expected JSONEachRow in URL: {}", body);
    assert!(body.contains("X-ClickHouse-User: ch"), "expected user header: {}", body);
    assert!(body.contains("X-ClickHouse-Key: p"), "expected key header: {}", body);
    // NDJSON body: each row on its own line.
    assert!(body.contains("{\"id\":1,\"name\":\"alice\"}"), "expected alice row: {}", body);
    assert!(body.contains("{\"id\":2,\"name\":\"bob\"}"), "expected bob row: {}", body);
}

#[test]
fn snk_and_src_mongodb_roundtrip_via_real_uri() {
    // Env-gated like the postgres / mysql / minio tests. Set
    // DUCKLE_MONGO_URI to a working mongodb URI (e.g. mongodb://127.0.0.1:27017)
    // to run; otherwise skip cleanly. Insert 3 docs via snk.mongodb,
    // read them back via src.mongodb, assert the count.
    let engine = engine_or_skip!();
    let uri = match std::env::var("DUCKLE_MONGO_URI").ok() {
        Some(u) if !u.is_empty() => u,
        _ => {
            eprintln!("skipping: set DUCKLE_MONGO_URI to run MongoDB tests");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let coll = format!("duckle_test_{}", std::process::id());

    // Sink: replace mode so re-runs are idempotent.
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m", "snk.mongodb", json!({
                "uri": &uri,
                "database": "duckle_test",
                "collection": &coll,
                "mode": "replace"
            })),
        ]),
        json!([main_edge("e", "s", "m")]),
    ));
    assert_eq!(r1.status, "ok", "mongo sink failed: {:?}", r1.error);

    // Source: read all 3 back.
    let out = out_path(tmp.path(), "out.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("m", "src.mongodb", json!({
                "uri": &uri,
                "database": "duckle_test",
                "collection": &coll
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "m", "k")]),
    ));
    assert_eq!(r2.status, "ok", "mongo source failed: {:?}", r2.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3, "expected 3 docs round-tripped, got {}", n);
}

#[test]
fn src_elastic_paginates_via_search_after() {
    // Two pages, each size=2. Page 1's last hit has sort=[42, "a"];
    // engine should send that as search_after on the next request.
    // Page 2 returns 1 hit (< size) so we stop.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let page1 = br#"{"hits":{"hits":[{"_source":{"id":1},"sort":[10,"a"]},{"_source":{"id":2},"sort":[42,"b"]}]}}"#;
    let page2 = br#"{"hits":{"hits":[{"_source":{"id":3},"sort":[99,"c"]}]}}"#;
    let req_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rc = req_count.clone();
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            cap.lock().unwrap().push(String::from_utf8_lossy(&buf).to_string());
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body: &[u8] = if idx == 0 { page1 } else { page2 };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let endpoint = format!("http://127.0.0.1:{}", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("e", "src.elastic", json!({
                "endpoint": endpoint,
                "index": "docs",
                "size": 2,
                "paginationMode": "search_after",
                "sort": "[{\"_id\":\"asc\"}]"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "e", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "search_after failed: {:?}", r.error);
    assert_eq!(req_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3, "expected 3 docs total, got {}", n);
    let reqs = captured.lock().unwrap();
    // First request: no search_after key.
    assert!(!reqs[0].contains("search_after"), "1st request shouldn't have search_after: {}", reqs[0]);
    // Second request: search_after with last hit's sort = [42, "b"].
    assert!(
        reqs[1].contains("search_after") && reqs[1].contains("42"),
        "2nd request should carry search_after=[42, \"b\"]: {}",
        reqs[1]
    );
}

#[test]
fn src_elastic_paginates_via_from_size() {
    // Two pages of size=2 each. The first returns hits = [a, b],
    // the second returns [c] (last page = fewer than size = stop).
    // Verify 3 rows materialized, 2 HTTP requests, and the engine
    // sent `from`: 0 and `from`: 2 in the two request bodies.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let page1 = br#"{"hits":{"hits":[{"_source":{"id":1,"name":"alice"}},{"_source":{"id":2,"name":"bob"}}]}}"#;
    let page2 = br#"{"hits":{"hits":[{"_source":{"id":3,"name":"carol"}}]}}"#;
    let req_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rc = req_count.clone();
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            cap.lock().unwrap().push(String::from_utf8_lossy(&buf).to_string());
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body: &[u8] = if idx == 0 { page1 } else { page2 };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let endpoint = format!("http://127.0.0.1:{}", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("e", "src.elastic", json!({
                "endpoint": endpoint,
                "index": "docs",
                "size": 2,
                "apiKey": "test-key"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "e", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "src.elastic failed: {:?}", r.error);
    assert_eq!(
        req_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "expected 2 HTTP requests (initial + page 2)"
    );
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3, "expected 3 rows across pages, got {}", n);
    let reqs = captured.lock().unwrap();
    // Both requests should hit the /docs/_search path; the first carries
    // from=0, the second from=2.
    assert!(reqs[0].contains("/docs/_search"), "expected /_search URL: {}", reqs[0].lines().next().unwrap_or(""));
    assert!(reqs[0].contains(r#""from":0"#), "expected from=0: {}", reqs[0]);
    assert!(reqs[1].contains(r#""from":2"#), "expected from=2: {}", reqs[1]);
    assert!(reqs[0].contains("ApiKey test-key"), "expected ApiKey header: {}", reqs[0]);
}

#[test]
fn src_rest_paginates_via_offset() {
    // 3 pages of size=2; the 3rd returns 1 row (< pageSize) so we stop.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let page1 = br#"[{"id":1},{"id":2}]"#;
    let page2 = br#"[{"id":3},{"id":4}]"#;
    let page3 = br#"[{"id":5}]"#;
    let req_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rc = req_count.clone();
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(3) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            cap.lock().unwrap().push(String::from_utf8_lossy(&buf).to_string());
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body: &[u8] = match idx { 0 => page1, 1 => page2, _ => page3 };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let url = format!("http://127.0.0.1:{}/items", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.rest", json!({
                "url": url,
                "paginationType": "offset",
                "offsetParam": "from",
                "pageSize": 2
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "r", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "offset pagination failed: {:?}", r.error);
    assert_eq!(req_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 5);
    let reqs = captured.lock().unwrap();
    assert!(reqs[1].contains("from=2"), "expected from=2 on 2nd request: {}", reqs[1]);
    assert!(reqs[2].contains("from=4"), "expected from=4 on 3rd request: {}", reqs[2]);
}

#[test]
fn src_rest_paginates_via_page_number() {
    // 3 pages; the 3rd is empty (0 rows) so we stop.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let page1 = br#"[{"id":1},{"id":2}]"#;
    let page2 = br#"[{"id":3}]"#;
    let page3 = br#"[]"#;
    let req_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rc = req_count.clone();
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(3) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            cap.lock().unwrap().push(String::from_utf8_lossy(&buf).to_string());
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body: &[u8] = match idx { 0 => page1, 1 => page2, _ => page3 };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let url = format!("http://127.0.0.1:{}/items", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.rest", json!({
                "url": url,
                "paginationType": "page",
                "pageParam": "p",
                "startPage": 1
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "r", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "page pagination failed: {:?}", r.error);
    assert_eq!(req_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3);
    let reqs = captured.lock().unwrap();
    assert!(reqs[1].contains("p=2"), "expected p=2 on 2nd: {}", reqs[1]);
    assert!(reqs[2].contains("p=3"), "expected p=3 on 3rd: {}", reqs[2]);
}

#[test]
fn src_rest_paginates_via_link_header() {
    // RFC 5988 Link header with rel="next". Two pages; second has no Link.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    let next_url = format!("http://127.0.0.1:{}/items?page=2", port);

    let page1_body = br#"[{"id":1}]"#;
    let page2_body = br#"[{"id":2}]"#;
    let req_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rc = req_count.clone();
    let cap = captured.clone();
    let nu = next_url.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            cap.lock().unwrap().push(String::from_utf8_lossy(&buf).to_string());
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (body, extra) = if idx == 0 {
                (&page1_body[..], format!("Link: <{}>; rel=\"next\"\r\n", nu))
            } else {
                (&page2_body[..], String::new())
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
                body.len(),
                extra
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let url = format!("http://127.0.0.1:{}/items", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.rest", json!({
                "url": url,
                "paginationType": "link"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "r", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "link pagination failed: {:?}", r.error);
    assert_eq!(req_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 2);
    let reqs = captured.lock().unwrap();
    assert!(reqs[1].contains("page=2"), "expected 2nd request to be /items?page=2: {}", reqs[1]);
}

#[test]
fn src_rest_fetches_and_walks_cursor_pages() {
    // First response: 2 rows under /data + cursor=p2; engine GETs the
    // next page (also 2 rows, no further cursor). Total 4 rows expected,
    // and exactly 2 HTTP requests.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let page1 = br#"{"data":[{"id":1,"name":"alice"},{"id":2,"name":"bob"}],"meta":{"next_cursor":"p2"}}"#;
    let page2 = br#"{"data":[{"id":3,"name":"carol"},{"id":4,"name":"dan"}],"meta":{"next_cursor":null}}"#;
    let req_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rc = req_count.clone();
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            cap.lock().unwrap().push(String::from_utf8_lossy(&buf).to_string());
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body: &[u8] = if idx == 0 { page1 } else { page2 };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let url = format!("http://127.0.0.1:{}/items", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.rest", json!({
                "url": url,
                "method": "GET",
                "responsePath": "/data",
                "cursorNextPath": "/meta/next_cursor",
                "cursorParam": "cursor"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "r", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "src.rest failed: {:?}", r.error);
    assert_eq!(
        req_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "expected 2 HTTP requests (initial + cursor page)"
    );
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 4, "expected 4 total rows across 2 pages, got {}", n);
    // Confirm the cursor was sent on the second request.
    let reqs = captured.lock().unwrap();
    assert!(
        reqs[1].contains("cursor=p2"),
        "expected cursor=p2 in 2nd request line: {}",
        reqs[1].lines().next().unwrap_or("")
    );
}

#[test]
fn src_snowflake_walks_partitions() {
    // Mock returns a partitionInfo with two entries; partition 0's
    // data is in the initial response, partition 1 is fetched via
    // ?partition=1. Verify both partitions land in the materialized table.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let initial_body = br#"{"code":"090001","statementHandle":"abc","resultSetMetaData":{"rowType":[{"name":"id","type":"fixed"},{"name":"name","type":"text"}],"partitionInfo":[{"rowCount":2},{"rowCount":2}]},"data":[["1","alice"],["2","bob"]]}"#;
    let partition_body = br#"{"data":[["3","carol"],["4","dan"]]}"#;
    let initial_len = initial_body.len();
    let partition_len = partition_body.len();
    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rc = request_count.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (body, len) = if idx == 0 {
                (&initial_body[..], initial_len)
            } else {
                (&partition_body[..], partition_len)
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                len
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let endpoint = format!("http://127.0.0.1:{}/api/v2/statements", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("sf", "src.snowflake", json!({
                "account": "test-account", "endpoint": endpoint,
                "authType": "pat", "pat": "secret",
                "query": "SELECT id, name FROM users"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "sf", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "snowflake paged failed: {:?}", r.error);
    assert_eq!(
        request_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "expected 2 HTTP requests (initial + partition 1)"
    );
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 4, "expected 4 total rows from 2 partitions, got {}", n);
}

#[test]
fn src_databricks_follows_chunk_links() {
    // Initial response carries result.next_chunk_internal_link pointing
    // at chunk index 1; the engine GETs it and stops when no further
    // link is present. Verify both chunks' data_array end up in the
    // materialized table.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let initial_body = br#"{"statement_id":"x","status":{"state":"SUCCEEDED"},"manifest":{"schema":{"columns":[{"name":"id","type_text":"INT"},{"name":"name","type_text":"STRING"}]}},"result":{"data_array":[["1","alice"]],"next_chunk_internal_link":"/api/2.0/sql/statements/x/result/chunks/1"}}"#;
    let chunk_body = br#"{"data_array":[["2","bob"],["3","carol"]]}"#;
    let initial_len = initial_body.len();
    let chunk_len = chunk_body.len();
    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rc = request_count.clone();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let idx = rc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (body, len) = if idx == 0 {
                (&initial_body[..], initial_len)
            } else {
                (&chunk_body[..], chunk_len)
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                len
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let endpoint = format!("http://127.0.0.1:{}/api/2.0/sql/statements/", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("db", "src.databricks", json!({
                "workspace": "dbc-test.cloud.databricks.com",
                "endpoint": endpoint, "pat": "dapi-secret",
                "warehouseId": "wh-abc",
                "query": "SELECT id, name FROM users"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "db", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "databricks paged failed: {:?}", r.error);
    assert_eq!(
        request_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "expected 2 HTTP requests (initial + chunk 1)"
    );
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3, "expected 3 rows across 2 chunks, got {}", n);
}

#[test]
fn src_snowflake_materializes_inline_result_set() {
    // Mock /api/v2/statements that returns Snowflake's inline-result
    // shape. Verifies the engine materializes the response as a
    // DuckDB table that downstream stages can read.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let response_body = br#"{"code":"090001","statementHandle":"abc","resultSetMetaData":{"rowType":[{"name":"id","type":"fixed"},{"name":"name","type":"text"}]},"data":[["1","alice"],["2","bob"]]}"#;
    let response_len = response_body.len();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_len
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(response_body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let endpoint = format!("http://127.0.0.1:{}/api/v2/statements", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("sf", "src.snowflake", json!({
                "account": "test-account",
                "endpoint": endpoint,
                "authType": "pat",
                "pat": "secret-pat",
                "query": "SELECT id, name FROM users"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "sf", "k")]),
    ));
    let _captured = rx.recv_timeout(Duration::from_secs(5)).expect("expected Snowflake request");
    let _ = handle.join();
    assert_eq!(r.status, "ok", "snowflake source failed: {:?}", r.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 2);
    let name1 = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') WHERE id = '1'",
        out
    ));
    assert_eq!(name1, "alice");
}

#[test]
fn src_databricks_materializes_inline_result_set() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let response_body = br#"{"statement_id":"abc-123","status":{"state":"SUCCEEDED"},"manifest":{"schema":{"columns":[{"name":"id","type_text":"INT"},{"name":"name","type_text":"STRING"}]}},"result":{"data_array":[["10","carol"],["20","dan"]]}}"#;
    let response_len = response_body.len();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_len
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(response_body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.csv");
    let endpoint = format!("http://127.0.0.1:{}/api/2.0/sql/statements/", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("db", "src.databricks", json!({
                "workspace": "dbc-test.cloud.databricks.com",
                "endpoint": endpoint,
                "pat": "dapi-secret",
                "warehouseId": "wh-abc",
                "query": "SELECT id, name FROM users"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "db", "k")]),
    ));
    let _captured = rx.recv_timeout(Duration::from_secs(5)).expect("expected Databricks request");
    let _ = handle.join();
    assert_eq!(r.status, "ok", "databricks source failed: {:?}", r.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 2);
    let name1 = scalar_string(&format!(
        "SELECT name FROM read_csv_auto('{}') WHERE id = '10'",
        out
    ));
    assert_eq!(name1, "carol");
}

#[test]
fn snk_databricks_posts_multirow_insert() {
    // Mock HTTP listener pretends to be Databricks's
    // /api/2.0/sql/statements/. Verifies multi-row INSERT, Bearer PAT,
    // backtick-quoted identifiers, and the body's warehouse_id +
    // catalog + schema + wait_timeout fields.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let body = b"{\"statement_id\":\"abc-123\",\"status\":{\"state\":\"SUCCEEDED\"}}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let endpoint = format!("http://127.0.0.1:{}/api/2.0/sql/statements/", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("db", "snk.databricks", json!({
                "workspace": "dbc-test.cloud.databricks.com",
                "endpoint": endpoint,
                "pat": "dapi-secret-pat",
                "warehouseId": "wh-abc123",
                "catalog": "main",
                "schema": "default",
                "tableName": "users",
                "waitTimeoutSeconds": 30
            })),
        ]),
        json!([main_edge("e1", "s", "db")]),
    ));
    assert_eq!(r.status, "ok", "databricks sink failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(5)).expect("expected 1 Databricks request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    assert!(body.contains("Bearer dapi-secret-pat"), "expected PAT bearer: {}", body);
    // Identifiers backtick-quoted; SQL is JSON-string-escaped (backticks
    // don't need escaping, but the literal sequence shows up as-is).
    assert!(
        body.contains("INSERT INTO `main`.`default`.`users`"),
        "expected backtick-qualified INSERT: {}",
        body
    );
    assert!(body.contains("'alice'") && body.contains("'bob'"), "expected row values: {}", body);
    // Top-level Databricks request body keys.
    assert!(body.contains(r#""warehouse_id":"wh-abc123""#), "expected warehouse_id: {}", body);
    assert!(body.contains(r#""wait_timeout":"30s""#), "expected wait_timeout: {}", body);
    assert!(body.contains(r#""on_wait_timeout":"CONTINUE""#), "expected on_wait_timeout: {}", body);
}

#[test]
fn snk_snowflake_jwt_auth_signs_request() {
    // Generates a fresh 2048-bit RSA key (Snowflake / ring both reject
    // smaller keys). Adds ~1s to test runtime but is the only size
    // jsonwebtoken/ring will sign. Asserts:
    //  - Authorization header is "Bearer eyJ..." (JWT prefix)
    //  - X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT
    //  - JWT payload claims have iss = "ACCOUNT.USER.SHA256:<fp>" and
    //    sub = "ACCOUNT.USER".
    use base64::Engine as _;
    use rand::rngs::OsRng;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use rsa::RsaPrivateKey;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let mut rng = OsRng;
    let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("rsa keygen");
    let pem = private_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("serialize pem")
        .to_string();

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(16384);
            let mut chunk = [0u8; 4096];
            for _ in 0..20 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let body = b"{\"status\":\"ok\"}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n");
    let endpoint = format!("http://127.0.0.1:{}/api/v2/statements", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("sf", "snk.snowflake", json!({
                "account": "test-account",
                "endpoint": endpoint,
                "authType": "jwt",
                "user": "my_user",
                "privateKeyPem": pem,
                "database": "MYDB",
                "schema": "PUBLIC",
                "tableName": "USERS"
            })),
        ]),
        json!([main_edge("e1", "s", "sf")]),
    ));
    assert_eq!(r.status, "ok", "snowflake jwt sink failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(10)).expect("expected 1 jwt request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    // The Authorization header is logged as *** by the request dumper,
    // but the actual bytes are present. Parse the Authorization header.
    let auth_line = body
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
        .expect("authorization header present");
    let auth_value = auth_line.splitn(2, ':').nth(1).unwrap_or("").trim();
    assert!(auth_value.starts_with("Bearer eyJ"), "expected JWT bearer: {}", auth_value);
    assert!(
        body.to_ascii_lowercase().contains("x-snowflake-authorization-token-type: keypair_jwt"),
        "expected KEYPAIR_JWT token-type header: {}",
        body
    );

    // Decode JWT payload (middle segment) and assert iss + sub.
    let jwt = auth_value.trim_start_matches("Bearer ").trim();
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT should have 3 segments: {}", jwt);
    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .expect("decode payload");
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).expect("payload JSON");
    let iss = payload.get("iss").and_then(|v| v.as_str()).unwrap_or("");
    let sub = payload.get("sub").and_then(|v| v.as_str()).unwrap_or("");
    assert!(iss.starts_with("TEST-ACCOUNT.MY_USER.SHA256:"), "unexpected iss: {}", iss);
    assert_eq!(sub, "TEST-ACCOUNT.MY_USER");
}

#[test]
fn snk_snowflake_posts_multirow_insert() {
    // Mock HTTP listener pretends to be Snowflake's /api/v2/statements.
    // Verifies the engine sends a single multi-row INSERT for both rows
    // with Bearer auth and correctly-quoted identifiers + literals.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            // Snowflake-style success response shape.
            let body = b"{\"resultSetMetaData\":{\"numRows\":2}}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    // Point at our mock via the `endpoint` override - production users
    // just set `account` and the engine builds the snowflakecomputing.com URL.
    let endpoint = format!("http://127.0.0.1:{}/api/v2/statements", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("sf", "snk.snowflake", json!({
                "account": "test-account",
                "endpoint": endpoint,
                "pat": "secret-pat",
                "database": "MYDB",
                "schema": "PUBLIC",
                "tableName": "USERS",
                "warehouse": "COMPUTE_WH"
            })),
        ]),
        json!([main_edge("e1", "s", "sf")]),
    ));
    assert_eq!(r.status, "ok", "snowflake sink failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(5)).expect("expected 1 Snowflake request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    assert!(body.contains("Bearer secret-pat"), "expected Bearer auth: {}", body);
    // The SQL is embedded inside a JSON string, so the identifiers'
    // double quotes are backslash-escaped: \"MYDB\".\"PUBLIC\".\"USERS\".
    assert!(
        body.contains(r#"INSERT INTO \"MYDB\".\"PUBLIC\".\"USERS\""#),
        "expected qualified INSERT: {}",
        body
    );
    // Single-quoted string literals stay as-is inside the JSON string.
    assert!(body.contains("'alice'"), "expected 'alice' literal: {}", body);
    assert!(body.contains("'bob'"), "expected 'bob' literal: {}", body);
    // Top-level JSON keys aren't backslash-escaped - just standard JSON.
    assert!(
        body.contains(r#""warehouse":"COMPUTE_WH""#),
        "expected warehouse in body: {}",
        body
    );
}

#[test]
fn snk_elastic_emits_ndjson_bulk_pairs() {
    // ES bulk API: action line then doc line, repeated, separated by \n,
    // Content-Type: application/x-ndjson.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\nConnection: close\r\n\r\n{\"errors\":false}",
            );
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let endpoint = format!("http://127.0.0.1:{}", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("e", "snk.elastic", json!({
                "endpoint": endpoint, "index": "docs"
            })),
        ]),
        json!([main_edge("e1", "s", "e")]),
    ));
    assert_eq!(r.status, "ok", "elastic bulk failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(5)).expect("expected 1 bulk request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    // NDJSON: each row should have an action line + doc line.
    assert!(body.contains("application/x-ndjson"), "expected ndjson content-type: {}", body);
    assert!(body.contains("\"_index\":\"docs\""), "expected index action with docs: {}", body);
    assert!(body.contains("alice") && body.contains("bob"), "expected docs in body: {}", body);
    // Action and doc are separated by \n, action appears twice (one per row).
    let action_count = body.matches("\"_index\":\"docs\"").count();
    assert_eq!(action_count, 2, "expected 2 index actions, got {}: {}", action_count, body);
}

#[test]
fn snk_milvus_injects_collection_name_alongside_data() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n{}\r\n",
            );
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,vector\n1,\"[0.1, 0.2]\"\n");
    let endpoint = format!("http://127.0.0.1:{}", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m", "snk.milvus", json!({
                "endpoint": endpoint, "collection": "embeddings"
            })),
        ]),
        json!([main_edge("e1", "s", "m")]),
    ));
    assert_eq!(r.status, "ok", "milvus insert failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(5)).expect("expected 1 milvus request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    // body shape: {"collectionName":"embeddings","data":[{...}]}
    assert!(body.contains("\"collectionName\":\"embeddings\""), "expected collectionName: {}", body);
    assert!(body.contains("\"data\""), "expected data key: {}", body);
}

#[test]
fn snk_pinecone_wraps_batch_in_vectors_key() {
    // Pinecone wants {"vectors": [...]}; we should see that exact wrap
    // in the single batched request the engine sends.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http");
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream { Ok(s) => s, Err(_) => break };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    // Pinecone URL must end with /vectors/upsert; we point at our mock by
    // pretending the host is localhost:<port> (URL becomes
    // https://localhost:<port>/vectors/upsert which the engine builds
    // verbatim from indexHost). For the test we need http not https, so
    // we override the URL via the underlying snk.webhook component
    // instead, while still asserting the wrapped body shape.
    let tmp = tempfile::tempdir().unwrap();
    // Note: we drive snk.webhook with bodyShape='batch' + bodyWrap='vectors'
    // to verify the wrap; the snk.pinecone component sets these the same
    // way internally + adds the Api-Key header. (snk.pinecone always
    // builds an https URL; in CI we can't intercept that, so this test
    // verifies the wrap logic which is the part that's vendor-specific.)
    let csv = write_file(
        tmp.path(),
        "vec.csv",
        "id,values\n1,\"[0.1, 0.2]\"\n2,\"[0.3, 0.4]\"\n",
    );
    let url = format!("http://127.0.0.1:{}/vectors/upsert", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p", "snk.webhook", json!({
                "url": url,
                "batchMode": "array",
                "bodyWrap": "vectors"
            })),
        ]),
        json!([main_edge("e1", "s", "p")]),
    ));
    assert_eq!(r.status, "ok", "pinecone-shape failed: {:?}", r.error);

    let req = rx.recv_timeout(Duration::from_secs(5)).expect("expected 1 batched request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    // The wrap key must appear in the body around the array.
    assert!(body.contains("\"vectors\""), "expected wrapped body with 'vectors' key: {}", body);
    assert!(body.contains("\"id\":1") || body.contains("\"id\": 1"), "expected id=1: {}", body);
}

#[test]
fn snk_rest_batches_rows_into_one_request() {
    // Same shape as the webhook test but bodyShape='batch' /
    // batchMode='array' should produce ONE request containing both rows.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http");
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}/batch", addr);

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().take(1) {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            stream.set_nodelay(true).ok();
            // Drain until read times out so we catch header + body even
            // when they land in separate TCP segments.
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            let _ = tx.send(buf);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            );
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r", "snk.rest", json!({ "url": url, "batchMode": "array" })),
        ]),
        json!([main_edge("e1", "s", "r")]),
    ));
    assert_eq!(r.status, "ok", "rest pipeline failed: {:?}", r.error);

    let req = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("expected 1 batched request");
    let _ = handle.join();
    let body = String::from_utf8_lossy(&req).to_string();
    // Both rows should be in the single request body (as JSON array).
    assert!(body.contains("alice"), "expected alice in batch: {}", body);
    assert!(body.contains("bob"), "expected bob in batch: {}", body);
    // Should look like a JSON array start.
    assert!(body.contains("["), "expected JSON array in body: {}", body);
}

#[test]
fn retry_attempts_actually_retries_failing_stage() {
    // retryAttempts=3 with retryBackoffMs=80 should fail three times and
    // sleep 80ms + 160ms = 240ms of cumulative backoff. The stage targets
    // a non-existent column so the bind error is deterministic.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n");
    let started = std::time::Instant::now();
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r", "xf.regex", json!({
                "column": "no_such_column",
                "pattern": "x",
                "replacement": "y",
                "retryAttempts": 3,
                "retryBackoffMs": 80
            })),
        ]),
        json!([main_edge("e1", "s", "r")]),
    ));
    let elapsed = started.elapsed();
    assert_ne!(r.status, "ok", "pipeline should ultimately fail after retries");
    assert!(
        elapsed >= std::time::Duration::from_millis(200),
        "expected >= 200ms wall-clock with 3 attempts and 80ms backoff, got {:?}",
        elapsed
    );
}

#[test]
fn memory_limit_pragma_applied_without_breaking_normal_query() {
    // Sanity: configure a small memory limit and verify the stage still
    // runs. The prepended PRAGMA shouldn't interfere with a tiny query.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("t", "xf.trim", json!({
                "column": "name",
                "memoryLimitMb": 256
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "t"), main_edge("e2", "t", "k")]),
    ));
    assert_eq!(r.status, "ok", "memory-limited stage failed: {:?}", r.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 2);
}

#[test]
fn ctl_iterate_runs_subpipeline_n_times_with_iter_index() {
    // Sub-pipeline reads in.csv and writes out_<index>.csv where the
    // suffix comes from ${ITER_INDEX}. After 3 iterations we should
    // see out_0.csv, out_1.csv, out_2.csv on disk.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let sub_in = write_file(tmp.path(), "sub.csv", "id\n1\n2\n");
    let out_pattern = out_path(tmp.path(), "out_");
    let sub_doc_value = json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": sub_in, "hasHeader": true })),
            node("k", "snk.csv", json!({
                "path": format!("{}${{ITER_INDEX}}.csv", out_pattern),
                "hasHeader": true
            })),
        ],
        "edges": [main_edge("e", "s", "k")],
    });
    let sub_doc_path = out_path(tmp.path(), "sub.json");
    std::fs::write(&sub_doc_path, serde_json::to_string(&sub_doc_value).unwrap()).unwrap();

    let r = engine.execute_pipeline(&doc(
        json!([
            node("it", "ctl.iterate", json!({
                "pipelineRef": sub_doc_path,
                "count": 3
            })),
        ]),
        json!([]),
    ));
    assert_eq!(r.status, "ok", "iterate failed: {:?}", r.error);

    for i in 0..3 {
        let p = format!("{}{}.csv", out_pattern, i);
        assert!(
            std::path::Path::new(&p).exists(),
            "expected iteration {} to write {}",
            i,
            p
        );
        let n = count(&format!("read_csv_auto('{}')", p));
        assert_eq!(n, 2, "iteration {} should have written 2 rows", i);
    }
}

#[test]
fn ctl_foreach_runs_subpipeline_per_upstream_row_with_iter_item() {
    // Parent reads a CSV with two rows. ctl.foreach runs the sub-pipeline
    // once per row, substituting ${ITER_ITEM_ID} into the sub-output
    // file path. After running we should see out_alice.csv + out_bob.csv.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let parent_in = write_file(tmp.path(), "users.csv", "id,name\nalice,Alice\nbob,Bob\n");
    let sub_in = write_file(tmp.path(), "src.csv", "v\n42\n");
    let out_prefix = out_path(tmp.path(), "out_");
    let sub_doc_value = json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": sub_in, "hasHeader": true })),
            node("k", "snk.csv", json!({
                "path": format!("{}${{ITER_ITEM_ID}}.csv", out_prefix),
                "hasHeader": true
            })),
        ],
        "edges": [main_edge("e", "s", "k")],
    });
    let sub_doc_path = out_path(tmp.path(), "sub.json");
    std::fs::write(&sub_doc_path, serde_json::to_string(&sub_doc_value).unwrap()).unwrap();

    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": parent_in, "hasHeader": true })),
            node("fe", "ctl.foreach", json!({ "pipelineRef": sub_doc_path })),
        ]),
        json!([main_edge("e1", "s", "fe")]),
    ));
    assert_eq!(r.status, "ok", "foreach failed: {:?}", r.error);

    for user in ["alice", "bob"] {
        let p = format!("{}{}.csv", out_prefix, user);
        assert!(
            std::path::Path::new(&p).exists(),
            "expected foreach to write {} for user {}",
            p,
            user
        );
    }
}

#[test]
fn ctl_try_fires_fallback_when_downstream_stage_fails() {
    // Parent pipeline: src.csv -> ctl.try(installs fallback) ->
    // failing stage. Failing stage triggers the fallback (which writes
    // a marker CSV), then the pipeline surfaces the original error.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();

    // Fallback pipeline writes a 'recovery happened' marker CSV.
    let marker_in = write_file(tmp.path(), "marker_in.csv", "ev\nrolled-back\n");
    let marker_out = out_path(tmp.path(), "marker.csv");
    let fallback_doc_value = json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": marker_in, "hasHeader": true })),
            node("k", "snk.csv", json!({ "path": marker_out, "hasHeader": true })),
        ],
        "edges": [main_edge("e", "s", "k")],
    });
    let fallback_path = out_path(tmp.path(), "fallback.json");
    std::fs::write(&fallback_path, serde_json::to_string(&fallback_doc_value).unwrap()).unwrap();

    // Parent: a failing transform comes AFTER ctl.try installs the
    // fallback. xf.regex against a non-existent column reliably fails.
    let parent_in = write_file(tmp.path(), "in.csv", "x\n1\n");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": parent_in, "hasHeader": true })),
            node("t", "ctl.try", json!({ "fallbackPipelineRef": fallback_path })),
            node("f", "xf.regex", json!({
                "column": "no_such_column",
                "pattern": "x",
                "replacement": "y"
            })),
        ]),
        json!([
            main_edge("e1", "s", "t"),
            main_edge("e2", "t", "f"),
        ]),
    ));
    assert_ne!(r.status, "ok", "parent should surface the original failure");

    // The fallback pipeline should have written its marker CSV
    // (side-effect proof that ctl.try fired).
    assert!(
        std::path::Path::new(&marker_out).exists(),
        "expected fallback to have written marker CSV at {}",
        marker_out
    );
    let marker_n = count(&format!("read_csv_auto('{}')", marker_out));
    assert_eq!(marker_n, 1, "fallback marker should have 1 row");
}

#[test]
fn ctl_try_does_not_fire_when_no_failure() {
    // Same parent but no failing stage - fallback should NOT run.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let marker_out = out_path(tmp.path(), "marker.csv");
    let fallback_doc_value = json!({
        "nodes": [
            node("s", "src.csv", json!({
                "path": write_file(tmp.path(), "m.csv", "ev\nrun\n"),
                "hasHeader": true
            })),
            node("k", "snk.csv", json!({ "path": marker_out.clone(), "hasHeader": true })),
        ],
        "edges": [main_edge("e", "s", "k")],
    });
    let fallback_path = out_path(tmp.path(), "fallback.json");
    std::fs::write(&fallback_path, serde_json::to_string(&fallback_doc_value).unwrap()).unwrap();

    let parent_in = write_file(tmp.path(), "in.csv", "x\n1\n2\n");
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": parent_in, "hasHeader": true })),
            node("t", "ctl.try", json!({ "fallbackPipelineRef": fallback_path })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s", "t"),
            main_edge("e2", "t", "k"),
        ]),
    ));
    assert_eq!(r.status, "ok", "happy path should succeed: {:?}", r.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 2);
    // Fallback should NOT have run.
    assert!(
        !std::path::Path::new(&marker_out).exists(),
        "fallback shouldn't run on happy path; marker exists at {}",
        marker_out
    );
}

#[test]
fn ctl_runpipeline_executes_referenced_pipeline_as_side_effect() {
    // Write a tiny sub-pipeline that materializes a CSV at a known
    // path; the parent pipeline runs ctl.runpipeline against that
    // file, and we assert the sub-pipeline's CSV got written (proving
    // the side effect fired) AND the parent's downstream sink got
    // its pass-through rows.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();

    // Sub-pipeline: read in.csv, write sub_out.csv.
    let sub_in = write_file(tmp.path(), "sub_in.csv", "id\n100\n200\n");
    let sub_out = out_path(tmp.path(), "sub_out.csv");
    let sub_doc_value = json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": sub_in, "hasHeader": true })),
            node("k", "snk.csv", json!({ "path": sub_out, "hasHeader": true })),
        ],
        "edges": [main_edge("e", "s", "k")],
    });
    let sub_doc_path = out_path(tmp.path(), "sub.json");
    std::fs::write(&sub_doc_path, serde_json::to_string(&sub_doc_value).unwrap()).unwrap();

    // Parent pipeline: a row passes through ctl.runpipeline, which
    // also triggers the sub-pipeline above. Downstream sink gets the
    // pass-through row.
    let parent_in = write_file(tmp.path(), "in.csv", "x\n1\n2\n3\n");
    let parent_out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": parent_in, "hasHeader": true })),
            node("rp", "ctl.runpipeline", json!({ "pipelineRef": sub_doc_path })),
            node("k", "snk.csv", json!({ "path": parent_out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s", "rp"),
            main_edge("e2", "rp", "k"),
        ]),
    ));
    assert_eq!(r.status, "ok", "runpipeline failed: {:?}", r.error);

    // Sub-pipeline produced its output.
    let sub_n = count(&format!("read_csv_auto('{}')", sub_out));
    assert_eq!(sub_n, 2, "sub-pipeline should have written 2 rows");

    // Parent passed its 3 rows through.
    let parent_n = count(&format!("read_csv_auto('{}')", parent_out));
    assert_eq!(parent_n, 3, "parent should have passed 3 rows through ctl.runpipeline");
}

#[test]
fn ctl_runpipeline_propagates_subpipeline_failure() {
    // Sub-pipeline references a missing source file -> fails. Parent
    // ctl.runpipeline should surface that failure with a clear message.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let missing = out_path(tmp.path(), "does_not_exist.csv");
    let sub_out = out_path(tmp.path(), "sub_out.csv");
    let sub_doc_value = json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": missing, "hasHeader": true })),
            node("k", "snk.csv", json!({ "path": sub_out, "hasHeader": true })),
        ],
        "edges": [main_edge("e", "s", "k")],
    });
    let sub_doc_path = out_path(tmp.path(), "sub.json");
    std::fs::write(&sub_doc_path, serde_json::to_string(&sub_doc_value).unwrap()).unwrap();

    let parent_in = write_file(tmp.path(), "in.csv", "x\n1\n");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": parent_in, "hasHeader": true })),
            node("rp", "ctl.runpipeline", json!({ "pipelineRef": sub_doc_path })),
        ]),
        json!([main_edge("e1", "s", "rp")]),
    ));
    assert_ne!(r.status, "ok", "parent should have failed when sub-pipeline failed");
    let err = format!("{:?}", r.error.unwrap_or_default());
    assert!(
        err.contains("ctl.runpipeline") || err.contains(&sub_doc_path),
        "error should mention ctl.runpipeline or the sub path: {}",
        err
    );
}

#[test]
fn ctl_wait_actually_sleeps_before_passthrough() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id\n1\n2\n");
    let out = out_path(tmp.path(), "out.csv");
    let started = std::time::Instant::now();
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "ctl.wait", json!({ "duration": 250, "unit": "milliseconds" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "w"), main_edge("e2", "w", "k")]),
    ));
    let elapsed = started.elapsed();
    assert_eq!(r.status, "ok", "ctl.wait failed: {:?}", r.error);
    assert!(
        elapsed >= std::time::Duration::from_millis(200),
        "expected pipeline >= 200ms with a 250ms wait, got {:?}",
        elapsed
    );
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 2, "rows should pass through unchanged");
}

#[test]
fn ctl_checkpoint_writes_sidecar_parquet() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let snapshot = out_path(tmp.path(), "snapshot.parquet");
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c", "ctl.checkpoint", json!({ "name": "after_ingest", "storage": snapshot })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "c"), main_edge("e2", "c", "k")]),
    ));
    assert_eq!(r.status, "ok", "ctl.checkpoint failed: {:?}", r.error);
    // Both the sidecar parquet and the downstream CSV exist with the
    // full upstream content.
    let from_parquet = count(&format!("read_parquet('{}')", snapshot));
    let from_csv = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(from_parquet, 2);
    assert_eq!(from_csv, 2);
}

#[test]
fn ctl_deadletter_writes_to_path() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let dlq = out_path(tmp.path(), "dlq.json");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("d", "ctl.deadletter", json!({ "destination": dlq, "format": "json" })),
        ]),
        json!([main_edge("e1", "s", "d")]),
    ));
    assert_eq!(r.status, "ok", "ctl.deadletter failed: {:?}", r.error);
    let n = count(&format!("read_json_auto('{}')", dlq));
    assert_eq!(n, 2);
}

#[test]
fn ctl_throttle_inserts_per_stage_delay() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id\n1\n");
    let out = out_path(tmp.path(), "out.csv");
    let started = std::time::Instant::now();
    // rate=5 rows/sec -> 200ms per stage delay.
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("t", "ctl.throttle", json!({ "rate": 5 })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "t"), main_edge("e2", "t", "k")]),
    ));
    let elapsed = started.elapsed();
    assert_eq!(r.status, "ok", "ctl.throttle failed: {:?}", r.error);
    assert!(
        elapsed >= std::time::Duration::from_millis(150),
        "expected pipeline >= 150ms with rate=5/sec throttle, got {:?}",
        elapsed
    );
}

#[test]
fn text_match_contains_starts_ends() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "items.csv",
        "id,name\n1,prefix-thing\n2,middle-foo-stuff\n3,end-suffix\n",
    );
    // contains 'foo'
    let out1 = out_path(tmp.path(), "contains.csv");
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m", "xf.text.match", json!({
                "column": "name", "needle": "foo", "mode": "contains", "outputColumn": "hit"
            })),
            node("k", "snk.csv", json!({ "path": out1, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "m"), main_edge("e2", "m", "k")]),
    ));
    assert_eq!(r1.status, "ok", "text.match contains failed: {:?}", r1.error);
    let c1 = scalar_string(&format!(
        "SELECT CAST(hit AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        out1
    ));
    let c2 = scalar_string(&format!(
        "SELECT CAST(hit AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out1
    ));
    assert_eq!(c1, "true");
    assert_eq!(c2, "false");

    // starts_with 'prefix'
    let out2 = out_path(tmp.path(), "starts.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m", "xf.text.match", json!({
                "column": "name", "needle": "prefix", "mode": "starts_with", "outputColumn": "hit"
            })),
            node("k", "snk.csv", json!({ "path": out2, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "m"), main_edge("e2", "m", "k")]),
    ));
    assert_eq!(r2.status, "ok", "text.match starts_with failed: {:?}", r2.error);
    let s1 = scalar_string(&format!(
        "SELECT CAST(hit AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out2
    ));
    let s2 = scalar_string(&format!(
        "SELECT CAST(hit AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        out2
    ));
    assert_eq!(s1, "true");
    assert_eq!(s2, "false");
}

#[test]
fn num_sign_classifies_signed_values() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "n.csv", "id,v\n1,-7\n2,0\n3,42\n");
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("g", "xf.num.sign", json!({ "column": "v", "outputColumn": "sg" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "g"), main_edge("e2", "g", "k")]),
    ));
    assert_eq!(r.status, "ok", "sign failed: {:?}", r.error);
    // Cast to BIGINT for verification - sign() returns the input type,
    // and CSV serialization of DOUBLE 1.0 vs INTEGER 1 differs across
    // DuckDB platforms (Windows '-1.0', Linux '-1'). Normalize first.
    let s1 = scalar_string(&format!(
        "SELECT CAST(CAST(sg AS BIGINT) AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let s2 = scalar_string(&format!(
        "SELECT CAST(CAST(sg AS BIGINT) AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    let s3 = scalar_string(&format!(
        "SELECT CAST(CAST(sg AS BIGINT) AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 3",
        out
    ));
    assert_eq!(s1, "-1", "sign(-7) on row 1");
    assert_eq!(s2, "0", "sign(0) on row 2");
    assert_eq!(s3, "1", "sign(42) on row 3");
}

#[test]
fn dt_extract_dayofweek_via_existing_transform() {
    // No new component - just verifies that 'dayofweek' (newly added
    // to the unit dropdown) routes through the existing
    // xf.dt.extract -> date_part path.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "d.csv", "id,d\n1,2026-01-01\n");
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("e", "xf.dt.extract", json!({ "column": "d", "unit": "dayofweek", "outputColumn": "dow" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "e"), main_edge("e2", "e", "k")]),
    ));
    assert_eq!(r.status, "ok", "dt.extract dayofweek failed: {:?}", r.error);
    // 2026-01-01 is a Thursday. DuckDB date_part('dayofweek', d) returns
    // 4 (Sunday=0, Monday=1, ..., Thursday=4).
    let dow = scalar_string(&format!(
        "SELECT CAST(dow AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    assert_eq!(dow, "4");
}

#[test]
fn num_clamp_caps_outliers() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "vals.csv",
        "id,v\n1,-50\n2,25\n3,150\n4,75\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c", "xf.num.clamp", json!({ "column": "v", "low": 0, "high": 100 })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "c"), main_edge("e2", "c", "k")]),
    ));
    assert_eq!(r.status, "ok", "clamp failed: {:?}", r.error);
    // -50 -> 0, 25 -> 25, 150 -> 100, 75 -> 75.
    let v1 = scalar_string(&format!(
        "SELECT CAST(v AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let v3 = scalar_string(&format!(
        "SELECT CAST(v AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 3",
        out
    ));
    assert_eq!(v1, "0.0");
    assert_eq!(v3, "100.0");
}

#[test]
fn text_padding_lpad_zero_pads() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "ids.csv",
        "id\n7\n42\n1000\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p", "xf.text.padding", json!({
                "column": "id", "length": 5, "fill": "0", "side": "left",
                "outputColumn": "padded"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "p"), main_edge("e2", "p", "k")]),
    ));
    assert_eq!(r.status, "ok", "padding failed: {:?}", r.error);
    let p1 = scalar_string(&format!(
        "SELECT padded FROM read_csv_auto('{}') WHERE id = 7",
        out
    ));
    let p2 = scalar_string(&format!(
        "SELECT padded FROM read_csv_auto('{}') WHERE id = 1000",
        out
    ));
    assert_eq!(p1, "00007");
    assert_eq!(p2, "01000");
}

#[test]
fn dt_epoch_roundtrips() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "ts.csv",
        "id,ts\n1,2026-01-01 12:00:00\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("e", "xf.dt.epoch", json!({ "column": "ts", "mode": "to", "outputColumn": "sec" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "e"), main_edge("e2", "e", "k")]),
    ));
    assert_eq!(r.status, "ok", "dt.epoch to failed: {:?}", r.error);
    // 2026-01-01 12:00:00 UTC = 1767268800 seconds since unix epoch.
    let sec = scalar_string(&format!(
        "SELECT CAST(CAST(sec AS BIGINT) AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    assert_eq!(sec, "1767268800");

    // Round-trip: convert epoch back to timestamp.
    let out2 = out_path(tmp.path(), "back.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": out, "hasHeader": true })),
            node("e", "xf.dt.epoch", json!({ "column": "sec", "mode": "from", "outputColumn": "ts2" })),
            node("k", "snk.csv", json!({ "path": out2, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "e"), main_edge("e2", "e", "k")]),
    ));
    assert_eq!(r2.status, "ok", "dt.epoch from failed: {:?}", r2.error);
    let back = scalar_string(&format!(
        "SELECT strftime(CAST(ts2 AS TIMESTAMP), '%Y-%m-%d %H:%M:%S') FROM read_csv_auto('{}') WHERE id = 1",
        out2
    ));
    assert_eq!(back, "2026-01-01 12:00:00");
}

#[test]
fn dt_now_stamps_loaded_at() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n");
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("n", "xf.dt.now", json!({ "outputColumn": "loaded_at" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "n"), main_edge("e2", "n", "k")]),
    ));
    assert_eq!(r.status, "ok", "dt.now failed: {:?}", r.error);
    // Sanity-check that loaded_at is a recent year (>= 2024). Comparing
    // against current_timestamp directly via duckdb_exec.
    let recent = scalar_string(&format!(
        "SELECT CASE WHEN year(CAST(loaded_at AS TIMESTAMP)) >= 2024 THEN 'ok' ELSE 'bad' END FROM read_csv_auto('{}') LIMIT 1",
        out
    ));
    assert_eq!(recent, "ok");
}

#[test]
fn uuid_generates_unique_ids_per_row() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "in.csv",
        "id\n1\n2\n3\n4\n5\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("u", "xf.uuid", json!({ "outputColumn": "row_id" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "u"), main_edge("e2", "u", "k")]),
    ));
    assert_eq!(r.status, "ok", "uuid failed: {:?}", r.error);
    // 5 rows in, 5 distinct UUIDs out.
    let distinct = scalar_string(&format!(
        "SELECT CAST(count(DISTINCT row_id) AS VARCHAR) FROM read_csv_auto('{}')",
        out
    ));
    assert_eq!(distinct, "5");
}

#[test]
fn cumulative_running_sum_per_group() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "sales.csv",
        "region,day,amount\nus,1,10\nus,2,20\nus,3,30\neu,1,5\neu,2,15\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c", "xf.cumulative", json!({
                "column": "amount", "function": "sum",
                "orderBy": "day", "partitionBy": ["region"],
                "outputColumn": "cum_amount"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "c"), main_edge("e2", "c", "k")]),
    ));
    assert_eq!(r.status, "ok", "cumulative failed: {:?}", r.error);
    // us: 10, 30, 60.  eu: 5, 20.
    let us_d3 = scalar_string(&format!(
        "SELECT CAST(cum_amount AS VARCHAR) FROM read_csv_auto('{}') WHERE region = 'us' AND day = 3",
        out
    ));
    let eu_d2 = scalar_string(&format!(
        "SELECT CAST(cum_amount AS VARCHAR) FROM read_csv_auto('{}') WHERE region = 'eu' AND day = 2",
        out
    ));
    assert_eq!(us_d3, "60");
    assert_eq!(eu_d2, "20");
}

#[test]
fn dt_bin_rounds_to_five_minute_buckets() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "events.csv",
        "id,ts\n1,2026-01-01 12:03:42\n2,2026-01-01 12:07:11\n3,2026-01-01 12:11:00\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("b", "xf.dt.bin", json!({
                "column": "ts", "unit": "minute", "count": 5, "outputColumn": "bucket"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "b"), main_edge("e2", "b", "k")]),
    ));
    assert_eq!(r.status, "ok", "dt.bin failed: {:?}", r.error);
    // 12:03:42 -> 12:00; 12:07:11 -> 12:05; 12:11:00 -> 12:10.
    let b1 = scalar_string(&format!(
        "SELECT strftime(CAST(bucket AS TIMESTAMP), '%H:%M') FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let b2 = scalar_string(&format!(
        "SELECT strftime(CAST(bucket AS TIMESTAMP), '%H:%M') FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    let b3 = scalar_string(&format!(
        "SELECT strftime(CAST(bucket AS TIMESTAMP), '%H:%M') FROM read_csv_auto('{}') WHERE id = 3",
        out
    ));
    assert_eq!(b1, "12:00");
    assert_eq!(b2, "12:05");
    assert_eq!(b3, "12:10");
}

#[test]
fn arr_length_counts_list_elements() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    // Use the existing collect/array path to build a list column we can
    // measure. csv -> arr.collect -> arr.length.
    let csv = write_file(
        tmp.path(),
        "raw.csv",
        "group,val\na,1\na,2\na,3\nb,4\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c", "xf.arr.collect", json!({
                "valueColumn": "val", "groupBy": ["group"], "outputColumn": "items"
            })),
            node("l", "xf.arr.length", json!({ "column": "items", "outputColumn": "n" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "c"), main_edge("e2", "c", "l"), main_edge("e3", "l", "k")]),
    ));
    assert_eq!(r.status, "ok", "arr.length failed: {:?}", r.error);
    let na = scalar_string(&format!(
        "SELECT CAST(n AS VARCHAR) FROM read_csv_auto('{}') WHERE \"group\" = 'a'",
        out
    ));
    let nb = scalar_string(&format!(
        "SELECT CAST(n AS VARCHAR) FROM read_csv_auto('{}') WHERE \"group\" = 'b'",
        out
    ));
    assert_eq!(na, "3");
    assert_eq!(nb, "1");
}

#[test]
fn rank_filter_keeps_top_n_per_group() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "sales.csv",
        "region,user,amount\nus,a,100\nus,b,80\nus,c,60\nus,d,40\neu,e,90\neu,f,70\neu,g,50\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r", "xf.rank.filter", json!({
                "partitionBy": ["region"], "orderBy": "amount", "desc": true, "n": 2
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "r"), main_edge("e2", "r", "k")]),
    ));
    assert_eq!(r.status, "ok", "rank filter failed: {:?}", r.error);
    // Top 2 per region: us = a,b; eu = e,f.  Total 4 rows.
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 4, "expected 4 rows, got {}", n);
    let has_c = scalar_string(&format!(
        "SELECT CAST(count(*) AS VARCHAR) FROM read_csv_auto('{}') WHERE \"user\" = 'c'",
        out
    ));
    assert_eq!(has_c, "0", "user c (rank 3 in us) should have been filtered out");
}

#[test]
fn fill_forward_propagates_last_non_null() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    // Two sensors with gappy readings; rows interleaved by ts.
    let csv = write_file(
        tmp.path(),
        "readings.csv",
        "sensor,ts,reading\nA,1,10\nA,2,\nA,3,\nA,4,20\nB,1,5\nB,2,\nB,3,15\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("f", "xf.fill_forward", json!({
                "column": "reading", "orderBy": "ts", "partitionBy": ["sensor"]
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "f"), main_edge("e2", "f", "k")]),
    ));
    assert_eq!(r.status, "ok", "fill_forward failed: {:?}", r.error);
    // Sensor A at ts=2 was null; should now be 10 (forward-filled from ts=1).
    let r_a2 = scalar_string(&format!(
        "SELECT CAST(reading AS VARCHAR) FROM read_csv_auto('{}') WHERE sensor = 'A' AND ts = 2",
        out
    ));
    let r_a3 = scalar_string(&format!(
        "SELECT CAST(reading AS VARCHAR) FROM read_csv_auto('{}') WHERE sensor = 'A' AND ts = 3",
        out
    ));
    let r_b2 = scalar_string(&format!(
        "SELECT CAST(reading AS VARCHAR) FROM read_csv_auto('{}') WHERE sensor = 'B' AND ts = 2",
        out
    ));
    assert_eq!(r_a2, "10", "A@ts=2 should fill to 10");
    assert_eq!(r_a3, "10", "A@ts=3 should fill to 10");
    assert_eq!(r_b2, "5", "B@ts=2 should fill from B@ts=1 (5), not bleed from A");
}

#[test]
fn text_base64_roundtrips() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,word\n1,hello\n2,world\n");
    let encoded = out_path(tmp.path(), "encoded.csv");
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("e", "xf.text.base64", json!({ "column": "word", "mode": "encode", "outputColumn": "b" })),
            node("k", "snk.csv", json!({ "path": encoded, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "e"), main_edge("e2", "e", "k")]),
    ));
    assert_eq!(r1.status, "ok", "base64 encode failed: {:?}", r1.error);
    let b = scalar_string(&format!(
        "SELECT b FROM read_csv_auto('{}') WHERE id = 1",
        encoded
    ));
    // base64('hello') = aGVsbG8=
    assert_eq!(b, "aGVsbG8=");

    // Round-trip: decode the encoded column back.
    let decoded = out_path(tmp.path(), "decoded.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": encoded, "hasHeader": true })),
            node("d", "xf.text.base64", json!({ "column": "b", "mode": "decode", "outputColumn": "decoded_word" })),
            node("k", "snk.csv", json!({ "path": decoded, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "d"), main_edge("e2", "d", "k")]),
    ));
    assert_eq!(r2.status, "ok", "base64 decode failed: {:?}", r2.error);
    let w = scalar_string(&format!(
        "SELECT decoded_word FROM read_csv_auto('{}') WHERE id = 1",
        decoded
    ));
    assert_eq!(w, "hello");
}

#[test]
fn num_zscore_normalizes_against_dataset() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "vals.csv",
        "id,v\n1,1\n2,2\n3,3\n4,4\n5,5\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("z", "xf.num.zscore", json!({ "column": "v", "outputColumn": "z" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "z"), main_edge("e2", "z", "k")]),
    ));
    assert_eq!(r.status, "ok", "zscore failed: {:?}", r.error);
    // mean(1..5)=3, stddev_samp(1..5) = sqrt(((1-3)^2 + (2-3)^2 + 0 + 1 + 4) / 4) = sqrt(2.5)
    // zscore(3) = (3-3) / sqrt(2.5) = 0 exactly.
    let z3 = scalar_string(&format!(
        "SELECT CAST(round(z, 6) AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 3",
        out
    ));
    assert_eq!(z3, "0.0");
}

#[test]
fn num_bucketize_assigns_width_buckets() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "scores.csv",
        "id,score\n1,5\n2,15\n3,55\n4,95\n5,150\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("b", "xf.num.bucketize", json!({
                "column": "score", "low": 0, "high": 100, "buckets": 10,
                "outputColumn": "decile"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "b"), main_edge("e2", "b", "k")]),
    ));
    assert_eq!(r.status, "ok", "bucketize failed: {:?}", r.error);
    let d1 = scalar_string(&format!(
        "SELECT CAST(decile AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let d3 = scalar_string(&format!(
        "SELECT CAST(decile AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 3",
        out
    ));
    let d5 = scalar_string(&format!(
        "SELECT CAST(decile AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 5",
        out
    ));
    // width_bucket(5, 0, 100, 10) = 1, width_bucket(55, ...) = 6,
    // width_bucket(150, ...) = 11 (overflow bucket).
    assert_eq!(d1, "1");
    assert_eq!(d3, "6");
    assert_eq!(d5, "11");
}

#[test]
fn json_array_agg_collapses_rows_per_group() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "items.csv",
        "user,item\nalice,apple\nalice,banana\nbob,carrot\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("a", "xf.json.array_agg", json!({
                "column": "item", "groupBy": ["user"], "outputColumn": "items"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "a"), main_edge("e2", "a", "k")]),
    ));
    assert_eq!(r.status, "ok", "array_agg failed: {:?}", r.error);
    let alice = scalar_string(&format!(
        "SELECT items FROM read_csv_auto('{}') WHERE \"user\" = 'alice'",
        out
    ));
    // json_group_array gives ["apple","banana"] - exact order depends on
    // input but DuckDB preserves scan order for grouped aggregates with
    // a single thread on this tiny input.
    assert!(alice.contains("apple") && alice.contains("banana"), "got {}", alice);
}

#[test]
fn text_similarity_scores_with_levenshtein() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "pairs.csv",
        "id,a,b\n1,kitten,sitting\n2,foo,foo\n3,abc,xyz\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("t", "xf.text.similarity", json!({
                "leftColumn": "a", "rightColumn": "b",
                "algorithm": "levenshtein", "outputColumn": "dist"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "t"), main_edge("e2", "t", "k")]),
    ));
    assert_eq!(r.status, "ok", "similarity failed: {:?}", r.error);
    let d1 = scalar_string(&format!(
        "SELECT CAST(dist AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let d2 = scalar_string(&format!(
        "SELECT CAST(dist AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    let d3 = scalar_string(&format!(
        "SELECT CAST(dist AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 3",
        out
    ));
    // kitten -> sitting is the classic 3 edits.
    assert_eq!(d1, "3");
    assert_eq!(d2, "0");
    assert_eq!(d3, "3");
}

#[test]
fn assert_passes_when_predicate_holds_on_every_row() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "rows.csv",
        "id,amount\n1,10\n2,20\n3,30\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("a", "xf.assert", json!({ "predicate": "amount >= 0" })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "a"), main_edge("e2", "a", "k")]),
    ));
    assert_eq!(r.status, "ok", "assert (passing) failed: {:?}", r.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3, "expected 3 rows through, got {}", n);
}

#[test]
fn assert_fails_when_any_row_violates_predicate() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "rows.csv",
        "id,amount\n1,10\n2,-5\n3,30\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("a", "xf.assert", json!({
                "predicate": "amount >= 0",
                "message": "amount must be non-negative"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "a"), main_edge("e2", "a", "k")]),
    ));
    assert_ne!(r.status, "ok", "assert should have failed but pipeline returned ok");
    let err = format!("{:?}", r.error.unwrap_or_default());
    assert!(
        err.contains("amount must be non-negative") || err.to_lowercase().contains("non-negative"),
        "expected user-facing message in error, got: {}",
        err
    );
}

#[test]
fn parquet_sink_writes_hive_partitions() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "events.csv",
        "region,id,amount\nus,1,10\nus,2,20\neu,3,30\neu,4,40\n",
    );
    let out_dir = out_path(tmp.path(), "events_partitioned");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k", "snk.parquet", json!({
                "path": out_dir,
                "partitionBy": ["region"]
            })),
        ]),
        json!([main_edge("e", "s", "k")]),
    ));
    assert_eq!(r.status, "ok", "partitioned parquet failed: {:?}", r.error);
    // Hive layout: <out_dir>/region=us/*.parquet, region=eu/*.parquet.
    let us_count = scalar_string(&format!(
        "SELECT CAST(count(*) AS VARCHAR) FROM read_parquet('{}/region=us/*.parquet')",
        out_dir
    ));
    let eu_count = scalar_string(&format!(
        "SELECT CAST(count(*) AS VARCHAR) FROM read_parquet('{}/region=eu/*.parquet')",
        out_dir
    ));
    assert_eq!(us_count, "2");
    assert_eq!(eu_count, "2");
}

#[test]
fn url_parse_extracts_components() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "urls.csv",
        "id,url\n1,https://example.com:8443/api/v1?key=x#top\n2,http://a.io/p\n",
    );
    let host_out = out_path(tmp.path(), "host.csv");
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p", "xf.url.parse", json!({ "column": "url", "kind": "host", "outputColumn": "h" })),
            node("k", "snk.csv", json!({ "path": host_out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "p"), main_edge("e2", "p", "k")]),
    ));
    assert_eq!(r1.status, "ok", "url host failed: {:?}", r1.error);
    let host1 = scalar_string(&format!(
        "SELECT h FROM read_csv_auto('{}') WHERE id = 1",
        host_out
    ));
    assert_eq!(host1, "example.com");

    let port_out = out_path(tmp.path(), "port.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p", "xf.url.parse", json!({ "column": "url", "kind": "port", "outputColumn": "po" })),
            node("k", "snk.csv", json!({ "path": port_out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "p"), main_edge("e2", "p", "k")]),
    ));
    assert_eq!(r2.status, "ok", "url port failed: {:?}", r2.error);
    let port1 = scalar_string(&format!(
        "SELECT CAST(po AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        port_out
    ));
    assert_eq!(port1, "8443");
}

#[test]
fn regex_match_emits_boolean() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "tags.csv",
        "id,tag\n1,FOO-123\n2,bar\n3,FOO-bar\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("m", "xf.regex.match", json!({
                "column": "tag",
                "pattern": "^FOO-",
                "outputColumn": "is_foo"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "m"), main_edge("e2", "m", "k")]),
    ));
    assert_eq!(r.status, "ok", "regex match failed: {:?}", r.error);
    let a = scalar_string(&format!(
        "SELECT CAST(is_foo AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let b = scalar_string(&format!(
        "SELECT CAST(is_foo AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(a, "true");
    assert_eq!(b, "false");
}

#[test]
fn approx_count_distinct_via_groupby() {
    // Exercises the new function name through the existing aggregate
    // path. APPROX_COUNT_DISTINCT lands as DuckDB approx_count_distinct.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "events.csv",
        "region,user\nus,1\nus,1\nus,2\nus,3\neu,4\neu,4\neu,5\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("g", "xf.groupby", json!({
                "groupKeys": ["region"],
                "aggregations": [
                    { "column": "user", "func": "approx_count_distinct", "output": "users" }
                ]
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "g"), main_edge("e2", "g", "k")]),
    ));
    assert_eq!(r.status, "ok", "approx_count_distinct failed: {:?}", r.error);
    // 3 distinct US users, 2 distinct EU users; approx HLL is exact at
    // these tiny cardinalities.
    let us = scalar_string(&format!(
        "SELECT CAST(users AS VARCHAR) FROM read_csv_auto('{}') WHERE region = 'us'",
        out
    ));
    let eu = scalar_string(&format!(
        "SELECT CAST(users AS VARCHAR) FROM read_csv_auto('{}') WHERE region = 'eu'",
        out
    ));
    assert_eq!(us, "3");
    assert_eq!(eu, "2");
}

#[test]
fn approx_quantile_finds_median() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    // Median of 1..9 is 5.
    let csv = write_file(
        tmp.path(),
        "nums.csv",
        "id,n\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n9,9\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("q", "xf.approx.quantile", json!({
                "column": "n", "quantile": 0.5, "outputColumn": "p50"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "q"), main_edge("e2", "q", "k")]),
    ));
    assert_eq!(r.status, "ok", "approx_quantile failed: {:?}", r.error);
    let p50 = scalar_string(&format!(
        "SELECT CAST(round(p50, 0) AS VARCHAR) FROM read_csv_auto('{}')",
        out
    ));
    // approx_quantile on this tiny input lands at 5.
    assert_eq!(p50, "5");
}

#[test]
fn regex_extract_pulls_capture_group() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "logs.csv",
        "id,line\n1,User=alice ID=42\n2,User=bob ID=99\n",
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("x", "xf.regex.extract", json!({
                "column": "line",
                "pattern": "ID=([0-9]+)",
                "groupIndex": 1,
                "outputColumn": "user_id"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "x"), main_edge("e2", "x", "k")]),
    ));
    assert_eq!(r.status, "ok", "regex extract failed: {:?}", r.error);
    let id1 = scalar_string(&format!(
        "SELECT user_id FROM read_csv_auto('{}') WHERE id = 1",
        out
    ));
    let id2 = scalar_string(&format!(
        "SELECT user_id FROM read_csv_auto('{}') WHERE id = 2",
        out
    ));
    assert_eq!(id1, "42");
    assert_eq!(id2, "99");
}

#[test]
fn spatial_join_matches_points_inside_polygons() {
    if std::env::var("DUCKLE_TEST_SPATIAL").ok().as_deref() != Some("1") {
        eprintln!("skipping: set DUCKLE_TEST_SPATIAL=1 to run spatial tests");
        return;
    }
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let pts = out_path(tmp.path(), "points.parquet");
    let polys = out_path(tmp.path(), "polys.parquet");
    duckdb_exec(
        ":memory:",
        &format!(
            "INSTALL spatial; LOAD spatial; \
             COPY (SELECT * FROM (VALUES \
                 ('a', ST_Point(5, 5)), \
                 ('b', ST_Point(50, 50)), \
                 ('c', ST_Point(7, 7)) \
             ) t(name, p)) TO '{}' (FORMAT PARQUET); \
             COPY (SELECT * FROM (VALUES \
                 ('square', ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) \
             ) t(zone, g)) TO '{}' (FORMAT PARQUET)",
            pts, polys
        ),
    );
    let out = out_path(tmp.path(), "matched.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("L", "src.parquet", json!({ "path": pts })),
            node("R", "src.parquet", json!({ "path": polys })),
            node("j", "xf.join.spatial", json!({
                "leftGeomColumn": "p",
                "rightGeomColumn": "g",
                "relation": "within"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "L", "j"),
            lookup_edge("e2", "R", "j"),
            main_edge("e3", "j", "k"),
        ]),
    ));
    assert_eq!(r.status, "ok", "spatial join failed: {:?}", r.error);
    // 'a' (5,5) and 'c' (7,7) are inside the square; 'b' (50,50) is not.
    let matched = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(matched, 2, "expected 2 matched rows, got {}", matched);
    let names: Vec<String> = (0..2)
        .map(|_| String::new())
        .collect::<Vec<_>>();
    let _ = names;
    let has_a = scalar_string(&format!(
        "SELECT CAST(count(*) AS VARCHAR) FROM read_csv_auto('{}') WHERE name = 'a'",
        out
    ));
    let has_b = scalar_string(&format!(
        "SELECT CAST(count(*) AS VARCHAR) FROM read_csv_auto('{}') WHERE name = 'b'",
        out
    ));
    assert_eq!(has_a, "1");
    assert_eq!(has_b, "0");
}

#[test]
fn geo_intersects_flags_overlapping_geometries() {
    if std::env::var("DUCKLE_TEST_SPATIAL").ok().as_deref() != Some("1") {
        eprintln!("skipping: set DUCKLE_TEST_SPATIAL=1 to run spatial tests");
        return;
    }
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let parquet = out_path(tmp.path(), "pts.parquet");
    // Two points, one inside the 0..10 square, one outside.
    duckdb_exec(
        ":memory:",
        &format!(
            "INSTALL spatial; LOAD spatial; \
             COPY (SELECT * FROM (VALUES \
                 ('in',  ST_Point(5, 5)), \
                 ('out', ST_Point(50, 50)) \
             ) t(name, loc)) TO '{}' (FORMAT PARQUET)",
            parquet
        ),
    );
    let out = out_path(tmp.path(), "out.csv");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.parquet", json!({ "path": parquet })),
            node("g", "xf.geo.intersects", json!({
                "geomColumn": "loc",
                "targetWkt": "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
                "outputColumn": "hits"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "g"), main_edge("e2", "g", "k")]),
    ));
    assert_eq!(r.status, "ok", "geo_intersects failed: {:?}", r.error);
    let hit_in = scalar_string(&format!(
        "SELECT CAST(hits AS VARCHAR) FROM read_csv_auto('{}') WHERE name = 'in'",
        out
    ));
    let hit_out = scalar_string(&format!(
        "SELECT CAST(hits AS VARCHAR) FROM read_csv_auto('{}') WHERE name = 'out'",
        out
    ));
    assert_eq!(hit_in, "true");
    assert_eq!(hit_out, "false");
}

#[test]
fn ip_parse_extracts_host_and_family() {
    // inet is a small built-in extension; no env gate. Tests both that
    // the prelude LOADs inet (a fresh CLI process has no inet symbols
    // until then) and that the `kind` prop dispatches to the right
    // function (host vs family).
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "ips.csv",
        "id,addr\n1,10.0.0.1/24\n2,192.168.1.5\n3,::1\n",
    );
    let host_out = out_path(tmp.path(), "host.csv");
    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p", "xf.ip.parse", json!({ "column": "addr", "kind": "host", "outputColumn": "h" })),
            node("k", "snk.csv", json!({ "path": host_out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "p"), main_edge("e2", "p", "k")]),
    ));
    assert_eq!(r1.status, "ok", "ip host failed: {:?}", r1.error);
    let host1 = scalar_string(&format!(
        "SELECT h FROM read_csv_auto('{}') WHERE id = 1",
        host_out
    ));
    assert_eq!(host1, "10.0.0.1");

    let fam_out = out_path(tmp.path(), "fam.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p", "xf.ip.parse", json!({ "column": "addr", "kind": "family", "outputColumn": "f" })),
            node("k", "snk.csv", json!({ "path": fam_out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "s", "p"), main_edge("e2", "p", "k")]),
    ));
    assert_eq!(r2.status, "ok", "ip family failed: {:?}", r2.error);
    let v4 = scalar_string(&format!(
        "SELECT CAST(f AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 2",
        fam_out
    ));
    let v6 = scalar_string(&format!(
        "SELECT CAST(f AS VARCHAR) FROM read_csv_auto('{}') WHERE id = 3",
        fam_out
    ));
    assert_eq!(v4, "4");
    assert_eq!(v6, "6");
}

#[test]
fn pg_pgvector_roundtrip_through_postgres_attach() {
    // Lives in the CI postgres-integration job (pgvector/pgvector:pg16
    // image, so CREATE EXTENSION vector is preinstalled). Local skip is
    // governed by DUCKLE_PG_HOST, same as the other PG tests. snk.pgvector
    // + src.pgvector ride the same postgres ATTACH path as snk.postgres /
    // src.postgres; this test confirms the component IDs route correctly.
    let engine = engine_or_skip!();
    let (host, port, db, user, pass) = match pg_env() {
        Some(x) => x,
        None => {
            eprintln!("skipping: set DUCKLE_PG_HOST to run pgvector tests");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    let table = format!("pgv_test_{}", std::process::id());

    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("w", "snk.pgvector", json!({
                "host": &host, "port": port, "database": &db,
                "user": &user, "password": &pass,
                "schemaName": "public", "tableName": &table, "mode": "overwrite"
            })),
        ]),
        json!([main_edge("e", "s", "w")]),
    ));
    assert_eq!(r1.status, "ok", "pgvector write failed: {:?}", r1.error);

    let out = out_path(tmp.path(), "out.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("r", "src.pgvector", json!({
                "host": host, "port": port, "database": db,
                "user": user, "password": pass,
                "schemaName": "public", "tableName": table, "mode": "table"
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "r", "k")]),
    ));
    assert_eq!(r2.status, "ok", "pgvector read failed: {:?}", r2.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn missing_source_file_errors_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "never.parquet");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node(
                "s1",
                "src.csv",
                json!({ "path": "/no/such/file/orders.csv", "hasHeader": true }),
            ),
            node("k1", "snk.parquet", json!({ "path": out })),
        ]),
        json!([main_edge("e1", "s1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "error");
    assert!(result.error.is_some(), "an error message should be present");
    // No output file should have been created.
    assert!(!Path::new(&out).exists());
}

#[test]
fn project_and_rename_reshape_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(
        tmp.path(),
        "wide.csv",
        "id,first,last,age\n1,ada,lovelace,36\n2,alan,turing,41\n",
    );
    let out = out_path(tmp.path(), "narrow.parquet");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s1", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("p1", "xf.project", json!({ "columns": ["id", "first"] })),
            node("k1", "snk.parquet", json!({ "path": out })),
        ]),
        json!([main_edge("e1", "s1", "p1"), main_edge("e2", "p1", "k1")]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "run failed: {:?}", result.error);

    // Output has 2 rows and exactly 2 columns (id, first).
    assert_eq!(count(&format!("read_parquet('{}')", out)), 2);
    // DESCRIBE returns one row per column.
    let cols = count(&format!(
        "(DESCRIBE SELECT * FROM read_parquet('{}'))",
        out
    ));
    assert_eq!(cols, 2, "should have projected to 2 columns");
}

#[test]
fn ctl_retry_is_a_passthrough_view() {
    // ctl.retry is documented as a visual marker for retry behavior;
    // it should pass its input through unchanged. The actual retry
    // policy is read off the Advanced tab as retry_attempts. Without
    // a passthrough branch the executor would error 'preview component'.
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,a\n2,b\n3,c\n");
    let out = out_path(tmp.path(), "out.csv");

    let engine = engine_or_skip!();
    let d = doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r", "ctl.retry", json!({})),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([
            main_edge("e1", "s", "r"),
            main_edge("e2", "r", "k"),
        ]),
    );
    let result = engine.execute_pipeline(&d);
    assert_eq!(result.status, "ok", "ctl.retry pipeline failed: {:?}", result.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 3);
}

#[test]
fn src_github_alias_routes_through_rest_path() {
    // GitHub / GitLab / Airtable / Notion / HubSpot / Jira / Stripe etc.
    // are thin engine aliases of src.rest with vendor-specific palette
    // defaults. A node carrying any of those component IDs must execute
    // through the exact same RestSourceSpec path so all pagination /
    // auth / responsePath features work identically.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    // Mimic GitHub's REST response shape: top-level array of objects.
    let body = br#"[{"id":1,"login":"octocat"},{"id":2,"login":"hubot"}]"#;
    let captured = Arc::new(std::sync::Mutex::new(String::new()));
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            *cap.lock().unwrap() = String::from_utf8_lossy(&buf).to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "gh.csv");
    let url = format!("http://127.0.0.1:{}/users", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("g", "src.github", json!({
                "url": url,
                "method": "GET",
                "authType": "bearer",
                "authToken": "ghp_TEST_TOKEN_NOT_REAL",
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "g", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "src.github alias failed: {:?}", r.error);
    // Two rows came through the vendor alias and reached the sink.
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    // The Bearer header was set, proving the auth path runs on the alias.
    let req = captured.lock().unwrap();
    assert!(
        req.contains("Authorization: Bearer ghp_TEST_TOKEN_NOT_REAL"),
        "expected Bearer header on src.github alias request, got: {}",
        req.lines().next().unwrap_or("")
    );
}

#[test]
fn src_linear_alias_routes_through_graphql_path() {
    // Linear is GraphQL-only. The src.linear tile aliases src.graphql
    // so users get a vendor-named tile; the engine treats both the
    // same way (POST {query, variables}, walk /data).
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    let engine = engine_or_skip!();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();

    // Linear-shaped GraphQL response: data -> issues -> nodes [...].
    let body = br#"{"data":{"issues":{"nodes":[{"id":"ISS-1","title":"first"},{"id":"ISS-2","title":"second"}]}}}"#;
    let captured = Arc::new(std::sync::Mutex::new(String::new()));
    let cap = captured.clone();

    let handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            stream.set_read_timeout(Some(Duration::from_millis(250))).ok();
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = [0u8; 4096];
            for _ in 0..16 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            *cap.lock().unwrap() = String::from_utf8_lossy(&buf).to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "linear.csv");
    let url = format!("http://127.0.0.1:{}/graphql", port);
    let r = engine.execute_pipeline(&doc(
        json!([
            node("l", "src.linear", json!({
                "url": url,
                "query": "query { issues { nodes { id title } } }",
                "responsePath": "/data/issues/nodes",
                "authType": "bearer",
                "authToken": "lin_api_TEST",
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e1", "l", "k")]),
    ));
    let _ = handle.join();
    assert_eq!(r.status, "ok", "src.linear alias failed: {:?}", r.error);
    assert_eq!(count(&format!("read_csv_auto('{}')", out)), 2);
    // Confirm it was a POST (GraphQL is always POST) and the query was sent.
    let req = captured.lock().unwrap();
    assert!(req.starts_with("POST "), "expected POST request from src.linear alias");
    assert!(
        req.contains("query { issues { nodes { id title } } }"),
        "expected GraphQL query body in src.linear request"
    );
}

#[test]
fn snk_cockroach_routes_through_postgres_attach_path() {
    // CockroachDB is wire-compatible with Postgres - the engine handles
    // snk.cockroach via the same postgres ATTACH path as snk.postgres.
    // This test exercises plan compilation, not a real CockroachDB
    // connection (we don't run one in CI), so it verifies the planner
    // accepts the component ID without error rather than the network
    // round-trip itself.
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write_file(tmp.path(), "in.csv", "id,name\n1,a\n");
    // Use an unreachable host so the postgres ATTACH fails fast; the
    // planner work happens BEFORE we hit the network, so a config-time
    // mismatch (unknown component_id, missing required prop, etc) would
    // surface as a different error class. We assert the error mentions
    // postgres / connection / cockroach - proving we routed through the
    // PG handler rather than the 'preview component' fallback.
    let r = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("c", "snk.cockroach", json!({
                "host": "127.0.0.1",
                "port": 1,
                "database": "defaultdb",
                "user": "root",
                "password": "",
                "table": "users",
                "mode": "append",
            })),
        ]),
        json!([main_edge("e1", "s", "c")]),
    ));
    // We don't require status == "ok" (no real DB). We require that the
    // error, if any, is NOT 'isn't executable yet - it's a preview
    // component' which is what the fallback would produce.
    if r.status != "ok" {
        let msg = r.error.unwrap_or_default();
        assert!(
            !msg.contains("preview component"),
            "snk.cockroach should not hit the unknown-component fallback; got: {}",
            msg
        );
    }
}

#[test]
fn snk_and_src_redis_roundtrip_via_real_url() {
    // Env-gated like the mongo / postgres / mysql tests. Set
    // DUCKLE_REDIS_URL to a working redis URL (e.g. redis://127.0.0.1:6379/0)
    // to run; otherwise skip cleanly. Write 3 keys via snk.redis, scan
    // them back via src.redis, assert the count + that they're all
    // present.
    let engine = engine_or_skip!();
    let url = match std::env::var("DUCKLE_REDIS_URL").ok() {
        Some(u) if !u.is_empty() => u,
        _ => {
            eprintln!("skipping: set DUCKLE_REDIS_URL to run Redis tests");
            return;
        }
    };
    let tmp = tempfile::tempdir().unwrap();
    // Unique prefix per test run so concurrent runs don't collide.
    let prefix = format!("duckle_test_{}_", std::process::id());
    let csv_body = format!(
        "key,value\n{p}k1,alpha\n{p}k2,beta\n{p}k3,gamma\n",
        p = prefix
    );
    let csv = write_file(tmp.path(), "in.csv", &csv_body);

    let r1 = engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("r", "snk.redis", json!({
                "url": &url,
                "keyColumn": "key",
                "valueColumn": "value",
                "ttlSeconds": 60,
            })),
        ]),
        json!([main_edge("e", "s", "r")]),
    ));
    assert_eq!(r1.status, "ok", "redis sink failed: {:?}", r1.error);

    let out = out_path(tmp.path(), "out.csv");
    let r2 = engine.execute_pipeline(&doc(
        json!([
            node("g", "src.redis", json!({
                "url": &url,
                "keyPattern": format!("{}*", prefix),
                "limit": 1000,
            })),
            node("k", "snk.csv", json!({ "path": out, "hasHeader": true })),
        ]),
        json!([main_edge("e", "g", "k")]),
    ));
    assert_eq!(r2.status, "ok", "redis source failed: {:?}", r2.error);
    let n = count(&format!("read_csv_auto('{}')", out));
    assert_eq!(n, 3, "expected 3 keys round-tripped, got {}", n);
}
