//! `snk.delta` against real Delta tables, read back through DuckDB's own
//! `delta_scan` - the delta-kernel reader - rather than through the sink.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use serde_json::{json, Value};
use std::path::Path;

fn engine() -> Option<(DuckdbEngine, String)> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())?;
    Some((DuckdbEngine::new(bin.clone().into()), bin))
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

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({ "id": id, "position": { "x": 0, "y": 0 },
            "data": { "label": id, "componentId": component, "properties": props } })
}

fn pipeline(csv: &str, table: &str, extra: Value) -> PipelineDoc {
    let mut props = json!({ "path": table });
    if let (Some(p), Some(e)) = (props.as_object_mut(), extra.as_object()) {
        p.extend(e.clone());
    }
    serde_json::from_value(json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("k", "snk.delta", props),
        ],
        "edges": [{ "id": "e", "source": "s", "target": "k" }]
    }))
    .expect("doc")
}

fn path(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().replace('\\', "/")
}

fn write(dir: &Path, name: &str, body: &str) -> String {
    std::fs::write(dir.join(name), body).unwrap();
    path(dir, name)
}

/// Query a Delta table through delta-kernel, independent of the sink.
fn read(bin: &str, sql: &str) -> Vec<Value> {
    let out = std::process::Command::new(bin)
        .args([":memory:", "-json", "-c", &format!("LOAD delta; {sql}")])
        .output()
        .expect("duckdb runs");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

/// A missing table is created from the input's schema, and later runs append.
/// DuckDB 1.5's delta extension can append but cannot create a table, so the
/// sink writes the first commit itself; that it is a real Delta table is proved by
/// the kernel reading it back, typed.
#[test]
fn a_delta_sink_creates_the_table_then_appends_to_it() {
    let (engine, bin) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(
        tmp.path(),
        "in.csv",
        "id,name,amount,seen\n1,a,9.50,2026-01-02 03:04:05\n2,b,1.25,2026-02-03 04:05:06\n",
    );
    let table = path(tmp.path(), "orders");
    for run in 1..=2 {
        let r = engine.execute_pipeline(&pipeline(&csv, &table, json!({})));
        assert_eq!(r.status, "ok", "run {run}: {:?}", r.error);
        assert_eq!(r.nodes.get("k").and_then(|n| n.rows), Some(2), "run {run} reports what it wrote");
    }
    let got = read(&bin, &format!("SELECT count(*) AS n FROM delta_scan('{table}')"));
    assert_eq!(got[0]["n"], 4, "created on the first run, appended on the second");
    let types = read(&bin, &format!("DESCRIBE SELECT * FROM delta_scan('{table}')"));
    let t: Vec<(&str, &str)> = types
        .iter()
        .map(|c| (c["column_name"].as_str().unwrap_or(""), c["column_type"].as_str().unwrap_or("")))
        .collect();
    assert_eq!(
        t,
        vec![("id", "BIGINT"), ("name", "VARCHAR"), ("amount", "DOUBLE"), ("seen", "TIMESTAMP")],
        "each column keeps its type, a naive timestamp included"
    );
    assert!(
        Path::new(&table).join("_delta_log").join("00000000000000000002.json").exists(),
        "one commit to create and one per append"
    );
}

/// A column the table does not have is refused by name, never dropped.
#[test]
fn a_column_the_table_lacks_is_refused_by_name() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let table = path(tmp.path(), "t");
    let first = write(tmp.path(), "a.csv", "id\n1\n");
    assert_eq!(engine.execute_pipeline(&pipeline(&first, &table, json!({}))).status, "ok");
    let wider = write(tmp.path(), "b.csv", "id,surprise\n2,x\n");
    let r = engine.execute_pipeline(&pipeline(&wider, &table, json!({})));
    assert_ne!(r.status, "ok", "a column with nowhere to go must not vanish");
    let err = r.error.unwrap_or_default();
    assert!(err.contains("the table has no column surprise"), "{err}");
}

/// A column the input does not supply is refused by name, never NULL-filled.
#[test]
fn a_column_the_input_lacks_is_refused_by_name() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let table = path(tmp.path(), "t");
    let first = write(tmp.path(), "a.csv", "id,note\n1,x\n");
    assert_eq!(engine.execute_pipeline(&pipeline(&first, &table, json!({}))).status, "ok");
    let narrower = write(tmp.path(), "b.csv", "id\n2\n");
    let r = engine.execute_pipeline(&pipeline(&narrower, &table, json!({})));
    assert_ne!(r.status, "ok");
    // The sink's own refusal, not DuckDB's binder happening to name the column.
    let err = r.error.unwrap_or_default();
    assert!(err.contains("the input has no column note"), "{err}");
}

/// Columns in a different order land in the right place. The extension's own
/// `INSERT ... BY NAME` fails an internal assertion, so the sink orders them.
#[test]
fn columns_are_matched_by_name_not_position() {
    let (engine, bin) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let table = path(tmp.path(), "t");
    let first = write(tmp.path(), "a.csv", "id,note\n1,x\n");
    assert_eq!(engine.execute_pipeline(&pipeline(&first, &table, json!({}))).status, "ok");
    let reordered = write(tmp.path(), "b.csv", "note,id\ny,2\n");
    let r = engine.execute_pipeline(&pipeline(&reordered, &table, json!({})));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let got = read(&bin, &format!("SELECT note FROM delta_scan('{table}') WHERE id = 2"));
    assert_eq!(got[0]["note"], "y");
}

/// Without createIfMissing, a missing table is an error rather than a new one
/// somewhere a typo put it.
#[test]
fn a_missing_table_is_refused_when_creating_is_off() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "a.csv", "id\n1\n");
    let table = path(tmp.path(), "not_there");
    let r = engine.execute_pipeline(&pipeline(&csv, &table, json!({ "createIfMissing": false })));
    assert_ne!(r.status, "ok");
    assert!(!Path::new(&table).exists(), "nothing was created");
}

/// A type Delta has no column type for is refused at create, naming the column,
/// rather than written as something else.
#[test]
fn a_type_delta_cannot_hold_is_refused_at_create() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "a.csv", "id\n1\n");
    let table = path(tmp.path(), "t");
    let doc: PipelineDoc = serde_json::from_value(json!({
        "nodes": [
            node("s", "src.csv", json!({ "path": csv, "hasHeader": true })),
            node("x", "code.sql", json!({ "sql": "SELECT id, INTERVAL 1 DAY AS wait FROM input" })),
            node("k", "snk.delta", json!({ "path": table })),
        ],
        "edges": [
            { "id": "e1", "source": "s", "target": "x" },
            { "id": "e2", "source": "x", "target": "k" }
        ]
    }))
    .unwrap();
    let r = engine.execute_pipeline(&doc);
    assert_ne!(r.status, "ok");
    let err = r.error.unwrap_or_default();
    assert!(err.contains("wait") && err.contains("INTERVAL"), "{err}");
    assert!(!Path::new(&table).join("_delta_log").exists(), "no half-made table is left");
}

/// Object storage is written only through the stores the sidecar is built for and
/// tested against (OneLake/ADLS, S3, GCS); any other scheme is refused by name.
#[test]
fn a_remote_path_outside_the_supported_stores_is_refused() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "a.csv", "id\n1\n");
    let r = engine.execute_pipeline(&pipeline(&csv, "https://example.com/orders", json!({})));
    assert_ne!(r.status, "ok");
    let err = r.error.unwrap_or_default();
    assert!(err.contains("https://example.com/orders") && err.contains("abfss://"), "{err}");
}

/// The duckle-delta sidecar, which makes the writes the delta extension cannot.
fn sidecar() -> bool {
    std::env::var("DUCKLE_DELTA_BIN").ok().filter(|b| Path::new(b).exists()).is_some()
}

macro_rules! sidecar_or_skip {
    () => {
        if !sidecar() {
            eprintln!("skipping: set DUCKLE_DELTA_BIN to a duckle-delta to run");
            return;
        }
    };
}

fn rows_of(bin: &str, table: &str) -> Vec<Value> {
    read(bin, &format!("SELECT * FROM delta_scan('{table}') ORDER BY id"))
}

/// Upsert, update and delete by key, on a table the extension created, each read
/// back through delta-kernel rather than through the sidecar that wrote it.
#[test]
fn upsert_update_and_delete_by_key() {
    let (engine, bin) = engine_or_skip!();
    sidecar_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let table = path(tmp.path(), "t");
    let seed = write(tmp.path(), "a.csv", "id,name\n1,a\n2,b\n3,c\n");
    let r = engine.execute_pipeline(&pipeline(&seed, &table, json!({})));
    assert_eq!(r.status, "ok", "{:?}", r.error);

    let keyed = |mode: &str| json!({ "mode": mode, "conflictColumns": ["id"] });
    let up = write(tmp.path(), "b.csv", "id,name\n2,B\n4,d\n");
    let r = engine.execute_pipeline(&pipeline(&up, &table, keyed("upsert")));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        rows_of(&bin, &table),
        json!([{"id":1,"name":"a"},{"id":2,"name":"B"},{"id":3,"name":"c"},{"id":4,"name":"d"}]).as_array().unwrap().clone()
    );

    let upd = write(tmp.path(), "c.csv", "id,name\n3,C\n9,z\n");
    let r = engine.execute_pipeline(&pipeline(&upd, &table, keyed("update")));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        rows_of(&bin, &table),
        json!([{"id":1,"name":"a"},{"id":2,"name":"B"},{"id":3,"name":"C"},{"id":4,"name":"d"}]).as_array().unwrap().clone(),
        "update changes 3 and does not insert 9"
    );

    let del = write(tmp.path(), "d.csv", "id\n1\n4\n");
    let r = engine.execute_pipeline(&pipeline(&del, &table, keyed("delete")));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        rows_of(&bin, &table),
        json!([{"id":2,"name":"B"},{"id":3,"name":"C"}]).as_array().unwrap().clone()
    );
}

/// With upsert, a row flagged in deleteColumn deletes its match, and the flag
/// column never reaches the table.
#[test]
fn a_delete_column_deletes_flagged_rows_and_is_not_written() {
    let (engine, bin) = engine_or_skip!();
    sidecar_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let table = path(tmp.path(), "t");
    let seed = write(tmp.path(), "a.csv", "id,name\n1,a\n2,b\n");
    assert_eq!(engine.execute_pipeline(&pipeline(&seed, &table, json!({}))).status, "ok");
    let cdc = write(tmp.path(), "b.csv", "id,name,op\n1,a,delete\n2,B,\n3,c,upsert\n");
    let props = json!({ "mode": "upsert", "conflictColumns": ["id"], "deleteColumn": "op" });
    let r = engine.execute_pipeline(&pipeline(&cdc, &table, props));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        rows_of(&bin, &table),
        json!([{"id":2,"name":"B"},{"id":3,"name":"c"}]).as_array().unwrap().clone()
    );
    let cols: Vec<Value> = read(&bin, &format!("DESCRIBE SELECT * FROM delta_scan('{table}')"))
        .iter()
        .map(|c| c["column_name"].clone())
        .collect();
    assert_eq!(cols, vec![json!("id"), json!("name")]);
}

/// The keyed modes refuse a configuration they cannot carry out, before running.
#[test]
fn a_keyed_mode_says_what_it_is_missing() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "a.csv", "id,op\n1,x\n");
    let table = path(tmp.path(), "t");
    for (props, needle) in [
        (json!({ "mode": "upsert" }), "conflictColumns"),
        (json!({ "mode": "merge", "conflictColumns": ["id"] }), "append, upsert, update or delete"),
        (json!({ "mode": "update", "conflictColumns": ["id"], "deleteColumn": "op" }), "deleteColumn applies to upsert"),
    ] {
        let r = engine.execute_pipeline(&pipeline(&csv, &table, props.clone()));
        assert_ne!(r.status, "ok", "{props}");
        let err = r.error.unwrap_or_default();
        assert!(err.contains(needle), "{props}: {err}");
    }
    assert!(!Path::new(&table).exists(), "nothing was written");
}

/// src.delta reads a table as it was: at a version (`asOfVersion`), or at a
/// point in time (`asOfTimestamp`, DuckDB 2.0). The sink's first commit
/// creates the table and each run appends one more, so versions 1 and 2 hold
/// one row and two.
#[test]
fn a_delta_source_reads_the_table_as_of_a_version_or_a_time() {
    let (engine, bin) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "in.csv", "id\n1\n");
    let table = path(tmp.path(), "t");
    let now_utc = || {
        read(&bin, "SELECT strftime(now() AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') || '+00' AS t")[0]["t"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(engine.execute_pipeline(&pipeline(&csv, &table, json!({}))).status, "ok");
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let between = now_utc();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert_eq!(engine.execute_pipeline(&pipeline(&csv, &table, json!({}))).status, "ok");

    let rows_as_of = |travel: Value| -> i64 {
        let out = path(tmp.path(), "out.csv");
        let mut props = json!({ "path": table });
        props.as_object_mut().unwrap().extend(travel.as_object().unwrap().clone());
        let doc: PipelineDoc = serde_json::from_value(json!({
            "nodes": [
                node("r", "src.delta", props),
                node("w", "snk.csv", json!({ "path": out, "hasHeader": true })),
            ],
            "edges": [{ "id": "e", "source": "r", "target": "w" }]
        }))
        .unwrap();
        let r = engine.execute_pipeline(&doc);
        assert_eq!(r.status, "ok", "{travel}: {:?}", r.error);
        read(&bin, &format!("SELECT count(*) AS n FROM read_csv('{out}')"))[0]["n"].as_i64().unwrap()
    };
    assert_eq!(rows_as_of(json!({})), 2, "the latest version");
    assert_eq!(rows_as_of(json!({ "asOfVersion": 1 })), 1, "as of version 1");
    assert_eq!(rows_as_of(json!({ "asOfTimestamp": between })), 1, "as of {between}");
}

/// `partitionBy` lays out a table the sink creates by those columns, the way
/// Delta records it: a `region=<value>` directory per value, and each file's
/// add action carrying its value, which is what the kernel reads back.
/// DuckDB 2.0's delta extension writes partitioned; 1.5's stops on an
/// internal assertion.
#[test]
fn a_delta_sink_partitions_a_table_it_creates() {
    let (engine, bin) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "in.csv", "id,region\n1,eu\n2,us\n3,eu\n");
    let table = path(tmp.path(), "t");
    let r = engine.execute_pipeline(&pipeline(&csv, &table, json!({ "partitionBy": ["region"] })));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    for dir in ["region=eu", "region=us"] {
        assert!(Path::new(&table).join(dir).is_dir(), "no {dir} directory");
    }
    let got = read(
        &bin,
        &format!("SELECT region, count(*) AS n FROM delta_scan('{table}') GROUP BY ALL ORDER BY ALL"),
    );
    assert_eq!(got, vec![json!({ "region": "eu", "n": 2 }), json!({ "region": "us", "n": 1 })]);
}

/// A partition column must be one of the input's, and Delta cannot hold a
/// table whose every column is a partition column. Both are refused before
/// anything is written.
#[test]
fn a_partitioning_delta_cannot_hold_is_refused_at_create() {
    let (engine, _) = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let csv = write(tmp.path(), "in.csv", "id,region\n1,eu\n");
    for (cols, says) in [(json!(["nope"]), "nope"), (json!(["id", "region"]), "every column")] {
        let table = path(tmp.path(), "t");
        let r = engine.execute_pipeline(&pipeline(&csv, &table, json!({ "partitionBy": cols })));
        assert_ne!(r.status, "ok", "{cols} was accepted");
        let err = r.error.unwrap_or_default();
        assert!(err.contains(says), "{cols}: {err}");
        assert!(!Path::new(&table).join("_delta_log").exists(), "{cols}: a table was started");
    }
}
