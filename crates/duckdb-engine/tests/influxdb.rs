//! src.influxdb and snk.influxdb against a real InfluxDB 3 server, when
//! DUCKLE_INFLUXDB_URL names one (DUCKLE_INFLUXDB_TOKEN its admin token). The
//! tests share one database, each writing measurements of its own. For a
//! throwaway server:
//!
//!   docker run -d --name duckle-influx -p 8181:8181 influxdb:3-core influxdb3 serve --node-id node0 --object-store memory
//!   docker exec duckle-influx influxdb3 create token --admin

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::path::Path;

struct Live {
    engine: DuckdbEngine,
    url: String,
    token: String,
}

fn live() -> Option<Live> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())?;
    let Some(url) = std::env::var("DUCKLE_INFLUXDB_URL").ok().filter(|u| !u.trim().is_empty()) else {
        eprintln!("skipping: set DUCKLE_INFLUXDB_URL (and DUCKLE_INFLUXDB_TOKEN) to an InfluxDB 3 server to run");
        return None;
    };
    let token = std::env::var("DUCKLE_INFLUXDB_TOKEN").unwrap_or_default();
    Some(Live { engine: DuckdbEngine::new(bin.into()), url, token })
}

/// The database every test writes to. InfluxDB 3 Core keeps at most five, and a
/// database per test used them up within one run, so later runs failed to write
/// at all.
const DATABASE: &str = "duckle_test";

/// A name no other test, and no earlier run, has used.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{prefix}_{}", nanos % 1_000_000_000_000)
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

fn with(mut props: Value, extra: Value) -> Value {
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    props
}

/// What `sql` selects (over a one-row `input`), written by snk.influxdb.
fn write(l: &Live, dir: &Path, database: &str, sql: &str, extra: Value) -> RunResult {
    let kick = dir.join("kick.csv");
    std::fs::write(&kick, "k\n1\n").unwrap();
    let props = with(json!({ "url": l.url, "token": l.token, "database": database }), extra);
    l.engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": norm(&kick), "hasHeader": true })),
            node("q", "code.sql", json!({ "sql": sql })),
            node("w", "snk.influxdb", props),
        ]),
        json!([edge("e1", "s", "q"), edge("e2", "q", "w")]),
    ))
}

/// src.influxdb asking `query`, into a Parquet file at `out`.
fn read(l: &Live, database: &str, query: &str, extra: Value, out: &str) -> RunResult {
    let props = with(json!({ "url": l.url, "token": l.token, "database": database, "query": query }), extra);
    l.engine.execute_pipeline(&doc(
        json!([node("r", "src.influxdb", props), node("k", "snk.parquet", json!({ "path": out }))]),
        json!([edge("e1", "r", "k")]),
    ))
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn types(out: &str) -> Vec<String> {
    duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect()
}

const CPU: &str = "SELECT v.* FROM input, (VALUES \
    ('a b', 'eu,west', 3::BIGINT, 1.5::DOUBLE, true, 'say \"hi\"', TIMESTAMP '2026-11-19 10:00:00'), \
    ('c', NULL, 4::BIGINT, 2.0::DOUBLE, false, 'plain', TIMESTAMP '2026-11-19 10:00:01')) v(host, region, count, usage, ok, note, t)";

#[test]
fn influxdb_writes_lines_and_reads_them_back_typed_by_sql() {
    let Some(l) = live() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let cpu = unique("cpu");
    let r = write(&l, tmp.path(), DATABASE, CPU, json!({ "measurement": cpu, "tags": "host, region", "timeColumn": "t" }));
    assert_eq!(r.status, "ok", "snk.influxdb failed: {:?}", r.error);
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&l, DATABASE, &format!("SELECT host, region, count, usage, ok, note, time FROM {cpu} ORDER BY time"), json!({}), &out);
    assert_eq!(r.status, "ok", "src.influxdb failed: {:?}", r.error);
    assert_eq!(
        types(&out),
        ["host VARCHAR", "region VARCHAR", "count BIGINT", "usage DOUBLE", "ok BOOLEAN", "note VARCHAR", "time TIMESTAMP_NS"],
        "as InfluxDB keeps each, through Parquet"
    );
    assert_eq!(
        duckdb_json(&format!("SELECT host, region, count, usage, ok, note, CAST(time AS VARCHAR) AS time FROM read_parquet('{out}')")),
        [
            json!({"host": "a b", "region": "eu,west", "count": 3, "usage": 1.5, "ok": true, "note": "say \"hi\"", "time": "2026-11-19 10:00:00"}),
            json!({"host": "c", "region": null, "count": 4, "usage": 2.0, "ok": false, "note": "plain", "time": "2026-11-19 10:00:01"}),
        ],
        "spaces, commas and quotes came through escaped and back"
    );
}

#[test]
fn influxdb_reads_with_influxql_as_any_influxdb_answers_it() {
    let Some(l) = live() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let cpu = unique("cpu");
    assert_eq!(write(&l, tmp.path(), DATABASE, CPU, json!({ "measurement": cpu, "tags": "host, region", "timeColumn": "t" })).status, "ok");
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&l, DATABASE, &format!("SELECT usage, host FROM {cpu}"), json!({ "language": "influxql" }), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(types(&out), ["measurement VARCHAR", "time TIMESTAMP_NS", "usage DOUBLE", "host VARCHAR"]);
    assert_eq!(
        duckdb_json(&format!("SELECT measurement, CAST(time AS VARCHAR) AS time, usage, host FROM read_parquet('{out}') ORDER BY time")),
        [
            json!({"measurement": cpu, "time": "2026-11-19 10:00:00", "usage": 1.5, "host": "a b"}),
            json!({"measurement": cpu, "time": "2026-11-19 10:00:01", "usage": 2.0, "host": "c"}),
        ]
    );
}

#[test]
fn influxdb_takes_the_measurement_from_a_column_and_the_time_in_milliseconds() {
    let Some(l) = live() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (mem, disk) = (unique("mem"), unique("disk"));
    let sql = format!(
        "SELECT v.* FROM input, (VALUES ('{mem}', 'h1', 10::BIGINT, 1795082400123::BIGINT), \
         ('{disk}', 'h1', 20::BIGINT, 1795082400456::BIGINT)) v(m, host, used, ms)"
    );
    let r = write(&l, tmp.path(), DATABASE, &sql, json!({ "measurementColumn": "m", "tags": "host", "timeColumn": "ms", "precision": "ms" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let out = norm(&tmp.path().join("mem.parquet"));
    assert_eq!(read(&l, DATABASE, &format!("SELECT used, time FROM {mem}"), json!({}), &out).status, "ok");
    assert_eq!(
        duckdb_json(&format!("SELECT used, CAST(time AS VARCHAR) AS time FROM read_parquet('{out}')")),
        [json!({"used": 10, "time": "2026-11-19 10:00:00.123"})]
    );
    let out = norm(&tmp.path().join("disk.parquet"));
    assert_eq!(read(&l, DATABASE, &format!("SELECT used FROM {disk}"), json!({}), &out).status, "ok");
    assert_eq!(duckdb_json(&format!("SELECT used FROM read_parquet('{out}')")), [json!({"used": 20})]);
}

#[test]
fn influxdb_says_what_the_server_refused() {
    let Some(l) = live() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let cpu = unique("cpu");
    let r = write(&l, tmp.path(), DATABASE, CPU, json!({ "measurement": cpu, "token": "not-the-token" }));
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("not authenticated"), "{:?}", r.error);
    assert_eq!(write(&l, tmp.path(), DATABASE, CPU, json!({ "measurement": cpu, "timeColumn": "t" })).status, "ok");
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&l, DATABASE, &format!("SELECT nope FROM {cpu}"), json!({}), &out);
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("No field named nope"), "{:?}", r.error);
    // A read makes no database, so asking one nobody wrote leaves the count alone.
    let r = read(&l, &unique("absent"), "SELECT * FROM cpu", json!({}), &out);
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("database not found"), "{:?}", r.error);
}

#[test]
fn influxdb_sends_nothing_when_a_row_cannot_be_a_line() {
    let Some(l) = live() else { return };
    let tmp = tempfile::tempdir().unwrap();
    // A database of its own, which InfluxDB makes only on the first line written.
    let database = unique("unwritten");
    let sql = "SELECT v.* FROM input, (VALUES ('a', 1.5::DOUBLE), ('b', NULL::DOUBLE)) v(host, usage)";
    let r = write(&l, tmp.path(), &database, sql, json!({ "measurement": "cpu", "tags": "host" }));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("row 2") && err.contains("every field is empty"), "{err}");
    let r = write(&l, tmp.path(), &database, "SELECT 'a' || chr(10) || 'b' AS note FROM input", json!({ "measurement": "cpu" }));
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("newline"), "{:?}", r.error);
    // Neither wrote a line, so the database was never made.
    let r = read(&l, &database, "SELECT * FROM cpu", json!({}), &norm(&tmp.path().join("out.parquet")));
    assert!(format!("{:?}", r.error).contains("database not found"), "{:?}", r.error);
}

#[test]
fn influxdb_types_an_empty_result_from_the_declared_schema() {
    let Some(l) = live() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let cpu = unique("cpu");
    assert_eq!(write(&l, tmp.path(), DATABASE, CPU, json!({ "measurement": cpu, "timeColumn": "t" })).status, "ok");
    let out = norm(&tmp.path().join("out.parquet"));
    let query = format!("SELECT host, usage FROM {cpu} WHERE usage > 100");
    let r = read(&l, DATABASE, &query, json!({}), &out);
    assert_eq!(r.status, "error", "no rows and no declared schema: nothing says what the columns are");
    assert!(format!("{:?}", r.error).contains("no schema is declared"), "{:?}", r.error);
    let mut source = node("r", "src.influxdb", json!({ "url": l.url, "token": l.token, "database": DATABASE, "query": query }));
    source["data"]["schema"] = json!([{ "name": "host", "type": "string" }, { "name": "usage", "type": "float64" }]);
    let r = l.engine.execute_pipeline(&doc(json!([source, node("k", "snk.parquet", json!({ "path": out }))]), json!([edge("e1", "r", "k")])));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(types(&out), ["host VARCHAR", "usage DOUBLE"]);
}
