//! src.trino against a Trino stand-in that runs inside the test: it answers
//! /v1/statement and the nextUri pages the way a coordinator does, and records
//! every request.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    head: String,
    body: String,
}

/// The stand-in. `answer(method, path, base url)` gives a status and a body.
struct Coordinator {
    url: String,
    seen: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Coordinator {
    fn start(answer: impl Fn(&str, &str, &str) -> (u16, String) + Send + 'static) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (keep, halt, base) = (seen.clone(), stop.clone(), url.clone());
        let handle = std::thread::spawn(move || {
            while !halt.load(Ordering::Relaxed) {
                let stream = match listener.accept() {
                    Ok((s, _)) => s,
                    Err(_) => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        continue;
                    }
                };
                stream.set_nonblocking(false).ok();
                stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).ok();
                let mut reader = std::io::BufReader::new(stream);
                let mut first = String::new();
                reader.read_line(&mut first).ok();
                let mut parts = first.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                let (mut head, mut len) = (String::new(), 0usize);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                    head.push_str(&line);
                }
                let mut body = vec![0u8; len];
                let _ = reader.read_exact(&mut body);
                let (status, reply) = answer(&method, &path, &base);
                keep.lock().unwrap().push(Request { method, path, head, body: String::from_utf8_lossy(&body).into_owned() });
                let mut stream = reader.into_inner();
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                        reply.len()
                    )
                    .as_bytes(),
                );
                let _ = stream.flush();
            }
        });
        Coordinator { url, seen, stop, handle: Some(handle) }
    }

    fn seen(&self) -> Vec<Request> {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// A query answered in pages: the first POST answer has no columns yet (the
/// query is queued), as a coordinator's does.
fn paged(pages: Vec<Value>) -> impl Fn(&str, &str, &str) -> (u16, String) + Send + 'static {
    move |method, path, base| {
        let i = if method == "POST" && path == "/v1/statement" {
            0
        } else {
            match path.rsplit('/').next().and_then(|n| n.parse::<usize>().ok()) {
                Some(n) => n,
                None => return (404, "{}".into()),
            }
        };
        let mut page = pages.get(i).cloned().unwrap_or(json!({}));
        page["id"] = json!("q1");
        if i + 1 < pages.len() {
            page["nextUri"] = json!(format!("{base}/v1/statement/executing/q1/{}", i + 1));
        }
        (200, page.to_string())
    }
}

fn columns() -> Value {
    json!([
        {"name": "id", "type": "bigint"}, {"name": "n", "type": "integer"}, {"name": "price", "type": "decimal(10,2)"},
        {"name": "score", "type": "double"}, {"name": "ratio", "type": "real"}, {"name": "name", "type": "varchar(20)"},
        {"name": "flag", "type": "boolean"}, {"name": "day", "type": "date"}, {"name": "ts", "type": "timestamp(3)"},
        {"name": "ts_zone", "type": "timestamp(3) with time zone"}, {"name": "bin", "type": "varbinary"},
        {"name": "doc", "type": "json"}, {"name": "uid", "type": "uuid"}, {"name": "tags", "type": "array(varchar)"},
        {"name": "id", "type": "bigint"},
    ])
}

fn source(server: &str, extra: Value) -> Value {
    let mut props = json!({ "server": server, "user": "ada", "catalog": "hive", "schema": "sales", "query": "SELECT * FROM orders" });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    props
}

fn read(engine: &DuckdbEngine, props: Value, out: &str) -> RunResult {
    let d: PipelineDoc = serde_json::from_value(json!({
        "nodes": [
            {"id": "t", "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": "t", "componentId": "src.trino", "properties": props}},
            {"id": "k", "type": "sink", "position": {"x": 0, "y": 0}, "data": {"label": "k", "componentId": "snk.parquet", "properties": {"path": out}}}
        ],
        "edges": [{"id": "e", "source": "t", "target": "k", "sourceHandle": "main", "targetHandle": "main", "data": {"connectionType": "main"}}]
    }))
    .unwrap();
    engine.execute_pipeline(&d)
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn out_path(dir: &std::path::Path) -> String {
    dir.join("out.parquet").to_string_lossy().replace('\\', "/")
}

#[test]
fn trino_reads_every_page_and_types_each_column() {
    let engine = engine_or_skip!();
    let row = |id: i64, zone: &str| {
        json!([id, 7, "12.50", "NaN", 0.5, "alpha", true, "2024-01-15", "2024-01-15 10:30:00.123",
               format!("2024-01-15 10:30:00.123 {zone}"), "AAE=", "{\"a\":1}", "b9a4c8de-0000-4000-8000-000000000001",
               ["x", "y"], id * 10])
    };
    let trino = Coordinator::start(paged(vec![
        json!({ "stats": { "state": "QUEUED" } }),
        json!({ "columns": columns(), "data": [row(1, "UTC"), row(2, "+01:00")] }),
        json!({ "columns": columns(), "data": [row(3, "America/New_York")], "stats": { "state": "FINISHED" } }),
    ]));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, source(&trino.url, json!({})), &out);
    assert_eq!(r.status, "ok", "src.trino failed: {:?}", r.error);

    let types: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        [
            "id BIGINT", "n INTEGER", "price DECIMAL(10,2)", "score DOUBLE", "ratio FLOAT", "name VARCHAR",
            "flag BOOLEAN", "day DATE", "ts TIMESTAMP", "ts_zone TIMESTAMP WITH TIME ZONE", "bin BLOB", "doc JSON",
            "uid UUID", "tags JSON", "id_2 BIGINT",
        ]
    );
    let rows = duckdb_json(&format!(
        "SELECT id, price::VARCHAR AS price, isnan(score) AS nan, name, day::VARCHAR AS day, ts::VARCHAR AS ts, \
         epoch(ts_zone)::DOUBLE AS zone, hex(bin) AS bin, doc::VARCHAR AS doc, uid::VARCHAR AS uid, tags::VARCHAR AS tags, id_2 \
         FROM read_parquet('{out}') ORDER BY id"
    ));
    assert_eq!(rows.len(), 3, "every page: {rows:?}");
    assert_eq!(
        rows[0],
        json!({"id": 1, "price": "12.50", "nan": true, "name": "alpha", "day": "2024-01-15",
               "ts": "2024-01-15 10:30:00.123", "zone": 1705314600.123, "bin": "0001", "doc": "{\"a\":1}",
               "uid": "b9a4c8de-0000-4000-8000-000000000001", "tags": "[\"x\",\"y\"]", "id_2": 10})
    );
    // An offset zone and a named one are read as the instants they are.
    assert_eq!(rows[1]["zone"], json!(1705311000.123));
    assert_eq!(rows[2]["zone"], json!(1705332600.123));

    let seen = trino.seen();
    assert_eq!(seen.iter().map(|r| r.method.as_str()).collect::<Vec<_>>(), ["POST", "GET", "GET"]);
    assert_eq!(seen[0].path, "/v1/statement");
    assert_eq!(seen[0].body, "SELECT * FROM orders");
    let head = seen[0].head.to_ascii_lowercase();
    for h in ["x-trino-user: ada", "x-trino-catalog: hive", "x-trino-schema: sales"] {
        assert!(head.contains(h), "{h}: {head}");
    }
    assert!(seen[2].path.ends_with("/q1/2"), "follows nextUri: {}", seen[2].path);
}

#[test]
fn trino_reads_an_empty_result_as_its_columns() {
    let engine = engine_or_skip!();
    let trino = Coordinator::start(paged(vec![json!({ "columns": [{"name": "id", "type": "bigint"}, {"name": "at", "type": "date"}], "data": [] })]));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, source(&trino.url, json!({})), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let types: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(types, ["id BIGINT", "at DATE"]);
}

#[test]
fn trino_says_why_the_query_failed() {
    let engine = engine_or_skip!();
    let trino = Coordinator::start(paged(vec![
        json!({}),
        json!({ "error": { "message": "line 1:8: Column 'x' cannot be resolved", "errorName": "COLUMN_NOT_FOUND", "errorType": "USER_ERROR" } }),
    ]));
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, source(&trino.url, json!({})), &out_path(tmp.path()));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("COLUMN_NOT_FOUND") && err.contains("cannot be resolved"), "{err}");
}

#[test]
fn trino_waits_out_a_busy_coordinator() {
    let engine = engine_or_skip!();
    let busy = Mutex::new(2);
    let pages = paged(vec![json!({}), json!({ "columns": [{"name": "id", "type": "bigint"}], "data": [[1]] })]);
    let trino = Coordinator::start(move |method, path, base| {
        let mut left = busy.lock().unwrap();
        if method == "GET" && *left > 0 {
            *left -= 1;
            return (503, String::new());
        }
        pages(method, path, base)
    });
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, source(&trino.url, json!({})), &out_path(tmp.path()));
    assert_eq!(r.status, "ok", "a 503 is asked again: {:?}", r.error);
    assert_eq!(trino.seen().len(), 4, "the POST, two busy answers, then the page");
}

#[test]
fn presto_is_asked_with_its_own_headers_and_each_sign_in_is_sent() {
    let engine = engine_or_skip!();
    let trino = Coordinator::start(paged(vec![json!({ "columns": [{"name": "id", "type": "bigint"}], "data": [[1]] })]));
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, source(&trino.url, json!({ "flavor": "presto", "password": "pw" })), &out_path(tmp.path()));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let head = trino.seen()[0].head.to_ascii_lowercase();
    assert!(head.contains("x-presto-user: ada") && !head.contains("x-trino-"), "{head}");
    // ada:pw
    assert!(head.contains("authorization: basic ywrhonb3"), "{head}");

    let trino = Coordinator::start(paged(vec![json!({ "columns": [{"name": "id", "type": "bigint"}], "data": [[1]] })]));
    let r = read(&engine, source(&trino.url, json!({ "accessToken": "jwt-1" })), &out_path(tmp.path()));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(trino.seen()[0].head.to_ascii_lowercase().contains("authorization: bearer jwt-1"));
}
