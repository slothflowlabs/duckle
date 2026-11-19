//! `src.mysql.cdc` against a real MySQL server, its binary log on in ROW format
//! with FULL row images, as MySQL 8 has it by default.
//!
//! Needs DUCKLE_MYSQL_HOST (with DUCKLE_MYSQL_PORT, _DB, _USER and _PASS, as the
//! MySQL integration job sets them) and DUCKLE_DUCKDB_BIN; without them the tests
//! skip. The user needs REPLICATION SLAVE and REPLICATION CLIENT, as root has.
//! Each test owns a table named for it. For a throwaway server:
//!
//!   docker run -d --name duckle-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=mytest -e MYSQL_DATABASE=ducktest mysql:8.0

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use mysql_async::prelude::Queryable;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Mutex;

/// The workspace is process-global, and these tests each set their own.
static ENV_LOCK: Mutex<()> = Mutex::new(());

struct Live {
    engine: DuckdbEngine,
    bin: String,
    host: String,
    port: u16,
    user: String,
    pass: String,
    db: String,
}

fn live() -> Option<Live> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())?;
    let Some(host) = std::env::var("DUCKLE_MYSQL_HOST").ok().filter(|h| !h.trim().is_empty()) else {
        eprintln!("skipping: set DUCKLE_MYSQL_HOST (and _PORT, _DB, _USER, _PASS) to a MySQL server to run");
        return None;
    };
    let var = |k: &str, d: &str| std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| d.to_string());
    Some(Live {
        engine: DuckdbEngine::new(bin.clone().into()),
        bin,
        host,
        port: var("DUCKLE_MYSQL_PORT", "3306").parse().expect("DUCKLE_MYSQL_PORT is a port"),
        user: var("DUCKLE_MYSQL_USER", "root"),
        pass: var("DUCKLE_MYSQL_PASS", ""),
        db: var("DUCKLE_MYSQL_DB", "ducktest"),
    })
}

/// Run statements in the test database, one after another in one session, and
/// hand back the rows of the last, as text.
fn sql(l: &Live, statements: &[&str]) -> Vec<Vec<Option<String>>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(l.host.clone())
            .tcp_port(l.port)
            .user(Some(l.user.clone()))
            .pass(Some(l.pass.clone()))
            .db_name(Some(l.db.clone()));
        let mut conn = mysql_async::Conn::new(opts).await.expect("connect to MySQL");
        let mut last = Vec::new();
        for s in statements {
            let rows: Vec<mysql_async::Row> = conn.query(*s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
            last = rows
                .into_iter()
                .map(|r| (0..r.len()).map(|i| r.get::<Option<String>, _>(i).flatten()).collect())
                .collect();
        }
        let _ = conn.disconnect().await;
        last
    })
}

fn pipeline(l: &Live, table: &str, extra: Value, out: &str, fail_after: bool) -> PipelineDoc {
    let mut props = json!({ "host": l.host, "port": l.port, "user": l.user, "password": l.pass, "database": l.db, "table": table });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    let node = |id: &str, component: &str, p: Value| {
        json!({"id": id, "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": id, "componentId": component, "properties": p}})
    };
    let edge = |id: &str, a: &str, b: &str| json!({"id": id, "source": a, "target": b, "data": {"connectionType": "main"}});
    let (nodes, edges) = if fail_after {
        (
            json!([
                node("c", "src.mysql.cdc", props),
                node("x", "code.sql", json!({ "sql": "SELECT error('the load stops here') AS e FROM input" })),
                node("k", "snk.parquet", json!({ "path": out })),
            ]),
            json!([edge("e1", "c", "x"), edge("e2", "x", "k")]),
        )
    } else {
        (json!([node("c", "src.mysql.cdc", props), node("k", "snk.parquet", json!({ "path": out }))]), json!([edge("e1", "c", "k")]))
    };
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn query(bin: &str, sql: &str) -> Vec<Value> {
    let out = std::process::Command::new(bin).args([":memory:", "-json", "-c", sql]).output().expect("duckdb runs");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn run(l: &Live, d: &PipelineDoc) -> RunResult {
    l.engine.execute_pipeline_named(d, "cdc_orders")
}

/// The operation and id of each change a run read, in the order it wrote them.
fn ops(l: &Live, out: &str) -> Vec<(String, i64)> {
    query(&l.bin, &format!("SELECT _op, id FROM read_parquet('{out}')"))
        .iter()
        .map(|r| (r["_op"].as_str().unwrap().to_string(), r["id"].as_i64().unwrap()))
        .collect()
}

fn out_of(ws: &tempfile::TempDir) -> String {
    ws.path().join("out.parquet").to_string_lossy().replace('\\', "/")
}

#[test]
fn mysql_cdc_reads_each_change_once_in_order_with_its_types() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(
        &l,
        &[
            "DROP TABLE IF EXISTS cdc_types",
            "CREATE TABLE cdc_types (id INT PRIMARY KEY, item VARCHAR(20), qty INT UNSIGNED, big BIGINT UNSIGNED, \
             price DECIMAL(10,2), placed DATETIME(3), seen TIMESTAMP(6) NULL, born DATE, wait TIME(3), yr YEAR, \
             tags SET('red','green','blue'), size ENUM('s','m','l'), note JSON, photo BLOB, open BIT(1), flags BIT(10))",
            // Before the first run, which starts at the end of the binlog: never read.
            "INSERT INTO cdc_types (id, item) VALUES (0, 'before')",
        ],
    );
    let out = out_of(&ws);
    let d = pipeline(&l, "cdc_types", json!({}), &out, false);
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(ops(&l, &out).is_empty(), "the first run starts at the end of the binlog");
    let types: Vec<String> = query(&l.bin, &format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        ["_op VARCHAR", "_file VARCHAR", "_pos BIGINT", "_ts TIMESTAMP WITH TIME ZONE", "id INTEGER", "item VARCHAR",
         "qty UINTEGER", "big UBIGINT", "price DECIMAL(10,2)", "placed TIMESTAMP", "seen TIMESTAMP WITH TIME ZONE",
         "born DATE", "wait INTERVAL", "yr SMALLINT", "tags VARCHAR", "size VARCHAR", "note JSON", "photo BLOB",
         "open BOOLEAN", "flags UBIGINT"],
        "a run with nothing to read is still typed, as MySQL declares each column"
    );

    sql(
        &l,
        &[
            "SET time_zone = '+00:00'",
            "INSERT INTO cdc_types VALUES (1, 'apple', 4294967295, 18446744073709551615, 12.34, '2026-11-19 10:00:00.123', \
             '2026-11-19 10:00:00.123456', '2026-11-19', '838:59:58.5', 2026, 'red,blue', 'm', '{\"a\": [1, 2]}', \
             0xFFD8, b'1', b'1000000001')",
            "INSERT INTO cdc_types (id, item) VALUES (2, 'pear')",
            "UPDATE cdc_types SET item = 'plum', qty = 5 WHERE id = 1",
            "DELETE FROM cdc_types WHERE id = 2",
            "START TRANSACTION",
            "INSERT INTO cdc_types (id, item) VALUES (3, 'fig')",
            "INSERT INTO cdc_types (id, item) VALUES (4, 'kiwi')",
            "COMMIT",
        ],
    );
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let read: Vec<(String, i64)> = ops(&l, &out);
    let expected: Vec<(String, i64)> =
        [("insert", 1), ("insert", 2), ("update", 1), ("delete", 2), ("insert", 3), ("insert", 4)]
            .iter()
            .map(|(o, i)| (o.to_string(), *i))
            .collect();
    assert_eq!(read, expected);
    let rows = query(
        &l.bin,
        &format!(
            "SELECT item, CAST(qty AS VARCHAR) AS qty, CAST(big AS VARCHAR) AS big, CAST(price AS VARCHAR) AS price, \
             CAST(placed AS VARCHAR) AS placed, epoch_us(seen) AS seen_us, CAST(born AS VARCHAR) AS born, \
             epoch(wait) AS wait_s, yr, tags, size, CAST(note AS VARCHAR) AS note, hex(photo) AS photo, open, flags, \
             _file LIKE '%.0%' AS has_file, _pos > 4 AS has_pos, _ts IS NOT NULL AS has_ts FROM read_parquet('{out}')"
        ),
    );
    assert_eq!(
        rows[0],
        json!({"item": "apple", "qty": "4294967295", "big": "18446744073709551615", "price": "12.34",
               "placed": "2026-11-19 10:00:00.123", "seen_us": 1795082400123456_i64, "born": "2026-11-19",
               "wait_s": 3020398.5, "yr": 2026, "tags": "red,blue", "size": "m", "note": "{\"a\":[1,2]}",
               "photo": "FFD8", "open": true, "flags": "513", "has_file": true, "has_pos": true, "has_ts": true})
    );
    // An update is one row of its new values; a delete, the row as it was.
    assert_eq!((rows[2]["item"].clone(), rows[2]["qty"].clone(), rows[2]["price"].clone()), (json!("plum"), json!("5"), json!("12.34")));
    assert_eq!(rows[3]["item"], json!("pear"));
    assert_eq!(rows[1]["qty"], Value::Null, "a NULL stays NULL");

    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(ops(&l, &out).is_empty(), "each change once");

    sql(&l, &["DROP TABLE IF EXISTS cdc_types_other", "CREATE TABLE cdc_types_other (id INT PRIMARY KEY)", "INSERT INTO cdc_types_other VALUES (1)"]);
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(ops(&l, &out).is_empty(), "another table's changes are not this table's");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mysql_cdc_reads_the_same_changes_again_after_a_failed_run() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(&l, &["DROP TABLE IF EXISTS cdc_retry", "CREATE TABLE cdc_retry (id INT PRIMARY KEY, item VARCHAR(20))"]);
    let out = out_of(&ws);
    assert_eq!(run(&l, &pipeline(&l, "cdc_retry", json!({}), &out, false)).status, "ok", "the position to start from");
    sql(&l, &["INSERT INTO cdc_retry VALUES (1, 'apple')"]);
    let failing = run(&l, &pipeline(&l, "cdc_retry", json!({}), &out, true));
    assert_eq!(failing.status, "error");
    let r = run(&l, &pipeline(&l, "cdc_retry", json!({}), &out, false));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(ops(&l, &out), [("insert".to_string(), 1)], "the failed run moved nothing on");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mysql_cdc_stops_after_a_batch_on_a_whole_transaction() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(&l, &["DROP TABLE IF EXISTS cdc_batch", "CREATE TABLE cdc_batch (id INT PRIMARY KEY)"]);
    let out = out_of(&ws);
    let d = pipeline(&l, "cdc_batch", json!({ "batchSize": 2 }), &out, false);
    assert_eq!(run(&l, &d).status, "ok");
    sql(
        &l,
        &[
            "INSERT INTO cdc_batch VALUES (1)",
            "START TRANSACTION",
            "INSERT INTO cdc_batch VALUES (2)",
            "INSERT INTO cdc_batch VALUES (3)",
            "COMMIT",
            "INSERT INTO cdc_batch VALUES (4)",
        ],
    );
    let ids = |l: &Live| ops(l, &out).into_iter().map(|(_, id)| id).collect::<Vec<_>>();
    assert_eq!(run(&l, &d).status, "ok");
    assert_eq!(ids(&l), [1, 2, 3], "a transaction is never split: the second runs past the limit to its end");
    assert_eq!(run(&l, &d).status, "ok");
    assert_eq!(ids(&l), [4], "the rest, on the next run");
    assert_eq!(run(&l, &d).status, "ok");
    assert!(ids(&l).is_empty());
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mysql_cdc_says_what_it_cannot_decode_rather_than_falling_over() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(&l, &["DROP TABLE IF EXISTS cdc_time1", "CREATE TABLE cdc_time1 (id INT PRIMARY KEY, wait TIME(1))"]);
    let out = out_of(&ws);
    let d = pipeline(&l, "cdc_time1", json!({}), &out, false);
    assert_eq!(run(&l, &d).status, "ok", "the position to start from");
    // A positive TIME(1) reads; a negative one with a fraction is what the
    // decoder misreads, and the run says so instead of writing a wrong time.
    sql(&l, &["INSERT INTO cdc_time1 VALUES (1, '12:00:00.5')"]);
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let got = query(&l.bin, &format!("SELECT CAST(wait AS VARCHAR) AS wait FROM read_parquet('{out}')"));
    assert_eq!(got, [json!({"wait": "12:00:00.5"})]);
    sql(&l, &["INSERT INTO cdc_time1 VALUES (2, '-01:00:00.5')"]);
    let r = run(&l, &d);
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("TIME(1)") && err.contains("could not be decoded"), "{err}");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

/// Puts a global setting back when the test ends, however it ends.
struct Restore<'a>(&'a Live, &'static str);

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        sql(self.0, &[self.1]);
    }
}

#[test]
fn mysql_cdc_says_what_the_server_does_not_log() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(&l, &["DROP TABLE IF EXISTS cdc_minimal", "CREATE TABLE cdc_minimal (id INT PRIMARY KEY)"]);
    let out = out_of(&ws);
    {
        sql(&l, &["SET GLOBAL binlog_row_image = 'MINIMAL'"]);
        let _restore = Restore(&l, "SET GLOBAL binlog_row_image = 'FULL'");
        let r = run(&l, &pipeline(&l, "cdc_minimal", json!({}), &out, false));
        assert_eq!(r.status, "error", "a MINIMAL row image leaves columns out of updates and deletes");
        let err = format!("{:?}", r.error);
        assert!(err.contains("binlog_row_image") && err.contains("FULL"), "{err}");
    }
    let r = run(&l, &pipeline(&l, "no_such_table", json!({}), &out, false));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("no_such_table"), "{err}");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mysql_cdc_says_when_the_binlog_it_stopped_in_was_purged() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(&l, &["DROP TABLE IF EXISTS cdc_purged", "CREATE TABLE cdc_purged (id INT PRIMARY KEY)"]);
    let out = out_of(&ws);
    let d = pipeline(&l, "cdc_purged", json!({}), &out, false);
    assert_eq!(run(&l, &d).status, "ok", "the position to start from");
    let logs = sql(&l, &["FLUSH BINARY LOGS", "INSERT INTO cdc_purged VALUES (1)", "FLUSH BINARY LOGS", "SHOW BINARY LOGS"]);
    let newest = logs.last().and_then(|r| r[0].clone()).expect("a binary log");
    sql(&l, &[&format!("PURGE BINARY LOGS TO '{newest}'")]);
    let r = run(&l, &d);
    assert_eq!(r.status, "error", "the change in a purged file was never read");
    let err = format!("{:?}", r.error);
    assert!(err.contains("purged") && err.contains("saved position"), "{err}");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mysql_cdc_connects_in_each_ssl_mode_and_verifies_when_asked() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    sql(&l, &["DROP TABLE IF EXISTS cdc_tls", "CREATE TABLE cdc_tls (id INT PRIMARY KEY)"]);
    let out = out_of(&ws);
    for mode in ["preferred", "required", "disabled"] {
        let r = run(&l, &pipeline(&l, "cdc_tls", json!({ "sslMode": mode }), &out, false));
        assert_eq!(r.status, "ok", "{mode}: {:?}", r.error);
    }
    // MySQL's own certificate is self-signed, so a check of it fails.
    let r = run(&l, &pipeline(&l, "cdc_tls", json!({ "sslMode": "verify_identity" }), &out, false));
    assert_eq!(r.status, "error", "verify_identity checks the certificate");
    std::env::remove_var("DUCKLE_WORKSPACE");
}
