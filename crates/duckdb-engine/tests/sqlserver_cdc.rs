//! `src.sqlserver.cdc` against a real SQL Server.
//!
//! Needs DUCKLE_MSSQL_HOST (and _PORT, _USER, _PASS), as the other live SQL
//! Server tests, and a login that may create databases. Each test makes its own
//! database with change data capture on, dropping it first, so a run that died
//! halfway does not poison the next. The capture job is not relied on: the tests
//! run the log scan themselves (sys.sp_cdc_scan), so no SQL Server Agent is
//! needed. For a throwaway server:
//!
//!   docker run -d -p 14330:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Duckle!Passw0rd mcr.microsoft.com/mssql/server:2022-latest
//!   DUCKLE_MSSQL_HOST=127.0.0.1 DUCKLE_MSSQL_PORT=14330 DUCKLE_MSSQL_PASS=Duckle!Passw0rd

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Mutex;

/// The workspace is process-global, and these tests each set their own.
static ENV_LOCK: Mutex<()> = Mutex::new(());

struct Server {
    engine: DuckdbEngine,
    bin: String,
    host: String,
    port: u16,
    user: String,
    pass: String,
}

fn server() -> Option<Server> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())?;
    let Ok(host) = std::env::var("DUCKLE_MSSQL_HOST") else {
        eprintln!("skipping: set DUCKLE_MSSQL_HOST to run against SQL Server");
        return None;
    };
    Some(Server {
        engine: DuckdbEngine::new(bin.clone().into()),
        bin,
        host,
        port: std::env::var("DUCKLE_MSSQL_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(1433),
        user: std::env::var("DUCKLE_MSSQL_USER").unwrap_or_else(|_| "sa".into()),
        pass: std::env::var("DUCKLE_MSSQL_PASS").unwrap_or_default(),
    })
}

/// Run T-SQL batches, one after another, in `database`.
fn tsql(s: &Server, database: &str, batches: &[&str]) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        use tokio_util::compat::TokioAsyncWriteCompatExt;
        let mut config = tiberius::Config::new();
        config.host(&s.host);
        config.port(s.port);
        config.authentication(tiberius::AuthMethod::sql_server(&s.user, &s.pass));
        config.database(database);
        config.trust_cert();
        let tcp = tokio::net::TcpStream::connect(config.get_addr()).await.expect("connect");
        let mut client = tiberius::Client::connect(config, tcp.compat_write()).await.expect("tds");
        for b in batches {
            client
                .simple_query(*b)
                .await
                .unwrap_or_else(|e| panic!("{b}: {e}"))
                .into_results()
                .await
                .unwrap_or_else(|e| panic!("{b}: {e}"));
        }
    });
}

/// A database of its own with change data capture on, and `dbo.orders` captured.
fn captured(s: &Server, db: &str) {
    tsql(
        s,
        "master",
        &[
            &format!("IF DB_ID(N'{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END"),
            &format!("CREATE DATABASE [{db}]"),
        ],
    );
    tsql(
        s,
        db,
        &[
            "EXEC sys.sp_cdc_enable_db",
            "CREATE TABLE dbo.orders (id int PRIMARY KEY, item nvarchar(40), qty int, price decimal(10,2), placed datetime2(3))",
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL",
        ],
    );
}

/// Harvest the log into the change tables now, as the capture job would.
fn scan(s: &Server, db: &str) {
    tsql(s, db, &["EXEC sys.sp_cdc_scan @maxtrans = 500, @maxscans = 10, @continuous = 0, @pollinginterval = 0"]);
}

fn pipeline(s: &Server, db: &str, extra: Value, out: &str, fail_after: bool) -> PipelineDoc {
    let mut props = json!({
        "host": s.host, "port": s.port, "user": s.user, "password": s.pass, "database": db,
        "trustCert": true, "table": "dbo.orders",
    });
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
                node("c", "src.sqlserver.cdc", props),
                node("x", "code.sql", json!({ "sql": "SELECT error('the load stops here') AS e FROM input" })),
                node("k", "snk.parquet", json!({ "path": out })),
            ]),
            json!([edge("e1", "c", "x"), edge("e2", "x", "k")]),
        )
    } else {
        (json!([node("c", "src.sqlserver.cdc", props), node("k", "snk.parquet", json!({ "path": out }))]), json!([edge("e1", "c", "k")]))
    };
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn query(bin: &str, sql: &str) -> Vec<Value> {
    let out = std::process::Command::new(bin).args([":memory:", "-json", "-c", sql]).output().expect("duckdb runs");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn changes(s: &Server, out: &str) -> Vec<Value> {
    query(&s.bin, &format!("SELECT _op, id, item, qty, price::VARCHAR AS price FROM read_parquet('{out}') ORDER BY _lsn, _seq"))
}

/// Run the pipeline under a workspace of its own, named, so its position is kept.
fn run(s: &Server, d: &PipelineDoc) -> RunResult {
    s.engine.execute_pipeline_named(d, "cdc_orders")
}

#[test]
fn sqlserver_cdc_reads_each_change_once_in_commit_order() {
    let Some(s) = server() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let db = "duckle_cdc_order";
    captured(&s, db);
    tsql(
        &s,
        db,
        &[
            "INSERT dbo.orders VALUES (1, N'apple', 2, 1.50, '2026-11-18T10:00:00'), (2, N'pear', 1, 0.99, '2026-11-18T10:05:00')",
            "UPDATE dbo.orders SET qty = 5 WHERE id = 1",
            "DELETE dbo.orders WHERE id = 2",
        ],
    );
    scan(&s, db);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let d = pipeline(&s, db, json!({}), &out, false);
    let r = run(&s, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        changes(&s, &out),
        vec![
            json!({"_op": "insert", "id": 1, "item": "apple", "qty": 2, "price": "1.50"}),
            json!({"_op": "insert", "id": 2, "item": "pear", "qty": 1, "price": "0.99"}),
            json!({"_op": "update", "id": 1, "item": "apple", "qty": 5, "price": "1.50"}),
            json!({"_op": "delete", "id": 2, "item": "pear", "qty": 1, "price": "0.99"}),
        ]
    );
    let types: Vec<String> = query(&s.bin, &format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        ["_op VARCHAR", "_lsn VARCHAR", "_seq VARCHAR", "_commit_ts TIMESTAMP", "id INTEGER", "item VARCHAR",
         "qty INTEGER", "price DECIMAL(10,2)", "placed TIMESTAMP"]
    );

    // Nothing new: no rows, the same columns.
    let r = run(&s, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(changes(&s, &out).is_empty(), "each change once");
    let names = query(&s.bin, &format!("DESCRIBE SELECT * FROM read_parquet('{out}')")).len();
    assert_eq!(names, 9, "an empty read keeps its columns");

    // And then only what came after.
    tsql(&s, db, &["INSERT dbo.orders VALUES (3, N'plum', 7, 2.00, NULL)"]);
    scan(&s, db);
    let r = run(&s, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(changes(&s, &out), vec![json!({"_op": "insert", "id": 3, "item": "plum", "qty": 7, "price": "2.00"})]);
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn sqlserver_cdc_reads_the_same_changes_again_after_a_failed_run() {
    let Some(s) = server() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let db = "duckle_cdc_retry";
    captured(&s, db);
    tsql(&s, db, &["INSERT dbo.orders VALUES (1, N'apple', 2, 1.50, NULL)"]);
    scan(&s, db);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let failing = run(&s, &pipeline(&s, db, json!({}), &out, true));
    assert_eq!(failing.status, "error");
    let r = run(&s, &pipeline(&s, db, json!({}), &out, false));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(changes(&s, &out).len(), 1, "the failed run moved nothing on");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn sqlserver_cdc_reads_a_batch_and_never_splits_a_transaction() {
    let Some(s) = server() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let db = "duckle_cdc_batch";
    captured(&s, db);
    tsql(
        &s,
        db,
        &[
            "BEGIN TRAN; INSERT dbo.orders VALUES (1, N'a', 1, 1, NULL), (2, N'b', 1, 1, NULL), (3, N'c', 1, 1, NULL); COMMIT",
            "INSERT dbo.orders VALUES (4, N'd', 1, 1, NULL)",
        ],
    );
    scan(&s, db);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let d = pipeline(&s, db, json!({ "batchSize": 2 }), &out, false);
    let r = run(&s, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let ids: Vec<Value> = changes(&s, &out).iter().map(|c| c["id"].clone()).collect();
    assert_eq!(ids, [json!(1), json!(2), json!(3)], "the whole transaction, though over the batch");
    let r = run(&s, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let ids: Vec<Value> = changes(&s, &out).iter().map(|c| c["id"].clone()).collect();
    assert_eq!(ids, [json!(4)], "the rest on the next run");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn sqlserver_cdc_refuses_changes_cleaned_up_before_it_read_them() {
    let Some(s) = server() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let db = "duckle_cdc_gap";
    captured(&s, db);
    tsql(&s, db, &["INSERT dbo.orders VALUES (1, N'apple', 2, 1.50, NULL)"]);
    scan(&s, db);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let d = pipeline(&s, db, json!({}), &out, false);
    assert_eq!(run(&s, &d).status, "ok");
    tsql(&s, db, &["INSERT dbo.orders VALUES (2, N'pear', 1, 0.99, NULL)", "INSERT dbo.orders VALUES (3, N'plum', 7, 2.00, NULL)"]);
    scan(&s, db);
    tsql(
        &s,
        db,
        &["DECLARE @lsn binary(10) = sys.fn_cdc_get_max_lsn(); \
           EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_orders', @low_water_mark = @lsn, @threshold = 5000"],
    );
    let r = run(&s, &d);
    assert_eq!(r.status, "error", "changes it never read are gone: going on would skip them");
    let err = format!("{:?}", r.error);
    assert!(err.contains("cleaned up"), "{err}");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn sqlserver_cdc_says_how_to_turn_it_on() {
    let Some(s) = server() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let db = "duckle_cdc_off";
    tsql(
        &s,
        "master",
        &[
            &format!("IF DB_ID(N'{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END"),
            &format!("CREATE DATABASE [{db}]"),
        ],
    );
    tsql(&s, db, &["CREATE TABLE dbo.orders (id int PRIMARY KEY, item nvarchar(40), qty int, price decimal(10,2), placed datetime2(3))"]);
    let r = run(&s, &pipeline(&s, db, json!({}), &out, false));
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("sp_cdc_enable_db"), "{:?}", r.error);
    tsql(&s, db, &["EXEC sys.sp_cdc_enable_db"]);
    let r = run(&s, &pipeline(&s, db, json!({}), &out, false));
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("sp_cdc_enable_table"), "{:?}", r.error);
    std::env::remove_var("DUCKLE_WORKSPACE");
}
