//! `src.mongodb.cdc` against a real MongoDB replica set.
//!
//! Change streams need a replica set (or a sharded cluster); a standalone server
//! refuses them. Needs DUCKLE_MONGO_RS_URI and DUCKLE_DUCKDB_BIN; without them the
//! tests skip. Each test owns a collection named for it. For a throwaway set:
//!
//!   docker run -d --name duckle-mongo -p 27018:27017 mongo:7 --replSet rs0 --bind_ip_all
//!   docker exec duckle-mongo mongosh --eval 'rs.initiate()'
//!   DUCKLE_MONGO_RS_URI=mongodb://127.0.0.1:27018/?directConnection=true

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use mongodb::bson::{doc, Document};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Mutex;

/// The workspace is process-global, and these tests each set their own.
static ENV_LOCK: Mutex<()> = Mutex::new(());

struct Live {
    engine: DuckdbEngine,
    bin: String,
    uri: String,
}

fn live() -> Option<Live> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())?;
    let Some(uri) = std::env::var("DUCKLE_MONGO_RS_URI").ok().filter(|u| !u.trim().is_empty()) else {
        eprintln!("skipping: set DUCKLE_MONGO_RS_URI to a MongoDB replica set to run");
        return None;
    };
    Some(Live { engine: DuckdbEngine::new(bin.clone().into()), bin, uri })
}

/// Do something to the collection, then return.
fn with_collection(uri: &str, name: &str, f: impl FnOnce(mongodb::Collection<Document>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let client = mongodb::Client::with_uri_str(uri).await.expect("connect");
        f(client.database("duckle_cdc").collection::<Document>(name)).await;
    });
}

fn fresh(uri: &str, name: &str) {
    with_collection(uri, name, |c| {
        Box::pin(async move {
            let _ = c.drop().await;
        })
    });
}

fn insert(uri: &str, name: &str, docs: Vec<Document>) {
    with_collection(uri, name, |c| {
        Box::pin(async move {
            c.insert_many(docs).await.expect("insert");
        })
    });
}

fn pipeline(l: &Live, collection: &str, extra: Value, out: &str, fail_after: bool) -> PipelineDoc {
    let mut props = json!({ "uri": l.uri, "database": "duckle_cdc", "collection": collection, "waitMs": 300 });
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
                node("c", "src.mongodb.cdc", props),
                node("x", "code.sql", json!({ "sql": "SELECT error('the load stops here') AS e FROM input" })),
                node("k", "snk.parquet", json!({ "path": out })),
            ]),
            json!([edge("e1", "c", "x"), edge("e2", "x", "k")]),
        )
    } else {
        (json!([node("c", "src.mongodb.cdc", props), node("k", "snk.parquet", json!({ "path": out }))]), json!([edge("e1", "c", "k")]))
    };
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn query(bin: &str, sql: &str) -> Vec<Value> {
    let out = std::process::Command::new(bin).args([":memory:", "-json", "-c", sql]).output().expect("duckdb runs");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

/// The changes a run read, in the order it wrote them.
fn changes(l: &Live, out: &str) -> Vec<Value> {
    query(
        &l.bin,
        &format!(
            "SELECT _op, _id::VARCHAR AS id, document::VARCHAR AS document, updated::VARCHAR AS updated, \
             removed::VARCHAR AS removed FROM read_parquet('{out}')"
        ),
    )
}

fn run(l: &Live, d: &PipelineDoc) -> RunResult {
    l.engine.execute_pipeline_named(d, "cdc_orders")
}

#[test]
fn mongodb_cdc_reads_each_change_once_in_order() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let coll = "orders_order";
    fresh(&l.uri, coll);
    // Before the first run: a change stream starts where it is opened, so this
    // one is never read.
    insert(&l.uri, coll, vec![doc! { "_id": 0, "item": "before" }]);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let d = pipeline(&l, coll, json!({}), &out, false);
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(changes(&l, &out).is_empty(), "the first run starts now");
    let types: Vec<String> = query(&l.bin, &format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        ["_op VARCHAR", "_token VARCHAR", "_ts TIMESTAMP WITH TIME ZONE", "_id JSON", "document JSON",
         "updated JSON", "removed JSON"],
        "a run with nothing to read is still typed"
    );

    with_collection(&l.uri, coll, |c| {
        Box::pin(async move {
            c.insert_one(doc! { "_id": 1, "item": "apple", "qty": 2 }).await.unwrap();
            c.insert_one(doc! { "_id": 2, "item": "pear", "qty": 1 }).await.unwrap();
            c.update_one(doc! { "_id": 1 }, doc! { "$set": { "qty": 5 }, "$unset": { "item": "" } }).await.unwrap();
            c.replace_one(doc! { "_id": 2 }, doc! { "item": "plum", "qty": 7 }).await.unwrap();
            c.insert_one(doc! { "_id": 3, "item": "fig", "qty": 9 }).await.unwrap();
            c.delete_one(doc! { "_id": 3 }).await.unwrap();
        })
    });
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let got = changes(&l, &out);
    let ops: Vec<&str> = got.iter().map(|c| c["_op"].as_str().unwrap()).collect();
    assert_eq!(ops, ["insert", "insert", "update", "replace", "insert", "delete"], "{got:?}");
    let ids: Vec<&str> = got.iter().map(|c| c["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["1", "2", "1", "2", "3", "3"]);
    // An update carries what changed, and the document as it now is.
    assert_eq!(got[2]["updated"], json!("{\"qty\":5}"));
    assert_eq!(got[2]["removed"], json!("[\"item\"]"));
    assert_eq!(got[2]["document"], json!("{\"_id\":1,\"qty\":5}"));
    assert_eq!(got[3]["document"], json!("{\"_id\":2,\"item\":\"plum\",\"qty\":7}"));
    // A delete has only the key.
    assert_eq!(got[5]["document"], Value::Null);

    // Nothing new: nothing read.
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(changes(&l, &out).is_empty(), "each change once");

    insert(&l.uri, coll, vec![doc! { "_id": 4, "item": "kiwi" }]);
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let got = changes(&l, &out);
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0]["id"], json!("4"));
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mongodb_cdc_reads_the_same_changes_again_after_a_failed_run() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let coll = "orders_retry";
    fresh(&l.uri, coll);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    assert_eq!(run(&l, &pipeline(&l, coll, json!({}), &out, false)).status, "ok", "the position to start from");
    insert(&l.uri, coll, vec![doc! { "_id": 1, "item": "apple" }]);
    let failing = run(&l, &pipeline(&l, coll, json!({}), &out, true));
    assert_eq!(failing.status, "error");
    let r = run(&l, &pipeline(&l, coll, json!({}), &out, false));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(changes(&l, &out).len(), 1, "the failed run moved nothing on");
    std::env::remove_var("DUCKLE_WORKSPACE");
}

#[test]
fn mongodb_cdc_stops_at_max_events_and_keeps_the_rest() {
    let Some(l) = live() else { return };
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let ws = tempfile::tempdir().unwrap();
    std::env::set_var("DUCKLE_WORKSPACE", ws.path());
    let coll = "orders_batch";
    fresh(&l.uri, coll);
    let out = ws.path().join("out.parquet").to_string_lossy().replace('\\', "/");
    let d = pipeline(&l, coll, json!({ "maxEvents": 2 }), &out, false);
    assert_eq!(run(&l, &d).status, "ok");
    with_collection(&l.uri, coll, |c| {
        Box::pin(async move {
            for i in 1..=3 {
                c.insert_one(doc! { "_id": i }).await.unwrap();
            }
        })
    });
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let ids: Vec<Value> = changes(&l, &out).iter().map(|c| c["id"].clone()).collect();
    assert_eq!(ids, [json!("1"), json!("2")], "no more than it was told to take");
    let r = run(&l, &d);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let ids: Vec<Value> = changes(&l, &out).iter().map(|c| c["id"].clone()).collect();
    assert_eq!(ids, [json!("3")], "the rest on the next run, none skipped");
    std::env::remove_var("DUCKLE_WORKSPACE");
}
