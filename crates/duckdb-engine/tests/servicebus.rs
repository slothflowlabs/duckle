//! src.servicebus and snk.servicebus against a Service Bus stand-in that runs
//! inside the test. It answers the REST calls (receive under a peek-lock or
//! with delete, complete, abandon, send a batch), checks every SAS signature
//! against the key, and records each call.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const KEY: &str = "c2VjcmV0LWtleS1mb3ItdGVzdHM=";

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
struct Call {
    method: String,
    path: String,
    head: String,
    body: String,
    signed: bool,
}

/// What the stand-in answers: status, extra headers, body.
type Reply = (u16, Vec<(String, String)>, String);

struct Bus {
    url: String,
    calls: Arc<Mutex<Vec<Call>>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

/// Does the Authorization header carry a valid SAS signature for the request?
fn signed_right(auth: &str) -> bool {
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    let Some(rest) = auth.strip_prefix("SharedAccessSignature ") else { return false };
    let field = |name: &str| rest.split('&').find_map(|p| p.strip_prefix(&format!("{name}=")).map(str::to_string));
    let (Some(sr), Some(sig), Some(se), Some(skn)) = (field("sr"), field("sig"), field("se"), field("skn")) else { return false };
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(format!("{sr}\n{se}").as_bytes());
    let want = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let want: String = url::form_urlencoded::byte_serialize(want.as_bytes()).collect();
    skn == "listen" && sig == want && se.parse::<i64>().map(|e| e > 1_700_000_000).unwrap_or(false)
}

impl Bus {
    fn start(answer: impl Fn(&str, &str, &str, &str) -> Reply + Send + 'static) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (keep, halt, base) = (calls.clone(), stop.clone(), url.clone());
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
                let (mut head, mut len, mut auth) = (String::new(), 0usize, String::new());
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                    if lower.starts_with("authorization:") {
                        auth = line[14..].trim().to_string();
                    }
                    head.push_str(&line);
                }
                let mut body = vec![0u8; len];
                let _ = reader.read_exact(&mut body);
                let body = String::from_utf8_lossy(&body).into_owned();
                let (status, headers, reply) = answer(&method, &path, &body, &base);
                keep.lock().unwrap().push(Call { method, path, head, body, signed: signed_right(&auth) });
                let mut stream = reader.into_inner();
                let mut out = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n", reply.len());
                for (k, v) in headers {
                    out.push_str(&format!("{k}: {v}\r\n"));
                }
                out.push_str("\r\n");
                out.push_str(&reply);
                let _ = stream.write_all(out.as_bytes());
                let _ = stream.flush();
            }
        });
        Bus { url, calls, stop, handle: Some(handle) }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn seen(&self) -> Vec<String> {
        self.calls().iter().map(|c| format!("{} {}", c.method, c.path)).collect()
    }
}

impl Drop for Bus {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// A queue at `entity` holding `n` messages: a peek-locked receive answers one
/// with its lock in Location, a deleting receive answers one with none, and a
/// complete (DELETE of a lock) or abandon (PUT) is taken.
fn queue(entity: &'static str, n: usize) -> impl Fn(&str, &str, &str, &str) -> Reply + Send + 'static {
    let left = Mutex::new((1..=n).collect::<VecDeque<usize>>());
    move |method, path, _body, base| {
        let head = format!("/{entity}/messages/head");
        if path.starts_with(&head) {
            let Some(i) = left.lock().unwrap().pop_front() else { return (204, vec![], String::new()) };
            let props = json!({"MessageId": format!("m{i}"), "Label": "order", "SequenceNumber": i, "DeliveryCount": 1,
                               "EnqueuedTimeUtc": "Thu, 19 Nov 2026 10:00:00 GMT", "LockToken": format!("lock-{i}")});
            let mut headers = vec![("BrokerProperties".to_string(), props.to_string()), ("Content-Type".to_string(), "application/json".to_string())];
            if method == "POST" {
                headers.push(("Location".to_string(), format!("{base}/{entity}/messages/m{i}/lock-{i}")));
                return (201, headers, format!("{{\"order\": {i}}}"));
            }
            return (200, headers, format!("{{\"order\": {i}}}"));
        }
        if path.starts_with(&format!("/{entity}/messages/m")) && (method == "DELETE" || method == "PUT") {
            return (200, vec![], String::new());
        }
        (404, vec![], "<Error><Code>404</Code><Detail>No such entity.</Detail></Error>".into())
    }
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

fn connection(bus: &Bus, extra: Value) -> Value {
    let mut props = json!({
        "connectionString": format!("Endpoint=sb://acme.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey={KEY}"),
        "endpoint": bus.url,
        "entity": "orders",
        "waitSeconds": 0,
    });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    props
}

fn read(engine: &DuckdbEngine, bus: &Bus, extra: Value, out: &str) -> RunResult {
    engine.execute_pipeline(&doc(
        json!([node("q", "src.servicebus", connection(bus, extra)), node("k", "snk.parquet", json!({ "path": out }))]),
        json!([edge("e1", "q", "k")]),
    ))
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
fn servicebus_source_takes_messages_under_a_lock_and_completes_them_once_the_run_succeeds() {
    let engine = engine_or_skip!();
    let bus = Bus::start(queue("orders", 3));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, &bus, json!({}), &out);
    assert_eq!(r.status, "ok", "src.servicebus failed: {:?}", r.error);
    let rows = duckdb_json(&format!(
        "SELECT message_id, body, subject, sequence_number, delivery_count, epoch(enqueued_at)::BIGINT AS enqueued, content_type \
         FROM read_parquet('{out}') ORDER BY sequence_number"
    ));
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(
        rows[0],
        json!({"message_id": "m1", "body": "{\"order\": 1}", "subject": "order", "sequence_number": 1, "delivery_count": 1,
               "enqueued": 1795082400, "content_type": "application/json"})
    );
    // One locked receive a message until the queue has none, then each lock completed.
    assert_eq!(
        bus.seen(),
        [
            "POST /orders/messages/head?timeout=0", "POST /orders/messages/head?timeout=0", "POST /orders/messages/head?timeout=0",
            "POST /orders/messages/head?timeout=0", "DELETE /orders/messages/m1/lock-1", "DELETE /orders/messages/m2/lock-2",
            "DELETE /orders/messages/m3/lock-3",
        ]
    );
    assert!(bus.calls().iter().all(|c| c.signed), "every call carries a valid SAS signature");
}

#[test]
fn servicebus_source_completes_nothing_when_the_run_fails() {
    let engine = engine_or_skip!();
    let bus = Bus::start(queue("orders", 2));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = engine.execute_pipeline(&doc(
        json!([
            node("q", "src.servicebus", connection(&bus, json!({}))),
            node("x", "code.sql", json!({ "sql": "SELECT error('the load stops here') AS e FROM input" })),
            node("k", "snk.parquet", json!({ "path": out })),
        ]),
        json!([edge("e1", "q", "x"), edge("e2", "x", "k")]),
    ));
    assert_eq!(r.status, "error");
    assert!(!bus.seen().iter().any(|c| c.starts_with("DELETE")), "the locks run out and the messages come back: {:?}", bus.seen());
}

#[test]
fn servicebus_source_can_take_messages_off_the_queue_as_it_reads() {
    let engine = engine_or_skip!();
    let bus = Bus::start(queue("orders", 2));
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, &bus, json!({ "receiveMode": "receiveAndDelete" }), &out_path(tmp.path()));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        bus.seen(),
        ["DELETE /orders/messages/head?timeout=0", "DELETE /orders/messages/head?timeout=0", "DELETE /orders/messages/head?timeout=0"],
        "removed as read, so nothing to complete after"
    );
}

#[test]
fn servicebus_source_reads_a_topic_subscription_and_an_empty_one_as_its_columns() {
    let engine = engine_or_skip!();
    let bus = Bus::start(queue("events/subscriptions/audit", 0));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, &bus, json!({ "entity": "events", "subscription": "audit" }), &out);
    assert_eq!(r.status, "ok", "an empty subscription is not an error: {:?}", r.error);
    assert_eq!(bus.seen(), ["POST /events/subscriptions/audit/messages/head?timeout=0"]);
    let names: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        names,
        ["message_id VARCHAR", "body VARCHAR", "subject VARCHAR", "session_id VARCHAR", "sequence_number BIGINT",
         "delivery_count BIGINT", "enqueued_at TIMESTAMP WITH TIME ZONE", "content_type VARCHAR"]
    );
}

#[test]
fn servicebus_source_preview_reads_one_message_and_puts_it_back() {
    let engine = engine_or_skip!();
    let bus = Bus::start(queue("orders", 5));
    let tmp = tempfile::tempdir().unwrap();
    let d = doc(
        json!([node("q", "src.servicebus", connection(&bus, json!({}))), node("k", "snk.parquet", json!({ "path": out_path(tmp.path()) }))]),
        json!([edge("e1", "q", "k")]),
    );
    let r = engine.execute_pipeline_with_events(&d, Some("q"), None, |_| {});
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(bus.seen(), ["POST /orders/messages/head?timeout=0", "PUT /orders/messages/m1/lock-1"]);

    // Receive and delete is a choice for runs: a preview of such a node still only looks.
    let bus = Bus::start(queue("orders", 5));
    let d = doc(
        json!([
            node("q", "src.servicebus", connection(&bus, json!({ "receiveMode": "receiveAndDelete" }))),
            node("k", "snk.parquet", json!({ "path": out_path(tmp.path()) })),
        ]),
        json!([edge("e1", "q", "k")]),
    );
    let r = engine.execute_pipeline_with_events(&d, Some("q"), None, |_| {});
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(bus.seen(), ["POST /orders/messages/head?timeout=0", "PUT /orders/messages/m1/lock-1"]);
}

#[test]
fn servicebus_says_what_service_bus_refused() {
    let engine = engine_or_skip!();
    let bus = Bus::start(|_, _, _, _| (401, vec![], "<Error><Code>401</Code><Detail>InvalidSignature: the token is not valid.</Detail></Error>".into()));
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, &bus, json!({}), &out_path(tmp.path()));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("401") && err.contains("InvalidSignature"), "{err}");
}

#[test]
fn servicebus_says_when_what_it_read_could_not_be_completed() {
    let engine = engine_or_skip!();
    let receive = queue("orders", 1);
    let bus = Bus::start(move |method, path, body, base| {
        if method == "DELETE" && path.contains("/lock-") {
            return (410, vec![], "<Error><Code>410</Code><Detail>The lock supplied is invalid.</Detail></Error>".into());
        }
        receive(method, path, body, base)
    });
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, &bus, json!({}), &out_path(tmp.path()));
    assert_eq!(r.status, "error", "a message left on the queue comes back, which the run says");
    let err = format!("{:?}", r.error);
    assert!(err.contains("lock supplied is invalid") && err.contains("delivered again"), "{err}");
}

fn send_rows(engine: &DuckdbEngine, bus: &Bus, sql: &str, extra: Value) -> RunResult {
    let tmp = tempfile::tempdir().unwrap();
    let kick = tmp.path().join("kick.csv");
    std::fs::write(&kick, "k\n1\n").unwrap();
    engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": kick.to_string_lossy().replace('\\', "/"), "hasHeader": true })),
            node("q", "code.sql", json!({ "sql": sql })),
            node("w", "snk.servicebus", connection(bus, extra)),
        ]),
        json!([edge("e1", "s", "q"), edge("e2", "q", "w")]),
    ))
}

#[test]
fn servicebus_sink_sends_rows_as_a_batch_with_each_message_s_properties() {
    let engine = engine_or_skip!();
    let bus = Bus::start(|method, path, _, _| match (method, path) {
        ("POST", "/orders/messages") => (201, vec![], String::new()),
        _ => (404, vec![], String::new()),
    });
    let r = send_rows(
        &engine,
        &bus,
        "SELECT 'hello ' || i AS payload, 'customer-' || (i % 2) AS customer, 'id-' || i AS mid FROM input, range(1, 4) t(i)",
        json!({ "bodyColumn": "payload", "sessionIdColumn": "customer", "messageIdColumn": "mid" }),
    );
    assert_eq!(r.status, "ok", "snk.servicebus failed: {:?}", r.error);
    let calls = bus.calls();
    assert_eq!(calls.len(), 1, "three small messages, one call");
    assert!(calls[0].head.to_ascii_lowercase().contains("content-type: application/vnd.microsoft.servicebus.json"), "{}", calls[0].head);
    assert!(calls[0].signed);
    let batch: Vec<Value> = serde_json::from_str(&calls[0].body).unwrap();
    let one = batch.iter().find(|m| m["Body"] == json!("hello 1")).expect("the column as the body");
    assert_eq!(one["BrokerProperties"], json!({"MessageId": "id-1", "SessionId": "customer-1"}));
}

#[test]
fn servicebus_sink_splits_a_batch_by_size_and_asks_a_busy_namespace_again() {
    let engine = engine_or_skip!();
    let busy = Mutex::new(1);
    let bus = Bus::start(move |method, path, _, _| {
        if method != "POST" || path != "/orders/messages" {
            return (404, vec![], String::new());
        }
        let mut left = busy.lock().unwrap();
        if *left > 0 {
            *left -= 1;
            return (503, vec![], "<Error><Code>503</Code><Detail>Server busy.</Detail></Error>".into());
        }
        (201, vec![], String::new())
    });
    // Three rows of 100 KiB each: two fit under 256 KiB, the third goes alone.
    let r = send_rows(&engine, &bus, "SELECT repeat('x', 102400) AS payload FROM input, range(1, 4) t(i)", json!({ "bodyColumn": "payload" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let sizes: Vec<usize> = bus
        .calls()
        .iter()
        .filter(|c| c.body.starts_with('['))
        .map(|c| serde_json::from_str::<Vec<Value>>(&c.body).unwrap().len())
        .collect();
    assert_eq!(sizes, [2, 2, 1], "the busy answer sent again, then the rest");
}
