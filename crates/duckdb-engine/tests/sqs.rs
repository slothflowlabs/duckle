//! src.sqs and snk.sqs against an SQS stand-in that runs inside the test. It
//! speaks SQS's JSON protocol (an `AmazonSQS.*` target, one JSON body) and
//! records every call, so a test can see what was asked and in what order.

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
struct Call {
    target: String,
    head: String,
    body: Value,
}

type Answer = Box<dyn Fn(&str, &Value) -> (u16, String) + Send>;

/// The stand-in. `answer` gets each call's action (ReceiveMessage, ...) and
/// body and gives back a status and a JSON body.
struct Sqs {
    url: String,
    calls: Arc<Mutex<Vec<Call>>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Sqs {
    fn start(answer: impl Fn(&str, &Value) -> (u16, String) + Send + 'static) -> Self {
        let answer: Answer = Box::new(answer);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (keep, halt) = (calls.clone(), stop.clone());
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
                let body: Value = serde_json::from_slice(&body).unwrap_or_default();
                let target = head
                    .lines()
                    .find_map(|l| l.to_ascii_lowercase().strip_prefix("x-amz-target:").map(|_| l[13..].trim().to_string()))
                    .unwrap_or_default();
                let action = target.trim_start_matches("AmazonSQS.").to_string();
                let (status, reply) = answer(&action, &body);
                keep.lock().unwrap().push(Call { target, head, body });
                let mut stream = reader.into_inner();
                let text = if status == 200 { "OK" } else { "Bad Request" };
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 {status} {text}\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                        reply.len()
                    )
                    .as_bytes(),
                );
                let _ = stream.flush();
            }
        });
        Sqs { url, calls, stop, handle: Some(handle) }
    }

    fn queue(&self, name: &str) -> String {
        format!("{}/123456789012/{name}", self.url)
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn actions(&self) -> Vec<String> {
        self.calls().iter().map(|c| c.target.trim_start_matches("AmazonSQS.").to_string()).collect()
    }
}

impl Drop for Sqs {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn message(i: usize) -> Value {
    json!({
        "MessageId": format!("m{i}"),
        "ReceiptHandle": format!("r{i}"),
        "MD5OfBody": "not checked",
        "Body": format!("{{\"order\": {i}}}"),
        "Attributes": {"SentTimestamp": "1763460000000", "ApproximateReceiveCount": "2"},
        "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "order"}},
    })
}

/// A queue holding `messages`, handed out up to the count each receive asks
/// for, and a delete that takes every entry.
fn queue_of(messages: Vec<Value>) -> impl Fn(&str, &Value) -> (u16, String) + Send + 'static {
    let left = Mutex::new(messages);
    move |action, body| match action {
        "ReceiveMessage" => {
            let mut left = left.lock().unwrap();
            let n = (body["MaxNumberOfMessages"].as_u64().unwrap_or(1) as usize).min(left.len());
            let taken: Vec<Value> = left.drain(..n).collect();
            let reply = if taken.is_empty() { json!({}) } else { json!({ "Messages": taken }) };
            (200, reply.to_string())
        }
        "DeleteMessageBatch" => {
            let ok: Vec<Value> = body["Entries"]
                .as_array()
                .map(|es| es.iter().map(|e| json!({ "Id": e["Id"] })).collect())
                .unwrap_or_default();
            (200, json!({ "Successful": ok, "Failed": [] }).to_string())
        }
        other => (400, json!({ "__type": "com.amazonaws.sqs#UnsupportedOperation", "message": other }).to_string()),
    }
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({
        "id": id,
        "type": "source",
        "position": {"x": 0, "y": 0},
        "data": {"label": id, "componentId": component, "properties": props}
    })
}

fn edge(id: &str, from: &str, to: &str) -> Value {
    json!({"id": id, "source": from, "target": to, "sourceHandle": "main", "targetHandle": "main", "data": {"connectionType": "main"}})
}

fn doc(nodes: Value, edges: Value) -> PipelineDoc {
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn credentials(queue_url: &str, extra: Value) -> Value {
    let mut props = json!({
        "queueUrl": queue_url,
        "region": "eu-west-1",
        "accessKeyId": "AKIDEXAMPLE",
        "secretAccessKey": "wJalrXUtnFEMI",
    });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    props
}

/// src.sqs into a Parquet file, and the result.
fn read_queue(engine: &DuckdbEngine, sqs: &Sqs, extra: Value, out: &str) -> RunResult {
    engine.execute_pipeline(&doc(
        json!([
            node("q", "src.sqs", credentials(&sqs.queue("orders"), extra)),
            node("k", "snk.parquet", json!({ "path": out })),
        ]),
        json!([edge("e1", "q", "k")]),
    ))
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn out_path(dir: &std::path::Path, name: &str) -> String {
    dir.join(name).to_string_lossy().replace('\\', "/")
}

#[test]
fn sqs_source_reads_the_queue_and_deletes_only_once_the_run_succeeds() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(queue_of((1..=12).map(message).collect()));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.parquet");
    let r = read_queue(&engine, &sqs, json!({}), &out);
    assert_eq!(r.status, "ok", "src.sqs failed: {:?}", r.error);

    let rows = duckdb_json(&format!(
        "SELECT message_id, body, epoch(sent_at)::BIGINT AS sent, receive_count, group_id, attributes::VARCHAR AS attributes \
         FROM read_parquet('{out}') ORDER BY message_id"
    ));
    assert_eq!(rows.len(), 12, "every message: {rows:?}");
    assert_eq!(
        rows[0],
        json!({"message_id": "m1", "body": "{\"order\": 1}", "sent": 1763460000, "receive_count": 2,
               "group_id": null, "attributes": "{\"kind\":\"order\"}"})
    );
    let types: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        ["message_id VARCHAR", "body VARCHAR", "sent_at TIMESTAMP WITH TIME ZONE", "receive_count BIGINT",
         "group_id VARCHAR", "attributes JSON"]
    );

    let calls = sqs.calls();
    // Ten at most a call; asked until the queue answers with nothing.
    assert_eq!(sqs.actions(), ["ReceiveMessage", "ReceiveMessage", "ReceiveMessage", "DeleteMessageBatch", "DeleteMessageBatch"]);
    let receive = &calls[0];
    assert_eq!(receive.body["QueueUrl"], json!(sqs.queue("orders")));
    assert_eq!(receive.body["MaxNumberOfMessages"], json!(10));
    assert_eq!(receive.body["WaitTimeSeconds"], json!(1));
    assert_eq!(receive.body["VisibilityTimeout"], json!(600), "hidden long enough for the run");
    let head = receive.head.to_ascii_lowercase();
    assert!(head.contains("content-type: application/x-amz-json-1.0"), "{head}");
    assert!(head.contains("/eu-west-1/sqs/aws4_request"), "signed for SQS in the region: {head}");
    let deleted: Vec<String> = calls[3..]
        .iter()
        .flat_map(|c| c.body["Entries"].as_array().cloned().unwrap_or_default())
        .map(|e| e["ReceiptHandle"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(deleted, (1..=12).map(|i| format!("r{i}")).collect::<Vec<_>>());
}

#[test]
fn sqs_source_leaves_the_messages_when_the_run_fails() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(queue_of((1..=3).map(message).collect()));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.parquet");
    let r = engine.execute_pipeline(&doc(
        json!([
            node("q", "src.sqs", credentials(&sqs.queue("orders"), json!({}))),
            node("x", "code.sql", json!({ "sql": "SELECT error('the load stops here') AS e FROM input" })),
            node("k", "snk.parquet", json!({ "path": out })),
        ]),
        json!([edge("e1", "q", "x"), edge("e2", "x", "k")]),
    ));
    assert_eq!(r.status, "error");
    assert!(
        !sqs.actions().iter().any(|a| a == "DeleteMessageBatch"),
        "a failed run deletes nothing, so the queue delivers them again: {:?}",
        sqs.actions()
    );
}

#[test]
fn sqs_source_preview_leaves_the_queue_as_it_was() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(queue_of((1..=25).map(message).collect()));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.parquet");
    let d = doc(
        json!([
            node("q", "src.sqs", credentials(&sqs.queue("orders"), json!({}))),
            node("k", "snk.parquet", json!({ "path": out })),
        ]),
        json!([edge("e1", "q", "k")]),
    );
    let r = engine.execute_pipeline_with_events(&d, Some("q"), None, |_| {});
    assert_eq!(r.status, "ok", "{:?}", r.error);
    // One look: messages left visible come straight back, so reading on would
    // only read the same ones again.
    assert_eq!(sqs.actions(), ["ReceiveMessage"], "and nothing deleted");
    assert_eq!(sqs.calls()[0].body["VisibilityTimeout"], json!(0), "a preview does not hide what it looked at");
}

#[test]
fn sqs_source_says_when_what_it_read_could_not_be_deleted() {
    let engine = engine_or_skip!();
    let receive = queue_of((1..=2).map(message).collect());
    let sqs = Sqs::start(move |action, body| {
        if action == "DeleteMessageBatch" {
            let refused = json!({ "Id": "1", "SenderFault": true, "Code": "ReceiptHandleIsInvalid", "Message": "expired" });
            return (200, json!({ "Successful": [{ "Id": "0" }], "Failed": [refused] }).to_string());
        }
        receive(action, body)
    });
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.parquet");
    let r = read_queue(&engine, &sqs, json!({}), &out);
    assert_eq!(r.status, "error", "a message left on the queue comes back, which the run says");
    let err = format!("{:?}", r.error);
    assert!(err.contains("ReceiptHandleIsInvalid") && err.contains("delivered again"), "{err}");
    let n = duckdb_json(&format!("SELECT count(*) AS n FROM read_parquet('{out}')"));
    assert_eq!(n[0]["n"], json!(2), "the rows were written all the same");
}

#[test]
fn sqs_source_reads_an_empty_queue_as_its_columns() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(queue_of(Vec::new()));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.parquet");
    let r = read_queue(&engine, &sqs, json!({}), &out);
    assert_eq!(r.status, "ok", "an empty queue is not an error: {:?}", r.error);
    let n = duckdb_json(&format!("SELECT count(*) AS n FROM read_parquet('{out}')"));
    assert_eq!(n[0]["n"], json!(0));
    let names: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| c["column_name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, ["message_id", "body", "sent_at", "receive_count", "group_id", "attributes"]);
    assert_eq!(sqs.actions(), ["ReceiveMessage"], "nothing received, nothing to delete");
}

#[test]
fn sqs_source_stops_at_max_messages() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(queue_of((1..=30).map(message).collect()));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path(), "out.parquet");
    let r = read_queue(&engine, &sqs, json!({ "maxMessages": 13, "waitSeconds": 0, "visibilityTimeout": 90 }), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let n = duckdb_json(&format!("SELECT count(*) AS n FROM read_parquet('{out}')"));
    assert_eq!(n[0]["n"], json!(13));
    let asked: Vec<Value> = sqs
        .calls()
        .iter()
        .filter(|c| c.target.ends_with("ReceiveMessage"))
        .map(|c| c.body["MaxNumberOfMessages"].clone())
        .collect();
    assert_eq!(asked, [json!(10), json!(3)], "no more than it was told to take");
    let first = &sqs.calls()[0].body;
    assert_eq!((first["WaitTimeSeconds"].clone(), first["VisibilityTimeout"].clone()), (json!(0), json!(90)));
}

#[test]
fn sqs_source_says_what_sqs_refused() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(|_, _| {
        (400, json!({ "__type": "com.amazonaws.sqs#QueueDoesNotExist", "message": "The specified queue does not exist." }).to_string())
    });
    let tmp = tempfile::tempdir().unwrap();
    let r = read_queue(&engine, &sqs, json!({}), &out_path(tmp.path(), "out.parquet"));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("QueueDoesNotExist") && err.contains("does not exist"), "{err}");
}

/// A queue that takes every message sent to it, unless `refuse` says otherwise
/// for an entry's body: Some((sender_fault, code)).
fn send_queue(
    refuse: impl Fn(&Value, usize) -> Option<(bool, &'static str)> + Send + 'static,
) -> impl Fn(&str, &Value) -> (u16, String) + Send + 'static {
    let attempt = Mutex::new(0usize);
    move |action, body| {
        if action != "SendMessageBatch" {
            return (400, json!({ "__type": "com.amazonaws.sqs#UnsupportedOperation", "message": action }).to_string());
        }
        let mut n = attempt.lock().unwrap();
        *n += 1;
        let (mut ok, mut failed) = (Vec::new(), Vec::new());
        for e in body["Entries"].as_array().cloned().unwrap_or_default() {
            match refuse(&e, *n) {
                Some((sender, code)) => failed.push(json!({ "Id": e["Id"], "SenderFault": sender, "Code": code, "Message": "not this one" })),
                None => ok.push(json!({ "Id": e["Id"], "MessageId": "x", "MD5OfMessageBody": "x" })),
            }
        }
        (200, json!({ "Successful": ok, "Failed": failed }).to_string())
    }
}

fn write_rows(engine: &DuckdbEngine, sqs: &Sqs, queue: &str, sql: &str, extra: Value) -> RunResult {
    let tmp = tempfile::tempdir().unwrap();
    let kick = tmp.path().join("kick.csv");
    std::fs::write(&kick, "k\n1\n").unwrap();
    engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": kick.to_string_lossy().replace('\\', "/"), "hasHeader": true })),
            node("q", "code.sql", json!({ "sql": sql })),
            node("w", "snk.sqs", credentials(&sqs.queue(queue), extra)),
        ]),
        json!([edge("e1", "s", "q"), edge("e2", "q", "w")]),
    ))
}

fn entries(sqs: &Sqs) -> Vec<Vec<Value>> {
    sqs.calls().iter().map(|c| c.body["Entries"].as_array().cloned().unwrap_or_default()).collect()
}

#[test]
fn sqs_sink_sends_each_row_as_a_message_ten_to_a_call() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(send_queue(|_, _| None));
    let r = write_rows(&engine, &sqs, "orders", "SELECT i AS id, 'n' || i AS name FROM input, range(1, 24) t(i)", json!({}));
    assert_eq!(r.status, "ok", "snk.sqs failed: {:?}", r.error);
    let calls = entries(&sqs);
    assert_eq!(calls.iter().map(Vec::len).collect::<Vec<_>>(), [10, 10, 3]);
    let mut bodies: Vec<Value> = calls
        .iter()
        .flatten()
        .map(|e| serde_json::from_str(e["MessageBody"].as_str().unwrap()).unwrap())
        .collect();
    bodies.sort_by_key(|b: &Value| b["id"].as_i64());
    assert_eq!(bodies[0], json!({ "id": 1, "name": "n1" }), "a row goes as its JSON");
    let ids: std::collections::HashSet<_> = calls[0].iter().map(|e| e["Id"].as_str().unwrap().to_string()).collect();
    assert_eq!(ids.len(), 10, "every entry of a call has its own id");
    assert_eq!(sqs.calls()[0].target, "AmazonSQS.SendMessageBatch");
}

#[test]
fn sqs_sink_sends_a_column_as_the_body_and_fills_a_fifo_queues_fields() {
    let engine = engine_or_skip!();
    let sqs = Sqs::start(send_queue(|_, _| None));
    let r = write_rows(
        &engine,
        &sqs,
        "orders.fifo",
        "SELECT 'hello ' || i AS payload, 'customer-' || (i % 2) AS customer, i AS id FROM input, range(1, 4) t(i)",
        json!({ "bodyColumn": "payload", "groupIdColumn": "customer", "deduplicationIdColumn": "id" }),
    );
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let sent = &entries(&sqs)[0];
    assert_eq!(sent.len(), 3);
    let one = sent.iter().find(|e| e["MessageBody"] == json!("hello 1")).expect("the column, as the body");
    assert_eq!(one["MessageGroupId"], json!("customer-1"));
    assert_eq!(one["MessageDeduplicationId"], json!("1"));
}

#[test]
fn sqs_sink_sends_again_what_sqs_could_not_take_and_names_what_it_refused() {
    let engine = engine_or_skip!();
    // The second entry fails on the first attempt only, on SQS's side.
    let sqs = Sqs::start(send_queue(|e, attempt| (attempt == 1 && e["Id"] == json!("1")).then_some((false, "InternalError"))));
    let r = write_rows(&engine, &sqs, "orders", "SELECT i AS id FROM input, range(1, 4) t(i)", json!({}));
    assert_eq!(r.status, "ok", "a failure on SQS's side is sent again: {:?}", r.error);
    let calls = entries(&sqs);
    assert_eq!(calls.iter().map(Vec::len).collect::<Vec<_>>(), [3, 1], "only the one SQS did not take");

    // A message SQS refuses as the sender's fault is not sent again.
    let sqs = Sqs::start(send_queue(|e, _| (e["Id"] == json!("2")).then_some((true, "InvalidParameterValue"))));
    let r = write_rows(&engine, &sqs, "orders", "SELECT i AS id FROM input, range(1, 4) t(i)", json!({}));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("InvalidParameterValue") && err.contains("row 3"), "{err}");
    assert_eq!(entries(&sqs).len(), 1, "a refusal is not retried");
}
