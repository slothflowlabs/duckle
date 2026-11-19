//! src.mqtt and snk.mqtt against a broker stand-in that runs inside the test,
//! and against a real broker when DUCKLE_MQTT_URL names one.
//!
//! The stand-in speaks MQTT 3.1.1 as the specification lays it out: it answers
//! CONNECT with the CONNACK it is given, SUBSCRIBE with a SUBACK and then the
//! messages it holds, and a QoS 1 PUBLISH with a PUBACK, and it records every
//! packet it hears. Over TLS it serves a certificate from a CA minted here,
//! which Duckle trusts through DUCKLE_CA_CERT.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{json, Value};
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// A message the stand-in holds for a subscriber.
#[derive(Clone)]
struct Held {
    topic: &'static str,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
}

fn held(topic: &'static str, payload: &[u8], qos: u8, retain: bool) -> Held {
    Held { topic, payload: payload.to_vec(), qos, retain }
}

/// How the stand-in answers.
#[derive(Clone, Default)]
struct Script {
    /// The CONNACK return code: 0 accepts.
    refuse: u8,
    /// The SUBACK codes. Empty grants each filter the QoS it asked for.
    suback: Vec<u8>,
    /// Sent after the SUBACK.
    held: Vec<Held>,
    /// Sent this long after those.
    late: Option<(Duration, Held)>,
    /// Hang up on hearing this many PUBLISH packets, acknowledging none.
    hang_up_after: Option<usize>,
    /// Send each held message's first byte, then the rest this long after.
    pause_mid_packet: Option<Duration>,
}

trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

struct Broker {
    port: u16,
    log: Arc<Mutex<Vec<String>>>,
    done: Arc<AtomicUsize>,
}

impl Broker {
    fn start(script: Script, tls: Option<Arc<rustls::ServerConfig>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (log, done) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicUsize::new(0)));
        let (keep, finished) = (log.clone(), done.clone());
        std::thread::spawn(move || {
            for tcp in listener.incoming() {
                let Ok(mut tcp) = tcp else { return };
                tcp.set_read_timeout(Some(Duration::from_secs(20))).ok();
                tcp.set_nodelay(true).ok();
                // A client that refuses the certificate or hangs up is an
                // outcome under test, not a fault of the stand-in.
                let _ = match &tls {
                    Some(config) => match rustls::ServerConnection::new(config.clone()) {
                        Ok(c) => converse(&mut rustls::StreamOwned::new(c, tcp), &script, &keep),
                        Err(e) => Err(io::Error::other(e)),
                    },
                    None => converse(&mut tcp, &script, &keep),
                };
                finished.fetch_add(1, Ordering::SeqCst);
            }
        });
        Broker { port, log, done }
    }

    fn url(&self, scheme: &str) -> String {
        format!("{scheme}://127.0.0.1:{}", self.port)
    }

    /// Every packet heard, once `connections` connections have ended.
    fn heard(&self, connections: usize) -> Vec<String> {
        let start = Instant::now();
        while self.done.load(Ordering::SeqCst) < connections && start.elapsed() < Duration::from_secs(10) {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.log.lock().unwrap().clone()
    }
}

fn read_packet(s: &mut dyn Stream) -> io::Result<(u8, Vec<u8>)> {
    let mut first = [0u8; 1];
    s.read_exact(&mut first)?;
    let (mut len, mut shift) = (0usize, 0);
    loop {
        let mut b = [0u8; 1];
        s.read_exact(&mut b)?;
        len |= ((b[0] & 0x7F) as usize) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; len];
    s.read_exact(&mut body)?;
    Ok((first[0], body))
}

fn frame(header: u8, body: &[u8]) -> Vec<u8> {
    let (mut out, mut n) = (vec![header], body.len());
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

fn send(s: &mut dyn Stream, header: u8, body: &[u8]) -> io::Result<()> {
    s.write_all(&frame(header, body))?;
    s.flush()
}

/// The string field at `at`, and where the next field starts.
fn string_at(body: &[u8], at: usize) -> (String, usize) {
    let len = u16::from_be_bytes([body[at], body[at + 1]]) as usize;
    (String::from_utf8_lossy(&body[at + 2..at + 2 + len]).into_owned(), at + 2 + len)
}

fn deliver(s: &mut dyn Stream, m: &Held, id: Option<u16>, pause: Option<Duration>) -> io::Result<()> {
    let mut body = (m.topic.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(m.topic.as_bytes());
    if let Some(id) = id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(&m.payload);
    let packet = frame(0x30 | (m.qos << 1) | u8::from(m.retain), &body);
    if let Some(pause) = pause {
        s.write_all(&packet[..1])?;
        s.flush()?;
        std::thread::sleep(pause);
        s.write_all(&packet[1..])?;
    } else {
        s.write_all(&packet)?;
    }
    s.flush()
}

fn converse(s: &mut dyn Stream, script: &Script, log: &Mutex<Vec<String>>) -> io::Result<()> {
    let say = |line: String| log.lock().unwrap().push(line);
    let mut publishes = 0;
    loop {
        let (header, body) = read_packet(s)?;
        match header >> 4 {
            1 => {
                // CONNECT: protocol name, level, flags and keep-alive, then the
                // client id and the sign-in its flags announce.
                let (_, at) = string_at(&body, 0);
                let (flags, keep_alive) = (body[at + 1], u16::from_be_bytes([body[at + 2], body[at + 3]]));
                let (id, mut at) = string_at(&body, at + 4);
                let (mut user, mut password) = ("-".to_string(), "-".to_string());
                if flags & 0x80 != 0 {
                    (user, at) = string_at(&body, at);
                }
                if flags & 0x40 != 0 {
                    (password, _) = string_at(&body, at);
                }
                let clean = (flags >> 1) & 1;
                say(format!("CONNECT id={id} clean={clean} keepalive={keep_alive} user={user} password={password}"));
                send(s, 0x20, &[0, script.refuse])?;
                if script.refuse != 0 {
                    return Ok(());
                }
            }
            8 => {
                // SUBSCRIBE: a packet id, then each filter and the QoS it asks for.
                let (mut at, mut filters, mut asked) = (2, Vec::new(), Vec::new());
                while at < body.len() {
                    let (filter, next) = string_at(&body, at);
                    filters.push(format!("{filter}@{}", body[next]));
                    asked.push(body[next]);
                    at = next + 1;
                }
                say(format!("SUBSCRIBE {}", filters.join(" ")));
                let codes = if script.suback.is_empty() { asked } else { script.suback.clone() };
                send(s, 0x90, &[&body[..2], &codes[..]].concat())?;
                let mut next_id = 0u16;
                let late = script.late.iter().map(|(after, m)| (*after, m));
                for (after, m) in script.held.iter().map(|m| (Duration::ZERO, m)).chain(late) {
                    std::thread::sleep(after);
                    let id = (m.qos > 0).then(|| {
                        next_id += 1;
                        next_id
                    });
                    deliver(s, m, id, script.pause_mid_packet)?;
                }
            }
            3 => {
                publishes += 1;
                let qos = (header >> 1) & 3;
                let (topic, mut at) = string_at(&body, 0);
                let mut id = "-".to_string();
                if qos > 0 {
                    id = u16::from_be_bytes([body[at], body[at + 1]]).to_string();
                    at += 2;
                }
                let payload = String::from_utf8_lossy(&body[at..]);
                say(format!("PUBLISH {topic} qos={qos} retain={} id={id} {payload}", header & 1));
                if script.hang_up_after == Some(publishes) {
                    return Ok(());
                }
                if qos == 1 && script.hang_up_after.is_none() {
                    send(s, 0x40, &body[at - 2..at])?;
                }
            }
            4 => say(format!("PUBACK {}", u16::from_be_bytes([body[0], body[1]]))),
            12 => {
                say("PINGREQ".into());
                send(s, 0xD0, &[])?;
            }
            14 => {
                say("DISCONNECT".into());
                return Ok(());
            }
            other => say(format!("packet type {other}")),
        }
    }
}

/// A CA's certificate, and a server configured with a certificate it issued
/// for 127.0.0.1.
fn pki() -> (String, Arc<rustls::ServerConfig>) {
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca.distinguished_name.push(rcgen::DnType::CommonName, "Duckle test CA");
    let ca = rcgen::CertifiedIssuer::self_signed(ca, rcgen::KeyPair::generate().unwrap()).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap().signed_by(&key, &ca).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![leaf.der().clone()], PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())))
        .unwrap();
    (ca.pem(), Arc::new(config))
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

/// A subscription to sensors/# on `url`, with what `extra` changes.
fn subscription(url: &str, extra: Value) -> Value {
    with(json!({ "url": url, "topics": "sensors/#", "waitMs": 300 }), extra)
}

fn reading(props: Value, out: &str) -> PipelineDoc {
    doc(json!([node("m", "src.mqtt", props), node("k", "snk.parquet", json!({ "path": out }))]), json!([edge("e1", "m", "k")]))
}

fn read(engine: &DuckdbEngine, props: Value, out: &str) -> RunResult {
    engine.execute_pipeline(&reading(props, out))
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn out_path(dir: &std::path::Path) -> String {
    dir.join("out.parquet").to_string_lossy().replace('\\', "/")
}

fn rows(out: &str) -> Vec<Value> {
    duckdb_json(&format!("SELECT topic, payload, encoding, qos, retained FROM read_parquet('{out}') ORDER BY topic, payload"))
}

/// Send what `sql` selects through snk.mqtt.
fn publish(engine: &DuckdbEngine, sql: &str, props: Value) -> RunResult {
    let tmp = tempfile::tempdir().unwrap();
    let kick = tmp.path().join("kick.csv");
    std::fs::write(&kick, "k\n1\n").unwrap();
    engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": kick.to_string_lossy().replace('\\', "/"), "hasHeader": true })),
            node("q", "code.sql", json!({ "sql": sql })),
            node("w", "snk.mqtt", props),
        ]),
        json!([edge("e1", "s", "q"), edge("e2", "q", "w")]),
    ))
}

/// The CONNECT a run sent, with a client id Duckle made up put as `fresh`.
fn connect_of(heard: &[String]) -> String {
    let line = heard.first().cloned().unwrap_or_default();
    let id = line.split_whitespace().nth(1).and_then(|f| f.strip_prefix("id=")).unwrap_or_default();
    let made_up = id.len() == 23 && id.starts_with("duckle-") && id[7..].chars().all(|c| c.is_ascii_hexdigit());
    if made_up { line.replacen(id, "fresh", 1) } else { line }
}

#[test]
fn mqtt_source_reads_each_message_as_a_row_and_acknowledges_qos_1() {
    let engine = engine_or_skip!();
    let broker = Broker::start(
        Script {
            held: vec![
                held("sensors/a", b"{\"t\": 21}", 1, true),
                held("sensors/b", &[0xFF, 0x00], 0, false),
                held("sensors/c", b"plain", 1, false),
            ],
            ..Default::default()
        },
        None,
    );
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, subscription(&broker.url("mqtt"), json!({})), &out);
    assert_eq!(r.status, "ok", "src.mqtt failed: {:?}", r.error);
    assert_eq!(
        rows(&out),
        [
            json!({"topic": "sensors/a", "payload": "{\"t\": 21}", "encoding": "utf-8", "qos": 1, "retained": true}),
            json!({"topic": "sensors/b", "payload": "/wA=", "encoding": "base64", "qos": 0, "retained": false}),
            json!({"topic": "sensors/c", "payload": "plain", "encoding": "utf-8", "qos": 1, "retained": false}),
        ]
    );
    let heard = broker.heard(1);
    assert_eq!(connect_of(&heard), "CONNECT id=fresh clean=1 keepalive=60 user=- password=-");
    assert_eq!(heard[1..], ["SUBSCRIBE sensors/#@1", "PUBACK 1", "PUBACK 2", "DISCONNECT"]);
}

#[test]
fn mqtt_source_keeps_a_session_and_signs_in_when_asked() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script::default(), None);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let props = json!({ "clientId": "plant-7", "persistentSession": true, "user": "ada", "password": "pw", "qos": "0" });
    let r = read(&engine, subscription(&broker.url("mqtt"), props), &out);
    assert_eq!(r.status, "ok", "nothing to read is not an error: {:?}", r.error);
    assert_eq!(
        broker.heard(1),
        ["CONNECT id=plant-7 clean=0 keepalive=60 user=ada password=pw", "SUBSCRIBE sensors/#@0", "DISCONNECT"]
    );
    let columns: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        columns,
        ["topic VARCHAR", "payload VARCHAR", "encoding VARCHAR", "qos INTEGER", "retained BOOLEAN",
         "received_at TIMESTAMP WITH TIME ZONE"]
    );
}

#[test]
fn mqtt_source_stops_at_max_messages_and_acknowledges_only_what_it_read() {
    let engine = engine_or_skip!();
    let five = ["sensors/1", "sensors/2", "sensors/3", "sensors/4", "sensors/5"].map(|t| held(t, b"x", 1, false));
    let broker = Broker::start(Script { held: five.to_vec(), ..Default::default() }, None);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, subscription(&broker.url("mqtt"), json!({ "maxMessages": 2 })), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(rows(&out).len(), 2);
    assert_eq!(broker.heard(1)[1..], ["SUBSCRIBE sensors/#@1", "PUBACK 1", "PUBACK 2", "DISCONNECT"]);
}

#[test]
fn mqtt_source_ends_once_a_wait_passes_with_no_message() {
    let engine = engine_or_skip!();
    let broker = Broker::start(
        Script {
            held: vec![held("sensors/a", b"now", 0, false)],
            late: Some((Duration::from_millis(1500), held("sensors/b", b"too late", 0, false))),
            ..Default::default()
        },
        None,
    );
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, subscription(&broker.url("mqtt"), json!({ "waitMs": 300 })), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(rows(&out), [json!({"topic": "sensors/a", "payload": "now", "encoding": "utf-8", "qos": 0, "retained": false})]);
}

#[test]
fn mqtt_source_reads_a_message_that_arrives_in_pieces() {
    let engine = engine_or_skip!();
    // The rest of the packet comes after the read's short poll has run out:
    // the byte already read must not be lost to it.
    let broker = Broker::start(
        Script {
            held: vec![held("sensors/a", b"whole", 1, false)],
            pause_mid_packet: Some(Duration::from_millis(600)),
            ..Default::default()
        },
        None,
    );
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, subscription(&broker.url("mqtt"), json!({ "waitMs": 300 })), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(rows(&out), [json!({"topic": "sensors/a", "payload": "whole", "encoding": "utf-8", "qos": 1, "retained": false})]);
}

#[test]
fn mqtt_refuses_a_qos_2_message_it_never_asked_for() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script { held: vec![held("sensors/a", b"x", 2, false)], ..Default::default() }, None);
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, subscription(&broker.url("mqtt"), json!({})), &out_path(tmp.path()));
    assert_eq!(r.status, "error", "a QoS 2 message needs a reply this client does not send");
    let err = format!("{:?}", r.error);
    assert!(err.contains("QoS 2"), "{err}");
}

#[test]
fn mqtt_says_why_the_broker_refused_the_connection() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    for (code, reason) in [(5u8, "not authorized"), (4, "user name or password")] {
        let broker = Broker::start(Script { refuse: code, ..Default::default() }, None);
        let r = read(&engine, subscription(&broker.url("mqtt"), json!({})), &out_path(tmp.path()));
        assert_eq!(r.status, "error");
        let err = format!("{:?}", r.error);
        assert!(err.contains(reason), "CONNACK {code}: {err}");
    }
}

#[test]
fn mqtt_says_which_subscription_the_broker_refused() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script { suback: vec![1, 0x80], ..Default::default() }, None);
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, subscription(&broker.url("mqtt"), json!({ "topics": "sensors/#, admin/#" })), &out_path(tmp.path()));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("refused the subscription to: admin/#"), "{err}");
}

#[test]
fn mqtt_source_preview_takes_nothing_from_a_kept_session() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script { held: vec![held("sensors/a", b"x", 1, false)], ..Default::default() }, None);
    let tmp = tempfile::tempdir().unwrap();
    let props = subscription(&broker.url("mqtt"), json!({ "clientId": "plant-7", "persistentSession": true }));
    let r = engine.execute_pipeline_with_events(&reading(props, &out_path(tmp.path())), Some("m"), None, |_| {});
    assert_eq!(r.status, "ok", "{:?}", r.error);
    // Its own id, and a clean session: the one kept for plant-7 is left as it was.
    assert_eq!(connect_of(&broker.heard(1)), "CONNECT id=fresh clean=1 keepalive=60 user=- password=-");
}

#[test]
fn mqtt_reads_over_tls_from_a_broker_duckle_trusts() {
    let engine = engine_or_skip!();
    let (ca_pem, config) = pki();
    let broker = Broker::start(Script { held: vec![held("sensors/a", b"over tls", 0, false)], ..Default::default() }, Some(config));
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let props = subscription(&broker.url("mqtts"), json!({}));
    let refused = read(&engine, props.clone(), &out);
    assert_eq!(refused.status, "error", "a certificate from a CA Duckle does not trust");
    assert!(format!("{:?}", refused.error).contains("TLS handshake"), "{:?}", refused.error);
    let ca = tmp.path().join("ca.pem");
    std::fs::write(&ca, &ca_pem).unwrap();
    std::env::set_var("DUCKLE_CA_CERT", &ca);
    let r = read(&engine, props, &out);
    std::env::remove_var("DUCKLE_CA_CERT");
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(rows(&out)[0]["payload"], json!("over tls"));
}

#[test]
fn mqtt_sink_publishes_each_row_to_its_topic_and_waits_for_each_puback() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script::default(), None);
    let r = publish(
        &engine,
        "SELECT 'plant/' || i AS topic, '{\"t\":' || i || '}' AS body FROM input, range(1, 4) t(i) ORDER BY i",
        json!({ "url": broker.url("mqtt"), "topicColumn": "topic", "payloadColumn": "body", "retain": true }),
    );
    assert_eq!(r.status, "ok", "snk.mqtt failed: {:?}", r.error);
    let heard = broker.heard(1);
    assert_eq!(connect_of(&heard), "CONNECT id=fresh clean=1 keepalive=60 user=- password=-");
    assert_eq!(
        heard[1..],
        [
            "PUBLISH plant/1 qos=1 retain=1 id=1 {\"t\":1}",
            "PUBLISH plant/2 qos=1 retain=1 id=2 {\"t\":2}",
            "PUBLISH plant/3 qos=1 retain=1 id=3 {\"t\":3}",
            "DISCONNECT",
        ]
    );
}

#[test]
fn mqtt_sink_sends_the_row_as_json_at_qos_0() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script::default(), None);
    let r = publish(
        &engine,
        "SELECT i, 'n' || i AS name FROM input, range(1, 3) t(i) ORDER BY i",
        json!({ "url": broker.url("mqtt"), "topic": "plant/all", "qos": "0" }),
    );
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(
        broker.heard(1)[1..],
        [
            "PUBLISH plant/all qos=0 retain=0 id=- {\"i\":1,\"name\":\"n1\"}",
            "PUBLISH plant/all qos=0 retain=0 id=- {\"i\":2,\"name\":\"n2\"}",
            "DISCONNECT",
        ]
    );
}

#[test]
fn mqtt_sink_fails_when_the_broker_leaves_messages_unacknowledged() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script { hang_up_after: Some(2), ..Default::default() }, None);
    let r = publish(
        &engine,
        "SELECT 'plant/' || i AS topic FROM input, range(1, 4) t(i)",
        json!({ "url": broker.url("mqtt"), "topicColumn": "topic" }),
    );
    assert_eq!(r.status, "error", "a QoS 1 message the broker never acknowledged");
    let err = format!("{:?}", r.error);
    assert!(err.contains("rows 1 to 3 were sent and not all acknowledged"), "{err}");
}

#[test]
fn mqtt_sink_sends_nothing_when_a_row_has_no_topic_it_can_take() {
    let engine = engine_or_skip!();
    let broker = Broker::start(Script::default(), None);
    for (sql, why) in [
        ("SELECT CASE WHEN i = 2 THEN NULL ELSE 'plant/' || i END AS topic FROM input, range(1, 4) t(i) ORDER BY i",
         "row 2 has no value in the topic column 'topic'"),
        ("SELECT 'plant/+' AS topic FROM input", "row 1: 'plant/+' has a wildcard"),
    ] {
        let r = publish(&engine, sql, json!({ "url": broker.url("mqtt"), "topicColumn": "topic" }));
        assert_eq!(r.status, "error");
        let err = format!("{:?}", r.error);
        assert!(err.contains(why), "{err}");
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(broker.heard(0), Vec::<String>::new(), "every message is built before the broker is called");
}

/// A broker to run against for real, such as mqtt://localhost:1883.
fn live() -> Option<String> {
    std::env::var("DUCKLE_MQTT_URL").ok().filter(|s| !s.is_empty())
}

macro_rules! live_or_skip {
    () => {
        match live() {
            Some(u) => u,
            None => {
                eprintln!("skipping: set DUCKLE_MQTT_URL to a broker, such as mqtt://localhost:1883, to run");
                return;
            }
        }
    };
}

fn unique() -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{}", nanos % 1_000_000_000_000)
}

#[test]
fn mqtt_live_retained_messages_go_out_and_come_back() {
    let engine = engine_or_skip!();
    let url = live_or_skip!();
    let root = format!("duckle-test/{}", unique());
    let sent = publish(
        &engine,
        &format!("SELECT '{root}/' || i AS topic, 'v' || i AS body FROM input, range(1, 4) t(i)"),
        json!({ "url": url, "topicColumn": "topic", "payloadColumn": "body", "retain": true }),
    );
    assert_eq!(sent.status, "ok", "{:?}", sent.error);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, json!({ "url": url, "topics": format!("{root}/#"), "waitMs": 1000 }), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let expected: Vec<Value> = (1..=3)
        .map(|i| json!({"topic": format!("{root}/{i}"), "payload": format!("v{i}"), "encoding": "utf-8", "qos": 1, "retained": true}))
        .collect();
    assert_eq!(rows(&out), expected);
}

#[test]
fn mqtt_live_a_kept_session_gets_what_was_sent_while_it_was_away() {
    let engine = engine_or_skip!();
    let url = live_or_skip!();
    let id = unique();
    let topic = format!("duckle-test/{id}/q");
    let source = json!({ "url": url, "topics": topic, "clientId": format!("duckle-{id}"), "persistentSession": true, "waitMs": 1000 });
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let opened = read(&engine, source.clone(), &out);
    assert_eq!(opened.status, "ok", "{:?}", opened.error);
    assert_eq!(rows(&out).len(), 0, "nothing sent yet: this run only opens the session");
    let sent = publish(
        &engine,
        &format!("SELECT '{topic}' AS topic, 'm' || i AS body FROM input, range(1, 4) t(i)"),
        json!({ "url": url, "topicColumn": "topic", "payloadColumn": "body" }),
    );
    assert_eq!(sent.status, "ok", "{:?}", sent.error);
    let back = read(&engine, source.clone(), &out);
    assert_eq!(back.status, "ok", "{:?}", back.error);
    let payloads: Vec<Value> = rows(&out).iter().map(|r| r["payload"].clone()).collect();
    assert_eq!(payloads, [json!("m1"), json!("m2"), json!("m3")], "held by the broker while no run was connected");
    let again = read(&engine, source, &out);
    assert_eq!(again.status, "ok", "{:?}", again.error);
    assert_eq!(rows(&out).len(), 0, "each was acknowledged as it was read");
}
