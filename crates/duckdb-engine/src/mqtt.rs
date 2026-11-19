//! MQTT 3.1.1 for src.mqtt and snk.mqtt: a small client on a TCP socket, or on
//! Duckle's own TLS for mqtts://, so no MQTT library and no second TLS stack.
//! It speaks the part of the protocol a batch read or write needs: CONNECT,
//! SUBSCRIBE, PUBLISH at QoS 0 and 1 with PUBACK, PINGREQ and DISCONNECT.

use crate::*;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

/// How long a connect, a CONNACK, a SUBACK, a PUBACK or the rest of a packet
/// already begun may take.
const STEP: Duration = Duration::from_secs(10);
/// The keep-alive asked for. A read with nothing to acknowledge pings at half of it.
const KEEP_ALIVE: u16 = 60;
/// QoS 1 messages a sink sends before it waits for their PUBACKs.
const WINDOW: usize = 100;

/// A connection, in the clear or over TLS.
enum Wire {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Wire {
    fn socket(&self) -> &TcpStream {
        match self {
            Wire::Plain(s) => s,
            Wire::Tls(t) => &t.sock,
        }
    }
}

impl Read for Wire {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Wire::Plain(s) => s.read(buf),
            Wire::Tls(t) => t.read(buf),
        }
    }
}

impl Write for Wire {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Wire::Plain(s) => s.write(buf),
            Wire::Tls(t) => t.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Wire::Plain(s) => s.flush(),
            Wire::Tls(t) => t.flush(),
        }
    }
}

/// MQTT's variable-length integer, for a packet's remaining length.
pub(crate) fn remaining_length(mut n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            return out;
        }
    }
}

/// A string field: two bytes of length, then the bytes.
fn field(bytes: &[u8]) -> Vec<u8> {
    let mut out = (bytes.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(bytes);
    out
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    out.extend(remaining_length(body.len()));
    out.extend_from_slice(body);
    out
}

/// The CONNECT packet: MQTT 3.1.1, a clean session or the one kept for this
/// client id, and the sign-in if there is one.
pub(crate) fn connect_packet(client_id: &str, clean: bool, user: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let mut flags = 0u8;
    if clean {
        flags |= 0x02;
    }
    if user.is_some() {
        flags |= 0x80;
    }
    if password.is_some() {
        flags |= 0x40;
    }
    let mut body = field(b"MQTT");
    body.push(4);
    body.push(flags);
    body.extend_from_slice(&KEEP_ALIVE.to_be_bytes());
    body.extend(field(client_id.as_bytes()));
    if let Some(u) = user {
        body.extend(field(u.as_bytes()));
    }
    if let Some(p) = password {
        body.extend(field(p.as_bytes()));
    }
    packet(0x10, &body)
}

/// One whole packet: its first byte and its body. Only the wait for the first
/// byte may time out; the rest is read under STEP, so a short poll never
/// leaves half a packet behind.
fn read_packet(w: &mut Wire) -> std::io::Result<(u8, Vec<u8>)> {
    let mut first = [0u8; 1];
    w.read_exact(&mut first)?;
    w.socket().set_read_timeout(Some(STEP))?;
    let (mut len, mut shift) = (0usize, 0u32);
    loop {
        let mut b = [0u8; 1];
        w.read_exact(&mut b)?;
        len |= ((b[0] & 0x7F) as usize) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 21 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "a packet length longer than MQTT allows"));
        }
    }
    let mut body = vec![0u8; len];
    w.read_exact(&mut body)?;
    Ok((first[0], body))
}

/// What a CONNACK's return code means.
pub(crate) fn connack_refusal(code: u8) -> &'static str {
    match code {
        1 => "it does not speak MQTT 3.1.1",
        2 => "it refused the client id",
        3 => "the MQTT service is unavailable",
        4 => "the user name or password is wrong",
        5 => "the client is not authorized",
        _ => "it gave a reason MQTT 3.1.1 does not define",
    }
}

fn timed_out(e: &std::io::Error) -> bool {
    matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
}

/// A client id for a connection that keeps no session: 23 characters, the
/// most every broker must take.
fn fresh_id() -> String {
    format!("duckle-{}", &uuid::Uuid::new_v4().simple().to_string()[..16])
}

/// Connect, over TLS if the URL said so, and be accepted.
fn open(c: &plan::MqttConn, client_id: &str, clean: bool) -> Result<Wire, EngineError> {
    let fail = |what: &str, e: &dyn std::fmt::Display| EngineError::Query(format!("mqtt {}:{}: {what}: {e}", c.host, c.port));
    let addr = (c.host.as_str(), c.port)
        .to_socket_addrs()
        .map_err(|e| fail("resolve", &e))?
        .next()
        .ok_or_else(|| fail("resolve", &"no address"))?;
    let tcp = TcpStream::connect_timeout(&addr, STEP).map_err(|e| fail("connect", &e))?;
    tcp.set_read_timeout(Some(STEP)).map_err(|e| fail("connect", &e))?;
    tcp.set_nodelay(true).ok();
    let mut wire = if c.tls {
        let name = rustls::pki_types::ServerName::try_from(c.host.clone()).map_err(|e| fail("TLS", &e))?;
        let conn = rustls::ClientConnection::new(std::sync::Arc::new(crate::tls::build_client_config()), name)
            .map_err(|e| fail("TLS", &e))?;
        let mut tls = rustls::StreamOwned::new(conn, tcp);
        // Finished here, so a refused certificate is reported as that.
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).map_err(|e| fail("TLS handshake", &e))?;
        }
        Wire::Tls(Box::new(tls))
    } else {
        Wire::Plain(tcp)
    };
    wire.write_all(&connect_packet(client_id, clean, c.user.as_deref(), c.password.as_deref()))
        .map_err(|e| fail("CONNECT", &e))?;
    let (header, body) = read_packet(&mut wire).map_err(|e| fail("waiting for CONNACK", &e))?;
    if header != 0x20 || body.len() < 2 {
        return Err(fail("CONNECT", &format!("the broker answered with packet type {} instead of CONNACK", header >> 4)));
    }
    if body[1] != 0 {
        return Err(fail("the broker refused the connection", &connack_refusal(body[1])));
    }
    Ok(wire)
}

/// A payload as text: itself when it is UTF-8, base64 otherwise, and which.
pub(crate) fn payload_text(bytes: &[u8]) -> (String, &'static str) {
    match std::str::from_utf8(bytes) {
        Ok(s) => (s.to_string(), "utf-8"),
        Err(_) => {
            use base64::Engine as _;
            (base64::engine::general_purpose::STANDARD.encode(bytes), "base64")
        }
    }
}

/// A PUBLISH packet's QoS, topic, packet id (QoS 1 and 2) and payload.
fn parse_publish(header: u8, body: &[u8]) -> Option<(u8, String, Option<u16>, &[u8])> {
    let qos = (header >> 1) & 0x03;
    let len = u16::from_be_bytes([*body.first()?, *body.get(1)?]) as usize;
    let topic = std::str::from_utf8(body.get(2..2 + len)?).ok()?.to_string();
    let mut at = 2 + len;
    let id = match qos {
        0 => None,
        1 | 2 => {
            let id = u16::from_be_bytes([*body.get(at)?, *body.get(at + 1)?]);
            at += 2;
            Some(id)
        }
        _ => return None,
    };
    Some((qos, topic, id, body.get(at..)?))
}

const COLUMNS: &str = "'topic': 'VARCHAR', 'payload': 'VARCHAR', 'encoding': 'VARCHAR', 'qos': 'VARCHAR', \
    'retained': 'VARCHAR', 'received_at': 'VARCHAR'";
const SELECT_LIST: &str = "\"topic\", \"payload\", \"encoding\", CAST(\"qos\" AS INTEGER) AS \"qos\", \
    CAST(\"retained\" AS BOOLEAN) AS \"retained\", CAST(\"received_at\" AS TIMESTAMPTZ) AS \"received_at\"";

impl DuckdbEngine {
    /// src.mqtt: subscribe, then take messages until Max messages, or until a
    /// wait passes with none. A QoS 1 message is acknowledged as it is read. A
    /// preview or a probe connects as a fresh client with a clean session, so
    /// it takes nothing from a session kept for the runs.
    pub(crate) fn run_mqtt_source(&self, db: &Path, spec: &plan::MqttSourceSpec, peek: bool) -> Result<String, EngineError> {
        let c = &spec.conn;
        let fail = |what: &str, e: &dyn std::fmt::Display| EngineError::Query(format!("mqtt {}:{}: {what}: {e}", c.host, c.port));
        let keep = spec.persistent_session && !peek;
        // A clean connect under the kept session's own id would end that session.
        let client_id = if spec.persistent_session && peek { fresh_id() } else { c.client_id.clone().unwrap_or_else(fresh_id) };
        let mut wire = open(c, &client_id, !keep)?;
        let mut body = 1u16.to_be_bytes().to_vec();
        for t in &spec.topics {
            body.extend(field(t.as_bytes()));
            body.push(spec.qos);
        }
        wire.write_all(&packet(0x82, &body)).map_err(|e| fail("SUBSCRIBE", &e))?;
        let mut last_sent = Instant::now();
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let (mut count, mut subscribed, mut heard) = (0usize, false, Instant::now());
        while count < spec.max_messages {
            self.check_cancelled()?;
            // Until the SUBACK the broker has STEP to confirm; after it, the
            // read ends once the wait passes with no message.
            let wait = if subscribed { Duration::from_millis(spec.wait_ms) } else { STEP };
            let left = (heard + wait).saturating_duration_since(Instant::now());
            if left.is_zero() {
                if subscribed {
                    break;
                }
                return Err(fail("SUBSCRIBE", &"the broker never confirmed the subscription"));
            }
            if last_sent.elapsed() >= Duration::from_secs(u64::from(KEEP_ALIVE) / 2) {
                wire.write_all(&[0xC0, 0x00]).map_err(|e| fail("PINGREQ", &e))?;
                last_sent = Instant::now();
            }
            // Short polls, so a cancelled run is noticed within a moment.
            wire.socket().set_read_timeout(Some(left.min(Duration::from_millis(250)))).map_err(|e| fail("read", &e))?;
            let (header, body) = match read_packet(&mut wire) {
                Ok(p) => p,
                Err(e) if timed_out(&e) => continue,
                Err(e) => return Err(fail("read", &e)),
            };
            match header >> 4 {
                // SUBACK: 0x80 for each filter the broker refused.
                9 => {
                    if let Some(i) = body.iter().skip(2).position(|&b| b == 0x80) {
                        let topic = spec.topics.get(i).map(String::as_str).unwrap_or("?");
                        return Err(fail("the broker refused the subscription to", &topic));
                    }
                    subscribed = true;
                    heard = Instant::now();
                }
                3 => {
                    let Some((qos, topic, id, payload)) = parse_publish(header, &body) else {
                        return Err(fail("read", &"a PUBLISH packet MQTT 3.1.1 does not allow"));
                    };
                    if qos == 2 {
                        return Err(fail("read", &"the broker sent a QoS 2 message to a subscription at QoS 1 or below"));
                    }
                    if let Some(id) = id {
                        wire.write_all(&packet(0x40, &id.to_be_bytes())).map_err(|e| fail("PUBACK", &e))?;
                        last_sent = Instant::now();
                    }
                    let (text, encoding) = payload_text(payload);
                    writer.write_row(&serde_json::json!({
                        "topic": topic,
                        "payload": text,
                        "encoding": encoding,
                        "qos": qos.to_string(),
                        "retained": (header & 0x01 == 1).to_string(),
                        "received_at": chrono::Utc::now().to_rfc3339(),
                    }))?;
                    count += 1;
                    heard = Instant::now();
                }
                // PINGRESP, and anything else the broker may send.
                _ => {}
            }
        }
        let _ = wire.write_all(&[0xE0, 0x00]);
        let _ = wire.flush();
        writer.finalize_typed(&self.bin, db, &spec.node_id, COLUMNS, SELECT_LIST)?;
        Ok(format!("mqtt: received {count} message(s) into {}", spec.node_id))
    }

    /// snk.mqtt: each upstream row as one message. Every message is built
    /// before the first is sent, so a row with no topic sends nothing. At QoS 1
    /// the broker's PUBACKs are awaited a window at a time, and the run fails
    /// if any message went unacknowledged.
    pub(crate) fn run_mqtt_sink(&self, db: &Path, spec: &plan::MqttSinkSpec) -> Result<String, EngineError> {
        let c = &spec.conn;
        let fail = |what: &str, e: &dyn std::fmt::Display| EngineError::Query(format!("mqtt {}:{}: {what}: {e}", c.host, c.port));
        let rows = self.run_rows(Some(db), &format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view)))?;
        let text = |v: Option<&JsonValue>| match v {
            Some(JsonValue::String(s)) => Some(s.clone()),
            Some(JsonValue::Null) | None => None,
            Some(other) => Some(other.to_string()),
        };
        let mut messages: Vec<(String, String)> = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let topic = match &spec.topic_column {
                Some(col) => text(row.get(col)).ok_or_else(|| {
                    EngineError::Query(format!("mqtt: row {} has no value in the topic column '{col}'", i + 1))
                })?,
                None => spec.topic.clone().unwrap_or_default(),
            };
            if let Some(why) = plan::mqtt_topic_problem(&topic) {
                return Err(EngineError::Query(format!("mqtt: row {}: {why}", i + 1)));
            }
            let payload = match &spec.payload_column {
                Some(col) => text(row.get(col)).unwrap_or_default(),
                None => row.to_string(),
            };
            messages.push((topic, payload));
        }
        let client_id = c.client_id.clone().unwrap_or_else(fresh_id);
        let mut wire = open(c, &client_id, true)?;
        let header = 0x30 | (spec.qos << 1) | u8::from(spec.retain);
        let mut sent = 0usize;
        for window in messages.chunks(WINDOW) {
            self.check_cancelled()?;
            let mut out = Vec::new();
            let mut waiting = std::collections::BTreeSet::new();
            for (k, (topic, payload)) in window.iter().enumerate() {
                let mut body = field(topic.as_bytes());
                if spec.qos > 0 {
                    // Ids 1 to WINDOW, free again once the window is acknowledged.
                    let id = (k + 1) as u16;
                    body.extend_from_slice(&id.to_be_bytes());
                    waiting.insert(id);
                }
                body.extend_from_slice(payload.as_bytes());
                out.extend(packet(header, &body));
            }
            wire.write_all(&out).and_then(|_| wire.flush()).map_err(|e| fail("PUBLISH", &e))?;
            wire.socket().set_read_timeout(Some(STEP)).map_err(|e| fail("PUBLISH", &e))?;
            while !waiting.is_empty() {
                let (h, b) = read_packet(&mut wire).map_err(|e| {
                    let rows = format!("rows {} to {} were sent and not all acknowledged", sent + 1, sent + window.len());
                    fail(&rows, &e)
                })?;
                if h >> 4 == 4 && b.len() >= 2 {
                    waiting.remove(&u16::from_be_bytes([b[0], b[1]]));
                }
            }
            sent += window.len();
        }
        let _ = wire.write_all(&[0xE0, 0x00]);
        let _ = wire.flush();
        Ok(format!("mqtt: published {} message(s)", messages.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_remaining_length_takes_one_to_four_bytes() {
        assert_eq!(remaining_length(0), [0x00]);
        assert_eq!(remaining_length(127), [0x7F]);
        assert_eq!(remaining_length(128), [0x80, 0x01]);
        assert_eq!(remaining_length(16_383), [0xFF, 0x7F]);
        assert_eq!(remaining_length(2_097_152), [0x80, 0x80, 0x80, 0x01]);
    }

    #[test]
    fn a_connect_packet_says_311_and_what_session_it_wants() {
        let p = connect_packet("c1", true, Some("ada"), Some("pw"));
        assert_eq!(&p[..2], [0x10, 23], "6 + 1 + 1 + 2 bytes of header, then 4, 5 and 4 of id, user and password");
        assert_eq!(&p[2..10], [0, 4, b'M', b'Q', b'T', b'T', 4, 0x80 | 0x40 | 0x02]);
        assert_eq!(&p[10..12], [0, 60]);
        assert_eq!(&p[12..16], [0, 2, b'c', b'1']);
        let kept = connect_packet("c1", false, None, None);
        assert_eq!(kept[9], 0, "no clean session, no sign-in");
    }

    #[test]
    fn a_publish_gives_its_qos_topic_id_and_payload() {
        let body = [&field(b"a/b")[..], &[0, 7], b"hi"].concat();
        assert_eq!(parse_publish(0x32, &body), Some((1, "a/b".to_string(), Some(7), &b"hi"[..])));
        let body = [&field(b"a/b")[..], b"hi"].concat();
        assert_eq!(parse_publish(0x31, &body), Some((0, "a/b".to_string(), None, &b"hi"[..])));
        assert_eq!(parse_publish(0x36, &body), None, "QoS 3 does not exist");
    }

    #[test]
    fn a_payload_is_text_or_base64_and_says_which() {
        assert_eq!(payload_text(b"{\"t\": 21}"), ("{\"t\": 21}".to_string(), "utf-8"));
        assert_eq!(payload_text(&[0xFF, 0x00]), ("/wA=".to_string(), "base64"));
    }

    #[test]
    fn each_connack_refusal_is_named() {
        assert!(connack_refusal(4).contains("password"));
        assert!(connack_refusal(5).contains("not authorized"));
        assert_eq!(fresh_id().len(), 23);
    }
}
