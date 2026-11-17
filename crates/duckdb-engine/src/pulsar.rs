//! Apache Pulsar over its WebSocket API: a topic read through the reader
//! endpoint, rows produced through the producer endpoint. The broker serves the
//! API on its HTTP port and speaks JSON, so neither a Pulsar client library nor
//! protoc is needed.

use crate::*;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use tokio_tungstenite::tungstenite::Message;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// The handshake for one of Pulsar's WebSocket endpoints: the address checked
/// against the network allowlist, and the token, if any, as a bearer.
fn pulsar_request(
    url: &str,
    target: &plan::PulsarTarget,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    crate::policy::outbound_host_allowed(url)?;
    let headers: Vec<(String, String)> =
        target.token.iter().map(|t| ("Authorization".to_string(), format!("Bearer {t}"))).collect();
    crate::connectors::websocket_request(url, &headers)
}

/// A message id as Pulsar's WebSocket API writes one: the message's
/// MessageIdData (ledger, entry, and a batch size of 0) in base64.
fn pulsar_message_id(ledger: u64, entry: u64) -> String {
    fn varint(mut n: u64, out: &mut Vec<u8>) {
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }
    let mut bytes = vec![0x08];
    varint(ledger, &mut bytes);
    bytes.push(0x10);
    varint(entry, &mut bytes);
    bytes.extend([0x30, 0x00]);
    B64.encode(bytes)
}

/// Where a read with Resume on starts after: the last message id saved, or
/// `earliest` when a first read pinned an empty topic. Another topic's place
/// reads as none.
fn read_pulsar_position(path: &std::path::Path, topic_path: &str) -> Option<String> {
    let state: JsonValue = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    if state.get("topic").and_then(JsonValue::as_str) != Some(topic_path) {
        return None;
    }
    state.get("message_id").and_then(JsonValue::as_str).filter(|s| !s.is_empty()).map(str::to_string)
}

/// The id of the topic's last message, for a first read from the newest that
/// must keep its place even if it reads nothing; `earliest` while the topic is
/// empty or not made yet, since then everything after now is everything.
fn pulsar_tip(target: &plan::PulsarTarget) -> Result<String, EngineError> {
    let http = target.ws_url.replacen("ws", "http", 1);
    let url = format!("{http}/admin/v2/{}/lastMessageId", target.topic_path);
    let mut req = crate::tls::http_agent().get(&url);
    if let Some(token) = &target.token {
        req = req.set("Authorization", &format!("Bearer {token}"));
    }
    let last: JsonValue = match req.call() {
        Ok(r) => r.into_json().map_err(|e| EngineError::Query(format!("pulsar: {url}: {e}")))?,
        Err(ureq::Error::Status(404, _)) => return Ok("earliest".to_string()),
        Err(ureq::Error::Status(code, r)) => {
            let body: JsonValue = r.into_json().unwrap_or_default();
            let said = body.get("reason").and_then(JsonValue::as_str).unwrap_or("no reason given");
            return Err(EngineError::Query(format!("pulsar: {url} answered HTTP {code}: {said}")));
        }
        Err(e) => return Err(EngineError::Query(format!("pulsar: {url}: {e}"))),
    };
    let ledger = last.get("ledgerId").and_then(JsonValue::as_i64).unwrap_or(-1);
    let entry = last.get("entryId").and_then(JsonValue::as_i64).unwrap_or(-1);
    Ok(if ledger < 0 || entry < 0 { "earliest".to_string() } else { pulsar_message_id(ledger as u64, entry as u64) })
}

fn runtime() -> Result<tokio::runtime::Runtime, EngineError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| EngineError::Query(format!("pulsar: tokio rt: {e}")))
}

impl DuckdbEngine {
    pub(crate) fn run_pulsar_source(
        &self,
        db: &Path,
        spec: &plan::PulsarSourceSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let state_path =
            if spec.track_position { crate::connectors::incremental_state_path(pipeline_name, &spec.node_id) } else { None };
        let prior = state_path.as_deref().and_then(crate::read_state_snapshot);
        // Where this read starts after. A saved place wins. A first read with
        // Resume on, from the newest, pins the tip now, so that a run that finds
        // nothing still keeps its place for the next one.
        let start = match state_path.as_deref().and_then(|p| read_pulsar_position(p, &spec.target.topic_path)) {
            Some(saved) => saved,
            None if spec.earliest => "earliest".to_string(),
            None if spec.track_position => pulsar_tip(&spec.target)?,
            None => "latest".to_string(),
        };
        let url = format!(
            "{}/ws/v2/reader/{}?messageId={}",
            spec.target.ws_url,
            spec.target.topic_path,
            urlencode_simple(&start)
        );
        let request = pulsar_request(&url, &spec.target).map_err(|e| EngineError::Query(format!("pulsar: {e}")))?;
        let cancel = self.cancel.clone();
        let read: Result<(Vec<JsonValue>, Option<String>), String> = runtime()?.block_on(async {
            let (mut ws, _) = tokio_tungstenite::connect_async(request).await.map_err(|e| format!("{url}: {e}"))?;
            let mut rows = Vec::new();
            let mut last = None;
            while (rows.len() as u64) < spec.max_messages {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let frame = match tokio::time::timeout(std::time::Duration::from_millis(spec.idle_ms), ws.next()).await {
                    Err(_) => break, // quiet for the idle timeout
                    Ok(None) | Ok(Some(Ok(Message::Close(_)))) => break,
                    Ok(Some(Err(e))) => return Err(format!("{url}: {e}")),
                    Ok(Some(Ok(Message::Text(t)))) => t,
                    Ok(Some(Ok(_))) => continue,
                };
                let m: JsonValue = serde_json::from_str(&frame).map_err(|e| format!("{url}: a message that is not JSON: {e}"))?;
                let Some(id) = m.get("messageId").and_then(JsonValue::as_str).map(str::to_string) else { continue };
                // The reader sends more only as the messages it sent are
                // acknowledged.
                ws.send(Message::Text(serde_json::json!({ "messageId": id }).to_string().into()))
                    .await
                    .map_err(|e| format!("{url}: {e}"))?;
                let payload = m.get("payload").and_then(JsonValue::as_str).and_then(|p| B64.decode(p).ok()).unwrap_or_default();
                rows.push(serde_json::json!({
                    "message_id": id,
                    "key": m.get("key").cloned().unwrap_or(JsonValue::Null),
                    "value": String::from_utf8_lossy(&payload),
                    "publish_time": m.get("publishTime").cloned().unwrap_or(JsonValue::Null),
                    "properties": m.get("properties").map(|p| p.to_string()).unwrap_or_default(),
                }));
                last = Some(id);
            }
            let _ = ws.close(None).await;
            Ok((rows, last))
        });
        let (rows, last) = match read {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("pulsar: {e}"))),
        };
        let columns: Vec<duckle_metadata::Column> = ["message_id", "key", "value", "publish_time", "properties"]
            .iter()
            .map(|name| duckle_metadata::Column {
                tags: Vec::new(),
                name: name.to_string(),
                data_type: duckle_metadata::DataType::String,
                nullable: true,
                primary_key: None,
                format: None,
            })
            .collect();
        materialize_jsonobjects_as_table_typed(&self.bin, db, &spec.node_id, &rows, Some(&columns))?;
        // Queued, not written: it lands only if the whole run succeeds, so a run
        // that fails downstream reads these messages again (at least once). It
        // is written when nothing was read too, for the place a first read pinned.
        if let Some(path) = state_path {
            pending.push(crate::PendingWrite::state(
                path,
                serde_json::json!({ "topic": spec.target.topic_path, "message_id": last.unwrap_or(start) }),
                prior,
            ));
        }
        Ok(format!("pulsar: read {} message(s) from {} into {}", rows.len(), spec.target.topic_path, spec.node_id))
    }

    pub(crate) fn run_pulsar_sink(&self, db: &Path, spec: &plan::PulsarSinkSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(Some(db), &format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view)))?;
        // Every message is made before any is sent, so a row without its key
        // column fails the run with nothing produced.
        let mut messages = Vec::with_capacity(rows.len());
        for (n, row) in rows.iter().enumerate() {
            let mut message = serde_json::json!({ "payload": B64.encode(row.to_string()), "context": n.to_string() });
            if let Some(column) = &spec.key_column {
                match row.get(column) {
                    None => {
                        return Err(EngineError::Query(format!(
                            "pulsar: row {} has no column {column} to key its message by",
                            n + 1
                        )))
                    }
                    Some(JsonValue::Null) => {}
                    Some(JsonValue::String(s)) => message["key"] = JsonValue::String(s.clone()),
                    Some(other) => message["key"] = JsonValue::String(other.to_string()),
                }
            }
            messages.push(message);
        }
        if messages.is_empty() {
            return Ok(format!("pulsar: 0 rows to produce into {}", spec.target.topic_path));
        }
        let url = format!("{}/ws/v2/producer/{}", spec.target.ws_url, spec.target.topic_path);
        let request = pulsar_request(&url, &spec.target).map_err(|e| EngineError::Query(format!("pulsar: {e}")))?;
        let cancel = self.cancel.clone();
        runtime()?
            .block_on(async {
                let (mut ws, _) = tokio_tungstenite::connect_async(request).await.map_err(|e| format!("{url}: {e}"))?;
                // A window of messages in flight at once, under the producer's own
                // limit on pending messages (1000 unless the broker sets another).
                for window in messages.chunks(500) {
                    if cancel.load(Ordering::Relaxed) {
                        return Err("cancelled".to_string());
                    }
                    for message in window {
                        ws.send(Message::Text(message.to_string().into())).await.map_err(|e| format!("{url}: {e}"))?;
                    }
                    let mut acknowledged = 0;
                    while acknowledged < window.len() {
                        let frame = match tokio::time::timeout(std::time::Duration::from_secs(30), ws.next()).await {
                            Err(_) => return Err(format!("{url}: no acknowledgement within 30 s")),
                            Ok(None) | Ok(Some(Ok(Message::Close(_)))) => {
                                return Err(format!("{url}: closed before every message was acknowledged"))
                            }
                            Ok(Some(Err(e))) => return Err(format!("{url}: {e}")),
                            Ok(Some(Ok(Message::Text(t)))) => t,
                            Ok(Some(Ok(_))) => continue,
                        };
                        let ack: JsonValue = serde_json::from_str(&frame).map_err(|e| format!("{url}: {e}"))?;
                        if ack.get("result").and_then(JsonValue::as_str) != Some("ok") {
                            let row = ack.get("context").and_then(JsonValue::as_str).and_then(|c| c.parse::<usize>().ok());
                            let said = ack.get("errorMsg").and_then(JsonValue::as_str).unwrap_or("no reason given");
                            return Err(match row {
                                Some(n) => format!("row {} was refused: {said}", n + 1),
                                None => format!("a message was refused: {said}"),
                            });
                        }
                        acknowledged += 1;
                    }
                }
                let _ = ws.close(None).await;
                Ok(())
            })
            .map_err(|e| if e == "cancelled" { EngineError::Cancelled } else { EngineError::Query(format!("pulsar: {e}")) })?;
        Ok(format!("pulsar: produced {} message(s) into {}", messages.len(), spec.target.topic_path))
    }
}

#[cfg(test)]
mod tests {
    /// An id Pulsar itself handed out (ledger 11, entry 2), and one whose
    /// ledger takes two bytes, worked out apart from this code.
    #[test]
    fn a_message_id_is_written_as_pulsar_writes_one() {
        assert_eq!(super::pulsar_message_id(11, 2), "CAsQAjAA");
        assert_eq!(super::pulsar_message_id(300, 5), "CKwCEAUwAA==");
    }

    #[test]
    fn a_saved_place_is_used_only_for_its_own_topic() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("p.json");
        std::fs::write(&path, r#"{"topic":"persistent/public/default/orders","message_id":"CAsQAjAA"}"#).unwrap();
        assert_eq!(super::read_pulsar_position(&path, "persistent/public/default/orders").as_deref(), Some("CAsQAjAA"));
        assert_eq!(super::read_pulsar_position(&path, "persistent/public/default/shipments"), None);
        assert_eq!(super::read_pulsar_position(&tmp.path().join("absent.json"), "persistent/public/default/orders"), None);
    }
}
