//! Azure Service Bus for src.servicebus and snk.servicebus, over the REST API,
//! signed with a SAS token made from the connection string the Azure portal
//! hands out: no Azure SDK.
//!
//! A read takes each message under a peek-lock and completes (deletes) it only
//! once the whole run has succeeded, so a run that fails leaves its messages to
//! be delivered again when their locks run out. Receive-and-delete takes them
//! off the queue as they are read instead.

use crate::*;

/// What one send call carries at most: a Standard-tier message's size.
const BATCH_BYTES: usize = 256 * 1024;
const RETRIES: u32 = 4;
/// How long a SAS token made for one call stays valid.
const TOKEN_SECS: i64 = 3600;

/// Messages a run took under peek-lock, to complete once it has succeeded.
#[derive(Debug, Clone)]
pub(crate) struct ServiceBusReceived {
    pub target: plan::ServiceBusTarget,
    /// Each message's lock URI, from the receive's Location header.
    pub locks: Vec<String>,
}

fn encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// A SAS token for `resource`, expiring at `expiry` (Unix seconds). The key is
/// used as the text it is, as Service Bus signs with it.
pub(crate) fn sas_token(resource: &str, key_name: &str, key: &str, expiry: i64) -> String {
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    let sr = encode(resource);
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key.as_bytes()).expect("HMAC takes a key of any length");
    mac.update(format!("{sr}\n{expiry}").as_bytes());
    let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    format!("SharedAccessSignature sr={sr}&sig={}&se={expiry}&skn={}", encode(&sig), encode(key_name))
}

fn authorization(t: &plan::ServiceBusTarget) -> String {
    sas_token(&t.resource(), &t.key_name, &t.key, chrono::Utc::now().timestamp() + TOKEN_SECS)
}

/// The reason in a Service Bus error body: `<Error><Code>..</Code><Detail>..</Detail></Error>`.
fn detail(body: &str) -> String {
    let inner = body.split_once("<Detail>").and_then(|(_, rest)| rest.split_once("</Detail>")).map(|(d, _)| d.trim());
    match inner {
        Some(d) if !d.is_empty() => d.to_string(),
        _ => body.lines().next().unwrap_or("").trim().to_string(),
    }
}

/// One call, asked again while Service Bus says it is busy (429, 500, 503).
fn call(
    engine: &DuckdbEngine,
    t: &plan::ServiceBusTarget,
    method: &str,
    url: &str,
    content_type: Option<&str>,
    body: &str,
) -> Result<ureq::Response, EngineError> {
    let mut attempt = 0;
    loop {
        engine.check_cancelled()?;
        let mut req = crate::tls::http_agent().request(method, url).set("Authorization", &authorization(t));
        if let Some(ct) = content_type {
            req = req.set("Content-Type", ct);
        }
        match req.send_string(body) {
            Ok(r) => return Ok(r),
            Err(ureq::Error::Status(code, _)) if matches!(code, 429 | 500 | 503) && attempt < RETRIES => {
                engine.ai_sleep_cancellable(100 << attempt)?;
                attempt += 1;
            }
            Err(ureq::Error::Status(code, r)) => {
                let said = detail(&r.into_string().unwrap_or_default());
                return Err(EngineError::Query(format!("servicebus: {method} {url} was refused (HTTP {code}): {said}")));
            }
            Err(e) => return Err(EngineError::Query(format!("servicebus: {method} {url}: {e}"))),
        }
    }
}

const COLUMNS: &str = "'message_id': 'VARCHAR', 'body': 'VARCHAR', 'subject': 'VARCHAR', 'session_id': 'VARCHAR', \
    'sequence_number': 'VARCHAR', 'delivery_count': 'VARCHAR', 'enqueued_at': 'VARCHAR', 'content_type': 'VARCHAR'";
const SELECT_LIST: &str = "\"message_id\", \"body\", \"subject\", \"session_id\", \
    TRY_CAST(\"sequence_number\" AS BIGINT) AS \"sequence_number\", TRY_CAST(\"delivery_count\" AS BIGINT) AS \"delivery_count\", \
    TRY_CAST(\"enqueued_at\" AS TIMESTAMPTZ) AS \"enqueued_at\", \"content_type\"";

/// A received message as a row of text, from its BrokerProperties header.
fn message_row(props: &JsonValue, body: String, content_type: Option<&str>) -> JsonValue {
    let text = |k: &str| match props.get(k) {
        Some(JsonValue::String(s)) => Some(s.clone()),
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(other.to_string()),
    };
    // EnqueuedTimeUtc is an RFC 1123 date ("Thu, 19 Nov 2026 10:00:00 GMT").
    let enqueued = text("EnqueuedTimeUtc")
        .and_then(|d| chrono::DateTime::parse_from_rfc2822(&d).ok())
        .map(|d| d.to_rfc3339());
    serde_json::json!({
        "message_id": text("MessageId"),
        "body": body,
        "subject": text("Label"),
        "session_id": text("SessionId"),
        "sequence_number": text("SequenceNumber"),
        "delivery_count": text("DeliveryCount"),
        "enqueued_at": enqueued,
        "content_type": content_type,
    })
}

/// Complete what a successful run took under peek-lock. A message that could
/// not be completed (its lock ran out, say) is delivered again, which is said.
pub(crate) fn complete_received(engine: &DuckdbEngine, received: &ServiceBusReceived) -> Result<(), String> {
    let mut refused: Vec<String> = Vec::new();
    for lock in &received.locks {
        if let Err(e) = call(engine, &received.target, "DELETE", lock, None, "") {
            refused.push(e.to_string());
        }
    }
    if refused.is_empty() {
        return Ok(());
    }
    refused.dedup();
    Err(format!(
        "{} message(s) read from {} could not be completed, so they will be delivered again: {}",
        refused.len(),
        received.target.entity,
        refused.join("; ")
    ))
}

impl DuckdbEngine {
    /// src.servicebus: one message a call until the queue has none to give or
    /// Max messages is reached. `peek` is a preview or a probe: it reads one
    /// message and puts it straight back, completing nothing.
    pub(crate) fn run_servicebus_source(
        &self,
        db: &Path,
        spec: &plan::ServiceBusSourceSpec,
        peek: bool,
        received: &mut Vec<ServiceBusReceived>,
    ) -> Result<String, EngineError> {
        let t = &spec.target;
        let head = format!("{}/{}/messages/head?timeout={}", t.base_url, t.receive_path(), spec.wait_seconds);
        let destructive = spec.receive_and_delete && !peek;
        let limit = if peek { 1 } else { spec.max_messages };
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut locks: Vec<String> = Vec::new();
        let mut count = 0_usize;
        while count < limit {
            let resp = call(self, t, if destructive { "DELETE" } else { "POST" }, &head, None, "")?;
            if resp.status() == 204 {
                break;
            }
            let props: JsonValue = resp
                .header("BrokerProperties")
                .and_then(|h| serde_json::from_str(h).ok())
                .unwrap_or(JsonValue::Null);
            let lock = resp.header("Location").map(str::to_string);
            let content_type = resp.header("Content-Type").map(str::to_string);
            let body = resp.into_string().map_err(|e| EngineError::Query(format!("servicebus: message body: {e}")))?;
            writer.write_row(&message_row(&props, body, content_type.as_deref()))?;
            count += 1;
            if !destructive {
                let lock = lock.ok_or_else(|| {
                    EngineError::Query("servicebus: a peek-locked message came without its lock (no Location header)".into())
                })?;
                locks.push(lock);
            }
        }
        writer.finalize_typed(&self.bin, db, &spec.node_id, COLUMNS, SELECT_LIST)?;
        if peek {
            // Put back what a preview looked at, rather than leave it locked.
            for lock in &locks {
                let _ = call(self, t, "PUT", lock, None, "");
            }
        } else if !locks.is_empty() {
            received.push(ServiceBusReceived { target: t.clone(), locks });
        }
        Ok(format!("servicebus: received {count} message(s) from {} into {}", t.receive_path(), spec.node_id))
    }

    /// snk.servicebus: each upstream row as one message, sent in batches of at
    /// most 256 KiB. Every message is built first, so a row missing a value
    /// sends nothing.
    pub(crate) fn run_servicebus_sink(&self, db: &Path, spec: &plan::ServiceBusSinkSpec) -> Result<String, EngineError> {
        let rows = self.run_rows(Some(db), &format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view)))?;
        let text = |v: Option<&JsonValue>| match v {
            Some(JsonValue::String(s)) => Some(s.clone()),
            Some(JsonValue::Null) | None => None,
            Some(other) => Some(other.to_string()),
        };
        let needed = |row: &JsonValue, i: usize, column: &str, what: &str| -> Result<String, EngineError> {
            text(row.get(column)).ok_or_else(|| {
                EngineError::Query(format!("servicebus: row {} has no value in the {what} column '{column}'", i + 1))
            })
        };
        let mut messages: Vec<(JsonValue, usize)> = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let body = match &spec.body_column {
                Some(c) => needed(row, i, c, "body")?,
                None => row.to_string(),
            };
            let mut props = serde_json::Map::new();
            if let Some(c) = &spec.message_id_column {
                props.insert("MessageId".into(), JsonValue::String(needed(row, i, c, "message id")?));
            }
            if let Some(c) = &spec.session_id_column {
                props.insert("SessionId".into(), JsonValue::String(needed(row, i, c, "session id")?));
            }
            let size = body.len() + 64;
            messages.push((serde_json::json!({ "Body": body, "BrokerProperties": props }), size));
        }
        let url = format!("{}/{}/messages", spec.target.base_url, spec.target.entity);
        let mut start = 0;
        while start < messages.len() {
            let (mut end, mut bytes) = (start, 0);
            while end < messages.len() && (end == start || bytes + messages[end].1 <= BATCH_BYTES) {
                bytes += messages[end].1;
                end += 1;
            }
            let batch: Vec<&JsonValue> = messages[start..end].iter().map(|(m, _)| m).collect();
            call(self, &spec.target, "POST", &url, Some("application/vnd.microsoft.servicebus.json"), &serde_json::to_string(&batch).unwrap_or_default())?;
            start = end;
        }
        Ok(format!("servicebus: sent {} message(s) to {}", messages.len(), spec.target.entity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sas_token_signs_the_encoded_resource_and_its_expiry() {
        let token = sas_token("https://acme.servicebus.windows.net/orders", "listen", "k3y", 1_800_000_000);
        // Recomputed by hand from the documented recipe.
        use base64::Engine as _;
        use hmac::{Hmac, KeyInit, Mac};
        let sr = "https%3A%2F%2Facme.servicebus.windows.net%2Forders";
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(b"k3y").unwrap();
        mac.update(format!("{sr}\n1800000000").as_bytes());
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let sig: String = url::form_urlencoded::byte_serialize(sig.as_bytes()).collect();
        assert_eq!(token, format!("SharedAccessSignature sr={sr}&sig={sig}&se=1800000000&skn=listen"));
    }

    #[test]
    fn an_error_body_gives_its_detail() {
        assert_eq!(detail("<Error><Code>401</Code><Detail>InvalidSignature: the token is wrong.</Detail></Error>"), "InvalidSignature: the token is wrong.");
        assert_eq!(detail("plain words\nmore"), "plain words");
    }

    #[test]
    fn a_message_becomes_a_row_of_text() {
        let props = serde_json::json!({"MessageId": "m1", "Label": "order", "SequenceNumber": 7, "DeliveryCount": 1,
                                       "EnqueuedTimeUtc": "Thu, 19 Nov 2026 10:00:00 GMT"});
        let row = message_row(&props, "hi".into(), Some("application/json"));
        assert_eq!(row["enqueued_at"], serde_json::json!("2026-11-19T10:00:00+00:00"));
        assert_eq!(row["sequence_number"], serde_json::json!("7"));
        assert_eq!(row["session_id"], JsonValue::Null);
    }
}
