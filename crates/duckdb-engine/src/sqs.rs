//! Amazon SQS for src.sqs and snk.sqs, over SQS's JSON protocol signed with
//! SigV4, the way the Kinesis connectors call Kinesis: no AWS SDK.
//!
//! A read is at-least-once: received messages stay hidden for the visibility
//! timeout and are deleted only once the whole run has succeeded, so a run that
//! fails leaves them to be delivered again.

use crate::util::{aws_json_content_type, aws_sigv4_sign};
use crate::*;

/// What a send call may carry: ten messages, 256 KiB between them.
const BATCH: usize = 10;
const BATCH_BYTES: usize = 256 * 1024;
const RETRIES: u32 = 4;

/// Messages a run received, to delete once it has succeeded.
#[derive(Debug, Clone)]
pub(crate) struct SqsReceived {
    pub queue: plan::SqsQueue,
    pub receipts: Vec<String>,
}

/// Where a queue's calls go and the host they are signed for: the queue URL's
/// origin, which for AWS is the region's SQS endpoint and for a local SQS
/// (ElasticMQ, LocalStack) is the emulator.
fn origin(queue_url: &str) -> Result<(String, String), EngineError> {
    let parsed = url::Url::parse(queue_url).map_err(|e| EngineError::Config(format!("sqs: queue URL {queue_url}: {e}")))?;
    let host = parsed.host_str().unwrap_or_default().to_string();
    let host = match parsed.port() {
        Some(p) => format!("{host}:{p}"),
        None => host,
    };
    Ok((format!("{}://{host}/", parsed.scheme()), host))
}

/// One SQS action with its JSON body, and SQS's answer.
fn sqs_call(queue: &plan::SqsQueue, action: &str, body: &JsonValue) -> Result<JsonValue, EngineError> {
    let (url, host) = origin(&queue.queue_url)?;
    let target = format!("AmazonSQS.{action}");
    let text = body.to_string();
    let now = chrono::Utc::now();
    let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let signed = aws_sigv4_sign(
        "POST",
        "/",
        "",
        &host,
        &datetime,
        &date,
        "sqs",
        &queue.region,
        &target,
        &text,
        &queue.access_key_id,
        &queue.secret_access_key,
        queue.session_token.as_deref(),
    );
    let mut req = crate::tls::http_agent()
        .post(&url)
        .set("Host", &host)
        .set("Content-Type", aws_json_content_type("sqs"))
        .set("X-Amz-Date", &datetime)
        .set("X-Amz-Target", &target)
        .set("Authorization", &signed.authorization);
    if let Some(token) = &queue.session_token {
        req = req.set("X-Amz-Security-Token", token);
    }
    match req.send_string(&text) {
        Ok(r) => r.into_json().map_err(|e| EngineError::Query(format!("sqs: {action}: {e}"))),
        Err(ureq::Error::Status(code, r)) => {
            let said: JsonValue = r.into_json().unwrap_or_default();
            // "com.amazonaws.sqs#QueueDoesNotExist" -> QueueDoesNotExist
            let kind = said.get("__type").and_then(JsonValue::as_str).map(|t| t.rsplit('#').next().unwrap_or(t));
            let message = said.get("message").or_else(|| said.get("Message")).and_then(JsonValue::as_str);
            Err(EngineError::Query(format!(
                "sqs: {action} on {} was refused (HTTP {code}): {}: {}",
                queue.queue_url,
                kind.unwrap_or("no error type given"),
                message.unwrap_or("no reason given")
            )))
        }
        Err(e) => Err(EngineError::Query(format!("sqs: {action} on {}: {e}", queue.queue_url))),
    }
}

/// A message as a row: every field as text, typed by `SELECT_LIST`.
fn message_row(m: &JsonValue) -> JsonValue {
    let attribute = |name: &str| m.pointer(&format!("/Attributes/{name}")).cloned().unwrap_or(JsonValue::Null);
    let attributes: serde_json::Map<String, JsonValue> = m
        .get("MessageAttributes")
        .and_then(JsonValue::as_object)
        .map(|attrs| {
            attrs
                .iter()
                .map(|(name, a)| {
                    let value = a.get("StringValue").or_else(|| a.get("BinaryValue")).cloned().unwrap_or(JsonValue::Null);
                    (name.clone(), value)
                })
                .collect()
        })
        .unwrap_or_default();
    serde_json::json!({
        "message_id": m.get("MessageId").cloned().unwrap_or(JsonValue::Null),
        "body": m.get("Body").cloned().unwrap_or(JsonValue::Null),
        "sent_ms": attribute("SentTimestamp"),
        "receive_count": attribute("ApproximateReceiveCount"),
        "group_id": attribute("MessageGroupId"),
        "attributes": JsonValue::Object(attributes).to_string(),
    })
}

const COLUMNS: &str = "'message_id': 'VARCHAR', 'body': 'VARCHAR', 'sent_ms': 'VARCHAR', \
    'receive_count': 'VARCHAR', 'group_id': 'VARCHAR', 'attributes': 'VARCHAR'";
const SELECT_LIST: &str = "\"message_id\", \"body\", \
    to_timestamp(TRY_CAST(\"sent_ms\" AS BIGINT) / 1000.0) AS \"sent_at\", \
    TRY_CAST(\"receive_count\" AS BIGINT) AS \"receive_count\", \"group_id\", \
    CAST(\"attributes\" AS JSON) AS \"attributes\"";

/// A value as the text SQS carries: a string as it is, a number or a boolean
/// as written, and nothing for a null.
fn text(v: Option<&JsonValue>) -> Option<String> {
    match v {
        Some(JsonValue::String(s)) => Some(s.clone()),
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

/// Delete what a successful run received. The messages that could not be
/// deleted are named with SQS's reason; they will be delivered again.
pub(crate) fn delete_received(received: &SqsReceived) -> Result<(), String> {
    let mut refused: Vec<String> = Vec::new();
    for (n, chunk) in received.receipts.chunks(BATCH).enumerate() {
        let entries: Vec<JsonValue> = chunk
            .iter()
            .enumerate()
            .map(|(i, handle)| serde_json::json!({ "Id": (n * BATCH + i).to_string(), "ReceiptHandle": handle }))
            .collect();
        let body = serde_json::json!({ "QueueUrl": received.queue.queue_url, "Entries": entries });
        match sqs_call(&received.queue, "DeleteMessageBatch", &body) {
            Ok(answer) => {
                for f in answer.get("Failed").and_then(JsonValue::as_array).cloned().unwrap_or_default() {
                    refused.push(text(f.get("Code")).unwrap_or_else(|| "no reason given".into()));
                }
            }
            Err(e) => refused.extend(std::iter::repeat_n(e.to_string(), chunk.len())),
        }
    }
    if refused.is_empty() {
        return Ok(());
    }
    refused.dedup();
    Err(format!(
        "messages read from {} could not all be deleted, so they will be delivered again: {}",
        received.queue.queue_url,
        refused.join("; ")
    ))
}

impl DuckdbEngine {
    /// src.sqs: receive up to the most it is told to take, ten a call, until
    /// the queue has none to give. `peek` is a preview or a probe: one look,
    /// nothing hidden, nothing deleted.
    pub(crate) fn run_sqs_source(
        &self,
        db: &Path,
        spec: &plan::SqsSourceSpec,
        peek: bool,
        received: &mut Vec<SqsReceived>,
    ) -> Result<String, EngineError> {
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut receipts: Vec<String> = Vec::new();
        while receipts.len() < spec.max_messages {
            self.check_cancelled()?;
            let body = serde_json::json!({
                "QueueUrl": spec.queue.queue_url,
                "MaxNumberOfMessages": (spec.max_messages - receipts.len()).min(BATCH),
                "WaitTimeSeconds": spec.wait_seconds,
                "VisibilityTimeout": if peek { 0 } else { spec.visibility_timeout },
                "MessageSystemAttributeNames": ["All"],
                "MessageAttributeNames": ["All"],
            });
            let answer = sqs_call(&spec.queue, "ReceiveMessage", &body)?;
            let messages = answer.get("Messages").and_then(JsonValue::as_array).cloned().unwrap_or_default();
            if messages.is_empty() {
                break;
            }
            for m in &messages {
                writer.write_row(&message_row(m))?;
                receipts.push(text(m.get("ReceiptHandle")).unwrap_or_default());
            }
            // Left visible, the same messages would come straight back.
            if peek {
                break;
            }
        }
        let count = receipts.len();
        writer.finalize_typed(&self.bin, db, &spec.node_id, COLUMNS, SELECT_LIST)?;
        // Deleted after the run, and only after one that succeeded in full: a
        // preview or a probe never reaches that point.
        if !receipts.is_empty() {
            received.push(SqsReceived { queue: spec.queue.clone(), receipts });
        }
        Ok(format!("sqs: received {count} message(s) from {} into {}", spec.queue.queue_url, spec.node_id))
    }

    /// snk.sqs: each upstream row as one message, ten to a call. Every message
    /// is built first, so a row missing a value fails the run with nothing
    /// sent. What SQS fails on its own side is sent again; what it refuses as
    /// the sender's fault fails the run, naming the row.
    pub(crate) fn run_sqs_sink(&self, db: &Path, spec: &plan::SqsSinkSpec) -> Result<String, EngineError> {
        let rows = self.run_rows(Some(db), &format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view)))?;
        let needed = |row: &JsonValue, i: usize, column: &str, what: &str| -> Result<String, EngineError> {
            text(row.get(column)).ok_or_else(|| {
                EngineError::Query(format!("sqs: row {} has no value in the {what} column '{column}'", i + 1))
            })
        };
        // (entry, bytes it adds to a call)
        let mut entries: Vec<(JsonValue, usize)> = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let body = match &spec.body_column {
                Some(c) => needed(row, i, c, "body")?,
                None => row.to_string(),
            };
            let mut entry = serde_json::json!({ "Id": i.to_string(), "MessageBody": body });
            if let Some(c) = &spec.group_id_column {
                entry["MessageGroupId"] = JsonValue::String(needed(row, i, c, "message group")?);
            }
            if let Some(c) = &spec.deduplication_id_column {
                entry["MessageDeduplicationId"] = JsonValue::String(needed(row, i, c, "deduplication id")?);
            }
            let bytes = body.len();
            entries.push((entry, bytes));
        }
        let mut start = 0;
        while start < entries.len() {
            // As many as one call takes; a message over 256 KiB goes alone,
            // for SQS to take or refuse.
            let (mut end, mut bytes) = (start, 0);
            while end < entries.len() && end - start < BATCH && (end == start || bytes + entries[end].1 <= BATCH_BYTES) {
                bytes += entries[end].1;
                end += 1;
            }
            let mut pending: Vec<JsonValue> = entries[start..end].iter().map(|(e, _)| e.clone()).collect();
            let mut attempt = 0;
            while !pending.is_empty() {
                self.check_cancelled()?;
                let body = serde_json::json!({ "QueueUrl": spec.queue.queue_url, "Entries": pending });
                let answer = sqs_call(&spec.queue, "SendMessageBatch", &body)?;
                let failed = answer.get("Failed").and_then(JsonValue::as_array).cloned().unwrap_or_default();
                if let Some(f) = failed.iter().find(|f| f.get("SenderFault").and_then(JsonValue::as_bool) == Some(true)) {
                    let row = text(f.get("Id")).and_then(|id| id.parse::<usize>().ok()).map_or(0, |i| i + 1);
                    return Err(EngineError::Query(format!(
                        "sqs: {} refused row {row}: {}: {}",
                        spec.queue.queue_url,
                        text(f.get("Code")).unwrap_or_else(|| "no error code".into()),
                        text(f.get("Message")).unwrap_or_else(|| "no reason given".into())
                    )));
                }
                let again: std::collections::HashSet<String> = failed.iter().filter_map(|f| text(f.get("Id"))).collect();
                pending.retain(|e| text(e.get("Id")).is_some_and(|id| again.contains(&id)));
                if pending.is_empty() {
                    break;
                }
                if attempt == RETRIES {
                    return Err(EngineError::Query(format!(
                        "sqs: {} did not take {} message(s) after {} tries: {}",
                        spec.queue.queue_url,
                        pending.len(),
                        RETRIES + 1,
                        text(failed.first().and_then(|f| f.get("Code"))).unwrap_or_else(|| "no error code".into())
                    )));
                }
                self.ai_sleep_cancellable(100 << attempt)?;
                attempt += 1;
            }
            start = end;
        }
        Ok(format!("sqs: sent {} message(s) to {}", entries.len(), spec.queue.queue_url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_queue_is_called_at_its_own_origin() {
        let (url, host) = origin("https://sqs.eu-west-1.amazonaws.com/123456789012/orders").unwrap();
        assert_eq!((url.as_str(), host.as_str()), ("https://sqs.eu-west-1.amazonaws.com/", "sqs.eu-west-1.amazonaws.com"));
        let (url, host) = origin("http://localhost:9324/000000000000/orders").unwrap();
        assert_eq!((url.as_str(), host.as_str()), ("http://localhost:9324/", "localhost:9324"));
    }

    #[test]
    fn a_message_becomes_a_row_of_text() {
        let row = message_row(&serde_json::json!({
            "MessageId": "m1", "ReceiptHandle": "r1", "Body": "hi",
            "Attributes": {"SentTimestamp": "1763460000000", "ApproximateReceiveCount": "3", "MessageGroupId": "g"},
            "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "order"}, "raw": {"DataType": "Binary", "BinaryValue": "AAE="}},
        }));
        assert_eq!(
            row,
            serde_json::json!({"message_id": "m1", "body": "hi", "sent_ms": "1763460000000", "receive_count": "3",
                               "group_id": "g", "attributes": "{\"kind\":\"order\",\"raw\":\"AAE=\"}"})
        );
    }
}
