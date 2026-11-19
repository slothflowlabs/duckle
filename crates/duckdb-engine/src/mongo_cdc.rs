//! src.mongodb.cdc: every change MongoDB records for one collection - inserts,
//! updates, replaces, deletes - read through a change stream with the driver
//! src.mongodb uses. The resume token is saved only when the run succeeds, so a
//! run that fails reads the same changes again. A change stream starts where it
//! is opened: the first run reads what happens from then on and saves where it
//! got to, so later runs miss nothing in between.
//!
//! Each change is a row of the same columns whatever the documents hold, the
//! document itself as JSON: a collection has no schema, and a run with no
//! changes must have the columns a run with changes has.

use crate::*;
use mongodb::bson::{Bson, Document};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::FullDocumentType;

const COLUMNS: &str = "'_op': 'VARCHAR', '_token': 'VARCHAR', '_ts': 'VARCHAR', '_id': 'VARCHAR', \
    'document': 'VARCHAR', 'updated': 'VARCHAR', 'removed': 'VARCHAR'";
const SELECT_LIST: &str = "\"_op\", \"_token\", to_timestamp(TRY_CAST(\"_ts\" AS BIGINT)) AS \"_ts\", \
    CAST(\"_id\" AS JSON) AS \"_id\", CAST(\"document\" AS JSON) AS \"document\", \
    CAST(\"updated\" AS JSON) AS \"updated\", CAST(\"removed\" AS JSON) AS \"removed\"";

/// A BSON value as relaxed extended JSON text: numbers and strings as they are,
/// a date as an ISO string, an ObjectId as {"$oid": ...}.
fn ext_json(b: Bson) -> String {
    b.into_relaxed_extjson().to_string()
}

fn op_name(op: &OperationType) -> String {
    match op {
        OperationType::Insert => "insert",
        OperationType::Update => "update",
        OperationType::Replace => "replace",
        OperationType::Delete => "delete",
        OperationType::Drop => "drop",
        OperationType::Rename => "rename",
        OperationType::DropDatabase => "dropDatabase",
        OperationType::Invalidate => "invalidate",
        OperationType::Other(name) => return name.clone(),
        // A kind a later driver adds: by its own name.
        other => return format!("{other:?}").to_ascii_lowercase(),
    }
    .to_string()
}

/// The resume token as the JSON text the state file keeps.
pub(crate) fn token_text(t: &ResumeToken) -> Result<String, String> {
    mongodb::bson::to_bson(t).map(ext_json).map_err(|e| format!("resume token: {e}"))
}

/// The resume token back from the state file's text.
pub(crate) fn token_from(text: &str) -> Result<ResumeToken, String> {
    let json: JsonValue = serde_json::from_str(text).map_err(|e| format!("resume token: {e}"))?;
    let bson = Bson::try_from(json).map_err(|e| format!("resume token: {e}"))?;
    mongodb::bson::from_bson(bson).map_err(|e| format!("resume token: {e}"))
}

/// A change as a row of text, typed by SELECT_LIST.
fn event_row(e: &ChangeStreamEvent<Document>) -> JsonValue {
    let token = mongodb::bson::to_bson(&e.id).ok().and_then(|b| match b {
        Bson::Document(d) => d.get_str("_data").ok().map(str::to_string),
        _ => None,
    });
    serde_json::json!({
        "_op": op_name(&e.operation_type),
        "_token": token,
        "_ts": e.cluster_time.map(|t| t.time.to_string()),
        "_id": e.document_key.as_ref().and_then(|k| k.get("_id")).cloned().map(ext_json),
        "document": e.full_document.clone().map(|d| ext_json(Bson::Document(d))),
        "updated": e.update_description.as_ref().map(|u| ext_json(Bson::Document(u.updated_fields.clone()))),
        "removed": e.update_description.as_ref().map(|u| JsonValue::from(u.removed_fields.clone()).to_string()),
    })
}

/// A driver error, with what to do about the two a change stream meets that
/// say nothing useful on their own.
fn explain(e: mongodb::error::Error) -> EngineError {
    let code = match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => Some(c.code),
        _ => None,
    };
    let text = e.to_string();
    let message = match code {
        // ChangeStreamHistoryLost: the oplog rolled over past the saved token.
        Some(286) => "the changes after the position the last run saved are no longer in the oplog, which has \
                      rolled over since. Load the collection again, then clear this node's saved position \
                      (duckle-runner backfill clear, or the Backfill panel) to start from now"
            .to_string(),
        _ if code == Some(40573) || text.contains("only supported on replica sets") => {
            "change streams need a replica set or a sharded cluster, and this server is a standalone".to_string()
        }
        _ => text,
    };
    EngineError::Query(format!("src.mongodb.cdc: {message}"))
}

impl DuckdbEngine {
    pub(crate) fn run_mongo_cdc(
        &self,
        db: &Path,
        spec: &plan::MongoCdcSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let state_path = crate::connectors::incremental_state_path(pipeline_name, &spec.node_id).ok_or_else(|| {
            EngineError::Config(
                "src.mongodb.cdc: needs a workspace to keep its position in (DUCKLE_WORKSPACE); without one every run \
                 would start where it opened the stream and miss what happened between runs"
                    .into(),
            )
        })?;
        let prior = crate::read_state_snapshot(&state_path);
        let saved = match prior.as_deref().map(serde_json::from_str::<JsonValue>) {
            Some(Ok(state)) => match state.get("resumeToken") {
                Some(t) => Some(token_from(&t.to_string()).map_err(|e| EngineError::Query(format!("src.mongodb.cdc: {e}")))?),
                None => None,
            },
            _ => None,
        };
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("src.mongodb.cdc: tokio rt: {e}")))?;
        let (count, token) = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&spec.uri).await.map_err(explain)?;
            let collection = client.database(&spec.database).collection::<Document>(&spec.collection);
            // Each read waits this long for a change before the run calls it done.
            let mut watch = collection.watch().max_await_time(std::time::Duration::from_millis(spec.wait_ms));
            if spec.full_document {
                watch = watch.full_document(FullDocumentType::UpdateLookup);
            }
            if let Some(t) = saved.clone() {
                watch = watch.start_after(t);
            }
            let mut stream = watch.await.map_err(explain)?;
            let mut count = 0_u64;
            while count < spec.max_events {
                self.check_cancelled()?;
                let Some(event) = stream.next_if_any().await.map_err(explain)? else { break };
                writer.write_row(&event_row(&event))?;
                count += 1;
                // The collection was dropped or renamed: the stream is over.
                if matches!(event.operation_type, OperationType::Invalidate) {
                    break;
                }
            }
            // The stream's token, as the change streams spec has the driver keep
            // it: the last change returned while its batch lasts, then the
            // server's own position past the batch. So stopping at the limit
            // resumes at the next change, and a run with none saves where it got to.
            Ok::<_, EngineError>((count, stream.resume_token()))
        })?;
        writer.finalize_typed(&self.bin, db, &spec.node_id, COLUMNS, SELECT_LIST)?;
        if let Some(t) = token {
            let text = token_text(&t).map_err(|e| EngineError::Query(format!("src.mongodb.cdc: {e}")))?;
            let token_json: JsonValue = serde_json::from_str(&text).unwrap_or(JsonValue::Null);
            pending.push(crate::PendingWrite::state(
                state_path,
                serde_json::json!({ "resumeToken": token_json, "collection": format!("{}.{}", spec.database, spec.collection) }),
                prior,
            ));
        }
        Ok(format!("mongodb.cdc: read {count} change(s) of {}.{} into {}", spec.database, spec.collection, spec.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resume_token_comes_back_from_the_state_file_as_it_went_in() {
        let token: ResumeToken = mongodb::bson::from_bson(Bson::Document(mongodb::bson::doc! { "_data": "8263A1B2C3000000012B" })).unwrap();
        let text = token_text(&token).unwrap();
        assert_eq!(text, "{\"_data\":\"8263A1B2C3000000012B\"}");
        assert_eq!(token_from(&text).unwrap(), token);
        assert!(token_from("not json").is_err());
    }

    #[test]
    fn each_operation_keeps_its_name() {
        assert_eq!(op_name(&OperationType::Insert), "insert");
        assert_eq!(op_name(&OperationType::Replace), "replace");
        assert_eq!(op_name(&OperationType::DropDatabase), "dropDatabase");
        assert_eq!(op_name(&OperationType::Other("modify".into())), "modify");
    }
}
