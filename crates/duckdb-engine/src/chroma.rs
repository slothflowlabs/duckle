//! Chroma: a collection's records read page by page, and rows upserted into
//! one, over Chroma's HTTP API (v2), on a server of its own or Chroma Cloud.
//!
//! Chroma finds a collection by name and takes record calls by its id, so each
//! run asks for the id first. It computes no embeddings on the server: a record
//! is written with its own.

use crate::*;

/// One call to Chroma, `path` after `/api/v2`. Chroma Cloud takes its API key
/// in `x-chroma-token`; a server of one's own takes none.
fn chroma_call(
    agent: &ureq::Agent,
    target: &plan::ChromaTarget,
    method: &str,
    path: &str,
    body: Option<&JsonValue>,
) -> Result<JsonValue, EngineError> {
    let mut req = agent.request(method, &format!("{}/api/v2{}", target.endpoint, path));
    if let Some(key) = &target.api_key {
        req = req.set("x-chroma-token", key);
    }
    let answer = match body {
        Some(b) => req.send_json(b),
        None => req.call(),
    };
    match answer {
        Ok(r) => Ok(r.into_json::<JsonValue>().unwrap_or(JsonValue::Null)),
        Err(ureq::Error::Status(code, r)) => {
            let body: JsonValue = r.into_json().unwrap_or_default();
            let said = body.get("message").and_then(JsonValue::as_str).unwrap_or("no reason given");
            Err(EngineError::Query(format!("chroma: {} answered HTTP {code}: {said}", target.endpoint)))
        }
        Err(e) => Err(EngineError::Query(format!("chroma: {}: {e}", target.endpoint))),
    }
}

/// The path of the target's collections, under its tenant and database.
fn collections(target: &plan::ChromaTarget) -> String {
    format!(
        "/tenants/{}/databases/{}/collections",
        urlencode_simple(&target.tenant),
        urlencode_simple(&target.database)
    )
}

/// The id a collection's record calls take.
fn collection_id(answer: &JsonValue, target: &plan::ChromaTarget) -> Result<String, EngineError> {
    answer.get("id").and_then(JsonValue::as_str).map(str::to_string).ok_or_else(|| {
        EngineError::Query(format!("chroma: no id came back for collection {}", target.collection))
    })
}

impl DuckdbEngine {
    pub(crate) fn run_chroma_source(&self, db: &Path, spec: &plan::ChromaSourceSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let agent = crate::tls::http_agent();
        let base = collections(&spec.target);
        let found = chroma_call(&agent, &spec.target, "GET", &format!("{base}/{}", urlencode_simple(&spec.target.collection)), None)?;
        let records = format!("{base}/{}/get", collection_id(&found, &spec.target)?);
        let mut include = vec!["documents", "metadatas"];
        if spec.with_vector {
            include.push("embeddings");
        }
        let page = |offset: u64, limit: u64| -> Result<JsonValue, EngineError> {
            let mut body = serde_json::json!({ "limit": limit, "offset": offset, "include": include });
            if let Some(filter) = &spec.filter {
                body["where"] = filter.clone();
            }
            chroma_call(&agent, &spec.target, "POST", &records, Some(&body))
        };
        let mut rows: Vec<JsonValue> = Vec::new();
        let mut full = false;
        for n in 0..spec.max_pages {
            self.check_cancelled()?;
            let got = page(n * spec.page_size, spec.page_size)?;
            let ids = got.get("ids").and_then(JsonValue::as_array).cloned().unwrap_or_default();
            for (i, id) in ids.iter().enumerate() {
                let mut row = serde_json::Map::new();
                // Metadata first, so a key named like a record's own field
                // cannot take its place.
                if let Some(metadata) = got.pointer(&format!("/metadatas/{i}")).and_then(JsonValue::as_object) {
                    row.extend(metadata.clone());
                }
                row.insert("id".into(), id.clone());
                row.insert("document".into(), got.pointer(&format!("/documents/{i}")).cloned().unwrap_or_default());
                if spec.with_vector {
                    row.insert("vector".into(), got.pointer(&format!("/embeddings/{i}")).cloned().unwrap_or_default());
                }
                rows.push(JsonValue::Object(row));
            }
            full = ids.len() as u64 == spec.page_size;
            if !full {
                break;
            }
        }
        // A full last page may be the collection's end or the page cap's: ask
        // for one record more to know which, rather than read part of it.
        let more = full && page(rows.len() as u64, 1)?.get("ids").and_then(JsonValue::as_array).is_some_and(|i| !i.is_empty());
        if more {
            return Err(EngineError::Query(format!(
                "chroma: read {} records of {} in {} pages of {}, and more remain; raise Max pages or Page size",
                rows.len(),
                spec.target.collection,
                spec.max_pages,
                spec.page_size
            )));
        }
        // An empty collection still reads as a table, of the columns every
        // record has.
        let columns: Vec<duckle_metadata::Column> = ["id", "document"]
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
        Ok(format!("chroma: read {} record(s) of {} into {}", rows.len(), spec.target.collection, spec.node_id))
    }

    pub(crate) fn run_chroma_sink(&self, db: &Path, spec: &plan::ChromaSinkSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(Some(db), &format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view)))?;
        // Every record is made before anything is sent, so a row Chroma could
        // not hold fails the run with nothing written.
        let mut ids = Vec::with_capacity(rows.len());
        let mut embeddings = Vec::with_capacity(rows.len());
        let mut documents = Vec::with_capacity(rows.len());
        let mut metadatas = Vec::with_capacity(rows.len());
        for (n, row) in rows.iter().enumerate() {
            let empty = serde_json::Map::new();
            let row = row.as_object().unwrap_or(&empty);
            ids.push(match row.get(&spec.id_column) {
                Some(JsonValue::String(s)) if !s.is_empty() => JsonValue::String(s.clone()),
                Some(v @ (JsonValue::Number(_) | JsonValue::Bool(_))) => JsonValue::String(v.to_string()),
                _ => {
                    return Err(EngineError::Query(format!(
                        "chroma: row {} has no {} to be its id",
                        n + 1,
                        spec.id_column
                    )))
                }
            });
            embeddings.push(match row.get(&spec.vector_column) {
                Some(JsonValue::Array(v)) if !v.is_empty() && v.iter().all(JsonValue::is_number) => JsonValue::Array(v.clone()),
                _ => {
                    return Err(EngineError::Query(format!(
                        "chroma: row {} has no embedding: {} is not a list of numbers, and Chroma takes no record without one",
                        n + 1,
                        spec.vector_column
                    )))
                }
            });
            documents.push(match spec.document_column.as_ref().and_then(|c| row.get(c)) {
                None | Some(JsonValue::Null) => JsonValue::Null,
                Some(JsonValue::String(s)) => JsonValue::String(s.clone()),
                Some(other) => JsonValue::String(other.to_string()),
            });
            // The other columns are metadata: text, numbers and true/false as
            // they are, a null left out, and a list or a record as its JSON text.
            let metadata: serde_json::Map<String, JsonValue> = row
                .iter()
                .filter(|(k, v)| {
                    **k != spec.id_column && **k != spec.vector_column && Some(*k) != spec.document_column.as_ref() && !v.is_null()
                })
                .map(|(k, v)| {
                    let v = match v {
                        JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_) => v.clone(),
                        other => JsonValue::String(other.to_string()),
                    };
                    (k.clone(), v)
                })
                .collect();
            metadatas.push(if metadata.is_empty() { JsonValue::Null } else { JsonValue::Object(metadata) });
        }
        if ids.is_empty() {
            return Ok(format!("chroma: 0 rows to upsert into {}", spec.target.collection));
        }
        let agent = crate::tls::http_agent();
        let base = collections(&spec.target);
        let made = chroma_call(
            &agent,
            &spec.target,
            "POST",
            &base,
            Some(&serde_json::json!({ "name": spec.target.collection, "get_or_create": true })),
        )?;
        let upsert = format!("{base}/{}/upsert", collection_id(&made, &spec.target)?);
        // The server says how many records a call may carry.
        let most = chroma_call(&agent, &spec.target, "GET", "/pre-flight-checks", None)?
            .get("max_batch_size")
            .and_then(JsonValue::as_u64)
            .map(|n| n as usize)
            .unwrap_or(spec.batch_size);
        let batch = spec.batch_size.min(most).max(1);
        let mut start = 0;
        while start < ids.len() {
            self.check_cancelled()?;
            let end = (start + batch).min(ids.len());
            let mut body = serde_json::json!({
                "ids": &ids[start..end],
                "embeddings": &embeddings[start..end],
                "metadatas": &metadatas[start..end],
            });
            if spec.document_column.is_some() {
                body["documents"] = JsonValue::from(documents[start..end].to_vec());
            }
            chroma_call(&agent, &spec.target, "POST", &upsert, Some(&body))?;
            start = end;
        }
        Ok(format!("chroma: upserted {} record(s) into {}", ids.len(), spec.target.collection))
    }
}
