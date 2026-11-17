//! Pinecone: every record of a serverless index's namespace, read by listing
//! its ids a page at a time and fetching each page's records. Pinecone has no
//! call that reads an index whole, and listing is for serverless indexes.

use crate::*;

/// The API version the calls are written for.
const API_VERSION: &str = "2025-01";

/// One GET against the index, its key and API version sent with it.
fn pinecone_get(agent: &ureq::Agent, spec: &plan::PineconeSourceSpec, path_and_query: &str) -> Result<JsonValue, EngineError> {
    let req = agent
        .get(&format!("{}{}", spec.index_url, path_and_query))
        .set("Api-Key", &spec.api_key)
        .set("X-Pinecone-API-Version", API_VERSION);
    match req.call() {
        Ok(r) => r.into_json().map_err(|e| EngineError::Query(format!("pinecone: {}: {e}", spec.index_url))),
        Err(ureq::Error::Status(code, r)) => {
            let body: JsonValue = r.into_json().unwrap_or_default();
            let said = body
                .pointer("/error/message")
                .or_else(|| body.get("message"))
                .and_then(JsonValue::as_str)
                .unwrap_or("no reason given")
                .trim_end_matches('.');
            let hint = match code {
                401 | 403 => " Check the API key.",
                404 => " Check the index host: the address the console shows for the index.",
                _ => "",
            };
            Err(EngineError::Query(format!("pinecone: {} answered HTTP {code}: {said}.{hint}", spec.index_url)))
        }
        Err(e) => Err(EngineError::Query(format!("pinecone: {}: {e}", spec.index_url))),
    }
}

/// A page of the namespace's ids, after `token` when there is one, and the
/// token for the page after it.
fn list_page(
    agent: &ureq::Agent,
    spec: &plan::PineconeSourceSpec,
    limit: u64,
    token: Option<&str>,
) -> Result<(Vec<String>, Option<String>), EngineError> {
    let mut query = format!("/vectors/list?namespace={}&limit={limit}", urlencode_simple(&spec.namespace));
    if let Some(prefix) = &spec.prefix {
        query.push_str(&format!("&prefix={}", urlencode_simple(prefix)));
    }
    if let Some(token) = token {
        query.push_str(&format!("&paginationToken={}", urlencode_simple(token)));
    }
    let page = pinecone_get(agent, spec, &query)?;
    let ids = page
        .get("vectors")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| v.get("id").and_then(JsonValue::as_str).map(str::to_string))
        .collect();
    let next = page.pointer("/pagination/next").and_then(JsonValue::as_str).filter(|t| !t.is_empty()).map(str::to_string);
    Ok((ids, next))
}

impl DuckdbEngine {
    pub(crate) fn run_pinecone_source(&self, db: &Path, spec: &plan::PineconeSourceSpec) -> Result<String, EngineError> {
        let agent = crate::tls::http_agent();
        let mut rows: Vec<JsonValue> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let (ids, next) = list_page(&agent, spec, spec.page_size, token.as_deref())?;
            if !ids.is_empty() {
                let wanted: String = ids.iter().map(|id| format!("&ids={}", urlencode_simple(id))).collect();
                let fetched = pinecone_get(
                    &agent,
                    spec,
                    &format!("/vectors/fetch?namespace={}{wanted}", urlencode_simple(&spec.namespace)),
                )?;
                for id in &ids {
                    // A record deleted between the list and the fetch is gone.
                    let Some(record) = fetched.get("vectors").and_then(|v| v.get(id)) else { continue };
                    let mut row = serde_json::Map::new();
                    // Metadata first, so a key named id cannot take the record's place.
                    if let Some(metadata) = record.get("metadata").and_then(JsonValue::as_object) {
                        row.extend(metadata.clone());
                    }
                    row.insert("id".into(), JsonValue::String(id.clone()));
                    if spec.with_vector {
                        row.insert("vector".into(), record.get("values").cloned().unwrap_or_default());
                    }
                    rows.push(JsonValue::Object(row));
                }
            }
            token = next;
            if token.is_none() {
                break;
            }
        }
        // A token left after the last page allowed may lead to more ids or to
        // none; one id more says which, rather than reading part of the index.
        if let Some(t) = &token {
            if !list_page(&agent, spec, 1, Some(t))?.0.is_empty() {
                return Err(EngineError::Query(format!(
                    "pinecone: read {} records in {} pages of {}, and more remain; raise Max pages",
                    rows.len(),
                    spec.max_pages,
                    spec.page_size
                )));
            }
        }
        let columns = [duckle_metadata::Column {
            tags: Vec::new(),
            name: "id".to_string(),
            data_type: duckle_metadata::DataType::String,
            nullable: true,
            primary_key: None,
            format: None,
        }];
        materialize_jsonobjects_as_table_typed(&self.bin, db, &spec.node_id, &rows, Some(&columns))?;
        Ok(format!("pinecone: read {} record(s) into {}", rows.len(), spec.node_id))
    }
}
