//! Engine utilities: secret collection/redaction for SQL export, procedural
//! step notes, XML/Avro/git parsing, glob matching, AWS SigV4 signing,
//! DynamoDB unwrap, a tiny HTTP reader, cosine similarity, prompt templating,
//! PII regexes and text chunking. Extracted from lib.rs; re-exported via
//! pub(crate) use util::* so crate:: paths are unchanged.

use crate::*;

/// True for a property key that holds a credential (case-insensitive
/// substring match), so its value should never appear in exported SQL.
pub fn is_secret_prop_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    [
        "password", "passwd", "secret", "token", "apikey", "api_key",
        "privatekey", "private_key", "accesskey", "access_key", "pat",
        "clientsecret", "client_secret", "connectionstring", "connection_string",
        "sas", "credential",
    ]
    .iter()
    .any(|needle| k.contains(needle))
}

/// A secret found in the pipeline: its plaintext VALUE and the named
/// placeholder that stands in for it in exported SQL (e.g. value
/// "sup3r" under prop key "password" -> placeholder "${DUCKLE_PASSWORD}").
pub(crate) struct Secret {
    value: String,
    placeholder: String,
}

/// Turn a secret prop key into an env-style placeholder name, e.g.
/// "password" -> "${DUCKLE_PASSWORD}", "client_secret" ->
/// "${DUCKLE_CLIENT_SECRET}", "apiKey" -> "${DUCKLE_API_KEY}". Non
/// alphanumeric characters become underscores; camelCase boundaries are
/// split so the result reads as a conventional env var.
pub(crate) fn secret_placeholder(key: &str) -> String {
    let mut out = String::from("DUCKLE_");
    let mut prev_lower = false;
    for ch in key.chars() {
        if ch.is_ascii_uppercase() && prev_lower {
            out.push('_');
        }
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
        prev_lower = ch.is_ascii_lowercase() || ch.is_ascii_digit();
    }
    format!("${{{}}}", out.trim_end_matches('_'))
}

/// Collect the plaintext secrets configured anywhere in the pipeline, so
/// they can be replaced in display-only SQL. Only strings of a few chars
/// or more are taken, to avoid redacting incidental short values that
/// collide with SQL tokens. Sorted longest-value-first so a value that
/// contains another is replaced first.
pub(crate) fn collect_secrets(doc: &PipelineDoc) -> Vec<Secret> {
    let mut out: Vec<Secret> = Vec::new();
    for node in &doc.nodes {
        if let Some(JsonValue::Object(props)) = node.data.properties.as_ref() {
            for (key, val) in props {
                if is_secret_prop_key(key) {
                    if let Some(s) = val.as_str() {
                        if s.len() >= 4 {
                            out.push(Secret {
                                value: s.to_string(),
                                placeholder: secret_placeholder(key),
                            });
                        }
                    }
                }
            }
        }
    }
    out.sort_by(|a, b| b.value.len().cmp(&a.value.len()));
    out.dedup_by(|a, b| a.value == b.value);
    out
}

/// Replace each known secret value in `sql` with its named placeholder
/// (e.g. ${DUCKLE_PASSWORD}), so the exported script stays structurally
/// valid and is safe to share - the user substitutes the real value at
/// run time. The export path can opt out of this entirely to emit raw
/// credentials (DUCKLE_EXPORT_INCLUDE_SECRETS=1).
pub(crate) fn redact_secret_values(sql: &str, secrets: &[Secret]) -> String {
    let mut out = sql.to_string();
    for secret in secrets {
        if out.contains(secret.value.as_str()) {
            out = out.replace(secret.value.as_str(), &secret.placeholder);
        }
    }
    out
}

/// A human-readable comment describing a stage that has no DuckDB SQL
/// (a driver source/sink or a ctl.* control step). Keeps the SQL export
/// complete + self-documenting instead of emitting a bare empty stage.
pub(crate) fn procedural_note(s: &plan::Stage) -> String {
    let cid = s.component_id.as_str();
    let body = if let Some(RuntimeSpec::RunJob { path, vars }) = s.runtime.as_ref() {
        if vars.is_empty() {
            format!("control step: runs sub-pipeline '{}' as a side effect", path)
        } else {
            format!(
                "control step: runs job '{}' with {} context var(s)",
                path,
                vars.len()
            )
        }
    } else if let Some(RuntimeSpec::Iterate { path, count }) = s.runtime.as_ref() {
        format!(
            "control step: runs sub-pipeline '{}' x{} (ctl.iterate)",
            path, count
        )
    } else if let Some(RuntimeSpec::Foreach(p)) = s.runtime.as_ref() {
        format!("control step: runs sub-pipeline '{}' once per upstream row (ctl.foreach)", p)
    } else if let Some(RuntimeSpec::Parallelize(spec)) = s.runtime.as_ref() {
        format!(
            "control step: runs {} downstream branch(es) in parallel",
            spec.branches.len()
        )
    } else if let Some(RuntimeSpec::InstallFallback(p)) = s.runtime.as_ref() {
        format!("control step: installs fallback pipeline '{}' (ctl.try)", p)
    } else if cid.starts_with("snk.") {
        match s.from.as_deref() {
            Some(from) => format!(
                "sink: '{}' connector writes rows from \"{}\" (runs in the Duckle runtime, no DuckDB SQL)",
                cid, from
            ),
            None => format!(
                "sink: '{}' connector (runs in the Duckle runtime, no DuckDB SQL)",
                cid
            ),
        }
    } else if cid.starts_with("src.") {
        format!(
            "source: '{}' connector fetches rows and materializes them as \"{}\" (runs in the Duckle runtime, no DuckDB SQL)",
            cid, s.node_id
        )
    } else if cid.starts_with("code.") {
        format!(
            "code step: '{}' transforms rows in the Duckle runtime (no DuckDB SQL)",
            cid
        )
    } else if cid.starts_with("xf.ai.") {
        format!(
            "AI step: '{}' processes rows in the Duckle runtime (no DuckDB SQL)",
            cid
        )
    } else {
        format!(
            "'{}' runs in the Duckle runtime (no DuckDB SQL)",
            cid
        )
    };
    format!("/* {} */", body)
}

/// Finalize an XML element being popped from the stack: convert it
/// to a JSON value, push to rows if its path matches row_path, and
/// merge it into its parent (multiple same-named children collapse
/// to an array). Standalone (not a method) so the borrow checker
/// doesn't complain about &mut stack + &mut rows at the same time.
pub(crate) fn xml_close_element(
    stack: &mut Vec<(String, serde_json::Map<String, JsonValue>, String)>,
    rows: &mut Vec<JsonValue>,
    row_path: &[String],
    name: &str,
    mut builder: serde_json::Map<String, JsonValue>,
    text: String,
) {
    let text_trimmed = text.trim().to_string();
    let value: JsonValue = if builder.is_empty() && !text_trimmed.is_empty() {
        JsonValue::String(text_trimmed)
    } else if builder.is_empty() {
        JsonValue::Null
    } else {
        if !text_trimmed.is_empty() {
            builder.insert("_text".into(), JsonValue::String(text_trimmed));
        }
        JsonValue::Object(builder)
    };

    // Check if (stack path + name) ends with row_path. Empty row_path
    // matches every element - useful for "every immediate child" type
    // use cases when combined with a single-segment path.
    let mut current_path: Vec<&str> = stack.iter().map(|(n, _, _)| n.as_str()).collect();
    current_path.push(name);
    // Compare element names ignoring namespace prefix on both sides
    // (`soap:Envelope` matches user's `Envelope` as well as their
    // `soap:Envelope`). The user can still preserve namespaces in
    // their row_path if they want exact-match against a single ns.
    fn local(name: &str) -> &str {
        match name.rfind(':') {
            Some(i) => &name[i + 1..],
            None => name,
        }
    }
    let matches = if row_path.is_empty() {
        // No filter - match every direct child of the root only, to
        // avoid emitting nested structures as separate rows.
        current_path.len() == 1
    } else {
        current_path.len() >= row_path.len()
            && current_path[current_path.len() - row_path.len()..]
                .iter()
                .zip(row_path.iter())
                .all(|(a, b)| local(a) == local(b.as_str()))
    };

    if matches {
        rows.push(value.clone());
    }

    if let Some((_, parent_builder, _)) = stack.last_mut() {
        match parent_builder.get_mut(name) {
            Some(JsonValue::Array(arr)) => arr.push(value),
            Some(existing) => {
                let prev = std::mem::replace(existing, JsonValue::Null);
                *existing = JsonValue::Array(vec![prev, value]);
            }
            None => {
                parent_builder.insert(name.to_string(), value);
            }
        }
    }
}

/// Parse `content` as XML and walk slash-separated `row_path` (e.g.
/// `library/books/book`). Each match becomes one row, with attributes
/// keyed `@name`, text content under `_text`, and nested children
/// nested as sub-objects. Shared between src.xml (file input) and the
/// XML response branch of src.rest / src.soap (in-memory string input).
pub(crate) fn walk_xml_to_rows(
    content: &str,
    row_path: &str,
    cancel: &Arc<AtomicBool>,
) -> Result<Vec<JsonValue>, EngineError> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(content);
    reader.config_mut().trim_text(true);
    let row_path_parts: Vec<String> = row_path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut stack: Vec<(String, serde_json::Map<String, JsonValue>, String)> = Vec::new();
    let mut rows: Vec<JsonValue> = Vec::new();
    let mut buf = Vec::new();
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(EngineError::Cancelled);
        }
        let event = reader
            .read_event_into(&mut buf)
            .map_err(|e| EngineError::Query(format!("xml: parse: {}", e)))?;
        match event {
            Event::Eof => break,
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let v = String::from_utf8_lossy(&attr.value).to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                stack.push((name, builder, String::new()));
            }
            Event::Empty(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let v = String::from_utf8_lossy(&attr.value).to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                xml_close_element(
                    &mut stack,
                    &mut rows,
                    &row_path_parts,
                    &name,
                    builder,
                    String::new(),
                );
            }
            Event::Text(e) => {
                let text = String::from_utf8_lossy(
                    e.unescape().unwrap_or_default().as_ref().as_bytes(),
                )
                .to_string();
                if let Some(last) = stack.last_mut() {
                    last.2.push_str(&text);
                }
            }
            Event::End(_) => {
                if let Some((name, builder, text)) = stack.pop() {
                    xml_close_element(&mut stack, &mut rows, &row_path_parts, &name, builder, text);
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(rows)
}

/// Convert a JSON value into an apache-avro Value matching the
/// shapes the inferred schemas can hold. Objects + arrays JSON-
/// stringify into a String field since the inferred schema treats
/// them as strings.
pub(crate) fn json_to_avro_value(v: &JsonValue) -> apache_avro::types::Value {
    use apache_avro::types::Value as A;
    match v {
        JsonValue::Null => A::Null,
        JsonValue::Bool(b) => A::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                A::Long(i)
            } else if let Some(f) = n.as_f64() {
                A::Double(f)
            } else {
                A::String(n.to_string())
            }
        }
        JsonValue::String(s) => A::String(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            A::String(serde_json::to_string(v).unwrap_or_default())
        }
    }
}

/// Infer an Avro JSON-schema type for a single JSON value. Used by
/// snk.avro when schemaJson isn't supplied. Numeric values get the
/// most-permissive numeric type (double); strings stay string;
/// booleans stay boolean; nulls become "null"; everything else
/// (objects, arrays) falls back to string with the JSON encoding.
pub(crate) fn infer_avro_field_type(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Null => JsonValue::String("null".into()),
        JsonValue::Bool(_) => JsonValue::String("boolean".into()),
        JsonValue::Number(n) => {
            if n.is_i64() {
                JsonValue::String("long".into())
            } else {
                JsonValue::String("double".into())
            }
        }
        JsonValue::String(_) => JsonValue::String("string".into()),
        JsonValue::Array(_) | JsonValue::Object(_) => JsonValue::String("string".into()),
    }
}

/// Parse `git log -z --pretty=format:%H%x09%h%x09%an%x09%ae%x09%ad%x09%s`
/// output. Records are NUL-separated; fields are TAB-separated. Subjects
/// may contain anything except NUL.
pub(crate) fn parse_git_log(bytes: &[u8]) -> Vec<JsonValue> {
    let mut out: Vec<JsonValue> = Vec::new();
    for rec in bytes.split(|b| *b == 0) {
        if rec.is_empty() {
            continue;
        }
        let s = String::from_utf8_lossy(rec);
        let parts: Vec<&str> = s.splitn(6, '\t').collect();
        if parts.len() < 6 {
            continue;
        }
        let mut row = serde_json::Map::new();
        row.insert("hash".into(), JsonValue::String(parts[0].to_string()));
        row.insert("short_hash".into(), JsonValue::String(parts[1].to_string()));
        row.insert(
            "author_name".into(),
            JsonValue::String(parts[2].to_string()),
        );
        row.insert(
            "author_email".into(),
            JsonValue::String(parts[3].to_string()),
        );
        row.insert("date".into(), JsonValue::String(parts[4].to_string()));
        row.insert("subject".into(), JsonValue::String(parts[5].to_string()));
        out.push(JsonValue::Object(row));
    }
    out
}

/// Tiny shell-style glob matcher for src.ftp's pattern filter.
/// Supports `*` (zero or more chars) and `?` (one char). No bracket
/// expressions, no escape - matches the common ETL `orders_*.csv`
/// shape without pulling in a glob crate.
pub(crate) fn glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    fn go(p: &[char], n: &[char]) -> bool {
        if p.is_empty() {
            return n.is_empty();
        }
        match p[0] {
            '*' => {
                // Skip consecutive stars, then try every split.
                let mut i = 1;
                while i < p.len() && p[i] == '*' {
                    i += 1;
                }
                if i == p.len() {
                    return true;
                }
                for j in 0..=n.len() {
                    if go(&p[i..], &n[j..]) {
                        return true;
                    }
                }
                false
            }
            '?' => !n.is_empty() && go(&p[1..], &n[1..]),
            c => !n.is_empty() && n[0] == c && go(&p[1..], &n[1..]),
        }
    }
    go(&p, &n)
}

/// Parse `git ls-tree -r -z --long <rev>` output. Records are NUL-
/// separated; each record is `<mode> <type> <hash> <size>\t<path>`.
pub(crate) fn parse_git_ls_tree(bytes: &[u8], max_rows: usize) -> Vec<JsonValue> {
    let mut out: Vec<JsonValue> = Vec::new();
    for rec in bytes.split(|b| *b == 0) {
        if rec.is_empty() {
            continue;
        }
        if out.len() >= max_rows {
            break;
        }
        let s = String::from_utf8_lossy(rec);
        let mut split = s.splitn(2, '\t');
        let meta = split.next().unwrap_or("");
        let path = split.next().unwrap_or("");
        let meta_parts: Vec<&str> = meta.split_whitespace().collect();
        if meta_parts.len() < 4 {
            continue;
        }
        let size: JsonValue = meta_parts[3]
            .parse::<i64>()
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null);
        let mut row = serde_json::Map::new();
        row.insert("mode".into(), JsonValue::String(meta_parts[0].to_string()));
        row.insert("type".into(), JsonValue::String(meta_parts[1].to_string()));
        row.insert("hash".into(), JsonValue::String(meta_parts[2].to_string()));
        row.insert("size".into(), size);
        row.insert("path".into(), JsonValue::String(path.to_string()));
        out.push(JsonValue::Object(row));
    }
    out
}

/// AWS SigV4 signed-headers bundle. We only need the Authorization
/// value; X-Amz-Date / X-Amz-Security-Token / Host are set on the
/// request separately so they show up in the canonical headers.
pub(crate) struct SigV4Signed {
    pub authorization: String,
}

/// Compute an AWS SigV4 v4 signature for a JSON-API style request
/// (DynamoDB, Kinesis, etc - the "x-amz-target" header is part of
/// the signed headers list). Returns the Authorization header value
/// to set on the request.
///
/// Steps mirror the AWS Signing Process exactly:
/// 1. Canonical request (method + path + query + canonical headers
///    + signed headers + hashed payload)
/// 2. String to sign (algorithm + datetime + scope + hashed canonical)
/// 3. Derive signing key (HMAC chain: date, region, service, "aws4_request")
/// 4. Sign string-to-sign with derived key
/// 5. Build authorization header
#[allow(clippy::too_many_arguments)]
pub(crate) fn aws_sigv4_sign(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    host: &str,
    amz_date: &str,
    short_date: &str,
    service: &str,
    region: &str,
    amz_target: &str,
    payload: &str,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
) -> SigV4Signed {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;
    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }
    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(key).expect("hmac");
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let sha256_hex = |s: &str| -> String { hex(&Sha256::digest(s.as_bytes())) };
    // 1. Canonical request. Headers must be sorted lexically.
    let mut canonical_headers: Vec<(String, String)> = vec![
        ("content-type".into(), "application/x-amz-json-1.0".into()),
        ("host".into(), host.to_string()),
        ("x-amz-date".into(), amz_date.to_string()),
        ("x-amz-target".into(), amz_target.to_string()),
    ];
    if let Some(tok) = session_token {
        canonical_headers.push(("x-amz-security-token".into(), tok.to_string()));
    }
    canonical_headers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_header_block: String = canonical_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();
    let signed_headers_list: String = canonical_headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let payload_hash = sha256_hex(payload);
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        canonical_uri,
        canonical_query,
        canonical_header_block,
        signed_headers_list,
        payload_hash
    );
    // 2. String to sign.
    let scope = format!("{}/{}/{}/aws4_request", short_date, region, service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        sha256_hex(&canonical_request)
    );
    // 3. Derive signing key.
    let k_secret = format!("AWS4{}", secret_access_key);
    let k_date = mac(k_secret.as_bytes(), short_date.as_bytes());
    let k_region = mac(&k_date, region.as_bytes());
    let k_service = mac(&k_region, service.as_bytes());
    let k_signing = mac(&k_service, b"aws4_request");
    // 4. Sign string-to-sign.
    let signature = hex(&mac(&k_signing, string_to_sign.as_bytes()));
    // 5. Authorization header.
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key_id, scope, signed_headers_list, signature
    );
    SigV4Signed { authorization }
}

/// Unwrap DynamoDB's typed-attribute representation into plain JSON.
/// {"S": "x"} -> "x"
/// {"N": "5"} -> 5 (number; falls back to string if not parseable)
/// {"BOOL": true} -> true
/// {"NULL": true} -> null
/// {"L": [...]} -> array (recursive)
/// {"M": {...}} -> object (recursive, attribute names as keys)
/// {"SS": ["a","b"]} -> ["a","b"]
/// {"NS": ["1","2"]} -> [1, 2]
/// Unknown shapes pass through unchanged.
pub(crate) fn unwrap_dynamodb_attrs(v: &JsonValue) -> JsonValue {
    let JsonValue::Object(obj) = v else {
        return v.clone();
    };
    // Top-level Items rows look like {col: {S: "x"}, col2: {N: "5"}}
    // - unwrap each value but keep the keys.
    let mut out = serde_json::Map::new();
    for (k, attr) in obj {
        out.insert(k.clone(), unwrap_dynamodb_value(attr));
    }
    JsonValue::Object(out)
}

pub(crate) fn unwrap_dynamodb_value(v: &JsonValue) -> JsonValue {
    let JsonValue::Object(o) = v else {
        return v.clone();
    };
    if o.len() != 1 {
        return v.clone();
    }
    let (tag, inner) = o.iter().next().unwrap();
    match tag.as_str() {
        "S" => inner.clone(),
        "N" => {
            if let JsonValue::String(s) = inner {
                if let Ok(i) = s.parse::<i64>() {
                    return JsonValue::from(i);
                }
                if let Ok(f) = s.parse::<f64>() {
                    return JsonValue::from(f);
                }
                inner.clone()
            } else {
                inner.clone()
            }
        }
        "BOOL" => inner.clone(),
        "NULL" => JsonValue::Null,
        "L" => {
            if let JsonValue::Array(arr) = inner {
                JsonValue::Array(arr.iter().map(unwrap_dynamodb_value).collect())
            } else {
                inner.clone()
            }
        }
        "M" => {
            if let JsonValue::Object(m) = inner {
                let mut out = serde_json::Map::new();
                for (k, attr) in m {
                    out.insert(k.clone(), unwrap_dynamodb_value(attr));
                }
                JsonValue::Object(out)
            } else {
                inner.clone()
            }
        }
        "SS" => inner.clone(),
        "NS" => {
            if let JsonValue::Array(arr) = inner {
                JsonValue::Array(
                    arr.iter()
                        .map(|x| match x {
                            JsonValue::String(s) => s
                                .parse::<i64>()
                                .map(JsonValue::from)
                                .or_else(|_| s.parse::<f64>().map(JsonValue::from))
                                .unwrap_or_else(|_| x.clone()),
                            other => other.clone(),
                        })
                        .collect(),
                )
            } else {
                inner.clone()
            }
        }
        _ => v.clone(),
    }
}

/// Read one HTTP/1.x request off `stream` and return (method, path,
/// headers, body). Tiny ad-hoc parser - good enough for webhook
/// receivers from well-behaved clients. Reads until Content-Length
/// bytes of body have arrived; rejects requests with no
/// Content-Length when there's a non-empty body indication.
pub(crate) fn read_http_request(
    stream: &mut std::net::TcpStream,
) -> Result<(String, String, Vec<(String, String)>, Vec<u8>), String> {
    use std::io::Read;
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    // Read until we see end-of-headers (\r\n\r\n).
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        if buf.len() > 1_048_576 {
            return Err("request too large".into());
        }
        match stream.read(&mut chunk) {
            Ok(0) => return Err("connection closed before headers".into()),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => return Err(format!("read: {}", e)),
        }
    }
    let split_at = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "no header/body split".to_string())?;
    let head = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or_else(|| "empty request".to_string())?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    // Body: any bytes we've already read past the header split + more
    // until we have content_length bytes total.
    let mut body: Vec<u8> = buf[split_at + 4..].to_vec();
    while body.len() < content_length {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    body.truncate(content_length);
    Ok((method, path, headers, body))
}

/// Cosine similarity between two equal-length float vectors. Used by
/// xf.ai.dedupe. Returns 0.0 if either vector is empty / lengths
/// mismatch / either has zero magnitude (all-zero vector).
pub(crate) fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Render a prompt template by substituting `{column_name}` tokens
/// with the row's value for that column. Missing columns or non-
/// scalar values become empty strings. Used by xf.ai.llm and
/// xf.ai.classify.
pub(crate) fn render_prompt_template(template: &str, row: &JsonValue) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    let obj = row.as_object();
    while let Some(c) = chars.next() {
        if c != '{' {
            out.push(c);
            continue;
        }
        let mut key = String::new();
        let mut closed = false;
        for k in chars.by_ref() {
            if k == '}' {
                closed = true;
                break;
            }
            key.push(k);
        }
        if !closed {
            // Unclosed `{...` -> emit literally so user sees mistake.
            out.push('{');
            out.push_str(&key);
            continue;
        }
        let val = obj
            .and_then(|m| m.get(&key))
            .map(|v| match v {
                JsonValue::String(s) => s.clone(),
                JsonValue::Null => String::new(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        out.push_str(&val);
    }
    out
}

/// Compile the regex set for xf.ai.pii based on the user's `types`
/// selection (empty = all). Each regex is paired with the replacement
/// label that gets substituted in for each match. Conservative
/// patterns - favor false-negatives over false-positives. Users with
/// stricter needs should follow up with an LLM-backed pass.
pub(crate) fn pii_patterns(types: &[String]) -> Vec<(regex::Regex, &'static str)> {
    let want = |t: &str| -> bool { types.is_empty() || types.iter().any(|s| s == t) };
    let mut out: Vec<(regex::Regex, &'static str)> = Vec::new();
    if want("email") {
        // RFC 5322 lite - good enough for production-ish ETL use.
        out.push((
            regex::Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").unwrap(),
            "[REDACTED-EMAIL]",
        ));
    }
    if want("credit_card") {
        // Run BEFORE phone so a 16-digit number isn't half-eaten by
        // the phone matcher.
        out.push((
            regex::Regex::new(r"\b(?:\d[ -]*?){13,19}\b").unwrap(),
            "[REDACTED-CREDIT-CARD]",
        ));
    }
    if want("ssn") {
        out.push((
            regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap(),
            "[REDACTED-SSN]",
        ));
    }
    if want("phone") {
        // US-ish plus E.164. REQUIRES a separator (space/dash) or
        // parentheses between groups, so a bare run of digits is NOT
        // treated as a phone. The previous pattern had no separator
        // requirement and no word boundaries, so it destructively
        // redacted any 10-digit token (order ids, account numbers,
        // epoch timestamps) as [REDACTED-PHONE], and partially ate the
        // digits of long/letter-glued card numbers the credit_card
        // pattern missed - both contradict the module's documented
        // "favor false-negatives" design. Won't catch every
        // international format (intentionally conservative).
        // No leading \b: a literal "(" has no word boundary before it, so
        // anchoring there would break the "(415) 555-0100" form. The
        // separator requirement inside the pattern is what rejects bare
        // digit runs; the trailing \b keeps it from eating glued suffixes.
        out.push((
            regex::Regex::new(
                r"(?:\+?\d{1,3}[ -])?(?:\(\d{3}\)[ -]?|\d{3}[ -])\d{3}[ -]\d{4}\b",
            )
            .unwrap(),
            "[REDACTED-PHONE]",
        ));
    }
    out
}

/// Split `text` into chunks of at most `size` chars with `overlap`
/// chars between successive chunks. Walks in char (not byte) windows
/// to avoid splitting UTF-8 sequences. Returns at least one chunk
/// even for empty input - callers usually want a row to exist.
pub(crate) fn chunk_text(text: &str, size: usize, overlap: usize) -> Vec<String> {
    if size == 0 {
        return vec![text.to_string()];
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= size {
        return vec![text.to_string()];
    }
    let step = size.saturating_sub(overlap).max(1);
    let mut out: Vec<String> = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let end = (start + size).min(chars.len());
        out.push(chars[start..end].iter().collect());
        if end == chars.len() {
            break;
        }
        start += step;
    }
    out
}
