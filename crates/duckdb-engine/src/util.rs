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
    // "pat" (personal access token) is the one needle matched whole rather
    // than as a substring, because as a substring it also swallowed path,
    // filePath, rowPath, jsonPath, recordsPath, pattern, keyPattern and
    // loadSpatial. Every one of those was then exported as ${DUCKLE_PATH},
    // which left the compiled SQL from .sql() / .explain() unrunnable and
    // stopped a "no files found" error from naming the file it could not
    // find. `pat` is the only property key that actually holds a token.
    if k == "pat" {
        return true;
    }
    [
        "password", "passwd", "secret", "token", "apikey", "api_key",
        "privatekey", "private_key", "accesskey", "access_key",
        "clientsecret", "client_secret", "connectionstring", "connection_string",
        "sas", "credential",
    ]
    .iter()
    .any(|needle| k.contains(needle))
}

/// Every secret-keyed property in a pipeline whose value is a literal rather than a
/// placeholder, named as `node label / property key`.
///
/// A value under a key like `password` or `accessKey` is a credential. Written as
/// `${ENV:NAME}` or `${context.var}` it is a reference and travels harmlessly; typed in
/// directly it IS the credential, and goes wherever the pipeline goes - into the workspace
/// file, into git if the workspace is committed, and into the body of a deploy.
///
/// `duckle-runner build` already refuses to package a pipeline in that state. This is the
/// same judgement, shared so the deploy path can apply it rather than growing a second
/// opinion about what counts as a secret.
///
/// Empty values are ignored: an unfilled field is not a leak.
pub fn literal_secrets(doc: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    let Some(nodes) = doc.get("nodes").and_then(|n| n.as_array()) else {
        return out;
    };
    for node in nodes {
        let data = node.get("data");
        let label = data
            .and_then(|d| d.get("label"))
            .and_then(|l| l.as_str())
            .or_else(|| node.get("id").and_then(|i| i.as_str()))
            .unwrap_or("a node");
        let Some(props) = data.and_then(|d| d.get("properties")).and_then(|p| p.as_object())
        else {
            continue;
        };
        for (key, value) in props {
            if !is_secret_prop_key(key) {
                continue;
            }
            let Some(text) = value.as_str() else { continue };
            let text = text.trim();
            // A placeholder is a reference to a secret, not the secret. `${ENV:PGPASS}`,
            // `${context.pw}` and a saved-connection ref all pass.
            if text.is_empty() || (text.starts_with("${") && text.ends_with('}')) {
                continue;
            }
            out.push(format!("{label} / {key}"));
        }
    }
    out
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

/// Collect the plaintext secrets configured anywhere in the pipeline, so they
/// can be replaced in display-only SQL. Every non-empty value under a secret
/// key is taken regardless of length - a short password is still a password,
/// and a length floor here would leak it. Collisions with ordinary SQL tokens
/// are handled at replace time by [`replace_delimited`], not by dropping the
/// secret. Sorted longest-value-first so a value that contains another is
/// replaced first.
pub(crate) fn collect_secrets(doc: &PipelineDoc) -> Vec<Secret> {
    let mut out: Vec<Secret> = Vec::new();
    for node in &doc.nodes {
        if let Some(JsonValue::Object(props)) = node.data.properties.as_ref() {
            for (key, val) in props {
                if is_secret_prop_key(key) {
                    if let Some(s) = val.as_str() {
                        // Any non-empty value under a secret key is a
                        // credential and must be redacted regardless of length
                        // (a short password is still a password). Only skip an
                        // empty/whitespace value - redacting "" would splice
                        // the placeholder across the whole SQL - and `${...}`
                        // env placeholders, which are already safe to share.
                        let t = s.trim();
                        if !t.is_empty() && !t.starts_with("${") {
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
        out = replace_delimited(&out, secret.value.as_str(), &secret.placeholder);
        // Credentials are also embedded as SQL string literals with single
        // quotes doubled (sql_escape / "''"); redact that form too so a value
        // containing a quote does not leak past the raw-value replace above.
        if secret.value.contains('\'') {
            let escaped = secret.value.replace('\'', "''");
            out = replace_delimited(&out, &escaped, &secret.placeholder);
        }
    }
    out
}

/// Replace `needle` with `placeholder`, but only where the match is a whole
/// token - neither neighbour may be a character that could continue an
/// identifier.
///
/// A blind `str::replace` corrupts the SQL whenever a credential happens to be
/// a substring of an ordinary identifier, and that is not hypothetical:
/// password "prod" rewrote the output path `production_report.parquet` to
/// `${DUCKLE_PASSWORD}uction_report.parquet`, and a one-character password "p"
/// turned `LOAD postgres` into `LOAD ${DUCKLE_PASSWORD}ostgres`. An exported
/// script is meant to be runnable, and that is not.
///
/// Requiring delimiters costs nothing in safety, because a credential is never
/// spelled as part of a longer word. Wherever one actually appears -
/// `password=hunter2` in a libpq string, `user:hunter2@host` in a URL,
/// `'hunter2'` as a literal - it is bounded by `=`, `'`, `:`, `@`, whitespace
/// or the end of the string. So every real occurrence is still redacted,
/// including short values: the length floor that this deliberately does NOT
/// reintroduce is what would leak them.
fn replace_delimited(haystack: &str, needle: &str, placeholder: &str) -> String {
    if needle.is_empty() || !haystack.contains(needle) {
        return haystack.to_string();
    }
    // `_` counts as an identifier character, so a password equal to `report`
    // does not carve up `report_id`.
    let continues_ident = |c: char| c.is_alphanumeric() || c == '_';

    let mut out = String::with_capacity(haystack.len());
    let mut rest = haystack;
    while let Some(hit) = rest.find(needle) {
        let before_ok = rest[..hit]
            .chars()
            .next_back()
            .map(|c| !continues_ident(c))
            .unwrap_or(true);
        let after = hit + needle.len();
        let after_ok = rest[after..]
            .chars()
            .next()
            .map(|c| !continues_ident(c))
            .unwrap_or(true);

        out.push_str(&rest[..hit]);
        if before_ok && after_ok {
            out.push_str(placeholder);
        } else {
            out.push_str(needle); // part of a longer identifier - leave it alone
        }
        rest = &rest[after..];
    }
    out.push_str(rest);
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
    } else if let Some(RuntimeSpec::Foreach { path, concurrency, .. }) = s.runtime.as_ref() {
        if *concurrency > 1 {
            format!(
                "control step: runs sub-pipeline '{}' once per upstream row, up to {} at a time (ctl.foreach)",
                path, concurrency
            )
        } else {
            format!("control step: runs sub-pipeline '{}' once per upstream row (ctl.foreach)", path)
        }
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

/// Resolve a general entity reference into the text it stands for.
///
/// quick-xml 0.42 split `&amp;`, `&#60;` and friends out of `Event::Text` into
/// their own `Event::GeneralRef`. Code that matches only Text and CData
/// therefore drops every entity in element content silently - `Ben &amp; Jerry`
/// arrives as `Ben  Jerry` - which is why both parsers below handle it.
///
/// An entity that cannot be resolved (one declared in a DTD) is kept verbatim
/// as `&name;` rather than dropped. Losing it silently is the failure this
/// exists to prevent, and a literal is at least visible.
pub(crate) fn xml_entity_text(e: &quick_xml::events::BytesRef) -> String {
    match e.resolve_char_ref() {
        Ok(Some(c)) => return c.to_string(),
        // A malformed numeric reference (&#xZZ;) is content we cannot read;
        // keep it literal rather than guess.
        Ok(None) => {}
        Err(_) => return format!("&{};", e.borrow().into_inner()),
    }
    let name = e.borrow().into_inner();
    match quick_xml::escape::resolve_predefined_entity(&name) {
        Some(t) => t.to_string(),
        None => format!("&{};", name),
    }
}

/// Finalize an XML element being popped from the stack: convert it
/// to a JSON value, push to rows if its path matches row_path, and
/// merge it into its parent (multiple same-named children collapse
/// to an array). Standalone (not a method) so the borrow checker
/// doesn't complain about &mut stack + &mut rows at the same time.
/// Build the JSON value for a closed XML element from its attribute/child
/// builder and accumulated text. Empty element -> Null; text-only -> String;
/// otherwise an object, with any trailing text under `_text`. Shared by the
/// buffered (`walk_xml_to_rows`) and streaming (`stream_xml_rows`) walkers.
fn xml_element_value(mut builder: serde_json::Map<String, JsonValue>, text: String) -> JsonValue {
    let text_trimmed = text.trim().to_string();
    if builder.is_empty() && !text_trimmed.is_empty() {
        JsonValue::String(text_trimmed)
    } else if builder.is_empty() {
        JsonValue::Null
    } else {
        if !text_trimmed.is_empty() {
            builder.insert("_text".into(), JsonValue::String(text_trimmed));
        }
        JsonValue::Object(builder)
    }
}

/// Does the current element path (`stack` names + `name`) end with `row_path`?
/// Element names compare by local part, so `soap:Envelope` matches a user's
/// `Envelope` (and vice-versa); the user can still write the prefix to pin a
/// single namespace. An empty `row_path` matches only direct children of the
/// root, avoiding emitting nested structures as separate rows.
fn xml_path_matches(
    stack: &[(String, serde_json::Map<String, JsonValue>, String)],
    name: &str,
    row_path: &[String],
) -> bool {
    fn local(name: &str) -> &str {
        match name.rfind(':') {
            Some(i) => &name[i + 1..],
            None => name,
        }
    }
    let mut current_path: Vec<&str> = stack.iter().map(|(n, _, _)| n.as_str()).collect();
    current_path.push(name);
    if row_path.is_empty() {
        current_path.len() == 1
    } else {
        current_path.len() >= row_path.len()
            && current_path[current_path.len() - row_path.len()..]
                .iter()
                .zip(row_path.iter())
                .all(|(a, b)| local(a) == local(b.as_str()))
    }
}

pub(crate) fn xml_close_element(
    stack: &mut Vec<(String, serde_json::Map<String, JsonValue>, String)>,
    rows: &mut Vec<JsonValue>,
    row_path: &[String],
    name: &str,
    builder: serde_json::Map<String, JsonValue>,
    text: String,
) {
    let matches = xml_path_matches(stack, name, row_path);
    let value = xml_element_value(builder, text);
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
    // NOT trim_text(true). It trims every Text event individually, and since
    // quick-xml 0.42 splits an entity out of the run of text around it, that
    // eats the spaces beside it: `Ben &amp; Jerry` arrives as `Ben&Jerry`.
    // Whitespace between pretty-printed tags is discarded anyway, because
    // xml_element_value trims the text accumulated for the whole element.
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
                let name = e.name().as_ref().to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", attr.key.as_ref());
                    let v = attr.value.to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                stack.push((name, builder, String::new()));
            }
            Event::Empty(e) => {
                let name = e.name().as_ref().to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", attr.key.as_ref());
                    let v = attr.value.to_string();
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
                // quick-xml 0.42 decodes and unescapes to a Cow<str>, so this
                // no longer round-trips through bytes.
                let text = e
                    .xml_content(quick_xml::XmlVersion::Implicit1_0)
                    .to_string();
                if let Some(last) = stack.last_mut() {
                    last.2.push_str(&text);
                }
            }
            Event::GeneralRef(e) => {
                if let Some(last) = stack.last_mut() {
                    last.2.push_str(&xml_entity_text(&e));
                }
            }
            Event::CData(e) => {
                // CDATA holds literal text (no XML entity escaping). snk.xml
                // writes complex / JSON-encoded cell values inside CDATA, and an
                // author may wrap any value this way, so capture it like Text -
                // otherwise the content is silently dropped (issue #33).
                let text = e.into_inner().as_ref().to_string();
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

/// Streaming close: on a `row_path` match, emit the element via `emit` and drop
/// it - it is NOT nested into its parent, so ancestors (root, containers) never
/// accumulate the whole document. A matched element is never itself an ancestor
/// of another match (their paths differ in depth), so skipping the parent-nest
/// changes no output while keeping live memory at O(one row + nesting depth).
fn xml_close_element_streaming(
    stack: &mut Vec<(String, serde_json::Map<String, JsonValue>, String)>,
    row_path: &[String],
    name: &str,
    builder: serde_json::Map<String, JsonValue>,
    text: String,
    emit: &mut dyn FnMut(&JsonValue) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let matches = xml_path_matches(stack, name, row_path);
    let value = xml_element_value(builder, text);
    if matches {
        return emit(&value);
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
    Ok(())
}

/// Streaming XML pull-parser. Reads events from `reader` and emits each
/// `row_path` match via `emit` the moment it closes, holding only the current
/// element stack (nesting depth) plus the row being built in memory - never the
/// whole document. This lets src.xml ingest multi-GB (and gzipped) files that
/// `walk_xml_to_rows` (whole file in a String, all rows in a Vec, every match
/// re-nested into the root) would exhaust RAM on (issue #186). Row shape is
/// identical to `walk_xml_to_rows` for well-formed documents.
pub(crate) fn stream_xml_rows<R: std::io::BufRead>(
    reader: R,
    row_path: &str,
    cancel: &Arc<AtomicBool>,
    emit: &mut dyn FnMut(&JsonValue) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut xr = Reader::from_reader(reader);
    // NOT trim_text(true). It trims every Text event individually, and since
    // quick-xml 0.42 splits an entity out of the run of text around it, that
    // eats the spaces beside it: `Ben &amp; Jerry` arrives as `Ben&Jerry`.
    // Whitespace between pretty-printed tags is discarded anyway, because
    // xml_element_value trims the text accumulated for the whole element.
    let row_path_parts: Vec<String> = row_path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut stack: Vec<(String, serde_json::Map<String, JsonValue>, String)> = Vec::new();
    let mut buf = Vec::new();
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(EngineError::Cancelled);
        }
        let event = xr
            .read_event_into(&mut buf)
            .map_err(|e| EngineError::Query(format!("xml: parse: {}", e)))?;
        match event {
            Event::Eof => break,
            Event::Start(e) => {
                let name = e.name().as_ref().to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", attr.key.as_ref());
                    let v = attr.value.to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                stack.push((name, builder, String::new()));
            }
            Event::Empty(e) => {
                let name = e.name().as_ref().to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", attr.key.as_ref());
                    let v = attr.value.to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                xml_close_element_streaming(
                    &mut stack,
                    &row_path_parts,
                    &name,
                    builder,
                    String::new(),
                    emit,
                )?;
            }
            Event::Text(e) => {
                // quick-xml 0.42 decodes and unescapes to a Cow<str>, so this
                // no longer round-trips through bytes.
                let text = e
                    .xml_content(quick_xml::XmlVersion::Implicit1_0)
                    .to_string();
                if let Some(last) = stack.last_mut() {
                    last.2.push_str(&text);
                }
            }
            Event::GeneralRef(e) => {
                if let Some(last) = stack.last_mut() {
                    last.2.push_str(&xml_entity_text(&e));
                }
            }
            Event::CData(e) => {
                let text = e.into_inner().as_ref().to_string();
                if let Some(last) = stack.last_mut() {
                    last.2.push_str(&text);
                }
            }
            Event::End(_) => {
                if let Some((name, builder, text)) = stack.pop() {
                    xml_close_element_streaming(
                        &mut stack,
                        &row_path_parts,
                        &name,
                        builder,
                        text,
                        emit,
                    )?;
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(())
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

/// Infer a nullable Avro field type for column `name` by scanning `rows`
/// for the first non-null value. Used by snk.avro when schemaJson isn't
/// supplied. Every field is a `["null", T]` union so ANY row may be null
/// without the writer rejecting it - inferring from row 0 alone would pin a
/// leading-null column to the null-only "null" type (which then rejects every
/// later non-null value) and a leading-value column to a non-nullable type
/// (which rejects every later null). Numeric columns get a `["null","long",
/// "double"]` union so a mix of integer and fractional values both validate.
/// Strings/booleans map to their type; objects, arrays and all-null columns
/// fall back to string (objects/arrays are JSON-stringified on write).
pub(crate) fn infer_avro_nullable_field(rows: &[JsonValue], name: &str) -> JsonValue {
    let first_non_null = rows.iter().filter_map(|r| r.as_object()).find_map(|o| {
        match o.get(name) {
            Some(v) if !v.is_null() => Some(v),
            _ => None,
        }
    });
    let mut branches: Vec<&str> = vec!["null"];
    match first_non_null {
        Some(JsonValue::Bool(_)) => branches.push("boolean"),
        Some(JsonValue::Number(_)) => {
            branches.push("long");
            branches.push("double");
        }
        // strings, objects, arrays (JSON-stringified) and all-null columns
        _ => branches.push("string"),
    }
    JsonValue::Array(branches.into_iter().map(|s| JsonValue::String(s.into())).collect())
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
    use hmac::{Hmac, KeyInit, Mac};
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
    let mut saw_content_length = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
                saw_content_length = true;
            }
            headers.push((k, v));
        }
    }
    // Body: any bytes we've already read past the header split + more
    // until we have content_length bytes total.
    // Cap the declared body size so an attacker-controlled (or lying)
    // Content-Length can't grow `body` unboundedly in RAM.
    const MAX_WEBHOOK_BODY: usize = 16 * 1024 * 1024;
    if content_length > MAX_WEBHOOK_BODY {
        return Err(format!(
            "request body too large ({} bytes; max {})",
            content_length, MAX_WEBHOOK_BODY
        ));
    }
    let mut body: Vec<u8> = buf[split_at + 4..].to_vec();
    // Only read-to-length + truncate when Content-Length was declared. Without
    // it, keep whatever body bytes were already buffered rather than truncating
    // to nothing (which silently dropped the payload).
    if saw_content_length {
        while body.len() < content_length {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => body.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        body.truncate(content_length);
    }
    Ok((method, path, headers, body))
}

/// Cosine similarity between two equal-length float vectors. Returns 0.0 if
/// either vector is empty / lengths mismatch / either has zero magnitude.
/// Retained for the public API + unit tests; xf.ai.dedupe uses
/// cosine_similarity_with_norms to avoid recomputing norms in its O(N^2) loop.
#[allow(dead_code)]
pub(crate) fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    cosine_similarity_with_norms(a, l2_norm(a), b, l2_norm(b))
}

/// L2 norm (sqrt of the sum of squares) of a float vector.
pub(crate) fn l2_norm(v: &[f64]) -> f64 {
    v.iter().map(|x| x * x).sum::<f64>().sqrt()
}

/// Cosine similarity using precomputed L2 norms. Bit-identical to
/// `cosine_similarity(a, b)` when `norm_a == l2_norm(a)` and
/// `norm_b == l2_norm(b)`, but only does the dot-product pass - used by
/// xf.ai.dedupe so each kept vector's norm is computed once instead of on
/// every one of the O(N^2) comparisons.
pub(crate) fn cosine_similarity_with_norms(a: &[f64], norm_a: f64, b: &[f64], norm_b: f64) -> f64 {
    if a.is_empty() || a.len() != b.len() || norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    let mut dot = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
    }
    dot / (norm_a * norm_b)
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

/// Post-process a written .xlsx so Excel preserves leading/trailing whitespace
/// in text cells. DuckDB's excel writer serializes cell text as `<t>...</t>`
/// without `xml:space="preserve"`; per the OOXML spec Excel then normalizes
/// (strips) the edge whitespace of those elements when it loads the workbook,
/// so a SQL Server nvarchar value like "   note" reads back as "note" (#141).
/// We reopen the file (a zip), add `xml:space="preserve"` to every `<t>`
/// element in the worksheet / shared-strings parts, and repack. Best-effort:
/// the caller logs and continues on error, since the unmodified file is still a
/// valid workbook.
pub(crate) fn finalize_xlsx_whitespace(path: &std::path::Path) -> std::io::Result<()> {
    use std::io::{Read, Write};
    let bytes = std::fs::read(path)?;
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut out_buf: Vec<u8> = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut out_buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for i in 0..archive.len() {
            let mut entry = archive
                .by_index(i)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let name = entry.name().to_string();
            // Cell strings live in the worksheet parts (the native inlineStr
            // writer) and in sharedStrings.xml (the GDAL writer); only those
            // parts need patching.
            let is_text_part =
                name.starts_with("xl/worksheets/") || name == "xl/sharedStrings.xml";
            if is_text_part {
                let mut content = String::new();
                entry.read_to_string(&mut content)?;
                // Bare "<t>" only. Never touch "<t " (already carries
                // attributes, so already correct) or "<t/>" (empty, no text).
                let patched = content.replace("<t>", "<t xml:space=\"preserve\">");
                writer
                    .start_file(name, opts)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                writer.write_all(patched.as_bytes())?;
            } else {
                // Everything else is copied verbatim (keeps its compression).
                writer
                    .raw_copy_file(entry)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }
        }
        writer
            .finish()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    }

    // Replace the original via a temp file + rename so a failure mid-write can
    // never leave a truncated .xlsx in place.
    let tmp = path.with_extension("xlsx.tmp");
    std::fs::write(&tmp, &out_buf)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
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

#[cfg(test)]
mod tests {
    /// A credential typed straight into a node travels with the pipeline: into the
    /// workspace file, into git if that is committed, and into a deploy body. `build`
    /// already refuses to package one; this is the shared judgement the deploy path uses
    /// so the two cannot disagree about what a secret is.
    #[test]
    fn a_typed_in_credential_is_reported_and_a_placeholder_is_not() {
        let doc = serde_json::json!({
            "nodes": [
                { "id": "n1", "data": { "label": "Postgres",
                    "properties": { "host": "db.internal", "password": "hunter2" } } },
                { "id": "n2", "data": { "label": "S3",
                    "properties": { "accessKey": "${ENV:AWS_KEY}", "secretKey": "" } } },
                { "id": "n3", "data": { "label": "Ref",
                    "properties": { "password": "${context.pgpass}" } } }
            ]
        });

        let found = literal_secrets(&doc);
        assert_eq!(found, ["Postgres / password"], "found: {found:?}");
    }

    /// The shapes that must not panic or report: no nodes, no data, no properties.
    #[test]
    fn a_pipeline_with_nothing_to_scan_reports_nothing() {
        for doc in [
            serde_json::json!({}),
            serde_json::json!({ "nodes": [] }),
            serde_json::json!({ "nodes": [{ "id": "n1" }] }),
            serde_json::json!({ "nodes": [{ "id": "n1", "data": {} }] }),
            serde_json::json!({ "nodes": [{ "id": "n1", "data": { "properties": {} } }] }),
        ] {
            assert!(literal_secrets(&doc).is_empty(), "reported on {doc}");
        }
    }

    use super::{
        finalize_xlsx_whitespace, infer_avro_nullable_field, is_secret_prop_key, literal_secrets,
        stream_xml_rows, walk_xml_to_rows,
    };
    use serde_json::json;
    use std::io::{Read, Write};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    #[test]
    fn path_like_keys_are_not_treated_as_secrets() {
        // "pat" as a substring needle matched every one of these, so their
        // values exported as ${DUCKLE_PATH}: the SQL from .sql() would not run,
        // and a missing-file error could not say which file was missing.
        for key in [
            "path", "filePath", "rowPath", "jsonPath", "recordsPath", "remotePath",
            "resultsPath", "responsePath", "pathFilter", "pattern", "keyPattern",
            "loadSpatial", "geospatial", "patch",
        ] {
            assert!(!is_secret_prop_key(key), "{} is not a credential", key);
        }
        // The key that actually holds a token still is one, and the real
        // credential needles are untouched.
        assert!(is_secret_prop_key("pat"));
        assert!(is_secret_prop_key("PAT"));
        for key in [
            "password", "apiKey", "accessKey", "clientSecret", "token",
            "privateKey", "connectionString", "credential", "sas",
            // paths that reach a credential keep matching on their own needles
            "credentialsPath", "privateKeyPath",
        ] {
            assert!(is_secret_prop_key(key), "{} must stay redacted", key);
        }
    }

    #[test]
    fn xlsx_whitespace_preserve_is_injected() {
        // #141: DuckDB's xlsx writer emits <t>   text</t> without
        // xml:space="preserve", so Excel strips the leading spaces on load.
        // finalize_xlsx_whitespace must add the attribute (and copy every other
        // zip entry verbatim).
        let path = std::env::temp_dir()
            .join(format!("duckle_xlsx_ws_{}.xlsx", std::process::id()));
        {
            let f = std::fs::File::create(&path).unwrap();
            let mut zw = zip::ZipWriter::new(f);
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            zw.start_file("[Content_Types].xml", opts).unwrap();
            zw.write_all(b"<Types/>").unwrap();
            zw.start_file("xl/worksheets/sheet1.xml", opts).unwrap();
            zw.write_all(
                b"<worksheet><c t=\"inlineStr\"><is><t>   lead</t></is></c></worksheet>",
            )
            .unwrap();
            zw.finish().unwrap();
        }

        finalize_xlsx_whitespace(&path).unwrap();

        let mut archive = zip::ZipArchive::new(std::fs::File::open(&path).unwrap()).unwrap();
        let mut sheet = String::new();
        archive
            .by_name("xl/worksheets/sheet1.xml")
            .unwrap()
            .read_to_string(&mut sheet)
            .unwrap();
        assert!(
            sheet.contains("<t xml:space=\"preserve\">   lead</t>"),
            "leading whitespace must be preserved, got: {}",
            sheet
        );
        // Non-text parts are copied byte-for-byte.
        let mut ct = String::new();
        archive
            .by_name("[Content_Types].xml")
            .unwrap()
            .read_to_string(&mut ct)
            .unwrap();
        assert_eq!(ct, "<Types/>");

        let _ = std::fs::remove_file(&path);
    }

        /// Element text is decoded, not passed through raw. This is the exact
    /// behaviour that moved in quick-xml 0.42: `xml_content` went from a
    /// fallible byte-ish result to an infallible `Cow<str>` that decodes and
    /// unescapes for us, and the migration deleted the decoding this code used
    /// to do itself. If that were wrong, `&amp;` would reach a column as the
    /// five literal characters instead of one ampersand.
    #[test]
    fn xml_entities_in_element_text_are_decoded() {
        let xml = "<r><row><name>Ben &amp; Jerry&apos;s</name>                   <note>a &lt; b &gt; c</note></row></r>";
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let rows = walk_xml_to_rows(xml, "row", &cancel).expect("parses");
        assert_eq!(rows.len(), 1);
        let o = rows[0].as_object().expect("object");
        assert_eq!(
            o.get("name").and_then(|v| v.as_str()),
            Some("Ben & Jerry's"),
            "&amp; and &apos; must arrive decoded"
        );
        assert_eq!(
            o.get("note").and_then(|v| v.as_str()),
            Some("a < b > c"),
            "&lt; and &gt; must arrive decoded"
        );
    }

#[test]
    fn xml_cdata_text_is_captured_not_dropped() {
        // issue #33: a value wrapped in <![CDATA[...]]> (how snk.xml writes
        // complex/JSON cells) was skipped on read, so the column came back empty.
        let xml = "<root><row><id>1</id><payload><![CDATA[{\"a\":1}]]></payload></row>\
                   <row><id>2</id><payload>plain</payload></row></root>";
        let cancel = Arc::new(AtomicBool::new(false));
        let rows = walk_xml_to_rows(xml, "root/row", &cancel).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["payload"], json!("{\"a\":1}"), "CDATA content must be captured");
        assert_eq!(rows[0]["id"], json!("1"));
        assert_eq!(rows[1]["payload"], json!("plain"), "plain text still works");
    }

    #[test]
    fn stream_xml_rows_matches_walk_output() {
        // #186: the streaming walker (used by src.xml for large / gzipped files)
        // must emit byte-identical rows to walk_xml_to_rows across attributes,
        // nested + repeated children, CDATA, plain text, self-closing rows,
        // bare single-segment paths and the empty (root) path.
        let cases: &[(&str, &str)] = &[
            (
                "<catalog><book id=\"1\"><title>A</title><tags><t>x</t><t>y</t></tags></book>\
                 <book id=\"2\"><title>B</title></book></catalog>",
                "catalog/book",
            ),
            (
                "<root><row><id>1</id><payload><![CDATA[{\"a\":1}]]></payload></row>\
                 <row><id>2</id><payload>plain</payload></row></root>",
                "root/row",
            ),
            ("<a><item>1</item><item>2</item></a>", "item"),
            ("<a><one>1</one><two>2</two></a>", ""),
            ("<root><row a=\"1\"/><row a=\"2\"/></root>", "root/row"),
        ];
        let cancel = Arc::new(AtomicBool::new(false));
        for (xml, path) in cases {
            let expected = walk_xml_to_rows(xml, path, &cancel).unwrap();
            let mut got: Vec<serde_json::Value> = Vec::new();
            stream_xml_rows(std::io::Cursor::new(xml.as_bytes()), path, &cancel, &mut |row| {
                got.push(row.clone());
                Ok(())
            })
            .unwrap();
            assert_eq!(got, expected, "stream vs walk mismatch for path {:?} in {}", path, xml);
        }
    }

    #[test]
    fn stream_xml_rows_emits_incrementally_without_root_buildup() {
        // A repeating-row document: every <row> match is emitted and dropped, so
        // the root <feed> never accumulates the rows. We can't measure RAM here,
        // but we can prove each row arrives independently and in order.
        let xml = "<feed><row><n>1</n></row><row><n>2</n></row><row><n>3</n></row></feed>";
        let cancel = Arc::new(AtomicBool::new(false));
        let mut seen: Vec<String> = Vec::new();
        stream_xml_rows(std::io::Cursor::new(xml.as_bytes()), "feed/row", &cancel, &mut |row| {
            seen.push(row["n"].as_str().unwrap().to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec!["1", "2", "3"]);
    }

    #[test]
    fn stream_xml_rows_propagates_cancel() {
        let xml = "<feed><row><n>1</n></row><row><n>2</n></row></feed>";
        let cancel = Arc::new(AtomicBool::new(true));
        let r = stream_xml_rows(std::io::Cursor::new(xml.as_bytes()), "feed/row", &cancel, &mut |_| Ok(()));
        assert!(matches!(r, Err(crate::EngineError::Cancelled)));
    }

    #[test]
    fn avro_field_is_nullable_union_inferred_past_leading_null() {
        // Column `a` is null in row 0 but an integer in row 1: the inferred
        // type must be a nullable numeric union, not the null-only "null"
        // type (which would reject the later non-null value).
        let rows = vec![json!({ "a": null, "b": "x" }), json!({ "a": 5, "b": "y" })];
        assert_eq!(
            infer_avro_nullable_field(&rows, "a"),
            json!(["null", "long", "double"])
        );
        assert_eq!(infer_avro_nullable_field(&rows, "b"), json!(["null", "string"]));
    }

    #[test]
    fn avro_all_null_column_defaults_to_nullable_string() {
        let rows = vec![json!({ "c": null }), json!({ "c": null })];
        assert_eq!(infer_avro_nullable_field(&rows, "c"), json!(["null", "string"]));
    }

    #[test]
    fn avro_boolean_and_object_columns() {
        let rows = vec![json!({ "flag": true, "obj": { "k": 1 } })];
        assert_eq!(
            infer_avro_nullable_field(&rows, "flag"),
            json!(["null", "boolean"])
        );
        // Objects/arrays are JSON-stringified on write, so they map to string.
        assert_eq!(infer_avro_nullable_field(&rows, "obj"), json!(["null", "string"]));
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    fn secret(value: &str) -> Secret {
        Secret { value: value.to_string(), placeholder: "${DUCKLE_PASSWORD}".to_string() }
    }

    /// The reported defect: a credential that happens to be a substring of an
    /// identifier used to corrupt the SQL around it.
    #[test]
    fn a_password_inside_an_identifier_is_left_alone() {
        let sql = "COPY (SELECT * FROM \"t\") TO '/data/production_report.parquet'";
        assert_eq!(redact_secret_values(sql, &[secret("prod")]), sql);

        let sql = "LOAD postgres; SELECT * FROM public.orders";
        assert_eq!(redact_secret_values(sql, &[secret("p")]), sql);
    }

    /// ...while every place a credential actually appears is still redacted,
    /// at any length. A length floor would have let these through.
    #[test]
    fn a_password_in_credential_position_is_always_redacted() {
        for (sql, pw) in [
            ("ATTACH 'host=h dbname=d user=u password=prod'", "prod"),
            ("ATTACH 'host=h dbname=d user=u password=p'", "p"),
            ("ATTACH 'mysql://u:prod@host:3306/db'", "prod"),
            ("CREATE SECRET (KEY_ID 'a', SECRET 'prod')", "prod"),
        ] {
            let got = redact_secret_values(sql, &[secret(pw)]);
            assert!(got.contains("${DUCKLE_PASSWORD}"), "not redacted: {sql}");
            assert!(!got.contains(&format!("={pw}'")), "value survived: {got}");
        }
    }

    /// Underscore continues an identifier, so a password equal to a column
    /// stem must not split a longer column name.
    #[test]
    fn underscore_counts_as_part_of_an_identifier() {
        let sql = "SELECT report_id FROM t WHERE x = 1";
        assert_eq!(redact_secret_values(sql, &[secret("report")]), sql);
    }

    /// A value appearing both ways in one statement: redact the credential,
    /// keep the identifier.
    #[test]
    fn mixed_occurrences_are_handled_independently() {
        let sql = "ATTACH 'dbname=shop password=shop' AS s; SELECT * FROM shopping_cart";
        let got = redact_secret_values(sql, &[secret("shop")]);
        assert!(got.contains("shopping_cart"), "identifier was corrupted: {got}");
        assert!(!got.contains("password=shop"), "credential survived: {got}");
        assert_eq!(got.matches("${DUCKLE_PASSWORD}").count(), 2, "{got}");
    }
}

/// Serialises every test in this crate that sets `DUCKLE_WORKSPACE`, which is
/// process-global while `cargo test` runs tests in parallel.
///
/// It has to be ONE lock. Two test modules with a `Mutex` each do not
/// serialize against one another, and the symptom is the confusing one: each
/// module passes when run alone and fails in the full suite.
#[cfg(test)]
pub(crate) fn workspace_env_guard() -> std::sync::MutexGuard<'static, ()> {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// #255: resolve a link found in a page against the page it came from.
///
/// Server-rendered pagination puts a relative href in the markup - `?page=2`,
/// `/companies?p=2`, `../next` - and following it means joining it to the
/// current URL the way a browser would. Getting this wrong does not error, it
/// fetches the WRONG page, so each shape is spelled out and tested rather than
/// approximated with string concatenation.
///
/// `None` when there is nothing usable, which the caller treats as "no next
/// page" rather than as a failure.
pub(crate) fn resolve_url(base: &str, href: &str) -> Option<String> {
    let href = href.trim();
    if href.is_empty() {
        return None;
    }
    // Already absolute: a scheme is `letter *( letter / digit / + / - / . ) :`.
    if let Some(i) = href.find(':') {
        let scheme = &href[..i];
        if !scheme.is_empty()
            && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
        {
            return Some(href.to_string());
        }
    }
    let (scheme, rest) = base.split_once("://")?;
    // Protocol-relative: keep the page's own scheme.
    if let Some(hostpath) = href.strip_prefix("//") {
        return Some(format!("{scheme}://{hostpath}"));
    }
    let (authority, base_path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    // The base path without its query or fragment - a link is resolved against
    // the document, not against the query that produced it.
    let base_path = base_path
        .split(['?', '#'])
        .next()
        .unwrap_or("/");
    let root = format!("{scheme}://{authority}");

    if let Some(frag) = href.strip_prefix('#') {
        return Some(format!("{root}{base_path}#{frag}"));
    }
    if href.starts_with('?') {
        return Some(format!("{root}{base_path}{href}"));
    }
    if href.starts_with('/') {
        return Some(format!("{root}{}", normalize_path(href)));
    }
    // Relative to the DIRECTORY of the current document, so `next` beside
    // `/a/b/page.html` is `/a/b/next`, not `/a/b/page.html/next`.
    let dir = match base_path.rfind('/') {
        Some(i) => &base_path[..=i],
        None => "/",
    };
    Some(format!("{root}{}", normalize_path(&format!("{dir}{href}"))))
}

/// Collapse `.` and `..` in a path, the way a browser does before requesting.
///
/// A `..` that would climb past the root is dropped rather than kept: no server
/// can serve it, and keeping it would send a request that is certain to fail.
fn normalize_path(path: &str) -> String {
    let (path, tail) = match path.find(['?', '#']) {
        Some(i) => (&path[..i], &path[i..]),
        None => (path, ""),
    };
    let mut out: Vec<&str> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                out.pop();
            }
            s => out.push(s),
        }
    }
    let trailing = path.ends_with('/') && !out.is_empty();
    let mut s = String::from("/");
    s.push_str(&out.join("/"));
    if trailing {
        s.push('/');
    }
    s.push_str(tail);
    s
}

#[cfg(test)]
mod url_resolve_tests {
    use super::resolve_url;

    const PAGE: &str = "https://example.com/a/b/list.html?p=1";

    #[test]
    fn an_absolute_link_is_left_alone() {
        assert_eq!(
            resolve_url(PAGE, "https://other.test/x").as_deref(),
            Some("https://other.test/x")
        );
    }

    #[test]
    fn a_protocol_relative_link_keeps_the_pages_scheme() {
        assert_eq!(
            resolve_url(PAGE, "//cdn.test/x").as_deref(),
            Some("https://cdn.test/x")
        );
    }

    #[test]
    fn a_root_relative_link_replaces_the_whole_path() {
        assert_eq!(
            resolve_url(PAGE, "/companies?p=2").as_deref(),
            Some("https://example.com/companies?p=2")
        );
    }

    /// The commonest shape in server-rendered pagination, and the one string
    /// concatenation gets wrong: it belongs to the DIRECTORY, not to the file.
    #[test]
    fn a_relative_link_resolves_against_the_directory() {
        assert_eq!(
            resolve_url(PAGE, "page2.html").as_deref(),
            Some("https://example.com/a/b/page2.html")
        );
    }

    #[test]
    fn a_query_only_link_keeps_the_path_and_replaces_the_query() {
        assert_eq!(
            resolve_url(PAGE, "?p=2").as_deref(),
            Some("https://example.com/a/b/list.html?p=2")
        );
    }

    #[test]
    fn dot_segments_are_collapsed() {
        assert_eq!(
            resolve_url(PAGE, "../c/page2.html").as_deref(),
            Some("https://example.com/a/c/page2.html")
        );
        assert_eq!(
            resolve_url(PAGE, "./page2.html").as_deref(),
            Some("https://example.com/a/b/page2.html")
        );
    }

    /// Climbing past the root is not a URL any server can serve, so it is
    /// clamped rather than sent.
    #[test]
    fn climbing_past_the_root_is_clamped() {
        assert_eq!(
            resolve_url(PAGE, "../../../../x").as_deref(),
            Some("https://example.com/x")
        );
    }

    #[test]
    fn nothing_usable_is_none_rather_than_an_error() {
        assert_eq!(resolve_url(PAGE, "   "), None);
        assert_eq!(resolve_url("not a url", "x"), None);
    }
}
