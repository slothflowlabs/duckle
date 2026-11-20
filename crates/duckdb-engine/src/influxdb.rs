//! InfluxDB for src.influxdb and snk.influxdb, over its HTTP API.
//!
//! src.influxdb asks InfluxDB 3 for SQL results as Parquet, so every column
//! arrives with the type InfluxDB keeps it as; or asks any InfluxDB (1.x, 2.x
//! with a DBRP mapping, 3) for InfluxQL results through the v1 /query endpoint,
//! which answers in JSON. snk.influxdb writes line protocol to /api/v2/write,
//! which InfluxDB 2, InfluxDB 3 and InfluxDB 1.8 and later all take.

use crate::orc::{sql_path, temp_beside};
use crate::*;
use std::io::Read;

/// Lines in one write.
const BATCH_LINES: usize = 5000;
const RETRIES: u32 = 4;

/// `s` with each of `specials` escaped by a backslash, as line protocol wants.
fn escape(s: &str, specials: &[char]) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if specials.contains(&c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// A measurement name: commas and spaces escaped.
pub(crate) fn measurement_text(s: &str) -> String {
    escape(s, &[',', ' '])
}

/// A tag key, tag value or field key: commas, equals signs and spaces escaped.
pub(crate) fn key_text(s: &str) -> String {
    escape(s, &[',', '=', ' '])
}

/// A string field value: quoted, its quotes and backslashes escaped.
pub(crate) fn string_text(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// How a DuckDB column goes into a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldKind {
    Int,
    UInt,
    Float,
    Bool,
    Text,
}

pub(crate) fn field_kind(duckdb_type: &str) -> FieldKind {
    match duckdb_type.trim() {
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" => FieldKind::Int,
        "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" | "UHUGEINT" => FieldKind::UInt,
        "FLOAT" | "DOUBLE" => FieldKind::Float,
        t if t.starts_with("DECIMAL") => FieldKind::Float,
        "BOOLEAN" => FieldKind::Bool,
        _ => FieldKind::Text,
    }
}

/// One field value in line protocol, or why it cannot be one.
pub(crate) fn field_text(kind: FieldKind, v: &JsonValue) -> Result<Option<String>, String> {
    let as_text = |v: &JsonValue| match v {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    };
    if v.is_null() {
        return Ok(None);
    }
    Ok(Some(match kind {
        FieldKind::Int => format!("{}i", as_text(v).parse::<i64>().map_err(|_| format!("{} is not a 64-bit integer", as_text(v)))?),
        FieldKind::UInt => format!("{}u", as_text(v).parse::<u64>().map_err(|_| format!("{} is not a 64-bit unsigned integer", as_text(v)))?),
        FieldKind::Float => {
            let x: f64 = as_text(v).parse().map_err(|_| format!("{} is not a number", as_text(v)))?;
            if !x.is_finite() {
                return Err(format!("{x} is not a value line protocol can carry"));
            }
            as_text(v)
        }
        FieldKind::Bool => match v {
            JsonValue::Bool(b) => b.to_string(),
            other => return Err(format!("{other} is not a boolean")),
        },
        FieldKind::Text => {
            let s = as_text(v);
            if s.contains('\n') {
                return Err("a value holds a newline, which line protocol cannot carry".into());
            }
            string_text(&s)
        }
    }))
}

/// The SQL that turns the time column into whole units of `precision`.
pub(crate) fn time_expr(column: &str, duckdb_type: &str, precision: &str) -> Result<String, String> {
    let q = plan::quote_ident(column);
    let t = duckdb_type.trim();
    if t.starts_with("TIMESTAMP") || t == "DATE" {
        let ns = format!("epoch_ns(CAST({q} AS TIMESTAMP_NS))");
        let divisor = match precision {
            "s" => 1_000_000_000,
            "ms" => 1_000_000,
            "us" => 1_000,
            _ => 1,
        };
        return Ok(if divisor == 1 { ns } else { format!("({ns} // {divisor})") });
    }
    if matches!(field_kind(t), FieldKind::Int | FieldKind::UInt) {
        // Already a count of `precision` units since the epoch.
        return Ok(q);
    }
    Err(format!("the time column {column} is a {t}; give it a timestamp, a date or a whole number"))
}

/// The error InfluxDB gave, from its JSON body or as the text it sent.
pub(crate) fn said(body: &str) -> String {
    serde_json::from_str::<JsonValue>(body)
        .ok()
        .and_then(|v| v.get("message").or_else(|| v.get("error")).and_then(JsonValue::as_str).map(str::to_string))
        .unwrap_or_else(|| body.trim().chars().take(500).collect())
}

fn call(
    engine: &DuckdbEngine,
    method: &str,
    url: &str,
    auth: Option<String>,
    content_type: &str,
    body: &str,
) -> Result<ureq::Response, EngineError> {
    let mut attempt = 0;
    loop {
        engine.check_cancelled()?;
        let mut req = crate::tls::http_agent().request(method, url).set("Content-Type", content_type);
        if let Some(a) = &auth {
            req = req.set("Authorization", a);
        }
        let sent = if method == "GET" { req.call() } else { req.send_string(body) };
        match sent {
            Ok(r) => return Ok(r),
            Err(ureq::Error::Status(code, _)) if matches!(code, 429 | 503) && attempt < RETRIES => {
                engine.ai_sleep_cancellable(200 << attempt)?;
                attempt += 1;
            }
            Err(ureq::Error::Status(code, r)) => {
                return Err(EngineError::Query(format!(
                    "influxdb: {url} was refused (HTTP {code}): {}",
                    said(&r.into_string().unwrap_or_default())
                )))
            }
            Err(e) => return Err(EngineError::Query(format!("influxdb: {url}: {e}"))),
        }
    }
}

/// InfluxQL's answer as rows, a column for the measurement and each tag, then
/// the query's columns; numbers as DOUBLE, since the JSON does not say which
/// were integers. A result the server cut short is refused.
pub(crate) fn influxql_rows(body: &JsonValue) -> Result<(Vec<JsonValue>, Vec<(String, &'static str)>), String> {
    let mut rows = Vec::new();
    let mut columns: Vec<(String, &'static str)> = vec![("measurement".into(), "VARCHAR")];
    let add = |name: &str, ty: &'static str, columns: &mut Vec<(String, &'static str)>| {
        if !columns.iter().any(|(n, _)| n == name) {
            columns.push((name.to_string(), ty));
        }
    };
    for result in body.get("results").and_then(JsonValue::as_array).cloned().unwrap_or_default() {
        if let Some(e) = result.get("error").and_then(JsonValue::as_str) {
            return Err(e.to_string());
        }
        if result.get("partial").and_then(JsonValue::as_bool) == Some(true) {
            return Err("the server cut the result short (partial); narrow the query or raise its max-row-limit".into());
        }
        for series in result.get("series").and_then(JsonValue::as_array).cloned().unwrap_or_default() {
            if series.get("partial").and_then(JsonValue::as_bool) == Some(true) {
                return Err("the server cut the result short (partial); narrow the query or raise its max-row-limit".into());
            }
            let name = series.get("name").and_then(JsonValue::as_str).unwrap_or_default().to_string();
            let tags = series.get("tags").and_then(JsonValue::as_object).cloned().unwrap_or_default();
            for k in tags.keys() {
                add(k, "VARCHAR", &mut columns);
            }
            let cols: Vec<String> = series
                .get("columns")
                .and_then(JsonValue::as_array)
                .map(|a| a.iter().filter_map(|c| c.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            for values in series.get("values").and_then(JsonValue::as_array).cloned().unwrap_or_default() {
                let mut row = serde_json::Map::new();
                row.insert("measurement".into(), JsonValue::String(name.clone()));
                for (k, v) in &tags {
                    row.insert(k.clone(), v.clone());
                }
                for (i, c) in cols.iter().enumerate() {
                    let v = values.get(i).cloned().unwrap_or(JsonValue::Null);
                    let ty = match (c.as_str(), &v) {
                        ("time", _) => "TIMESTAMP_NS",
                        (_, JsonValue::Number(_)) => "DOUBLE",
                        (_, JsonValue::Bool(_)) => "BOOLEAN",
                        (_, JsonValue::Null) => "",
                        _ => "VARCHAR",
                    };
                    if !ty.is_empty() {
                        add(c, ty, &mut columns);
                    }
                    let v = match (c.as_str(), v) {
                        // epoch=ns: nanoseconds since the epoch, as text DuckDB casts.
                        ("time", JsonValue::Number(n)) => n
                            .as_i64()
                            .and_then(|ns| chrono::DateTime::from_timestamp(ns.div_euclid(1_000_000_000), ns.rem_euclid(1_000_000_000) as u32))
                            .map(|t| JsonValue::String(t.format("%Y-%m-%d %H:%M:%S%.9f").to_string()))
                            .unwrap_or(JsonValue::Null),
                        (_, JsonValue::String(s)) => JsonValue::String(s),
                        (_, JsonValue::Null) => JsonValue::Null,
                        (_, other) => JsonValue::String(other.to_string()),
                    };
                    row.insert(c.clone(), v);
                }
                rows.push(JsonValue::Object(row));
            }
        }
    }
    Ok((rows, columns))
}

impl DuckdbEngine {
    pub(crate) fn run_influxdb_source(&self, db: &Path, spec: &plan::InfluxSourceSpec) -> Result<String, EngineError> {
        if spec.influxql {
            return self.influxql_source(db, spec);
        }
        // SQL on InfluxDB 3, answered as Parquet so every column keeps its type.
        let url = format!("{}/api/v3/query_sql", spec.base_url);
        let body = serde_json::json!({ "db": spec.database, "q": spec.query, "format": "parquet" }).to_string();
        let auth = spec.token.as_ref().map(|t| format!("Bearer {t}"));
        let resp = call(self, "POST", &url, auth, "application/json", &body)?;
        let mut bytes = Vec::new();
        resp.into_reader()
            .read_to_end(&mut bytes)
            .map_err(|e| EngineError::Query(format!("influxdb: {url}: {e}")))?;
        if bytes.is_empty() {
            // No rows, and with them no schema: the node's declared one types it.
            crate::materialize_empty_result(&self.bin, db, &spec.node_id, spec.declared_schema.as_deref())?;
            return Ok(format!("influxdb: read 0 rows into {}", spec.node_id));
        }
        let parquet_path = temp_beside(db, "influxdb", &spec.node_id);
        std::fs::write(&parquet_path, &bytes).map_err(|e| EngineError::Query(format!("influxdb: temp file: {e}")))?;
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            sql_path(&parquet_path)
        );
        let created = self.run(Some(db), &create, false);
        let _ = std::fs::remove_file(&parquet_path);
        created?;
        Ok(format!("influxdb: read the SQL result into {}", spec.node_id))
    }

    fn influxql_source(&self, db: &Path, spec: &plan::InfluxSourceSpec) -> Result<String, EngineError> {
        let mut url = url::Url::parse(&format!("{}/query", spec.base_url)).map_err(|e| EngineError::Query(format!("influxdb: {e}")))?;
        url.query_pairs_mut().append_pair("db", &spec.database).append_pair("q", &spec.query).append_pair("epoch", "ns");
        let auth = spec.token.as_ref().map(|t| format!("Token {t}"));
        let text = call(self, "GET", url.as_str(), auth, "application/json", "")?
            .into_string()
            .map_err(|e| EngineError::Query(format!("influxdb: {e}")))?;
        let body: JsonValue = serde_json::from_str(&text).map_err(|e| EngineError::Query(format!("influxdb: an answer that is not JSON: {e}")))?;
        let (rows, columns) = influxql_rows(&body).map_err(|e| EngineError::Query(format!("influxdb: {e}")))?;
        if rows.is_empty() {
            crate::materialize_empty_result(&self.bin, db, &spec.node_id, spec.declared_schema.as_deref())?;
            return Ok(format!("influxdb: read 0 rows into {}", spec.node_id));
        }
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        for row in &rows {
            writer.write_row(row)?;
        }
        let spec_cols: Vec<String> = columns.iter().map(|(n, _)| format!("'{}': 'VARCHAR'", n.replace('\'', "''"))).collect();
        let select: Vec<String> = columns
            .iter()
            .map(|(n, ty)| {
                let q = plan::quote_ident(n);
                if *ty == "VARCHAR" { q } else { format!("CAST({q} AS {ty}) AS {q}") }
            })
            .collect();
        writer.finalize_typed(&self.bin, db, &spec.node_id, &spec_cols.join(", "), &select.join(", "))?;
        Ok(format!("influxdb: read {} rows into {}", rows.len(), spec.node_id))
    }

    /// snk.influxdb: each upstream row as one line. Every line is built before
    /// the first write, so a row that cannot be one sends nothing.
    pub(crate) fn run_influxdb_sink(&self, db: &Path, spec: &plan::InfluxSinkSpec) -> Result<String, EngineError> {
        let view = plan::quote_ident(&spec.from_view);
        let described = self.run_rows(Some(db), &format!("DESCRIBE SELECT * FROM {view}"))?;
        let types: Vec<(String, String)> = described
            .iter()
            .map(|c| {
                (
                    c.get("column_name").and_then(JsonValue::as_str).unwrap_or_default().to_string(),
                    c.get("column_type").and_then(JsonValue::as_str).unwrap_or_default().to_string(),
                )
            })
            .collect();
        let type_of = |name: &str| types.iter().find(|(n, _)| n == name).map(|(_, t)| t.clone());
        let fail = |e: String| EngineError::Query(format!("snk.influxdb: {e}"));
        for c in spec.tags.iter().chain(&spec.fields).chain(&spec.measurement_column).chain(&spec.time_column) {
            if type_of(c).is_none() {
                return Err(fail(format!("there is no column {c} upstream")));
            }
        }
        let fields: Vec<String> = if spec.fields.is_empty() {
            types
                .iter()
                .map(|(n, _)| n.clone())
                .filter(|n| !spec.tags.contains(n) && Some(n) != spec.measurement_column.as_ref() && Some(n) != spec.time_column.as_ref())
                .collect()
        } else {
            spec.fields.clone()
        };
        let mut select: Vec<String> = Vec::new();
        if let Some(m) = &spec.measurement_column {
            select.push(format!("CAST({} AS VARCHAR) AS \"__m\"", plan::quote_ident(m)));
        }
        for t in &spec.tags {
            select.push(format!("CAST({} AS VARCHAR) AS {}", plan::quote_ident(t), plan::quote_ident(&format!("__t_{t}"))));
        }
        for f in &fields {
            select.push(format!("{} AS {}", plan::quote_ident(f), plan::quote_ident(&format!("__f_{f}"))));
        }
        if let Some(t) = &spec.time_column {
            select.push(format!("{} AS \"__time\"", time_expr(t, &type_of(t).unwrap_or_default(), &spec.precision).map_err(fail)?));
        }
        let rows = self.run_rows(Some(db), &format!("SELECT {} FROM {view}", select.join(", ")))?;
        let kinds: Vec<FieldKind> = fields.iter().map(|f| field_kind(&type_of(f).unwrap_or_default())).collect();
        let mut lines = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let at = |why: String| fail(format!("row {}: {why}", i + 1));
            let name = match &spec.measurement_column {
                Some(_) => row.get("__m").and_then(JsonValue::as_str).filter(|s| !s.is_empty()).map(str::to_string)
                    .ok_or_else(|| at("it has no measurement".into()))?,
                None => spec.measurement.clone().unwrap_or_default(),
            };
            let mut line = measurement_text(&name);
            for t in &spec.tags {
                match row.get(&format!("__t_{t}")).and_then(JsonValue::as_str).filter(|s| !s.is_empty()) {
                    Some(v) if v.contains('\n') => return Err(at(format!("the tag {t} holds a newline, which line protocol cannot carry"))),
                    Some(v) => line.push_str(&format!(",{}={}", key_text(t), key_text(v))),
                    None => {}
                }
            }
            let mut written = Vec::new();
            for (f, kind) in fields.iter().zip(&kinds) {
                let v = row.get(&format!("__f_{f}")).cloned().unwrap_or(JsonValue::Null);
                if let Some(text) = field_text(*kind, &v).map_err(|e| at(format!("{f}: {e}")))? {
                    written.push(format!("{}={text}", key_text(f)));
                }
            }
            if written.is_empty() {
                return Err(at("every field is empty, and a line needs one".into()));
            }
            line.push(' ');
            line.push_str(&written.join(","));
            if spec.time_column.is_some() {
                match row.get("__time") {
                    Some(JsonValue::Null) | None => {}
                    Some(t) => line.push_str(&format!(" {}", t.as_str().map(str::to_string).unwrap_or_else(|| t.to_string()))),
                }
            }
            lines.push(line);
        }
        let mut url = url::Url::parse(&format!("{}/api/v2/write", spec.base_url)).map_err(|e| fail(e.to_string()))?;
        url.query_pairs_mut().append_pair("bucket", &spec.database).append_pair("precision", &spec.precision);
        if let Some(org) = &spec.org {
            url.query_pairs_mut().append_pair("org", org);
        }
        let auth = spec.token.as_ref().map(|t| format!("Token {t}"));
        for batch in lines.chunks(BATCH_LINES) {
            call(self, "POST", url.as_str(), auth.clone(), "text/plain; charset=utf-8", &batch.join("\n"))?;
        }
        Ok(format!("influxdb: wrote {} line(s) to {}", lines.len(), spec.database))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_protocol_escapes_what_each_part_must() {
        assert_eq!(measurement_text("cpu load,x"), "cpu\\ load\\,x");
        assert_eq!(key_text("a b=c,d"), "a\\ b\\=c\\,d");
        assert_eq!(string_text("say \"hi\" \\o/"), "\"say \\\"hi\\\" \\\\o/\"");
    }

    #[test]
    fn each_field_kind_is_written_as_line_protocol_types_it() {
        assert_eq!(field_kind("BIGINT"), FieldKind::Int);
        assert_eq!(field_kind("UBIGINT"), FieldKind::UInt);
        assert_eq!(field_kind("DECIMAL(10,2)"), FieldKind::Float);
        assert_eq!(field_kind("TIMESTAMP"), FieldKind::Text);
        assert_eq!(field_text(FieldKind::Int, &serde_json::json!(3)).unwrap().as_deref(), Some("3i"));
        assert_eq!(field_text(FieldKind::UInt, &serde_json::json!(3)).unwrap().as_deref(), Some("3u"));
        assert_eq!(field_text(FieldKind::Float, &serde_json::json!(1.5)).unwrap().as_deref(), Some("1.5"));
        assert_eq!(field_text(FieldKind::Bool, &serde_json::json!(true)).unwrap().as_deref(), Some("true"));
        assert_eq!(field_text(FieldKind::Text, &serde_json::json!("a\"b")).unwrap().as_deref(), Some("\"a\\\"b\""));
        assert_eq!(field_text(FieldKind::Int, &JsonValue::Null).unwrap(), None);
        assert!(field_text(FieldKind::Text, &serde_json::json!("a\nb")).is_err());
        assert!(field_text(FieldKind::Float, &serde_json::json!("nan")).is_err());
    }

    #[test]
    fn a_time_column_becomes_whole_units_of_the_precision() {
        assert_eq!(time_expr("t", "TIMESTAMP", "ns").unwrap(), "epoch_ns(CAST(\"t\" AS TIMESTAMP_NS))");
        assert_eq!(time_expr("t", "TIMESTAMP WITH TIME ZONE", "ms").unwrap(), "(epoch_ns(CAST(\"t\" AS TIMESTAMP_NS)) // 1000000)");
        assert_eq!(time_expr("t", "BIGINT", "s").unwrap(), "\"t\"");
        assert!(time_expr("t", "VARCHAR", "ns").unwrap_err().contains("VARCHAR"));
    }

    #[test]
    fn influxql_series_become_rows_and_a_cut_result_is_refused() {
        let body = serde_json::json!({"results": [{"statement_id": 0, "series": [
            {"name": "cpu", "tags": {"host": "a"}, "columns": ["time", "usage", "note"], "values": [[1795082400000000001_i64, 1.5, "x"]]}
        ]}]});
        let (rows, columns) = influxql_rows(&body).unwrap();
        assert_eq!(rows[0]["measurement"], "cpu");
        assert_eq!(rows[0]["host"], "a");
        assert_eq!(rows[0]["time"], "2026-11-19 10:00:00.000000001");
        assert_eq!(rows[0]["usage"], "1.5");
        let names: Vec<String> = columns.iter().map(|(n, t)| format!("{n} {t}")).collect();
        assert_eq!(names, ["measurement VARCHAR", "host VARCHAR", "time TIMESTAMP_NS", "usage DOUBLE", "note VARCHAR"]);
        let cut = serde_json::json!({"results": [{"statement_id": 0, "partial": true, "series": []}]});
        assert!(influxql_rows(&cut).unwrap_err().contains("partial"));
        let failed = serde_json::json!({"results": [{"statement_id": 0, "error": "database not found: nope"}]});
        assert_eq!(influxql_rows(&failed).unwrap_err(), "database not found: nope");
    }

    #[test]
    fn an_error_body_gives_its_message() {
        assert_eq!(said(r#"{"code":"invalid","message":"line protocol parse failed"}"#), "line protocol parse failed");
        assert_eq!(said(r#"{"error":"the request was not authenticated"}"#), "the request was not authenticated");
        assert_eq!(said("Schema error: No field named nope."), "Schema error: No field named nope.");
    }
}
