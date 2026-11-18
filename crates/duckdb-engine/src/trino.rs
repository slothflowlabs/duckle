//! Trino, Starburst and Presto through the client REST protocol: the statement
//! is POSTed to /v1/statement, then each answer's nextUri is followed until
//! there is none. Columns are typed from the types the server reports, so an
//! empty result is a table of its columns too.

use crate::*;

/// How many times a busy server (502, 503, 504) is asked again, as the
/// protocol tells a client to.
const BUSY_RETRIES: u32 = 10;

/// How a column of a Trino type is read from the answer's JSON, and the SQL
/// that makes the DuckDB type of it. `{c}` is the cell.
fn trino_column(trino_type: &str) -> (&'static str, String) {
    let t = trino_type.trim().to_ascii_lowercase();
    let base = t.split('(').next().unwrap_or("").trim();
    let precision = t
        .split_once('(')
        .and_then(|(_, rest)| rest.split(|c| c == ')' || c == ',').next())
        .and_then(|p| p.trim().parse::<u32>().ok());
    let cast = |to: &str| format!("CAST({{c}} AS {to})");
    match base {
        "boolean" => ("VARCHAR", cast("BOOLEAN")),
        "tinyint" => ("VARCHAR", cast("TINYINT")),
        "smallint" => ("VARCHAR", cast("SMALLINT")),
        "integer" => ("VARCHAR", cast("INTEGER")),
        "bigint" => ("VARCHAR", cast("BIGINT")),
        "real" => ("VARCHAR", cast("FLOAT")),
        "double" => ("VARCHAR", cast("DOUBLE")),
        // decimal(p, s) as written, which DuckDB reads the same way.
        "decimal" => ("VARCHAR", cast(&t.to_ascii_uppercase())),
        "varchar" | "char" => ("VARCHAR", "{c}".to_string()),
        "varbinary" => ("VARCHAR", "from_base64({c})".to_string()),
        "date" => ("VARCHAR", cast("DATE")),
        "json" => ("VARCHAR", cast("JSON")),
        "uuid" => ("VARCHAR", cast("UUID")),
        // A zone written as an offset comes with a space before it
        // (10:30:00.000 +01:00), which DuckDB reads only without one.
        "timestamp" if t.ends_with("with time zone") => (
            "VARCHAR",
            r"CAST(regexp_replace({c}, ' ([+-][0-9][0-9]:[0-9][0-9])$', '\1') AS TIMESTAMPTZ)".to_string(),
        ),
        // Precision past the microsecond needs DuckDB's nanosecond type.
        "timestamp" if precision.unwrap_or(3) > 6 => ("VARCHAR", cast("TIMESTAMP_NS")),
        "timestamp" => ("VARCHAR", cast("TIMESTAMP")),
        "time" if !t.ends_with("with time zone") => ("VARCHAR", cast("TIME")),
        // Nested values arrive as JSON and stay JSON.
        "array" | "map" | "row" => ("JSON", "{c}".to_string()),
        // Intervals, IP addresses, a time with a zone, sketches: as text.
        _ => ("VARCHAR", "{c}".to_string()),
    }
}

/// read_json's `columns` map and the projection for a result's columns.
fn trino_columns(columns: &[(String, String)]) -> (String, String) {
    let mut read = Vec::with_capacity(columns.len());
    let mut select = Vec::with_capacity(columns.len());
    let mut taken: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (i, (name, ty)) in columns.iter().enumerate() {
        let key = format!("c{i}");
        let (read_as, expr) = trino_column(ty);
        read.push(format!("'{key}': '{read_as}'"));
        // A result can name two columns alike (a.id, b.id); a table cannot.
        let mut unique = name.clone();
        let mut n = 2;
        while !taken.insert(unique.to_ascii_lowercase()) {
            unique = format!("{name}_{n}");
            n += 1;
        }
        select.push(format!("{} AS {}", expr.replace("{c}", &plan::quote_ident(&key)), plan::quote_ident(&unique)));
    }
    (read.join(", "), select.join(", "))
}

/// One request, asked again while the server says it is busy.
fn trino_call(
    engine: &DuckdbEngine,
    spec: &plan::TrinoSourceSpec,
    method: &str,
    url: &str,
    body: Option<&str>,
) -> Result<JsonValue, EngineError> {
    let prefix = spec.flavor.header_prefix();
    let mut attempt = 0;
    loop {
        engine.check_cancelled()?;
        let mut req = crate::tls::http_agent()
            .request(method, url)
            .set(&format!("{prefix}User"), &spec.user)
            .set(&format!("{prefix}Source"), "duckle");
        if let Some(c) = &spec.catalog {
            req = req.set(&format!("{prefix}Catalog"), c);
        }
        if let Some(s) = &spec.schema {
            req = req.set(&format!("{prefix}Schema"), s);
        }
        req = match &spec.auth {
            plan::TrinoAuth::None => req,
            plan::TrinoAuth::Password(p) => {
                use base64::Engine as _;
                let pair = base64::engine::general_purpose::STANDARD.encode(format!("{}:{p}", spec.user));
                req.set("Authorization", &format!("Basic {pair}"))
            }
            plan::TrinoAuth::Token(t) => req.set("Authorization", &format!("Bearer {t}")),
        };
        let sent = match body {
            Some(text) => req.set("Content-Type", "text/plain; charset=utf-8").send_string(text),
            None => req.call(),
        };
        match sent {
            Ok(r) => return r.into_json().map_err(|e| EngineError::Query(format!("trino: {url}: {e}"))),
            Err(ureq::Error::Status(code, _)) if matches!(code, 502..=504) && attempt < BUSY_RETRIES => {
                std::thread::sleep(std::time::Duration::from_millis(100 * (1 << attempt.min(3))));
                attempt += 1;
            }
            Err(ureq::Error::Status(code, r)) => {
                let said = r.into_string().unwrap_or_default();
                let said = said.lines().next().unwrap_or("").trim();
                return Err(EngineError::Query(format!(
                    "trino: {} {url} was refused (HTTP {code}){}",
                    method,
                    if said.is_empty() { String::new() } else { format!(": {said}") }
                )));
            }
            Err(e) => return Err(EngineError::Query(format!("trino: {url}: {e}"))),
        }
    }
}

impl DuckdbEngine {
    pub(crate) fn run_trino_source(&self, db: &Path, spec: &plan::TrinoSourceSpec) -> Result<String, EngineError> {
        let start = format!("{}/v1/statement", spec.server.trim_end_matches('/'));
        let mut page = trino_call(self, spec, "POST", &start, Some(&spec.query))?;
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut columns: Option<Vec<(String, String)>> = None;
        let mut rows = 0_usize;
        loop {
            if let Some(err) = page.get("error") {
                let name = err.get("errorName").and_then(JsonValue::as_str).unwrap_or("QUERY_FAILED");
                let message = err.get("message").and_then(JsonValue::as_str).unwrap_or("no reason given");
                return Err(EngineError::Query(format!("trino: the query failed: {name}: {message}")));
            }
            if columns.is_none() {
                columns = page.get("columns").and_then(JsonValue::as_array).map(|cs| {
                    cs.iter()
                        .map(|c| {
                            let name = c.get("name").and_then(JsonValue::as_str).unwrap_or("").to_string();
                            let ty = c.get("type").and_then(JsonValue::as_str).unwrap_or("varchar").to_string();
                            (name, ty)
                        })
                        .collect()
                });
            }
            for row in page.get("data").and_then(JsonValue::as_array).cloned().unwrap_or_default() {
                let cells = row.as_array().cloned().unwrap_or_default();
                let object: serde_json::Map<String, JsonValue> =
                    cells.into_iter().enumerate().map(|(i, v)| (format!("c{i}"), v)).collect();
                writer.write_row(&JsonValue::Object(object))?;
                rows += 1;
            }
            let Some(next) = page.get("nextUri").and_then(JsonValue::as_str).map(str::to_string) else {
                break;
            };
            if self.check_cancelled().is_err() {
                // Stop the query on the server too, rather than leave it running.
                let _ = crate::tls::http_agent().delete(&next).call();
                return Err(EngineError::Cancelled);
            }
            page = trino_call(self, spec, "GET", &next, None)?;
        }
        let columns = columns.ok_or_else(|| {
            EngineError::Query("trino: the statement returned no columns; src.trino runs a query that returns rows".into())
        })?;
        let (read, select) = trino_columns(&columns);
        writer.finalize_typed(&self.bin, db, &spec.node_id, &read, &select)?;
        Ok(format!("trino: read {rows} row(s) into {}", spec.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_trino_type_is_read_as_its_duckdb_type() {
        for (t, read, expr) in [
            ("bigint", "VARCHAR", "CAST({c} AS BIGINT)"),
            ("decimal(10,2)", "VARCHAR", "CAST({c} AS DECIMAL(10,2))"),
            ("varchar(20)", "VARCHAR", "{c}"),
            ("varbinary", "VARCHAR", "from_base64({c})"),
            ("timestamp(3)", "VARCHAR", "CAST({c} AS TIMESTAMP)"),
            ("timestamp(6)", "VARCHAR", "CAST({c} AS TIMESTAMP)"),
            // Past the microsecond, DuckDB's nanosecond type keeps the digits.
            ("timestamp(9)", "VARCHAR", "CAST({c} AS TIMESTAMP_NS)"),
            ("time(3)", "VARCHAR", "CAST({c} AS TIME)"),
            // DuckDB has no type for a time of day with a zone: text.
            ("time(3) with time zone", "VARCHAR", "{c}"),
            ("interval day to second", "VARCHAR", "{c}"),
            ("array(varchar)", "JSON", "{c}"),
            ("map(varchar, integer)", "JSON", "{c}"),
            ("row(a integer, b varchar)", "JSON", "{c}"),
        ] {
            assert_eq!(trino_column(t), (read, expr.to_string()), "{t}");
        }
        assert!(trino_column("timestamp(3) with time zone").1.contains("AS TIMESTAMPTZ"));
    }
}
