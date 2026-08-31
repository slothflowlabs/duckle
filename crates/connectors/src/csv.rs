//! CSV (and TSV) source connector.
//!
//! Reads the header row, scans up to `sample_rows` records, infers
//! per-column types, and returns a [`duckle_plugin_sdk::Inspection`]
//! with the schema plus the sampled rows as JSON values for preview.

use async_trait::async_trait;
use duckle_metadata::{Column, DataType};
use duckle_plugin_sdk::{Connector, ConnectorKind, Inspection, InspectError, SchemaInspector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

const DEFAULT_SAMPLE_ROWS: usize = 200;

/// Cap the bytes read during inspection so a multi-GB CSV is not fully loaded
/// into memory just to sample the first rows.
const MAX_INSPECT_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvOptions {
    pub path: String,
    #[serde(default = "default_has_header", alias = "hasHeader")]
    pub has_header: bool,
    #[serde(default = "default_delimiter")]
    pub delimiter: String,
    #[serde(default = "default_quote_char", alias = "quoteChar")]
    pub quote_char: String,
    #[serde(default = "default_encoding")]
    pub encoding: String,
    #[serde(default, alias = "skipLines")]
    pub skip_lines: usize,
    #[serde(default = "default_sample_rows", alias = "sampleRows")]
    pub sample_rows: usize,
    #[serde(default, alias = "nullValue")]
    pub null_value: Option<String>,
}

fn default_has_header() -> bool {
    true
}
fn default_delimiter() -> String {
    ",".into()
}
fn default_quote_char() -> String {
    "\"".into()
}
fn default_encoding() -> String {
    "utf-8".into()
}
fn default_sample_rows() -> usize {
    DEFAULT_SAMPLE_ROWS
}

/// CSV source connector. Stateless - one instance handles all CSV
/// schema inspections.
pub struct CsvConnector;

impl CsvConnector {
    pub const COMPONENT_ID: &'static str = "src.csv";
}

#[async_trait]
impl SchemaInspector for CsvConnector {
    fn component_id(&self) -> &str {
        Self::COMPONENT_ID
    }

    async fn inspect(&self, config: JsonValue) -> Result<Inspection, InspectError> {
        let opts: CsvOptions = serde_json::from_value(config)
            .map_err(|e| InspectError::Config(e.to_string()))?;
        // Run the synchronous CSV parsing on a blocking task so we don't
        // stall the Tokio runtime if the file is large.
        let inspection = tokio::task::spawn_blocking(move || inspect_csv(opts))
            .await
            .map_err(|e| InspectError::Other(e.to_string()))??;
        Ok(inspection)
    }
}

#[async_trait]
impl Connector for CsvConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Source
    }
}

fn inspect_csv(opts: CsvOptions) -> Result<Inspection, InspectError> {
    let path = std::path::PathBuf::from(&opts.path);
    if !path.exists() {
        return Err(InspectError::Config(format!(
            "File does not exist: {}",
            opts.path
        )));
    }

    // Decode the file body up front via encoding_rs so we can support
    // utf-16 / latin-1 / windows-1252 in addition to utf-8. Inspection only
    // needs the header plus a sample, so cap the read to avoid loading a
    // multi-GB file into memory just to look at the first few rows.
    let mut raw = Vec::new();
    {
        let file = File::open(&path)?;
        BufReader::new(file)
            .take(MAX_INSPECT_BYTES)
            .read_to_end(&mut raw)?;
    }
    let truncated = raw.len() as u64 == MAX_INSPECT_BYTES;
    let encoding = encoding_rs::Encoding::for_label(opts.encoding.as_bytes())
        .ok_or_else(|| InspectError::Config(format!("Unknown encoding {}", opts.encoding)))?;
    // encoding_rs substitutes U+FFFD for malformed bytes rather than failing,
    // which is fine for inspection - the had_errors flag is intentionally
    // ignored.
    let (decoded, _, _had_errors) = encoding.decode(&raw);
    let mut text = decoded.into_owned();
    // If we hit the byte cap the final line is probably partial - drop it so
    // we never infer from or preview a truncated record.
    if truncated {
        if let Some(nl) = text.rfind('\n') {
            text.truncate(nl);
        }
    }

    // Skip leading lines by slicing past the first N newlines - no per-line
    // allocation.
    let body = skip_lines_slice(&text, opts.skip_lines);

    let delim = parse_single_byte_option(&opts.delimiter, "delimiter")?;
    let quote = parse_quote_option(&opts.quote_char)?;

    let mut reader = reader_builder(opts.has_header, delim, quote).from_reader(body.as_bytes());

    let raw_headers: Vec<String> = if opts.has_header {
        reader
            .headers()
            .map_err(|e| InspectError::Parse(format!("Header parse: {}", e)))?
            .iter()
            .map(String::from)
            .collect()
    } else {
        // For headerless files, peek the first row to determine width, then
        // rebuild the reader so that first row is still read as a sample.
        let width = reader
            .records()
            .next()
            .ok_or_else(|| InspectError::Parse("Empty file".into()))?
            .map_err(|e| InspectError::Parse(format!("Row parse: {}", e)))?
            .len();
        reader = reader_builder(false, delim, quote).from_reader(body.as_bytes());
        (0..width).map(|i| format!("col_{}", i + 1)).collect()
    };
    // Empty or duplicate header names lose preview data and confuse downstream
    // column references; normalise them to unique, non-empty names.
    let headers = sanitize_headers(raw_headers);

    let null_sentinel = opts.null_value.clone();
    let mut samples: Vec<csv::StringRecord> = Vec::with_capacity(opts.sample_rows);
    for (i, result) in reader.records().enumerate() {
        if i >= opts.sample_rows {
            break;
        }
        let record =
            result.map_err(|e| InspectError::Parse(format!("Row {} parse: {}", i + 1, e)))?;
        samples.push(record);
    }

    let columns: Vec<Column> = headers
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let inferred = infer_column_type(&samples, idx, null_sentinel.as_deref());
            Column {
                tags: Vec::new(),
                name: name.clone(),
                data_type: inferred,
                nullable: column_has_nulls(&samples, idx, null_sentinel.as_deref()),
                primary_key: None,
                format: None,
            }
        })
        .collect();

    let preview_rows = samples
        .iter()
        .take(8)
        .map(|record| build_preview_row(&headers, record, null_sentinel.as_deref(), &columns))
        .collect();

    Ok(Inspection {
        schema: columns,
        sample_rows: preview_rows,
    })
}

fn cell_value<'a>(record: &'a csv::StringRecord, idx: usize) -> Option<&'a str> {
    record.get(idx)
}

fn is_null(cell: &str, sentinel: Option<&str>) -> bool {
    let trimmed = cell.trim();
    if trimmed.is_empty() {
        return true;
    }
    if let Some(s) = sentinel {
        if trimmed == s {
            return true;
        }
    }
    matches!(trimmed.to_ascii_lowercase().as_str(), "null" | "na" | "n/a")
}

fn column_has_nulls(
    samples: &[csv::StringRecord],
    idx: usize,
    sentinel: Option<&str>,
) -> bool {
    samples
        .iter()
        .any(|r| cell_value(r, idx).map_or(true, |c| is_null(c, sentinel)))
}

fn infer_column_type(
    samples: &[csv::StringRecord],
    idx: usize,
    sentinel: Option<&str>,
) -> DataType {
    let mut has_value = false;
    let mut all_int = true;
    let mut all_float = true;
    let mut all_bool = true;
    let mut saw_named_bool = false;
    let mut all_date = true;
    let mut all_timestamp = true;

    for record in samples {
        let Some(raw) = cell_value(record, idx) else {
            continue;
        };
        if is_null(raw, sentinel) {
            continue;
        }
        let v = raw.trim();
        has_value = true;

        if all_int && v.parse::<i64>().is_err() {
            all_int = false;
        }
        if all_float && parse_finite_f64(v).is_none() {
            all_float = false;
        }
        if all_bool {
            match v.to_ascii_lowercase().as_str() {
                "true" | "false" | "yes" | "no" => saw_named_bool = true,
                "0" | "1" => {}
                _ => all_bool = false,
            }
        }
        if all_date && !is_date_like(v) {
            all_date = false;
        }
        if all_timestamp && !is_timestamp_like(v) {
            all_timestamp = false;
        }
    }

    if !has_value {
        return DataType::String;
    }
    // Order matters: timestamp before date (more specific). For numbers Int64
    // wins over Bool for ambiguous "0"/"1" columns - a column is only Bool when
    // at least one explicit true/false/yes/no token appears, otherwise binary
    // numeric flags would be mis-typed as booleans.
    if all_timestamp {
        return DataType::Timestamp;
    }
    if all_date {
        return DataType::Date;
    }
    if all_int {
        return DataType::Int64;
    }
    if all_float {
        return DataType::Float64;
    }
    if all_bool && saw_named_bool {
        return DataType::Bool;
    }
    DataType::String
}

fn is_date_like(s: &str) -> bool {
    // YYYY-MM-DD
    if s.len() != 10 {
        return false;
    }
    // A real date is pure ASCII; bail before the byte-index slices below, which
    // would panic on a multibyte UTF-8 char straddling a slice boundary.
    if !s.is_ascii() {
        return false;
    }
    let bytes = s.as_bytes();
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && s[0..4].chars().all(|c| c.is_ascii_digit())
        && s[5..7].chars().all(|c| c.is_ascii_digit())
        && s[8..10].chars().all(|c| c.is_ascii_digit())
}

fn is_timestamp_like(s: &str) -> bool {
    // YYYY-MM-DD HH:MM[:SS][.fff][Z|+HH:MM]
    if s.len() < 16 {
        return false;
    }
    // ASCII guard: the `&s[..10]` / `&s[11..]` byte slices below would panic on
    // a multibyte char at the boundary, and a real timestamp is ASCII anyway.
    if !s.is_ascii() {
        return false;
    }
    if !is_date_like(&s[..10]) {
        return false;
    }
    let sep = s.as_bytes()[10];
    if sep != b'T' && sep != b' ' {
        return false;
    }
    let time = &s[11..];
    // very forgiving: just check H:M structure
    let mut parts = time.split(|c: char| c == ':' || c == '.' || c == '+' || c == '-' || c == 'Z');
    matches!(parts.next(), Some(p) if p.chars().all(|c| c.is_ascii_digit()) && (p.len() == 1 || p.len() == 2))
        && matches!(parts.next(), Some(p) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

fn build_preview_row(
    headers: &[String],
    record: &csv::StringRecord,
    sentinel: Option<&str>,
    columns: &[Column],
) -> JsonValue {
    let mut map = Map::with_capacity(headers.len());
    for (i, name) in headers.iter().enumerate() {
        let raw = cell_value(record, i).unwrap_or("");
        if is_null(raw, sentinel) {
            map.insert(name.clone(), JsonValue::Null);
            continue;
        }
        let trimmed = raw.trim();
        let parsed = match columns.get(i).map(|c| c.data_type) {
            Some(DataType::Int64) => trimmed.parse::<i64>().map(JsonValue::from).ok(),
            Some(DataType::Float64) => parse_finite_f64(trimmed).map(JsonValue::from),
            Some(DataType::Bool) => match trimmed.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Some(json!(true)),
                "false" | "0" | "no" => Some(json!(false)),
                _ => None,
            },
            _ => None,
        };
        map.insert(name.clone(), parsed.unwrap_or_else(|| JsonValue::String(trimmed.to_string())));
    }
    JsonValue::Object(map)
}

/// Parse a single-byte CSV option (delimiter / quote char). Accepts the
/// literal escape "\t" for tab. Fails loudly on empty or multi-byte input
/// rather than silently using the first byte.
fn parse_single_byte_option(value: &str, name: &str) -> Result<u8, InspectError> {
    if value == "\\t" {
        return Ok(b'\t');
    }
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(InspectError::Config(format!(
            "{} must be a single byte, got {:?}",
            name, value
        )));
    }
    Ok(bytes[0])
}

/// Parse the quote-char option. An empty value means "no quoting" (the UI's
/// "None" choice); anything else must be a single byte.
fn parse_quote_option(value: &str) -> Result<Option<u8>, InspectError> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_single_byte_option(value, "quoteChar").map(Some)
}

fn reader_builder(has_headers: bool, delim: u8, quote: Option<u8>) -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    builder
        .has_headers(has_headers)
        .delimiter(delim)
        .flexible(true);
    match quote {
        Some(q) => {
            builder.quote(q);
        }
        // An empty quote char ("None" in the UI) disables quote processing.
        None => {
            builder.quoting(false);
        }
    }
    builder
}

/// Return the slice of `text` after the first `lines_to_skip` newlines without
/// allocating a new String.
fn skip_lines_slice(text: &str, lines_to_skip: usize) -> &str {
    if lines_to_skip == 0 {
        return text;
    }
    let mut skipped = 0;
    for (idx, ch) in text.char_indices() {
        if ch == '\n' {
            skipped += 1;
            if skipped == lines_to_skip {
                return &text[idx + 1..];
            }
        }
    }
    ""
}

/// Replace empty header names with col_N and disambiguate duplicates with a
/// numeric suffix so preview rows never collide and downstream column
/// references stay unambiguous.
fn sanitize_headers(headers: Vec<String>) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::with_capacity(headers.len());
    for (i, h) in headers.into_iter().enumerate() {
        let base = if h.trim().is_empty() {
            format!("col_{}", i + 1)
        } else {
            h
        };
        let count = seen.entry(base.clone()).or_insert(0);
        *count += 1;
        if *count == 1 {
            out.push(base);
        } else {
            out.push(format!("{}_{}", base, count));
        }
    }
    out
}

/// Parse a float, rejecting non-finite values (NaN / inf) which JSON cannot
/// represent and which should not classify a column as Float64.
fn parse_finite_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok().filter(|n| n.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_csv(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[tokio::test]
    async fn infers_basic_schema() {
        let f = write_csv(
            "order_id,status,amount,created_at\n\
             1001,paid,129.95,2026-05-18\n\
             1002,pending,49.00,2026-05-18\n\
             1003,paid,12.50,2026-05-19\n",
        );
        let cfg = serde_json::json!({ "path": f.path().to_str().unwrap() });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        let schema = &inspection.schema;
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[0].name, "order_id");
        assert_eq!(schema[0].data_type, DataType::Int64);
        assert_eq!(schema[1].data_type, DataType::String);
        assert_eq!(schema[2].data_type, DataType::Float64);
        assert_eq!(schema[3].data_type, DataType::Date);
        assert_eq!(inspection.sample_rows.len(), 3);
    }

    #[tokio::test]
    async fn handles_null_sentinel() {
        let f = write_csv(
            "id,amount\n\
             1,100\n\
             2,NA\n\
             3,200\n",
        );
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "nullValue": "NA",
        });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        assert_eq!(inspection.schema[1].data_type, DataType::Int64);
        assert!(inspection.schema[1].nullable);
    }

    #[tokio::test]
    async fn missing_file_errors() {
        let cfg = serde_json::json!({ "path": "/nonexistent/path/orders.csv" });
        let err = CsvConnector.inspect(cfg).await.unwrap_err();
        assert!(matches!(err, InspectError::Config(_)));
    }

    #[tokio::test]
    async fn headerless_csv_synthesizes_names() {
        let f = write_csv("1,alice,10.5\n2,bob,20.0\n");
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "hasHeader": false,
        });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        let names: Vec<_> = inspection.schema.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["col_1", "col_2", "col_3"]);
        assert_eq!(inspection.schema[0].data_type, DataType::Int64);
        assert_eq!(inspection.schema[1].data_type, DataType::String);
        assert_eq!(inspection.schema[2].data_type, DataType::Float64);
        // The first row must still be sampled (not consumed as a header).
        assert_eq!(inspection.sample_rows.len(), 2);
    }

    #[tokio::test]
    async fn custom_delimiter() {
        let f = write_csv("id;name\n1;alice\n2;bob\n");
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "delimiter": ";",
        });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        assert_eq!(inspection.schema.len(), 2);
        assert_eq!(inspection.schema[0].name, "id");
        assert_eq!(inspection.schema[1].name, "name");
    }

    #[tokio::test]
    async fn tab_delimiter_literal_escape() {
        let f = write_csv("id\tname\n1\talice\n2\tbob\n");
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "delimiter": "\\t",
        });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        assert_eq!(inspection.schema.len(), 2);
        assert_eq!(inspection.schema[0].name, "id");
        assert_eq!(inspection.schema[0].data_type, DataType::Int64);
    }

    #[tokio::test]
    async fn skip_lines_drops_preamble() {
        let f = write_csv("# generated report\n# 2026-05-30\nid,name\n1,alice\n2,bob\n");
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "skipLines": 2,
        });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        let names: Vec<_> = inspection.schema.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
    }

    #[tokio::test]
    async fn duplicate_headers_disambiguated() {
        let f = write_csv("a,a,b\n1,2,3\n");
        let cfg = serde_json::json!({ "path": f.path().to_str().unwrap() });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        let names: Vec<_> = inspection.schema.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["a", "a_2", "b"]);
        // Preview keeps all three values rather than the later "a" overwriting.
        let row = &inspection.sample_rows[0];
        assert_eq!(row["a"], serde_json::json!(1));
        assert_eq!(row["a_2"], serde_json::json!(2));
        assert_eq!(row["b"], serde_json::json!(3));
    }

    #[tokio::test]
    async fn empty_header_names_filled() {
        let f = write_csv("id,,amount\n1,x,9\n");
        let cfg = serde_json::json!({ "path": f.path().to_str().unwrap() });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        let names: Vec<_> = inspection.schema.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "col_2", "amount"]);
    }

    #[tokio::test]
    async fn invalid_delimiter_errors() {
        let f = write_csv("a;;b\n1;;2\n");
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "delimiter": ";;",
        });
        let err = CsvConnector.inspect(cfg).await.unwrap_err();
        assert!(matches!(err, InspectError::Config(_)));
    }

    #[tokio::test]
    async fn bool_vs_int_inference() {
        let f = write_csv("flag,bin\ntrue,0\nfalse,1\n");
        let cfg = serde_json::json!({ "path": f.path().to_str().unwrap() });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        // Named booleans -> Bool; ambiguous 0/1 stays numeric.
        assert_eq!(inspection.schema[0].data_type, DataType::Bool);
        assert_eq!(inspection.schema[1].data_type, DataType::Int64);
    }

    #[tokio::test]
    async fn empty_quote_char_disables_quoting() {
        // The UI "None" quote option sends an empty string; it must not error.
        let f = write_csv("id,name\n1,alice\n");
        let cfg = serde_json::json!({
            "path": f.path().to_str().unwrap(),
            "quoteChar": "",
        });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        assert_eq!(inspection.schema.len(), 2);
    }

    #[tokio::test]
    async fn non_finite_floats_not_float64() {
        let f = write_csv("x\n1.5\nNaN\n");
        let cfg = serde_json::json!({ "path": f.path().to_str().unwrap() });
        let inspection = CsvConnector.inspect(cfg).await.unwrap();
        // A column containing NaN must not be classified as Float64.
        assert_eq!(inspection.schema[0].data_type, DataType::String);
    }
}
