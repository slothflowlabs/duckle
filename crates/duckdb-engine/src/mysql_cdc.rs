//! src.mysql.cdc: every insert, update and delete of one MySQL (or MariaDB)
//! table, read from the binary log the way a replica reads it, from the
//! position the last successful run saved to the end of the log as it stands
//! when the run asks. The position is saved only when the run succeeds, so a
//! failed run reads the same changes again. A first run starts at the end of the
//! log: load the table once (src.mysql), then read its changes from there.

use crate::*;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, OptsBuilder, SslOpts, Value};

/// A column of the captured table, as information_schema describes it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Column {
    pub name: String,
    /// DATA_TYPE: int, varchar, enum...
    pub data_type: String,
    /// COLUMN_TYPE: int unsigned, decimal(10,2), enum('s','m')...
    pub column_type: String,
}

impl Column {
    fn unsigned(&self) -> bool {
        self.column_type.contains("unsigned")
    }
}

/// What a change row's text is cast to, by the type MySQL declares the column
/// with. `{c}` is the cell.
pub(crate) fn cast_for(col: &Column) -> String {
    let cast = |to: &str| format!("CAST({{c}} AS {to})");
    // A zero date ('0000-00-00') has no DuckDB value: it reads as NULL.
    let try_cast = |to: &str| format!("TRY_CAST({{c}} AS {to})");
    let unsigned = col.unsigned();
    match col.data_type.as_str() {
        "tinyint" => cast(if unsigned { "UTINYINT" } else { "TINYINT" }),
        "smallint" => cast(if unsigned { "USMALLINT" } else { "SMALLINT" }),
        "mediumint" | "int" | "integer" => cast(if unsigned { "UINTEGER" } else { "INTEGER" }),
        "bigint" => cast(if unsigned { "UBIGINT" } else { "BIGINT" }),
        // Up to DuckDB's 38 digits as declared; wider stays text, digit for digit.
        "decimal" | "numeric" => match precision_scale(&col.column_type) {
            Some((p, s)) if p <= 38 => cast(&format!("DECIMAL({p},{s})")),
            _ => "{c}".to_string(),
        },
        "float" => cast("FLOAT"),
        "double" | "real" => cast("DOUBLE"),
        "bit" if col.column_type == "bit(1)" => cast("BOOLEAN"),
        "bit" => cast("UBIGINT"),
        "date" => try_cast("DATE"),
        "datetime" => try_cast("TIMESTAMP"),
        "timestamp" => try_cast("TIMESTAMPTZ"),
        // MySQL's TIME runs from -838:59:59 to 838:59:59: a span, not a time of day.
        "time" => cast("INTERVAL"),
        "year" => cast("SMALLINT"),
        "json" => cast("JSON"),
        // Bytes arrive as base64.
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry" | "point" | "linestring"
        | "polygon" | "multipoint" | "multilinestring" | "multipolygon" | "geomcollection" | "geometrycollection"
        | "vector" => "from_base64({c})".to_string(),
        // Text, enum and set: as text.
        _ => "{c}".to_string(),
    }
}

/// (precision, scale) from `decimal(10,2)`.
fn precision_scale(column_type: &str) -> Option<(u32, u32)> {
    let inner = column_type.split_once('(')?.1.split_once(')')?.0;
    let (p, s) = inner.split_once(',').unwrap_or((inner, "0"));
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
}

/// The members of `enum('a','b')` or `set('a','b')`, in order, a doubled quote
/// in one read as one.
pub(crate) fn members(column_type: &str) -> Vec<String> {
    let Some(inner) = column_type.split_once('(').and_then(|(_, rest)| rest.rsplit_once(')')).map(|(i, _)| i) else {
        return Vec::new();
    };
    let (mut out, mut cur, mut quoted, mut chars) = (Vec::new(), String::new(), false, inner.chars().peekable());
    while let Some(c) = chars.next() {
        match (c, quoted) {
            ('\'', false) => quoted = true,
            ('\'', true) if chars.peek() == Some(&'\'') => {
                cur.push('\'');
                chars.next();
            }
            ('\'', true) => {
                quoted = false;
                out.push(std::mem::take(&mut cur));
            }
            (c, true) => cur.push(c),
            _ => {}
        }
    }
    out
}

fn base64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// A cell as the text its column's cast reads, or null.
pub(crate) fn cell_text(col: &Column, value: Option<&BinlogValue<'_>>) -> Result<JsonValue, String> {
    let text = |s: String| Ok(JsonValue::String(s));
    let value = match value {
        None | Some(BinlogValue::Value(Value::NULL)) => return Ok(JsonValue::Null),
        Some(BinlogValue::Jsonb(j)) => {
            let json = serde_json::Value::try_from(j.clone()).map_err(|e| format!("{}: {e}", col.name))?;
            return text(json.to_string());
        }
        Some(BinlogValue::JsonDiff(_)) => {
            return Err(format!(
                "{} was logged as a partial JSON update (binlog_row_value_options = PARTIAL_JSON), which holds \
                 only what changed; set binlog_row_value_options to '' so the whole value is logged",
                col.name
            ))
        }
        Some(BinlogValue::Value(v)) => v,
    };
    let t = col.data_type.as_str();
    match value {
        Value::NULL => Ok(JsonValue::Null),
        Value::Int(i) => {
            if t == "enum" {
                // 1-based; 0 is the empty string MySQL stores for a bad value.
                let m = members(&col.column_type);
                return text(usize::try_from(*i).ok().and_then(|i| i.checked_sub(1)).and_then(|i| m.get(i).cloned()).unwrap_or_default());
            }
            if t == "timestamp" {
                // A TIMESTAMP from before MySQL 5.6, in seconds.
                return Ok(epoch_text(*i, 0).map(JsonValue::String).unwrap_or(JsonValue::Null));
            }
            // Read as signed when the log does not say a column is unsigned.
            let bits = match t {
                "tinyint" => 8,
                "smallint" => 16,
                "mediumint" => 24,
                "int" | "integer" => 32,
                _ => 64,
            };
            if col.unsigned() && *i < 0 {
                let u = if bits == 64 { *i as u64 } else { (*i + (1_i64 << bits)) as u64 };
                return text(u.to_string());
            }
            text(i.to_string())
        }
        Value::UInt(u) => text(u.to_string()),
        Value::Float(f) => text(f.to_string()),
        Value::Double(d) => text(d.to_string()),
        Value::Date(y, mo, d, h, mi, s, us) => {
            if *y == 0 && *mo == 0 && *d == 0 {
                return Ok(JsonValue::Null);
            }
            if t == "date" {
                return text(format!("{y:04}-{mo:02}-{d:02}"));
            }
            text(format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"))
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let hours = u64::from(*days) * 24 + u64::from(*h);
            text(format!("{}{hours}:{mi:02}:{s:02}.{us:06}", if *neg { "-" } else { "" }))
        }
        Value::Bytes(b) => match t {
            "timestamp" => {
                // Seconds since the epoch, with any fraction after a dot.
                let raw = String::from_utf8_lossy(b);
                let (sec, frac) = raw.split_once('.').unwrap_or((&raw, "0"));
                let usec = format!("{frac:0<6}")[..6].parse::<u32>().unwrap_or(0);
                Ok(sec.parse::<i64>().ok().and_then(|s| epoch_text(s, usec)).map(JsonValue::String).unwrap_or(JsonValue::Null))
            }
            "bit" => text(b.iter().fold(0_u64, |a, &x| (a << 8) | u64::from(x)).to_string()),
            "set" => {
                let bits = b.iter().rev().fold(0_u64, |a, &x| (a << 8) | u64::from(x));
                let picked: Vec<String> = members(&col.column_type)
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| *i < 64 && bits & (1 << i) != 0)
                    .map(|(_, m)| m)
                    .collect();
                text(picked.join(","))
            }
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry" | "point"
            | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon" | "geomcollection"
            | "geometrycollection" | "vector" => text(base64(b)),
            _ => text(String::from_utf8_lossy(b).into_owned()),
        },
    }
}

/// A TIMESTAMP's seconds since the epoch as UTC text; None for 0, MySQL's zero.
fn epoch_text(sec: i64, usec: u32) -> Option<String> {
    if sec == 0 && usec == 0 {
        return None;
    }
    let t = chrono::DateTime::from_timestamp(sec, usec * 1000)?;
    Some(t.format("%Y-%m-%d %H:%M:%S%.6f+00").to_string())
}

/// The replica id a read registers as, when the node names none: made from the
/// pipeline and the node, so it stays the same run after run, and kept above a
/// billion, away from the small ids servers are usually given. Two replicas with
/// one id push each other off the primary.
pub(crate) fn default_server_id(pipeline: Option<&str>, node: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in pipeline.unwrap_or("").bytes().chain([0]).chain(node.bytes()) {
        h = (h ^ u32::from(b)).wrapping_mul(0x0100_0193);
    }
    1_000_000_000 + h % 3_000_000_000
}

/// One change as a row: what happened, where in the log, when, then the row as
/// it is after an insert or an update and as it was before a delete.
fn change_row(op: &str, file: &str, pos: u64, ts: u32, cols: &[Column], row: &BinlogRow) -> Result<JsonValue, String> {
    let mut obj = serde_json::Map::new();
    obj.insert("_op".into(), JsonValue::String(op.into()));
    obj.insert("_file".into(), JsonValue::String(file.into()));
    obj.insert("_pos".into(), JsonValue::String(pos.to_string()));
    obj.insert(
        "_ts".into(),
        epoch_text(i64::from(ts), 0).map(JsonValue::String).unwrap_or(JsonValue::Null),
    );
    for (i, col) in cols.iter().enumerate() {
        obj.insert(col.name.clone(), cell_text(col, row.as_ref(i))?);
    }
    Ok(JsonValue::Object(obj))
}

/// Why a change could not be read, from a panic in the binlog decoder.
/// mysql_common 0.37 subtracts 256 from an unsigned fraction when it decodes a
/// negative TIME(1) or TIME(2): with overflow checks on (Cargo.toml turns them
/// on for it in release builds too) that panics here rather than wrapping into
/// a wrong time.
fn decode_panic(payload: Box<dyn std::any::Any + Send>) -> String {
    let what = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "the decoder failed".into());
    format!(
        "a change could not be decoded from the binary log ({what}). The decoder misreads a negative TIME(1) \
         or TIME(2) value with a fraction; TIME(3) or more, or TIME with no fraction, reads correctly"
    )
}

/// Connect as the node says: MySQL's ssl modes, `preferred` falling back to a
/// plain connection only when the server offers no TLS at all.
async fn connect(spec: &plan::MysqlCdcSpec) -> Result<Conn, String> {
    let base = OptsBuilder::default()
        .ip_or_hostname(spec.host.clone())
        .tcp_port(spec.port)
        .user(Some(spec.user.clone()))
        .pass(Some(spec.password.clone()));
    if spec.ssl_mode == "disabled" {
        return Conn::new(base).await.map_err(|e| e.to_string());
    }
    // mysql_async builds its TLS config on rustls's process default, and this
    // build carries two providers, so without one chosen it would panic.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut ssl = SslOpts::default();
    match spec.ssl_mode.as_str() {
        "verify_ca" | "verify_identity" => {
            if let Some(ca) = std::env::var_os("DUCKLE_CA_CERT").filter(|c| !c.is_empty()) {
                ssl = ssl.with_root_certs(vec![std::path::PathBuf::from(ca).into()]);
            }
            ssl = ssl.with_danger_skip_domain_validation(spec.ssl_mode == "verify_ca");
        }
        // preferred and required encrypt and check nothing, as MySQL's do.
        _ => ssl = ssl.with_danger_accept_invalid_certs(true),
    }
    match Conn::new(base.clone().ssl_opts(Some(ssl))).await {
        Err(mysql_async::Error::Driver(mysql_async::DriverError::NoClientSslFlagFromServer)) if spec.ssl_mode == "preferred" => {
            Conn::new(base).await.map_err(|e| e.to_string())
        }
        other => other.map_err(|e| e.to_string()),
    }
}

/// The end of the binary log: (file, position).
async fn log_end(conn: &mut Conn) -> Result<(String, u64), String> {
    // SHOW MASTER STATUS is gone from MySQL 8.4; older servers and MariaDB lack the new name.
    let row: Option<mysql_async::Row> = match conn.query_first("SHOW BINARY LOG STATUS").await {
        Ok(r) => r,
        Err(_) => conn.query_first("SHOW MASTER STATUS").await.map_err(|e| e.to_string())?,
    };
    let row = row.ok_or("binary logging is off on this server (log_bin), so there is no log to read")?;
    let file: String = row.get(0).ok_or("no binary log file")?;
    let pos: u64 = row.get(1).ok_or("no binary log position")?;
    Ok((file, pos))
}

impl DuckdbEngine {
    pub(crate) fn run_mysql_cdc(
        &self,
        db: &Path,
        spec: &plan::MysqlCdcSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let state_path = crate::connectors::incremental_state_path(pipeline_name, &spec.node_id).ok_or_else(|| {
            EngineError::Config(
                "src.mysql.cdc: needs a workspace to keep its position in (DUCKLE_WORKSPACE); without one every run would start at the end of the log".into(),
            )
        })?;
        let prior = crate::read_state_snapshot(&state_path);
        let saved: Option<(String, u64)> = prior
            .as_deref()
            .and_then(|t| serde_json::from_str::<JsonValue>(t).ok())
            .and_then(|v| Some((v.get("file")?.as_str()?.to_string(), v.get("pos")?.as_u64()?)));
        let server_id = spec.server_id.unwrap_or_else(|| default_server_id(pipeline_name, &spec.node_id));
        let table = format!("{}.{}", spec.database, spec.table);
        let fail = |e: String| EngineError::Query(format!("src.mysql.cdc: {table}: {e}"));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("src.mysql.cdc: tokio rt: {e}")))?;
        let writer = JsonLinesWriter::open(&spec.node_id)?;
        let (count, cols, to, writer) = rt
            .block_on(async {
                use futures_util::StreamExt;
                let mut writer = writer;
                let mut conn = connect(spec).await?;
                let settings: Option<(String, String, String)> = conn
                    .query_first("SELECT CAST(@@GLOBAL.log_bin AS CHAR), @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image")
                    .await
                    .map_err(|e| e.to_string())?;
                let (log_bin, format, image) = settings.unwrap_or_default();
                if log_bin != "1" {
                    return Err("binary logging is off on this server (log_bin), so there is no log to read".to_string());
                }
                if !format.eq_ignore_ascii_case("ROW") {
                    return Err(format!(
                        "binlog_format is {format}; change data capture reads row changes, so it needs ROW (SET GLOBAL binlog_format = 'ROW')"
                    ));
                }
                if !image.eq_ignore_ascii_case("FULL") {
                    return Err(format!(
                        "binlog_row_image is {image}, which leaves columns out of updates and deletes; this source needs FULL \
                         (SET GLOBAL binlog_row_image = 'FULL')"
                    ));
                }
                let cols: Vec<Column> = conn
                    .exec::<(String, String, String), _, _>(
                        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                        (spec.database.as_str(), spec.table.as_str()),
                    )
                    .await
                    .map_err(|e| e.to_string())?
                    .into_iter()
                    .map(|(name, data_type, column_type)| Column {
                        name,
                        data_type: data_type.to_ascii_lowercase(),
                        column_type: column_type.to_ascii_lowercase(),
                    })
                    .collect();
                if cols.is_empty() {
                    return Err(format!("there is no table {table}, or this user may not see it"));
                }
                let end = log_end(&mut conn).await?;
                let start = match &saved {
                    None => {
                        // A first run starts here, at the end: nothing to read yet.
                        let _ = conn.disconnect().await;
                        return Ok::<_, String>((0, cols, Some(end), writer));
                    }
                    Some(s) => s.clone(),
                };
                // Log_name first; MySQL 8 adds File_size and Encrypted after it.
                let logs: Vec<String> = conn
                    .query::<mysql_async::Row, _>("SHOW BINARY LOGS")
                    .await
                    .map_err(|e| e.to_string())?
                    .iter()
                    .filter_map(|r| r.get::<String, _>(0))
                    .collect();
                if !logs.contains(&start.0) {
                    return Err(format!(
                        "the binary log {}, where the last run stopped at {}, was purged before this run read it, \
                         and the changes in it with it. Load the table again, then clear this node's saved position \
                         to start from the end of the log.",
                        start.0, start.1
                    ));
                }
                if start == end {
                    let _ = conn.disconnect().await;
                    return Ok((0, cols, Some(start), writer));
                }
                let request = BinlogStreamRequest::new(server_id)
                    .with_filename(start.0.as_bytes())
                    .with_pos(start.1)
                    .with_non_blocking();
                let mut stream = conn.get_binlog_stream(request).await.map_err(|e| e.to_string())?;
                let (mut file, mut committed) = (start.0.clone(), start.clone());
                let (mut in_tx, mut count, mut held): (bool, usize, Vec<JsonValue>) = (false, 0, Vec::new());
                while let Some(event) = stream.next().await {
                    let event = event.map_err(|e| e.to_string())?;
                    let header = event.header();
                    let pos = u64::from(header.log_pos());
                    match event.read_data().map_err(|e| e.to_string())? {
                        Some(EventData::RotateEvent(r)) => {
                            // A dump opens with a rotate naming the file it starts in;
                            // only a rotate to the next file moves the position.
                            let name = r.name().into_owned();
                            if name != file && !in_tx {
                                committed = (name.clone(), r.position());
                            }
                            file = name;
                            continue;
                        }
                        Some(EventData::RowsEvent(rows)) => {
                            let Some(tme) = stream.get_tme(rows.table_id()) else { continue };
                            if tme.database_name() != spec.database || tme.table_name() != spec.table {
                                continue;
                            }
                            if tme.columns_count() as usize != cols.len() {
                                return Err(format!(
                                    "its columns changed after these changes were logged ({} then, {} now), so they \
                                     cannot be read against what the table is now; load it again and clear this node's saved position",
                                    tme.columns_count(),
                                    cols.len()
                                ));
                            }
                            let op = match &rows {
                                RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => "insert",
                                RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => "delete",
                                _ => "update",
                            };
                            // The decoder can panic on a value it misreads (see
                            // decode_panic): caught, the run fails saying so.
                            let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                let mut out = Vec::new();
                                for row in rows.rows(tme) {
                                    let (before, after) = row.map_err(|e| e.to_string())?;
                                    let image = if op == "delete" { before } else { after };
                                    if let Some(image) = image {
                                        out.push(change_row(op, &file, pos, header.timestamp(), &cols, &image)?);
                                    }
                                }
                                Ok::<_, String>(out)
                            }))
                            .map_err(decode_panic)?;
                            held.extend(decoded?);
                            in_tx = true;
                        }
                        Some(EventData::QueryEvent(q)) => {
                            let sql = q.query().trim().to_ascii_uppercase();
                            if sql == "BEGIN" {
                                in_tx = true;
                            } else if sql == "ROLLBACK" {
                                held.clear();
                                in_tx = false;
                            } else if sql == "COMMIT" {
                                in_tx = false;
                            } else if in_tx {
                                continue;
                            }
                        }
                        Some(EventData::XidEvent(_)) => in_tx = false,
                        _ => {}
                    }
                    // The header events a dump begins with carry no position, or one
                    // from the start of the file: the position only ever moves on.
                    if !in_tx && pos > 0 && (file != committed.0 || pos > committed.1) {
                        // A transaction is whole: what it changed is kept, and the
                        // position moves past it.
                        for row in held.drain(..) {
                            writer.write_row(&row).map_err(|e| e.to_string())?;
                            count += 1;
                        }
                        committed = (file.clone(), pos);
                        if count >= spec.batch_size as usize {
                            break;
                        }
                    }
                }
                drop(stream);
                Ok((count, cols, Some(committed), writer))
            })
            .map_err(fail)?;
        let mut spec_cols = vec!["'_op': 'VARCHAR'".to_string(), "'_file': 'VARCHAR'".into(), "'_pos': 'VARCHAR'".into(), "'_ts': 'VARCHAR'".into()];
        let mut select = vec![
            "\"_op\"".to_string(),
            "\"_file\"".into(),
            "CAST(\"_pos\" AS BIGINT) AS \"_pos\"".into(),
            "CAST(\"_ts\" AS TIMESTAMPTZ) AS \"_ts\"".into(),
        ];
        for col in &cols {
            spec_cols.push(format!("'{}': 'VARCHAR'", col.name.replace('\'', "''")));
            select.push(format!(
                "{} AS {}",
                cast_for(col).replace("{c}", &plan::quote_ident(&col.name)),
                plan::quote_ident(&col.name)
            ));
        }
        writer.finalize_typed(&self.bin, db, &spec.node_id, &spec_cols.join(", "), &select.join(", "))?;
        if let Some((file, pos)) = to.filter(|t| saved.as_ref() != Some(t)) {
            pending.push(crate::PendingWrite::state(state_path, serde_json::json!({ "file": file, "pos": pos }), prior));
        }
        Ok(format!("mysql.cdc: read {count} change(s) of {table} into {}", spec.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, data_type: &str, column_type: &str) -> Column {
        Column { name: name.into(), data_type: data_type.into(), column_type: column_type.into() }
    }

    #[test]
    fn each_mysql_type_is_cast_from_its_text() {
        for (data_type, column_type, cast) in [
            ("int", "int", "CAST({c} AS INTEGER)"),
            ("int", "int unsigned", "CAST({c} AS UINTEGER)"),
            ("bigint", "bigint unsigned", "CAST({c} AS UBIGINT)"),
            ("tinyint", "tinyint(1)", "CAST({c} AS TINYINT)"),
            ("decimal", "decimal(10,2)", "CAST({c} AS DECIMAL(10,2))"),
            ("decimal", "decimal(65,30)", "{c}"),
            ("bit", "bit(1)", "CAST({c} AS BOOLEAN)"),
            ("bit", "bit(10)", "CAST({c} AS UBIGINT)"),
            ("date", "date", "TRY_CAST({c} AS DATE)"),
            ("datetime", "datetime(3)", "TRY_CAST({c} AS TIMESTAMP)"),
            ("timestamp", "timestamp(6)", "TRY_CAST({c} AS TIMESTAMPTZ)"),
            ("time", "time(1)", "CAST({c} AS INTERVAL)"),
            ("json", "json", "CAST({c} AS JSON)"),
            ("blob", "blob", "from_base64({c})"),
            ("enum", "enum('s','m')", "{c}"),
            ("varchar", "varchar(20)", "{c}"),
        ] {
            assert_eq!(cast_for(&col("c", data_type, column_type)), cast, "{column_type}");
        }
    }

    #[test]
    fn enum_and_set_members_come_out_of_the_column_type() {
        assert_eq!(members("enum('s','m','l')"), ["s", "m", "l"]);
        assert_eq!(members("set('it''s','a,b')"), ["it's", "a,b"]);
    }

    #[test]
    fn each_binlog_value_is_written_as_its_cast_reads_it() {
        let v = |x: Value| Some(BinlogValue::Value(x));
        let cell = |c: &Column, x: Option<BinlogValue<'static>>| cell_text(c, x.as_ref()).unwrap();
        let s = |x: &str| JsonValue::String(x.into());
        assert_eq!(cell(&col("e", "enum", "enum('s','m','l')"), v(Value::Int(2))), s("m"));
        assert_eq!(cell(&col("e", "enum", "enum('s','m','l')"), v(Value::Int(0))), s(""), "MySQL's empty enum value");
        assert_eq!(cell(&col("t", "set", "set('red','green','blue')"), v(Value::Bytes(vec![0b101]))), s("red,blue"));
        // Past eight members the mask takes two bytes, the low one first.
        let nine = col("t", "set", "set('a','b','c','d','e','f','g','h','i')");
        assert_eq!(cell(&nine, v(Value::Bytes(vec![0x01, 0x00]))), s("a"));
        assert_eq!(cell(&nine, v(Value::Bytes(vec![0x00, 0x01]))), s("i"));
        assert_eq!(cell(&col("b", "bit", "bit(10)"), v(Value::Bytes(vec![0x02, 0x01]))), s("513"));
        assert_eq!(cell(&col("u", "int", "int unsigned"), v(Value::Int(-1))), s("4294967295"), "read as signed by a log that does not say");
        assert_eq!(cell(&col("u", "bigint", "bigint unsigned"), v(Value::Int(-1))), s("18446744073709551615"));
        assert_eq!(cell(&col("i", "int", "int"), v(Value::Int(-1))), s("-1"));
        assert_eq!(
            cell(&col("ts", "timestamp", "timestamp(6)"), v(Value::Bytes(b"1795082400.123456".to_vec()))),
            s("2026-11-19 10:00:00.123456+00")
        );
        assert_eq!(cell(&col("ts", "timestamp", "timestamp"), v(Value::Bytes(b"0".to_vec()))), JsonValue::Null, "MySQL's zero");
        assert_eq!(cell(&col("d", "date", "date"), v(Value::Date(2026, 11, 19, 0, 0, 0, 0))), s("2026-11-19"));
        assert_eq!(cell(&col("d", "date", "date"), v(Value::Date(0, 0, 0, 0, 0, 0, 0))), JsonValue::Null);
        assert_eq!(
            cell(&col("dt", "datetime", "datetime(3)"), v(Value::Date(2026, 11, 19, 10, 0, 0, 123_000))),
            s("2026-11-19 10:00:00.123000")
        );
        assert_eq!(cell(&col("w", "time", "time(1)"), v(Value::Time(true, 34, 22, 59, 58, 500_000))), s("-838:59:58.500000"));
        assert_eq!(cell(&col("p", "blob", "blob"), v(Value::Bytes(vec![0xFF, 0xD8]))), s("/9g="));
        assert_eq!(cell(&col("n", "varchar", "varchar(20)"), v(Value::NULL)), JsonValue::Null);
        assert_eq!(cell(&col("n", "varchar", "varchar(20)"), None), JsonValue::Null);
    }

    #[test]
    fn a_default_server_id_is_steady_and_kept_high() {
        let a = default_server_id(Some("orders"), "n1");
        assert_eq!(a, default_server_id(Some("orders"), "n1"), "the same run after run");
        assert_ne!(a, default_server_id(Some("orders"), "n2"));
        assert_ne!(a, default_server_id(Some("ordersn"), "1"), "the pipeline and the node do not run together");
        assert!(a >= 1_000_000_000);
    }
}
