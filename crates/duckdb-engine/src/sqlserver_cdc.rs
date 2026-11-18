//! src.sqlserver.cdc: every insert, update and delete SQL Server's change data
//! capture recorded for one table, in commit order, read from the capture
//! instance's change table between the position the last successful run saved
//! and the newest change there is. The position is saved only when the run
//! succeeds, so a failed run reads the same changes again.
//!
//! The change table is read directly, as Debezium reads it: the
//! cdc.fn_cdc_get_all_changes_* functions refuse a range with no change in it,
//! and a run with nothing new is the common case.

use crate::*;

/// What a change row is cast to from the text `sqlserver_cell_to_json` writes,
/// by the system_type_name SQL Server describes it with. `{c}` is the cell.
pub(crate) fn cdc_column(system_type: &str) -> String {
    let t = system_type.trim().to_ascii_lowercase();
    let base = t.split(['(', ' ']).next().unwrap_or("");
    let cast = |to: &str| format!("CAST({{c}} AS {to})");
    match base {
        "bit" => cast("BOOLEAN"),
        // SQL Server's tinyint runs 0 to 255.
        "tinyint" => cast("UTINYINT"),
        "smallint" => cast("SMALLINT"),
        "int" => cast("INTEGER"),
        "bigint" => cast("BIGINT"),
        "real" => cast("FLOAT"),
        "float" => cast("DOUBLE"),
        // decimal(10,2) as declared; numeric is the same type by another name.
        "decimal" | "numeric" => cast(&t.replacen("numeric", "decimal", 1).to_ascii_uppercase()),
        "money" => cast("DECIMAL(19,4)"),
        "smallmoney" => cast("DECIMAL(10,4)"),
        "date" => cast("DATE"),
        "time" => cast("TIME"),
        "datetime" | "datetime2" | "smalldatetime" => cast("TIMESTAMP"),
        "datetimeoffset" => cast("TIMESTAMPTZ"),
        "uniqueidentifier" => cast("UUID"),
        // Bytes arrive as base64.
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => "from_base64({c})".to_string(),
        // Text, xml, sql_variant, the spatial types: as text.
        _ => "{c}".to_string(),
    }
}

/// `[name]`, with any `]` in it doubled.
fn bracket(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// A T-SQL string literal.
fn nstr(s: &str) -> String {
    format!("N'{}'", s.replace('\'', "''"))
}

fn lsn(hex: &str) -> String {
    format!("CONVERT(binary(10), '{}', 1)", hex.replace('\'', ""))
}

/// Where a read starts: the oldest change kept, on a first run, or after the
/// last change a successful run read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Start {
    Oldest(String),
    After(String),
}

/// The WHERE clause for the changes in (from, to]: updates' before-images left
/// out, so an update is one row of its new values.
pub(crate) fn change_range(from: &Start, to: &str) -> String {
    let start = match from {
        Start::Oldest(min) => format!("c.[__$start_lsn] >= {}", lsn(min)),
        Start::After(saved) => format!("c.[__$start_lsn] > {}", lsn(saved)),
    };
    format!("c.[__$operation] <> 3 AND {start} AND c.[__$start_lsn] <= {}", lsn(to))
}

/// The changes, each as `_op`, `_lsn`, `_seq`, `_commit_ts` and then the
/// captured columns, in commit order.
pub(crate) fn change_query(instance: &str, columns: &[String], range: &str) -> String {
    let captured: Vec<String> = columns.iter().map(|c| format!("c.{}", bracket(c))).collect();
    format!(
        "SELECT CASE c.[__$operation] WHEN 1 THEN 'delete' WHEN 2 THEN 'insert' ELSE 'update' END AS [_op], \
         CONVERT(varchar(22), c.[__$start_lsn], 1) AS [_lsn], CONVERT(varchar(22), c.[__$seqval], 1) AS [_seq], \
         sys.fn_cdc_map_lsn_to_time(c.[__$start_lsn]) AS [_commit_ts]{}{} \
         FROM [cdc].{} AS c WHERE {range} ORDER BY c.[__$start_lsn], c.[__$seqval], c.[__$operation]",
        if captured.is_empty() { "" } else { ", " },
        captured.join(", "),
        bracket(&format!("{instance}_CT")),
    )
}

type Tds = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

async fn rows(client: &mut Tds, sql: &str) -> Result<Vec<tiberius::Row>, String> {
    let stream = client.simple_query(sql).await.map_err(|e| e.to_string())?;
    stream.into_first_result().await.map_err(|e| e.to_string())
}

fn text(row: &tiberius::Row, i: usize) -> Option<String> {
    row.try_get::<&str, _>(i).ok().flatten().map(str::to_string)
}

impl DuckdbEngine {
    pub(crate) fn run_sqlserver_cdc(
        &self,
        db: &Path,
        spec: &plan::SqlServerCdcSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let state_path = crate::connectors::incremental_state_path(pipeline_name, &spec.node_id).ok_or_else(|| {
            EngineError::Config(
                "src.sqlserver.cdc: needs a workspace to keep its position in (DUCKLE_WORKSPACE); without one every run would read every change again".into(),
            )
        })?;
        let prior = crate::read_state_snapshot(&state_path);
        let saved: Option<String> = prior
            .as_deref()
            .and_then(|t| serde_json::from_str::<JsonValue>(t).ok())
            .and_then(|v| v.get("lsn").and_then(JsonValue::as_str).map(str::to_string));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("sqlserver.cdc: tokio rt: {e}")))?;
        let writer = JsonLinesWriter::open(&spec.node_id)?;
        let table = format!("{}.{}", spec.schema, spec.table);
        let fail = |e: String| EngineError::Query(format!("src.sqlserver.cdc: {table}: {e}"));
        let (count, columns, to, instance, writer) = rt
            .block_on(async {
                use futures_util::TryStreamExt;
                use tiberius::QueryItem;
                let mut writer = writer;
                let mut config = tiberius::Config::new();
                config.host(&spec.host);
                config.port(spec.port);
                config.authentication(tiberius::AuthMethod::sql_server(&spec.user, &spec.password));
                config.database(&spec.database);
                if spec.trust_cert {
                    config.trust_cert();
                }
                if !spec.encrypt {
                    config.encryption(tiberius::EncryptionLevel::NotSupported);
                }
                let mut client = crate::connectors::tds_connect(config, crate::connectors::CONNECT_TIMEOUT).await?;

                // The table's capture instance: the newest, when a schema change
                // left two side by side.
                let found = rows(
                    &mut client,
                    &format!(
                        "SELECT TOP 1 ct.capture_instance FROM cdc.change_tables ct \
                         JOIN sys.tables t ON t.object_id = ct.source_object_id \
                         JOIN sys.schemas s ON s.schema_id = t.schema_id \
                         WHERE s.name = {} AND t.name = {} ORDER BY ct.create_date DESC",
                        nstr(&spec.schema),
                        nstr(&spec.table)
                    ),
                )
                .await
                .map_err(|e| {
                    if e.contains("cdc.change_tables") {
                        format!(
                            "change data capture is not enabled on database {}; enable it with EXEC sys.sp_cdc_enable_db",
                            spec.database
                        )
                    } else {
                        e
                    }
                })?;
                let instance = found.first().and_then(|r| text(r, 0)).ok_or_else(|| {
                    format!(
                        "change data capture is not enabled for this table; enable it with EXEC sys.sp_cdc_enable_table \
                         @source_schema = {}, @source_name = {}, @role_name = NULL",
                        nstr(&spec.schema),
                        nstr(&spec.table)
                    )
                })?;
                let bounds = rows(
                    &mut client,
                    &format!(
                        "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn({}), 1), CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)",
                        nstr(&instance)
                    ),
                )
                .await?;
                let (min, max) = bounds
                    .first()
                    .and_then(|r| Some((text(r, 0)?, text(r, 1)?)))
                    .ok_or("no LSN range for the capture instance")?;
                if min.trim_start_matches("0x").chars().all(|c| c == '0') {
                    return Err(format!(
                        "this user may not read capture instance {instance}: it is limited to a role (EXEC sys.sp_cdc_help_change_data_capture names it)"
                    ));
                }
                let from = match &saved {
                    // Changes after the saved one and before the oldest kept were
                    // cleaned up unread. Going on would skip them without a word.
                    Some(s) if s.as_str() < min.as_str() => {
                        return Err(format!(
                            "the changes after {s}, where the last run stopped, were cleaned up before this run read them \
                             (the oldest kept is {min}). Load the table again, then clear this node's saved position to start from the oldest change kept."
                        ))
                    }
                    Some(s) => Start::After(s.clone()),
                    None => Start::Oldest(min.clone()),
                };
                // At most batch_size changes, ending on a whole transaction:
                // every change of a transaction shares its start LSN.
                let range = change_range(&from, &max);
                let cut = rows(
                    &mut client,
                    &format!(
                        "SELECT CONVERT(varchar(22), MAX(x.l), 1), COUNT(*) FROM (SELECT TOP ({}) c.[__$start_lsn] AS l \
                         FROM [cdc].{} AS c WHERE {range} ORDER BY c.[__$start_lsn], c.[__$seqval]) x",
                        spec.batch_size,
                        bracket(&format!("{instance}_CT"))
                    ),
                )
                .await?;
                let taken = cut.first().and_then(|r| r.try_get::<i32, _>(1).ok().flatten()).unwrap_or(0) as u64;
                let to = match cut.first().and_then(|r| text(r, 0)) {
                    Some(last) if taken >= spec.batch_size => last,
                    _ => max.clone(),
                };
                let captured: Vec<String> = rows(
                    &mut client,
                    &format!(
                        "SELECT cc.column_name FROM cdc.captured_columns cc JOIN cdc.change_tables ct ON ct.object_id = cc.object_id \
                         WHERE ct.capture_instance = {} ORDER BY cc.column_ordinal",
                        nstr(&instance)
                    ),
                )
                .await?
                .iter()
                .filter_map(|r| text(r, 0))
                .collect();
                let query = change_query(&instance, &captured, &change_range(&from, &to));
                // Each column's type as SQL Server declares it, so a run with no
                // changes still has its columns, typed.
                let described = rows(
                    &mut client,
                    &format!(
                        "SELECT name, system_type_name FROM sys.dm_exec_describe_first_result_set({}, NULL, 0) ORDER BY column_ordinal",
                        nstr(&query)
                    ),
                )
                .await?;
                let columns: Vec<(String, String)> = described
                    .iter()
                    .filter_map(|r| Some((text(r, 0)?, text(r, 1).unwrap_or_default())))
                    .collect();
                let mut stream = client.simple_query(&query).await.map_err(|e| e.to_string())?;
                let mut count = 0_usize;
                while let Some(item) = stream.try_next().await.map_err(|e| e.to_string())? {
                    let QueryItem::Row(row) = item else { continue };
                    let mut obj = serde_json::Map::new();
                    for (i, col) in row.columns().iter().enumerate() {
                        obj.insert(col.name().to_string(), Self::sqlserver_cell_to_json(&row, col, i));
                    }
                    writer.write_row(&JsonValue::Object(obj)).map_err(|e| e.to_string())?;
                    count += 1;
                }
                // The range may be empty, as before the first capture scan: then
                // nothing was read, and the position stays where it was.
                let read_to = match &from {
                    Start::Oldest(min) if to.as_str() < min.as_str() => None,
                    Start::After(s) if to.as_str() <= s.as_str() => None,
                    _ => Some(to),
                };
                Ok::<_, String>((count, columns, read_to, instance, writer))
            })
            .map_err(fail)?;
        let spec_cols: Vec<String> = columns.iter().map(|(n, _)| format!("'{}': 'VARCHAR'", n.replace('\'', "''"))).collect();
        let select: Vec<String> = columns
            .iter()
            .map(|(n, ty)| format!("{} AS {}", cdc_column(ty).replace("{c}", &plan::quote_ident(n)), plan::quote_ident(n)))
            .collect();
        writer.finalize_typed(&self.bin, db, &spec.node_id, &spec_cols.join(", "), &select.join(", "))?;
        if let Some(to) = to {
            pending.push(crate::PendingWrite::state(
                state_path,
                serde_json::json!({ "lsn": to, "captureInstance": instance }),
                prior,
            ));
        }
        Ok(format!("sqlserver.cdc: read {count} change(s) of {table} into {}", spec.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_sql_server_type_is_cast_from_its_text() {
        for (t, want) in [
            ("int", "CAST({c} AS INTEGER)"),
            ("tinyint", "CAST({c} AS UTINYINT)"),
            ("decimal(10,2)", "CAST({c} AS DECIMAL(10,2))"),
            ("numeric(38,6)", "CAST({c} AS DECIMAL(38,6))"),
            ("money", "CAST({c} AS DECIMAL(19,4))"),
            ("datetime2(7)", "CAST({c} AS TIMESTAMP)"),
            ("datetimeoffset(7)", "CAST({c} AS TIMESTAMPTZ)"),
            ("uniqueidentifier", "CAST({c} AS UUID)"),
            ("varbinary(max)", "from_base64({c})"),
            ("nvarchar(50)", "{c}"),
            ("xml", "{c}"),
        ] {
            assert_eq!(cdc_column(t), want, "{t}");
        }
    }

    #[test]
    fn a_range_starts_at_the_oldest_change_or_after_the_saved_one() {
        let first = change_range(&Start::Oldest("0x0000002A000001A80003".into()), "0x0000002B000000100001");
        assert!(first.contains("[__$start_lsn] >= CONVERT(binary(10), '0x0000002A000001A80003', 1)"), "{first}");
        let next = change_range(&Start::After("0x0000002A000001A80003".into()), "0x0000002B000000100001");
        assert!(next.contains("[__$start_lsn] > CONVERT(binary(10), '0x0000002A000001A80003', 1)"), "{next}");
        assert!(next.contains("[__$start_lsn] <= CONVERT(binary(10), '0x0000002B000000100001', 1)"), "{next}");
        assert!(next.contains("[__$operation] <> 3"), "an update is its new values only: {next}");
    }

    #[test]
    fn a_name_with_a_bracket_cannot_end_its_identifier() {
        let q = change_query("dbo_odd]name", &["a]b".to_string()], "1 = 1");
        assert!(q.contains("c.[a]]b]") && q.contains("[cdc].[dbo_odd]]name_CT]"), "{q}");
    }
}
