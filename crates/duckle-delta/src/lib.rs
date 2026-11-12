//! Delta Lake writes for the engine's snk.delta that DuckDB's delta extension cannot
//! make: MERGE (upsert), UPDATE and DELETE by key, and appends to a table on object
//! storage. The engine writes the node's input to a Parquet file and sends a
//! [`Request`] naming it; delta-rs does the rest.

use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::datatypes::DataType;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::logical_expr::{cast, lit, Expr};
use deltalake::datafusion::prelude::{ParquetReadOptions, SessionContext};
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};

/// What to do with the rows in `source`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Add every row, creating the table when it is missing.
    Append,
    /// Update the rows whose keys match and insert the rest.
    Upsert,
    /// Update the rows whose keys match; insert nothing.
    Update,
    /// Delete the rows whose keys match.
    Delete,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    /// The table: a local directory or a URL (abfss://, az://, s3://, gs://).
    pub table: String,
    /// A Parquet file holding the rows to write.
    pub source: String,
    pub mode: Mode,
    /// Key columns for upsert, update and delete.
    #[serde(default)]
    pub keys: Vec<String>,
    /// With upsert: a source row whose value here equals `delete_value` deletes the
    /// matching target row instead. The column itself is never written.
    #[serde(default)]
    pub delete_column: Option<String>,
    #[serde(default)]
    pub delete_value: Option<String>,
    #[serde(default = "yes")]
    pub create_if_missing: bool,
    /// For a table this request creates.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// delta-rs storage options (azure_client_id, aws_region, ...). The environment's
    /// AZURE_* / AWS_* / GOOGLE_* variables apply as well.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    /// The table version this request committed.
    pub version: i64,
    /// Whether this request created the table.
    pub created: bool,
    pub inserted: u64,
    pub updated: u64,
    pub deleted: u64,
}

const SOURCE: &str = "source";
const TARGET: &str = "target";

/// A column by its exact name. Never parsed: a name holding a dot or a capital is
/// that name, not a qualified or folded one.
fn column(relation: Option<&str>, name: &str) -> Expr {
    Expr::Column(match relation {
        Some(r) => Column::new(Some(r), name),
        None => Column::new_unqualified(name),
    })
}

/// True where the flag column holds the delete value. A NULL flag is not a delete.
fn flagged(relation: Option<&str>, flag: &str, value: &str) -> Expr {
    cast(column(relation, flag), DataType::Utf8).in_list(flag_values(value), false)
}

/// The complement of `flagged`, NULL counting as "not a delete".
fn not_flagged(relation: Option<&str>, flag: &str, value: &str) -> Expr {
    let flag = cast(column(relation, flag), DataType::Utf8);
    flag.clone().is_null().or(flag.in_list(flag_values(value), true))
}

/// The values that delete. "delete" (the default, DuckLake CDC's tag) also takes
/// "deleted", the tag xf.cdc.diff gives a removed row; any other value matches
/// only itself. The engine's `plan::delete_flag_values` says the same.
fn flag_values(value: &str) -> Vec<Expr> {
    let mut values = vec![lit(value.to_string())];
    if value == "delete" {
        values.push(lit("deleted".to_string()));
    }
    values
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Append => "append",
        Mode::Upsert => "upsert",
        Mode::Update => "update",
        Mode::Delete => "delete",
    }
}

/// Carry out one request.
pub async fn run(req: Request) -> Result<Outcome, String> {
    let at = &req.table;
    let url = deltalake::ensure_table_uri(at).map_err(|e| format!("{at}: {e}"))?;
    let table = DeltaTable::try_from_url_with_storage_options(url, req.storage_options.clone())
        .await
        .map_err(|e| format!("cannot open the Delta table at {at}: {e}"))?;

    let ctx = SessionContext::new();
    let source = ctx
        .read_parquet(req.source.as_str(), ParquetReadOptions::default())
        .await
        .map_err(|e| format!("cannot read the input {}: {e}", req.source))?;
    let input: Vec<String> = source.schema().fields().iter().map(|f| f.name().clone()).collect();

    // The flag is a control column: it decides a row's fate and is never written.
    let flag = req.delete_column.as_deref().filter(|f| !f.is_empty());
    let delete_value = req.delete_value.as_deref().filter(|v| !v.is_empty()).unwrap_or("delete");
    if let Some(f) = flag {
        if req.mode != Mode::Upsert {
            return Err(format!("deleteColumn applies to upsert, not {}", mode_name(req.mode)));
        }
        if !input.iter().any(|c| c == f) {
            return Err(format!("deleteColumn names {f}, which the input does not have"));
        }
    }
    let data: Vec<String> = input.iter().filter(|c| Some(c.as_str()) != flag).cloned().collect();
    if req.mode != Mode::Append {
        if req.keys.is_empty() {
            return Err(format!("{} needs at least one key column (conflictColumns)", mode_name(req.mode)));
        }
        if let Some(k) = req.keys.iter().find(|k| !data.contains(k)) {
            return Err(format!("key column {k} is not in the input"));
        }
    }

    if table.version().is_none() {
        if matches!(req.mode, Mode::Update | Mode::Delete) || !req.create_if_missing {
            let why = if req.create_if_missing { "" } else { " and createIfMissing is off" };
            return Err(format!("there is no Delta table at {at}{why}"));
        }
        // Every row of an append, and every unflagged row of an upsert, is an insert.
        let rows = match flag {
            Some(f) => source
                .filter(not_flagged(None, f, delete_value))
                .and_then(|df| df.select(data.iter().map(|c| column(None, c)).collect::<Vec<_>>()))
                .map_err(|e| e.to_string())?,
            None => source,
        };
        let batches = rows.collect().await.map_err(|e| format!("cannot read the input: {e}"))?;
        let inserted = batches.iter().map(|b| b.num_rows() as u64).sum();
        let made = table
            .write(batches)
            .with_save_mode(SaveMode::Append)
            .with_partition_columns(req.partition_by.clone())
            .await
            .map_err(|e| format!("cannot create the Delta table at {at}: {e}"))?;
        return Ok(Outcome { version: made.version().map_or(0, |v| v as i64), created: true, inserted, ..Default::default() });
    }

    // By name, both ways, as an append through the engine checks: a column the table
    // lacks has nowhere to go, and one the input lacks would be written as NULL.
    let columns: Vec<String> = table
        .snapshot()
        .map_err(|e| e.to_string())?
        .schema()
        .fields()
        .map(|f| f.name().to_string())
        .collect();
    if let Some(k) = req.keys.iter().find(|k| !columns.contains(k)) {
        return Err(format!("key column {k} is not in the table at {at}"));
    }
    if req.mode != Mode::Delete {
        let extra: Vec<&str> = data.iter().filter(|c| !columns.contains(c)).map(String::as_str).collect();
        if !extra.is_empty() {
            return Err(format!("the table at {at} has no column {}", extra.join(", ")));
        }
    }
    if matches!(req.mode, Mode::Append | Mode::Upsert) {
        let missing: Vec<&str> = columns.iter().filter(|c| !data.contains(c)).map(String::as_str).collect();
        if !missing.is_empty() {
            return Err(format!(
                "the input has no column {}; {} writes every column of the table",
                missing.join(", "),
                mode_name(req.mode)
            ));
        }
    }

    if req.mode == Mode::Append {
        let batches = source.collect().await.map_err(|e| format!("cannot read the input: {e}"))?;
        let inserted = batches.iter().map(|b| b.num_rows() as u64).sum();
        let t = table
            .write(batches)
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| format!("cannot append to the Delta table at {at}: {e}"))?;
        return Ok(Outcome { version: t.version().map_or(0, |v| v as i64), inserted, ..Default::default() });
    }

    let settable: Vec<&String> = data.iter().filter(|c| !req.keys.contains(c)).collect();
    if req.mode == Mode::Update && settable.is_empty() {
        // Only keys: there is nothing to set, so nothing to commit.
        return Ok(Outcome { version: table.version().map_or(0, |v| v as i64), ..Default::default() });
    }

    let on = req
        .keys
        .iter()
        .map(|k| column(Some(TARGET), k).eq(column(Some(SOURCE), k)))
        .reduce(Expr::and)
        .expect("keys checked non-empty");
    let mut merge = table
        .merge(source, on)
        .with_source_alias(SOURCE)
        .with_target_alias(TARGET)
        .with_session_state(Arc::new(ctx.state()));
    let fail = |e: deltalake::DeltaTableError| format!("cannot {} the Delta table at {at}: {e}", mode_name(req.mode));
    let set_all = |mut u: deltalake::operations::merge::UpdateBuilder| {
        for c in &settable {
            u = u.update(Column::new_unqualified(c.as_str()), column(Some(SOURCE), c));
        }
        u
    };
    match req.mode {
        Mode::Delete => {
            merge = merge.when_matched_delete(|d| d).map_err(fail)?;
        }
        Mode::Update => {
            merge = merge.when_matched_update(set_all).map_err(fail)?;
        }
        Mode::Upsert => {
            // Clauses are tried in order and the first that applies wins, so a flagged
            // match is deleted before the update can see it.
            if let Some(f) = flag {
                merge = merge.when_matched_delete(|d| d.predicate(flagged(Some(SOURCE), f, delete_value))).map_err(fail)?;
            }
            if !settable.is_empty() {
                merge = merge.when_matched_update(set_all).map_err(fail)?;
            }
            merge = merge
                .when_not_matched_insert(|mut i| {
                    if let Some(f) = flag {
                        i = i.predicate(not_flagged(Some(SOURCE), f, delete_value));
                    }
                    for c in &data {
                        i = i.set(Column::new_unqualified(c.as_str()), column(Some(SOURCE), c));
                    }
                    i
                })
                .map_err(fail)?;
        }
        Mode::Append => unreachable!("appended above"),
    }
    let (t, m) = merge.await.map_err(fail)?;
    Ok(Outcome {
        version: t.version().map_or(0, |v| v as i64),
        created: false,
        inserted: m.num_target_rows_inserted as u64,
        updated: m.num_target_rows_updated as u64,
        deleted: m.num_target_rows_deleted as u64,
    })
}
