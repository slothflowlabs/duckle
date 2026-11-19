//! Apache ORC for src.orc and snk.orc, through `orc-rust`.
//!
//! Both directions cross through Parquet, which DuckDB reads and writes itself:
//! ORC -> Arrow batches -> a Parquet temp file -> read_parquet, and the upstream
//! view -> COPY to Parquet -> Arrow batches -> ORC. `orc-rust` and `parquet` are
//! on the engine's arrow major, so a batch passes between them as it is.

use crate::*;
use std::path::PathBuf;

use arrow_schema::DataType;
use orc_rust::compression::CompressionType;
use orc_rust::{ArrowReaderBuilder, ArrowWriterBuilder, TimestampPrecision};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

/// The files a src.orc path names: the file itself, or each file a pattern
/// matches, in the order glob walks them, by name, so every run reads them alike.
pub(crate) fn orc_files(path: &str) -> Result<Vec<PathBuf>, String> {
    if !path.contains(['*', '?', '[']) {
        return Ok(vec![PathBuf::from(path)]);
    }
    let entries = glob::glob(path).map_err(|e| format!("pattern {path}: {e}"))?;
    let mut files = Vec::new();
    for entry in entries {
        let p = entry.map_err(|e| format!("pattern {path}: {e}"))?;
        if p.is_file() {
            files.push(p);
        }
    }
    if files.is_empty() {
        return Err(format!("no file matches {path}"));
    }
    Ok(files)
}

/// How snk.orc writes a DuckDB column. `orc-rust` writes flat columns of
/// booleans, integers, floats, text, binary, dates and timestamps. Handed any
/// other type it panics, so every column is decided before a byte is written.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OrcColumn {
    /// As DuckDB has it.
    AsIs,
    /// Cast first, to a type that holds every value: ORC has no unsigned
    /// integers, and no UUID, JSON or enum type, so those values go as text.
    Cast(&'static str),
    /// No type the writer has holds it without loss.
    Refused,
}

pub(crate) fn orc_column(duckdb_type: &str) -> OrcColumn {
    match duckdb_type.trim() {
        "BOOLEAN" | "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "FLOAT" | "DOUBLE" | "VARCHAR"
        | "BLOB" | "DATE" | "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_NS"
        | "TIMESTAMP WITH TIME ZONE" => OrcColumn::AsIs,
        "UTINYINT" => OrcColumn::Cast("SMALLINT"),
        "USMALLINT" => OrcColumn::Cast("INTEGER"),
        "UINTEGER" => OrcColumn::Cast("BIGINT"),
        "UUID" | "JSON" => OrcColumn::Cast("VARCHAR"),
        // ENUM('a', 'b'), but not a list of them, ENUM('a', 'b')[].
        t if t.starts_with("ENUM(") && t.ends_with(')') => OrcColumn::Cast("VARCHAR"),
        _ => OrcColumn::Refused,
    }
}

/// Whether the ORC writer has an encoder for the Arrow type. A column of any
/// other type reaches its `unimplemented!`.
fn writable(t: &DataType) -> bool {
    match t {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32 => true,
        DataType::Timestamp(_, zone) => matches!(zone.as_deref(), None | Some("UTC")),
        _ => false,
    }
}

fn codec(c: plan::OrcCompression) -> Option<CompressionType> {
    match c {
        plan::OrcCompression::None => None,
        plan::OrcCompression::Snappy => Some(CompressionType::Snappy),
        plan::OrcCompression::Zstd => Some(CompressionType::Zstd),
        plan::OrcCompression::Zlib => Some(CompressionType::Zlib),
        plan::OrcCompression::Lz4 => Some(CompressionType::Lz4),
    }
}

/// A Parquet temp file beside the run database, named for the node it serves.
pub(crate) fn temp_beside(db: &Path, tag: &str, node: &str) -> PathBuf {
    let safe: String = node
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();
    let db_name = db.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    db.with_file_name(format!("{db_name}.{tag}-{safe}.parquet"))
}

pub(crate) fn sql_path(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/").replace('\'', "''")
}

impl DuckdbEngine {
    /// src.orc: every file the path names, as one table. The files must have
    /// the same columns. Timestamps are read to the microsecond, as DuckDB's
    /// TIMESTAMP is, which also keeps a date before 1677 in range.
    pub(crate) fn run_orc_source(&self, db: &Path, spec: &plan::OrcSourceSpec) -> Result<String, EngineError> {
        let files = orc_files(&spec.path).map_err(|e| EngineError::Query(format!("orc: {e}")))?;
        let parquet_path = temp_beside(db, "orc", &spec.node_id);
        let rows = match self.orc_files_to_parquet(&files, &parquet_path) {
            Ok(n) => n,
            Err(e) => {
                let _ = std::fs::remove_file(&parquet_path);
                return Err(e);
            }
        };
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            sql_path(&parquet_path)
        );
        let created = self.run(Some(db), &create, false);
        let _ = std::fs::remove_file(&parquet_path);
        created?;
        Ok(format!(
            "orc: materialized {rows} records from {} file(s) into {}",
            files.len(),
            spec.node_id
        ))
    }

    fn orc_files_to_parquet(&self, files: &[PathBuf], out: &Path) -> Result<u64, EngineError> {
        let mut rows = 0_u64;
        // The Parquet writer, and the columns of the first file, which every
        // later file must match.
        let mut target: Option<(ArrowWriter<std::fs::File>, Vec<String>)> = None;
        for file in files {
            let fail = |e: &dyn std::fmt::Display| EngineError::Query(format!("orc: {}: {e}", file.display()));
            let opened = std::fs::File::open(file).map_err(|e| fail(&e))?;
            let reader = ArrowReaderBuilder::try_new(opened)
                .map_err(|e| fail(&e))?
                .with_timestamp_precision(TimestampPrecision::Microsecond)
                .build();
            let schema = arrow_array::RecordBatchReader::schema(&reader);
            let columns: Vec<String> =
                schema.fields().iter().map(|f| format!("{} {}", f.name(), f.data_type())).collect();
            match &target {
                None => {
                    let sink = std::fs::File::create(out)
                        .map_err(|e| EngineError::Query(format!("orc: temp file {}: {e}", out.display())))?;
                    let writer = ArrowWriter::try_new(sink, schema.clone(), None).map_err(|e| fail(&e))?;
                    target = Some((writer, columns));
                }
                Some((_, first)) if *first != columns => {
                    return Err(EngineError::Query(format!(
                        "orc: {} has the columns ({}) and {} has ({}); files read together must have the same columns",
                        files[0].display(),
                        first.join(", "),
                        file.display(),
                        columns.join(", ")
                    )));
                }
                Some(_) => {}
            }
            let (writer, _) = target.as_mut().expect("opened for the first file");
            for batch in reader {
                self.check_cancelled()?;
                let batch = batch.map_err(|e| fail(&e))?;
                rows += batch.num_rows() as u64;
                writer.write(&batch).map_err(|e| fail(&e))?;
            }
        }
        if let Some((writer, _)) = target {
            writer
                .close()
                .map_err(|e| EngineError::Query(format!("orc: temp file {}: {e}", out.display())))?;
        }
        Ok(rows)
    }

    /// snk.orc: the upstream rows as one ORC file, which replaces the old one
    /// only once it is complete. Every column is decided first, so a column
    /// ORC cannot take fails the run before anything is written.
    pub(crate) fn run_orc_sink(&self, db: &Path, spec: &plan::OrcSinkSpec) -> Result<String, EngineError> {
        let view = plan::quote_ident(&spec.from_view);
        let described = self.run_rows(Some(db), &format!("DESCRIBE SELECT * FROM {view}"))?;
        let mut select = Vec::new();
        let mut refused = Vec::new();
        for column in &described {
            let name = column.get("column_name").and_then(JsonValue::as_str).unwrap_or_default();
            let ty = column.get("column_type").and_then(JsonValue::as_str).unwrap_or_default();
            let q = plan::quote_ident(name);
            match orc_column(ty) {
                OrcColumn::AsIs => select.push(q),
                OrcColumn::Cast(to) => select.push(format!("CAST({q} AS {to}) AS {q}")),
                OrcColumn::Refused => refused.push(format!("{name} ({ty})")),
            }
        }
        if !refused.is_empty() {
            return Err(EngineError::Query(format!(
                "snk.orc cannot write {}: it writes booleans, integers, floats, text, binary, dates and \
                 timestamps. Cast these upstream: a DECIMAL to DOUBLE or VARCHAR, a TIME to VARCHAR, a \
                 list, struct or map to VARCHAR with to_json()",
                refused.join(", ")
            )));
        }
        let parquet_path = temp_beside(db, "orc-snk", &spec.from_view);
        let copy = format!(
            "COPY (SELECT {} FROM {view}) TO '{}' (FORMAT parquet)",
            select.join(", "),
            sql_path(&parquet_path)
        );
        let written = self.run(Some(db), &copy, false).and_then(|_| self.parquet_to_orc(&parquet_path, spec));
        let _ = std::fs::remove_file(&parquet_path);
        let rows = written?;
        Ok(format!("orc: wrote {rows} rows to {}", spec.path))
    }

    fn parquet_to_orc(&self, parquet_path: &Path, spec: &plan::OrcSinkSpec) -> Result<u64, EngineError> {
        let fail = |e: &dyn std::fmt::Display| EngineError::Query(format!("snk.orc: {}: {e}", spec.path));
        let opened = std::fs::File::open(parquet_path).map_err(|e| fail(&e))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(opened)
            .map_err(|e| fail(&e))?
            .build()
            .map_err(|e| fail(&e))?;
        let schema = arrow_array::RecordBatchReader::schema(&reader);
        // The columns DESCRIBE let through should all arrive as types the writer
        // has; this keeps any that do not away from its panic.
        if let Some(f) = schema.fields().iter().find(|f| !writable(f.data_type())) {
            return Err(fail(&format!(
                "column {} arrived as {}, which the ORC writer has no encoder for",
                f.name(),
                f.data_type()
            )));
        }
        // Written beside the target and renamed over it when complete, so a
        // failed write never leaves half a file where the old one was.
        let partial = PathBuf::from(format!("{}.duckle-partial", spec.path));
        let result = (|| {
            let out = std::fs::File::create(&partial).map_err(|e| fail(&e))?;
            let mut builder = ArrowWriterBuilder::new(out, schema);
            if let Some(c) = codec(spec.compression) {
                builder = builder.with_compression(c);
            }
            let mut writer = builder.try_build().map_err(|e| fail(&e))?;
            let mut rows = 0_u64;
            for batch in reader {
                self.check_cancelled()?;
                let batch = batch.map_err(|e| fail(&e))?;
                rows += batch.num_rows() as u64;
                writer.write(&batch).map_err(|e| fail(&e))?;
            }
            writer.close().map_err(|e| fail(&e))?;
            std::fs::rename(&partial, &spec.path).map_err(|e| fail(&e))?;
            Ok(rows)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_duckdb_type_is_written_as_is_widened_or_refused() {
        for t in [
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "VARCHAR", "BLOB",
            "DATE", "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS", "TIMESTAMP WITH TIME ZONE",
        ] {
            assert_eq!(orc_column(t), OrcColumn::AsIs, "{t}");
        }
        assert_eq!(orc_column("UTINYINT"), OrcColumn::Cast("SMALLINT"));
        assert_eq!(orc_column("USMALLINT"), OrcColumn::Cast("INTEGER"));
        assert_eq!(orc_column("UINTEGER"), OrcColumn::Cast("BIGINT"));
        for t in ["UUID", "JSON", "ENUM('a', 'b')"] {
            assert_eq!(orc_column(t), OrcColumn::Cast("VARCHAR"), "{t}");
        }
        // No lossless home: UBIGINT and HUGEINT outgrow BIGINT, and the writer
        // has no decimal, time, interval or nested type.
        for t in [
            "UBIGINT", "HUGEINT", "UHUGEINT", "DECIMAL(10,2)", "TIME", "TIME WITH TIME ZONE", "INTERVAL",
            "INTEGER[]", "TIMESTAMP[]", "VARCHAR[3]", "ENUM('a', 'b')[]", "STRUCT(a INTEGER)",
            "MAP(VARCHAR, INTEGER)", "BIT",
        ] {
            assert_eq!(orc_column(t), OrcColumn::Refused, "{t}");
        }
    }

    #[test]
    fn only_types_the_writer_encodes_reach_it() {
        use arrow_schema::TimeUnit;
        assert!(writable(&DataType::Utf8));
        assert!(writable(&DataType::Timestamp(TimeUnit::Microsecond, None)));
        assert!(writable(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))));
        // Another zone, a decimal or a view type would reach its unimplemented!.
        assert!(!writable(&DataType::Timestamp(TimeUnit::Microsecond, Some("+02:00".into()))));
        assert!(!writable(&DataType::Decimal128(10, 2)));
        assert!(!writable(&DataType::Utf8View));
        assert!(!writable(&DataType::UInt8));
    }

    #[test]
    fn a_pattern_reads_every_file_it_matches_in_name_order() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["b.orc", "a.orc", "notes.txt"] {
            std::fs::write(dir.path().join(name), "x").unwrap();
        }
        std::fs::create_dir(dir.path().join("c.orc")).unwrap();
        let root = dir.path().to_string_lossy().replace('\\', "/");
        let found = orc_files(&format!("{root}/*.orc")).unwrap();
        let names: Vec<_> = found.iter().map(|p| p.file_name().unwrap().to_string_lossy().into_owned()).collect();
        assert_eq!(names, ["a.orc", "b.orc"], "files only, in name order");

        let err = orc_files(&format!("{root}/*.parquet")).unwrap_err();
        assert!(err.contains("no file matches"), "{err}");
        // A plain path is read as given; a missing one fails when it is opened.
        assert_eq!(orc_files("/lake/t.orc").unwrap(), [PathBuf::from("/lake/t.orc")]);
    }

    fn parquet_file(dir: &Path, column: &str, values: arrow_array::ArrayRef) -> PathBuf {
        let batch = arrow_array::RecordBatch::try_from_iter([(column, values)]).unwrap();
        let path = dir.join("in.parquet");
        let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        path
    }

    fn sink_to(target: &Path) -> plan::OrcSinkSpec {
        plan::OrcSinkSpec {
            from_view: "v".into(),
            path: target.to_string_lossy().into_owned(),
            compression: plan::OrcCompression::Snappy,
        }
    }

    #[test]
    fn a_column_the_writer_has_no_encoder_for_is_refused_not_panicked_on() {
        let dir = tempfile::tempdir().unwrap();
        let amounts = arrow_array::Decimal128Array::from(vec![1250_i128]).with_precision_and_scale(5, 2).unwrap();
        let parquet = parquet_file(dir.path(), "amount", std::sync::Arc::new(amounts));
        let target = dir.path().join("out.orc");
        let engine = DuckdbEngine::new("no-duckdb-needed".into());
        let err = engine.parquet_to_orc(&parquet, &sink_to(&target)).unwrap_err().to_string();
        assert!(err.contains("amount") && err.contains("no encoder"), "{err}");
        assert!(!target.exists(), "nothing written");
    }

    #[test]
    fn a_write_that_stops_part_way_leaves_the_old_file_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let parquet = parquet_file(dir.path(), "id", std::sync::Arc::new(arrow_array::Int64Array::from(vec![1_i64, 2, 3])));
        let target = dir.path().join("out.orc");
        std::fs::write(&target, "the file from the last run").unwrap();
        let engine = DuckdbEngine::new("no-duckdb-needed".into());
        engine.cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(engine.parquet_to_orc(&parquet, &sink_to(&target)).is_err(), "a cancelled run stops");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "the file from the last run");
        assert!(!dir.path().join("out.orc.duckle-partial").exists(), "nothing left beside it");
    }
}
