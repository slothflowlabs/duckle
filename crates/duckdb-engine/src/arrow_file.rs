//! Apache Arrow IPC for src.arrow and snk.arrow: the file format Feather v2 is,
//! and the stream format, as pandas, Polars, R and Spark write them.
//!
//! Both directions cross through Parquet, which DuckDB reads and writes itself,
//! as src.orc does: Arrow batches -> a Parquet temp file -> read_parquet, and the
//! upstream view -> COPY to Parquet -> Arrow batches -> an IPC file. The arrow
//! crates and `parquet` share the engine's arrow major, so a batch passes as it is.

use crate::orc::{orc_files, sql_path, temp_beside};
use crate::*;
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::io::Read;
use std::path::PathBuf;

/// What a file's first bytes say it is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IpcKind {
    /// "ARROW1": the IPC file format, which Feather v2 is.
    File,
    /// A continuation marker: the IPC stream format.
    Stream,
    /// "FEA1": Feather v1, retired since Arrow 0.17.
    FeatherV1,
    Other,
}

pub(crate) fn ipc_kind(head: &[u8]) -> IpcKind {
    if head.starts_with(b"ARROW1") {
        IpcKind::File
    } else if head.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]) {
        IpcKind::Stream
    } else if head.starts_with(b"FEA1") {
        IpcKind::FeatherV1
    } else {
        IpcKind::Other
    }
}

/// What snk.arrow casts a DuckDB column to before it crosses Parquet, where
/// DuckDB would otherwise narrow or hide it; None: as it is.
pub(crate) fn arrow_cast(duckdb_type: &str) -> Option<&'static str> {
    match duckdb_type.trim() {
        // DuckDB writes a HUGEINT to Parquet as a DOUBLE, losing digits.
        "HUGEINT" | "UHUGEINT" => Some("DECIMAL(38,0)"),
        // A UUID would arrive as 16 bytes, BIT and BIGNUM as bytes or worse.
        "UUID" | "BIT" | "BIGNUM" | "VARINT" => Some("VARCHAR"),
        _ => None,
    }
}

fn read_batches(file: &Path) -> Result<Box<dyn arrow_array::RecordBatchReader>, String> {
    let mut head = [0_u8; 8];
    let mut opened = std::fs::File::open(file).map_err(|e| e.to_string())?;
    let n = opened.read(&mut head).map_err(|e| e.to_string())?;
    let opened = std::fs::File::open(file).map_err(|e| e.to_string())?;
    match ipc_kind(&head[..n]) {
        IpcKind::File => Ok(Box::new(FileReader::try_new(opened, None).map_err(|e| e.to_string())?)),
        IpcKind::Stream => Ok(Box::new(StreamReader::try_new(opened, None).map_err(|e| e.to_string())?)),
        IpcKind::FeatherV1 => Err("this is Feather version 1, retired since Arrow 0.17; save it again as Feather \
             version 2 (pyarrow.feather.write_feather, or pandas DataFrame.to_feather), which this reads"
            .into()),
        IpcKind::Other => Err("this is neither an Arrow IPC file (Feather version 2) nor an Arrow IPC stream".into()),
    }
}

impl DuckdbEngine {
    /// src.arrow: every file the path names, as one table. The files must have
    /// the same columns.
    pub(crate) fn run_arrow_source(&self, db: &Path, spec: &plan::ArrowSourceSpec) -> Result<String, EngineError> {
        let files = orc_files(&spec.path).map_err(|e| EngineError::Query(format!("arrow: {e}")))?;
        let parquet_path = temp_beside(db, "arrow", &spec.node_id);
        let rows = match self.arrow_files_to_parquet(&files, &parquet_path) {
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
        Ok(format!("arrow: materialized {rows} records from {} file(s) into {}", files.len(), spec.node_id))
    }

    fn arrow_files_to_parquet(&self, files: &[PathBuf], out: &Path) -> Result<u64, EngineError> {
        let mut rows = 0_u64;
        // The Parquet writer, and the columns of the first file, which every
        // later file must match.
        let mut target: Option<(ArrowWriter<std::fs::File>, Vec<String>)> = None;
        for file in files {
            let fail = |e: &dyn std::fmt::Display| EngineError::Query(format!("arrow: {}: {e}", file.display()));
            let reader = read_batches(file).map_err(|e| fail(&e))?;
            let schema = reader.schema();
            let columns: Vec<String> = schema.fields().iter().map(|f| format!("{} {}", f.name(), f.data_type())).collect();
            match &target {
                None => {
                    let sink = std::fs::File::create(out)
                        .map_err(|e| EngineError::Query(format!("arrow: temp file {}: {e}", out.display())))?;
                    let writer = ArrowWriter::try_new(sink, schema.clone(), None).map_err(|e| fail(&e))?;
                    target = Some((writer, columns));
                }
                Some((_, first)) if *first != columns => {
                    return Err(EngineError::Query(format!(
                        "arrow: {} has the columns ({}) and {} has ({}); files read together must have the same columns",
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
            writer.close().map_err(|e| EngineError::Query(format!("arrow: temp file {}: {e}", out.display())))?;
        }
        Ok(rows)
    }

    /// snk.arrow: the upstream rows as one Arrow IPC file or stream, which
    /// replaces the old one only once it is complete.
    pub(crate) fn run_arrow_sink(&self, db: &Path, spec: &plan::ArrowSinkSpec) -> Result<String, EngineError> {
        let view = plan::quote_ident(&spec.from_view);
        let described = self.run_rows(Some(db), &format!("DESCRIBE SELECT * FROM {view}"))?;
        let select: Vec<String> = described
            .iter()
            .map(|column| {
                let name = column.get("column_name").and_then(JsonValue::as_str).unwrap_or_default();
                let ty = column.get("column_type").and_then(JsonValue::as_str).unwrap_or_default();
                let q = plan::quote_ident(name);
                match arrow_cast(ty) {
                    Some(to) => format!("CAST({q} AS {to}) AS {q}"),
                    None => q,
                }
            })
            .collect();
        let parquet_path = temp_beside(db, "arrow-snk", &spec.from_view);
        let copy = format!("COPY (SELECT {} FROM {view}) TO '{}' (FORMAT parquet)", select.join(", "), sql_path(&parquet_path));
        let written = self.run(Some(db), &copy, false).and_then(|_| self.parquet_to_arrow(&parquet_path, spec));
        let _ = std::fs::remove_file(&parquet_path);
        let rows = written?;
        Ok(format!("arrow: wrote {rows} rows to {}", spec.path))
    }

    fn parquet_to_arrow(&self, parquet_path: &Path, spec: &plan::ArrowSinkSpec) -> Result<u64, EngineError> {
        let fail = |e: &dyn std::fmt::Display| EngineError::Query(format!("snk.arrow: {}: {e}", spec.path));
        let opened = std::fs::File::open(parquet_path).map_err(|e| fail(&e))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(opened)
            .map_err(|e| fail(&e))?
            .build()
            .map_err(|e| fail(&e))?;
        let schema = arrow_array::RecordBatchReader::schema(&reader);
        let codec = match spec.compression {
            plan::ArrowCompression::None => None,
            plan::ArrowCompression::Lz4 => Some(arrow_ipc::CompressionType::LZ4_FRAME),
            plan::ArrowCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
        };
        let options = IpcWriteOptions::default().try_with_compression(codec).map_err(|e| fail(&e))?;
        // Written beside the target and renamed over it when complete, so a
        // failed write never leaves half a file where the old one was.
        let partial = PathBuf::from(format!("{}.duckle-partial", spec.path));
        let result = (|| {
            let out = std::fs::File::create(&partial).map_err(|e| fail(&e))?;
            let mut rows = 0_u64;
            if spec.stream {
                let mut writer = StreamWriter::try_new_with_options(out, &schema, options).map_err(|e| fail(&e))?;
                for batch in reader {
                    self.check_cancelled()?;
                    let batch = batch.map_err(|e| fail(&e))?;
                    rows += batch.num_rows() as u64;
                    writer.write(&batch).map_err(|e| fail(&e))?;
                }
                writer.finish().map_err(|e| fail(&e))?;
            } else {
                let mut writer = FileWriter::try_new_with_options(out, &schema, options).map_err(|e| fail(&e))?;
                for batch in reader {
                    self.check_cancelled()?;
                    let batch = batch.map_err(|e| fail(&e))?;
                    rows += batch.num_rows() as u64;
                    writer.write(&batch).map_err(|e| fail(&e))?;
                }
                writer.finish().map_err(|e| fail(&e))?;
            }
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
    fn a_file_says_what_it_is_in_its_first_bytes() {
        assert_eq!(ipc_kind(b"ARROW1\0\0"), IpcKind::File);
        assert_eq!(ipc_kind(&[0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0, 0, 0]), IpcKind::Stream);
        assert_eq!(ipc_kind(b"FEA1\0\0\0\0"), IpcKind::FeatherV1);
        assert_eq!(ipc_kind(b"id,name\n"), IpcKind::Other);
        assert_eq!(ipc_kind(b""), IpcKind::Other);
    }

    #[test]
    fn only_what_parquet_would_narrow_or_hide_is_cast() {
        assert_eq!(arrow_cast("HUGEINT"), Some("DECIMAL(38,0)"));
        assert_eq!(arrow_cast("UUID"), Some("VARCHAR"));
        assert_eq!(arrow_cast("BIGINT"), None);
        assert_eq!(arrow_cast("DECIMAL(10,2)"), None);
        assert_eq!(arrow_cast("VARCHAR[]"), None);
    }
}
