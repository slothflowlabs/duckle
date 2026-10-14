//! A Parquet file goes through `write` and comes back out of `read` with the
//! same values, and an append adds to the table rather than replacing it.
//!
//! These are the two subcommands snk.lancedb and src.lancedb shell out to in
//! connectors.rs, so this drives the real binary, as vortex_roundtrip.rs does,
//! and for the same reason: a lancedb release can compile against this crate
//! and still write or read the wrong thing, and before this test nothing ran
//! lancedb at all.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Float64Type, Int64Type};
use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};

const ROWS: i64 = 1000;

/// An integer key, a string, a float, a boolean, a date and a column with nulls.
fn fixture() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("d", DataType::Date32, false),
        Field::new("sparse", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new((0..ROWS).collect::<Int64Array>()),
            Arc::new((0..ROWS).map(|i| Some(format!("row_{i}"))).collect::<StringArray>()),
            Arc::new((0..ROWS).map(|i| i as f64 * 1.5).collect::<Float64Array>()),
            Arc::new((0..ROWS).map(|i| Some(i % 2 == 0)).collect::<BooleanArray>()),
            Arc::new((0..ROWS).map(|i| Some(20454 + i as i32)).collect::<Date32Array>()),
            Arc::new(
                (0..ROWS)
                    .map(|i| if i % 7 == 0 { None } else { Some(i) })
                    .collect::<Int64Array>(),
            ),
        ],
    )
    .expect("build fixture batch")
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
        .expect("parquet writer");
    w.write(batch).expect("write batch");
    w.close().expect("close parquet");
}

fn read_parquet(path: &Path) -> Vec<RecordBatch> {
    let file = std::fs::File::open(path).expect("open parquet");
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader")
        .build()
        .expect("build reader")
        .collect::<Result<_, _>>()
        .expect("read parquet")
}

/// Run the sidecar and fail with its own stderr, which says what went wrong.
fn run(args: &[&str]) {
    let out = Command::new(env!("CARGO_BIN_EXE_duckle-lance"))
        .args(args)
        .output()
        .expect("spawn duckle-lance");
    assert!(
        out.status.success(),
        "duckle-lance {} failed ({}): {}",
        args[0],
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
}

/// One cell as a comparable value. An unexpected type panics rather than being
/// skipped, so a round trip that changed a column's type fails here.
fn cell(col: &dyn Array, i: usize) -> Option<String> {
    if col.is_null(i) {
        return None;
    }
    Some(match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().value(i).to_string(),
        DataType::Float64 => col.as_primitive::<Float64Type>().value(i).to_string(),
        DataType::Boolean => col.as_boolean().value(i).to_string(),
        DataType::Date32 => col.as_primitive::<Date32Type>().value(i).to_string(),
        DataType::Utf8 => col.as_string::<i32>().value(i).to_string(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(i).to_string(),
        DataType::Utf8View => col.as_string_view().value(i).to_string(),
        other => panic!("round trip produced an unexpected type: {other:?}"),
    })
}

/// Every row as its cells, in column order, grouped by `id`. A scan need not
/// return rows in the order they were written, so rows are compared by key.
fn rows_by_id(batches: &[RecordBatch]) -> BTreeMap<String, Vec<Vec<Option<String>>>> {
    let mut rows: BTreeMap<String, Vec<Vec<Option<String>>>> = BTreeMap::new();
    for b in batches {
        for i in 0..b.num_rows() {
            let row: Vec<_> = b.columns().iter().map(|c| cell(c.as_ref(), i)).collect();
            let id = row[0].clone().expect("id is never null");
            rows.entry(id).or_default().push(row);
        }
    }
    rows
}

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch.schema().fields().iter().map(|f| f.name().clone()).collect()
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("duckle-lance-lt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir.join(name)
}

#[test]
fn a_parquet_file_survives_a_lance_round_trip_and_an_append_adds_to_it() {
    let src = scratch("src.parquet");
    let db = scratch("db");
    let back = scratch("back.parquet");
    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&back);
    let _ = std::fs::remove_dir_all(&db);

    let batch = fixture();
    write_parquet(&src, &batch);
    let (src_s, db_s, back_s) = (src.to_str().unwrap(), db.to_str().unwrap(), back.to_str().unwrap());

    run(&["write", "--uri", db_s, "--table", "t", "--in", src_s]);
    run(&["read", "--uri", db_s, "--table", "t", "--out", back_s]);

    // Values, not just a row count: a round trip that loses the nulls or shifts
    // the dates keeps the count and is still wrong.
    let want = rows_by_id(std::slice::from_ref(&batch));
    let got_batches = read_parquet(&back);
    for b in &got_batches {
        assert_eq!(column_names(b), column_names(&batch), "columns changed");
    }
    let got = rows_by_id(&got_batches);
    assert_eq!(got.len(), ROWS as usize, "row count changed");
    assert_eq!(got, want, "rows came back different");

    // append opens the table and adds to it; every row is then there twice.
    run(&["write", "--uri", db_s, "--table", "t", "--in", src_s, "--mode", "append"]);
    run(&["read", "--uri", db_s, "--table", "t", "--out", back_s]);
    let twice = rows_by_id(&read_parquet(&back));
    assert_eq!(twice.len(), ROWS as usize, "append changed the set of ids");
    for (id, rows) in &twice {
        assert_eq!(rows.len(), 2, "id {id} should appear twice after one append");
        assert_eq!(rows[0], rows[1], "id {id}: the appended copy differs");
        assert_eq!(rows[0], want[id][0], "id {id}: the appended rows differ from the source");
    }

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&back);
    let _ = std::fs::remove_dir_all(&db);
}
