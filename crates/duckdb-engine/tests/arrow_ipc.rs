//! src.arrow and snk.arrow: Arrow IPC files (Feather v2) and streams, written
//! and read here with the arrow crates, so what the tests check is what pandas,
//! Polars and R hand Duckle and get from it.

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow_ipc::CompressionType;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

fn engine() -> Option<DuckdbEngine> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok()?;
    let p = std::path::PathBuf::from(bin);
    p.exists().then(|| DuckdbEngine::new(p))
}

macro_rules! engine_or_skip {
    () => {
        match engine() {
            Some(e) => e,
            None => {
                eprintln!("skipping: set DUCKLE_DUCKDB_BIN to a duckdb CLI to run");
                return;
            }
        }
    };
}

fn norm(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({"id": id, "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": id, "componentId": component, "properties": props}})
}

fn edge(id: &str, a: &str, b: &str) -> Value {
    json!({"id": id, "source": a, "target": b, "sourceHandle": "main", "targetHandle": "main", "data": {"connectionType": "main"}})
}

fn doc(nodes: Value, edges: Value) -> PipelineDoc {
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

/// src.arrow reading `path`, into a Parquet file at `out`.
fn read(engine: &DuckdbEngine, path: &str, out: &str) -> RunResult {
    engine.execute_pipeline(&doc(
        json!([node("a", "src.arrow", json!({ "path": path })), node("k", "snk.parquet", json!({ "path": out }))]),
        json!([edge("e1", "a", "k")]),
    ))
}

/// What `sql` selects, written by snk.arrow with `props`.
fn write(engine: &DuckdbEngine, dir: &Path, sql: &str, props: Value) -> RunResult {
    let kick = dir.join("kick.csv");
    std::fs::write(&kick, "k\n1\n").unwrap();
    engine.execute_pipeline(&doc(
        json!([
            node("s", "src.csv", json!({ "path": norm(&kick), "hasHeader": true })),
            node("q", "code.sql", json!({ "sql": sql })),
            node("w", "snk.arrow", props),
        ]),
        json!([edge("e1", "s", "q"), edge("e2", "q", "w")]),
    ))
}

/// Rows `ids`, as pandas would hand them over: a decimal, a zoned timestamp, a
/// date, a list and a categorical (dictionary) column among them.
fn orders(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("price", DataType::Decimal128(10, 2), true),
        Field::new("seen", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("born", DataType::Date32, true),
        Field::new("ok", DataType::Boolean, true),
        Field::new_list("tags", Field::new_list_field(DataType::Utf8, true), true),
        Field::new("kind", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
    ]));
    let mut tags = ListBuilder::new(StringBuilder::new());
    for id in ids {
        tags.values().append_value(format!("t{id}"));
        tags.values().append_value("x");
        tags.append(true);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(StringArray::from(ids.iter().map(|i| format!("n{i}")).collect::<Vec<_>>())),
        Arc::new(
            Decimal128Array::from(ids.iter().map(|i| i128::from(*i) * 150).collect::<Vec<_>>())
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(TimestampMicrosecondArray::from(ids.iter().map(|i| 1_795_082_400_000_000 + i).collect::<Vec<_>>()).with_timezone("UTC")),
        Arc::new(Date32Array::from(ids.iter().map(|i| 20_776 + *i as i32).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(ids.iter().map(|i| i % 2 == 1).collect::<Vec<_>>())),
        Arc::new(tags.finish()),
        Arc::new(ids.iter().map(|i| if i % 2 == 1 { "odd" } else { "even" }).collect::<DictionaryArray<Int32Type>>()),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn write_file(path: &Path, batches: &[RecordBatch], options: IpcWriteOptions) {
    let mut w = FileWriter::try_new_with_options(std::fs::File::create(path).unwrap(), &batches[0].schema(), options).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
}

fn write_stream(path: &Path, batches: &[RecordBatch], options: IpcWriteOptions) {
    let mut w = StreamWriter::try_new_with_options(std::fs::File::create(path).unwrap(), &batches[0].schema(), options).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
}

fn compressed(codec: CompressionType) -> IpcWriteOptions {
    IpcWriteOptions::default().try_with_compression(Some(codec)).unwrap()
}

fn ids(out: &str) -> Vec<i64> {
    duckdb_json(&format!("SELECT id FROM read_parquet('{out}') ORDER BY id")).iter().map(|r| r["id"].as_i64().unwrap()).collect()
}

#[test]
fn arrow_source_reads_a_feather_file_with_its_types() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("orders.feather");
    // The file format keeps one dictionary a column, so both batches build the same one.
    write_file(&file, &[orders(&[1, 2]), orders(&[3, 4])], IpcWriteOptions::default());
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&engine, &norm(&file), &out);
    assert_eq!(r.status, "ok", "src.arrow failed: {:?}", r.error);
    let types: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        ["id BIGINT", "name VARCHAR", "price DECIMAL(10,2)", "seen TIMESTAMP WITH TIME ZONE", "born DATE", "ok BOOLEAN",
         "tags VARCHAR[]", "kind VARCHAR"]
    );
    let rows = duckdb_json(&format!(
        "SELECT id, name, CAST(price AS VARCHAR) AS price, epoch_us(seen) AS seen_us, CAST(born AS VARCHAR) AS born, ok, tags, kind \
         FROM read_parquet('{out}') ORDER BY id"
    ));
    assert_eq!(rows.len(), 4, "both record batches");
    assert_eq!(
        rows[0],
        json!({"id": 1, "name": "n1", "price": "1.50", "seen_us": 1795082400000001_i64, "born": "2026-11-20", "ok": true,
               "tags": ["t1", "x"], "kind": "odd"})
    );
}

#[test]
fn arrow_source_reads_a_stream_and_compressed_files() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    write_stream(&tmp.path().join("a.arrows"), &[orders(&[1])], IpcWriteOptions::default());
    write_file(&tmp.path().join("b.arrow"), &[orders(&[2])], compressed(CompressionType::ZSTD));
    write_file(&tmp.path().join("c.arrow"), &[orders(&[3])], compressed(CompressionType::LZ4_FRAME));
    for (name, id) in [("a.arrows", 1), ("b.arrow", 2), ("c.arrow", 3)] {
        let out = norm(&tmp.path().join(format!("{name}.parquet")));
        let r = read(&engine, &norm(&tmp.path().join(name)), &out);
        assert_eq!(r.status, "ok", "{name}: {:?}", r.error);
        assert_eq!(ids(&out), [id], "{name}");
    }
}

#[test]
fn arrow_source_reads_every_file_a_pattern_names_and_refuses_mixed_columns() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    write_file(&tmp.path().join("p1.arrow"), &[orders(&[1, 2])], IpcWriteOptions::default());
    write_file(&tmp.path().join("p2.arrow"), &[orders(&[3])], IpcWriteOptions::default());
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&engine, &norm(&tmp.path().join("p*.arrow")), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(ids(&out), [1, 2, 3]);
    let other = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![9])) as ArrayRef],
    )
    .unwrap();
    write_file(&tmp.path().join("p3.arrow"), &[other], IpcWriteOptions::default());
    let r = read(&engine, &norm(&tmp.path().join("p*.arrow")), &out);
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("p3.arrow") && err.contains("same columns"), "{err}");
}

#[test]
fn arrow_source_says_what_a_file_is_not() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let out = norm(&tmp.path().join("out.parquet"));
    std::fs::write(tmp.path().join("plain.arrow"), "id,name\n1,a\n").unwrap();
    let r = read(&engine, &norm(&tmp.path().join("plain.arrow")), &out);
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("neither an Arrow IPC file"), "{:?}", r.error);
    std::fs::write(tmp.path().join("old.feather"), b"FEA1\0\0\0\0rest").unwrap();
    let r = read(&engine, &norm(&tmp.path().join("old.feather")), &out);
    assert_eq!(r.status, "error");
    assert!(format!("{:?}", r.error).contains("Feather version 1"), "{:?}", r.error);
}

#[test]
fn arrow_sink_writes_a_file_with_its_types() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.arrow");
    let r = write(
        &engine,
        tmp.path(),
        "SELECT 1::BIGINT AS id, 'a' AS name, 12.34::DECIMAL(10,2) AS price, TIMESTAMPTZ '2026-11-19 10:00:00+00' AS seen, \
         DATE '2026-11-19' AS born, true AS ok, ['x', 'y'] AS tags, 12345678901234567890123456789012345678::HUGEINT AS big, \
         '0193b4b6-0000-7000-8000-000000000001'::UUID AS u FROM input",
        json!({ "path": norm(&path) }),
    );
    assert_eq!(r.status, "ok", "snk.arrow failed: {:?}", r.error);
    let reader = FileReader::try_new(std::fs::File::open(&path).unwrap(), None).expect("an Arrow IPC file");
    let schema = reader.schema();
    let types: Vec<(String, DataType)> = schema.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect();
    assert_eq!(types[0], ("id".into(), DataType::Int64));
    assert_eq!(types[1], ("name".into(), DataType::Utf8));
    assert_eq!(types[2], ("price".into(), DataType::Decimal128(10, 2)));
    assert!(matches!(types[3].1, DataType::Timestamp(TimeUnit::Microsecond, Some(_))), "{:?}", types[3]);
    assert_eq!(types[4], ("born".into(), DataType::Date32));
    assert_eq!(types[5], ("ok".into(), DataType::Boolean));
    assert!(matches!(&types[6].1, DataType::List(f) if f.data_type() == &DataType::Utf8), "{:?}", types[6]);
    assert_eq!(types[7], ("big".into(), DataType::Decimal128(38, 0)), "a HUGEINT whole, not a DOUBLE");
    assert_eq!(types[8], ("u".into(), DataType::Utf8), "a UUID as its text");
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let big = batches[0].column(7).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(big.value(0), 12345678901234567890123456789012345678_i128);
    let u = batches[0].column(8).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(u.value(0), "0193b4b6-0000-7000-8000-000000000001");
    assert!(!tmp.path().join("out.arrow.duckle-partial").exists());
}

#[test]
fn arrow_sink_writes_a_compressed_stream_and_file() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let sql = "SELECT i, repeat('x', 1000) AS pad FROM input, range(1, 2001) t(i)";
    let stream = tmp.path().join("out.arrows");
    let r = write(&engine, tmp.path(), sql, json!({ "path": norm(&stream), "format": "stream", "compression": "zstd" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let bytes = std::fs::read(&stream).unwrap();
    assert_eq!(&bytes[..4], [0xFF, 0xFF, 0xFF, 0xFF], "the stream format");
    assert!(bytes.len() < 200_000, "2 MB of padding, compressed: {} bytes", bytes.len());
    let rows: usize = StreamReader::try_new(std::fs::File::open(&stream).unwrap(), None).unwrap().map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 2000);
    let file = tmp.path().join("out.arrow");
    let r = write(&engine, tmp.path(), sql, json!({ "path": norm(&file), "compression": "lz4" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let bytes = std::fs::read(&file).unwrap();
    assert_eq!(&bytes[..6], b"ARROW1", "the file format, by default");
    assert!(bytes.len() < 200_000, "{} bytes", bytes.len());
}

#[test]
fn arrow_sink_leaves_the_old_file_when_the_write_fails() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("out.arrow");
    std::fs::write(&path, b"the old file").unwrap();
    // 39 digits: more than DECIMAL(38,0) holds, so the sink's own cast fails.
    let r = write(&engine, tmp.path(), "SELECT 170141183460469231731687303715884105727::HUGEINT AS big FROM input", json!({ "path": norm(&path) }));
    assert_eq!(r.status, "error");
    assert_eq!(std::fs::read(&path).unwrap(), b"the old file");
    assert!(!tmp.path().join("out.arrow.duckle-partial").exists());
}
