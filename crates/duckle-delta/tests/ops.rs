//! The sidecar against real Delta tables in a temp directory: every mode, the delete
//! flag, creating a missing table, the refusals, and that what it writes reads back.

use std::path::Path;
use std::sync::Arc;

use deltalake::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::parquet::arrow::ArrowWriter;
use duckle_delta::{run, Mode, Outcome, Request};

fn ints(v: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(v.to_vec()))
}

fn strs(v: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(v.to_vec()))
}

/// Write `cols` to a Parquet file in `dir`, the way the engine hands over a node's input.
fn parquet(dir: &Path, name: &str, cols: Vec<(&str, ArrayRef)>) -> String {
    let fields: Vec<Field> = cols.iter().map(|(n, a)| Field::new(*n, a.data_type().clone(), true)).collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    let path = dir.join(name);
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    path.to_string_lossy().into_owned()
}

fn req(table: &str, source: &str, mode: Mode) -> Request {
    Request {
        table: table.into(),
        source: source.into(),
        mode,
        keys: vec![],
        delete_column: None,
        delete_value: None,
        create_if_missing: true,
        partition_by: vec![],
        storage_options: Default::default(),
    }
}

fn keyed(table: &str, source: &str, mode: Mode, key: &str) -> Request {
    Request { keys: vec![key.into()], ..req(table, source, mode) }
}

/// Every row of the table as "a|b|...", ordered by `order`, NULL spelled out. A column
/// the table should not have shows up as an extra field.
async fn rows(table: &str, order: &str) -> Vec<String> {
    let t = deltalake::open_table(deltalake::ensure_table_uri(table).unwrap()).await.unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("t", t.table_provider().await.unwrap()).unwrap();
    let batches = ctx.sql(&format!("SELECT * FROM t ORDER BY {order}")).await.unwrap().collect().await.unwrap();
    let mut out = vec![];
    for b in &batches {
        let cols: Vec<ArrayRef> = b.columns().iter().map(|c| cast(c, &DataType::Utf8).unwrap()).collect();
        for r in 0..b.num_rows() {
            let fields: Vec<String> = cols
                .iter()
                .map(|c| {
                    let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                    if s.is_null(r) { "NULL".to_string() } else { s.value(r).to_string() }
                })
                .collect();
            out.push(fields.join("|"));
        }
    }
    out
}

fn counts(o: &Outcome) -> (u64, u64, u64) {
    (o.inserted, o.updated, o.deleted)
}

/// A table at `<dir>/t` holding `ids` with names `a`, `b`, `c`... in that order.
async fn table_with(dir: &Path, ids: &[i64]) -> String {
    let table = dir.join("t").to_string_lossy().into_owned();
    let names: Vec<String> = ids.iter().map(|i| ((b'a' + (*i as u8) - 1) as char).to_string()).collect();
    let names: Vec<Option<&str>> = names.iter().map(|s| Some(s.as_str())).collect();
    let src = parquet(dir, "seed.parquet", vec![("id", ints(ids)), ("name", strs(&names))]);
    run(req(&table, &src, Mode::Append)).await.unwrap();
    table
}

#[tokio::test]
async fn append_creates_the_table_then_appends() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("t").to_string_lossy().into_owned();
    let first = parquet(dir.path(), "1.parquet", vec![("id", ints(&[1, 2])), ("name", strs(&[Some("a"), Some("b")]))]);
    let made = run(req(&table, &first, Mode::Append)).await.unwrap();
    assert!(made.created, "{made:?}");
    assert_eq!(made.inserted, 2);
    let second = parquet(dir.path(), "2.parquet", vec![("id", ints(&[3])), ("name", strs(&[Some("c")]))]);
    let added = run(req(&table, &second, Mode::Append)).await.unwrap();
    assert!(!added.created);
    assert_eq!(added.version, made.version + 1);
    assert_eq!(rows(&table, "id").await, ["1|a", "2|b", "3|c"]);
}

#[tokio::test]
async fn upsert_updates_matched_rows_and_inserts_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2]).await;
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[2, 3])), ("name", strs(&[Some("B"), Some("c")]))]);
    let out = run(keyed(&table, &src, Mode::Upsert, "id")).await.unwrap();
    assert_eq!(rows(&table, "id").await, ["1|a", "2|B", "3|c"]);
    assert_eq!(counts(&out), (1, 1, 0));
}

#[tokio::test]
async fn update_changes_matched_rows_and_inserts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2]).await;
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[2, 3])), ("name", strs(&[Some("B"), Some("c")]))]);
    let out = run(keyed(&table, &src, Mode::Update, "id")).await.unwrap();
    assert_eq!(rows(&table, "id").await, ["1|a", "2|B"]);
    assert_eq!(counts(&out), (0, 1, 0));
}

#[tokio::test]
async fn update_may_name_only_some_columns_and_leaves_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2]).await;
    // Only the key: nothing to set, so nothing changes, and that is not an error.
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[2]))]);
    run(keyed(&table, &src, Mode::Update, "id")).await.unwrap();
    assert_eq!(rows(&table, "id").await, ["1|a", "2|b"]);
}

#[tokio::test]
async fn delete_removes_the_rows_whose_keys_arrive() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2, 3]).await;
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[2, 9]))]);
    let out = run(keyed(&table, &src, Mode::Delete, "id")).await.unwrap();
    assert_eq!(rows(&table, "id").await, ["1|a", "3|c"]);
    assert_eq!(counts(&out), (0, 0, 1));
}

#[tokio::test]
async fn a_flagged_row_deletes_instead_of_upserting_and_the_flag_is_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2]).await;
    let src = parquet(
        dir.path(),
        "s.parquet",
        vec![
            ("id", ints(&[1, 2, 3, 9, 4])),
            ("name", strs(&[Some("A"), Some("B"), Some("c"), Some("x"), Some("d")])),
            ("op", strs(&[Some("delete"), None, Some("upsert"), Some("delete"), None])),
        ],
    );
    let out = run(Request { delete_column: Some("op".into()), ..keyed(&table, &src, Mode::Upsert, "id") })
        .await
        .unwrap();
    // 1 deleted; 2 updated (a NULL flag is not "delete"); 3 inserted; 9 is flagged
    // but matches nothing, so it is neither deleted nor inserted; 4 is new with a NULL
    // flag, and NULL is not "delete", so it is inserted.
    assert_eq!(rows(&table, "id").await, ["2|B", "3|c", "4|d"]);
    assert_eq!(counts(&out), (2, 1, 1));
}

#[tokio::test]
async fn the_delete_value_can_be_named() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2]).await;
    let src = parquet(
        dir.path(),
        "s.parquet",
        vec![("id", ints(&[1, 2])), ("name", strs(&[Some("a"), Some("B")])), ("op", strs(&[Some("D"), Some("delete")]))],
    );
    let r = Request {
        delete_column: Some("op".into()),
        delete_value: Some("D".into()),
        ..keyed(&table, &src, Mode::Upsert, "id")
    };
    run(r).await.unwrap();
    // "D" deletes; "delete" is now just a value, so 2 is updated.
    assert_eq!(rows(&table, "id").await, ["2|B"]);
}

#[tokio::test]
async fn the_default_delete_value_also_takes_the_tag_cdc_diff_gives() {
    // The default, "delete", is DuckLake CDC's tag; xf.cdc.diff tags a removed
    // row "deleted". On the default both delete.
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1, 2, 3]).await;
    let src = parquet(
        dir.path(),
        "s.parquet",
        vec![("id", ints(&[1, 2])), ("name", strs(&[Some("a"), Some("b")])), ("op", strs(&[Some("deleted"), Some("delete")]))],
    );
    run(Request { delete_column: Some("op".into()), ..keyed(&table, &src, Mode::Upsert, "id") })
        .await
        .unwrap();
    assert_eq!(rows(&table, "id").await, ["3|c"]);
}

#[tokio::test]
async fn an_upsert_creates_a_missing_table_without_its_flagged_rows() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("t").to_string_lossy().into_owned();
    let src = parquet(
        dir.path(),
        "s.parquet",
        vec![("id", ints(&[1, 2])), ("name", strs(&[Some("a"), Some("b")])), ("op", strs(&[None, Some("delete")]))],
    );
    let out = run(Request { delete_column: Some("op".into()), ..keyed(&table, &src, Mode::Upsert, "id") })
        .await
        .unwrap();
    assert!(out.created);
    assert_eq!(rows(&table, "id").await, ["1|a"]);
    assert_eq!(counts(&out), (1, 0, 0));
}

#[tokio::test]
async fn update_and_delete_refuse_a_missing_table() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("t").to_string_lossy().into_owned();
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[1])), ("name", strs(&[Some("a")]))]);
    for mode in [Mode::Update, Mode::Delete] {
        let e = run(keyed(&table, &src, mode, "id")).await.unwrap_err();
        assert!(e.contains("no Delta table"), "{mode:?}: {e}");
    }
    assert!(!dir.path().join("t").join("_delta_log").exists(), "nothing may be created");
}

#[tokio::test]
async fn create_if_missing_off_refuses_a_missing_table() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("t").to_string_lossy().into_owned();
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[1]))]);
    for mode in [Mode::Append, Mode::Upsert] {
        let r = Request { create_if_missing: false, ..keyed(&table, &src, mode, "id") };
        let e = run(r).await.unwrap_err();
        assert!(e.contains("no Delta table"), "{mode:?}: {e}");
    }
}

#[tokio::test]
async fn keys_are_required_and_must_be_columns_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1]).await;
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[1])), ("name", strs(&[Some("a")]))]);
    let e = run(req(&table, &src, Mode::Upsert)).await.unwrap_err();
    assert!(e.contains("key column"), "{e}");
    let e = run(keyed(&table, &src, Mode::Upsert, "nope")).await.unwrap_err();
    assert!(e.contains("nope"), "{e}");
}

#[tokio::test]
async fn a_column_only_one_side_has_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let table = table_with(dir.path(), &[1]).await;
    // A column the table lacks would have nowhere to go.
    let extra = parquet(
        dir.path(),
        "x.parquet",
        vec![("id", ints(&[1])), ("name", strs(&[Some("a")])), ("extra", strs(&[Some("z")]))],
    );
    let e = run(keyed(&table, &extra, Mode::Upsert, "id")).await.unwrap_err();
    assert!(e.contains("extra"), "{e}");
    // A column the input lacks would be inserted as NULL.
    let short = parquet(dir.path(), "y.parquet", vec![("id", ints(&[2]))]);
    let e = run(keyed(&table, &short, Mode::Upsert, "id")).await.unwrap_err();
    assert!(e.contains("name"), "{e}");
}

#[tokio::test]
async fn a_column_with_a_dot_in_its_name_is_matched_literally() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("t").to_string_lossy().into_owned();
    let seed = parquet(dir.path(), "seed.parquet", vec![("a.b", ints(&[1, 2])), ("v", strs(&[Some("x"), Some("y")]))]);
    run(req(&table, &seed, Mode::Append)).await.unwrap();
    let src = parquet(dir.path(), "s.parquet", vec![("a.b", ints(&[2])), ("v", strs(&[Some("Y")]))]);
    run(keyed(&table, &src, Mode::Upsert, "a.b")).await.unwrap();
    assert_eq!(rows(&table, "\"a.b\"").await, ["1|x", "2|Y"]);
}

#[tokio::test]
async fn partition_by_lays_out_a_table_it_creates() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("t").to_string_lossy().into_owned();
    let src = parquet(dir.path(), "s.parquet", vec![("id", ints(&[1, 2])), ("region", strs(&[Some("eu"), Some("us")]))]);
    run(Request { partition_by: vec!["region".into()], ..req(&table, &src, Mode::Append) }).await.unwrap();
    assert!(dir.path().join("t").join("region=eu").is_dir());
    assert_eq!(rows(&table, "id").await, ["1|eu", "2|us"]);
}
