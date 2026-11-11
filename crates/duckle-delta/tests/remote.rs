//! The sidecar against real object storage: S3 through MinIO and Azure Blob through
//! Azurite, both started by .github/workflows/delta-sidecar.yml. Each test skips
//! unless its store is named in the environment, so a plain `cargo test` stays local.

use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::parquet::arrow::ArrowWriter;
use duckle_delta::{run, Mode, Request};

fn parquet(dir: &std::path::Path, name: &str, ids: &[i64], names: &[&str]) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(StringArray::from(names.to_vec())),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let path = dir.join(name);
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    path.to_string_lossy().into_owned()
}

async fn rows(table: &str, options: &HashMap<String, String>) -> Vec<String> {
    let url = deltalake::ensure_table_uri(table).unwrap();
    let t = deltalake::open_table_with_storage_options(url, options.clone()).await.unwrap();
    let ctx = SessionContext::new();
    t.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("t", t.table_provider().await.unwrap()).unwrap();
    let batches = ctx.sql("SELECT * FROM t ORDER BY id").await.unwrap().collect().await.unwrap();
    let mut out = vec![];
    for b in &batches {
        let cols: Vec<ArrayRef> = b.columns().iter().map(|c| cast(c, &DataType::Utf8).unwrap()).collect();
        for r in 0..b.num_rows() {
            let f: Vec<String> = cols
                .iter()
                .map(|c| c.as_any().downcast_ref::<StringArray>().unwrap())
                .map(|s| if s.is_null(r) { "NULL".into() } else { s.value(r).to_string() })
                .collect();
            out.push(f.join("|"));
        }
    }
    out
}

/// Create a table by appending, upsert into it, and read it back, all remotely.
async fn round_trip(table: String, options: HashMap<String, String>) {
    let dir = tempfile::tempdir().unwrap();
    let seed = parquet(dir.path(), "seed.parquet", &[1, 2], &["a", "b"]);
    let mut req = Request {
        table: table.clone(),
        source: seed,
        mode: Mode::Append,
        keys: vec![],
        delete_column: None,
        delete_value: None,
        create_if_missing: true,
        partition_by: vec![],
        storage_options: options.clone(),
    };
    let made = run(req.clone()).await.unwrap();
    assert!(made.created, "{made:?}");
    req.source = parquet(dir.path(), "up.parquet", &[2, 3], &["B", "c"]);
    req.mode = Mode::Upsert;
    req.keys = vec!["id".into()];
    let up = run(req).await.unwrap();
    assert_eq!((up.inserted, up.updated, up.deleted), (1, 1, 0));
    assert_eq!(rows(&table, &options).await, ["1|a", "2|B", "3|c"]);
}

fn unique() -> String {
    format!("t{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

#[tokio::test]
async fn an_upsert_round_trips_on_s3() {
    let Ok(endpoint) = std::env::var("DUCKLE_DELTA_S3_ENDPOINT") else {
        eprintln!("skipping: set DUCKLE_DELTA_S3_ENDPOINT (and a 'delta' bucket) to run");
        return;
    };
    let options = HashMap::from([
        ("aws_endpoint_url".to_string(), endpoint),
        ("aws_access_key_id".to_string(), std::env::var("DUCKLE_DELTA_S3_KEY").unwrap_or("admin".into())),
        ("aws_secret_access_key".to_string(), std::env::var("DUCKLE_DELTA_S3_SECRET").unwrap_or("password".into())),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("aws_allow_http".to_string(), "true".to_string()),
        // One writer here; concurrent writers on S3 need a locking provider.
        ("aws_s3_allow_unsafe_rename".to_string(), "true".to_string()),
    ]);
    round_trip(format!("s3://delta/{}", unique()), options).await;
}

#[tokio::test]
async fn an_upsert_round_trips_on_azure_blob() {
    if std::env::var("DUCKLE_DELTA_AZURITE").is_err() {
        eprintln!("skipping: set DUCKLE_DELTA_AZURITE (Azurite on 10000 with a 'delta' container) to run");
        return;
    }
    let options = HashMap::from([("azure_storage_use_emulator".to_string(), "true".to_string())]);
    round_trip(format!("az://delta/{}", unique()), options).await;
}
