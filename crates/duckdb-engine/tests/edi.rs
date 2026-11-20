//! src.edi against X12 and EDIFACT interchanges written here, as trading
//! partners send them: an X12 850 with two purchase orders, and an EDIFACT
//! ORDERS with its own delimiters, a release character and a Latin-1 name.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::path::Path;

const X12_850: &str = concat!(
    "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *261119*1000*U*00401*000000001*0*P*>~\r\n",
    "GS*PO*SENDER*RECEIVER*20261119*1000*7*X*004010~\r\n",
    "ST*850*0001~\r\n",
    "BEG*00*SA*PO-123**20261119~\r\n",
    "N1*ST*Acme Store~\r\n",
    "PO1*1*10*EA*9.99**VP*SKU-1>A~\r\n",
    "CTT*1~\r\n",
    "SE*6*0001~\r\n",
    "ST*850*0002~\r\n",
    "BEG*00*SA*PO-124**20261119~\r\n",
    "SE*3*0002~\r\n",
    "GE*2*7~\r\n",
    "IEA*1*000000001~\r\n",
);

/// UNOC is Latin-1: the ë in the name is the one byte 0xEB.
const EDIFACT_ORDERS: &[u8] = b"UNA:+.? 'UNB+UNOC:3+SENDER:ZZ+RECEIVER:ZZ+261119:1000+REF1'UNH+M1+ORDERS:D:96A:UN'BGM+220+PO?+123+9'DTM+137:20261119:102'NAD+BY+++Zo\xEB Store'UNT+5+M1'UNZ+1+REF1'";

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

fn read(engine: &DuckdbEngine, path: &str, out: &str) -> RunResult {
    let node = |id: &str, component: &str, props: Value| {
        json!({"id": id, "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": id, "componentId": component, "properties": props}})
    };
    let doc: PipelineDoc = serde_json::from_value(json!({
        "nodes": [node("e", "src.edi", json!({ "path": path })), node("k", "snk.parquet", json!({ "path": out }))],
        "edges": [{"id": "e1", "source": "e", "target": "k", "sourceHandle": "main", "targetHandle": "main", "data": {"connectionType": "main"}}],
    }))
    .unwrap();
    engine.execute_pipeline(&doc)
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

#[test]
fn edi_reads_an_x12_interchange_a_row_a_segment() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("po.x12");
    std::fs::write(&file, X12_850).unwrap();
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&engine, &norm(&file), &out);
    assert_eq!(r.status, "ok", "src.edi failed: {:?}", r.error);
    let types: Vec<String> = duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        types,
        ["file VARCHAR", "standard VARCHAR", "interchange_control VARCHAR", "sender VARCHAR", "receiver VARCHAR",
         "group_control VARCHAR", "message_control VARCHAR", "message_type VARCHAR", "position BIGINT", "segment VARCHAR",
         "elements VARCHAR[][]"]
    );
    assert_eq!(duckdb_json(&format!("SELECT count(*) AS n FROM read_parquet('{out}')")), [json!({"n": 13})]);
    // BEG03 of each purchase order, by the message it sits in.
    assert_eq!(
        duckdb_json(&format!(
            "SELECT message_control, message_type, group_control, interchange_control, sender, elements[3][1] AS po \
             FROM read_parquet('{out}') WHERE segment = 'BEG' ORDER BY position"
        )),
        [
            json!({"message_control": "0001", "message_type": "850", "group_control": "7", "interchange_control": "000000001", "sender": "SENDER", "po": "PO-123"}),
            json!({"message_control": "0002", "message_type": "850", "group_control": "7", "interchange_control": "000000001", "sender": "SENDER", "po": "PO-124"}),
        ]
    );
    // PO107 is a composite: its components, split on ISA16.
    assert_eq!(
        duckdb_json(&format!("SELECT elements[7] AS product FROM read_parquet('{out}') WHERE segment = 'PO1'")),
        [json!({"product": ["SKU-1", "A"]})]
    );
}

#[test]
fn edi_reads_an_edifact_interchange_with_its_own_delimiters() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("orders.edi");
    std::fs::write(&file, EDIFACT_ORDERS).unwrap();
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&engine, &norm(&file), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let rows = duckdb_json(&format!(
        "SELECT segment, standard, message_type, message_control, interchange_control, sender, elements \
         FROM read_parquet('{out}') ORDER BY position"
    ));
    let tags: Vec<&str> = rows.iter().map(|r| r["segment"].as_str().unwrap()).collect();
    assert_eq!(tags, ["UNB", "UNH", "BGM", "DTM", "NAD", "UNT", "UNZ"], "the UNA is not a segment");
    assert_eq!(
        rows[2],
        json!({"segment": "BGM", "standard": "EDIFACT", "message_type": "ORDERS", "message_control": "M1",
               "interchange_control": "REF1", "sender": "SENDER", "elements": [["220"], ["PO+123"], ["9"]]}),
        "a released + is text"
    );
    assert_eq!(rows[3]["elements"], json!([["137", "20261119", "102"]]), "a composite, by its components");
    assert_eq!(rows[4]["elements"][3], json!(["Zo\u{eb} Store"]), "Latin-1, as UNOC is");
}

#[test]
fn edi_reads_every_file_a_pattern_names_and_says_which_is_not_edi() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("a.edi"), X12_850).unwrap();
    std::fs::write(tmp.path().join("b.edi"), EDIFACT_ORDERS).unwrap();
    let out = norm(&tmp.path().join("out.parquet"));
    let r = read(&engine, &norm(&tmp.path().join("*.edi")), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let per_file = duckdb_json(&format!(
        "SELECT regexp_extract(file, '[ab]\\.edi$') AS file, standard, count(*) AS n FROM read_parquet('{out}') GROUP BY ALL ORDER BY 1"
    ));
    assert_eq!(per_file, [json!({"file": "a.edi", "standard": "X12", "n": 13}), json!({"file": "b.edi", "standard": "EDIFACT", "n": 7})]);
    std::fs::write(tmp.path().join("c.edi"), "hello, world").unwrap();
    let r = read(&engine, &norm(&tmp.path().join("*.edi")), &out);
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("c.edi") && err.contains("neither an X12"), "{err}");
}

#[test]
fn edi_says_when_an_interchange_is_cut_short() {
    let engine = engine_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("short.x12");
    std::fs::write(&file, X12_850.replace("IEA*1*000000001~\r\n", "")).unwrap();
    let r = read(&engine, &norm(&file), &norm(&tmp.path().join("out.parquet")));
    assert_eq!(r.status, "error", "a file that ends before its trailer was cut short in transfer");
    let err = format!("{:?}", r.error);
    assert!(err.contains("000000001") && err.contains("cut short"), "{err}");
}
