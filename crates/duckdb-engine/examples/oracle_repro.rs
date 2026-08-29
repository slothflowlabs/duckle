// Repro harness for issue #4 (Oracle wide-table "never-ending query").
// Replicates run_oracle_source faithfully: connect, fetch, convert each
// cell exactly like oracle_cell_to_json, write NDJSON, then time DuckDB
// read_json_auto on the result (the suspected hang phase). Prints the
// first NDJSON line so we can see the real DATE string format.
//
//   PATH must include the Instant Client dir, then:
//   cargo run --release --example oracle_repro -p duckle-duckdb-engine
use oracle::sql_type::OracleType;
use serde_json::{Map, Value};

fn cell_to_json(row: &oracle::Row, i: usize) -> Value {
    let infos = row.column_info();
    let oty = infos
        .get(i)
        .map(|c| c.oracle_type().clone())
        .unwrap_or(OracleType::Varchar2(0));
    match oty {
        OracleType::Number(_, scale) if scale == 0 => {
            if let Ok(Some(n)) = row.get::<usize, Option<i64>>(i) {
                return Value::from(n);
            }
            if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                return Value::String(s);
            }
            Value::Null
        }
        OracleType::Number(_, _) | OracleType::Float(_) | OracleType::BinaryDouble | OracleType::BinaryFloat => {
            if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                if let Ok(n) = s.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(n) {
                        return Value::Number(num);
                    }
                }
                return Value::String(s);
            }
            Value::Null
        }
        OracleType::Date | OracleType::Timestamp(_) | OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            row.get::<usize, Option<String>>(i).ok().flatten().map(Value::String).unwrap_or(Value::Null)
        }
        _ => row.get::<usize, Option<String>>(i).ok().flatten().map(Value::String).unwrap_or(Value::Null),
    }
}

fn main() {
    let user = std::env::var("ORA_USER").unwrap_or_else(|_| "system".into());
    let pass = std::env::var("ORA_PASS").unwrap_or_else(|_| "duckle".into());
    let conn = std::env::var("ORA_CONN").unwrap_or_else(|_| "//localhost:1521/XEPDB1".into());
    let duckdb = std::env::var("DUCKLE_DUCKDB_BIN")
        .unwrap_or_else(|_| r".duckdb-cli-v1.5.4\duckdb.exe".into());

    let table = std::env::var("ORA_TABLE").unwrap_or_else(|_| "dates".into());
    let c = oracle::Connection::connect(&user, &pass, &conn).expect("connect");
    eprintln!("connected to {} (table={})", conn, table);

    for n in [10usize, 100, 1000, 10500] {
        let q = format!("SELECT * FROM {} WHERE rownum <= {}", table, n);
        let t0 = std::time::Instant::now();
        let mut stmt = c.statement(&q).prefetch_rows(1000).build().expect("prepare");
        let rs = stmt.query(&[]).expect("query");
        let cols: Vec<String> = rs.column_info().iter().map(|c| c.name().to_string()).collect();
        let path = format!("ora_repro_{}.ndjson", n);
        let mut buf = String::new();
        let mut rows = 0usize;
        for row_res in rs {
            let row = row_res.expect("row");
            let mut obj = Map::new();
            for (i, name) in cols.iter().enumerate() {
                obj.insert(name.clone(), cell_to_json(&row, i));
            }
            buf.push_str(&Value::Object(obj).to_string());
            buf.push('\n');
            rows += 1;
        }
        std::fs::write(&path, &buf).expect("write ndjson");
        let fetch_ms = t0.elapsed().as_millis();
        if n == 100 {
            // Show the real serialized shape (esp. the DATE format).
            eprintln!("first NDJSON line (N=100): {}", buf.lines().next().unwrap_or(""));
        }
        // Time DuckDB read_json_auto on the produced file.
        let t1 = std::time::Instant::now();
        let out = std::process::Command::new(&duckdb)
            .arg(":memory:")
            .arg("-c")
            .arg(format!(
                "CREATE TABLE r AS SELECT * FROM read_json_auto('{}', format='newline_delimited'); SELECT count(*) FROM r;",
                path.replace('\\', "/")
            ))
            .output()
            .expect("run duckdb");
        let read_ms = t1.elapsed().as_millis();
        let status = if out.status.success() { "ok" } else { "FAIL" };
        println!(
            "N={:>6}: fetch+serialize={}ms  read_json_auto={}ms [{}] ({} rows)",
            n, fetch_ms, read_ms, status, rows
        );
        if !out.status.success() {
            eprintln!("  duckdb stderr: {}", String::from_utf8_lossy(&out.stderr).trim());
        }
        let _ = std::fs::remove_file(&path);
    }
    println!("done.");
}
