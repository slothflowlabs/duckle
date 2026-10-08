//! `duckle-runner contracts check` against pipelines shaped the way the editor
//! saves them: a sink whose own `schema` is empty, because 72 of 81 sinks take
//! their columns from upstream and their Schema tab is read-only.

use std::path::Path;
use std::process::Command;

/// A producer that computes `net_revenue` and writes it to a Parquet file, and a
/// consumer that sums it. `with_revenue` false is the same producer with the
/// column taken out of its Map, as the editor saves it after the edit.
fn write_workspace(dir: &Path, with_revenue: bool) {
    let mut cols = vec![r#"{"name":"id","type":"int64"}"#, r#"{"name":"amount","type":"float64"}"#];
    let mut outputs = vec![
        r#"{"id":"o1","name":"id","type":"int64","expression":"main.id"}"#,
        r#"{"id":"o2","name":"amount","type":"float64","expression":"main.amount"}"#,
    ];
    if with_revenue {
        cols.push(r#"{"name":"net_revenue","type":"float64"}"#);
        outputs.push(r#"{"id":"o3","name":"net_revenue","type":"float64","expression":"main.amount - 1"}"#);
    }
    let cols = cols.join(",");
    let producer = format!(
        r#"{{"nodes":[
             {{"id":"src","position":{{"x":0,"y":0}},"data":{{"label":"src","componentId":"src.csv",
               "properties":{{"path":"${{workspace}}/in.csv"}},"schema":[{{"name":"id","type":"int64"}},{{"name":"amount","type":"float64"}}]}}}},
             {{"id":"map","position":{{"x":1,"y":0}},"data":{{"label":"map","componentId":"xf.map",
               "properties":{{"mode":"visual","mapper":{{"outputs":[{outputs}]}}}},"schema":[{cols}]}}}},
             {{"id":"out","position":{{"x":2,"y":0}},"data":{{"label":"out","componentId":"snk.parquet",
               "properties":{{"path":"${{workspace}}/clean.parquet"}},"schema":[]}}}}],
           "edges":[{{"id":"e1","source":"src","target":"map","sourceHandle":"main","targetHandle":"main","data":{{"connectionType":"main"}}}},
                    {{"id":"e2","source":"map","target":"out","sourceHandle":"main","targetHandle":"main","data":{{"connectionType":"main"}}}}]}}"#,
        outputs = outputs.join(","),
    );
    let consumer = r#"{"nodes":[
         {"id":"in","position":{"x":0,"y":0},"data":{"label":"in","componentId":"src.parquet",
           "properties":{"path":"${workspace}/clean.parquet"}}},
         {"id":"sum","position":{"x":1,"y":0},"data":{"label":"sum","componentId":"xf.groupby",
           "properties":{"groupKeys":["id"],"aggregations":[{"column":"net_revenue","function":"sum","alias":"revenue"}]}}},
         {"id":"rep","position":{"x":2,"y":0},"data":{"label":"rep","componentId":"snk.csv",
           "properties":{"path":"${workspace}/report.csv"}}}],
       "edges":[{"id":"e1","source":"in","target":"sum"},{"id":"e2","source":"sum","target":"rep"}]}"#;
    std::fs::create_dir_all(dir.join("pipelines")).unwrap();
    std::fs::write(dir.join("pipelines").join("producer.json"), producer).unwrap();
    std::fs::write(dir.join("pipelines").join("consumer.json"), consumer).unwrap();
}

fn git(dir: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git runs")
        .status
        .success();
    assert!(ok, "git {args:?} failed");
}

#[test]
fn removing_a_column_a_consumer_reads_is_breaking_when_the_sink_declares_no_schema() {
    // The sink's own schema is empty, so the check compared nothing on either
    // side and answered "no breaking changes" for a column another pipeline sums.
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    write_workspace(ws, true);
    git(ws, &["init", "-q", "-b", "main"]);
    git(ws, &["add", "-A"]);
    git(ws, &["commit", "-q", "-m", "base"]);
    write_workspace(ws, false);

    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["contracts", "check", "--base", "main", "--workspace", "."])
        .current_dir(ws)
        .output()
        .expect("the runner starts");
    let said = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert_eq!(out.status.code(), Some(1), "{said}");
    assert!(said.contains("BREAKING") && said.contains("removes net_revenue, read by consumer"), "{said}");
}

#[test]
fn an_asset_with_no_schema_to_compare_is_reported_as_not_checked() {
    // Nothing saved a schema for the producer's Map either, so there is nothing to
    // compare. The gate says so, rather than a bare "no breaking changes" that
    // reads exactly like a clean result.
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    write_workspace(ws, true);
    let p = ws.join("pipelines").join("producer.json");
    let unschemed = std::fs::read_to_string(&p).unwrap().replace(
        r#""schema":[{"name":"id","type":"int64"},{"name":"amount","type":"float64"},{"name":"net_revenue","type":"float64"}]"#,
        r#""schema":[]"#,
    );
    assert!(unschemed.contains(r#""componentId":"xf.map""#) && !unschemed.contains("net_revenue\",\"type\":\"float64\"}]"));
    std::fs::write(&p, &unschemed).unwrap();
    git(ws, &["init", "-q", "-b", "main"]);
    git(ws, &["add", "-A"]);
    git(ws, &["commit", "-q", "-m", "base"]);

    let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(["contracts", "check", "--base", "main", "--workspace", "."])
        .current_dir(ws)
        .output()
        .expect("the runner starts");
    let said = String::from_utf8_lossy(&out.stdout).to_string();
    assert_eq!(out.status.code(), Some(0), "{said}");
    // two: the producer's Parquet, and the consumer's report, whose Group By never saved one either
    assert!(said.contains("2 produced asset(s) not checked"), "{said}");
}
