//! #309: what was different about these two runs.
//!
//! When a pipeline gets slower, or writes a different number of rows, the
//! operator's real question is which of several things changed: the code, the
//! parameters, the runtime, the inputs, or nothing at all and it simply ran
//! slower. Duckle records enough to answer that, spread across a run receipt
//! and a run-history record, and joining the two by hand is work nobody does at
//! 3am.
//!
//! ## Separated, not merged
//!
//! The differences are grouped by KIND - code, runtime, invocation, inputs,
//! execution, output - because that grouping IS the answer. "Seventeen things
//! differ" helps nobody; "the code is identical, the runtime is identical, one
//! input file has different rows" is the diagnosis.
//!
//! ## Explanations are rules, not guesses
//!
//! Each explanation is a stated rule over facts already in the two records, so
//! it can be read, disagreed with, and checked. A plausible sentence that
//! cannot be traced back to a fact is worse than no sentence, because it gets
//! believed.
//!
//! ## Nothing here reads data
//!
//! Row counts, hashes and durations only. A row-level comparison is a separate,
//! explicit operation - see the data-diff work - and must never be something a
//! `runs diff` does by accident.

use crate::history::RunRecord;
use crate::retry::RunReceipt;
use serde::Serialize;
use std::collections::BTreeSet;

pub const SCHEMA_VERSION: u32 = 1;

/// Which part of the world a difference is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Area {
    /// The pipeline document itself.
    Code,
    /// The engine, and what it was running on.
    Runtime,
    /// Who or what started it, and with which parameters.
    Invocation,
    /// What it read.
    Inputs,
    /// What happened while it ran.
    Execution,
    /// What it produced.
    Output,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Difference {
    pub area: Area,
    /// What differs, in the terms the reader already uses: `pipelineHash`,
    /// `node.load.rows`, `asset.lake/orders.parquet.rows`.
    pub field: String,
    pub a: String,
    pub b: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSide {
    pub run_id: String,
    pub at: String,
    pub status: String,
    pub trigger: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Diff {
    pub schema_version: u32,
    pub a: RunSide,
    pub b: RunSide,
    pub differences: Vec<Difference>,
    /// Deterministic readings of the differences above. Each one names the
    /// facts it rests on.
    pub explanations: Vec<String>,
    /// Areas this build could not compare, and why. A comparison that quietly
    /// omits what it could not see reads as "these are the same".
    pub not_compared: Vec<String>,
}

impl Diff {
    pub fn in_area(&self, area: Area) -> Vec<&Difference> {
        self.differences.iter().filter(|d| d.area == area).collect()
    }
}

fn push(out: &mut Vec<Difference>, area: Area, field: &str, a: String, b: String) {
    if a != b {
        out.push(Difference { area, field: field.to_string(), a, b });
    }
}

/// Absent is not zero, and saying "0" for it would report a collapse that never
/// happened.
fn show<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "-".into())
}

pub fn compare(
    a: &RunReceipt,
    b: &RunReceipt,
    a_record: Option<&RunRecord>,
    b_record: Option<&RunRecord>,
) -> Diff {
    let mut differences = Vec::new();
    let mut not_compared = Vec::new();

    // ---- code ----------------------------------------------------------
    push(
        &mut differences,
        Area::Code,
        "pipelineHash",
        a.pipeline_hash.clone(),
        b.pipeline_hash.clone(),
    );
    push(
        &mut differences,
        Area::Code,
        "pipelineName",
        a.pipeline_name.clone(),
        b.pipeline_name.clone(),
    );

    // ---- runtime -------------------------------------------------------
    push(
        &mut differences,
        Area::Runtime,
        "engineVersion",
        a.engine_version.clone(),
        b.engine_version.clone(),
    );

    // ---- invocation ----------------------------------------------------
    push(&mut differences, Area::Invocation, "trigger", a.trigger.clone(), b.trigger.clone());
    let names: BTreeSet<&String> = a.parameters.keys().chain(b.parameters.keys()).collect();
    for name in names {
        // Values are already redacted or digested at the boundary that recorded
        // them, so this can compare them without knowing which is which.
        push(
            &mut differences,
            Area::Invocation,
            &format!("parameter.{name}"),
            a.parameters.get(name).cloned().unwrap_or_else(|| "-".into()),
            b.parameters.get(name).cloned().unwrap_or_else(|| "-".into()),
        );
    }
    if a.parameters.is_empty() && b.parameters.is_empty() {
        not_compared.push(
            "parameters: neither run recorded any. Runs started before this existed, or \
             through a surface that does not take parameters, carry none."
                .into(),
        );
    }

    // ---- execution, per node -------------------------------------------
    let nodes: BTreeSet<&String> = a.nodes.keys().chain(b.nodes.keys()).collect();
    for node in nodes {
        let (x, y) = (a.nodes.get(node), b.nodes.get(node));
        push(
            &mut differences,
            Area::Execution,
            &format!("node.{node}.status"),
            x.map(|n| n.status.clone()).unwrap_or_else(|| "-".into()),
            y.map(|n| n.status.clone()).unwrap_or_else(|| "-".into()),
        );
        push(
            &mut differences,
            Area::Execution,
            &format!("node.{node}.rows"),
            show(x.and_then(|n| n.rows)),
            show(y.and_then(|n| n.rows)),
        );
        push(
            &mut differences,
            Area::Execution,
            &format!("node.{node}.durationMs"),
            show(x.and_then(|n| n.duration_ms)),
            show(y.and_then(|n| n.duration_ms)),
        );
        // A cache key IS the identity of what a node produced, so a changed one
        // means different output even when the row count matches.
        push(
            &mut differences,
            Area::Output,
            &format!("node.{node}.outputCacheKey"),
            show(x.and_then(|n| n.output_cache_key.clone())),
            show(y.and_then(|n| n.output_cache_key.clone())),
        );
    }
    push(&mut differences, Area::Execution, "status", a.status.clone(), b.status.clone());
    push(&mut differences, Area::Execution, "state", a.state.clone(), b.state.clone());

    // ---- what the history record adds ----------------------------------
    match (a_record, b_record) {
        (Some(x), Some(y)) => {
            push(
                &mut differences,
                Area::Execution,
                "durationMs",
                x.duration_ms.to_string(),
                y.duration_ms.to_string(),
            );
            push(&mut differences, Area::Output, "rows", x.rows.to_string(), y.rows.to_string());
            push(
                &mut differences,
                Area::Execution,
                "errorCategory",
                show(x.category.clone()),
                show(y.category.clone()),
            );
            push(
                &mut differences,
                Area::Output,
                "incomplete",
                x.incomplete.to_string(),
                y.incomplete.to_string(),
            );
            push(
                &mut differences,
                Area::Output,
                "unchanged",
                x.unchanged.to_string(),
                y.unchanged.to_string(),
            );
            let assets: BTreeSet<(&str, &str)> = x
                .assets
                .iter()
                .chain(y.assets.iter())
                .map(|t| (t.id.as_str(), t.direction.as_str()))
                .collect();
            for (id, direction) in assets {
                let find = |r: &RunRecord| {
                    r.assets
                        .iter()
                        .find(|t| t.id == id && t.direction == direction)
                        .map(|t| show(t.rows))
                };
                // Read assets are inputs; written ones are output. Filing them
                // together would defeat the point of separating the areas.
                let area = match direction {
                    "read" => Area::Inputs,
                    _ => Area::Output,
                };
                push(
                    &mut differences,
                    area,
                    &format!("asset.{id}.{direction}.rows"),
                    find(x).unwrap_or_else(|| "-".into()),
                    find(y).unwrap_or_else(|| "-".into()),
                );
            }
        }
        _ => not_compared.push(
            "duration, total rows and assets: at least one run has no history record. \
             The receipt alone does not carry them."
                .into(),
        ),
    }

    not_compared.push(
        "source content hashes and data-quality results: not recorded per run yet.".into(),
    );

    let side = |r: &RunReceipt, rec: Option<&RunRecord>| RunSide {
        run_id: r.run_id.clone(),
        at: r.at.clone(),
        status: r.status.clone(),
        trigger: r.trigger.clone(),
        duration_ms: rec.map(|x| x.duration_ms),
        rows: rec.map(|x| x.rows),
    };

    let explanations = explain(&differences);
    Diff {
        schema_version: SCHEMA_VERSION,
        a: side(a, a_record),
        b: side(b, b_record),
        differences,
        explanations,
        not_compared,
    }
}

/// Deterministic readings, each resting on facts in the list.
///
/// Rules rather than prose: every sentence here is reachable by inspecting the
/// differences, so a reader who disagrees can point at the rule. The ordering
/// matters - the most specific reading first - because the first line is the
/// one that gets read.
fn explain(differences: &[Difference]) -> Vec<String> {
    let has = |field: &str| differences.iter().any(|d| d.field == field);
    let any = |prefix: &str, suffix: &str| {
        differences.iter().any(|d| d.field.starts_with(prefix) && d.field.ends_with(suffix))
    };
    let in_area = |area: Area| differences.iter().any(|d| d.area == area);

    let mut out = Vec::new();
    let code = has("pipelineHash");
    let runtime = has("engineVersion");
    let params = differences.iter().any(|d| d.field.starts_with("parameter."));
    let inputs = in_area(Area::Inputs);

    if code {
        out.push("The pipeline document differs, so anything downstream of it may.".into());
    }
    if params {
        out.push(
            "The parameters differ. Where a value shows as `***` it was declared secret and \
             the comparison is on the declaration, not the value; a `#` prefix is a digest of \
             a value nobody declared."
                .into(),
        );
    }
    if runtime {
        out.push("The engine version differs, so behaviour changes are not necessarily yours."
            .into());
    }
    if inputs {
        out.push("An input asset returned a different number of rows.".into());
    }
    // The useful negative: same code, same runtime, same parameters, and the
    // output still moved. That points outward, and it is the reading an
    // operator most often needs and least often reaches on their own.
    if !code && !runtime && !params && in_area(Area::Output) {
        out.push(
            "Identical code, engine and parameters, but the output differs - which points at \
             the sources rather than at Duckle."
                .into(),
        );
    }
    if !code && !runtime && !params && !inputs && has("durationMs") && !in_area(Area::Output) {
        out.push(
            "Nothing about the run differs except how long it took: the same work, slower or \
             faster."
                .into(),
        );
    }
    // Found by running it: two runs whose node row counts moved 3 -> 7 produced
    // "no difference this build knows how to read", because every output rule
    // depended on the history record and neither run had one. A node's row
    // count is a difference in what was produced, and the receipt alone carries
    // it.
    if any("node.", ".rows") {
        out.push(
            "A node produced a different number of rows, which is visible from the receipts alone."
                .into(),
        );
    }
    if any("node.", ".outputCacheKey") {
        out.push(
            "A node's output-cache key differs, so it produced different content - a changed \
             key is a changed output even when the row count matches."
                .into(),
        );
    }
    if out.is_empty() {
        out.push("No difference this build knows how to read.".into());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::ReceiptNode;
    use std::collections::BTreeMap;

    fn receipt(id: &str) -> RunReceipt {
        RunReceipt {
            run_id: id.into(),
            trigger: "scheduled".into(),
            state: "finished".into(),
            pid: None,
            parent_run_id: None,
            at: "2026-09-01T00:00:00Z".into(),
            status: "ok".into(),
            pipeline_name: "nightly".into(),
            pipeline_path: "pipelines/nightly.json".into(),
            pipeline_hash: "aaa".into(),
            engine_version: "1.5.4".into(),
            parameters: BTreeMap::new(),
            nodes: BTreeMap::from([(
                "load".to_string(),
                ReceiptNode {
                    status: "ok".into(),
                    kind: Some("sink".into()),
                    output_cache_key: None,
                    rows: Some(100),
                    duration_ms: Some(1_000),
                },
            )]),
        }
    }

    fn record(rows: u64, ms: u64) -> RunRecord {
        RunRecord {
            run_id: None,
            at: "2026-09-01T00:00:00Z".into(),
            status: "ok".into(),
            duration_ms: ms,
            rows,
            node_count: 1,
            trigger: "scheduled".into(),
            error: None,
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            category: None,
            assets: vec![],
        }
    }

    #[test]
    fn identical_runs_differ_in_nothing() {
        let d = compare(&receipt("a"), &receipt("b"), None, None);
        assert!(d.differences.is_empty(), "{:?}", d.differences);
    }

    #[test]
    fn code_runtime_and_inputs_are_reported_separately() {
        let a = receipt("a");
        let mut b = receipt("b");
        b.pipeline_hash = "bbb".into();
        b.engine_version = "1.6.0".into();
        let d = compare(&a, &b, None, None);
        assert_eq!(d.in_area(Area::Code).len(), 1);
        assert_eq!(d.in_area(Area::Runtime).len(), 1);
        assert!(d.in_area(Area::Inputs).is_empty(), "nothing said about inputs");
        assert_eq!(d.in_area(Area::Code)[0].field, "pipelineHash");
    }

    #[test]
    fn per_node_rows_and_duration_are_comparable() {
        let a = receipt("a");
        let mut b = receipt("b");
        let n = b.nodes.get_mut("load").unwrap();
        n.rows = Some(250);
        n.duration_ms = Some(9_000);
        let d = compare(&a, &b, None, None);
        let fields: Vec<&str> = d.differences.iter().map(|x| x.field.as_str()).collect();
        assert!(fields.contains(&"node.load.rows"), "{fields:?}");
        assert!(fields.contains(&"node.load.durationMs"), "{fields:?}");
        let rows = d.differences.iter().find(|x| x.field == "node.load.rows").unwrap();
        assert_eq!((rows.a.as_str(), rows.b.as_str()), ("100", "250"));
    }

    #[test]
    fn a_node_missing_from_one_run_reads_as_absent_not_zero() {
        let a = receipt("a");
        let mut b = receipt("b");
        b.nodes.clear();
        let d = compare(&a, &b, None, None);
        let rows = d.differences.iter().find(|x| x.field == "node.load.rows").unwrap();
        assert_eq!(rows.b, "-", "absent must not read as 0: a failed run counts nothing");
    }

    #[test]
    fn a_secret_parameter_is_compared_without_being_revealed() {
        let mut a = receipt("a");
        let mut b = receipt("b");
        a.parameters = BTreeMap::from([("token".into(), "***".into())]);
        b.parameters = BTreeMap::from([("token".into(), "***".into())]);
        let d = compare(&a, &b, None, None);
        assert!(d.differences.is_empty(), "two redacted secrets compare equal");
        let rendered = format!("{d:?}");
        assert!(!rendered.contains("hunter2"));
    }

    #[test]
    fn a_changed_parameter_is_an_invocation_difference() {
        let mut a = receipt("a");
        let mut b = receipt("b");
        a.parameters = BTreeMap::from([("region".into(), "eu".into())]);
        b.parameters = BTreeMap::from([("region".into(), "us".into())]);
        let d = compare(&a, &b, None, None);
        assert_eq!(d.in_area(Area::Invocation).len(), 1);
        assert_eq!(d.in_area(Area::Invocation)[0].field, "parameter.region");
    }

    #[test]
    fn a_node_row_change_is_read_even_with_no_history_record() {
        // The case that produced "no difference this build knows how to read"
        // while staring at rows going 3 -> 7.
        let a = receipt("a");
        let mut b = receipt("b");
        b.nodes.get_mut("load").unwrap().rows = Some(700);
        let d = compare(&a, &b, None, None);
        assert!(
            d.explanations.iter().any(|e| e.contains("different number of rows")),
            "{:?}",
            d.explanations
        );
        assert!(!d.explanations.iter().any(|e| e.contains("No difference")));
    }

    #[test]
    fn the_same_everything_but_slower_says_exactly_that() {
        let a = receipt("a");
        let b = receipt("b");
        let d = compare(&a, &b, Some(&record(100, 1_000)), Some(&record(100, 9_000)));
        assert!(
            d.explanations.iter().any(|e| e.contains("the same work, slower or faster")),
            "{:?}",
            d.explanations
        );
    }

    #[test]
    fn same_code_and_different_output_points_outward() {
        let a = receipt("a");
        let b = receipt("b");
        let d = compare(&a, &b, Some(&record(100, 1_000)), Some(&record(250, 1_000)));
        assert!(
            d.explanations.iter().any(|e| e.contains("points at the sources")),
            "{:?}",
            d.explanations
        );
    }

    #[test]
    fn what_could_not_be_compared_is_stated() {
        let d = compare(&receipt("a"), &receipt("b"), None, None);
        assert!(
            d.not_compared.iter().any(|n| n.contains("history record")),
            "a comparison that omits what it could not see reads as 'the same': {:?}",
            d.not_compared
        );
        assert!(d.not_compared.iter().any(|n| n.contains("data-quality")));
    }

    #[test]
    fn a_read_asset_is_an_input_and_a_written_one_is_output() {
        use crate::history::AssetTouch;
        let mut x = record(100, 1_000);
        let mut y = record(100, 1_000);
        x.assets = vec![
            AssetTouch { id: "src.csv".into(), direction: "read".into(), rows: Some(10) },
            AssetTouch { id: "out.parquet".into(), direction: "write".into(), rows: Some(10) },
        ];
        y.assets = vec![
            AssetTouch { id: "src.csv".into(), direction: "read".into(), rows: Some(99) },
            AssetTouch { id: "out.parquet".into(), direction: "write".into(), rows: Some(99) },
        ];
        let d = compare(&receipt("a"), &receipt("b"), Some(&x), Some(&y));
        assert_eq!(d.in_area(Area::Inputs).len(), 1);
        assert_eq!(d.in_area(Area::Inputs)[0].field, "asset.src.csv.read.rows");
        assert!(d
            .in_area(Area::Output)
            .iter()
            .any(|f| f.field == "asset.out.parquet.write.rows"));
    }
}
