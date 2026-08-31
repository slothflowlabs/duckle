//! #302: will this producer change break something downstream?
//!
//! A pipeline can validate perfectly on its own and still break another one.
//! Duckle already knows each pipeline's declared schema and, through the
//! catalog, which pipelines read which assets. This puts the two together, so
//! the question "is this safe to deploy" has an answer that does not involve
//! opening every downstream pipeline by hand.
//!
//! ## Severity depends on the reader, not just the change
//!
//! Removing a column is only breaking if something reads it. That is the whole
//! point of doing this across pipelines rather than per pipeline: the same edit
//! is additive in one workspace and an outage in another, and only the
//! consumer graph can tell them apart.
//!
//! ## "References" is deliberately over-broad
//!
//! A consumer references a column if the name appears in its declared schema or
//! anywhere in its node properties - a filter predicate, a mapper expression, a
//! piece of SQL. That over-reports: a column called `id` will look referenced by
//! a pipeline that mentions `id` for another reason.
//!
//! Over-reporting is the safe direction for a deployment gate. A false
//! "breaking" costs a human thirty seconds; a false "compatible" costs a
//! production incident, and the whole reason this exists is that nobody has time
//! to check by hand. Where the answer is uncertain it is reported as
//! `PotentiallyBreaking` rather than dressed up as either.

use duckle_metadata::{Column, DataType};

/// What changed about one column between two versions of a producer's output.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "change")]
pub enum Change {
    Added { column: String, nullable: bool },
    Removed { column: String },
    TypeChanged { column: String, from: String, to: String, widening: bool },
    /// A column that was guaranteed present may now be null.
    NullabilityRelaxed { column: String },
}

impl Change {
    pub fn column(&self) -> &str {
        match self {
            Change::Added { column, .. }
            | Change::Removed { column }
            | Change::TypeChanged { column, .. }
            | Change::NullabilityRelaxed { column } => column,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
    Compatible,
    PotentiallyBreaking,
    Breaking,
}

/// Is `to` a superset of `from` - can every value of the old type be held by
/// the new one?
///
/// Only widenings that are true for every value are listed. Anything else is
/// left out on purpose: claiming a conversion is safe when it can round or
/// overflow is worse than saying nothing, because it is believed.
fn widens(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    if from == to {
        return true;
    }
    matches!(
        (from, to),
        (Int32, Int64)
            | (Int32, Float64)
            | (Int32, Decimal)
            | (Int64, Decimal)
            | (Float32, Float64)
            // Everything renders as text, so text is the universal widening -
            // it loses the type but never a value.
            | (Int32, String)
            | (Int64, String)
            | (Float32, String)
            | (Float64, String)
            | (Bool, String)
            | (Date, String)
            | (Time, String)
            | (Timestamp, String)
            | (Decimal, String)
            | (Date, Timestamp)
    )
}

fn type_name(t: &DataType) -> String {
    serde_json::to_value(t)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{t:?}"))
}

/// Everything that changed between two versions of one asset's schema.
pub fn classify(before: &[Column], after: &[Column]) -> Vec<Change> {
    let mut out = Vec::new();
    for b in before {
        match after.iter().find(|a| a.name == b.name) {
            None => out.push(Change::Removed { column: b.name.clone() }),
            Some(a) => {
                if a.data_type != b.data_type {
                    out.push(Change::TypeChanged {
                        column: b.name.clone(),
                        from: type_name(&b.data_type),
                        to: type_name(&a.data_type),
                        widening: widens(&b.data_type, &a.data_type),
                    });
                }
                if a.nullable && !b.nullable {
                    out.push(Change::NullabilityRelaxed { column: b.name.clone() });
                }
            }
        }
    }
    for a in after {
        if !before.iter().any(|b| b.name == a.name) {
            out.push(Change::Added { column: a.name.clone(), nullable: a.nullable });
        }
    }
    out
}

/// How bad is this change, given whether anything downstream reads the column?
///
/// A rename arrives here as a Removed plus an Added and is judged as the
/// removal, which is correct: to a consumer binding the old name, a rename and a
/// deletion are the same event.
pub fn severity(change: &Change, referenced: bool) -> Severity {
    match change {
        // Nothing downstream can bind a column that did not exist, so adding one
        // cannot break a reader.
        Change::Added { .. } => Severity::Compatible,
        Change::Removed { .. } => {
            if referenced {
                Severity::Breaking
            } else {
                // Not breaking anything we can see. Not compatible either: the
                // reference search only knows about pipelines in this
                // workspace, and a dashboard or a notebook is not one.
                Severity::PotentiallyBreaking
            }
        }
        Change::TypeChanged { widening, .. } => match (*widening, referenced) {
            (true, _) => Severity::Compatible,
            (false, true) => Severity::Breaking,
            (false, false) => Severity::PotentiallyBreaking,
        },
        Change::NullabilityRelaxed { .. } => {
            if referenced {
                // A reader that never had to handle a null now does. It will
                // not fail to bind, so it is not certainly broken - it is
                // exactly the change that produces wrong answers quietly.
                Severity::PotentiallyBreaking
            } else {
                Severity::Compatible
            }
        }
    }
}

/// Does this pipeline document mention `column` anywhere a reader would?
///
/// Declared schemas and every string in every node's properties. Over-broad on
/// purpose - see the module docs.
pub fn references(doc: &crate::PipelineDoc, column: &str) -> bool {
    fn in_value(v: &serde_json::Value, needle: &str) -> bool {
        match v {
            serde_json::Value::String(s) => contains_word(s, needle),
            serde_json::Value::Array(a) => a.iter().any(|x| in_value(x, needle)),
            serde_json::Value::Object(m) => m.values().any(|x| in_value(x, needle)),
            _ => false,
        }
    }
    for node in &doc.nodes {
        if let Some(schema) = node.data.schema.as_ref() {
            if schema.iter().any(|c| c.name == column) {
                return true;
            }
        }
        if let Some(props) = node.data.properties.as_ref() {
            if in_value(props, column) {
                return true;
            }
        }
    }
    false
}

/// Whole-word match, so `id` does not match `paid` and a column called `name`
/// is not found in every description in the file.
fn contains_word(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let bytes = haystack.as_bytes();
    let n = needle.as_bytes();
    let boundary = |c: u8| !(c.is_ascii_alphanumeric() || c == b'_');
    haystack.match_indices(needle).any(|(i, _)| {
        let before_ok = i == 0 || boundary(bytes[i - 1]);
        let after = i + n.len();
        let after_ok = after >= bytes.len() || boundary(bytes[after]);
        before_ok && after_ok
    })
}

/// One reported finding: a change, who it affects, and how badly.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    pub asset: String,
    #[serde(flatten)]
    pub change: Change,
    pub severity: Severity,
    /// Downstream pipelines that mention the affected column.
    pub affected: Vec<String>,
}

/// Judge one asset's change against the pipelines that read it.
///
/// `consumers` maps a downstream pipeline id to its document, which the caller
/// supplies because loading them is I/O and this stays testable without a
/// workspace.
pub fn check_asset(
    asset: &str,
    before: &[Column],
    after: &[Column],
    consumers: &[(String, crate::PipelineDoc)],
) -> Vec<Finding> {
    classify(before, after)
        .into_iter()
        .map(|change| {
            let affected: Vec<String> = consumers
                .iter()
                .filter(|(_, doc)| references(doc, change.column()))
                .map(|(id, _)| id.clone())
                .collect();
            let severity = severity(&change, !affected.is_empty());
            Finding { asset: asset.to_string(), change, severity, affected }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, t: DataType, nullable: bool) -> Column {
        Column {
            name: name.into(),
            data_type: t,
            nullable,
            primary_key: None,
            format: None,
            tags: Vec::new(),
        }
    }

    fn consumer(id: &str, props: serde_json::Value) -> (String, crate::PipelineDoc) {
        let doc = serde_json::from_value(serde_json::json!({
            "nodes": [{
                "id": "n", "position": { "x": 0, "y": 0 },
                "data": { "label": "n", "componentId": "xf.filter", "properties": props }
            }],
            "edges": []
        }))
        .unwrap();
        (id.to_string(), doc)
    }

    #[test]
    fn adding_a_column_cannot_break_a_reader() {
        let before = vec![col("id", DataType::Int64, false)];
        let after = vec![col("id", DataType::Int64, false), col("email", DataType::String, true)];
        let f = check_asset("/lake/x", &before, &after, &[]);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].severity, Severity::Compatible, "nothing can bind a column that did not exist");
    }

    /// The case the issue is about: it validates alone and breaks someone else.
    #[test]
    fn removing_a_column_something_reads_is_breaking_and_names_who() {
        let before = vec![col("id", DataType::Int64, false), col("amt", DataType::Int64, true)];
        let after = vec![col("id", DataType::Int64, false)];
        let consumers = vec![
            consumer("reporting", serde_json::json!({ "predicate": "amt > 0" })),
            consumer("unrelated", serde_json::json!({ "predicate": "id > 0" })),
        ];
        let f = check_asset("/lake/x", &before, &after, &consumers);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].severity, Severity::Breaking);
        assert_eq!(f[0].affected, vec!["reporting".to_string()], "and says who breaks");
    }

    /// Removing a column nothing in the workspace reads is not proof it is safe:
    /// a dashboard is not a pipeline.
    #[test]
    fn removing_an_unreferenced_column_is_potentially_breaking_not_compatible() {
        let before = vec![col("id", DataType::Int64, false), col("legacy", DataType::String, true)];
        let after = vec![col("id", DataType::Int64, false)];
        let f = check_asset("/lake/x", &before, &after, &[]);
        assert_eq!(f[0].severity, Severity::PotentiallyBreaking);
    }

    #[test]
    fn a_widening_type_change_is_compatible_and_a_narrowing_one_is_not() {
        let widen = check_asset(
            "/lake/x",
            &[col("n", DataType::Int32, true)],
            &[col("n", DataType::Int64, true)],
            &[consumer("r", serde_json::json!({ "predicate": "n > 0" }))],
        );
        assert_eq!(widen[0].severity, Severity::Compatible, "every int32 fits in an int64");

        let narrow = check_asset(
            "/lake/x",
            &[col("n", DataType::Int64, true)],
            &[col("n", DataType::Int32, true)],
            &[consumer("r", serde_json::json!({ "predicate": "n > 0" }))],
        );
        assert_eq!(narrow[0].severity, Severity::Breaking, "int64 does not fit in an int32");
    }

    /// The quiet one: nothing fails to bind, the answers just go wrong.
    #[test]
    fn a_column_that_may_now_be_null_is_potentially_breaking() {
        let f = check_asset(
            "/lake/x",
            &[col("amt", DataType::Int64, false)],
            &[col("amt", DataType::Int64, true)],
            &[consumer("r", serde_json::json!({ "predicate": "amt > 0" }))],
        );
        assert_eq!(f[0].change, Change::NullabilityRelaxed { column: "amt".into() });
        assert_eq!(f[0].severity, Severity::PotentiallyBreaking);
    }

    /// A rename is a removal plus an addition, and to a consumer binding the old
    /// name a rename and a deletion are the same event.
    #[test]
    fn a_rename_reads_as_a_break_for_whoever_used_the_old_name() {
        let f = check_asset(
            "/lake/x",
            &[col("amount", DataType::Int64, true)],
            &[col("amt", DataType::Int64, true)],
            &[consumer("r", serde_json::json!({ "predicate": "amount > 0" }))],
        );
        let removed = f.iter().find(|x| matches!(x.change, Change::Removed { .. })).unwrap();
        assert_eq!(removed.severity, Severity::Breaking);
        let added = f.iter().find(|x| matches!(x.change, Change::Added { .. })).unwrap();
        assert_eq!(added.severity, Severity::Compatible);
    }

    /// Whole-word matching, or a column called `id` is referenced by every
    /// pipeline in the workspace and the report becomes noise nobody reads.
    #[test]
    fn a_reference_is_a_whole_word_not_a_substring() {
        let doc = consumer("r", serde_json::json!({ "predicate": "paid > 0 AND validated" })).1;
        assert!(!references(&doc, "id"), "`id` must not match `paid` or `validated`");
        assert!(references(&doc, "paid"));

        let quoted = consumer("r", serde_json::json!({ "sql": r#"SELECT "amt" FROM t"# })).1;
        assert!(references(&quoted, "amt"), "a quoted identifier is still a reference");
    }

    /// A declared schema counts as a reference even when no property mentions it.
    #[test]
    fn a_declared_schema_counts_as_a_reference() {
        let doc: crate::PipelineDoc = serde_json::from_value(serde_json::json!({
            "nodes": [{
                "id": "n", "position": { "x": 0, "y": 0 },
                "data": {
                    "label": "n", "componentId": "src.csv", "properties": {},
                    "schema": [{ "name": "amt", "type": "int64" }]
                }
            }],
            "edges": []
        }))
        .unwrap();
        assert!(references(&doc, "amt"));
        assert!(!references(&doc, "other"));
    }
}
