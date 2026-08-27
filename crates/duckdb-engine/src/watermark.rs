//! Backfill support: inspect, set, and clear the persisted state that a node
//! advances only on a fully successful run.
//!
//! State lives at `<workspace>/state/<pipeline>/<node_id>.json`, and FIVE
//! different node kinds write there, each with its own shape:
//!
//! | kind          | written by             | shape                                    |
//! |---------------|------------------------|------------------------------------------|
//! | `incremental` | `xf.incremental`       | `{ value, type }`                        |
//! | `snapshot`    | `src.ducklake.changes` | `{ snapshot_id }`                        |
//! | `kafka`       | `src.kafka`            | `{ topic, partition, next_offset }`      |
//! | `spool`       | `src.spool`            | `{ path, next_offset }`                  |
//! | `tumble`      | `xf.tumble`            | `{ buffer, watermark, emitted_through }` |
//!
//! Editing this lets an operator replay from an earlier point ("backfill from
//! date X", "re-read from snapshot N") or clear it to force a full reload,
//! without touching the pipeline. The path rule mirrors the executor's own
//! resolution in connectors.rs.
//!
//! Two rules exist because they all share one directory. `list` reports every
//! shape, so state that exists cannot be invisible to the operator managing
//! it. And a write REFUSES when the file it would replace is a different kind:
//! writing `{value,type}` over a tumbling window's `{buffer,...}` would drop
//! the pointer to the rows it is holding, destroying them, and nothing would
//! report it.

use serde::Serialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// One node's saved watermark/snapshot, for display in the backfill UI.
#[derive(Debug, Clone, Serialize)]
pub struct WatermarkEntry {
    pub node_id: String,
    /// One of `incremental`, `snapshot`, `kafka`, `spool`, `tumble`.
    pub kind: String,
    /// The watermark value, snapshot id, or resume position, as a string.
    pub value: String,
    /// SQL type for incremental marks (e.g. TIMESTAMP, BIGINT); None otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_type: Option<String>,
    /// True when this kind can be edited through `set_*`. A Kafka resume point
    /// or a tumbling window's buffer pointer can be CLEARED but not hand-set:
    /// there is no single value that means the same thing to them.
    pub editable: bool,
}

/// Which node kind wrote a state file, decided by its shape.
///
/// Order matters: the more specific keys are tested first, because a future
/// shape that happens to carry a `value` would otherwise be read as an
/// incremental mark and become hand-editable by accident.
pub fn kind_of(v: &Value) -> &'static str {
    if v.get("buffer").is_some() && v.get("watermark").is_some() {
        "tumble"
    } else if v.get("next_offset").is_some() && v.get("topic").is_some() {
        "kafka"
    } else if v.get("next_offset").is_some() {
        "spool"
    } else if v.get("snapshot_id").is_some() {
        "snapshot"
    } else if v.get("value").is_some() {
        "incremental"
    } else {
        "unknown"
    }
}

/// Can this kind be given a value by hand?
fn kind_is_editable(kind: &str) -> bool {
    matches!(kind, "incremental" | "snapshot")
}

fn state_dir(workspace: &Path, pipeline: &str) -> PathBuf {
    workspace.join("state").join(sanitize_segment(pipeline))
}

/// Path to one node's state file under a workspace + pipeline name.
pub fn state_path(workspace: &Path, pipeline: &str, node_id: &str) -> PathBuf {
    state_dir(workspace, pipeline).join(format!("{}.json", sanitize_segment(node_id)))
}

/// List the saved watermarks/snapshots for a pipeline (empty if none).
/// node_id is recovered from the file stem, so it round-trips only when the
/// id had no characters the sanitizer rewrote - good enough for display and
/// for matching against the live graph's node ids.
pub fn list(workspace: &Path, pipeline: &str) -> Vec<WatermarkEntry> {
    let dir = state_dir(workspace, pipeline);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Some(node_id) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let kind = kind_of(&v);
        // A shape nobody recognises is still reported. Hiding it would leave an
        // operator unable to see - or clear - state that is affecting runs.
        let as_str = |x: Option<&Value>| -> String {
            match x {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                Some(other) => other.to_string(),
                None => String::new(),
            }
        };
        let value = match kind {
            "snapshot" => as_str(v.get("snapshot_id")),
            "incremental" => as_str(v.get("value")),
            "kafka" => format!(
                "{}[{}] @ {}",
                as_str(v.get("topic")),
                as_str(v.get("partition")),
                as_str(v.get("next_offset"))
            ),
            "spool" => format!("byte {}", as_str(v.get("next_offset"))),
            "tumble" => format!("watermark {}", as_str(v.get("watermark"))),
            _ => text.trim().chars().take(120).collect(),
        };
        out.push(WatermarkEntry {
            node_id: node_id.to_string(),
            kind: kind.to_string(),
            value,
            value_type: if kind == "incremental" {
                v.get("type").and_then(|x| x.as_str()).map(String::from)
            } else {
                None
            },
            editable: kind_is_editable(kind),
        });
    }
    out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    out
}

/// Set an incremental high-water mark. `value_type` defaults to VARCHAR.
pub fn set_incremental(
    workspace: &Path,
    pipeline: &str,
    node_id: &str,
    value: &str,
    value_type: Option<&str>,
) -> std::io::Result<()> {
    guard_kind(workspace, pipeline, node_id, "incremental")?;
    write_state(
        workspace,
        pipeline,
        node_id,
        &json!({ "value": value, "type": value_type.unwrap_or("VARCHAR") }),
    )
}

/// Refuse a write that would replace state of a DIFFERENT kind.
///
/// This is the guard that makes the operation safe to expose outside the
/// desktop UI. `{value,type}` written over a tumbling window's
/// `{buffer,watermark,emitted_through}` drops the pointer to the rows that
/// window is holding - they are deleted on the next prune and nothing reports
/// it. Same for a Kafka resume point: it would be replaced by a mark the
/// consumer cannot read, and the next run would start from the configured
/// position instead, silently skipping or replaying.
///
/// A node with no state yet takes any kind, which is what makes "seed a
/// watermark before the first run" work.
fn guard_kind(
    workspace: &Path,
    pipeline: &str,
    node_id: &str,
    writing: &str,
) -> std::io::Result<()> {
    let path = state_path(workspace, pipeline, node_id);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(());
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return Ok(());
    };
    let existing = kind_of(&v);
    if existing == writing {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        // No line continuations here: the source is CRLF, so a trailing
        // backslash does not swallow the indentation and the message reaches
        // the user with a run of spaces in the middle of it.
        format!(
            concat!(
                "{node} holds {existing} state, not {writing}. Setting a ",
                "{writing} value would destroy it - a {existing} node does not ",
                "resume from a hand-written mark. Clear it instead to start ",
                "that node over."
            ),
            node = node_id,
            existing = existing,
            writing = writing
        ),
    ))
}

/// Set a DuckLake CDC snapshot id.
pub fn set_snapshot(
    workspace: &Path,
    pipeline: &str,
    node_id: &str,
    snapshot_id: u64,
) -> std::io::Result<()> {
    guard_kind(workspace, pipeline, node_id, "snapshot")?;
    write_state(workspace, pipeline, node_id, &json!({ "snapshot_id": snapshot_id }))
}

/// Remove a node's state file so the next run starts from its initial value
/// (incremental) / earliest snapshot (CDC) - i.e. a full reload. A missing
/// file is treated as success.
pub fn clear(workspace: &Path, pipeline: &str, node_id: &str) -> std::io::Result<()> {
    let path = state_path(workspace, pipeline, node_id);
    // xf.tumble keeps the rows in its open windows in a sibling directory.
    // Removing only the pointer would leave those buffers orphaned on disk
    // forever, growing with every clear.
    let buffers = path.with_extension("tumble");
    if buffers.is_dir() {
        let _ = std::fs::remove_dir_all(&buffers);
    }
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn write_state(
    workspace: &Path,
    pipeline: &str,
    node_id: &str,
    value: &Value,
) -> std::io::Result<()> {
    let path = state_path(workspace, pipeline, node_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&path, text)
}

/// Filesystem-safe single path segment - keep alphanumerics, space, dash,
/// underscore, dot; replace anything else with '_'. Mirrors the executor's
/// sanitize_path_segment so paths line up with what a run actually writes.
fn sanitize_segment(name: &str) -> String {
    let cleaned: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, ' ' | '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let cleaned = cleaned.trim().trim_matches('.').trim();
    if cleaned.is_empty() {
        "pipeline".to_string()
    } else {
        cleaned.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_list_clear_roundtrip() {
        let ws = tempfile::tempdir().unwrap();
        set_incremental(ws.path(), "orders", "inc1", "2024-01-01", Some("TIMESTAMP")).unwrap();
        set_snapshot(ws.path(), "orders", "cdc1", 42).unwrap();

        let mut got = list(ws.path(), "orders");
        got.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].node_id, "cdc1");
        assert_eq!(got[0].kind, "snapshot");
        assert_eq!(got[0].value, "42");
        assert_eq!(got[1].node_id, "inc1");
        assert_eq!(got[1].kind, "incremental");
        assert_eq!(got[1].value, "2024-01-01");
        assert_eq!(got[1].value_type.as_deref(), Some("TIMESTAMP"));

        clear(ws.path(), "orders", "inc1").unwrap();
        assert_eq!(list(ws.path(), "orders").len(), 1);
        // Clearing a missing file is a no-op, not an error.
        clear(ws.path(), "orders", "inc1").unwrap();
    }

    #[test]
    fn matches_executor_path_layout() {
        let ws = tempfile::tempdir().unwrap();
        let p = state_path(ws.path(), "My Pipe", "node/1");
        // pipeline + node sanitized; under <ws>/state/.
        assert!(p.ends_with("state/My Pipe/node_1.json") || p.ends_with("state\\My Pipe\\node_1.json"));
    }
}

#[cfg(test)]
mod shape_tests {
    use super::*;

    fn ws() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }
    fn write(dir: &Path, pipeline: &str, node: &str, body: &str) {
        let p = state_path(dir, pipeline, node);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    /// Five node kinds write into one directory. State that exists and is
    /// affecting runs must never be invisible to the operator managing it.
    #[test]
    fn every_state_shape_is_listed() {
        let d = ws();
        write(d.path(), "p", "inc", r#"{"value":"2026-01-01","type":"TIMESTAMP"}"#);
        write(d.path(), "p", "cdc", r#"{"snapshot_id":42}"#);
        write(d.path(), "p", "kaf", r#"{"topic":"orders","partition":0,"next_offset":991}"#);
        write(d.path(), "p", "spl", r#"{"path":"/spool/a.ndjson","next_offset":4096}"#);
        write(
            d.path(),
            "p",
            "tum",
            r#"{"buffer":"buf-1.parquet","watermark":"2026-01-02 10:00:00","emitted_through":null}"#,
        );

        let got = list(d.path(), "p");
        let kinds: Vec<(&str, &str)> = got
            .iter()
            .map(|e| (e.node_id.as_str(), e.kind.as_str()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("cdc", "snapshot"),
                ("inc", "incremental"),
                ("kaf", "kafka"),
                ("spl", "spool"),
                ("tum", "tumble"),
            ],
            "a shape that is not listed is state the operator cannot see or clear"
        );
        // Only the hand-settable kinds say so.
        let editable: Vec<&str> = got.iter().filter(|e| e.editable).map(|e| e.node_id.as_str()).collect();
        assert_eq!(editable, vec!["cdc", "inc"]);
        // The unfamiliar kinds still show what they hold, or listing them is useless.
        let kaf = got.iter().find(|e| e.node_id == "kaf").unwrap();
        assert!(kaf.value.contains("orders") && kaf.value.contains("991"), "{}", kaf.value);
    }

    /// THE data-loss guard. `{value,type}` written over a tumbling window's
    /// state drops the pointer to the rows it is holding; they are pruned on
    /// the next run and nothing reports it.
    #[test]
    fn setting_a_value_on_another_kind_is_refused_not_written() {
        let d = ws();
        let body = r#"{"buffer":"buf-1.parquet","watermark":"2026-01-02 10:00:00"}"#;
        write(d.path(), "p", "tum", body);

        let err = set_incremental(d.path(), "p", "tum", "2026-01-01", Some("TIMESTAMP"))
            .expect_err("writing an incremental mark over tumble state must be refused");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let msg = err.to_string();
        assert!(msg.contains("tumble"), "the message should name what is there: {msg}");
        assert!(msg.contains("Clear it"), "and say what to do instead: {msg}");

        assert_eq!(
            std::fs::read_to_string(state_path(d.path(), "p", "tum")).unwrap(),
            body,
            "the refused write still modified the file"
        );
    }

    #[test]
    fn a_kafka_resume_point_cannot_be_hand_set_either() {
        let d = ws();
        write(d.path(), "p", "kaf", r#"{"topic":"orders","partition":0,"next_offset":991}"#);
        assert!(set_incremental(d.path(), "p", "kaf", "5", None).is_err());
        assert!(set_snapshot(d.path(), "p", "kaf", 5).is_err());
    }

    /// Same kind is a normal edit, and a node with no state yet takes any -
    /// which is what makes seeding a watermark before the first run work.
    #[test]
    fn setting_the_same_kind_and_seeding_a_new_node_both_work() {
        let d = ws();
        write(d.path(), "p", "inc", r#"{"value":"2026-01-01","type":"TIMESTAMP"}"#);
        set_incremental(d.path(), "p", "inc", "2025-06-01", Some("TIMESTAMP")).expect("same kind");
        assert!(std::fs::read_to_string(state_path(d.path(), "p", "inc"))
            .unwrap()
            .contains("2025-06-01"));
        set_incremental(d.path(), "p", "brand-new", "2026-01-01", None).expect("no state yet");
        set_snapshot(d.path(), "p", "cdc-new", 7).expect("no state yet");
    }

    /// Clearing a tumbling window must take its buffer directory with it, or
    /// the rows it was holding are orphaned on disk and grow with every clear.
    #[test]
    fn clearing_a_tumble_node_removes_the_rows_it_was_holding() {
        let d = ws();
        write(d.path(), "p", "tum", r#"{"buffer":"buf-1.parquet","watermark":"x"}"#);
        let buffers = state_path(d.path(), "p", "tum").with_extension("tumble");
        std::fs::create_dir_all(&buffers).unwrap();
        std::fs::write(buffers.join("buf-1.parquet"), b"rows").unwrap();

        clear(d.path(), "p", "tum").expect("clear");
        assert!(!state_path(d.path(), "p", "tum").exists());
        assert!(!buffers.exists(), "the buffered rows were left orphaned on disk");
    }
}
