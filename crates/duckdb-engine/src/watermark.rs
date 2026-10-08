//! Backfill support: inspect, set, and clear the persisted state that a node
//! advances only on a fully successful run.
//!
//! State lives at `<workspace>/state/<pipeline>/<node_id>.json`, and SIX
//! different node kinds write there, each with its own shape:
//!
//! | kind          | written by             | shape                                    |
//! |---------------|------------------------|------------------------------------------|
//! | `incremental` | `xf.incremental`       | `{ value, type }`                        |
//! | `snapshot`    | `src.ducklake.changes` | `{ snapshot_id }`                        |
//! | `kafka`       | `src.kafka`            | `{ topic, partition, next_offset }`      |
//! | `spool`       | `src.spool`            | `{ path, next_offset }`                  |
//! | `tumble`      | `xf.tumble`            | `{ buffer, watermark, emitted_through }` |
//! | `pg_lsn`      | `src.postgres.cdc`     | `{ slot, lsn }`                          |
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
    } else if v.get("lsn").is_some() && v.get("slot").is_some() {
        // Listed, never hand-editable: PostgreSQL will not move a slot back,
        // and moving one forward by hand skips changes nobody received.
        "pg_lsn"
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
            "pg_lsn" => format!("slot {} @ {}", as_str(v.get("slot")), as_str(v.get("lsn"))),
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
    guard_policy(workspace)?;
    guard_kind(workspace, pipeline, node_id, "incremental")?;
    write_state(
        workspace,
        pipeline,
        node_id,
        &json!({ "value": value, "type": value_type.unwrap_or("VARCHAR") }),
    )
}

/// Refuse a change the environment's policy does not permit.
///
/// The run-time check in `policy::check` stops a PIPELINE from advancing state.
/// It cannot see this path: `duckle-runner backfill --clear`, the HTTP API, MCP
/// and the desktop panel all arrive here directly. Since all four already share
/// these three functions, one guard here covers every one of them.
fn guard_policy(workspace: &Path) -> std::io::Result<()> {
    crate::policy::state_mutation_allowed(workspace)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::PermissionDenied, e.to_string()))
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
    guard_policy(workspace)?;
    guard_kind(workspace, pipeline, node_id, "snapshot")?;
    write_state(workspace, pipeline, node_id, &json!({ "snapshot_id": snapshot_id }))
}

/// The name an editor run and the Backfill panel keep a pipeline's state under:
/// its id, after moving what the editor saved under its display name.
///
/// The editor saves a pipeline as `pipelines/<id>.json`, and every other way of
/// running it - the CLI, the console, both schedulers, a sub-pipeline - names
/// the run after that file. Editor runs used the display name instead, so one
/// pipeline kept two positions: a watermark built up in the editor started over
/// when anything else ran the pipeline, a CDC feed re-delivered what the editor
/// had already applied, and renaming the pipeline orphaned its state.
///
/// The display name is used only when there is no id that can name a file, as
/// on a scratch canvas.
pub fn editor_state_name(
    workspace: Option<&Path>,
    pipeline_id: Option<&str>,
    display_name: Option<&str>,
) -> Option<String> {
    let display = display_name.map(str::trim).filter(|s| !s.is_empty());
    let Some(id) = pipeline_id.map(str::trim).filter(|id| names_a_file(id)) else {
        return display.map(str::to_string);
    };
    if let (Some(workspace), Some(display)) = (workspace, display) {
        let moved = move_display_name_state(workspace, display, id);
        if !moved.is_empty() {
            eprintln!(
                "duckle: moved {} saved state entr{} from state/{}/ to state/{}/, where every run of this pipeline keeps it",
                moved.len(),
                if moved.len() == 1 { "y" } else { "ies" },
                sanitize_segment(display),
                sanitize_segment(id)
            );
        }
    }
    Some(id.to_string())
}

/// The rule the servers apply to a name from the browser before it may name a file.
fn names_a_file(id: &str) -> bool {
    !id.is_empty() && id != "." && id != ".." && !id.contains(['/', '\\', ':', '\0'])
}

/// Move what the editor saved under `display` into the id's folder.
///
/// Moved, not copied, so a watermark cleared later is not brought back from the
/// old folder by the next run. When the id has no folder yet - the editor was
/// the only thing that ran the pipeline - the folder moves whole. Otherwise a
/// run from another surface wrote under the id, and that copy is the one every
/// surface but the editor has been reading, so only what it lacks is taken: a
/// node's file together with a tumbling window's rows beside it, and one node at
/// a time from `checkpoints/` and `baselines/`.
///
/// A display name can be another pipeline's file name, and every unnamed run
/// shares the folder called `pipeline`; either folder holds someone else's
/// state, so it stays where it is.
fn move_display_name_state(workspace: &Path, display: &str, id: &str) -> Vec<PathBuf> {
    let (from, to) = (state_dir(workspace, display), state_dir(workspace, id));
    let folder = sanitize_segment(display);
    if from == to || !from.is_dir() || folder == "pipeline" || has_pipeline_file(&workspace.join("pipelines"), &folder) {
        return Vec::new();
    }
    let mut moved = Vec::new();
    let mut take = |src: PathBuf, dst: PathBuf| {
        if dst.exists() {
            return;
        }
        if let Some(parent) = dst.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if std::fs::rename(&src, &dst).is_ok() {
            moved.push(dst);
        }
    };
    if !to.exists() {
        take(from, to);
        return moved;
    }
    let Ok(entries) = std::fs::read_dir(&from) else {
        return moved;
    };
    for entry in entries.flatten() {
        let (src, name) = (entry.path(), entry.file_name().to_string_lossy().into_owned());
        if src.is_dir() {
            if matches!(name.as_str(), "checkpoints" | "baselines") {
                for node in std::fs::read_dir(&src).into_iter().flatten().flatten() {
                    take(node.path(), to.join(&name).join(node.file_name()));
                }
            } else if !name.ends_with(".tumble") {
                take(src, to.join(&name));
            }
        } else if let Some(node) = name.strip_suffix(".json") {
            // A tumbling window's rows sit beside its pointer, and one without
            // the other is a window pointing at another window's rows.
            let rows = format!("{node}.tumble");
            if !to.join(&name).exists() && !to.join(&rows).exists() {
                take(src, to.join(&name));
                if from.join(&rows).is_dir() {
                    take(from.join(&rows), to.join(&rows));
                }
            }
        } else {
            take(src, to.join(&name));
        }
    }
    moved
}

/// Is there a pipeline file, anywhere under `dir`, whose state folder is `folder`?
fn has_pipeline_file(dir: &Path, folder: &str) -> bool {
    std::fs::read_dir(dir).into_iter().flatten().flatten().any(|e| {
        let p = e.path();
        if p.is_dir() {
            return has_pipeline_file(&p, folder);
        }
        p.extension().and_then(|x| x.to_str()) == Some("json")
            && p.file_stem().is_some_and(|s| sanitize_segment(&s.to_string_lossy()) == folder)
    })
}

/// Remove a node's state file so the next run starts from its initial value
/// (incremental) / earliest snapshot (CDC) - i.e. a full reload. A missing
/// file is treated as success.
pub fn clear(workspace: &Path, pipeline: &str, node_id: &str) -> std::io::Result<()> {
    guard_policy(workspace)?;
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
mod editor_name_tests {
    use super::*;

    const DISPLAY: &str = "3. Incremental load (watermark)";

    fn put(ws: &Path, rel: &str, body: &str) {
        let p = ws.join("state").join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }
    fn read(ws: &Path, rel: &str) -> Option<String> {
        std::fs::read_to_string(ws.join("state").join(rel)).ok()
    }

    /// The editor named a run after the pipeline's display name; the CLI, the
    /// console, both schedulers and sub-pipelines name it after its file.
    #[test]
    fn an_editor_run_is_named_after_the_pipeline_file() {
        let name = |id, display| editor_state_name(None, id, display);
        assert_eq!(name(Some("incremental_load"), Some(DISPLAY)).as_deref(), Some("incremental_load"));
        // A scratch canvas has no file to be named after.
        assert_eq!(name(None, Some(DISPLAY)).as_deref(), Some(DISPLAY));
        // An id that cannot name a file is not used as one.
        assert_eq!(name(Some("../outside"), Some(DISPLAY)).as_deref(), Some(DISPLAY));
        assert_eq!(name(Some("  "), None), None);
    }

    /// One pipeline kept two positions: a watermark the editor built up started
    /// over when anything else ran it, and a CDC feed re-delivered what the
    /// editor had applied.
    #[test]
    fn state_saved_under_the_display_name_moves_to_the_id_once() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let old = sanitize_segment(DISPLAY);
        put(ws, &format!("{old}/inc.json"), r#"{"value":"500","type":"BIGINT"}"#);
        put(ws, &format!("{old}/checkpoints/cls.ndjson"), "{}\n");
        put(ws, &format!("{old}/win.json"), r#"{"buffer":"b","watermark":"w","emitted_through":"e"}"#);
        put(ws, &format!("{old}/win.tumble/part-0.parquet"), "rows");

        assert_eq!(
            editor_state_name(Some(ws), Some("incremental_load"), Some(DISPLAY)).as_deref(),
            Some("incremental_load")
        );
        let inc = list(ws, "incremental_load").into_iter().find(|e| e.node_id == "inc");
        assert_eq!(inc.map(|e| e.value), Some("500".to_string()));
        assert!(read(ws, "incremental_load/checkpoints/cls.ndjson").is_some(), "paid-for answers stay paid for");
        assert!(read(ws, "incremental_load/win.tumble/part-0.parquet").is_some(), "a window's rows travel with it");
        assert!(read(ws, &format!("{old}/inc.json")).is_none(), "moved, not copied");

        // Cleared afterwards, it stays cleared: nothing comes back from the old folder.
        clear(ws, "incremental_load", "inc").unwrap();
        editor_state_name(Some(ws), Some("incremental_load"), Some(DISPLAY));
        assert!(read(ws, "incremental_load/inc.json").is_none());
    }

    /// A run from another surface already wrote under the id, and that copy is
    /// the one every surface but the editor has been reading.
    #[test]
    fn the_id_keeps_the_state_another_surface_advanced() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let old = sanitize_segment(DISPLAY);
        put(ws, &format!("{old}/inc.json"), r#"{"value":"450","type":"BIGINT"}"#);
        put(ws, &format!("{old}/cdc.json"), r#"{"slot":"duckle","lsn":"0/16B3748"}"#);
        put(ws, &format!("{old}/win.json"), r#"{"buffer":"editor","watermark":"w","emitted_through":"e"}"#);
        put(ws, &format!("{old}/win.tumble/part-1.parquet"), "editor rows");
        put(ws, &format!("{old}/hour.json"), r#"{"buffer":"editor","watermark":"w","emitted_through":"e"}"#);
        put(ws, "incremental_load/inc.json", r#"{"value":"500","type":"BIGINT"}"#);
        put(ws, "incremental_load/win.json", r#"{"buffer":"cli","watermark":"w","emitted_through":"e"}"#);
        put(ws, "incremental_load/hour.tumble/part-0.parquet", "cli rows");

        editor_state_name(Some(ws), Some("incremental_load"), Some(DISPLAY));
        assert!(read(ws, "incremental_load/inc.json").unwrap().contains("500"));
        assert!(read(ws, "incremental_load/cdc.json").is_some(), "a node only the editor ran is taken");
        assert!(read(ws, "incremental_load/win.tumble/part-1.parquet").is_none(), "rows of another window");
        assert!(read(ws, "incremental_load/hour.json").is_none(), "a pointer to rows it did not write");
        assert!(read(ws, &format!("{old}/inc.json")).is_some(), "the one not taken is left where it was");
    }

    /// A display name can be another pipeline's file name, and then that folder
    /// is the other pipeline's state.
    #[test]
    fn a_display_name_that_is_another_pipelines_file_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines").join("nightly")).unwrap();
        std::fs::write(ws.join("pipelines").join("nightly").join("daily.json"), "{}").unwrap();
        put(ws, "daily/inc.json", r#"{"value":"7","type":"BIGINT"}"#);
        put(ws, "pipeline/inc.json", r#"{"value":"8","type":"BIGINT"}"#);

        assert_eq!(editor_state_name(Some(ws), Some("orders"), Some("daily")).as_deref(), Some("orders"));
        assert!(read(ws, "daily/inc.json").is_some());
        // Nor is the folder every unnamed run shares.
        editor_state_name(Some(ws), Some("orders"), Some("pipeline"));
        assert!(read(ws, "pipeline/inc.json").is_some());
        assert!(read(ws, "orders/inc.json").is_none());
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

    /// Six node kinds write into one directory. State that exists and is
    /// affecting runs must never be invisible to the operator managing it.
    #[test]
    fn every_state_shape_is_listed() {
        let d = ws();
        write(d.path(), "p", "inc", r#"{"value":"2026-01-01","type":"TIMESTAMP"}"#);
        write(d.path(), "p", "cdc", r#"{"snapshot_id":42}"#);
        write(d.path(), "p", "kaf", r#"{"topic":"orders","partition":0,"next_offset":991}"#);
        write(d.path(), "p", "spl", r#"{"path":"/spool/a.ndjson","next_offset":4096}"#);
        write(d.path(), "p", "pgc", r#"{"slot":"duckle_orders","lsn":"0/15260B8"}"#);
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
                ("pgc", "pg_lsn"),
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
        let pgc = got.iter().find(|e| e.node_id == "pgc").unwrap();
        assert!(pgc.value.contains("duckle_orders") && pgc.value.contains("0/15260B8"), "{}", pgc.value);
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
