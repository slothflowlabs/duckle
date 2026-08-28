//! Item-level checkpointing for work that costs money (#252 slice 2).
//!
//! A whole-stage cache is an optimisation. This is a durability guarantee about
//! work already performed, and the difference is the case that motivated it:
//!
//! ```text
//! 399,999 successful paid calls
//! request 400,000 fails permanently
//! stage has no completed cache
//! rerun repeats all 399,999 calls
//! ```
//!
//! So a completed item becomes durable **as it finishes**, not when the stage
//! does, and a rerun reuses it.
//!
//! # What is stored
//!
//! The OUTPUT, not merely the fact of success. A success marker without the
//! output leaves an awkward state: the item must not run again, and the row the
//! final relation needs cannot be reconstructed - so the stage is not actually
//! resumable. For paid extraction the output IS the thing that was bought.
//!
//! # What identifies an item
//!
//! An explicit logical key where one is configured, AND the input fingerprint,
//! AND the configuration fingerprint. The business key alone is not enough:
//!
//! ```text
//! company_id = 123, description = "old"   -> extracted
//! company_id = 123, description = "new"   -> must NOT reuse the old result
//! ```
//!
//! With no key configured the fallback is a canonical hash of the whole input
//! row. A volatile column - a run id, an ingestion timestamp - then costs cache
//! hits, which is recomputation rather than incorrect reuse. That is the safe
//! direction, and it is why no attempt is made to guess which columns are
//! volatile.
//!
//! # Ordering
//!
//! The output is finalised before the record that references it. Here the
//! output is stored inline, so one append is both - a record can never point at
//! an output that does not exist.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::Value as JsonValue;

use crate::EngineError;

/// One completed item.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    /// Identity: logical key + input fingerprint + config fingerprint.
    pub key: String,
    /// RFC3339, for retention.
    pub at: String,
    /// What the item produced. Inline, so the record and the output it refers
    /// to are the same append.
    pub output: JsonValue,
}

/// An append-only checkpoint for one node of one pipeline.
///
/// Namespaced by pipeline and node rather than by batch and row index: a
/// positional identity changes when an upstream result is reordered or
/// re-dispatched, and then a completed result maps onto a different item -
/// which skips work that was never done and reports success.
pub struct Store {
    path: PathBuf,
    done: BTreeMap<String, JsonValue>,
    /// Appends come from several worker threads at once, and a half-written
    /// line is a lost item that looks like an unfinished one.
    writer: Mutex<std::fs::File>,
}

impl Store {
    /// Open (and create) the store for a node, loading what is already done.
    pub fn open(workspace: &Path, pipeline: &str, node_id: &str) -> Result<Self, EngineError> {
        let dir = workspace
            .join("state")
            .join(sanitize(pipeline))
            .join("checkpoints");
        std::fs::create_dir_all(&dir).map_err(|e| {
            EngineError::Config(format!("checkpoint: create {}: {e}", dir.display()))
        })?;
        let path = dir.join(format!("{}.ndjson", sanitize(node_id)));
        let done = read_entries(&path)
            .into_iter()
            .map(|e| (e.key, e.output))
            .collect();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| EngineError::Config(format!("checkpoint: {}: {e}", path.display())))?;
        Ok(Store { path, done, writer: Mutex::new(file) })
    }

    pub fn completed(&self) -> usize {
        self.done.len()
    }

    /// What this item produced last time, if it has been done.
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.done.get(key)
    }

    /// Record a completed item. Appended immediately, so a crash on the next
    /// one keeps this one.
    pub fn record(&self, key: &str, output: &JsonValue) -> Result<(), EngineError> {
        let entry = Entry {
            key: key.to_string(),
            at: chrono::Utc::now().to_rfc3339(),
            output: output.clone(),
        };
        let line = serde_json::to_string(&entry)
            .map_err(|e| EngineError::Query(format!("checkpoint: encode: {e}")))?;
        let mut f = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // One write of one whole line, so concurrent workers interleave records
        // rather than fragments of them.
        f.write_all(format!("{line}\n").as_bytes())
            .map_err(|e| EngineError::Query(format!("checkpoint: {}: {e}", self.path.display())))?;
        Ok(())
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// Every readable entry. A torn line costs that item, not the file: it will be
/// recomputed, which is the safe direction.
pub fn read_entries(path: &Path) -> Vec<Entry> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<Entry>(l).ok())
        .collect()
}

/// A stable hash of a JSON value.
///
/// Object keys are visited in sorted order, so two rows that differ only in the
/// order their columns came back in produce the same fingerprint. Without that,
/// a query plan change would silently invalidate every checkpoint.
pub fn fingerprint(v: &JsonValue) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    feed(&mut h, v);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn feed(h: &mut sha2::Sha256, v: &JsonValue) {
    use sha2::Digest;
    match v {
        JsonValue::Null => h.update(b"n"),
        JsonValue::Bool(b) => {
            h.update(b"b");
            h.update([*b as u8]);
        }
        JsonValue::Number(n) => {
            h.update(b"#");
            h.update(n.to_string().as_bytes());
        }
        JsonValue::String(s) => {
            h.update(b"s");
            h.update((s.len() as u64).to_le_bytes());
            h.update(s.as_bytes());
        }
        JsonValue::Array(a) => {
            h.update(b"[");
            for x in a {
                feed(h, x);
            }
            h.update(b"]");
        }
        JsonValue::Object(o) => {
            h.update(b"{");
            // BTreeMap ordering: serde_json preserves insertion order with the
            // preserve_order feature, and column order is not identity.
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            for k in keys {
                h.update((k.len() as u64).to_le_bytes());
                h.update(k.as_bytes());
                feed(h, &o[k]);
            }
            h.update(b"}");
        }
    }
}

/// The identity of one item.
///
/// All three parts, deliberately. A logical key alone would reuse a result for
/// a row whose content changed; an input fingerprint alone would miss that the
/// prompt or the model changed underneath it.
pub fn item_key(row: &JsonValue, key_columns: &[String], config_fp: &str) -> String {
    let logical = if key_columns.is_empty() {
        // No key configured: the whole row IS the key. A volatile column costs
        // cache hits rather than causing wrong reuse.
        JsonValue::Null
    } else {
        JsonValue::Array(
            key_columns
                .iter()
                .map(|c| row.get(c).cloned().unwrap_or(JsonValue::Null))
                .collect(),
        )
    };
    fingerprint(&serde_json::json!({
        "logical": logical,
        "input": fingerprint(row),
        "config": config_fp,
    }))
}

/// What a stage's checkpoint holds and how big it is.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub pipeline: String,
    pub node: String,
    pub entries: usize,
    pub bytes: u64,
    pub oldest: Option<String>,
    pub newest: Option<String>,
}

/// Every checkpoint in a workspace.
pub fn statuses(workspace: &Path) -> Vec<Status> {
    let mut out = Vec::new();
    let Ok(pipelines) = std::fs::read_dir(workspace.join("state")) else {
        return out;
    };
    for p in pipelines.flatten() {
        let dir = p.path().join("checkpoints");
        let Ok(files) = std::fs::read_dir(&dir) else {
            continue;
        };
        for f in files.flatten() {
            let path = f.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
                continue;
            }
            let entries = read_entries(&path);
            let mut ats: Vec<String> = entries.iter().map(|e| e.at.clone()).collect();
            ats.sort();
            out.push(Status {
                pipeline: p.file_name().to_string_lossy().into_owned(),
                node: path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default(),
                entries: entries.len(),
                bytes: std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
                oldest: ats.first().cloned(),
                newest: ats.last().cloned(),
            });
        }
    }
    out.sort_by(|a, b| (&a.pipeline, &a.node).cmp(&(&b.pipeline, &b.node)));
    out
}

/// Drop entries older than `retain_days`, and then oldest-first until the file
/// is under `max_bytes`. Returns how many were removed.
///
/// A referenced output is never removed while its record still says it is
/// reusable, which here is automatic: the record and the output are one line,
/// so dropping the record drops the output with it. The item is recomputed next
/// run, which is the safe direction.
pub fn prune(
    workspace: &Path,
    retain_days: Option<u64>,
    max_bytes: Option<u64>,
) -> Result<usize, EngineError> {
    let mut removed = 0usize;
    let Ok(pipelines) = std::fs::read_dir(workspace.join("state")) else {
        return Ok(0);
    };
    for p in pipelines.flatten() {
        let dir = p.path().join("checkpoints");
        let Ok(files) = std::fs::read_dir(&dir) else {
            continue;
        };
        for f in files.flatten() {
            let path = f.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
                continue;
            }
            let mut entries = read_entries(&path);
            let before = entries.len();

            if let Some(days) = retain_days {
                let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);
                entries.retain(|e| {
                    chrono::DateTime::parse_from_rfc3339(&e.at)
                        .map(|t| t.with_timezone(&chrono::Utc) >= cutoff)
                        // An unreadable timestamp is kept: dropping a paid
                        // result because its clock string is odd is the wrong
                        // way to be strict.
                        .unwrap_or(true)
                });
            }
            if let Some(cap) = max_bytes {
                // Oldest first, because the newest results are the ones a rerun
                // is most likely to want.
                entries.sort_by(|a, b| a.at.cmp(&b.at));
                let mut size: u64 = entries
                    .iter()
                    .map(|e| serde_json::to_string(e).map(|s| s.len() as u64 + 1).unwrap_or(0))
                    .sum();
                let mut i = 0;
                while size > cap && i < entries.len() {
                    size -= serde_json::to_string(&entries[i])
                        .map(|s| s.len() as u64 + 1)
                        .unwrap_or(0);
                    i += 1;
                }
                entries.drain(..i);
            }

            if entries.len() == before {
                continue;
            }
            removed += before - entries.len();
            let mut text = String::new();
            for e in &entries {
                text.push_str(&serde_json::to_string(e).unwrap_or_default());
                text.push('\n');
            }
            // Written beside and renamed, so a crash mid-prune cannot leave a
            // half file that reads as a shorter checkpoint.
            let tmp = path.with_extension(format!("ndjson.{}.tmp", std::process::id()));
            std::fs::write(&tmp, text)
                .map_err(|e| EngineError::Config(format!("checkpoint prune: {e}")))?;
            let _ = std::fs::remove_file(&path);
            std::fs::rename(&tmp, &path).map_err(|e| {
                let _ = std::fs::remove_file(&tmp);
                EngineError::Config(format!("checkpoint prune: {e}"))
            })?;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_completed_item_is_reused_and_a_changed_one_is_not() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path(), "p", "n").unwrap();
        let cfg = "model-a";
        let row = json!({ "company_id": 123, "description": "old" });
        let key = item_key(&row, &["company_id".into()], cfg);
        assert!(store.get(&key).is_none(), "nothing done yet");
        store.record(&key, &json!({ "answer": "first" })).unwrap();

        // Reopened, the result is there - which is what makes a rerun cheap.
        let store = Store::open(tmp.path(), "p", "n").unwrap();
        assert_eq!(store.get(&key), Some(&json!({ "answer": "first" })));

        // Same business key, different content: the old answer describes the
        // old description and reusing it would be silently wrong.
        let changed = json!({ "company_id": 123, "description": "new" });
        let changed_key = item_key(&changed, &["company_id".into()], cfg);
        assert_ne!(changed_key, key, "content changed, so identity must change");
        assert!(store.get(&changed_key).is_none());
    }

    /// A prompt or model change invalidates everything, because the stored
    /// output was produced by the old one.
    #[test]
    fn changing_the_configuration_invalidates_the_checkpoint() {
        let row = json!({ "id": 1 });
        let a = item_key(&row, &[], "model=gpt-4,prompt=summarise");
        let b = item_key(&row, &[], "model=gpt-4,prompt=translate");
        assert_ne!(a, b, "a different prompt is a different job");
    }

    /// Column ORDER is not identity. Without this a query-plan change would
    /// silently invalidate every checkpoint and re-buy the whole dataset.
    #[test]
    fn the_fingerprint_does_not_depend_on_column_order() {
        let a: JsonValue = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let b: JsonValue = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        assert_eq!(fingerprint(&a), fingerprint(&b));
        // But a different VALUE is a different row.
        let c: JsonValue = serde_json::from_str(r#"{"a":1,"b":3}"#).unwrap();
        assert_ne!(fingerprint(&a), fingerprint(&c));
    }

    /// Types are part of identity: 1 and "1" are different inputs and may well
    /// produce different answers.
    #[test]
    fn the_fingerprint_separates_a_number_from_its_text() {
        assert_ne!(fingerprint(&json!({ "v": 1 })), fingerprint(&json!({ "v": "1" })));
        assert_ne!(fingerprint(&json!({ "v": null })), fingerprint(&json!({ "v": "" })));
        // And a value cannot be smuggled across a key boundary.
        assert_ne!(
            fingerprint(&json!({ "ab": "c" })),
            fingerprint(&json!({ "a": "bc" }))
        );
    }

    /// With no key configured the whole row is the key, so a volatile column
    /// costs a cache hit rather than causing wrong reuse.
    #[test]
    fn without_a_key_a_volatile_column_costs_reuse_rather_than_correctness() {
        let a = json!({ "id": 1, "run_id": "r1" });
        let b = json!({ "id": 1, "run_id": "r2" });
        assert_ne!(item_key(&a, &[], "c"), item_key(&b, &[], "c"));
        // Naming the logical key is what buys the reuse back.
        assert_ne!(
            item_key(&a, &["id".into()], "c"),
            item_key(&b, &["id".into()], "c"),
            "the input fingerprint is still part of identity, by design"
        );
    }

    #[test]
    fn prune_drops_the_oldest_first_and_keeps_the_rest_readable() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path(), "p", "n").unwrap();
        for i in 0..20 {
            store
                .record(&format!("k{i}"), &json!({ "n": i, "pad": "x".repeat(200) }))
                .unwrap();
        }
        let before = statuses(tmp.path());
        assert_eq!(before[0].entries, 20);

        let removed = prune(tmp.path(), None, Some(1500)).unwrap();
        assert!(removed > 0, "something should have been dropped");
        let after = statuses(tmp.path());
        assert!(after[0].bytes <= 1600, "under the cap: {}", after[0].bytes);
        // What survives is still loadable, which is the part that matters -
        // a prune that corrupts the file loses everything, not just the excess.
        let store = Store::open(tmp.path(), "p", "n").unwrap();
        assert_eq!(store.completed(), after[0].entries);
        assert!(store.get("k19").is_some(), "the newest is kept");
    }

    /// A torn last line costs that item, not the file.
    #[test]
    fn a_damaged_line_costs_one_item_not_the_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path(), "p", "n").unwrap();
        store.record("good", &json!({ "v": 1 })).unwrap();
        drop(store);
        let path = tmp.path().join("state").join("p").join("checkpoints").join("n.ndjson");
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str("{\"key\":\"torn\",\"at\"");
        std::fs::write(&path, text).unwrap();

        let store = Store::open(tmp.path(), "p", "n").unwrap();
        assert_eq!(store.completed(), 1);
        assert!(store.get("good").is_some(), "the intact record survives");
    }
}
