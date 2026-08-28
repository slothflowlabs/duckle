//! #252 slice 1: reuse a completed stage's OUTPUT when nothing that produced it
//! has changed.
//!
//! Distinct from the item checkpoint in [`crate::checkpoint`], and the two
//! should not be confused. The checkpoint remembers each ITEM as it is bought,
//! so a run that dies at item 400,000 does not repurchase the first 399,999.
//! This remembers the WHOLE relation a stage produced, so a stage whose inputs
//! and configuration are unchanged does not run at all.
//!
//! Deliberately conservative, because a cache that returns a stale answer is
//! worse than no cache:
//!
//! - **Opt-in.** Nothing is cached unless the node asks.
//! - **Keyed on everything that could change the answer** - the component, the
//!   node's configuration, and a checksum of the relation it reads.
//! - **Refused where the input cannot be established.** A stage with no
//!   upstream relation reads the outside world, and its configuration alone
//!   does not say what it read. Caching on config only would return last week's
//!   parse of a file that has since changed.
//! - **Visible.** A restored stage says so in its own message, so a run that
//!   did no work cannot look like a run that did.

use std::path::{Path, PathBuf};

use crate::EngineError;

/// What a cacheable stage needs to look itself up.
pub struct Key {
    pub dir: PathBuf,
    pub key: String,
}

impl Key {
    pub fn file(&self) -> PathBuf {
        self.dir.join(format!("{}.parquet", self.key))
    }
    /// Enough of the key to identify it in a message without being a wall.
    pub fn short(&self) -> &str {
        &self.key[..self.key.len().min(12)]
    }
}

/// Where a pipeline's cached outputs live.
pub fn dir(workspace: &Path, pipeline: &str, node_id: &str) -> PathBuf {
    workspace
        .join("cache")
        .join(crate::connectors::sanitize_path_segment(pipeline))
        .join(crate::connectors::sanitize_path_segment(node_id))
}

/// The identity of one completed output.
///
/// `config_fp` is fixed at plan time from the component and its properties;
/// `input_fp` is a checksum of the relation the stage reads, taken now.
pub fn key_for(
    workspace: &Path,
    pipeline: &str,
    node_id: &str,
    config_fp: &str,
    input_fp: &str,
) -> Key {
    let key = crate::checkpoint::fingerprint(&serde_json::json!({
        "config": config_fp,
        "input": input_fp,
    }));
    Key {
        dir: dir(workspace, pipeline, node_id),
        key,
    }
}

/// A checksum of everything in one relation.
///
/// One aggregate pass, no materialisation. That is not free, but it is the
/// price of knowing the input did not change - and this only runs for stages
/// whose work is expected to dwarf a scan, which is the whole reason for
/// caching them.
pub fn input_fingerprint(
    bin: &Path,
    db: &Path,
    view: &str,
    run_rows: impl Fn(&Path, &str) -> Result<Vec<serde_json::Value>, EngineError>,
) -> Result<String, EngineError> {
    let _ = bin;
    let sql = format!(
        "SELECT count(*) AS n, coalesce(sum(hash(t::VARCHAR)::HUGEINT), 0)::VARCHAR AS h FROM {} t",
        crate::plan::quote_ident(view)
    );
    let rows = run_rows(db, &sql)?;
    let row = rows.first().cloned().unwrap_or(serde_json::Value::Null);
    // Count AND checksum: a sum alone collides too easily on small integers,
    // and a count alone misses an edit that keeps the row count.
    let n = row.get("n").map(|v| v.to_string()).unwrap_or_default();
    let h = row.get("h").map(|v| v.to_string()).unwrap_or_default();
    Ok(format!("{n}:{h}"))
}

/// Is there a cached output for this key?
pub fn hit(key: &Key) -> bool {
    key.file().is_file()
}

/// Put a cached output back as the stage's relation.
pub fn restore(bin: &Path, db: &Path, node_id: &str, key: &Key) -> Result<(), EngineError> {
    let sql = format!(
        "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
        crate::plan::quote_ident(node_id),
        key.file()
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''")
    );
    crate::apply_duckdb_sql(bin, db, &sql)
}

/// Keep what this stage produced, so an unchanged rerun can skip it.
///
/// Best effort: a cache that could not be written is a slower next run, not a
/// failed this one.
pub fn store(bin: &Path, db: &Path, node_id: &str, key: &Key) {
    if std::fs::create_dir_all(&key.dir).is_err() {
        return;
    }
    let tmp = key.dir.join(format!("{}.writing.parquet", key.key));
    let sql = format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        crate::plan::quote_ident(node_id),
        tmp.to_string_lossy().replace('\\', "/").replace('\'', "''")
    );
    if crate::apply_duckdb_sql(bin, db, &sql).is_err() {
        let _ = std::fs::remove_file(&tmp);
        return;
    }
    // Renamed into place so a reader never sees a half-written cache file. On
    // Windows rename replaces the destination, so removing it first would only
    // open a window where it does not exist.
    if std::fs::rename(&tmp, key.file()).is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
}

/// One stage's cached outputs, for `duckle-runner cache list`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Entry {
    pub pipeline: String,
    pub node: String,
    pub files: usize,
    pub bytes: u64,
}

/// Every cached output in a workspace.
///
/// A missing cache directory is an empty list, not an error: a workspace that
/// has never cached anything is the normal case, not a broken one.
pub fn entries(workspace: &Path) -> Vec<Entry> {
    let mut out = Vec::new();
    let Ok(pipelines) = std::fs::read_dir(workspace.join("cache")) else {
        return out;
    };
    for p in pipelines.flatten() {
        let Ok(nodes) = std::fs::read_dir(p.path()) else {
            continue;
        };
        for n in nodes.flatten() {
            let mut files = 0usize;
            let mut bytes = 0u64;
            if let Ok(rd) = std::fs::read_dir(n.path()) {
                for f in rd.flatten() {
                    if f.path().extension().map(|x| x == "parquet").unwrap_or(false) {
                        files += 1;
                        bytes += f.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                }
            }
            if files > 0 {
                out.push(Entry {
                    pipeline: p.file_name().to_string_lossy().to_string(),
                    node: n.file_name().to_string_lossy().to_string(),
                    files,
                    bytes,
                });
            }
        }
    }
    out.sort_by(|a, b| (&a.pipeline, &a.node).cmp(&(&b.pipeline, &b.node)));
    out
}

/// Drop cached outputs, optionally narrowed to one pipeline.
///
/// Safe to call at any time: everything here can be recomputed, which is the
/// difference from the item checkpoint, where an entry is a result that was
/// already paid for and pruning it costs money.
pub fn clear(workspace: &Path, pipeline: Option<&str>) -> usize {
    let mut removed = 0usize;
    for e in entries(workspace) {
        if pipeline.map(|p| p != e.pipeline).unwrap_or(false) {
            continue;
        }
        let d = workspace
            .join("cache")
            .join(crate::connectors::sanitize_path_segment(&e.pipeline))
            .join(crate::connectors::sanitize_path_segment(&e.node));
        if std::fs::remove_dir_all(&d).is_ok() {
            removed += e.files;
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_changes_when_the_config_or_the_input_does() {
        let ws = std::path::Path::new("/ws");
        let a = key_for(ws, "p", "n", "cfg1", "in1");
        assert_eq!(a.key, key_for(ws, "p", "n", "cfg1", "in1").key, "stable");
        assert_ne!(
            a.key,
            key_for(ws, "p", "n", "cfg2", "in1").key,
            "config counts"
        );
        assert_ne!(
            a.key,
            key_for(ws, "p", "n", "cfg1", "in2").key,
            "input counts"
        );
    }

    /// Two nodes with the same configuration must not share a cache entry:
    /// they are different stages and a rename would swap their outputs.
    #[test]
    fn two_nodes_do_not_share_a_directory() {
        let ws = std::path::Path::new("/ws");
        let a = key_for(ws, "p", "n1", "cfg", "in");
        let b = key_for(ws, "p", "n2", "cfg", "in");
        assert_ne!(a.dir, b.dir);
    }

    #[test]
    fn the_short_form_is_readable_and_bounded() {
        let k = key_for(std::path::Path::new("/ws"), "p", "n", "c", "i");
        assert_eq!(k.short().len(), 12);
        assert!(k.file().to_string_lossy().ends_with(".parquet"));
    }
}
