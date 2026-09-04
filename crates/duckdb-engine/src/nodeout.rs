//! #305: what a node durably produced, addressable without re-deriving it.
//!
//! A retry that wants to skip an expensive `download -> parse -> normalize`
//! chain needs to answer one question: is node N's output still here, and is it
//! the same bytes? The receipt could not answer it. It recorded that a node
//! succeeded and, for the six cache-eligible components, the KEY its output was
//! filed under - and a key is a hash of the INPUTS, so it says the work would
//! produce the same thing, not that the thing is still on disk.
//!
//! ## Reuse a verified output, never a historical status
//!
//! Louis's framing on the issue, and it is the whole contract. "Node D
//! succeeded" is a fact about the past. "Node D's output is at this path, is
//! this many bytes, and hashes to this" is a fact about the present, and only
//! the second one may be bound into a new run. A cache pruned since is the
//! ordinary way the two differ, and it is silent.
//!
//! ## Generic on purpose
//!
//! There are already three near-identical shapes for "a file something
//! produced" - [`crate::backfill::SliceArtifact`], [`crate::plugin::Artifact`]
//! and `ArtifactRef` - each with its own hasher and its own record, and none of
//! them says which NODE produced it in a way a planner can join on. This is the
//! shape they can converge onto rather than a fourth private one: the issue
//! asks for a durable node-output reference, not a retry checkpoint format.
//!
//! Nothing is converted here yet. Introducing the type and moving one producer
//! onto it is a change that can be reviewed; rewriting all three at once is not.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// What kind of thing the output is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// Rows - a stage's own relation, stored as parquet. Bindable back into a
    /// later run as the same relation name.
    Relation,
    /// A file a component produced: a document, a model, a report. Recorded so
    /// provenance can name it; NOT bindable, because nothing downstream reads
    /// it as a relation.
    Artifact,
}

/// One node's durable output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeOutput {
    pub node_id: String,
    /// The run that produced it, so a reused output is traceable to the work
    /// that did it rather than to the run that skipped it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub kind: Kind,
    /// Where it is. Forward slashes, so the same file recorded on Windows and
    /// read on Linux is one uri rather than two.
    pub uri: String,
    pub sha256: String,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// The columns, where the output is tabular.
    ///
    /// Empty means NOT RECORDED, not "no columns" - the two are different and
    /// this cannot tell them apart, so [`verify`] does not treat an empty list
    /// as evidence of anything.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// The output-cache key it was filed under, when it came from that cache.
    /// Carried for provenance; it is not what decides reuse, because a key is a
    /// hash of the inputs and says nothing about the file still being there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
}

/// Why a recorded output cannot be reused.
///
/// Separated because the operator response differs: a file that is gone needs
/// the producer re-run, and a file whose bytes changed needs someone to find
/// out who wrote to the cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Missing {
    Gone,
    WrongSize { found: u64 },
    WrongHash,
    Unreadable(String),
}

impl Missing {
    pub fn describe(&self) -> String {
        match self {
            Missing::Gone => "its output is gone".to_string(),
            Missing::WrongSize { found } => {
                format!("its output is {found} bytes and was recorded as a different size")
            }
            Missing::WrongHash => "its output no longer hashes to what was recorded".to_string(),
            Missing::Unreadable(e) => format!("its output could not be read: {e}"),
        }
    }
}

impl NodeOutput {
    /// Record a file as a node's output, hashing it now.
    ///
    /// Hashed at record time rather than at reuse time, because the point is to
    /// notice a change: a hash taken when the file is read would agree with
    /// itself no matter what happened in between.
    pub fn of_file(
        node_id: &str,
        run_id: Option<&str>,
        kind: Kind,
        path: &Path,
    ) -> Result<Self, String> {
        let meta = std::fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let sha256 = crate::backfill::hash_file(path)
            .ok_or_else(|| format!("{}: could not be hashed", path.display()))?;
        Ok(NodeOutput {
            node_id: node_id.to_string(),
            run_id: run_id.map(str::to_string),
            kind,
            uri: path.to_string_lossy().replace(char::from(92), "/"),
            sha256,
            bytes: meta.len(),
            rows: None,
            columns: Vec::new(),
            cache_key: None,
        })
    }

    /// Is what was recorded still there, and still the same bytes?
    ///
    /// Size first because it is a `stat` and rules out the common case; the
    /// hash only when the size agrees, because reading the file is the
    /// expensive half and a size mismatch has already answered the question.
    pub fn verify(&self) -> Result<(), Missing> {
        let meta = match std::fs::metadata(self.path()) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(Missing::Gone),
            Err(e) => return Err(Missing::Unreadable(e.to_string())),
        };
        if meta.len() != self.bytes {
            return Err(Missing::WrongSize { found: meta.len() });
        }
        match crate::backfill::hash_file(self.path()) {
            None => Err(Missing::Unreadable("could not be hashed".to_string())),
            Some(h) if !h.eq_ignore_ascii_case(&self.sha256) => Err(Missing::WrongHash),
            Some(_) => Ok(()),
        }
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.uri)
    }

    /// The statement that makes this output the node's relation.
    ///
    /// The same shape `outcache::restore` and `plan::apply_stage_cache_in`
    /// already emit, and deliberately so: the stage keeps its own relation
    /// name, so nothing downstream can tell a bound output from a computed one.
    ///
    /// `None` for an artifact. A document or a model is not a relation, and
    /// producing SQL that pretended otherwise would fail at run time instead of
    /// here.
    pub fn bind_sql(&self) -> Option<String> {
        if self.kind != Kind::Relation {
            return None;
        }
        Some(format!(
            "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
            crate::plan::quote_ident(&self.node_id),
            self.uri.replace('\'', "''")
        ))
    }
}

/// Every recorded output that is still exactly what was recorded.
///
/// The unverified ones are returned separately rather than dropped: "node D
/// cannot be reused because its output is gone" is the sentence a retry has to
/// be able to say, and a filter would leave it with nothing to say it from.
pub fn verified(
    outputs: &std::collections::BTreeMap<String, NodeOutput>,
) -> (
    std::collections::BTreeMap<String, NodeOutput>,
    std::collections::BTreeMap<String, Missing>,
) {
    let mut ok = std::collections::BTreeMap::new();
    let mut bad = std::collections::BTreeMap::new();
    for (id, out) in outputs {
        match out.verify() {
            Ok(()) => {
                ok.insert(id.clone(), out.clone());
            }
            Err(why) => {
                bad.insert(id.clone(), why);
            }
        }
    }
    (ok, bad)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrote(dir: &Path, name: &str, body: &[u8]) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn a_recorded_output_carries_what_it_takes_to_check_it() {
        let tmp = tempfile::tempdir().unwrap();
        let p = wrote(tmp.path(), "parse.parquet", b"rows");
        let out = NodeOutput::of_file("parse", Some("run-1"), Kind::Relation, &p).unwrap();
        assert_eq!(out.node_id, "parse");
        assert_eq!(out.run_id.as_deref(), Some("run-1"));
        assert_eq!(out.bytes, 4);
        assert_eq!(out.sha256.len(), 64, "a full sha256: {}", out.sha256);
        assert!(!out.uri.contains(char::from(92)), "forward slashes: {}", out.uri);
        assert_eq!(out.verify(), Ok(()));
    }

    /// The failure the whole type exists for: a receipt that says "succeeded"
    /// while the output has been pruned.
    #[test]
    fn an_output_that_went_away_is_not_reusable() {
        let tmp = tempfile::tempdir().unwrap();
        let p = wrote(tmp.path(), "parse.parquet", b"rows");
        let out = NodeOutput::of_file("parse", None, Kind::Relation, &p).unwrap();
        std::fs::remove_file(&p).unwrap();
        assert_eq!(out.verify(), Err(Missing::Gone));
    }

    #[test]
    fn an_output_that_changed_underneath_is_not_reusable() {
        let tmp = tempfile::tempdir().unwrap();
        let p = wrote(tmp.path(), "parse.parquet", b"rows");
        let out = NodeOutput::of_file("parse", None, Kind::Relation, &p).unwrap();

        // Same length, different bytes: only the hash can tell.
        std::fs::write(&p, b"ROWS").unwrap();
        assert_eq!(out.verify(), Err(Missing::WrongHash), "an edit in place is caught");

        std::fs::write(&p, b"more rows").unwrap();
        assert_eq!(out.verify(), Err(Missing::WrongSize { found: 9 }));
    }

    #[test]
    fn a_relation_binds_and_an_artifact_does_not() {
        let tmp = tempfile::tempdir().unwrap();
        let p = wrote(tmp.path(), "parse.parquet", b"rows");
        let rel = NodeOutput::of_file("parse", None, Kind::Relation, &p).unwrap();
        let sql = rel.bind_sql().expect("a relation binds");
        assert!(sql.starts_with("CREATE OR REPLACE VIEW \"parse\" AS"), "{sql}");
        assert!(sql.contains("read_parquet('"), "{sql}");

        let art = NodeOutput { kind: Kind::Artifact, ..rel };
        assert_eq!(art.bind_sql(), None, "a report is not a relation");
    }

    #[test]
    fn a_quote_in_the_path_cannot_break_out_of_the_literal() {
        let tmp = tempfile::tempdir().unwrap();
        let p = wrote(tmp.path(), "it's.parquet", b"rows");
        let out = NodeOutput::of_file("n", None, Kind::Relation, &p).unwrap();
        let sql = out.bind_sql().unwrap();
        assert!(sql.contains("it''s.parquet"), "the quote is doubled: {sql}");
    }

    #[test]
    fn verified_separates_what_can_be_bound_from_why_the_rest_cannot() {
        let tmp = tempfile::tempdir().unwrap();
        let good = wrote(tmp.path(), "a.parquet", b"aa");
        let gone = wrote(tmp.path(), "b.parquet", b"bb");
        let mut all = std::collections::BTreeMap::new();
        all.insert("a".to_string(), NodeOutput::of_file("a", None, Kind::Relation, &good).unwrap());
        all.insert("b".to_string(), NodeOutput::of_file("b", None, Kind::Relation, &gone).unwrap());
        std::fs::remove_file(&gone).unwrap();

        let (ok, bad) = verified(&all);
        assert_eq!(ok.keys().collect::<Vec<_>>(), ["a"]);
        assert_eq!(bad["b"], Missing::Gone);
        assert!(bad["b"].describe().contains("gone"), "it can be said out loud");
    }

    #[test]
    fn a_receipt_written_before_this_existed_still_reads() {
        // Absent is the ordinary case: every run before this shipped.
        let json = serde_json::json!({
            "nodeId": "parse",
            "kind": "relation",
            "uri": "cache/p/parse/k.parquet",
            "sha256": "ab",
            "bytes": 4
        });
        let out: NodeOutput = serde_json::from_value(json).expect("the optional fields default");
        assert_eq!(out.rows, None);
        assert!(out.columns.is_empty());
        assert_eq!(out.cache_key, None);
    }
}
