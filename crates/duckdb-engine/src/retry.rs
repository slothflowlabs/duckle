//! #305: retry a failed run without repeating what is already known-good, and
//! without repeating a side effect nobody asked to repeat.
//!
//! ## Why a receipt exists at all
//!
//! A retry has to answer "is this the same work?" before it can reuse anything,
//! and nothing recorded today can answer it. [`crate::history::RunRecord`] keeps
//! aggregate status, a row total and an error string; it has no per-node
//! outcome, no pipeline identity, and no link between runs. `RunResult` and
//! `NodeRunStatus` carry the detail but are `Serialize` only, so a finished run
//! is unreadable the moment the process ends. The run log is NDJSON gated on
//! `DUCKLE_LOG_DIR`, with every run of a pipeline in one unindexed file.
//!
//! So this module writes a small, addressable **receipt** per run, and the
//! retry planner reads it. The receipt is deliberately not the run history: the
//! history is a human-facing list capped at 50 entries, and a retry needs a
//! record keyed by run id that does not age out from under it.
//!
//! ## What a retry can and cannot reuse
//!
//! Reuse rides entirely on the existing output cache, and that cache is
//! narrower than it looks:
//!
//! - It is **opt-in per node** (`cacheOutput`), declared by six components out
//!   of ~382, so a pipeline that never ticked the box reuses nothing.
//! - It skips a stage's **own compute**, given an input that already exists in
//!   this run. `outcache::input_fingerprint` reads the upstream relation out of
//!   the run's temp database to form the key, so the input must have been
//!   produced first. It does not skip producing the input, and sources are
//!   refused outright.
//!
//! That is why this planner promises reuse per node rather than "resume from
//! node N": the honest unit is a stage whose recorded output still exists and
//! whose identity still matches, not a cut across the graph.
//!
//! ## The refusal is the point
//!
//! A sink writes somewhere outside the run. Nothing in the engine can currently
//! tell an idempotent sink from one that must not be repeated: there is no
//! side-effect classification, and write mode is a per-connector property with
//! no shared meaning. So the planner does not guess. It reports every sink it
//! would re-execute and refuses to plan the retry until the operator says to
//! rewrite them. Being told "this will write to 3 sinks again" is the whole
//! value; quietly doing it is the failure this exists to prevent.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The engine build a run happened under. A parser fix or a changed default
/// makes the same input produce a different answer, which is the same reason
/// the output cache bakes the build into its key.
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// One node, as the run left it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReceiptNode {
    /// "ok", "unchanged", "skipped", "error" - straight off the run result.
    pub status: String,
    /// "source" / "transform" / "sink" and friends. Carried because the sink
    /// refusal depends on it and re-deriving it later would mean re-planning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The output-cache key this node's result was stored under, when it had
    /// one. Absent for every node that is not cache-eligible, which is most of
    /// them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_cache_key: Option<String>,
}

/// What a run was, in the terms a retry needs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RunReceipt {
    pub run_id: String,
    /// The run this one was a retry of. `None` for an original run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub at: String,
    pub status: String,
    pub pipeline_name: String,
    /// Where the pipeline file was, so a retry can find the same one rather
    /// than asking the operator to remember.
    pub pipeline_path: String,
    /// sha256 of the pipeline document **as parsed**, before any resolution
    /// pass. Taken pre-resolution deliberately: `apply_time_builtins` stamps a
    /// fresh date into the document on every run, so a hash taken afterwards
    /// would differ every day and call an unchanged pipeline changed.
    pub pipeline_hash: String,
    pub engine_version: String,
    pub nodes: BTreeMap<String, ReceiptNode>,
}

/// Receipts live beside the run history but keyed by run id, because that is
/// what a retry has in its hand.
pub fn dir(workspace: &Path) -> PathBuf {
    workspace.join("runs").join("receipts")
}

fn path_for(workspace: &Path, run_id: &str) -> PathBuf {
    dir(workspace).join(format!("{}.json", crate::connectors::sanitize_path_segment(run_id)))
}

/// How many receipts a workspace keeps. Larger than the history's 50 because a
/// receipt is small and a retry is most wanted for a run that is not the most
/// recent one.
const MAX_RECEIPTS: usize = 200;

/// The identity of a pipeline document, for deciding whether two runs are the
/// same work.
pub fn pipeline_hash(doc: &crate::PipelineDoc) -> String {
    let bytes = serde_json::to_vec(doc).unwrap_or_default();
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(&bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Write a receipt. Best-effort in the caller's hands: a run that cannot record
/// itself is still a run that happened.
pub fn write(workspace: &Path, receipt: &RunReceipt) -> std::io::Result<()> {
    let d = dir(workspace);
    std::fs::create_dir_all(&d)?;
    let text = serde_json::to_string_pretty(receipt).unwrap_or_default();
    std::fs::write(path_for(workspace, &receipt.run_id), text)?;
    prune(&d);
    Ok(())
}

/// Keep the newest [`MAX_RECEIPTS`]. Best-effort: failing to prune must never
/// fail a run.
fn prune(d: &Path) {
    let mut entries: Vec<(std::time::SystemTime, PathBuf)> = match std::fs::read_dir(d) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .filter_map(|e| {
                let m = e.metadata().ok()?;
                Some((m.modified().ok()?, e.path()))
            })
            .collect(),
        Err(_) => return,
    };
    if entries.len() <= MAX_RECEIPTS {
        return;
    }
    entries.sort_by_key(|(t, _)| *t);
    for (_, p) in entries.iter().take(entries.len() - MAX_RECEIPTS) {
        let _ = std::fs::remove_file(p);
    }
}

/// Why a receipt could not be read. Absent and unreadable are told apart on
/// purpose: the run history collapses both into an empty list, so a corrupt
/// file there reads as "no runs", and a retry must not repeat that.
#[derive(Debug, PartialEq)]
pub enum LoadError {
    NotFound,
    Unreadable(String),
}

pub fn load(workspace: &Path, run_id: &str) -> Result<RunReceipt, LoadError> {
    let p = path_for(workspace, run_id);
    let text = match std::fs::read_to_string(&p) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(LoadError::NotFound),
        Err(e) => return Err(LoadError::Unreadable(e.to_string())),
    };
    serde_json::from_str(&text).map_err(|e| LoadError::Unreadable(e.to_string()))
}

/// What the retry will do with one node.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum Action {
    /// The recorded output still exists and the identity still matches.
    Reuse { evidence: String },
    /// It will run again, and why.
    ReExecute { reason: String },
    /// It will run again AND it writes somewhere outside the run.
    RewriteSink { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Decision {
    pub node_id: String,
    #[serde(flatten)]
    pub action: Action,
}

/// A refusal is a plan that was not made. It carries a stable code so a caller
/// can branch on it without matching on prose.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Refusal {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub run_id: String,
    pub parent_run_id: String,
    /// Set when the retry cannot be planned. When present, `decisions` is
    /// empty: a refusal plans nothing, rather than planning something and
    /// hoping the caller checks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<Refusal>,
    pub decisions: Vec<Decision>,
    /// Sinks that would be written again. Empty unless the plan proceeds.
    pub sinks_to_rewrite: Vec<String>,
}

impl Plan {
    fn refused(parent: &str, code: &str, message: String) -> Self {
        Plan {
            run_id: String::new(),
            parent_run_id: parent.to_string(),
            refusal: Some(Refusal { code: code.to_string(), message }),
            decisions: Vec::new(),
            sinks_to_rewrite: Vec::new(),
        }
    }
}

/// Plan a retry of `run_id` against the pipeline as it stands now.
///
/// `cache_hit` answers "is the output recorded under this key still on disk?".
/// It is a parameter rather than a direct call so the rule can be tested
/// without a workspace full of parquet, and so the planner never has to know
/// where the cache keeps things.
pub fn plan(
    workspace: &Path,
    run_id: &str,
    doc: &crate::PipelineDoc,
    new_run_id: &str,
    allow_changed: bool,
    rerun_sinks: bool,
    cache_hit: &dyn Fn(&str, &str) -> Option<String>,
) -> Plan {
    let prior = match load(workspace, run_id) {
        Ok(r) => r,
        Err(LoadError::NotFound) => {
            return Plan::refused(
                run_id,
                "retry:no-receipt",
                format!(
                    "no receipt for run {run_id}. Only a run started by `duckle-runner --pipeline` \
                     writes one, so a run from the API, the scheduler or the desktop app cannot be \
                     retried by id yet."
                ),
            )
        }
        Err(LoadError::Unreadable(e)) => {
            return Plan::refused(
                run_id,
                "retry:unreadable-receipt",
                format!("the receipt for run {run_id} could not be read ({e}). Refusing to guess."),
            )
        }
    };

    if prior.status == "ok" {
        return Plan::refused(
            run_id,
            "retry:run-succeeded",
            format!(
                "run {run_id} succeeded. Retrying it would repeat work that already landed, \
                 including anything it wrote."
            ),
        );
    }

    let now_hash = pipeline_hash(doc);
    if now_hash != prior.pipeline_hash && !allow_changed {
        return Plan::refused(
            run_id,
            "retry:pipeline-changed",
            format!(
                "the pipeline has changed since run {run_id} (was {}, now {}). Nothing recorded by \
                 that run describes this pipeline, so reuse cannot be justified. Re-run it \
                 normally, or pass --allow-changed to retry with reuse disabled.",
                &prior.pipeline_hash[..prior.pipeline_hash.len().min(12)],
                &now_hash[..now_hash.len().min(12)]
            ),
        );
    }
    if prior.engine_version != ENGINE_VERSION && !allow_changed {
        return Plan::refused(
            run_id,
            "retry:engine-changed",
            format!(
                "run {run_id} ran under engine {} and this is {ENGINE_VERSION}. A fix or a changed \
                 default can make the same input produce a different answer, so its outputs are \
                 not reusable. Pass --allow-changed to retry with reuse disabled.",
                prior.engine_version
            ),
        );
    }
    // A changed pipeline or engine may still be retried, but never with reuse:
    // the recorded outputs describe work that no longer exists.
    let reuse_allowed = now_hash == prior.pipeline_hash && prior.engine_version == ENGINE_VERSION;

    let mut decisions = Vec::new();
    let mut sinks = Vec::new();
    for node in &doc.nodes {
        let id = node.id.clone();
        let prior_node = prior.nodes.get(&id);
        // Whether this writes outside the run is a property of the PIPELINE, not
        // of what the broken run managed to record. A run that died at the first
        // node records nothing for anything downstream, so asking the receipt
        // would answer "not a sink" for every sink the failure never reached -
        // and the retry would then re-run them without saying so.
        let is_sink = node
            .data
            .component_id
            .as_deref()
            .is_some_and(|c| c.starts_with("snk."))
            || prior_node.and_then(|n| n.kind.as_deref()) == Some("sink");

        let action = if !reuse_allowed {
            let reason = "the pipeline or engine changed, so nothing recorded is reusable".to_string();
            if is_sink { Action::RewriteSink { reason } } else { Action::ReExecute { reason } }
        } else {
            match prior_node {
                None => {
                    let reason = "the previous run did not record this node".to_string();
                    if is_sink { Action::RewriteSink { reason } } else { Action::ReExecute { reason } }
                }
                Some(n) if n.status == "error" => {
                    let reason = "it failed last time".to_string();
                    if is_sink { Action::RewriteSink { reason } } else { Action::ReExecute { reason } }
                }
                Some(n) => match n.output_cache_key.as_deref() {
                    // A sink is never reused even with a key: its effect is
                    // outside the run, and restoring a table does not undo or
                    // redo that.
                    Some(_) if is_sink => Action::RewriteSink {
                        reason: "a sink writes outside the run, so its result is not reusable".into(),
                    },
                    Some(key) => match cache_hit(&id, key) {
                        Some(evidence) => Action::Reuse { evidence },
                        // The receipt says it succeeded; the output is gone.
                        // Trusting the receipt here is what would turn
                        // "verified reuse" into decoration.
                        None => Action::ReExecute {
                            reason: "the recorded output is gone".into(),
                        },
                    },
                    None => {
                        let reason = "nothing was cached for it".to_string();
                        if is_sink { Action::RewriteSink { reason } } else { Action::ReExecute { reason } }
                    }
                },
            }
        };
        if matches!(action, Action::RewriteSink { .. }) {
            sinks.push(id.clone());
        }
        decisions.push(Decision { node_id: id, action });
    }

    if !sinks.is_empty() && !rerun_sinks {
        return Plan::refused(
            run_id,
            "retry:would-rewrite-sinks",
            format!(
                "this retry would write again to {}: {}. Nothing here can tell a sink that is safe \
                 to repeat from one that is not, so it will not be decided for you. Re-run with \
                 --rerun-sinks once you know repeating those writes is safe.",
                sinks.len(),
                sinks.join(", ")
            ),
        );
    }

    Plan {
        run_id: new_run_id.to_string(),
        parent_run_id: run_id.to_string(),
        refusal: None,
        decisions,
        sinks_to_rewrite: sinks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_with(nodes: &[(&str, &str)]) -> crate::PipelineDoc {
        let ns: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(id, comp)| {
                serde_json::json!({
                    "id": id,
                    "position": { "x": 0, "y": 0 },
                    "data": { "label": id, "componentId": comp, "properties": {} }
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({ "nodes": ns, "edges": [] })).unwrap()
    }

    fn receipt(status: &str, hash: &str, nodes: &[(&str, &str, Option<&str>, &str)]) -> RunReceipt {
        RunReceipt {
            run_id: "r1".into(),
            parent_run_id: None,
            at: "2026-08-31T00:00:00Z".into(),
            status: status.into(),
            pipeline_name: "p".into(),
            pipeline_path: "/tmp/p.json".into(),
            pipeline_hash: hash.into(),
            engine_version: ENGINE_VERSION.into(),
            nodes: nodes
                .iter()
                .map(|(id, st, key, kind)| {
                    (
                        (*id).to_string(),
                        ReceiptNode {
                            status: (*st).to_string(),
                            kind: Some((*kind).to_string()),
                            output_cache_key: key.map(|k| k.to_string()),
                        },
                    )
                })
                .collect(),
        }
    }

    fn hit_always(_: &str, _: &str) -> Option<String> {
        Some("<ws>/cache/p/extract/KEY.parquet".to_string())
    }
    fn hit_never(_: &str, _: &str) -> Option<String> {
        None
    }

    /// A receipt has to survive the process, or a retry has nothing to read.
    #[test]
    fn a_receipt_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let r = receipt("error", "abc", &[("a", "ok", Some("K"), "source")]);
        write(tmp.path(), &r).unwrap();
        assert_eq!(load(tmp.path(), "r1").unwrap(), r);
    }

    /// Absent and unreadable are different answers. The run history collapses
    /// both into an empty list, so a corrupt file there reads as "no runs";
    /// a retry that repeated that would silently refuse for the wrong reason.
    #[test]
    fn an_absent_receipt_is_not_an_unreadable_one() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(load(tmp.path(), "nope"), Err(LoadError::NotFound));
        std::fs::create_dir_all(dir(tmp.path())).unwrap();
        std::fs::write(dir(tmp.path()).join("bad.json"), "{ not json").unwrap();
        assert!(matches!(load(tmp.path(), "bad"), Err(LoadError::Unreadable(_))));
    }

    /// The pipeline hash must ignore what a run stamps into the document.
    /// `apply_time_builtins` rewrites a dated path on every run, so a hash
    /// taken after resolution would call an unchanged pipeline changed every
    /// day and reuse would never once apply.
    #[test]
    fn the_hash_is_of_the_document_as_authored() {
        let a = doc_with(&[("n", "src.csv")]);
        let b = doc_with(&[("n", "src.csv")]);
        assert_eq!(pipeline_hash(&a), pipeline_hash(&b));
        let c = doc_with(&[("n", "src.json")]);
        assert_ne!(pipeline_hash(&a), pipeline_hash(&c), "a real edit must change it");
    }

    /// AC1, as far as it honestly reaches: a node whose output was cached and
    /// still exists is reused.
    #[test]
    fn an_unchanged_pipeline_plans_reuse_for_a_cached_node() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("extract", "src.xml")]);
        let h = pipeline_hash(&d);
        write(tmp.path(), &receipt("error", &h, &[("extract", "ok", Some("K"), "source")])).unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_always);
        assert!(p.refusal.is_none(), "should plan: {:?}", p.refusal);
        assert_eq!(
            p.decisions[0].action,
            Action::Reuse { evidence: "<ws>/cache/p/extract/KEY.parquet".into() },
            "a cached node whose output is still there must be reused"
        );
    }

    /// The word "verified" in the acceptance criteria has to mean something.
    /// The receipt saying a node succeeded is not evidence its output still
    /// exists; only looking is.
    #[test]
    fn a_missing_cache_file_is_not_reuse() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("extract", "src.xml")]);
        let h = pipeline_hash(&d);
        write(tmp.path(), &receipt("error", &h, &[("extract", "ok", Some("K"), "source")])).unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_never);
        assert_eq!(
            p.decisions[0].action,
            Action::ReExecute { reason: "the recorded output is gone".into() },
            "trusting the receipt here would promise reuse of a file that is not there"
        );
    }

    /// AC2. A refusal plans NOTHING - it does not hand back decisions and hope
    /// the caller checks the refusal first.
    #[test]
    fn a_changed_pipeline_refuses_and_plans_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("extract", "src.xml")]);
        write(tmp.path(), &receipt("error", "a-different-hash", &[("extract", "ok", Some("K"), "source")]))
            .unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_always);
        assert_eq!(p.refusal.as_ref().map(|r| r.code.as_str()), Some("retry:pipeline-changed"));
        assert!(p.decisions.is_empty(), "a refusal must not also plan work");
    }

    /// --allow-changed proceeds, but never with reuse: the recorded outputs
    /// describe work that no longer exists.
    #[test]
    fn allow_changed_proceeds_without_reusing_anything() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("extract", "src.xml")]);
        write(tmp.path(), &receipt("error", "a-different-hash", &[("extract", "ok", Some("K"), "source")]))
            .unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", true, false, &hit_always);
        assert!(p.refusal.is_none(), "allow-changed must proceed: {:?}", p.refusal);
        assert!(
            !matches!(p.decisions[0].action, Action::Reuse { .. }),
            "but it must not reuse an output from a pipeline that changed"
        );
    }

    /// AC3, the part that matters. A sink writes outside the run and nothing
    /// here can tell a safe repeat from an unsafe one, so it is not decided
    /// for the operator.
    #[test]
    fn a_retry_that_would_write_to_a_sink_refuses_until_told() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("extract", "src.xml"), ("publish", "snk.csv")]);
        let h = pipeline_hash(&d);
        write(
            tmp.path(),
            &receipt(
                "error",
                &h,
                &[("extract", "ok", Some("K"), "source"), ("publish", "error", None, "sink")],
            ),
        )
        .unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_always);
        let r = p.refusal.expect("must refuse");
        assert_eq!(r.code, "retry:would-rewrite-sinks");
        assert!(r.message.contains("publish"), "must name the sink: {}", r.message);

        // Told explicitly, it proceeds and still says which sinks it will write.
        let ok = plan(tmp.path(), "r1", &d, "r2", false, true, &hit_always);
        assert!(ok.refusal.is_none());
        assert_eq!(ok.sinks_to_rewrite, vec!["publish".to_string()]);
    }

    /// A sink is never reused even when a key was recorded for it: restoring a
    /// table does not redo, or undo, a write that happened outside the run.
    #[test]
    fn a_sink_is_never_reused_even_with_a_cache_key() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("publish", "snk.csv")]);
        let h = pipeline_hash(&d);
        write(tmp.path(), &receipt("error", &h, &[("publish", "ok", Some("K"), "sink")])).unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", false, true, &hit_always);
        assert!(
            matches!(p.decisions[0].action, Action::RewriteSink { .. }),
            "got {:?}",
            p.decisions[0].action
        );
    }

    /// A sink the previous run never REACHED is still a sink.
    ///
    /// The failure that motivated this test: a run that died at the first node
    /// records nothing for anything downstream, so asking the receipt "was this
    /// a sink?" answers no, and the retry planned a quiet re-run of a node that
    /// writes outside the run. The kind has to come from the pipeline being
    /// retried, not from what the broken run managed to record.
    #[test]
    fn a_sink_the_previous_run_never_reached_is_still_a_sink() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("extract", "src.csv"), ("publish", "snk.csv")]);
        let h = pipeline_hash(&d);
        // Only `extract` is recorded, and it failed. `publish` never ran.
        write(tmp.path(), &receipt("error", &h, &[("extract", "error", None, "source")])).unwrap();

        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_always);
        let r = p.refusal.expect("a retry that would write to an unreached sink must still refuse");
        assert_eq!(r.code, "retry:would-rewrite-sinks");
        assert!(r.message.contains("publish"), "must name it: {}", r.message);
    }

    /// Retrying a run that worked would repeat everything it did, including
    /// what it wrote.
    #[test]
    fn a_run_that_succeeded_is_not_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("n", "src.xml")]);
        let h = pipeline_hash(&d);
        write(tmp.path(), &receipt("ok", &h, &[("n", "ok", None, "source")])).unwrap();
        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_always);
        assert_eq!(p.refusal.map(|r| r.code), Some("retry:run-succeeded".to_string()));
    }

    /// Most runs write no receipt yet. Saying so beats guessing at what the
    /// run did.
    #[test]
    fn a_run_with_no_receipt_says_so() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("n", "src.xml")]);
        let p = plan(tmp.path(), "ghost", &d, "r2", false, false, &hit_always);
        let r = p.refusal.expect("must refuse");
        assert_eq!(r.code, "retry:no-receipt");
        assert!(r.message.contains("duckle-runner"), "must say who writes one: {}", r.message);
    }

    /// A node the previous run never reached is re-executed, not assumed fine.
    #[test]
    fn a_node_the_previous_run_never_reached_is_re_executed() {
        let tmp = tempfile::tempdir().unwrap();
        let d = doc_with(&[("a", "src.xml"), ("b", "xf.filter")]);
        let h = pipeline_hash(&d);
        write(tmp.path(), &receipt("error", &h, &[("a", "ok", Some("K"), "source")])).unwrap();
        let p = plan(tmp.path(), "r1", &d, "r2", false, false, &hit_always);
        let b = p.decisions.iter().find(|d| d.node_id == "b").unwrap();
        assert_eq!(
            b.action,
            Action::ReExecute { reason: "the previous run did not record this node".into() }
        );
    }
}
