//! #295: a backfill that survives a restart, and knows which day failed.
//!
//! The plan is written before anything runs and updated as each slice
//! finishes, so a server that dies halfway through comes back knowing what it
//! had done. That is the whole difference between a backfill and a long run:
//! one can be resumed and retried per slice, the other can only be started
//! again.
//!
//! ## States mean what they say
//!
//! `running` is a claim about a process that exists. On the next start,
//! anything still marked running is turned into `interrupted` - the same
//! reconciliation a run receipt gets - because a slice that was killed and one
//! that is quietly still going call for opposite responses, and telling them
//! apart afterwards is impossible if both read `running` forever.
//!
//! ## Retry means the failures, not the day's work
//!
//! Retrying moves `failed` back to `requested` and leaves `succeeded` alone.
//! A backfill of a thousand days that fails on four should cost four runs to
//! finish, not a thousand.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// Wanted, not yet started.
    Requested,
    /// A process claimed it and said so on disk before starting.
    Running,
    Succeeded,
    Failed,
    /// The process holding it went away.
    Interrupted,
    Cancelled,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Requested => "requested",
            State::Running => "running",
            State::Succeeded => "succeeded",
            State::Failed => "failed",
            State::Interrupted => "interrupted",
            State::Cancelled => "cancelled",
        }
    }

    /// Whether this slice still needs attention - it has not succeeded and was
    /// not cancelled. Used to decide whether a backfill is finished, and what
    /// a retry should pick up.
    pub fn is_open(self) -> bool {
        matches!(self, State::Requested | State::Failed | State::Interrupted)
    }

    /// Whether a worker may claim it in THIS pass.
    ///
    /// Only `requested`. A failed slice still needs attention and must not be
    /// re-claimed by the same run: an executor that claimed anything `is_open`
    /// picked its own failure straight back up and retried it forever, which
    /// is how a five-day backfill with one missing file never terminated.
    /// Retrying is a deliberate act that moves failures back to `requested`.
    pub fn is_claimable(self) -> bool {
        self == State::Requested
    }
}

/// What kind of slice a ledger holds.
///
/// #306: a chunk is a slice with a different generator, so it is the same
/// ledger with the same lifecycle - requested/running/succeeded/failed, retry,
/// restart reconciliation, run ids, provenance - rather than a second one that
/// would quietly drift from this one. Only what a slice IS differs: a partition
/// binds parameters over a time window, a chunk binds a predicate over a key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A window of a partitioned pipeline (#295).
    #[default]
    Partition,
    /// A bounded read of one source, by predicate (#306).
    Chunk,
    /// One link of an ordered delta chain (#326).
    Sequence,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Partition => "partition",
            Kind::Chunk => "chunk",
            Kind::Sequence => "sequence",
        }
    }
}

/// The durable output a slice produced.
///
/// #306: "the query finished" is not "the slice succeeded". A process that dies
/// between the read and the commit would otherwise leave a slice marked done
/// whose output is not there, and the retry that is supposed to fix exactly
/// that would skip it - silently. So the file is committed and hashed BEFORE
/// the ledger moves, and this is what was committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SliceArtifact {
    pub uri: String,
    pub hash: String,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRun {
    pub key: String,
    pub state: State,
    /// The durable run this slice produced, so the receipt, the log and the
    /// lineage for one day are all reachable from the backfill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// What this slice binds, carried so a retry uses the same values the
    /// original attempt did rather than regenerating them from a definition
    /// that may since have been edited.
    pub params: std::collections::BTreeMap<String, String>,
    /// #295: what this slice IS, independent of which backfill asked for it.
    ///
    /// pipeline + partition + release, and the schedule occurrence when a
    /// schedule caused it. Two requests for the same slice of the same release
    /// carry the same id, which is what lets a restart or a race find that the
    /// work is already done instead of doing it again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence: Option<String>,
    /// #306: what this slice READS, when it is a chunk. A WHERE fragment over
    /// the key, generated once and stored, so a retry sends the same bytes the
    /// first attempt did rather than regenerating them from bounds the source
    /// may have grown past.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// #306: what this slice PRODUCED, committed and hashed before the state
    /// moved to succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<SliceArtifact>,
    /// #326: the key that must have SUCCEEDED before this slice may be claimed.
    ///
    /// Set only on an ordered chain that declared `requireContinuity`, which is
    /// what keeps continuity opt-in: a plain watermark is allowed to have gaps,
    /// and only the operator knows whether this feed promises not to.
    ///
    /// Absent means nothing has to come first - the first link of an epoch,
    /// whose predecessor is the baseline. Present and naming a key that is not
    /// in this ledger means the predecessor was never published, and the slice
    /// stays blocked; that is the difference between a hole and a baseline, and
    /// storing the requirement on the SUCCESSOR is what makes an absent
    /// predecessor block rather than silently pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires: Option<String>,
    /// #326: the object this link applies, for provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Backfill {
    pub id: String,
    pub pipeline: String,
    pub pipeline_path: String,
    pub created_at: String,
    /// The release active when the backfill was created (#297), so every slice
    /// is traceable to the code it was meant to run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<String>,
    pub max_concurrent: usize,
    /// The process that is executing it, when one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// #306. Defaulted rather than required, so every ledger written before
    /// chunks existed still loads and still reads as what it is.
    #[serde(default)]
    pub kind: Kind,
    /// The source node a chunked extract reads, when this is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_node: Option<String>,
    /// Where a chunked extract stages its parts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging: Option<String>,
    /// #326: which generation of an ordered chain this is - the full snapshot
    /// that reset it. Carried for provenance; the blocking is done by
    /// [`PartitionRun::requires`], which the plan builder already resolved
    /// against this epoch's baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
    pub partitions: Vec<PartitionRun>,
}

impl Backfill {
    pub fn counts(&self) -> std::collections::BTreeMap<&'static str, usize> {
        let mut out = std::collections::BTreeMap::new();
        for p in &self.partitions {
            *out.entry(p.state.as_str()).or_insert(0) += 1;
        }
        out
    }

    pub fn is_done(&self) -> bool {
        !self.partitions.iter().any(|p| p.state.is_open())
    }

    /// Whether the slice at `idx` may be claimed in this pass.
    ///
    /// #326. The ordering constraint an ordered feed needs is a claim
    /// predicate, not a second executor and not a scheduler:
    ///
    /// ```text
    /// is_claimable(slice)
    ///     -> requested?
    ///     -> if ordered: predecessor succeeded, or predecessor == baseline
    /// ```
    ///
    /// The ledger is the only record of how far the chain has got, so this
    /// consults the slices themselves rather than a position pointer that would
    /// have to be kept in agreement with them.
    ///
    /// Note which way round the requirement is stored. A missing delta has no
    /// slice - there is nothing to run - so asking "did my predecessor succeed"
    /// of a ledger that has no row for it must answer NO. It does, because the
    /// requirement lives on the successor and names a key: a key with no
    /// succeeded slice is unsatisfied whether it failed or was never published.
    pub fn claimable(&self, idx: usize) -> bool {
        self.partitions.get(idx).is_some_and(|p| {
            p.state.is_claimable() && self.blocking(p).is_none()
        })
    }

    /// The predecessor holding this slice back, if one is.
    fn blocking<'a>(&'a self, p: &'a PartitionRun) -> Option<&'a str> {
        let required = p.requires.as_deref()?;
        match self.partitions.iter().any(|q| q.key == required && q.state == State::Succeeded) {
            true => None,
            false => Some(required),
        }
    }

    /// Why a slice is not claimable, in the words an operator needs.
    ///
    /// The two cases read the same to the ledger and are different to a person:
    /// a predecessor that failed is a retry, and one that was never published
    /// is a conversation with the publisher.
    pub fn blocked_reason(&self, idx: usize) -> Option<String> {
        let p = self.partitions.get(idx)?;
        let required = self.blocking(p)?;
        let published = self.partitions.iter().any(|q| q.key == required);
        Some(match published {
            true => format!("waiting for {required}, which has not succeeded"),
            false => format!("waiting for {required}, which was never published"),
        })
    }

    /// Every slice a worker could take right now.
    pub fn claimable_count(&self) -> usize {
        (0..self.partitions.len()).filter(|i| self.claimable(*i)).count()
    }

    /// Move every failed or interrupted slice back to requested.
    ///
    /// Succeeded slices are untouched: a backfill of a thousand days that
    /// failed on four should cost four runs to finish, not a thousand.
    pub fn retry_open(&mut self, only: Option<&[String]>) -> usize {
        let mut n = 0;
        for p in self.partitions.iter_mut() {
            let wanted = only.is_none_or(|keys| keys.iter().any(|k| k == &p.key));
            if wanted && matches!(p.state, State::Failed | State::Interrupted) {
                p.state = State::Requested;
                p.error = None;
                n += 1;
            }
        }
        n
    }

    /// #306: a slice is only still succeeded if what it produced is still there.
    ///
    /// A crash between the read and the commit cannot leave a bad `succeeded`:
    /// [`commit`] hashes and then renames, so an incomplete part is never at the
    /// final path. What this catches is the output going away or changing
    /// afterwards, which a retry must treat as work to redo rather than skip -
    /// the exact failure resumability exists to prevent, and the one that would
    /// otherwise be silent.
    ///
    /// `deep` re-hashes every part. Off, the check is existence and length,
    /// which is what crash-safety needs and costs nothing. On, it reads every
    /// byte, which is the only thing that catches a file edited in place, and
    /// on a chunked extract that means reading the whole extract again.
    pub fn recheck_artifacts(&mut self, deep: bool) -> Vec<String> {
        let mut reset = Vec::new();
        for p in self.partitions.iter_mut() {
            if p.state != State::Succeeded {
                continue;
            }
            let Some(a) = p.artifact.clone() else { continue };
            let wrong = match std::fs::metadata(&a.uri) {
                Err(_) => Some("its output is gone".to_string()),
                Ok(m) if m.len() != a.bytes => {
                    Some(format!("its output is {} bytes and {} were committed", m.len(), a.bytes))
                }
                Ok(_) if deep => match hash_file(Path::new(&a.uri)) {
                    Some(h) if !h.eq_ignore_ascii_case(&a.hash) => {
                        Some("its output no longer hashes to what was committed".to_string())
                    }
                    None => Some("its output could not be read".to_string()),
                    Some(_) => None,
                },
                Ok(_) => None,
            };
            if let Some(why) = wrong {
                p.state = State::Requested;
                p.error = Some(format!("must be redone: {why}"));
                p.artifact = None;
                reset.push(p.key.clone());
            }
        }
        reset
    }

    pub fn cancel(&mut self) -> usize {
        let mut n = 0;
        for p in self.partitions.iter_mut() {
            if p.state.is_open() {
                p.state = State::Cancelled;
                n += 1;
            }
        }
        n
    }
}

/// The identity of one slice of work (#295).
///
/// Deterministic and order-independent: the same pipeline, partition, release
/// and schedule occurrence always hash to the same id, on any machine and in
/// any process. That is the whole point - a value that varied per request could
/// not answer "has this already been done".
///
/// The release is part of it because the same date against different code is
/// different work; a rebuild that changes a pipeline changes the release, and
/// the slice becomes newly wanted rather than silently already-done.
pub fn occurrence_id(
    pipeline: &str,
    partition: &str,
    release: Option<&str>,
    schedule_occurrence: Option<&str>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    // Length-prefixed, so ("ab", "c") and ("a", "bc") are different slices
    // rather than the same one - a joiner character would collide the moment a
    // partition key contained it.
    for part in [
        pipeline,
        partition,
        release.unwrap_or(""),
        schedule_occurrence.unwrap_or(""),
    ] {
        h.update(part.len().to_le_bytes());
        h.update(part.as_bytes());
    }
    h.finalize().iter().take(16).map(|b| format!("{b:02x}")).collect()
}

/// The run that already did this exact slice, if one did.
///
/// Searched across every plan in the workspace rather than within one, because
/// "has this been done" is a question about the work, not about which backfill
/// happened to ask. A restart that recreates a plan, or two schedules firing
/// the same occurrence, both land here.
///
/// Returns the slice itself, not just its run id: a chunk's output is the
/// point of reusing it (#306), and a reuse that forgot the artifact would mark
/// the new slice succeeded with nothing to show for it.
pub fn already_succeeded(workspace: &Path, occurrence: &str) -> Option<(String, PartitionRun)> {
    for b in list(workspace) {
        for p in &b.partitions {
            if p.state == State::Succeeded && p.occurrence.as_deref() == Some(occurrence) {
                return Some((b.id.clone(), p.clone()));
            }
        }
    }
    None
}

/// sha256 of a file, streamed.
pub fn hash_file(path: &Path) -> Option<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;
    let mut h = Sha256::new();
    // Streamed rather than read to a Vec: a chunk part is the size of a chunk,
    // and the whole point of chunking is that the whole thing does not fit
    // comfortably in memory.
    let mut buf = vec![0u8; 1 << 16];
    loop {
        match f.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => h.update(&buf[..n]),
            Err(_) => return None,
        }
    }
    Some(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

/// Publish a slice's output, and say what was published.
///
/// The order is the rule (#306): the bytes are flushed to disk, then hashed,
/// then moved into place. Nothing is ever at the final path that was not
/// complete when it got there, so a process killed at any point leaves either
/// no part or a whole one, and never a truncated file that a later run would
/// count as done.
pub fn commit(tmp: &Path, final_path: &Path, rows: Option<u64>) -> Result<SliceArtifact, String> {
    let bytes = {
        // Opened for WRITE even though nothing is written: FlushFileBuffers
        // needs write access, so a read-only handle fails the fsync below with
        // "Access is denied" on Windows and with nothing at all on Unix.
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp)
            .map_err(|e| format!("{}: {e}", tmp.display()))?;
        // fsync before hashing and renaming: a rename is atomic with respect to
        // other readers, not with respect to power loss, and the file's own
        // contents have to be durable before its name says they are.
        f.sync_all().map_err(|e| format!("{}: {e}", tmp.display()))?;
        f.metadata().map_err(|e| format!("{}: {e}", tmp.display()))?.len()
    };
    let hash = hash_file(tmp).ok_or_else(|| format!("{}: could not be hashed", tmp.display()))?;
    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    // Rename, never remove-then-rename: on Windows a rename over an existing
    // file replaces it, and unlinking first opens a window where the part is
    // simply absent.
    std::fs::rename(tmp, final_path)
        .map_err(|e| format!("{} -> {}: {e}", tmp.display(), final_path.display()))?;
    Ok(SliceArtifact {
        uri: final_path.display().to_string().replace(char::from(92), "/"),
        hash,
        bytes,
        rows,
    })
}

pub fn dir(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("backfills")
}

pub fn path_for(workspace: &Path, id: &str) -> PathBuf {
    dir(workspace).join(format!("{id}.json"))
}

pub fn new_id(pipeline: &str) -> String {
    let safe: String = pipeline
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("bf-{safe}-{stamp}")
}

/// Write the plan, atomically.
///
/// Temp then rename, never unlink first: a reader must see the previous
/// complete plan or the new one, and a backfill whose file is briefly absent
/// is one a concurrent `status` reports as missing entirely.
pub fn save(workspace: &Path, backfill: &Backfill) -> Result<(), String> {
    let dir = dir(workspace);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let body = serde_json::to_string_pretty(backfill).map_err(|e| e.to_string())?;
    let tmp = dir.join(format!(".{}.tmp", backfill.id));
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path_for(workspace, &backfill.id)).map_err(|e| e.to_string())
}

pub fn load(workspace: &Path, id: &str) -> Result<Backfill, String> {
    let path = path_for(workspace, id);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

pub fn list(workspace: &Path) -> Vec<Backfill> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir(workspace)) else { return out };
    for e in entries.flatten() {
        if e.path().extension().is_none_or(|x| x != "json") {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(e.path()) {
            if let Ok(b) = serde_json::from_str::<Backfill>(&text) {
                out.push(b);
            }
        }
    }
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    out
}

/// Turn abandoned `running` slices into `interrupted`.
///
/// The same treatment a run receipt gets, for the same reason: a slice whose
/// process was killed and one still going look identical from outside, and
/// they call for opposite responses.
pub fn reconcile(workspace: &Path, live_pids: &dyn Fn(u32) -> bool) -> Vec<String> {
    let mut changed = Vec::new();
    for mut b in list(workspace) {
        if b.pid.is_some_and(|pid| live_pids(pid)) {
            continue;
        }
        let mut touched = false;
        for p in b.partitions.iter_mut() {
            if p.state == State::Running {
                p.state = State::Interrupted;
                touched = true;
            }
        }
        if touched {
            b.pid = None;
            if save(workspace, &b).is_ok() {
                changed.push(b.id.clone());
            }
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{Cadence, PartitionDef};

    fn plan(days: (&str, &str)) -> Backfill {
        let def = PartitionDef::Time {
            cadence: Cadence::Day,
            timezone: "UTC".into(),
            parameter_start: "window_start".into(),
            parameter_end: "window_end".into(),
        };
        let parts = crate::partition::generate(&def, days.0, days.1).unwrap();
        Backfill {
            id: "bf-test-1".into(),
            pipeline: "accounts".into(),
            pipeline_path: "pipelines/accounts.json".into(),
            created_at: "2026-09-01T00:00:00Z".into(),
            release_id: Some("rel-1".into()),
            max_concurrent: 4,
            pid: Some(std::process::id()),
            kind: Kind::Partition,
            chunk_node: None,
            staging: None,
            epoch: None,
            partitions: parts
                .into_iter()
                .map(|p| PartitionRun {
                    key: p.key,
                    state: State::Requested,
                    run_id: None,
                    attempts: 0,
                    error: None,
                    finished_at: None,
                    params: p.params,
                    occurrence: None,
                    predicate: None,
                    artifact: None,
                    requires: None,
                    source_uri: None,
                })
                .collect(),
        }
    }

    #[test]
    fn the_same_slice_of_the_same_release_has_the_same_identity() {
        // The whole point: a value that varied per request could not answer
        // "has this already been done".
        let a = occurrence_id("accounts", "2020-01-03", Some("rel-1"), Some("nightly@02:00"));
        let b = occurrence_id("accounts", "2020-01-03", Some("rel-1"), Some("nightly@02:00"));
        assert_eq!(a, b);

        // Any part differing is different work.
        assert_ne!(a, occurrence_id("orders", "2020-01-03", Some("rel-1"), Some("nightly@02:00")));
        assert_ne!(a, occurrence_id("accounts", "2020-01-04", Some("rel-1"), Some("nightly@02:00")));
        assert_ne!(a, occurrence_id("accounts", "2020-01-03", Some("rel-2"), Some("nightly@02:00")),
            "the same date against different code is different work");
        assert_ne!(a, occurrence_id("accounts", "2020-01-03", Some("rel-1"), Some("nightly@03:00")));
    }

    #[test]
    fn parts_cannot_run_together_to_collide() {
        // Length-prefixed rather than joined: with a separator, a partition key
        // containing it would silently become a different slice's identity.
        assert_ne!(
            occurrence_id("ab", "c", None, None),
            occurrence_id("a", "bc", None, None)
        );
        assert_ne!(
            occurrence_id("a-b", "c", None, None),
            occurrence_id("a", "b-c", None, None)
        );
    }

    #[test]
    fn a_slice_already_done_is_found_across_backfills() {
        // "Has this been done" is a question about the work, not about which
        // backfill happened to ask - a restart recreates the plan under a new
        // id, and the answer must still be yes.
        let tmp = tempfile::tempdir().unwrap();
        let occ = occurrence_id("accounts", "2020-01-01", Some("rel-1"), None);
        let mut first = plan(("2020-01-01", "2020-01-02"));
        first.id = "bf-first".into();
        first.partitions[0].occurrence = Some(occ.clone());
        first.partitions[0].state = State::Succeeded;
        first.partitions[0].run_id = Some("run-a".into());
        save(tmp.path(), &first).unwrap();

        let found = already_succeeded(tmp.path(), &occ);
        let (from, slice) = found.expect("an identical slice that succeeded was not found");
        assert_eq!(from, "bf-first");
        assert_eq!(slice.run_id.as_deref(), Some("run-a"));

        // A slice that only FAILED is not done, and must be retried rather
        // than skipped.
        let other = occurrence_id("accounts", "2020-01-02", Some("rel-1"), None);
        let mut second = plan(("2020-01-01", "2020-01-02"));
        second.id = "bf-second".into();
        second.partitions[0].occurrence = Some(other.clone());
        second.partitions[0].state = State::Failed;
        save(tmp.path(), &second).unwrap();
        assert!(already_succeeded(tmp.path(), &other).is_none());
    }

    #[test]
    fn a_plan_survives_being_written_and_read_back() {
        // Criterion 3: restarting the server must not lose the plan.
        let tmp = tempfile::tempdir().unwrap();
        let b = plan(("2020-01-01", "2020-01-05"));
        save(tmp.path(), &b).unwrap();
        let back = load(tmp.path(), &b.id).unwrap();
        assert_eq!(back, b);
        assert_eq!(back.partitions.len(), 5);
        assert_eq!(back.partitions[0].params.get("window_start").is_some(), true);
    }

    #[test]
    fn retrying_touches_only_the_failures() {
        // Criterion 4: a thousand days failing on four should cost four runs.
        let tmp = tempfile::tempdir().unwrap();
        let mut b = plan(("2020-01-01", "2020-01-05"));
        b.partitions[0].state = State::Succeeded;
        b.partitions[1].state = State::Failed;
        b.partitions[2].state = State::Succeeded;
        b.partitions[3].state = State::Interrupted;
        b.partitions[4].state = State::Requested;
        assert_eq!(b.retry_open(None), 2, "only the failed and interrupted ones");
        assert_eq!(b.partitions[0].state, State::Succeeded, "a success must not be redone");
        assert_eq!(b.partitions[1].state, State::Requested);
        assert_eq!(b.partitions[3].state, State::Requested);
        save(tmp.path(), &b).unwrap();
    }

    #[test]
    fn retrying_can_name_one_partition() {
        let mut b = plan(("2020-01-01", "2020-01-03"));
        for p in b.partitions.iter_mut() {
            p.state = State::Failed;
        }
        assert_eq!(b.retry_open(Some(&["2020-01-02".to_string()])), 1);
        assert_eq!(b.partitions[0].state, State::Failed);
        assert_eq!(b.partitions[1].state, State::Requested);
    }

    #[test]
    fn a_killed_backfill_is_interrupted_and_a_live_one_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let mut dead = plan(("2020-01-01", "2020-01-02"));
        dead.id = "bf-dead".into();
        dead.pid = Some(4242);
        dead.partitions[0].state = State::Running;
        save(tmp.path(), &dead).unwrap();

        let mut alive = plan(("2020-01-01", "2020-01-02"));
        alive.id = "bf-alive".into();
        alive.pid = Some(7);
        alive.partitions[0].state = State::Running;
        save(tmp.path(), &alive).unwrap();

        let changed = reconcile(tmp.path(), &|pid| pid == 7);
        assert_eq!(changed, vec!["bf-dead".to_string()]);
        assert_eq!(load(tmp.path(), "bf-dead").unwrap().partitions[0].state, State::Interrupted);
        assert_eq!(
            load(tmp.path(), "bf-alive").unwrap().partitions[0].state,
            State::Running,
            "a live backfill must not be reaped"
        );
    }

    #[test]
    fn cancelling_leaves_finished_work_alone() {
        let mut b = plan(("2020-01-01", "2020-01-04"));
        b.partitions[0].state = State::Succeeded;
        b.partitions[1].state = State::Failed;
        assert_eq!(b.cancel(), 3, "the failed one and the two still wanted");
        assert_eq!(b.partitions[0].state, State::Succeeded);
        assert!(b.is_done());
    }

    #[test]
    fn a_failed_slice_is_not_claimable_again_in_the_same_pass() {
        // An executor that claimed anything `is_open` picked its own failure
        // straight back up and retried it forever - a five-day backfill with
        // one missing file never terminated.
        assert!(State::Requested.is_claimable());
        for s in [State::Failed, State::Interrupted, State::Running, State::Succeeded, State::Cancelled] {
            assert!(!s.is_claimable(), "{s:?} must not be re-claimed mid-pass");
        }
        // But a failure is still open, so the backfill is not done and a retry
        // will pick it up.
        assert!(State::Failed.is_open());
        assert!(State::Interrupted.is_open());
    }

    #[test]
    fn a_backfill_is_done_only_when_nothing_is_still_open() {
        let mut b = plan(("2020-01-01", "2020-01-02"));
        assert!(!b.is_done());
        b.partitions[0].state = State::Succeeded;
        b.partitions[1].state = State::Failed;
        assert!(!b.is_done(), "a failure is still open until it is retried or cancelled");
        b.partitions[1].state = State::Succeeded;
        assert!(b.is_done());
        assert_eq!(b.counts().get("succeeded"), Some(&2));
    }

    /// A chunk slice writes a file, and the file is what "succeeded" means.
    #[test]
    fn a_part_is_durable_and_hashed_before_it_is_named() {
        let dir = std::env::temp_dir().join(format!("duckle-commit-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let tmp = dir.join("part-0.tmp");
        let out = dir.join("part-0.parquet");
        std::fs::write(&tmp, b"chunk one").unwrap();

        let a = commit(&tmp, &out, Some(9)).unwrap();

        assert!(!tmp.exists(), "the temp file is still there, so the move was a copy");
        assert!(out.exists(), "the part is not at its final path");
        assert_eq!(a.bytes, 9);
        assert_eq!(a.rows, Some(9));
        assert_eq!(a.hash, hash_file(&out).unwrap(), "the recorded hash is not the file's");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn one_chunk(uri: &str, hash: &str, bytes: u64) -> Backfill {
        let mut b = plan(("2020-01-01", "2020-01-01"));
        b.kind = Kind::Chunk;
        b.partitions[0].state = State::Succeeded;
        b.partitions[0].predicate = Some("id >= 0 AND id < 10".into());
        b.partitions[0].artifact = Some(SliceArtifact {
            uri: uri.into(),
            hash: hash.into(),
            bytes,
            rows: None,
        });
        b
    }

    /// The failure resumability exists to prevent: a slice marked done whose
    /// output is not there, which a retry would skip.
    #[test]
    fn a_succeeded_slice_whose_output_vanished_is_redone() {
        let mut b = one_chunk("/no/such/part.parquet", "deadbeef", 9);
        let reset = b.recheck_artifacts(false);
        assert_eq!(reset.len(), 1, "a missing part was left as succeeded");
        assert_eq!(b.partitions[0].state, State::Requested);
        assert!(b.partitions[0].artifact.is_none(), "a gone artifact is still recorded");
    }

    /// Length alone catches the truncation case, without reading the file.
    #[test]
    fn a_short_output_is_redone_without_reading_it() {
        let dir = std::env::temp_dir().join(format!("duckle-short-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let f = dir.join("part.parquet");
        std::fs::write(&f, b"tiny").unwrap();
        let mut b = one_chunk(&f.display().to_string(), &hash_file(&f).unwrap(), 999);
        assert_eq!(b.recheck_artifacts(false).len(), 1, "a short part was left as succeeded");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// And the case only a re-read can see: same length, different bytes.
    #[test]
    fn an_edited_output_is_caught_only_by_the_deep_check() {
        let dir = std::env::temp_dir().join(format!("duckle-edited-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let f = dir.join("part.parquet");
        std::fs::write(&f, b"aaaa").unwrap();
        let recorded = hash_file(&f).unwrap();
        std::fs::write(&f, b"bbbb").unwrap();

        let mut shallow = one_chunk(&f.display().to_string(), &recorded, 4);
        assert!(
            shallow.recheck_artifacts(false).is_empty(),
            "the cheap check claimed to detect an edit it cannot see"
        );
        let mut deep = one_chunk(&f.display().to_string(), &recorded, 4);
        assert_eq!(deep.recheck_artifacts(true).len(), 1, "the deep check missed an edited part");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every ledger written before chunks existed must still load, and read as
    /// what it is rather than as a chunked extract with no chunks.
    #[test]
    fn a_ledger_written_before_chunks_existed_still_loads() {
        let old = serde_json::json!({
            "id": "bf-accounts-1",
            "pipeline": "accounts",
            "pipelinePath": "pipelines/accounts.json",
            "createdAt": "2026-08-01T00:00:00Z",
            "maxConcurrent": 4,
            "partitions": [{
                "key": "2020-01-01",
                "state": "succeeded",
                "attempts": 1,
                "params": { "window_start": "2020-01-01" }
            }]
        });
        let b: Backfill = serde_json::from_value(old).expect("an existing ledger no longer loads");
        assert_eq!(b.kind, Kind::Partition);
        assert_eq!(b.partitions[0].state, State::Succeeded);
        assert!(b.partitions[0].predicate.is_none());
        // And a partition slice has no artifact, so the rule that a slice is
        // only succeeded once its output is committed must not demote it.
        let mut b = b;
        assert!(
            b.recheck_artifacts(true).is_empty(),
            "a partition slice was reset for not having a chunk's artifact"
        );
    }

}
