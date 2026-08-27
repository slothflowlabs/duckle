//! Work handed out as a file, so more than one machine can get through it.
//!
//! `ctl.foreach` normally runs its per-row children inside the process that
//! reached the node. That is bounded by one machine no matter how many rows
//! there are, and it loses everything if that machine dies half way.
//!
//! With `dispatch: "queue"` the rows are written here instead, one JSON object
//! per line, and the node returns. The file is then the work: any number of
//! `duckle-runner` processes can read it, claim an item apiece through the
//! existing run lock, and run it. Nothing needs a queue server, a database or a
//! network service - a batch is a file in the workspace, which is the same
//! thing every other piece of Duckle state already is.
//!
//! # Why NDJSON, and why a version on every line
//!
//! One object per line means a worker can stream a batch of 400,000 items
//! without holding it in memory, and a half-written last line is discardable
//! rather than fatal - the failure mode of a single JSON array. `v` is on every
//! line rather than in a header because a worker may start reading at any
//! offset, and a line that cannot say what it is cannot be safely skipped.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::EngineError;

/// One unit of work: one row of the driving query, and the child to run for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    /// Format version of THIS line. See the module note.
    pub v: u32,
    /// The batch this line belongs to, repeated per line so a line stays
    /// meaningful when it is copied out of the file into a log or a message.
    pub batch: String,
    /// Position in the driving query. Ordering information, never identity:
    /// see `item`.
    pub index: usize,
    /// What this item IS, from `ctl.foreach`'s item key column. This is what
    /// makes the run name and therefore the watermark, so it is the field that
    /// decides whether two items share state. Absent when no item key was set,
    /// in which case every item of the batch is the same named run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<String>,
    /// The child pipeline reference, exactly as authored on the node.
    pub child: String,
    /// The `${ITER_*}` substitutions for this row.
    pub vars: std::collections::BTreeMap<String, String>,
    /// How often to retry this item, and how long to wait between tries.
    ///
    /// Carried per line rather than in a batch header for the reason the rest
    /// of this line is: a line copied out of the file into a log or a message
    /// still says everything about the item, including how many tries it gets.
    /// Absent means unlimited, which is what every batch written before this
    /// existed meant, so old batches keep their behaviour exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
}

/// When a failed item should be tried again, and when to stop trying.
///
/// A permanently bad item - a 404 that will always be a 404, a document no
/// parser here can read - otherwise stays claimable forever and takes a worker
/// slot on every pass. That is the whole problem this solves: not making
/// failures succeed, but putting a bound on how long they are chased.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    /// Tries in total, the first attempt included. 0 means unlimited.
    #[serde(default)]
    pub max_attempts: u32,
    /// "fixed" or "exponential". Anything else is read as fixed rather than
    /// rejected: a policy from a newer build must not stop the batch.
    #[serde(default = "default_backoff")]
    pub backoff: String,
    /// The wait after the first failure. Also the whole wait when fixed.
    #[serde(default)]
    pub initial_seconds: u64,
    /// A ceiling on the exponential wait, so it does not run away to days.
    #[serde(default)]
    pub max_seconds: u64,
}

fn default_backoff() -> String {
    "fixed".to_string()
}

impl RetryPolicy {
    /// How long to wait after `attempts` failed tries.
    pub fn delay_seconds(&self, attempts: u32) -> u64 {
        if self.initial_seconds == 0 || attempts == 0 {
            return 0;
        }
        if self.backoff != "exponential" {
            return self.initial_seconds;
        }
        // Doubling, capped. The shift is bounded first: 1u64 << 64 is undefined
        // and 30 doublings is already a third of a year, so anything past it is
        // the ceiling by definition.
        let steps = (attempts - 1).min(30);
        let grown = self.initial_seconds.saturating_mul(1u64 << steps);
        let cap = if self.max_seconds == 0 { u64::MAX } else { self.max_seconds };
        grown.min(cap)
    }
}

pub fn batches_dir(workspace: &Path) -> PathBuf {
    workspace.join("batches")
}

pub fn batch_path(workspace: &Path, batch_id: &str) -> PathBuf {
    batches_dir(workspace).join(format!("{batch_id}.ndjson"))
}

/// A batch id that is unique per dispatch and still says what it came from.
///
/// The node id leads so a directory listing groups a node's batches together;
/// the timestamp makes two dispatches of the same node distinct. Milliseconds
/// because a fast pipeline can dispatch the same node twice in one second.
pub fn new_batch_id(node_id: &str, at: chrono::DateTime<chrono::Utc>) -> String {
    let node: String = node_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("{}-{}", node, at.format("%Y%m%dT%H%M%S%3f"))
}

/// Write a batch, and return where it went.
///
/// Written to a temp name and renamed, like every other store here: a worker
/// scanning the folder must never see a batch that is still being written and
/// conclude the work is smaller than it is.
pub fn write(workspace: &Path, batch_id: &str, items: &[WorkItem]) -> Result<PathBuf, EngineError> {
    let dir = batches_dir(workspace);
    std::fs::create_dir_all(&dir).map_err(|e| {
        EngineError::Config(format!("batch: cannot create {}: {}", dir.display(), e))
    })?;
    let final_path = batch_path(workspace, batch_id);
    let tmp = dir.join(format!("{batch_id}.{}.ndjson.tmp", std::process::id()));

    let mut out = String::new();
    for item in items {
        let line = serde_json::to_string(item)
            .map_err(|e| EngineError::Config(format!("batch: encode item: {e}")))?;
        out.push_str(&line);
        out.push('\n');
    }
    {
        let mut f = std::fs::File::create(&tmp).map_err(|e| {
            EngineError::Config(format!("batch: cannot write {}: {}", tmp.display(), e))
        })?;
        f.write_all(out.as_bytes())
            .map_err(|e| EngineError::Config(format!("batch: cannot write {}: {}", tmp.display(), e)))?;
    }
    if let Err(e) = std::fs::rename(&tmp, &final_path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(EngineError::Config(format!(
            "batch: cannot place {}: {}",
            final_path.display(),
            e
        )));
    }
    Ok(final_path)
}

/// Read a batch back.
///
/// A line that will not parse is skipped and counted rather than failing the
/// whole read: a batch is appended to by a crashing process's last write, and
/// losing 400,000 good items to one torn line would be the worse outcome. A
/// line whose `v` this build does not know is skipped the same way, because
/// guessing at a format from the future is how corruption gets executed.
pub fn read(path: &Path) -> Result<(Vec<WorkItem>, usize), EngineError> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        EngineError::Config(format!("batch: cannot read {}: {}", path.display(), e))
    })?;
    let mut items = Vec::new();
    let mut skipped = 0usize;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<WorkItem>(line) {
            Ok(item) if item.v == 1 => items.push(item),
            _ => skipped += 1,
        }
    }
    Ok((items, skipped))
}

/// One recorded attempt, appended by a worker after the fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerLine {
    pub v: u32,
    pub index: usize,
    /// "ok" or "error".
    pub status: String,
    pub at: String,
    /// Which worker ran it, for reading a ledger after the fact.
    pub worker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn ledger_path(workspace: &Path, batch_id: &str) -> PathBuf {
    batches_dir(workspace).join(format!("{batch_id}.ledger.ndjson"))
}

/// Every readable ledger line for a batch, oldest first.
///
/// Lives here rather than in the worker because the console reads the same
/// file to show progress. Two readers of one format is how a format drifts.
pub fn ledger(workspace: &Path, batch_id: &str) -> Vec<LedgerLine> {
    let Ok(text) = std::fs::read_to_string(ledger_path(workspace, batch_id)) else {
        return Vec::new();
    };
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<LedgerLine>(l).ok())
        .filter(|l| l.v == 1)
        .collect()
}

/// Which items are finished.
///
/// Only successes count. Treating a failure as done would let one transient
/// network error permanently consume an item, so a failed item stays claimable
/// and its failure stays in the ledger to look at.
pub fn finished(workspace: &Path, batch_id: &str) -> std::collections::HashSet<usize> {
    ledger(workspace, batch_id)
        .into_iter()
        .filter(|l| l.status == "ok")
        .map(|l| l.index)
        .collect()
}

/// Where one item stands right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Succeeded. Never claimed again.
    Done,
    /// Claimable now.
    Ready,
    /// Failed, and its backoff has not elapsed yet.
    Waiting,
    /// Out of attempts. Not claimed again until someone resets it, which is
    /// the point: a permanently bad item stops taking a worker slot on every
    /// pass, and stays visible instead of disappearing.
    Dead,
}

/// One item's history, reduced to what decides whether to run it now.
///
/// Derived from the ledger rather than stored, because the ledger already
/// records every attempt and a second store would be a second truth. The
/// engine, the CLI and the console all call this, so what a worker skips and
/// what an operator is shown cannot disagree.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemState {
    pub index: usize,
    pub item: Option<String>,
    pub phase: Phase,
    /// Tries since the last manual reset. A reset does not erase history, so
    /// this can be lower than the number of failures in the file.
    pub attempts: u32,
    pub last_attempt_at: Option<String>,
    /// When the backoff elapses. None when there is nothing to wait for.
    pub next_attempt_at: Option<String>,
    pub last_error: Option<String>,
}

/// A ledger line that resets an item's attempt count without deleting what
/// happened to it.
///
/// Appending "start counting again from here" keeps the failures readable,
/// which rewriting the file to drop them does not. An operator retrying a dead
/// item wants to know it died four times before, and a support question a month
/// later needs the errors, not a clean slate.
pub const RETRY_MARKER: &str = "retry";

/// Every item of a batch, with the state the ledger puts it in.
pub fn item_states(
    workspace: &Path,
    batch_id: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<ItemState> {
    let (items, _) = read(&batch_path(workspace, batch_id)).unwrap_or_default();
    let lines = ledger(workspace, batch_id);
    items
        .iter()
        .map(|it| item_state(it, &lines, now))
        .collect()
}

fn item_state(
    it: &WorkItem,
    lines: &[LedgerLine],
    now: chrono::DateTime<chrono::Utc>,
) -> ItemState {
    let mine: Vec<&LedgerLine> = lines.iter().filter(|l| l.index == it.index).collect();
    // Only what happened AFTER the most recent reset counts towards attempts.
    let from = mine
        .iter()
        .rposition(|l| l.status == RETRY_MARKER)
        .map(|i| i + 1)
        .unwrap_or(0);
    let since = &mine[from..];

    if since.iter().any(|l| l.status == "ok") {
        return ItemState {
            index: it.index,
            item: it.item.clone(),
            phase: Phase::Done,
            attempts: since.iter().filter(|l| l.status != RETRY_MARKER).count() as u32,
            last_attempt_at: since.last().map(|l| l.at.clone()),
            next_attempt_at: None,
            last_error: None,
        };
    }

    let failures: Vec<&&LedgerLine> = since.iter().filter(|l| l.status != RETRY_MARKER).collect();
    let attempts = failures.len() as u32;
    let last = failures.last().copied();
    let policy = it.retry.as_ref();
    let out_of_tries = policy
        .map(|p| p.max_attempts > 0 && attempts >= p.max_attempts)
        .unwrap_or(false);

    // The wait runs from the last attempt, so a worker that has been down for
    // an hour finds the backlog ready rather than waiting an hour more.
    let next = match (last, policy) {
        (Some(l), Some(p)) if !out_of_tries && p.delay_seconds(attempts) > 0 => {
            chrono::DateTime::parse_from_rfc3339(&l.at)
                .ok()
                .map(|t| t.with_timezone(&chrono::Utc)
                    + chrono::Duration::seconds(p.delay_seconds(attempts) as i64))
        }
        _ => None,
    };

    let phase = if out_of_tries {
        Phase::Dead
    } else if next.map(|t| t > now).unwrap_or(false) {
        Phase::Waiting
    } else {
        Phase::Ready
    };

    ItemState {
        index: it.index,
        item: it.item.clone(),
        phase,
        attempts,
        last_attempt_at: last.map(|l| l.at.clone()),
        next_attempt_at: next.map(|t| t.to_rfc3339()),
        last_error: last.and_then(|l| l.error.clone()),
    }
}

/// Start an item's attempt count again, keeping its history.
///
/// `only_dead` retries just the items that ran out of attempts, which is the
/// common case after fixing whatever made them fail. Returns how many were
/// reset.
pub fn reset_attempts(
    workspace: &Path,
    batch_id: &str,
    only_dead: bool,
    worker: &str,
) -> Result<usize, EngineError> {
    let _guard = crate::runlock::lock_store(workspace, &format!("ledger-{batch_id}"))
        .map_err(EngineError::Config)?;
    let now = chrono::Utc::now();
    let targets: Vec<usize> = item_states(workspace, batch_id, now)
        .into_iter()
        .filter(|s| match s.phase {
            Phase::Dead => true,
            Phase::Waiting | Phase::Ready => !only_dead && s.attempts > 0,
            Phase::Done => false,
        })
        .map(|s| s.index)
        .collect();
    if targets.is_empty() {
        return Ok(0);
    }
    let p = ledger_path(workspace, batch_id);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| EngineError::Config(e.to_string()))?;
    }
    let mut out = String::new();
    for index in &targets {
        let line = LedgerLine {
            v: 1,
            index: *index,
            status: RETRY_MARKER.into(),
            at: now.to_rfc3339(),
            worker: worker.to_string(),
            error: None,
        };
        out.push_str(
            &serde_json::to_string(&line).map_err(|e| EngineError::Config(e.to_string()))?,
        );
        out.push('\n');
    }
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| EngineError::Config(format!("{}: {e}", p.display())))?;
    f.write_all(out.as_bytes())
        .map_err(|e| EngineError::Config(format!("{}: {e}", p.display())))?;
    Ok(targets.len())
}

/// A batch as an operator needs to see it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchStatus {
    pub id: String,
    pub items: usize,
    pub done: usize,
    /// Items whose most recent attempt failed and which are still to be retried.
    pub failed: usize,
    /// Failed items still inside their backoff, so no worker will take them yet.
    /// Counted out of `failed` would hide them; they are a subset of it.
    pub waiting: usize,
    /// Items that used up their attempts. Nothing will claim these again until
    /// someone resets them, which is why they are reported separately from
    /// `failed` - "12 failed" reads as work in progress, "12 dead" does not.
    pub dead: usize,
    pub pending: usize,
    /// Items being run right now, counted by asking the run lock rather than by
    /// trusting a heartbeat: a worker that died is not "running", and there is
    /// no lease to have gone stale.
    pub running: usize,
    /// RFC3339 of the newest ledger line, or None when nothing has run.
    pub last_activity: Option<String>,
    /// Lines that could not be read, so a partial view never reads as complete.
    pub unreadable: usize,
}

/// Summarise every batch in the workspace, newest id last.
pub fn statuses(workspace: &Path) -> Vec<BatchStatus> {
    let dir = batches_dir(workspace);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut ids: Vec<String> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("ndjson"))
        .filter(|p| !p.to_string_lossy().contains(".ledger."))
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .collect();
    ids.sort();
    ids.into_iter().map(|id| status(workspace, &id)).collect()
}

/// Summarise one batch.
pub fn status(workspace: &Path, batch_id: &str) -> BatchStatus {
    let (items, unreadable) = read(&batch_path(workspace, batch_id)).unwrap_or_default();
    let lines = ledger(workspace, batch_id);
    let done: std::collections::HashSet<usize> =
        lines.iter().filter(|l| l.status == "ok").map(|l| l.index).collect();
    // A failure only counts while the item has not since succeeded.
    let failed: std::collections::HashSet<usize> = lines
        .iter()
        .filter(|l| l.status != "ok")
        .map(|l| l.index)
        .filter(|i| !done.contains(i))
        .collect();
    let running = items
        .iter()
        .filter(|i| !done.contains(&i.index))
        .filter(|i| {
            // Asking the lock IS the liveness check: if it is held, some live
            // process has it, and if that process died the kernel already let
            // it go. Taking it here and dropping it immediately is safe
            // because a worker re-checks the ledger after it claims.
            let key = format!("{}-{}", batch_id, i.index);
            crate::runlock::try_acquire_nested(workspace, "batch", &key).is_none()
        })
        .count();
    let last_activity = lines.iter().map(|l| l.at.clone()).max();
    let states = item_states(workspace, batch_id, chrono::Utc::now());
    BatchStatus {
        id: batch_id.to_string(),
        items: items.len(),
        done: done.len(),
        failed: failed.len(),
        waiting: states.iter().filter(|s| s.phase == Phase::Waiting).count(),
        dead: states.iter().filter(|s| s.phase == Phase::Dead).count(),
        pending: items.len().saturating_sub(done.len()),
        running,
        last_activity,
        unreadable,
    }
}

/// Forget the recorded failures for a batch, so its unfinished items are tried
/// again. Successes are kept, so a redrive never re-runs work that is done.
///
/// Rewrites the ledger rather than appending, because "this failure no longer
/// counts" cannot be expressed by adding a line.
pub fn redrive(workspace: &Path, batch_id: &str) -> Result<usize, EngineError> {
    let _guard = crate::runlock::lock_store(workspace, &format!("ledger-{batch_id}"))
        .map_err(EngineError::Config)?;
    let lines = ledger(workspace, batch_id);
    let kept: Vec<&LedgerLine> = lines.iter().filter(|l| l.status == "ok").collect();
    let dropped = lines.len() - kept.len();
    if dropped == 0 {
        return Ok(0);
    }
    let mut out = String::new();
    for l in kept {
        out.push_str(&serde_json::to_string(l).map_err(|e| EngineError::Config(e.to_string()))?);
        out.push('\n');
    }
    let p = ledger_path(workspace, batch_id);
    let tmp = p.with_extension(format!("ndjson.{}.tmp", std::process::id()));
    std::fs::write(&tmp, out)
        .map_err(|e| EngineError::Config(format!("redrive: {}: {e}", tmp.display())))?;
    if let Err(e) = std::fs::rename(&tmp, &p) {
        let _ = std::fs::remove_file(&tmp);
        return Err(EngineError::Config(format!("redrive: {}: {e}", p.display())));
    }
    Ok(dropped)
}

/// What a batch will write, and whether its items can safely run at once.
///
/// A queued batch is about to be spread across workers. That is only safe when
/// the items write to DIFFERENT places: 400 items each loading their own table
/// is exactly what this is for, and 400 items all appending to one file is a
/// pile-up that a queue turns from slow into wrong. The difference is invisible
/// on the canvas, because both look like one sink node with a variable in it.
///
/// So the targets are worked out before the batch is handed to anyone, by
/// substituting each item's variables into the child and asking the workspace
/// catalog what the resulting nodes name - the same function that builds the
/// asset graph, so the answer agrees with everything else in the product.
#[derive(Debug, Default)]
pub struct BatchSafety {
    /// Items whose write targets nothing else in the batch writes.
    pub disjoint: usize,
    /// Write target -> how many items write it, for targets shared by more
    /// than one item. These are the collisions.
    pub shared: std::collections::BTreeMap<String, usize>,
    /// Items whose child could not be read or named, so nothing is claimed
    /// about them either way.
    pub unknown: usize,
}

impl BatchSafety {
    /// The line to show. `None` when there is nothing worth saying.
    pub fn note(&self) -> Option<String> {
        if self.shared.is_empty() && self.unknown == 0 {
            return None;
        }
        let mut parts = Vec::new();
        if !self.shared.is_empty() {
            let worst = self
                .shared
                .iter()
                .max_by_key(|(_, n)| **n)
                .map(|(k, n)| format!("{n} items write {k}"))
                .unwrap_or_default();
            parts.push(format!(
                "{} target(s) are written by more than one item ({}). Workers run items at the \
                 same time, so these will collide unless the sink is an upsert or the target is \
                 append-safe",
                self.shared.len(),
                worst
            ));
        }
        if self.unknown > 0 {
            parts.push(format!(
                "{} item(s) could not be checked, so this is not a clean bill of health",
                self.unknown
            ));
        }
        Some(parts.join("; "))
    }
}

/// Work out what each item writes, without running anything.
///
/// `read_child` is how a child reference becomes its raw JSON, injected so this
/// is testable without a workspace on disk.
pub fn inspect<F>(items: &[WorkItem], mut read_child: F) -> BatchSafety
where
    F: FnMut(&str) -> Option<String>,
{
    let mut writes_by_item: Vec<Vec<String>> = Vec::with_capacity(items.len());
    let mut safety = BatchSafety::default();

    for item in items {
        let Some(raw) = read_child(&item.child) else {
            safety.unknown += 1;
            writes_by_item.push(Vec::new());
            continue;
        };
        let subs: std::collections::HashMap<String, String> =
            item.vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let resolved = crate::connectors::substitute_into_child(&raw, &subs);
        let Ok(doc) = serde_json::from_str::<serde_json::Value>(&resolved) else {
            safety.unknown += 1;
            writes_by_item.push(Vec::new());
            continue;
        };
        let mut writes = Vec::new();
        let mut named_any = false;
        for node in doc.get("nodes").and_then(|n| n.as_array()).into_iter().flatten() {
            let Some(cid) = node.pointer("/data/componentId").and_then(|v| v.as_str()) else {
                continue;
            };
            if !cid.starts_with("snk.") {
                continue;
            }
            let props = node
                .pointer("/data/properties")
                .cloned()
                .unwrap_or(serde_json::Value::Object(Default::default()));
            if let Ok(asset) = crate::catalog::asset_of(cid, &props) {
                named_any = true;
                writes.push(asset.id);
            }
        }
        // A child with sinks none of which could be named tells us nothing.
        if !named_any {
            safety.unknown += 1;
        }
        writes_by_item.push(writes);
    }

    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for writes in &writes_by_item {
        // Count an item ONCE per target, so a child writing the same table from
        // two nodes is not mistaken for two items colliding.
        let mut seen = std::collections::BTreeSet::new();
        for w in writes {
            if seen.insert(w.clone()) {
                *counts.entry(w.clone()).or_default() += 1;
            }
        }
    }
    safety.shared = counts.iter().filter(|(_, n)| **n > 1).map(|(k, n)| (k.clone(), *n)).collect();
    safety.disjoint = writes_by_item
        .iter()
        .filter(|writes| !writes.is_empty() && writes.iter().all(|w| counts.get(w) == Some(&1)))
        .count();
    safety
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(i: usize, name: &str) -> WorkItem {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("ITER_INDEX".to_string(), i.to_string());
        vars.insert("ITER_ITEM_TABLE_NAME".to_string(), name.to_string());
        WorkItem {
            v: 1,
            batch: "n1-20260816T101112123".into(),
            index: i,
            item: Some(name.into()),
            child: "pipelines/sync-one-table.json".into(),
            vars,
            retry: None,
        }
    }

    #[test]
    fn a_batch_round_trips_one_line_per_item() {
        let tmp = tempfile::tempdir().unwrap();
        let items = vec![item(0, "orders"), item(1, "customers")];
        let path = write(tmp.path(), "n1-20260816T101112123", &items).unwrap();

        // One line per item, so a worker can stream rather than load.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert_eq!(raw.lines().count(), 2);
        assert!(raw.lines().all(|l| l.contains("\"v\":1")), "every line must carry its version");

        let (back, skipped) = read(&path).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(back.len(), 2);
        assert_eq!(back[1].item.as_deref(), Some("customers"));
        assert_eq!(back[1].vars["ITER_ITEM_TABLE_NAME"], "customers");
    }

    /// A torn last line must cost one item, not the whole batch.
    #[test]
    fn a_damaged_line_is_skipped_and_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(tmp.path(), "b", &[item(0, "orders"), item(1, "customers")]).unwrap();
        let mut raw = std::fs::read_to_string(&path).unwrap();
        raw.push_str("{\"v\":1,\"batch\":\"b\",\"index\":2,\"chi");
        std::fs::write(&path, raw).unwrap();

        let (back, skipped) = read(&path).unwrap();
        assert_eq!(back.len(), 2, "the intact items must survive a torn line");
        assert_eq!(skipped, 1);
    }

    /// A line from a future format is skipped, not guessed at.
    #[test]
    fn an_unknown_version_is_not_executed() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(tmp.path(), "b", &[item(0, "orders")]).unwrap();
        let mut raw = std::fs::read_to_string(&path).unwrap();
        let mut future = item(1, "customers");
        future.v = 99;
        raw.push_str(&serde_json::to_string(&future).unwrap());
        raw.push('\n');
        std::fs::write(&path, raw).unwrap();

        let (back, skipped) = read(&path).unwrap();
        assert_eq!(back.len(), 1, "a v99 line must not be run by a v1 worker");
        assert_eq!(skipped, 1);
    }

    fn child_writing(path_expr: &str) -> String {
        serde_json::json!({
            "name": "load-one",
            "nodes": [
                {"id":"s","data":{"componentId":"src.csv","properties":{"path":"/in.csv"}}},
                {"id":"k","data":{"componentId":"snk.parquet","properties":{"path":path_expr}}}
            ],
            "edges": []
        })
        .to_string()
    }

    /// Items that each write their own target are safe to spread over workers.
    #[test]
    fn a_batch_whose_items_write_different_targets_is_reported_disjoint() {
        let items = vec![item(0, "orders"), item(1, "customers")];
        let safety = inspect(&items, |_| Some(child_writing("/lake/${ITER_ITEM_TABLE_NAME}.parquet")));
        assert_eq!(safety.disjoint, 2);
        assert!(safety.shared.is_empty(), "{:?}", safety.shared);
        assert_eq!(safety.unknown, 0);
        assert!(safety.note().is_none(), "nothing to warn about");
    }

    /// Items that all write ONE target will collide once workers run them at
    /// the same time, and that is invisible on the canvas: it is the same sink
    /// node either way, just without the variable in the path.
    #[test]
    fn a_batch_whose_items_share_a_target_is_reported_as_a_collision() {
        let items = vec![item(0, "orders"), item(1, "customers"), item(2, "invoices")];
        let safety = inspect(&items, |_| Some(child_writing("/lake/everything.parquet")));
        assert_eq!(safety.disjoint, 0);
        assert_eq!(safety.shared.len(), 1);
        assert_eq!(safety.shared.values().next(), Some(&3));
        let note = safety.note().expect("a collision must be reported");
        assert!(note.contains("3 items write"), "{note}");
        assert!(note.contains("collide"), "{note}");
    }

    /// A child that cannot be read is counted, not silently treated as safe.
    #[test]
    fn items_that_cannot_be_checked_are_not_called_safe() {
        let items = vec![item(0, "orders"), item(1, "customers")];
        let safety = inspect(&items, |_| None);
        assert_eq!(safety.unknown, 2);
        assert_eq!(safety.disjoint, 0);
        let note = safety.note().expect("an unchecked batch must say so");
        assert!(note.contains("not a clean bill of health"), "{note}");
    }

    /// One child writing the same table twice is not two items colliding.
    #[test]
    fn a_child_with_two_nodes_writing_one_table_is_not_a_collision() {
        let doc = serde_json::json!({
            "name": "load-one",
            "nodes": [
                {"id":"a","data":{"componentId":"snk.parquet","properties":{"path":"/lake/${ITER_ITEM_TABLE_NAME}.parquet"}}},
                {"id":"b","data":{"componentId":"snk.parquet","properties":{"path":"/lake/${ITER_ITEM_TABLE_NAME}.parquet"}}}
            ],
            "edges": []
        })
        .to_string();
        let items = vec![item(0, "orders"), item(1, "customers")];
        let safety = inspect(&items, |_| Some(doc.clone()));
        assert!(safety.shared.is_empty(), "one item's own two sinks were counted as a clash: {:?}", safety.shared);
        assert_eq!(safety.disjoint, 2);
    }

    /// Two dispatches of one node are two batches.
    #[test]
    fn a_batch_id_is_unique_per_dispatch_and_names_its_node() {
        use chrono::TimeZone;
        let t1 = chrono::Utc.with_ymd_and_hms(2026, 8, 16, 10, 11, 12).unwrap();
        let a = new_batch_id("foreach-1", t1);
        let b = new_batch_id("foreach-1", t1 + chrono::Duration::milliseconds(7));
        assert_ne!(a, b, "two dispatches in the same second collided");
        assert!(a.starts_with("foreach-1-"), "{a}");
        // A node id that would escape the folder cannot.
        assert!(!new_batch_id("../../etc/passwd", t1).contains('/'));
    }

    /// A worker must never see a half-written batch.
    #[test]
    fn a_batch_appears_whole_or_not_at_all() {
        let tmp = tempfile::tempdir().unwrap();
        let items: Vec<WorkItem> = (0..500).map(|i| item(i, &format!("t{i}"))).collect();
        let path = write(tmp.path(), "big", &items).unwrap();
        // Nothing is left behind mid-write, and the only file present is complete.
        let stray: Vec<_> = std::fs::read_dir(batches_dir(tmp.path()))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
            .collect();
        assert!(stray.is_empty(), "a temp batch file was left in the folder");
        assert_eq!(read(&path).unwrap().0.len(), 500);
    }

    // -----------------------------------------------------------------------
    // #277 - retry policy, backoff and dead-letter state for queued work.
    // -----------------------------------------------------------------------

    fn with_policy(mut it: WorkItem, p: RetryPolicy) -> WorkItem {
        it.retry = Some(p);
        it
    }

    fn attempt(index: usize, status: &str, at: &str, error: Option<&str>) -> LedgerLine {
        LedgerLine {
            v: 1,
            index,
            status: status.into(),
            at: at.into(),
            worker: "w".into(),
            error: error.map(str::to_string),
        }
    }

    fn t(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&chrono::Utc)
    }

    /// The whole point: an item that will never succeed has to STOP being
    /// claimed. Without a bound it is retried on every worker pass forever and
    /// takes a slot from work that could finish.
    #[test]
    fn an_item_out_of_attempts_stops_being_claimable() {
        let it = with_policy(
            item(0, "orders"),
            RetryPolicy {
                max_attempts: 3,
                backoff: "fixed".into(),
                initial_seconds: 0,
                max_seconds: 0,
            },
        );
        let now = t("2026-08-27T12:00:00Z");
        let fail = |n: &str| attempt(0, "error", n, Some("permanent 404"));

        let two = vec![fail("2026-08-27T11:00:00Z"), fail("2026-08-27T11:10:00Z")];
        assert_eq!(item_state(&it, &two, now).phase, Phase::Ready, "2 of 3 tries used");

        let three = vec![
            fail("2026-08-27T11:00:00Z"),
            fail("2026-08-27T11:10:00Z"),
            fail("2026-08-27T11:20:00Z"),
        ];
        let st = item_state(&it, &three, now);
        assert_eq!(st.phase, Phase::Dead);
        assert_eq!(st.attempts, 3);
        assert_eq!(st.last_error.as_deref(), Some("permanent 404"));
    }

    /// No policy has to mean exactly what it meant before this existed, or
    /// every batch already on disk changes behaviour on upgrade.
    #[test]
    fn without_a_policy_an_item_is_retried_forever_as_before() {
        let it = item(0, "orders");
        let lines: Vec<LedgerLine> = (0..50)
            .map(|i| attempt(0, "error", &format!("2026-08-27T10:{:02}:00Z", i), Some("boom")))
            .collect();
        let st = item_state(&it, &lines, t("2026-08-27T12:00:00Z"));
        assert_eq!(st.phase, Phase::Ready, "50 failures and still claimable, as before");
        assert_eq!(st.attempts, 50);
    }

    /// A failure inside its backoff must not be handed to a worker yet, and
    /// must become claimable on its own once the wait elapses - no sweeper, no
    /// second process to run.
    #[test]
    fn a_failure_waits_out_its_backoff_then_becomes_claimable() {
        let it = with_policy(
            item(0, "orders"),
            RetryPolicy {
                max_attempts: 0,
                backoff: "fixed".into(),
                initial_seconds: 300,
                max_seconds: 0,
            },
        );
        let lines = vec![attempt(0, "error", "2026-08-27T12:00:00Z", Some("429"))];

        let st = item_state(&it, &lines, t("2026-08-27T12:04:00Z"));
        assert_eq!(st.phase, Phase::Waiting, "4 minutes into a 5 minute wait");
        assert_eq!(st.next_attempt_at.as_deref(), Some("2026-08-27T12:05:00+00:00"));

        assert_eq!(
            item_state(&it, &lines, t("2026-08-27T12:05:01Z")).phase,
            Phase::Ready,
            "the wait elapsed, so it is claimable again"
        );
    }

    /// Exponential backoff doubles and then stops at the ceiling. Getting the
    /// ceiling wrong is how a retry ends up days away and looks like a hang.
    #[test]
    fn exponential_backoff_doubles_up_to_the_ceiling() {
        let p = RetryPolicy {
            max_attempts: 0,
            backoff: "exponential".into(),
            initial_seconds: 30,
            max_seconds: 3600,
        };
        assert_eq!(p.delay_seconds(1), 30);
        assert_eq!(p.delay_seconds(2), 60);
        assert_eq!(p.delay_seconds(3), 120);
        assert_eq!(p.delay_seconds(8), 3600, "3840 would exceed the ceiling");
        assert_eq!(p.delay_seconds(60), 3600, "and it stays there rather than overflowing");
        assert_eq!(p.delay_seconds(0), 0, "nothing has failed yet");

        // Fixed ignores the doubling entirely.
        let fixed = RetryPolicy { backoff: "fixed".into(), ..p.clone() };
        assert_eq!(fixed.delay_seconds(5), 30);
        // A policy shape from a newer build is read as fixed rather than
        // rejected: refusing it would stop the batch over a spelling.
        let unknown = RetryPolicy { backoff: "fibonacci".into(), ..p };
        assert_eq!(unknown.delay_seconds(5), 30);
    }

    /// A manual retry has to reset the count WITHOUT deleting what happened.
    /// An operator retrying a dead item needs to know it died four times first.
    #[test]
    fn a_reset_starts_the_count_again_and_keeps_the_history() {
        let tmp = tempfile::tempdir().unwrap();
        let id = "b1";
        let it = with_policy(
            item(0, "orders"),
            RetryPolicy {
                max_attempts: 2,
                backoff: "fixed".into(),
                initial_seconds: 0,
                max_seconds: 0,
            },
        );
        write(tmp.path(), id, &[it]).unwrap();
        let p = ledger_path(tmp.path(), id);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let text: String = [
            attempt(0, "error", "2026-08-27T10:00:00Z", Some("first")),
            attempt(0, "error", "2026-08-27T10:05:00Z", Some("second")),
        ]
        .iter()
        .map(|l| serde_json::to_string(l).unwrap() + "\n")
        .collect();
        std::fs::write(&p, text).unwrap();

        let now = chrono::Utc::now();
        assert_eq!(item_states(tmp.path(), id, now)[0].phase, Phase::Dead);

        assert_eq!(reset_attempts(tmp.path(), id, true, "operator").unwrap(), 1);
        let after = &item_states(tmp.path(), id, now)[0];
        assert_eq!(after.phase, Phase::Ready, "it is claimable again");
        assert_eq!(after.attempts, 0, "the count starts over");

        // The failures are still readable - that is the difference between this
        // and rewriting the ledger to drop them.
        let raw = std::fs::read_to_string(&p).unwrap();
        assert!(raw.contains("first") && raw.contains("second"), "history was deleted: {raw}");

        // And a second reset with nothing dead does nothing rather than
        // stacking markers.
        assert_eq!(reset_attempts(tmp.path(), id, true, "operator").unwrap(), 0);
    }

    /// A success after failures ends the item. A done item that came back as
    /// claimable would be duplicated work at best.
    #[test]
    fn a_success_ends_an_item_even_after_failures() {
        let it = with_policy(
            item(0, "orders"),
            RetryPolicy {
                max_attempts: 2,
                backoff: "fixed".into(),
                initial_seconds: 600,
                max_seconds: 0,
            },
        );
        let lines = vec![
            attempt(0, "error", "2026-08-27T10:00:00Z", Some("transient")),
            attempt(0, "ok", "2026-08-27T10:05:00Z", None),
        ];
        let st = item_state(&it, &lines, t("2026-08-27T10:06:00Z"));
        assert_eq!(st.phase, Phase::Done);
        assert_eq!(st.next_attempt_at, None, "nothing is waiting for a done item");
    }

}
