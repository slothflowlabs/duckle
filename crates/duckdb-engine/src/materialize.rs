//! #325: a durable record of every successful publication.
//!
//! A pipeline finishing and committing its outputs is an EVENT, and nothing
//! recorded it. Freshness derived it by rescanning run history; a downstream
//! pipeline that wanted to run when its input was republished had nothing to
//! subscribe to at all.
//!
//! ## One record per successful run, not per asset
//!
//! Louis's point on the issue, and it settles a question rather than deferring
//! one: a run that commits four tables is ONE publication. Recording it per
//! asset would mean four events for one commit, and a subscriber would need a
//! debounce window and a timer to collapse them back into the thing that
//! actually happened. Per run, the coalescing is free and there is no window to
//! tune.
//!
//! ## Delivery may be best-effort. This may not.
//!
//! The two look alike and are not. A dropped alert loses a notification; a
//! dropped materialization event loses the WORK - downstream never runs, and
//! the producer will not publish again until its next cycle, so the loss can be
//! a whole day long with nothing saying so.
//!
//! ## It needs a catalog
//!
//! A publication is a publication OF something, and what a run wrote is known
//! only through the catalog - the record's asset list is a join of the run's
//! nodes against the catalog's touches. A workspace that has never built one
//! records runs with no assets and therefore no events. That is consistent,
//! since there is nothing to name, but the symptom is silence: the run
//! succeeds and the log stays empty.
//!
//! ## It is made recoverable rather than promised to be reliable. The run record
//! is written first and is the source of truth; an event is a fast index over
//! it. If the append fails - a full disk, a read-only mount - the record still
//! carries everything the event would have, and [`reconcile`] rebuilds what is
//! missing. So a failure costs latency, not the event.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::history::RunRecord;

/// One committed publication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// Stable identity of this publication.
    ///
    /// Deterministic, so rebuilding an event from its run record produces the
    /// same id rather than a second event for work that happened once. That is
    /// what makes [`reconcile`] safe to run repeatedly.
    pub event_id: String,
    pub pipeline_id: String,
    /// The run that committed it, so the receipt, the log and the lineage are
    /// all reachable from the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// #297: the code this was produced by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<String>,
    /// #295: which slice, when the run was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    /// What kicked the producer off.
    pub trigger: String,
    /// RFC3339 of the run that committed this.
    pub committed_at: String,
    /// The assets it WROTE. Reads are not a publication.
    pub assets: Vec<String>,
}

/// The identity of a publication.
///
/// Length-prefixed so two different splits cannot collide - the same trick
/// `backfill::occurrence_id` uses, for the same reason: joining fields with a
/// separator makes `a|bc` and `ab|c` the same string.
pub fn event_id(
    pipeline_id: &str,
    run_id: Option<&str>,
    committed_at: &str,
    release_id: Option<&str>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for part in [pipeline_id, run_id.unwrap_or(""), committed_at, release_id.unwrap_or("")] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    format!("mat-{}", &hex[..16])
}

pub fn log_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("materializations.ndjson")
}

/// Whether this run published anything.
///
/// The same predicate the catalog's freshness uses, reused rather than
/// restated: a run that failed did not publish, and neither did one that
/// stopped at a ceiling - its rows are correct and are not all of them, and
/// unlike a failure it looks healthy. Two definitions of "materialized" would
/// be one too many.
pub fn is_publication(record: &RunRecord) -> bool {
    record.status == "ok"
        && !record.incomplete
        && record.assets.iter().any(|a| a.direction == "write")
}

/// The event a run record represents, if it is a publication.
///
/// `workspace` is only used to pick up the release and partition from the run's
/// receipt: a run record does not carry them, and they are provenance rather
/// than identity - the id is pipeline + run + commit time, which is already
/// unique. Absent when there is no receipt, which is the case for a run started
/// before receipts existed.
pub fn event_of(workspace: Option<&Path>, pipeline_id: &str, record: &RunRecord) -> Option<Event> {
    if !is_publication(record) {
        return None;
    }
    let receipt = match (workspace, record.run_id.as_deref()) {
        (Some(ws), Some(id)) => crate::retry::load(ws, id).ok(),
        _ => None,
    };
    let release_id = receipt.as_ref().and_then(|r| r.release_id.clone());
    Some(Event {
        event_id: event_id(
            pipeline_id,
            record.run_id.as_deref(),
            &record.at,
            release_id.as_deref(),
        ),
        pipeline_id: pipeline_id.to_string(),
        run_id: record.run_id.clone(),
        release_id,
        partition_key: receipt.as_ref().and_then(|r| r.partition_key.clone()),
        trigger: record.trigger.clone(),
        committed_at: record.at.clone(),
        assets: record
            .assets
            .iter()
            .filter(|a| a.direction == "write")
            .map(|a| a.id.clone())
            .collect(),
    })
}

/// Append one event, if this run was a publication.
///
/// Append-only, the pattern the batch ledger and the listen log already use:
/// the writer never rewrites and a reader never deletes, so a concurrent read
/// cannot see a half-rewritten file. That also rules out run history as the
/// log - it trims to a maximum and rewrites wholesale, so a consumer could miss
/// an event simply because the producer was busy.
pub fn append(workspace: &Path, pipeline_id: &str, record: &RunRecord) -> Result<Option<Event>, String> {
    let Some(event) = event_of(Some(workspace), pipeline_id, record) else {
        return Ok(None);
    };
    let path = log_path(workspace);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let line = serde_json::to_string(&event).map_err(|e| e.to_string())?;
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    writeln!(f, "{line}").map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(Some(event))
}

/// Every event in the log, oldest first.
///
/// A line that will not parse is skipped rather than failing the read: the log
/// is append-only and a torn last line from a killed process must not make
/// every earlier event unreadable.
pub fn read(workspace: &Path) -> Vec<Event> {
    let Ok(text) = std::fs::read_to_string(log_path(workspace)) else {
        return Vec::new();
    };
    text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
}

/// Rebuild events that run history has and the log does not.
///
/// This is what makes losing an append survivable. The run record is written
/// first and carries everything the event does, so a failed append is a gap in
/// an index rather than a lost publication - and because the id is derived from
/// the record, rebuilding produces the same event rather than a duplicate.
///
/// Returns the events it added.
pub fn reconcile(workspace: &Path, pipelines: &[String]) -> Vec<Event> {
    // Recognised by WHAT THE PUBLICATION IS - pipeline, run, commit time - not
    // by `event_id`.
    //
    // #303: the id is hashed over those three AND the release, and the release
    // is read from the producer's receipt. Once retention prunes that receipt
    // the same publication hashes to a different id, so an id-keyed check does
    // not recognise the event already on disk, appends a second one, and every
    // subscription delivers the same publication twice. The three fields below
    // are all carried on the event itself, so this cannot drift with what
    // retention has removed.
    let known: std::collections::BTreeSet<(String, Option<String>, String)> = read(workspace)
        .into_iter()
        .map(|e| (e.pipeline_id, e.run_id, e.committed_at))
        .collect();
    let mut added = Vec::new();
    for pipeline in pipelines {
        for record in crate::history::load_run_history(workspace, pipeline) {
            let Some(event) = event_of(Some(workspace), pipeline, &record) else { continue };
            if known.contains(&(
                event.pipeline_id.clone(),
                event.run_id.clone(),
                event.committed_at.clone(),
            )) {
                continue;
            }
            if append(workspace, pipeline, &record).is_ok() {
                added.push(event);
            }
        }
    }
    added
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::AssetTouch;

    fn record(status: &str, incomplete: bool, writes: &[&str]) -> RunRecord {
        let mut r = RunRecord {
            run_id: Some("run-1".into()),
            at: "2026-09-03T10:00:00Z".into(),
            status: status.into(),
            duration_ms: 10,
            rows: 5,
            node_count: 2,
            trigger: "scheduled".into(),
            error: None,
            unchanged: false,
            incomplete,
            incomplete_reason: None,
            category: None,
            assets: Vec::new(),
        };
        r.assets = writes
            .iter()
            .map(|id| AssetTouch {
                id: (*id).to_string(),
                direction: "write".into(),
                rows: Some(5),
            })
            .collect();
        r
    }

    /// One publication, however many tables it committed. Per asset would mean
    /// four events for one commit and a subscriber needing a window to collapse
    /// them back into what happened.
    #[test]
    fn a_run_that_commits_four_tables_is_one_event() {
        let e = event_of(None, "nightly", &record("ok", false, &["/a", "/b", "/c", "/d"]))
            .expect("a publication");
        assert_eq!(e.assets.len(), 4);
        assert_eq!(e.pipeline_id, "nightly");
    }

    /// The predicate is the catalog's, so "materialized" means one thing.
    #[test]
    fn a_failed_or_partial_run_publishes_nothing() {
        assert!(event_of(None, "p", &record("error", false, &["/a"])).is_none());
        assert!(
            event_of(None, "p", &record("ok", true, &["/a"])).is_none(),
            "an incomplete run stopped at a ceiling: its rows are correct and are not all of them"
        );
        // And a run that only READ something is not a publication.
        let mut only_read = record("ok", false, &[]);
        only_read.assets = vec![AssetTouch {
            id: "/a".into(),
            direction: "read".into(),
            rows: Some(1),
        }];
        assert!(event_of(None, "p", &only_read).is_none());
    }

    /// Deterministic, which is what makes rebuilding safe to run repeatedly.
    #[test]
    fn the_same_publication_has_the_same_id() {
        let r = record("ok", false, &["/a"]);
        assert_eq!(event_of(None, "p", &r).unwrap().event_id, event_of(None, "p", &r).unwrap().event_id);
        // A different run of the same pipeline is a different publication.
        let mut later = r.clone();
        later.run_id = Some("run-2".into());
        assert_ne!(event_of(None, "p", &r).unwrap().event_id, event_of(None, "p", &later).unwrap().event_id);
    }

    #[test]
    fn appending_writes_one_line_per_publication() {
        let ws = tempfile::tempdir().unwrap();
        append(ws.path(), "p", &record("ok", false, &["/a"])).unwrap();
        append(ws.path(), "p", &record("error", false, &["/a"])).unwrap();
        let events = read(ws.path());
        assert_eq!(events.len(), 1, "a failed run was logged as a publication");
        assert_eq!(events[0].assets, vec!["/a".to_string()]);
    }

    /// A torn last line from a killed process must not make every earlier event
    /// unreadable - the whole point of append-only is that the past is safe.
    #[test]
    fn a_half_written_line_does_not_hide_the_rest() {
        let ws = tempfile::tempdir().unwrap();
        append(ws.path(), "p", &record("ok", false, &["/a"])).unwrap();
        let path = log_path(ws.path());
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str("{\"eventId\":\"mat-tru");
        std::fs::write(&path, text).unwrap();
        assert_eq!(read(ws.path()).len(), 1, "one torn line hid a complete event");
    }
}

#[cfg(test)]
mod emitted_once {
    use super::*;
    use crate::history::{append_run_record, AssetTouch};

    fn published(run: &str, asset: &str) -> RunRecord {
        RunRecord {
            run_id: Some(run.into()),
            at: format!("2026-09-03T10:00:0{}Z", run.len() % 10),
            status: "ok".into(),
            duration_ms: 10,
            rows: 5,
            node_count: 2,
            trigger: "scheduled".into(),
            error: None,
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            category: None,
            assets: vec![AssetTouch {
                id: asset.into(),
                direction: "write".into(),
                rows: Some(5),
            }],
        }
    }

    /// #325: recording the run and recording the publication must not be two
    /// things a caller has to remember. Four places append a record, and only
    /// three of them raise alerts - which is the standing evidence that a
    /// per-site emitter drifts.
    #[test]
    fn recording_a_run_records_its_publication() {
        let ws = tempfile::tempdir().unwrap();
        append_run_record(ws.path(), "nightly", published("run-1", "/lake/orders"))
            .unwrap();
        let events = read(ws.path());
        assert_eq!(events.len(), 1, "appending a run did not record its publication");
        assert_eq!(events[0].pipeline_id, "nightly");
        assert_eq!(events[0].assets, vec!["/lake/orders".to_string()]);
    }

    /// And rebuilding is idempotent, which is what makes a failed append
    /// survivable rather than a lost publication.
    #[test]
    fn reconciling_adds_what_is_missing_and_nothing_else() {
        let ws = tempfile::tempdir().unwrap();
        append_run_record(ws.path(), "nightly", published("run-1", "/lake/orders"))
            .unwrap();
        // Simulate the append having failed: the record is on disk, the log is
        // not. This is the exact state a full disk leaves behind.
        std::fs::remove_file(log_path(ws.path())).unwrap();
        assert!(read(ws.path()).is_empty());

        let added = reconcile(ws.path(), &["nightly".to_string()]);
        assert_eq!(added.len(), 1, "a publication that lost its event was not rebuilt");
        assert_eq!(read(ws.path()).len(), 1);

        // Running it again adds nothing: the id comes from the record, so the
        // same publication is the same event.
        assert!(reconcile(ws.path(), &["nightly".to_string()]).is_empty());
        assert_eq!(read(ws.path()).len(), 1, "reconciling twice duplicated an event");
    }
}

#[cfg(test)]
mod needs_a_catalog {
    use super::*;

    /// A publication is a publication OF something, and what a run wrote is
    /// only known through the catalog: `RunRecord::from_result_in` fills the
    /// asset list by joining the run's nodes against the catalog's touches, and
    /// returns the record untouched when there is no catalog to join against.
    ///
    /// So a workspace that has never built one records runs with no assets and
    /// therefore no events. That is consistent - there is nothing to name - but
    /// it is worth a test, because the symptom is silence: the run succeeds,
    /// the log stays empty, and nothing says why.
    #[test]
    fn a_run_with_no_named_assets_publishes_nothing() {
        let ws = tempfile::tempdir().unwrap();
        let no_assets = RunRecord {
            run_id: Some("run-1".into()),
            at: "2026-09-03T10:00:00Z".into(),
            status: "ok".into(),
            duration_ms: 10,
            rows: 5,
            node_count: 2,
            trigger: "manual".into(),
            error: None,
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            category: None,
            assets: Vec::new(),
        };
        assert!(
            event_of(None, "p", &no_assets).is_none(),
            "a run that named no asset was recorded as publishing one"
        );
        crate::history::append_run_record(ws.path(), "p", no_assets).unwrap();
        assert!(read(ws.path()).is_empty());
    }
}

/// #303 x #325: a pruned receipt must not change what a publication IS.
#[cfg(test)]
mod survives_a_pruned_receipt {
    use super::*;
    use crate::history::{append_run_record, AssetTouch};

    fn published(ws: &Path, run: &str, partition: Option<&str>) -> RunRecord {
        // The receipt is where an event gets its partition and release, so a
        // partitioned publication needs one.
        let mut receipt = crate::retry::begin(ws, run, "scheduled", "producer", "p.json", "h", None);
        receipt.partition_key = partition.map(str::to_string);
        receipt.release_id = Some("rel-9".into());
        crate::retry::write(ws, &receipt).unwrap();
        RunRecord {
            run_id: Some(run.into()),
            at: "2026-09-04T04:00:00Z".into(),
            status: "ok".into(),
            duration_ms: 10,
            rows: 5,
            node_count: 1,
            trigger: "scheduled".into(),
            error: None,
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            category: None,
            assets: vec![AssetTouch {
                id: "/data/a.parquet".into(),
                direction: "write".into(),
                rows: Some(5),
            }],
        }
    }

    /// Louis's regression, and it is sharper than "the partition goes missing".
    ///
    /// `event_id` is hashed over pipeline + run + time + RELEASE, and the
    /// release is read from the receipt. Prune the receipt and the same
    /// publication hashes to a DIFFERENT id - so reconcile does not recognise
    /// the event it already has, appends a second one, and every subscription
    /// delivers the same publication twice.
    #[test]
    fn a_pruned_receipt_does_not_duplicate_the_publication() {
        let ws = tempfile::tempdir().unwrap();
        let record = published(ws.path(), "run-7", Some("2026-09-04"));
        // `append_run_record` emits the event itself - appending again here
        // would be the test writing the duplicate it then blames the code for.
        append_run_record(ws.path(), "producer", record.clone()).unwrap();
        let before = read(ws.path());
        assert_eq!(before.len(), 1, "one publication, one event");
        assert_eq!(before[0].partition_key.as_deref(), Some("2026-09-04"));

        // Retention removes the producer's receipt.
        std::fs::remove_file(crate::retry::path_for_test(ws.path(), "run-7")).unwrap();

        let added = reconcile(ws.path(), &["producer".to_string()]);
        assert!(
            added.is_empty(),
            "a publication that is already recorded must not be recorded again: {added:?}"
        );
        let events = read(ws.path());
        assert_eq!(events.len(), 1, "one publication, one event: {events:?}");
        assert_eq!(
            events[0].partition_key.as_deref(),
            Some("2026-09-04"),
            "the retained event keeps the provenance it was written with"
        );
    }

    /// The event is self-contained (Louis's Option A): everything a delivery
    /// needs to bind parameters is copied onto it at publication time, so the
    /// receipt is not consulted again to interpret it.
    #[test]
    fn a_retained_event_carries_its_own_provenance() {
        let ws = tempfile::tempdir().unwrap();
        let record = published(ws.path(), "run-8", Some("2026-09-04"));
        append_run_record(ws.path(), "producer", record).unwrap();
        std::fs::remove_file(crate::retry::path_for_test(ws.path(), "run-8")).unwrap();

        let e = &read(ws.path())[0];
        assert_eq!(e.partition_key.as_deref(), Some("2026-09-04"));
        assert_eq!(e.release_id.as_deref(), Some("rel-9"));
        assert_eq!(e.run_id.as_deref(), Some("run-8"));
        assert_eq!(e.committed_at, "2026-09-04T04:00:00Z");
    }
}

/// #303: which publications survive a retention horizon.
///
/// The invariant, in Louis's words: never prune an object that is still needed
/// to interpret a retained durable object. Here that is one concrete rule -
/// **an event a retained delivery refers to is protected** - because a delivery
/// naming an event nobody can read is a record that cannot be explained, and
/// the pending and failed deliveries that are deliberately never aged out are
/// exactly the ones that would be orphaned first.
///
/// ## What pruning an event MEANS
///
/// It bounds replay, and that has to be said out loud rather than discovered:
///
/// > A subscription can only replay publication events still inside the
/// > retention horizon.
///
/// A subscriber added today receives the publications still on the log, not
/// every publication that ever happened. That is what makes a new subscription
/// deterministic instead of "replay forever", and it is the reason the horizon
/// is a policy an operator sets rather than a number this module picks.
pub fn retain(
    events: &[Event],
    before: &str,
    protected: &std::collections::BTreeSet<String>,
) -> (Vec<Event>, Vec<Event>) {
    let mut kept = Vec::new();
    let mut dropped = Vec::new();
    for e in events {
        // An undated event is kept: a record nobody can date is not evidence
        // that it is old.
        let old_enough = !e.committed_at.is_empty() && e.committed_at.as_str() < before;
        match old_enough && !protected.contains(&e.event_id) {
            true => dropped.push(e.clone()),
            false => kept.push(e.clone()),
        }
    }
    (kept, dropped)
}

/// Rewrite the log to exactly these events, oldest first.
///
/// Temp then rename, so a reader sees the whole previous log or the whole new
/// one. The log is append-only in normal operation; this is the one writer that
/// rewrites it, and it must not leave a half-written file behind for the pump
/// to read as "nothing was ever published".
pub fn keep_only(workspace: &Path, kept: &[Event]) -> Result<(), String> {
    let path = log_path(workspace);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let mut body = String::new();
    for e in kept {
        body.push_str(&serde_json::to_string(e).map_err(|e| e.to_string())?);
        body.push('\n');
    }
    let tmp = path.with_extension("ndjson.tmp");
    std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
}

/// The runs a retained event or delivery still points at.
///
/// #303: a receipt named by something we are keeping must outlive it. The event
/// carries its own provenance, so this is not needed to READ one - it is needed
/// so that "which run produced this" stays answerable, which is the whole point
/// of recording the run id on the event at all.
pub fn referenced_runs(
    events: &[Event],
    deliveries: &[crate::subscribe::Delivery],
) -> std::collections::BTreeSet<String> {
    events
        .iter()
        .filter_map(|e| e.run_id.clone())
        .chain(deliveries.iter().filter_map(|d| d.run_id.clone()))
        .collect()
}

#[cfg(test)]
mod event_retention {
    use super::*;

    fn ev(id: &str, at: &str, run: Option<&str>) -> Event {
        Event {
            event_id: id.into(),
            pipeline_id: "producer".into(),
            run_id: run.map(str::to_string),
            release_id: None,
            partition_key: Some("2026-09-04".into()),
            trigger: "scheduled".into(),
            committed_at: at.into(),
            assets: vec!["/data/a.parquet".into()],
        }
    }

    #[test]
    fn an_old_publication_ages_out_and_a_recent_one_does_not() {
        let events = vec![
            ev("old", "2026-01-01T00:00:00Z", None),
            ev("new", "2026-09-01T00:00:00Z", None),
        ];
        let (kept, dropped) = retain(&events, "2026-06-01T00:00:00Z", &Default::default());
        assert_eq!(dropped.iter().map(|e| e.event_id.clone()).collect::<Vec<_>>(), ["old"]);
        assert_eq!(kept.iter().map(|e| e.event_id.clone()).collect::<Vec<_>>(), ["new"]);
    }

    /// The invariant: a delivery that is deliberately never aged out must not
    /// be left pointing at a publication nobody can read.
    #[test]
    fn an_event_a_retained_delivery_needs_is_protected() {
        let events = vec![ev("old", "2026-01-01T00:00:00Z", None)];
        let protected = ["old".to_string()].into_iter().collect();
        let (kept, dropped) = retain(&events, "2026-06-01T00:00:00Z", &protected);
        assert!(dropped.is_empty(), "a pending delivery still needs it");
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn an_undated_publication_is_kept() {
        let events = vec![ev("undated", "", None)];
        let (kept, dropped) = retain(&events, "2026-06-01T00:00:00Z", &Default::default());
        assert!(dropped.is_empty());
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn rewriting_the_log_keeps_exactly_what_survived() {
        let ws = tempfile::tempdir().unwrap();
        let events = vec![
            ev("old", "2026-01-01T00:00:00Z", None),
            ev("new", "2026-09-01T00:00:00Z", None),
        ];
        keep_only(ws.path(), &events).unwrap();
        assert_eq!(read(ws.path()).len(), 2);

        let (kept, _) = retain(&events, "2026-06-01T00:00:00Z", &Default::default());
        keep_only(ws.path(), &kept).unwrap();
        let back = read(ws.path());
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].event_id, "new");
        // Still parseable as a log, not just as bytes.
        assert_eq!(back[0].partition_key.as_deref(), Some("2026-09-04"));
    }

    #[test]
    fn the_runs_a_retained_record_points_at_are_named() {
        let events = vec![ev("e1", "2026-09-01T00:00:00Z", Some("run-a"))];
        let runs = referenced_runs(&events, &[]);
        assert!(runs.contains("run-a"));
        assert_eq!(runs.len(), 1);
    }
}
