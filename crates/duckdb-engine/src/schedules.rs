//! The workspace schedule store, shared by everything that schedules a run.
//!
//! There used to be two stores. The desktop app kept `schedules.json`, a list
//! of records with their own ids, their own names, cron/interval/file-watch
//! kinds and run history. `duckle-runner serve` kept `panel-schedules.json`, an
//! object keyed by pipeline id holding `{enabled, intervalMinutes, cron}`.
//! Neither could see the other, so a schedule set up on the desktop was
//! invisible to the web console and the console's own list was invisible to the
//! desktop. The natural response to a console that shows nothing scheduled is
//! to schedule it again, which is how one pipeline ends up with two owners.
//!
//! This is the one store: `<workspace>/schedules.json`, in the record format,
//! which is a superset of what the console models.
//!
//! Sharing a file between processes means no writer may assume the copy it read
//! is still current. Every mutation goes through [`update`], which takes an
//! exclusive lock, re-reads from disk, applies the change to *that* list, and
//! writes it back through a temporary file. A blind write of an in-memory list
//! would silently drop whatever the other process added since, and the symptom
//! - a schedule that was definitely saved, gone an hour later - is close to
//! unfalsifiable after the fact.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::runlock;

/// How a schedule decides it is time to run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Standard 5-field cron (minute hour day month weekday), or 6/7-field
    /// with a leading seconds field. Evaluated in the machine's local time
    /// zone (issue #194).
    Cron { expr: String },
    /// Fire every N seconds since last run (or app start).
    Interval { seconds: u64 },
    /// Fire when a file or folder changes (debounced ~2s).
    FileWatch {
        path: String,
        #[serde(default)]
        recursive: bool,
    },
}

/// One scheduled pipeline.
///
/// Timestamps are UTC in the file. Cron is evaluated in local time at fire
/// time, which is a display-vs-storage split, not a contradiction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub pipeline_id: String,
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// The plan this fires, when it fires a plan rather than a single pipeline.
    ///
    /// Absent means the pipeline named by `pipeline_id`, which is what every schedule
    /// written before plans existed means, so an existing store keeps working untouched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    pub kind: ScheduleKind,
    /// #318: the IANA zone a cron expression is read in, e.g. "Europe/Brussels".
    ///
    /// Absent means the machine's own zone, which is what #194 settled on and
    /// what every schedule written before this means, so an existing store
    /// keeps its behaviour untouched. Set, it makes "03:00" mean 03:00 there
    /// regardless of where the runner is deployed - which is the point, since a
    /// container is usually UTC and the person who wrote the schedule is not.
    ///
    /// Only meaningful for cron schedules. An interval is an elapsed duration
    /// and a zone must not quietly turn "every 24 hours" into "the same clock
    /// time tomorrow".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// #296: days this schedule must not fire on - a maintenance window.
    ///
    /// Civil dates in the schedule's own zone, so "2026-12-25" is Christmas
    /// where the schedule lives rather than a UTC window clipping two local
    /// days. Empty by default, which is every schedule written before this.
    #[serde(default, skip_serializing_if = "crate::cronzone::Exclusions::is_empty")]
    pub exclude: crate::cronzone::Exclusions,
    /// #296: what to do about occurrences that came due while nobody was
    /// listening - the server down, the machine asleep.
    ///
    /// `skip` is the default and is exactly today's behaviour, so no existing
    /// schedule changes when this ships (AC5). What changes for everyone is
    /// that a skipped occurrence is now RECORDED rather than silently absent.
    #[serde(default)]
    pub misfire: crate::occurrences::Misfire,
    /// #296: how far a catch-up may go. Only consulted when `misfire` is not
    /// `skip`, and never optional in effect: `all` without a bound is the
    /// difference between a catch-up and an outage.
    #[serde(default)]
    pub catchup: crate::occurrences::Bounds,
    /// #296: what to do with an occurrence that comes due while this schedule's
    /// previous run is still going. `skip` is the default and today's behaviour.
    #[serde(default)]
    pub overlap: crate::occurrences::Overlap,
    /// #317: the parameter values this schedule's runs are given, checked against
    /// the pipeline's declared contract like a value from any other surface.
    ///
    /// Optional rather than empty-by-default: absent means "this save did not say",
    /// so an editor that knows nothing of parameters keeps the ones the store has
    /// instead of wiping them. An empty set, said explicitly, clears them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub last_run_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub last_run_status: Option<String>,
    #[serde(default)]
    pub last_run_duration_ms: Option<u64>,
    #[serde(default)]
    pub last_run_error: Option<String>,
    #[serde(default)]
    pub next_run_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn default_true() -> bool {
    true
}

/// What makes a schedule saveable.
///
/// One rule for every editor that saves one. It lived in the desktop scheduler,
/// and the web editor had no way to save at all; a second copy there is how two
/// surfaces come to disagree about what they accept. It was split out of saving
/// once already because saving and evaluating disagreed: a bare cron parse
/// REFUSED a five-field expression that the scheduler normalised and fired.
pub fn validate(schedule: &Schedule) -> Result<(), String> {
    // #318: an unknown zone is refused here rather than at fire time, so a typo
    // is a save error in front of the person who made it, not a job that
    // quietly runs on UTC in a container.
    crate::cronzone::resolve_zone(schedule.timezone.as_deref())?;
    schedule.exclude.validate()?;
    // #317: each pipeline in a plan has its own contract, so one set of values
    // for all of them is not something a schedule can say yet. Refused here, in
    // front of whoever wrote it, rather than ignored when it fires.
    if schedule.plan_id.is_some() && schedule.params.as_ref().is_some_and(|p| !p.is_empty()) {
        return Err(
            "A schedule that runs a plan cannot bind parameter values yet; bind them on a \
             schedule of the pipeline itself"
                .into(),
        );
    }
    match &schedule.kind {
        ScheduleKind::Cron { expr } => {
            let normalized = crate::cronzone::normalize_cron(expr).ok_or_else(|| {
                format!("Invalid cron expression: {expr:?} does not have 5, 6 or 7 fields")
            })?;
            normalized
                .parse::<cron::Schedule>()
                .map_err(|e| format!("Invalid cron expression: {}", e))?;
        }
        ScheduleKind::Interval { seconds } => {
            if *seconds < 1 {
                return Err("Interval must be at least 1 second".into());
            }
        }
        ScheduleKind::FileWatch { path, .. } => {
            if path.trim().is_empty() {
                return Err("Watch path is required".into());
            }
        }
    }
    Ok(())
}

/// Put a saved schedule into the list: over the one with its id, or at the end.
///
/// A save carries configuration, not history, so the run fields the store
/// already has are kept rather than wiped to null by a payload without them.
/// The plan a schedule runs is kept for the same reason, and it matters more:
/// an editor with no plan field sends "no plan" for a schedule that has one,
/// and believing it would leave the schedule pointed at the label in its
/// pipeline_id, which is not a file. Clearing a plan is done by naming a
/// pipeline instead, not by saying nothing.
pub fn merge_saved(list: &mut Vec<Schedule>, saved: Schedule) {
    match list.iter().position(|s| s.id == saved.id) {
        Some(idx) => {
            let prev = &list[idx];
            let mut next = saved;
            next.last_run_at = prev.last_run_at;
            next.last_run_status = prev.last_run_status.clone();
            next.last_run_duration_ms = prev.last_run_duration_ms;
            next.last_run_error = prev.last_run_error.clone();
            next.plan_id = next.plan_id.or_else(|| prev.plan_id.clone());
            // The same for parameter values: a save that does not mention them
            // keeps them.
            next.params = next.params.or_else(|| prev.params.clone());
            list[idx] = next;
        }
        None => list.push(saved),
    }
}

/// The id for a schedule saved without one.
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn schedules_path(workspace: &Path) -> PathBuf {
    workspace.join("schedules.json")
}

/// The store as it is on disk right now.
///
/// A missing file is an empty list, not an error: a fresh workspace has no
/// schedules and that is not a problem to report. A file that exists but will
/// not parse IS an error, because silently treating a corrupt store as empty
/// is how a scheduler stops running everything without saying why.
pub fn load(workspace: &Path) -> Result<Vec<Schedule>, String> {
    let p = schedules_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    if content.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&content).map_err(|e| format!("Parse schedules.json: {}", e))
}

/// Apply a change to the store and persist it, as one exclusive step.
///
/// `f` receives the list as it is on disk at this moment, not a copy the caller
/// read earlier, so a change made by the other process in between survives.
/// Returns the list as written, which callers should adopt as their new
/// in-memory state for the same reason.
pub fn update<F>(workspace: &Path, f: F) -> Result<Vec<Schedule>, String>
where
    F: FnOnce(&mut Vec<Schedule>),
{
    let _guard = runlock::lock_store(workspace, "schedules")?;
    let mut list = load(workspace)?;
    f(&mut list);
    write_atomically(workspace, &list)?;
    Ok(list)
}

/// Write through a temporary file in the same directory, then rename over.
///
/// A reader either sees the previous store or the new one. Writing in place
/// leaves a window where the file is half-written, and the other process is
/// polling it every few seconds, so that window gets hit.
fn write_atomically(workspace: &Path, list: &[Schedule]) -> Result<(), String> {
    let p = schedules_path(workspace);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let body = serde_json::to_string_pretty(list).map_err(|e| e.to_string())?;
    let tmp = p.with_extension("json.tmp");
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    // No unlink first. This used to call remove_file on Windows, justified by
    // "Windows refuses a rename onto an existing file", which is not true of
    // this rename: std::fs::rename is MoveFileExW with MOVEFILE_REPLACE_EXISTING,
    // and it replaces the destination even while a reader holds it open. The
    // unlink bought nothing and cost the guarantee the function exists for -
    // it opened a window where the store was simply absent, which `load` reads
    // as an empty schedule list, and if the rename then failed the store was
    // gone for good.
    std::fs::rename(&tmp, &p).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interval(pipeline: &str, seconds: u64) -> Schedule {
        Schedule {
            timezone: None,
            exclude: Default::default(),
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
            id: format!("id-{pipeline}"),
            pipeline_id: pipeline.into(),
            plan_id: None,
            name: pipeline.into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        }
    }

    #[test]
    fn a_missing_store_is_empty_and_a_corrupt_one_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_empty(), "a fresh workspace has no schedules");

        std::fs::write(schedules_path(ws), b"{ not json").unwrap();
        assert!(
            load(ws).is_err(),
            "a corrupt store must be reported, not read as 'nothing scheduled'"
        );
    }

    #[test]
    fn a_write_keeps_what_the_other_process_added_in_the_meantime() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The desktop app loads the store and holds it in memory.
        update(ws, |v| v.push(interval("nightly", 3600))).unwrap();
        let stale = load(ws).unwrap();
        assert_eq!(stale.len(), 1);

        // The web console adds one while the desktop still holds the old copy.
        update(ws, |v| v.push(interval("hourly", 60))).unwrap();

        // The desktop now records a run. It must not write `stale` back over
        // the top, which is what a blind save of in-memory state would do.
        update(ws, |v| {
            let s = v.iter_mut().find(|s| s.pipeline_id == "nightly").expect("lost the schedule");
            s.last_run_status = Some("success".into());
        })
        .unwrap();

        let after = load(ws).unwrap();
        let ids: Vec<&str> = after.iter().map(|s| s.pipeline_id.as_str()).collect();
        assert_eq!(ids, vec!["nightly", "hourly"], "a concurrent addition was dropped");
        assert_eq!(after[0].last_run_status.as_deref(), Some("success"));
    }

    #[test]
    fn the_store_survives_writers_running_at_the_same_time() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();

        // Every thread appends one schedule. With a read-modify-write under the
        // lock all of them survive; with a plain read-then-write most are lost.
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let ws = ws.clone();
                std::thread::spawn(move || {
                    update(&ws, |v| v.push(interval(&format!("p{i}"), 60))).unwrap();
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        let after = load(&ws).unwrap();
        assert_eq!(after.len(), 8, "concurrent writers lost updates");
    }

    /// The store is never absent, not even for an instant.
    ///
    /// `write_atomically` used to unlink the destination before renaming over
    /// it, on the premise that Windows refuses a rename onto an existing file.
    /// It does not: `std::fs::rename` is `MoveFileExW` with
    /// `MOVEFILE_REPLACE_EXISTING`. The unlink bought nothing and cost the
    /// guarantee the function is named for - a reader landing in that window
    /// sees no file and reads it as an empty schedule list, and a rename that
    /// then failed would have left the store gone for good.
    ///
    /// This asserts the premise directly, because it is the premise that was
    /// wrong, and then that a reader running flat out beside a writer never
    /// observes the store missing or short.
    #[test]
    fn a_rename_replaces_the_store_without_ever_unlinking_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        update(&ws, |v| v.push(interval("p0", 60))).unwrap();
        let store = schedules_path(&ws);
        assert!(store.exists());

        // The premise, measured rather than assumed: a rename onto a file that
        // already exists succeeds, and succeeds while a reader holds it open.
        let held = std::fs::File::open(&store).unwrap();
        let side = store.with_extension("json.probe");
        std::fs::write(&side, std::fs::read(&store).unwrap()).unwrap();
        std::fs::rename(&side, &store).expect("rename onto an existing store failed");
        drop(held);

        // And the property that matters: a reader beside a writer never sees
        // the store vanish. With the unlink present this fails within a few
        // hundred iterations on Windows.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen_missing = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader = {
            let (ws, stop, seen) = (ws.clone(), stop.clone(), seen_missing.clone());
            std::thread::spawn(move || {
                let path = schedules_path(&ws);
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if !path.exists() {
                        seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        };
        for i in 0..300 {
            update(&ws, |v| {
                v.clear();
                v.push(interval(&format!("p{i}"), 60));
            })
            .unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().unwrap();
        assert_eq!(
            seen_missing.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the store was observably absent during a write"
        );
    }
}

#[cfg(test)]
mod schedule_params {
    use super::*;

    fn schedule(json: serde_json::Value) -> Schedule {
        serde_json::from_value(json).expect("a schedule")
    }

    /// #317: a schedule binds parameter values for the pipeline it runs.
    #[test]
    fn a_schedule_carries_its_parameter_values() {
        let s = schedule(serde_json::json!({
            "id": "s1", "pipeline_id": "orders", "name": "n",
            "kind": { "type": "interval", "seconds": 60 },
            "params": { "region": "us" }
        }));
        assert_eq!(s.params.as_ref().and_then(|p| p.get("region")).map(String::as_str), Some("us"));
        assert!(validate(&s).is_ok());
        let back = serde_json::to_value(&s).unwrap();
        assert_eq!(back["params"]["region"], "us");
    }

    /// A plan's pipelines each have their own contract, so one set of values
    /// for all of them is not something a schedule can say yet; refused at save
    /// rather than ignored at 3am.
    #[test]
    fn a_plan_schedule_with_parameter_values_is_refused() {
        let s = schedule(serde_json::json!({
            "id": "s1", "pipeline_id": "nightly", "name": "n", "plan_id": "nightly",
            "kind": { "type": "interval", "seconds": 60 },
            "params": { "region": "us" }
        }));
        let err = validate(&s).unwrap_err();
        assert!(err.contains("plan") && err.contains("parameter"), "{err}");
    }

    /// An editor that knows nothing of parameters - the console's form, an older
    /// desktop - saves without the field, and that must not wipe the values the
    /// schedule has. An empty set, said explicitly, does clear them.
    #[test]
    fn a_save_that_does_not_mention_parameters_keeps_them() {
        let mut list = vec![schedule(serde_json::json!({
            "id": "s1", "pipeline_id": "orders", "name": "n",
            "kind": { "type": "interval", "seconds": 60 }, "params": { "region": "us" }
        }))];
        merge_saved(&mut list, schedule(serde_json::json!({
            "id": "s1", "pipeline_id": "orders", "name": "renamed",
            "kind": { "type": "interval", "seconds": 120 }
        })));
        assert_eq!(list[0].name, "renamed");
        assert_eq!(list[0].params.as_ref().and_then(|p| p.get("region")).map(String::as_str), Some("us"));
        merge_saved(&mut list, schedule(serde_json::json!({
            "id": "s1", "pipeline_id": "orders", "name": "renamed",
            "kind": { "type": "interval", "seconds": 120 }, "params": {}
        })));
        assert!(list[0].params.as_ref().is_some_and(|p| p.is_empty()), "{:?}", list[0].params);
    }
}
