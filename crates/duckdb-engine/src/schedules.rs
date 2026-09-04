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
