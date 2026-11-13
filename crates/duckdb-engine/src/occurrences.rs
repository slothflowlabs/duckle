//! #296: a durable record of every occurrence a schedule was due for.
//!
//! The scheduler decides WHAT occurrences exist; the ordinary durable run path
//! decides HOW they execute. That split is the whole design, and it needs one
//! thing that did not exist: a record per occurrence rather than a single
//! `next_run_at` instant.
//!
//! ## Why an instant is not enough
//!
//! `next_run_at` answers "when next", and every policy in #296 asks a different
//! question. "Which occurrences did we miss while the server was down" cannot be
//! answered by a pointer that was simply moved forward; neither can "has this
//! occurrence already run", which is what stops a restart doing it twice.
//!
//! On `duckle serve` it is worse than one instant: the tick loop's state is two
//! in-memory maps built inside the thread, so a restart re-arms from now and the
//! missed window is not merely unanswered, it is gone.
//!
//! ## One identity, two consumers
//!
//! The id is deterministic over the schedule and the instant it was due, so the
//! same occurrence recorded twice - by a restart, or by both schedulers - is the
//! same row. It is the string `backfill::occurrence_id` already takes as
//! `schedule_occurrence`, so a partitioned run launched by a schedule and the
//! schedule's own ledger are talking about one thing rather than two.
//!
//! ## Recording a decision is the point, not a side effect
//!
//! Every occurrence gets an outcome, including the ones that did not run. "It
//! was excluded", "it was overdue and the policy is skip" and "nothing
//! happened" are three different states that a moved pointer renders as one.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// What the scheduler decided to do about one occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum Decision {
    /// A run was started for it.
    Fired,
    /// It was due and deliberately not run, with the reason an operator needs.
    Skipped { reason: String },
    /// An exclusion calendar covered it (#296). Separate from `Skipped` because
    /// it is a configured intention rather than a policy consequence.
    Excluded,
    /// It came due while the schedule's previous run was still going, and the
    /// overlap policy holds it until that run ends (#296). Recorded again, as
    /// `fired` or `skipped`, once that is decided: an occurrence is what its
    /// latest record says, and the ledger stays append-only.
    Queued { reason: String },
}

impl Decision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Decision::Fired => "fired",
            Decision::Skipped { .. } => "skipped",
            Decision::Excluded => "excluded",
            Decision::Queued { .. } => "queued",
        }
    }
}

/// One occurrence, and what became of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Occurrence {
    pub occurrence_id: String,
    pub schedule_id: String,
    /// The instant it was due, in UTC.
    pub scheduled_for: String,
    /// What the clock read where the schedule lives. Kept because "03:00
    /// Brussels" is what the operator asked for and the UTC instant is not
    /// recognisable to them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub local: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(flatten)]
    pub decision: Decision,
    /// The run it started, when it started one, so the occurrence and the run
    /// are reachable from each other.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// When the decision was taken, which is not when it was due - the gap
    /// between the two IS the scheduler lag #296 asks to expose.
    pub decided_at: String,
}

/// The identity of one occurrence.
///
/// Length-prefixed, like every other id in the engine, so two different splits
/// cannot collide: joining with a separator would make `("ab", "c")` and
/// `("a", "bc")` the same occurrence the moment a schedule id contained it.
pub fn occurrence_id(schedule_id: &str, scheduled_for: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for part in [schedule_id, scheduled_for] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    format!("occ-{}", &hex[..16])
}

pub fn log_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("occurrences.ndjson")
}

/// The instant an interval schedule last came due, at or before `now`.
///
/// Intervals run on a grid of whole multiples of the interval since the Unix epoch, in
/// UTC: every 15 minutes means :00, :15, :30 and :45, whichever process is looking and
/// whenever it started. Counting from when a process first saw the schedule gave every
/// process its own occurrences, so there was nothing for two of them to agree on.
pub fn interval_slot(
    now: chrono::DateTime<chrono::Utc>,
    seconds: u64,
) -> chrono::DateTime<chrono::Utc> {
    let seconds = interval_seconds(seconds);
    let t = now.timestamp();
    chrono::DateTime::from_timestamp(t - t.rem_euclid(seconds), 0).unwrap_or(now)
}

/// The first instant on an interval schedule's grid after `now`.
pub fn next_interval_slot(
    now: chrono::DateTime<chrono::Utc>,
    seconds: u64,
) -> chrono::DateTime<chrono::Utc> {
    interval_slot(now, seconds) + chrono::Duration::seconds(interval_seconds(seconds))
}

/// Ten thousand years is a typo rather than a schedule, and the date arithmetic ends there.
fn interval_seconds(seconds: u64) -> i64 {
    seconds.clamp(1, 315_360_000_000) as i64
}

/// How far a schedule has been claimed, in `.duckle/claims/`.
#[derive(Debug, Serialize, Deserialize)]
struct Claimed {
    through: chrono::DateTime<chrono::Utc>,
}

/// Take one occurrence for this process. True means it is this process's to run.
///
/// This is what lets several schedulers share a workspace. Each one works out for itself
/// that an occurrence is due, and the first to get here takes it, so a run happens once
/// however many are looking: two `serve` replicas, or a laptop and a server. The run lock
/// could not do this, because it stops two runs overlapping and not a second run after the
/// first has finished.
///
/// It records one instant per schedule rather than one file per occurrence, because a
/// minutely schedule would otherwise leave half a million files a year. Occurrences are
/// claimed in order, so "claimed through" says everything: an instant at or before it has
/// been taken. That also means a process whose clock runs behind cannot take an occurrence
/// a faster one already ran.
///
/// The record goes under the store lock, so this is as good as the storage's locks, which
/// is what `duckle-runner work` already asks of a shared workspace. An `Err` means the claim
/// could not be made at all. The callers run the schedule anyway: a full disk should not
/// silently stop every schedule, and on one scheduler the claim was never needed.
pub fn claim(
    workspace: &Path,
    schedule_id: &str,
    due: chrono::DateTime<chrono::Utc>,
) -> Result<bool, String> {
    let name = occurrence_id(schedule_id, "claim");
    let _guard = crate::runlock::lock_store(workspace, &name)?;
    let path = workspace.join(".duckle").join("claims").join(format!("{name}.json"));
    let claimed: Option<Claimed> = match std::fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text).ok(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    if claimed.is_some_and(|c| c.through >= due) {
        return Ok(false);
    }
    let dir = path.parent().ok_or("a claim has a folder")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let record = Claimed { through: due };
    let text = serde_json::to_string(&record).map_err(|e| e.to_string())?;
    // Temp then rename, so a process killed here leaves the last claim and not half of one.
    let temp = path.with_extension("json.tmp");
    std::fs::write(&temp, text).map_err(|e| format!("{}: {e}", temp.display()))?;
    std::fs::rename(&temp, &path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(true)
}

/// Append one occurrence. Append-only, like the materialization log and for the
/// same reason: a writer never rewrites, so a concurrent reader cannot see half
/// a record.
pub fn record(workspace: &Path, occ: &Occurrence) -> Result<(), String> {
    let path = log_path(workspace);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let line = serde_json::to_string(occ).map_err(|e| e.to_string())?;
    crate::ndjson::append_records(&path, &line).map_err(|e| format!("{}: {e}", path.display()))
}

/// Every recorded occurrence, oldest first.
///
/// A line that will not parse is skipped rather than failing the read: a torn
/// last line from a killed process must not make every earlier occurrence
/// unreadable.
pub fn read(workspace: &Path) -> Vec<Occurrence> {
    let Ok(text) = std::fs::read_to_string(log_path(workspace)) else { return Vec::new() };
    text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
}

/// The ids already recorded, for the duplicate check a restart depends on.
pub fn recorded(workspace: &Path) -> std::collections::BTreeSet<String> {
    read(workspace).into_iter().map(|o| o.occurrence_id).collect()
}

/// The same occurrence, decided again. The ledger is append-only, so a queued
/// occurrence that runs, or is superseded, gets a second record with the same id.
pub fn decided(occ: &Occurrence, decision: Decision, decided_at: &str) -> Occurrence {
    Occurrence { decision, decided_at: decided_at.to_string(), ..occ.clone() }
}

/// This schedule's occurrences whose latest record is `queued`, oldest first:
/// the ones still waiting for a previous run to end.
///
/// Read from the ledger rather than kept in memory, so an occurrence that was
/// waiting when the server stopped still runs when it comes back.
pub fn waiting(workspace: &Path, schedule_id: &str) -> Vec<Occurrence> {
    let mut latest: std::collections::BTreeMap<String, Occurrence> = Default::default();
    for occ in read(workspace).into_iter().filter(|o| o.schedule_id == schedule_id) {
        latest.insert(occ.occurrence_id.clone(), occ);
    }
    let mut out: Vec<Occurrence> =
        latest.into_values().filter(|o| matches!(o.decision, Decision::Queued { .. })).collect();
    out.sort_by(|a, b| a.scheduled_for.cmp(&b.scheduled_for));
    out
}

/// What to do with an occurrence that comes due while the schedule's previous
/// run is still going (#296).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Overlap {
    /// Record it as skipped and run nothing. What every schedule did before the
    /// policy existed, and the default, so none of them changes (AC5).
    #[default]
    Skip,
    /// Run it once the previous run ends.
    Queue,
    /// Stop the previous run, then run this one.
    Replace,
    /// Run it alongside the previous run. Resource pools still apply.
    Allow,
}

/// What becomes of an occurrence that came due while its schedule's previous run
/// is still going.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnOverlap {
    /// Start it anyway.
    Run,
    /// Record it as skipped, for this reason.
    Skip(String),
    /// Record it as queued, for this reason, and start it once the schedule is
    /// free. `stop_running` asks the run in the way to stop first; `supersede`
    /// are waiting occurrences this one takes the place of, recorded skipped.
    Wait { reason: String, stop_running: bool, supersede: Vec<Occurrence> },
}

/// Decide what the overlap policy makes of an occurrence that came due while the
/// previous run is still going, given what is already waiting.
///
/// One occurrence waits at most. A scheduled run is not told which occurrence it
/// is, so a second waiting run would repeat the first with the same inputs, and a
/// schedule that is always slower than its interval would queue without end.
pub fn on_overlap(policy: Overlap, waiting: &[Occurrence]) -> OnOverlap {
    match policy {
        Overlap::Allow => OnOverlap::Run,
        Overlap::Skip => OnOverlap::Skip(
            "the previous run was still going, and this schedule's overlap policy is skip".into(),
        ),
        Overlap::Queue if !waiting.is_empty() => OnOverlap::Skip(
            "an earlier occurrence is already waiting for the previous run to end".into(),
        ),
        Overlap::Queue => OnOverlap::Wait {
            reason: "waiting for the previous run to end".into(),
            stop_running: false,
            supersede: Vec::new(),
        },
        Overlap::Replace => OnOverlap::Wait {
            reason: "stopping the previous run to run this one".into(),
            stop_running: true,
            supersede: waiting.to_vec(),
        },
    }
}

/// What to do about occurrences that came due while nobody was listening.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Misfire {
    /// Record them and run none. Today's behaviour, and the default, so an
    /// existing schedule keeps behaving exactly as it does now (AC5).
    #[default]
    Skip,
    /// Run only the newest. For a refresh where yesterday's answer is worthless.
    Latest,
    /// Run each of them, bounded. For a feed where every date matters.
    All,
}

/// Bounds on catch-up, so a server returning after a year does not launch a
/// year of work.
///
/// The issue asks for these by name and they are not optional decoration: `all`
/// without a bound is the difference between a catch-up and an outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bounds {
    pub max_catchup_runs: usize,
    pub max_catchup_age_days: u64,
}

impl Default for Bounds {
    fn default() -> Self {
        // The issue's own suggestion. A month of daily occurrences is a
        // plausible outage; a year is a mistake somebody should have to type.
        Bounds { max_catchup_runs: 31, max_catchup_age_days: 45 }
    }
}

/// Every occurrence a cron schedule was due for in `(after, now]`.
///
/// Enumerated through the shared evaluator, so the zone and the exclusion
/// calendar decide this the same way they decide the next occurrence - an
/// excluded day must not reappear as a missed one to catch up on.
///
/// Bounded while walking rather than afterwards: a schedule that has been down
/// for a year would otherwise build a year of instants in order to throw them
/// away, and the walk is the expensive half.
pub fn missed(
    expr: &str,
    zone: &crate::cronzone::Zone,
    exclude: &crate::cronzone::Exclusions,
    after: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
    bounds: &Bounds,
) -> Result<Vec<crate::cronzone::Occurrence>, String> {
    let floor = now - chrono::Duration::days(bounds.max_catchup_age_days as i64);
    let mut cursor = after.max(floor);
    let mut out = Vec::new();
    // A hard ceiling on the walk itself, not only on what is returned: an
    // expression that fires every minute over a 45-day window is 64,800
    // occurrences, and the bound is what stops that being enumerated.
    let ceiling = bounds.max_catchup_runs.saturating_mul(4).max(bounds.max_catchup_runs) + 1_000;
    for _ in 0..ceiling {
        let (occ, _skipped) = crate::cronzone::next_after_excluding(expr, zone, exclude, cursor)?;
        let Some(occ) = occ else { break };
        if occ.at > now {
            break;
        }
        cursor = occ.at;
        out.push(occ);
    }
    // The NEWEST are kept when there are too many. Dropping the oldest is the
    // only honest truncation: the recent ones are the ones still worth doing,
    // and a catch-up that started at the far end would spend its whole budget
    // on the least useful work.
    if out.len() > bounds.max_catchup_runs {
        out.drain(..out.len() - bounds.max_catchup_runs);
    }
    Ok(out)
}

/// Which of the missed occurrences the policy actually runs.
///
/// Returns every occurrence with its decision, not only the ones that run - the
/// recorded outcome for the others is the point of AC1, and a filter would
/// throw away exactly what has to be persisted.
pub fn decide(
    missed: &[crate::cronzone::Occurrence],
    policy: Misfire,
) -> Vec<(crate::cronzone::Occurrence, Decision)> {
    let last = missed.len().saturating_sub(1);
    missed
        .iter()
        .enumerate()
        .map(|(i, occ)| {
            let decision = match policy {
                Misfire::Skip => Decision::Skipped {
                    reason: "overdue, and this schedule's misfire policy is skip".into(),
                },
                Misfire::All => Decision::Fired,
                Misfire::Latest if i == last => Decision::Fired,
                Misfire::Latest => Decision::Skipped {
                    reason: "superseded by a newer overdue occurrence".into(),
                },
            };
            (occ.clone(), decision)
        })
        .collect()
}

/// The newest occurrence this schedule has a record of.
///
/// The anchor a catch-up measures from. Taken from the LEDGER rather than from
/// `next_run_at`, because the ledger is what survives a restart and the pointer
/// is what a restart re-arms from "now".
pub fn last_recorded(
    workspace: &Path,
    schedule_id: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    read(workspace)
        .into_iter()
        .filter(|o| o.schedule_id == schedule_id)
        .filter_map(|o| chrono::DateTime::parse_from_rfc3339(&o.scheduled_for).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
        .max()
}

/// What this tick should do about occurrences that came due while nobody ran
/// them, and what should be recorded either way.
///
/// Empty when the schedule has no record at all. That is deliberate and it is
/// the difference between a catch-up and an accident: a schedule seen for the
/// first time - newly created, or newly reaching a server that has never run
/// it - has no anchor to measure from, and measuring from the epoch would fire
/// its entire history. First sighting ARMS; it does not fire. That is what the
/// tick loop already did and what this must not change.
///
/// AC3 - a restart must not decide the same occurrence twice - is satisfied by
/// where the walk STARTS rather than by filtering afterwards. `last_recorded`
/// is the newest recorded instant and `missed` enumerates strictly after it, so
/// an occurrence that has a record cannot come back. A dedup pass on top was
/// written first and removed: reverting it changed no test, because there was
/// no case it could catch.
pub fn catch_up(
    workspace: &Path,
    schedule_id: &str,
    expr: &str,
    zone: &crate::cronzone::Zone,
    exclude: &crate::cronzone::Exclusions,
    misfire: Misfire,
    bounds: &Bounds,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<(crate::cronzone::Occurrence, Decision)>, String> {
    let Some(after) = last_recorded(workspace, schedule_id) else {
        return Ok(Vec::new());
    };
    Ok(decide(&missed(expr, zone, exclude, after, now, bounds)?, misfire))
}

/// Build the record for one occurrence.
pub fn entry(
    schedule_id: &str,
    occ: &crate::cronzone::Occurrence,
    timezone: Option<&str>,
    decision: Decision,
    run_id: Option<String>,
    decided_at: &str,
) -> Occurrence {
    let scheduled_for = occ.at.to_rfc3339();
    Occurrence {
        occurrence_id: occurrence_id(schedule_id, &scheduled_for),
        schedule_id: schedule_id.to_string(),
        scheduled_for,
        local: occ.local.clone(),
        timezone: timezone.map(str::to_string),
        decision,
        run_id,
        decided_at: decided_at.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cronzone::{resolve_zone, Exclusions};
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(y, mo, d, h, 0, 0).single().expect("one instant")
    }

    #[test]
    fn the_same_occurrence_has_the_same_id_and_a_different_one_does_not() {
        let a = occurrence_id("s1", "2026-09-04T03:00:00Z");
        assert_eq!(a, occurrence_id("s1", "2026-09-04T03:00:00Z"), "not deterministic");
        assert_ne!(a, occurrence_id("s2", "2026-09-04T03:00:00Z"));
        assert_ne!(a, occurrence_id("s1", "2026-09-05T03:00:00Z"));
        // Length-prefixed, so a separator in an id cannot collide two.
        assert_ne!(occurrence_id("a", "bc"), occurrence_id("ab", "c"));
    }

    #[test]
    fn an_interval_falls_on_the_same_instants_whoever_is_looking() {
        let at = |h, m, s| chrono::Utc.with_ymd_and_hms(2026, 10, 30, h, m, s).single().unwrap();
        // Two processes that started at different times agree on every instant.
        assert_eq!(interval_slot(at(12, 7, 13), 300), at(12, 5, 0));
        assert_eq!(interval_slot(at(12, 9, 59), 300), at(12, 5, 0));
        assert_eq!(interval_slot(at(12, 10, 0), 300), at(12, 10, 0), "the boundary is on the grid");
        assert_eq!(next_interval_slot(at(12, 7, 13), 300), at(12, 10, 0));
        assert_eq!(
            next_interval_slot(at(12, 10, 0), 300),
            at(12, 15, 0),
            "next means strictly after, or a schedule fires twice on its own boundary"
        );
        // Whatever an interval is, a due time still comes out of it.
        assert!(next_interval_slot(at(12, 0, 0), 0) > at(12, 0, 0));
        assert!(next_interval_slot(at(12, 0, 0), u64::MAX) > at(12, 0, 0));
    }

    #[test]
    fn an_occurrence_can_be_claimed_once() {
        let tmp = tempfile::tempdir().unwrap();
        let (ws, t) = (tmp.path(), utc(2026, 10, 30, 12));
        assert_eq!(claim(ws, "s1", t), Ok(true), "the first to ask takes it");
        assert_eq!(claim(ws, "s1", t), Ok(false), "and the second does not");
        assert_eq!(claim(ws, "s2", t), Ok(true), "another schedule's occurrence is its own");
        assert_eq!(claim(ws, "s1", utc(2026, 10, 30, 13)), Ok(true), "the next one is free");
        // A process running behind cannot take what a faster one already ran.
        assert_eq!(claim(ws, "s1", t), Ok(false));
    }

    /// The property the whole design rests on, with the race actually run: however many
    /// schedulers reach an occurrence at the same moment, one of them gets it.
    #[test]
    fn schedulers_racing_for_an_occurrence_take_it_exactly_once() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        for hour in 0..20 {
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(6));
            let racers: Vec<_> = (0..6)
                .map(|_| {
                    let (ws, barrier) = (ws.clone(), barrier.clone());
                    std::thread::spawn(move || {
                        barrier.wait();
                        claim(&ws, "s1", utc(2026, 10, 30, hour)).unwrap()
                    })
                })
                .collect();
            let wins = racers.into_iter().map(|r| r.join().unwrap()).filter(|won| *won).count();
            assert_eq!(wins, 1, "hour {hour}");
        }
    }

    #[test]
    fn a_downtime_window_enumerates_the_days_it_covered() {
        let utc_zone = resolve_zone(Some("UTC")).unwrap();
        let none = Exclusions::default();
        // Down from the 1st to the 5th, daily at 03:00.
        let m = missed(
            "0 3 * * *",
            &utc_zone,
            &none,
            utc(2026, 9, 1, 4),
            utc(2026, 9, 5, 12),
            &Bounds::default(),
        )
        .unwrap();
        let days: Vec<u32> = m.iter().map(|o| chrono::Datelike::day(&o.at)).collect();
        assert_eq!(days, [2, 3, 4, 5], "one per missed day: {days:?}");
    }

    /// An excluded day must not come back as a missed one to catch up on.
    #[test]
    fn an_excluded_day_is_not_a_missed_occurrence() {
        let utc_zone = resolve_zone(Some("UTC")).unwrap();
        let ex: Exclusions =
            serde_json::from_value(serde_json::json!({ "dates": ["2026-09-03"] })).unwrap();
        let m = missed(
            "0 3 * * *",
            &utc_zone,
            &ex,
            utc(2026, 9, 1, 4),
            utc(2026, 9, 5, 12),
            &Bounds::default(),
        )
        .unwrap();
        let days: Vec<u32> = m.iter().map(|o| chrono::Datelike::day(&o.at)).collect();
        assert_eq!(days, [2, 4, 5], "the 3rd was excluded: {days:?}");
    }

    /// AC2: catch-up is bounded, and the bound keeps the NEWEST.
    #[test]
    fn catch_up_is_bounded_and_keeps_the_recent_end() {
        let utc_zone = resolve_zone(Some("UTC")).unwrap();
        let none = Exclusions::default();
        let bounds = Bounds { max_catchup_runs: 3, max_catchup_age_days: 365 };
        let m = missed(
            "0 3 * * *",
            &utc_zone,
            &none,
            utc(2026, 9, 1, 4),
            utc(2026, 9, 10, 12),
            &bounds,
        )
        .unwrap();
        let days: Vec<u32> = m.iter().map(|o| chrono::Datelike::day(&o.at)).collect();
        assert_eq!(days, [8, 9, 10], "the recent end is what is still worth doing: {days:?}");
    }

    #[test]
    fn the_age_bound_stops_a_year_of_catch_up() {
        let utc_zone = resolve_zone(Some("UTC")).unwrap();
        let none = Exclusions::default();
        let bounds = Bounds { max_catchup_runs: 1000, max_catchup_age_days: 3 };
        let m = missed(
            "0 3 * * *",
            &utc_zone,
            &none,
            utc(2025, 9, 1, 4),
            utc(2026, 9, 10, 12),
            &bounds,
        )
        .unwrap();
        assert!(m.len() <= 4, "a year down must not enumerate a year: {}", m.len());
    }

    #[test]
    fn every_policy_records_every_occurrence() {
        let utc_zone = resolve_zone(Some("UTC")).unwrap();
        let m = missed(
            "0 3 * * *",
            &utc_zone,
            &Exclusions::default(),
            utc(2026, 9, 1, 4),
            utc(2026, 9, 4, 12),
            &Bounds::default(),
        )
        .unwrap();
        assert_eq!(m.len(), 3);

        let fired = |d: &[(crate::cronzone::Occurrence, Decision)]| {
            d.iter().filter(|(_, x)| *x == Decision::Fired).count()
        };

        // AC5: skip is the default and preserves today's behaviour - nothing
        // runs - but each one is now RECORDED rather than silently absent.
        let skipped = decide(&m, Misfire::Skip);
        assert_eq!(skipped.len(), 3, "every occurrence gets an outcome");
        assert_eq!(fired(&skipped), 0);

        let latest = decide(&m, Misfire::Latest);
        assert_eq!(latest.len(), 3);
        assert_eq!(fired(&latest), 1, "only the newest");
        assert_eq!(latest[2].1, Decision::Fired, "and it is the LAST one");
        assert!(matches!(latest[0].1, Decision::Skipped { .. }));

        let all = decide(&m, Misfire::All);
        assert_eq!(fired(&all), 3);
    }

    #[test]
    fn nothing_missed_decides_nothing() {
        assert!(decide(&[], Misfire::All).is_empty());
        assert!(decide(&[], Misfire::Latest).is_empty());
    }

    /// AC3: the same occurrence must not be created twice after a restart.
    #[test]
    fn a_recorded_occurrence_is_recognised_again() {
        let ws = tempfile::tempdir().unwrap();
        let utc_zone = resolve_zone(Some("UTC")).unwrap();
        let m = missed(
            "0 3 * * *",
            &utc_zone,
            &Exclusions::default(),
            utc(2026, 9, 1, 4),
            utc(2026, 9, 3, 12),
            &Bounds::default(),
        )
        .unwrap();
        for (occ, decision) in decide(&m, Misfire::All) {
            record(ws.path(), &entry("s1", &occ, Some("UTC"), decision, None, "2026-09-03T12:00:00Z"))
                .unwrap();
        }
        assert_eq!(read(ws.path()).len(), 2);

        // A restart enumerates the same window and must recognise all of it.
        let seen = recorded(ws.path());
        let again = missed(
            "0 3 * * *",
            &utc_zone,
            &Exclusions::default(),
            utc(2026, 9, 1, 4),
            utc(2026, 9, 3, 12),
            &Bounds::default(),
        )
        .unwrap();
        let fresh: Vec<_> = again
            .iter()
            .filter(|o| !seen.contains(&occurrence_id("s1", &o.at.to_rfc3339())))
            .collect();
        assert!(fresh.is_empty(), "a restart would have run these again: {fresh:?}");
    }

    #[test]
    fn a_record_round_trips_and_keeps_what_an_operator_reads() {
        let ws = tempfile::tempdir().unwrap();
        let brussels = resolve_zone(Some("Europe/Brussels")).unwrap();
        let (occ, _) = crate::cronzone::next_after_excluding(
            "0 3 * * *",
            &brussels,
            &Exclusions::default(),
            utc(2026, 8, 15, 0),
        )
        .unwrap();
        let occ = occ.expect("an occurrence");
        let e = entry(
            "s1",
            &occ,
            Some("Europe/Brussels"),
            Decision::Fired,
            Some("run-9".into()),
            "2026-08-15T01:00:05Z",
        );
        record(ws.path(), &e).unwrap();

        let back = read(ws.path());
        assert_eq!(back.len(), 1);
        assert_eq!(back[0], e);
        assert_eq!(back[0].timezone.as_deref(), Some("Europe/Brussels"));
        assert!(back[0].local.contains("03:00"), "the local reading is kept: {}", back[0].local);
        assert_eq!(back[0].decision, Decision::Fired);
        assert_eq!(back[0].run_id.as_deref(), Some("run-9"));
    }

    #[test]
    fn a_torn_line_does_not_hide_the_records_before_it() {
        let ws = tempfile::tempdir().unwrap();
        let e = Occurrence {
            occurrence_id: occurrence_id("s1", "2026-09-04T03:00:00Z"),
            schedule_id: "s1".into(),
            scheduled_for: "2026-09-04T03:00:00Z".into(),
            local: "2026-09-04 03:00:00".into(),
            timezone: None,
            decision: Decision::Excluded,
            run_id: None,
            decided_at: "2026-09-04T03:00:01Z".into(),
        };
        record(ws.path(), &e).unwrap();
        use std::io::Write;
        let mut f =
            std::fs::OpenOptions::new().append(true).open(log_path(ws.path())).unwrap();
        write!(f, "{{\"occurrenceId\":\"half").unwrap();
        drop(f);
        assert_eq!(read(ws.path()).len(), 1, "one torn line hid a complete record");
    }
}

/// #296: catch-up decided over the recorded ledger, not over a pointer.
#[cfg(test)]
mod catching_up {
    use super::*;
    use crate::cronzone::{resolve_zone, Exclusions};
    use chrono::TimeZone;

    fn utc(d: u32, h: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 9, d, h, 0, 0).single().expect("one instant")
    }

    fn fired(ws: &Path, at: chrono::DateTime<chrono::Utc>) {
        let scheduled_for = at.to_rfc3339();
        record(
            ws,
            &Occurrence {
                occurrence_id: occurrence_id("s1", &scheduled_for),
                schedule_id: "s1".into(),
                scheduled_for,
                local: String::new(),
                timezone: Some("UTC".into()),
                decision: Decision::Fired,
                run_id: Some("run-1".into()),
                decided_at: at.to_rfc3339(),
            },
        )
        .unwrap();
    }

    fn overdue(
        ws: &Path,
        misfire: Misfire,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Vec<(crate::cronzone::Occurrence, Decision)> {
        catch_up(
            ws,
            "s1",
            "0 3 * * *",
            &resolve_zone(Some("UTC")).unwrap(),
            &Exclusions::default(),
            misfire,
            &Bounds::default(),
            now,
        )
        .unwrap()
    }

    /// The accident this must not be: a schedule nobody has a record of has no
    /// anchor, and measuring from the epoch would fire its whole history.
    #[test]
    fn a_schedule_seen_for_the_first_time_catches_up_nothing() {
        let ws = tempfile::tempdir().unwrap();
        assert!(overdue(ws.path(), Misfire::All, utc(10, 12)).is_empty());
    }

    /// The server was down from the 1st to the 5th.
    #[test]
    fn a_downtime_window_is_decided_by_the_policy() {
        let ws = tempfile::tempdir().unwrap();
        fired(ws.path(), utc(1, 3));

        let all = overdue(ws.path(), Misfire::All, utc(5, 12));
        let days: Vec<u32> = all.iter().map(|(o, _)| chrono::Datelike::day(&o.at)).collect();
        assert_eq!(days, [2, 3, 4, 5], "one per missed day: {days:?}");
        assert!(all.iter().all(|(_, d)| *d == Decision::Fired));

        let latest = overdue(ws.path(), Misfire::Latest, utc(5, 12));
        assert_eq!(latest.len(), 4, "every occurrence still gets an outcome");
        assert_eq!(
            latest.iter().filter(|(_, d)| *d == Decision::Fired).count(),
            1,
            "only the newest runs"
        );
        assert_eq!(latest[3].1, Decision::Fired, "and it is the newest");

        let skip = overdue(ws.path(), Misfire::Skip, utc(5, 12));
        assert_eq!(skip.len(), 4);
        assert!(
            skip.iter().all(|(_, d)| matches!(d, Decision::Skipped { .. })),
            "skip preserves today's behaviour, and now records it"
        );
    }

    /// AC3: a restart re-enumerates the same window and must recognise what it
    /// already decided.
    #[test]
    fn a_restart_does_not_decide_the_same_occurrence_twice() {
        let ws = tempfile::tempdir().unwrap();
        fired(ws.path(), utc(1, 3));

        let first = overdue(ws.path(), Misfire::All, utc(4, 12));
        assert_eq!(first.len(), 3);
        // The tick records what it decided, as the scheduler does.
        for (occ, decision) in &first {
            record(
                ws.path(),
                &entry("s1", occ, Some("UTC"), decision.clone(), None, "2026-09-04T12:00:00Z"),
            )
            .unwrap();
        }

        let again = overdue(ws.path(), Misfire::All, utc(4, 12));
        assert!(again.is_empty(), "a restart would have run these again: {again:?}");
    }

    /// An excluded day is not an occurrence, so it is not one to catch up on.
    #[test]
    fn an_excluded_day_is_not_caught_up() {
        let ws = tempfile::tempdir().unwrap();
        fired(ws.path(), utc(1, 3));
        let out = catch_up(
            ws.path(),
            "s1",
            "0 3 * * *",
            &resolve_zone(Some("UTC")).unwrap(),
            &serde_json::from_value(serde_json::json!({ "dates": ["2026-09-03"] })).unwrap(),
            Misfire::All,
            &Bounds::default(),
            utc(5, 12),
        )
        .unwrap();
        let days: Vec<u32> = out.iter().map(|(o, _)| chrono::Datelike::day(&o.at)).collect();
        assert_eq!(days, [2, 4, 5], "the 3rd was excluded: {days:?}");
    }

    /// AC2, through the whole path rather than only through `missed`.
    #[test]
    fn a_long_outage_is_bounded() {
        let ws = tempfile::tempdir().unwrap();
        fired(ws.path(), utc(1, 3));
        let out = catch_up(
            ws.path(),
            "s1",
            "0 3 * * *",
            &resolve_zone(Some("UTC")).unwrap(),
            &Exclusions::default(),
            Misfire::All,
            &Bounds { max_catchup_runs: 3, max_catchup_age_days: 365 },
            utc(20, 12),
        )
        .unwrap();
        assert_eq!(out.len(), 3, "bounded to 3");
        let days: Vec<u32> = out.iter().map(|(o, _)| chrono::Datelike::day(&o.at)).collect();
        assert_eq!(days, [18, 19, 20], "and kept the recent end: {days:?}");
    }

    #[test]
    fn nothing_overdue_decides_nothing() {
        let ws = tempfile::tempdir().unwrap();
        fired(ws.path(), utc(5, 3));
        assert!(overdue(ws.path(), Misfire::All, utc(5, 4)).is_empty(), "not due again yet");
    }
}

/// #296: what an occurrence becomes when the previous run is still going.
#[cfg(test)]
mod overlapping {
    use super::*;

    fn queued(ws: &Path, schedule: &str, at: &str) -> Occurrence {
        let occ = Occurrence {
            occurrence_id: occurrence_id(schedule, at),
            schedule_id: schedule.into(),
            scheduled_for: at.into(),
            local: String::new(),
            timezone: None,
            decision: Decision::Queued { reason: "waiting".into() },
            run_id: None,
            decided_at: at.into(),
        };
        record(ws, &occ).unwrap();
        occ
    }

    /// Two instants whose ids sort the other way round from their times, so the
    /// order `waiting` returns can only be the times' if it sorts by them.
    fn times_whose_ids_sort_backwards() -> (String, String) {
        for minute in 1..60 {
            let (early, late) = ("2026-11-13T09:00:00+00:00".to_string(), format!("2026-11-13T10:{minute:02}:00+00:00"));
            if occurrence_id("s", &early) > occurrence_id("s", &late) {
                return (early, late);
            }
        }
        unreachable!("an hour of minutes has a pair whose ids sort backwards")
    }

    #[test]
    fn an_occurrence_waits_until_a_later_record_decides_it() {
        let ws = tempfile::tempdir().unwrap();
        let (early_at, late_at) = times_whose_ids_sort_backwards();
        // Recorded newest first, so neither the ids nor the ledger's order is time.
        let late = queued(ws.path(), "s", &late_at);
        let early = queued(ws.path(), "s", &early_at);
        queued(ws.path(), "other", "2026-11-13T09:00:00+00:00");
        let ids = |w: Vec<Occurrence>| w.into_iter().map(|o| o.occurrence_id).collect::<Vec<_>>();
        assert_eq!(
            ids(waiting(ws.path(), "s")),
            vec![early.occurrence_id.clone(), late.occurrence_id.clone()],
            "oldest first, and only this schedule's"
        );

        record(ws.path(), &decided(&early, Decision::Fired, "2026-11-13T10:05:00+00:00")).unwrap();
        assert_eq!(ids(waiting(ws.path(), "s")), vec![late.occurrence_id.clone()], "it ran");
        let skipped = Decision::Skipped { reason: "replaced".into() };
        record(ws.path(), &decided(&late, skipped, "2026-11-13T10:06:00+00:00")).unwrap();
        assert!(waiting(ws.path(), "s").is_empty(), "skipped is decided too");
    }

    #[test]
    fn a_queued_decision_is_recorded_as_queued_with_its_reason() {
        let occ = Occurrence {
            occurrence_id: "occ-1".into(),
            schedule_id: "s".into(),
            scheduled_for: "2026-11-13T09:00:00+00:00".into(),
            local: String::new(),
            timezone: None,
            decision: Decision::Queued { reason: "waiting".into() },
            run_id: None,
            decided_at: "2026-11-13T09:00:01+00:00".into(),
        };
        let json = serde_json::to_value(&occ).unwrap();
        assert_eq!(json["decision"], "queued");
        assert_eq!(json["reason"], "waiting");
        assert_eq!(serde_json::from_value::<Occurrence>(json).unwrap(), occ);
        assert_eq!(occ.decision.as_str(), "queued");
    }

    #[test]
    fn skip_is_the_default_and_the_policy_reads_as_written() {
        assert_eq!(Overlap::default(), Overlap::Skip);
        for (text, policy) in [
            ("skip", Overlap::Skip),
            ("queue", Overlap::Queue),
            ("replace", Overlap::Replace),
            ("allow", Overlap::Allow),
        ] {
            assert_eq!(serde_json::from_value::<Overlap>(serde_json::json!(text)).unwrap(), policy);
        }
        assert!(serde_json::from_value::<Overlap>(serde_json::json!("qeue")).is_err());
    }

    #[test]
    fn each_policy_decides_an_overlap_its_own_way() {
        let ws = tempfile::tempdir().unwrap();
        let older = queued(ws.path(), "s", "2026-11-13T09:00:00+00:00");

        assert_eq!(on_overlap(Overlap::Allow, &[]), OnOverlap::Run);
        assert!(matches!(on_overlap(Overlap::Skip, &[]), OnOverlap::Skip(r) if r.contains("still going")));
        assert!(matches!(
            on_overlap(Overlap::Queue, &[]),
            OnOverlap::Wait { stop_running: false, ref supersede, .. } if supersede.is_empty()
        ));
        // One waits at most: a second would repeat the first with the same inputs.
        assert!(matches!(
            on_overlap(Overlap::Queue, std::slice::from_ref(&older)),
            OnOverlap::Skip(r) if r.contains("already waiting")
        ));
        // Replace takes the place of whatever was waiting, and stops the run.
        assert_eq!(
            on_overlap(Overlap::Replace, std::slice::from_ref(&older)),
            OnOverlap::Wait {
                reason: "stopping the previous run to run this one".into(),
                stop_running: true,
                supersede: vec![older.clone()],
            }
        );
    }
}
