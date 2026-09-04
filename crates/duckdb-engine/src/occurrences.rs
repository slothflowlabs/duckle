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
}

impl Decision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Decision::Fired => "fired",
            Decision::Skipped { .. } => "skipped",
            Decision::Excluded => "excluded",
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

/// Append one occurrence. Append-only, like the materialization log and for the
/// same reason: a writer never rewrites, so a concurrent reader cannot see half
/// a record.
pub fn record(workspace: &Path, occ: &Occurrence) -> Result<(), String> {
    let path = log_path(workspace);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let line = serde_json::to_string(occ).map_err(|e| e.to_string())?;
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    writeln!(f, "{line}").map_err(|e| format!("{}: {e}", path.display()))
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
