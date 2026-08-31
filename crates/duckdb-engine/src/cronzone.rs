//! #318: evaluate a cron schedule in an explicit IANA time zone.
//!
//! ## Why this is shared code
//!
//! There are two schedulers - the desktop/embedded one in `duckle-scheduler`
//! and the web console's in `duckle-runner`'s `serve` - and they have disagreed
//! about time before. #194 was exactly that: one evaluated cron in UTC and the
//! other in local time, so the same expression fired at two different moments
//! depending on which surface owned it. Both now call in here, so a third
//! implementation cannot quietly appear.
//!
//! ## What a zone changes, and what it does not
//!
//! A cron expression is **civil time**: "0 0 3 * * *" means three in the
//! morning as a person reads a clock. Which instant that is depends on the
//! zone, and on the day, because offsets move. An **interval** schedule is an
//! elapsed duration and is deliberately untouched by any of this: "every 24
//! hours" is 24 hours, not "the same clock time tomorrow", and a zone must not
//! silently turn one into the other.
//!
//! With no zone configured the machine's local zone is used, which is the
//! behaviour #194 settled on and what every existing schedule already means.
//!
//! ## Daylight saving, decided rather than discovered
//!
//! Twice a year a civil time is not a single instant, and a scheduler that has
//! not decided what to do will do something different on each surface, or
//! something different after a restart.
//!
//! - **Nonexistent** (spring forward): 02:30 simply does not happen. The
//!   occurrence is SKIPPED and the next one is taken, rather than being nudged
//!   to 03:30 - a job asked to run at 02:30 on a day with no 02:30 has not been
//!   missed by the scheduler, the day is short. Reported so a caller can hand
//!   it to a misfire policy instead of it vanishing.
//! - **Ambiguous** (autumn back): 02:30 happens twice. The EARLIER instant is
//!   taken, so the job runs once, at the first 02:30. Running twice would be a
//!   duplicate nobody asked for; running at the later one would delay it by an
//!   hour for no reason. The rule is arbitrary but it is written down, tested,
//!   and the same after a restart, which is the property that matters.

use chrono::{DateTime, LocalResult, NaiveDateTime, Offset, TimeZone, Utc};

/// The zone a schedule is evaluated in.
#[derive(Debug, Clone, PartialEq)]
pub enum Zone {
    /// No zone configured: the machine's local zone, as #194 settled.
    Local,
    Named(chrono_tz::Tz),
}

/// Resolve a configured zone name.
///
/// An unknown name is an ERROR, never a silent fall back to local. A schedule
/// that says `Europe/Brussel` (a typo) must not quietly run on UTC in a
/// container and be discovered a quarter later.
pub fn resolve_zone(tz: Option<&str>) -> Result<Zone, String> {
    match tz.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(Zone::Local),
        Some(name) => name.parse::<chrono_tz::Tz>().map(Zone::Named).map_err(|_| {
            format!(
                "unknown time zone {name:?}. Use an IANA name such as Europe/Brussels or \
                 America/New_York, or leave it unset to use the machine's own zone."
            )
        }),
    }
}

/// The `cron` crate expects a 6- or 7-field expression (seconds first). Accept a
/// standard 5-field cron by prepending a zero seconds field.
///
/// Shared so the two schedulers cannot drift: a hand-edited 5-field expression
/// used to parse to None on one surface and silently never fire.
pub fn normalize_cron(expr: &str) -> Option<String> {
    match expr.split_whitespace().count() {
        5 => Some(format!("0 {}", expr)),
        6 | 7 => Some(expr.to_string()),
        _ => None,
    }
}

/// Why an occurrence was not the one the expression literally named.
#[derive(Debug, Clone, PartialEq)]
pub enum Adjustment {
    /// The civil time does not exist on that day (spring forward). Skipped.
    NonexistentLocalTime(String),
    /// The civil time happens twice (autumn back). The earlier one was taken.
    AmbiguousLocalTime(String),
}

/// One resolved occurrence.
#[derive(Debug, Clone, PartialEq)]
pub struct Occurrence {
    /// The absolute instant it fires.
    pub at: DateTime<Utc>,
    /// What the clock reads in the schedule's own zone, kept because "03:00
    /// Brussels" is what the operator asked for and the UTC instant is not
    /// recognisable to them.
    pub local: String,
    /// The zone's offset at that instant, in seconds east of UTC.
    pub offset_seconds: i32,
    /// Set when a daylight-saving transition made the literal reading of the
    /// expression impossible or ambiguous.
    pub adjustment: Option<Adjustment>,
}

/// Turn a civil time in `zone` into an instant, deciding the two daylight
/// saving cases rather than letting them decide themselves.
///
/// `Ok(None)` means the civil time does not exist and the caller should take
/// the next occurrence.
fn civil_to_instant<T: TimeZone>(
    zone: &T,
    naive: NaiveDateTime,
) -> (Option<DateTime<Utc>>, Option<Adjustment>)
where
    T::Offset: std::fmt::Display,
{
    match zone.from_local_datetime(&naive) {
        LocalResult::Single(dt) => (Some(dt.with_timezone(&Utc)), None),
        // Twice. Take the first, so the job runs once and always at the same
        // one of the two.
        LocalResult::Ambiguous(earlier, _later) => (
            Some(earlier.with_timezone(&Utc)),
            Some(Adjustment::AmbiguousLocalTime(naive.to_string())),
        ),
        // Never. Skip it rather than inventing a nearby time.
        LocalResult::None => (None, Some(Adjustment::NonexistentLocalTime(naive.to_string()))),
    }
}

/// The next occurrence strictly after `after`, evaluated as civil time in
/// `zone`.
///
/// Returns the adjustments that were skipped on the way, so a caller can record
/// "this schedule had no 02:30 today" rather than the occurrence silently
/// disappearing.
pub fn next_after(
    expr: &str,
    zone: &Zone,
    after: DateTime<Utc>,
) -> Result<(Option<Occurrence>, Vec<Adjustment>), String> {
    let normalized = normalize_cron(expr)
        .ok_or_else(|| format!("{expr:?} is not a 5, 6 or 7 field cron expression"))?;
    let schedule: cron::Schedule = normalized
        .parse()
        .map_err(|e| format!("cannot parse cron {expr:?}: {e}"))?;
    match zone {
        Zone::Local => walk(&schedule, &chrono::Local, after, expr),
        Zone::Named(tz) => walk(&schedule, tz, after, expr),
    }
}

/// Enumerate the CIVIL times an expression names, then map each into the zone.
///
/// The enumeration deliberately happens in naive space - the cursor is a naive
/// civil time carried in a `DateTime<Utc>` purely as a vehicle - rather than by
/// asking the cron crate to iterate in the target zone directly. Iterating in
/// the zone hides the interesting case: the crate resolves civil times itself
/// and simply never yields one that does not exist, so a spring-forward gap
/// becomes invisible and cannot be reported. Here the gap is seen, decided, and
/// handed back.
fn walk<T: TimeZone>(
    schedule: &cron::Schedule,
    tz: &T,
    after: DateTime<Utc>,
    expr: &str,
) -> Result<(Option<Occurrence>, Vec<Adjustment>), String>
where
    T::Offset: std::fmt::Display,
{
    // A spring-forward gap can only swallow a run of consecutive occurrences,
    // never an unbounded one. The cap is a backstop so an expression naming a
    // civil time that never exists reports rather than hangs.
    const MAX_SKIPS: usize = 64;
    let mut skipped: Vec<Adjustment> = Vec::new();

    let start_civil = after.with_timezone(tz).naive_local();
    let mut cursor: DateTime<Utc> = Utc.from_utc_datetime(&start_civil);

    for _ in 0..MAX_SKIPS {
        let Some(next) = schedule.after(&cursor).next() else {
            return Ok((None, skipped));
        };
        // Its naive part IS the civil time the expression named; the Utc
        // wrapper carried it here and means nothing else.
        let civil = next.naive_utc();
        cursor = next;
        let (instant, adjustment) = civil_to_instant(tz, civil);
        match instant {
            Some(at) => {
                let offset_seconds = at.with_timezone(tz).offset().fix().local_minus_utc();
                return Ok((
                    Some(Occurrence {
                        at,
                        local: civil.to_string(),
                        offset_seconds,
                        adjustment,
                    }),
                    skipped,
                ));
            }
            None => skipped.extend(adjustment),
        }
    }
    Err(format!(
        "cron {expr:?} named no civil time that exists in this zone within {MAX_SKIPS} occurrences"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    /// A typo must not become "runs on UTC in a container", found a quarter later.
    #[test]
    fn an_unknown_zone_is_refused_rather_than_falling_back() {
        let e = resolve_zone(Some("Europe/Brussel")).unwrap_err();
        assert!(e.contains("Europe/Brussel"), "must name it: {e}");
        assert!(e.contains("IANA"), "must say what a good one looks like: {e}");
        assert_eq!(resolve_zone(None), Ok(Zone::Local));
        assert_eq!(resolve_zone(Some("  ")), Ok(Zone::Local), "blank is unset");
        assert!(matches!(resolve_zone(Some("Europe/Brussels")), Ok(Zone::Named(_))));
    }

    /// The point of the issue: 03:00 Brussels stays 03:00 Brussels wherever the
    /// runner is deployed. In January that is 02:00 UTC.
    #[test]
    fn a_named_zone_fires_at_the_civil_time_it_names() {
        let zone = resolve_zone(Some("Europe/Brussels")).unwrap();
        let (occ, _) = next_after("0 0 3 * * *", &zone, utc(2026, 1, 10, 12, 0)).unwrap();
        let occ = occ.expect("an occurrence");
        assert_eq!(occ.at, utc(2026, 1, 11, 2, 0), "03:00 CET is 02:00 UTC");
        assert_eq!(occ.local, "2026-01-11 03:00:00");
        assert_eq!(occ.offset_seconds, 3600);
    }

    /// And in summer the same expression is a different instant, which is the
    /// whole reason civil time is not a fixed offset.
    #[test]
    fn the_same_expression_is_a_different_instant_in_summer() {
        let zone = resolve_zone(Some("Europe/Brussels")).unwrap();
        let (occ, _) = next_after("0 0 3 * * *", &zone, utc(2026, 7, 10, 12, 0)).unwrap();
        assert_eq!(occ.unwrap().at, utc(2026, 7, 11, 1, 0), "03:00 CEST is 01:00 UTC");
    }

    /// Spring forward: 02:30 does not exist on 2026-03-29 in Brussels. The
    /// occurrence is skipped, not nudged to 03:30, and the skip is reported so
    /// it can be handed to a misfire policy rather than vanishing.
    #[test]
    fn a_nonexistent_civil_time_is_skipped_and_reported() {
        let zone = resolve_zone(Some("Europe/Brussels")).unwrap();
        let (occ, skipped) = next_after("0 30 2 * * *", &zone, utc(2026, 3, 28, 12, 0)).unwrap();
        let occ = occ.expect("an occurrence");
        assert!(
            matches!(skipped.first(), Some(Adjustment::NonexistentLocalTime(_))),
            "the missing 02:30 must be reported, not silently dropped: {skipped:?}"
        );
        assert_eq!(
            occ.local, "2026-03-30 02:30:00",
            "it runs the NEXT day, rather than being nudged to 03:30 on the short day"
        );
    }

    /// Autumn back: 02:30 happens twice on 2026-10-25. It must fire once, at
    /// the earlier of the two, and say that it was ambiguous.
    #[test]
    fn an_ambiguous_civil_time_fires_once_at_the_earlier_instant() {
        let zone = resolve_zone(Some("Europe/Brussels")).unwrap();
        let (occ, _) = next_after("0 30 2 * * *", &zone, utc(2026, 10, 24, 12, 0)).unwrap();
        let occ = occ.expect("an occurrence");
        assert_eq!(occ.local, "2026-10-25 02:30:00");
        // The earlier 02:30 is still CEST (+2), so 00:30 UTC. The later one
        // would be 01:30 UTC.
        assert_eq!(occ.at, utc(2026, 10, 25, 0, 30), "the EARLIER of the two");
        assert!(
            matches!(occ.adjustment, Some(Adjustment::AmbiguousLocalTime(_))),
            "and it must say so: {:?}",
            occ.adjustment
        );
    }

    /// A five-field expression is accepted on both surfaces, or a hand-edited
    /// schedule silently never fires.
    #[test]
    fn five_field_expressions_are_accepted() {
        assert_eq!(normalize_cron("0 3 * * *").as_deref(), Some("0 0 3 * * *"));
        assert_eq!(normalize_cron("0 0 3 * * *").as_deref(), Some("0 0 3 * * *"));
        assert_eq!(normalize_cron("nonsense"), None);
        let zone = resolve_zone(Some("UTC")).unwrap();
        let (occ, _) = next_after("0 3 * * *", &zone, utc(2026, 1, 10, 12, 0)).unwrap();
        assert_eq!(occ.unwrap().at, utc(2026, 1, 11, 3, 0));
    }

    /// A bad expression is an error a caller can report, not a schedule that
    /// quietly never runs.
    #[test]
    fn an_unparseable_expression_is_an_error() {
        let zone = resolve_zone(None).unwrap();
        assert!(next_after("not a cron", &zone, Utc::now()).is_err());
    }
}
