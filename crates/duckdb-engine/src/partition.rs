//! #295: a pipeline that processes one slice at a time, and the slices it has.
//!
//! A backfill over three years of daily data is not one opaque multi-hour run.
//! It is a thousand independently addressable ones, and the difference matters
//! the moment the four hundredth fails: with slices you retry that day, and
//! without them you start again.
//!
//! This module is only the definition and the generator - what the slices ARE.
//! Which of them have run, and how that survives a restart, is the backfill
//! store; keeping them apart is what lets the interesting part be tested
//! without a filesystem.
//!
//! ## Boundaries are computed in the partition's own zone
//!
//! "One day" in `Europe/Brussels` is 23 hours in March and 25 in October, and a
//! backfill that generated UTC days would silently process an hour twice and
//! skip another. The zone is part of the definition for that reason, and the
//! boundaries come out as instants, so what the pipeline receives is not
//! ambiguous.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cadence {
    Hour,
    Day,
    Week,
    Month,
    Year,
}

/// How a pipeline is sliced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PartitionDef {
    /// One slice per calendar unit, in a named zone.
    #[serde(rename_all = "camelCase")]
    Time {
        cadence: Cadence,
        #[serde(default = "utc")]
        timezone: String,
        /// The parameter the window start is bound to.
        #[serde(default = "default_start")]
        parameter_start: String,
        #[serde(default = "default_end")]
        parameter_end: String,
    },
    /// One slice per key, fixed in the document.
    #[serde(rename_all = "camelCase")]
    Static {
        keys: Vec<String>,
        #[serde(default = "default_key_param")]
        parameter: String,
    },
}

fn utc() -> String {
    "UTC".to_string()
}
fn default_start() -> String {
    "window_start".to_string()
}
fn default_end() -> String {
    "window_end".to_string()
}
fn default_key_param() -> String {
    "partition".to_string()
}

/// One slice: what it is called, and what the pipeline is given for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Partition {
    /// Stable and human-readable: `2026-08-29`, `2026-08`, `BE`. This is what
    /// names the run, so it is what decides whether two requests are the same
    /// slice - and therefore what stops the same partition being launched
    /// twice.
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// The parameters this slice binds, ready for the #317 boundary.
    pub params: BTreeMap<String, String>,
}

fn zone(name: &str) -> Result<chrono_tz::Tz, String> {
    name.parse::<chrono_tz::Tz>()
        .map_err(|_| format!("unknown timezone {name:?}"))
}

/// The first instant of the slice containing `date`, in `tz`.
fn floor(date: chrono::NaiveDate, cadence: Cadence, tz: chrono_tz::Tz) -> Option<chrono::DateTime<chrono_tz::Tz>> {
    use chrono::{Datelike, NaiveTime};
    let day = match cadence {
        Cadence::Hour | Cadence::Day => date,
        // ISO weeks start on Monday, which is what every European data
        // publication this is aimed at means by "week".
        Cadence::Week => date - chrono::Duration::days(date.weekday().num_days_from_monday() as i64),
        Cadence::Month => date.with_day(1)?,
        Cadence::Year => date.with_month(1)?.with_day(1)?,
    };
    let naive = day.and_time(NaiveTime::MIN);
    // A DST spring-forward can delete local midnight. `.latest()` then gives
    // the first instant that does exist, which is the honest start of that day
    // rather than a time nobody experienced.
    use chrono::TimeZone;
    tz.from_local_datetime(&naive)
        .latest()
        .or_else(|| tz.from_local_datetime(&naive).earliest())
}

fn advance(
    at: chrono::DateTime<chrono_tz::Tz>,
    cadence: Cadence,
    tz: chrono_tz::Tz,
) -> Option<chrono::DateTime<chrono_tz::Tz>> {
    use chrono::{Datelike, Months, NaiveTime, TimeZone};
    if cadence == Cadence::Hour {
        return Some(at + chrono::Duration::hours(1));
    }
    let local = at.date_naive();
    let next = match cadence {
        Cadence::Hour => unreachable!("handled above"),
        Cadence::Day => local + chrono::Duration::days(1),
        Cadence::Week => local + chrono::Duration::days(7),
        Cadence::Month => local.checked_add_months(Months::new(1))?.with_day(1)?,
        Cadence::Year => local.checked_add_months(Months::new(12))?.with_month(1)?.with_day(1)?,
    };
    let naive = next.and_time(NaiveTime::MIN);
    tz.from_local_datetime(&naive)
        .latest()
        .or_else(|| tz.from_local_datetime(&naive).earliest())
}

fn key_for(at: chrono::DateTime<chrono_tz::Tz>, cadence: Cadence) -> String {
    match cadence {
        Cadence::Hour => at.format("%Y-%m-%dT%H").to_string(),
        Cadence::Day | Cadence::Week => at.format("%Y-%m-%d").to_string(),
        Cadence::Month => at.format("%Y-%m").to_string(),
        Cadence::Year => at.format("%Y").to_string(),
    }
}

/// Every slice from `from` up to and including the slice containing `to`.
///
/// Inclusive of the end slice because an operator writing `--from 2020-01-01
/// --to 2026-08-29` means to process the 29th. A half-open range here would
/// silently drop the last day of every backfill anybody ever ran.
pub fn generate(def: &PartitionDef, from: &str, to: &str) -> Result<Vec<Partition>, String> {
    match def {
        PartitionDef::Static { keys, parameter } => {
            let mut seen = std::collections::BTreeSet::new();
            let mut out = Vec::new();
            for key in keys {
                let key = key.trim();
                if key.is_empty() || !seen.insert(key.to_string()) {
                    continue;
                }
                out.push(Partition {
                    key: key.to_string(),
                    start: None,
                    end: None,
                    params: BTreeMap::from([
                        (parameter.clone(), key.to_string()),
                        ("partition_key".to_string(), key.to_string()),
                    ]),
                });
            }
            Ok(out)
        }
        PartitionDef::Time { cadence, timezone, parameter_start, parameter_end } => {
            let tz = zone(timezone)?;
            let parse = |s: &str, what: &str| {
                chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .map_err(|_| format!("{what} {s:?} is not a date (YYYY-MM-DD)"))
            };
            let from_date = parse(from, "--from")?;
            let to_date = parse(to, "--to")?;
            if to_date < from_date {
                return Err(format!("--to {to} is before --from {from}"));
            }
            let mut at = floor(from_date, *cadence, tz)
                .ok_or_else(|| format!("cannot place {from} in {timezone}"))?;
            let stop = floor(to_date, *cadence, tz)
                .ok_or_else(|| format!("cannot place {to} in {timezone}"))?;
            let mut out = Vec::new();
            // A guard rather than a while-true: a cadence and zone combination
            // that failed to advance would otherwise spin forever building an
            // ever-growing vector, which is a worse failure than a wrong count.
            while at <= stop && out.len() < 200_000 {
                let next = advance(at, *cadence, tz)
                    .ok_or_else(|| "cannot advance past this date".to_string())?;
                if next <= at {
                    return Err("the cadence did not advance".to_string());
                }
                let start = at.to_rfc3339();
                let end = next.to_rfc3339();
                let key = key_for(at, *cadence);
                out.push(Partition {
                    key: key.clone(),
                    start: Some(start.clone()),
                    end: Some(end.clone()),
                    params: BTreeMap::from([
                        (parameter_start.clone(), start),
                        (parameter_end.clone(), end),
                        // #295 asks that every partition run RECEIVE the key,
                        // not only be recorded against it. It is the only
                        // value that names a file or a directory the way a
                        // person would - `2020-01-03.csv` - so without it a
                        // time-partitioned pipeline has to reconstruct the
                        // date from a timestamp in SQL.
                        ("partition_key".to_string(), key),
                    ]),
                });
                at = next;
            }
            Ok(out)
        }
    }
}

/// The partition definition a pipeline document declares, if any.
pub fn of(doc: &serde_json::Value) -> Option<PartitionDef> {
    serde_json::from_value(doc.get("partition")?.clone()).ok()
}

/// The parameters this definition binds for one key.
///
/// #325: a consumer triggered by a partitioned publication should receive the
/// same window the producer had, and the honest way to get it is to ask the
/// definition rather than to copy values across. `generate` already knows what
/// a key binds; this asks it about one key.
///
/// `None` when the key is not one this definition produces - a `2026-09-03`
/// against a `Static` definition listing `BE`, `NL`, or a malformed date. That
/// is a refusal rather than a guess: binding a window nobody asked for is how a
/// consumer writes correct-looking rows into the wrong day.
pub fn params_for(def: &PartitionDef, key: &str) -> Option<BTreeMap<String, String>> {
    let key = key.trim();
    match def {
        // Generating a range would enumerate every key; a static definition is
        // a fixed list, so the answer is a lookup.
        PartitionDef::Static { .. } => generate(def, key, key)
            .ok()?
            .into_iter()
            .find(|p| p.key == key)
            .map(|p| p.params),
        // A time key names exactly one slice, so generating from it to itself
        // produces that slice and no other.
        PartitionDef::Time { .. } => {
            let day = match key.split('T').next().unwrap_or(key) {
                d if d.len() == 4 => format!("{d}-01-01"),
                d if d.len() == 7 => format!("{d}-01"),
                d => d.to_string(),
            };
            let mut found = generate(def, &day, &day).ok()?;
            match found.len() {
                1 if found[0].key == key => Some(found.remove(0).params),
                _ => None,
            }
        }
    }
}

/// The canonical key naming the slice that contains this date and hour.
///
/// #326 parses a key out of a filename - `D20260901.zip` - and needs the same
/// spelling [`generate`] would have produced for that day, or the chain and
/// the ledger would be talking about the same slice under two names.
///
/// `hour` is only read for [`Cadence::Hour`]; every coarser cadence names a
/// whole calendar unit and the hour inside it is not part of the label.
pub fn key_of(date: chrono::NaiveDate, hour: u32, cadence: Cadence) -> Option<String> {
    if hour > 23 {
        return None;
    }
    let tz = chrono_tz::UTC;
    let at = floor(date, cadence, tz)?;
    let at = match cadence {
        Cadence::Hour => at + chrono::Duration::hours(hour as i64),
        _ => at,
    };
    Some(key_for(at, cadence))
}

/// The key immediately after `key`. The whole of #326's continuity primitive.
///
/// Computed with the same `floor`/`advance` pair [`generate`] uses rather than
/// a second arithmetic, because "what comes after 2026-02-28" has to be one
/// answer: a duplicate would have to be independently right about leap years,
/// month lengths and the Monday an ISO week starts on, and the copy that drifts
/// is the one nobody is testing.
///
/// A key is a calendar LABEL, not an instant - `2026-09-01` is a publisher's
/// name for a day, not a moment - so the walk is done in UTC. That is not an
/// assumption about where the data came from: for day, week, month and year the
/// successor label is the same in every zone, because it is arithmetic on the
/// label. `Hour` is the one cadence where a zone with DST would disagree, and a
/// feed that names its files by local hour across a fold has an ambiguous key
/// of its own making, which no successor function can repair.
pub fn next_key(key: &str, cadence: Cadence) -> Option<String> {
    let tz = chrono_tz::UTC;
    let at = key_instant(key, cadence, tz)?;
    let next = advance(at, cadence, tz)?;
    if next <= at {
        return None;
    }
    Some(key_for(next, cadence))
}

/// A canonical key, read back as the instant it names.
fn key_instant(key: &str, cadence: Cadence, tz: chrono_tz::Tz) -> Option<chrono::DateTime<chrono_tz::Tz>> {
    let key = key.trim();
    if cadence == Cadence::Hour {
        let (date, hour) = key.split_once('T')?;
        let date = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
        let hour: u32 = hour.parse().ok()?;
        if hour > 23 {
            return None;
        }
        return Some(floor(date, cadence, tz)? + chrono::Duration::hours(hour as i64));
    }
    // A month key is the month's first day and a year key its first day of
    // January; `floor` then normalises a week key onto its Monday.
    let text = match cadence {
        Cadence::Year => format!("{key}-01-01"),
        Cadence::Month => format!("{key}-01"),
        _ => key.to_string(),
    };
    floor(chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d").ok()?, cadence, tz)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(cadence: Cadence, tz: &str) -> PartitionDef {
        PartitionDef::Time {
            cadence,
            timezone: tz.to_string(),
            parameter_start: "window_start".into(),
            parameter_end: "window_end".into(),
        }
    }

    #[test]
    fn a_day_range_is_one_partition_per_day_and_includes_the_last() {
        // An operator writing --to 2020-01-05 means to process the 5th. A
        // half-open range would silently drop the last day of every backfill.
        let p = generate(&time(Cadence::Day, "UTC"), "2020-01-01", "2020-01-05").unwrap();
        assert_eq!(p.len(), 5);
        assert_eq!(p[0].key, "2020-01-01");
        assert_eq!(p[4].key, "2020-01-05");
    }

    #[test]
    fn each_window_ends_exactly_where_the_next_begins() {
        // A gap loses rows and an overlap counts them twice; both are silent.
        let p = generate(&time(Cadence::Day, "Europe/Brussels"), "2020-03-27", "2020-03-31").unwrap();
        for pair in p.windows(2) {
            assert_eq!(pair[0].end, pair[1].start, "{:?} -> {:?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn a_spring_forward_day_is_twenty_three_hours_not_twenty_four() {
        // Brussels loses an hour on 2026-03-29. A backfill generating UTC days
        // would process an hour twice somewhere and skip another.
        let p = generate(&time(Cadence::Day, "Europe/Brussels"), "2026-03-29", "2026-03-29").unwrap();
        assert_eq!(p.len(), 1);
        let start = chrono::DateTime::parse_from_rfc3339(p[0].start.as_ref().unwrap()).unwrap();
        let end = chrono::DateTime::parse_from_rfc3339(p[0].end.as_ref().unwrap()).unwrap();
        assert_eq!((end - start).num_hours(), 23, "{:?}", p[0]);
    }

    #[test]
    fn an_autumn_back_day_is_twenty_five_hours() {
        let p = generate(&time(Cadence::Day, "Europe/Brussels"), "2026-10-25", "2026-10-25").unwrap();
        let start = chrono::DateTime::parse_from_rfc3339(p[0].start.as_ref().unwrap()).unwrap();
        let end = chrono::DateTime::parse_from_rfc3339(p[0].end.as_ref().unwrap()).unwrap();
        assert_eq!((end - start).num_hours(), 25, "{:?}", p[0]);
    }

    #[test]
    fn months_and_years_start_on_their_first_day() {
        let m = generate(&time(Cadence::Month, "UTC"), "2020-01-15", "2020-04-02").unwrap();
        assert_eq!(
            m.iter().map(|p| p.key.as_str()).collect::<Vec<_>>(),
            vec!["2020-01", "2020-02", "2020-03", "2020-04"],
            "the slice CONTAINING the from-date is the first one"
        );
        let y = generate(&time(Cadence::Year, "UTC"), "2019-06-01", "2021-02-01").unwrap();
        assert_eq!(y.iter().map(|p| p.key.as_str()).collect::<Vec<_>>(), vec!["2019", "2020", "2021"]);
    }

    #[test]
    fn a_week_starts_on_monday() {
        // 2026-08-29 is a Saturday; its week began on the 24th.
        let p = generate(&time(Cadence::Week, "UTC"), "2026-08-29", "2026-08-29").unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].key, "2026-08-24");
    }

    #[test]
    fn the_window_is_bound_to_the_parameters_the_pipeline_named() {
        let def = PartitionDef::Time {
            cadence: Cadence::Day,
            timezone: "UTC".into(),
            parameter_start: "from_ts".into(),
            parameter_end: "to_ts".into(),
        };
        let p = generate(&def, "2020-01-01", "2020-01-01").unwrap();
        assert_eq!(p[0].params.get("from_ts"), p[0].start.as_ref());
        assert_eq!(p[0].params.get("to_ts"), p[0].end.as_ref());
    }

    #[test]
    fn every_partition_receives_its_own_key() {
        // #295: a run should RECEIVE partition_key, not only be recorded
        // against it. It is the only value that names a file the way a person
        // would, so without it a daily pipeline has to rebuild the date from a
        // timestamp in SQL.
        let p = generate(&time(Cadence::Day, "UTC"), "2020-01-03", "2020-01-03").unwrap();
        assert_eq!(p[0].params.get("partition_key").map(String::as_str), Some("2020-01-03"));
        let def = PartitionDef::Static {
            keys: vec!["BE".into()],
            parameter: "jurisdiction".into(),
        };
        let s = generate(&def, "", "").unwrap();
        assert_eq!(s[0].params.get("partition_key").map(String::as_str), Some("BE"));
        assert_eq!(s[0].params.get("jurisdiction").map(String::as_str), Some("BE"));
    }

    #[test]
    fn static_keys_are_deduplicated_and_bound_to_one_parameter() {
        let def = PartitionDef::Static {
            keys: vec!["BE".into(), "NL".into(), " BE ".into(), "".into(), "GB".into()],
            parameter: "jurisdiction".into(),
        };
        let p = generate(&def, "", "").unwrap();
        assert_eq!(p.iter().map(|x| x.key.as_str()).collect::<Vec<_>>(), vec!["BE", "NL", "GB"]);
        assert_eq!(p[0].params.get("jurisdiction").map(String::as_str), Some("BE"));
        assert!(p[0].start.is_none(), "a static key has no window");
    }

    #[test]
    fn a_backwards_range_is_refused_rather_than_producing_nothing() {
        // Producing nothing would read as "there was nothing to do".
        let e = generate(&time(Cadence::Day, "UTC"), "2020-02-01", "2020-01-01").unwrap_err();
        assert!(e.contains("before"), "{e}");
    }

    #[test]
    fn a_bad_date_or_zone_is_named() {
        assert!(generate(&time(Cadence::Day, "UTC"), "01/02/2020", "2020-01-01").is_err());
        assert!(generate(&time(Cadence::Day, "Mars/Olympus"), "2020-01-01", "2020-01-02").is_err());
    }

    #[test]
    fn a_definition_is_read_off_the_document() {
        let doc = serde_json::json!({
            "partition": { "type": "time", "cadence": "day", "timezone": "Europe/Brussels" },
            "nodes": []
        });
        let def = of(&doc).expect("a definition");
        // Defaults fill in the parameter names.
        let p = generate(&def, "2020-01-01", "2020-01-01").unwrap();
        assert!(p[0].params.contains_key("window_start"));
        assert!(p[0].params.contains_key("window_end"));
        assert!(of(&serde_json::json!({ "nodes": [] })).is_none());
    }

    /// #326: the successor must be the one `generate` would have produced.
    ///
    /// This is the guard that matters, because the failure it catches is
    /// silent: a `next_key` that disagreed with the generator by one day would
    /// make every chain look like it had a gap, and the gap would be at a
    /// different place for every cadence.
    #[test]
    fn the_successor_is_the_generators_next_key() {
        for cadence in [Cadence::Hour, Cadence::Day, Cadence::Week, Cadence::Month, Cadence::Year] {
            // A range wide enough to cross a leap day, a year boundary and
            // several month lengths.
            let keys: Vec<String> = generate(&time(cadence, "UTC"), "2023-12-28", "2024-03-04")
                .expect("slices")
                .into_iter()
                .map(|p| p.key)
                .collect();
            assert!(keys.len() > 1, "{cadence:?} produced {} slices", keys.len());
            for pair in keys.windows(2) {
                assert_eq!(
                    next_key(&pair[0], cadence).as_deref(),
                    Some(pair[1].as_str()),
                    "{cadence:?}: the successor of {} must be {}",
                    pair[0],
                    pair[1]
                );
            }
        }
    }

    #[test]
    fn the_successor_crosses_the_awkward_boundaries() {
        let day = |k: &str| next_key(k, Cadence::Day).unwrap();
        assert_eq!(day("2024-02-28"), "2024-02-29", "2024 is a leap year");
        assert_eq!(day("2023-02-28"), "2023-03-01", "2023 is not");
        assert_eq!(day("2026-12-31"), "2027-01-01");
        assert_eq!(next_key("2026-01", Cadence::Month).unwrap(), "2026-02");
        assert_eq!(next_key("2026-12", Cadence::Month).unwrap(), "2027-01");
        assert_eq!(next_key("2026", Cadence::Year).unwrap(), "2027");
        assert_eq!(next_key("2026-09-01T23", Cadence::Hour).unwrap(), "2026-09-02T00");
        // A week key names its Monday, so the successor is seven days on.
        assert_eq!(next_key("2026-08-31", Cadence::Week).unwrap(), "2026-09-07");
    }

    #[test]
    fn a_key_that_is_not_one_has_no_successor() {
        assert_eq!(next_key("not-a-date", Cadence::Day), None);
        assert_eq!(next_key("2026-02-30", Cadence::Day), None, "February has no 30th");
        assert_eq!(next_key("2026-09-01T24", Cadence::Hour), None, "there is no hour 24");
        assert_eq!(next_key("2026-09-01", Cadence::Hour), None, "an hour key carries an hour");
    }

    #[test]
    fn a_parsed_date_canonicalises_to_the_generators_spelling() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        assert_eq!(key_of(d, 0, Cadence::Day).unwrap(), "2026-09-03");
        assert_eq!(key_of(d, 0, Cadence::Month).unwrap(), "2026-09");
        assert_eq!(key_of(d, 0, Cadence::Year).unwrap(), "2026");
        assert_eq!(key_of(d, 7, Cadence::Hour).unwrap(), "2026-09-03T07");
        // A Thursday's week key is the Monday of that week.
        assert_eq!(key_of(d, 0, Cadence::Week).unwrap(), "2026-08-31");
        assert_eq!(key_of(d, 24, Cadence::Hour), None);
    }
}
