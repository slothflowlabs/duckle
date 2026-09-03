//! #304: expected freshness, evaluated without waiting for a run to fail.
//!
//! A dataset goes stale in ways that produce no failed run at all: a schedule
//! switched off, a server down, a source that stopped publishing, a run that
//! was never queued, an overlap policy that skipped every occurrence. Alerting
//! on failures cannot see any of those, because nothing failed.
//!
//! So an asset can declare how old it is allowed to get, and that is checked on
//! a clock rather than at the end of a run.
//!
//! ## Where the SLA lives
//!
//! On the same `owners.json` rule that already carries ownership, description
//! and tags. That file's own reasoning applies exactly: these are authored
//! together by the same person, and a second file would drift from this one.
//!
//! ```json
//! { "match": "/lake/raw/*", "owner": "data-eng", "maximumAge": "36h" }
//! ```
//!
//! ## What counts as fresh
//!
//! Only a run that **completed** and wrote the asset. A failed run never
//! counted; an `incomplete` one did, and should not have. An incomplete run is
//! one that stopped at a ceiling - a budget, a page that failed mid-walk - so
//! its rows are correct and are not all of them. Treating a partial publish as
//! a refresh is precisely the "failed or partial publish must not refresh the
//! asset" case, and it is the quieter half: a failure is visible, a truncated
//! success looks like a healthy one.

use std::collections::BTreeMap;
use std::path::Path;

/// Where an asset stands against its declared freshness.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum State {
    /// No SLA declared, or never successfully written, so there is nothing to
    /// judge it against. Deliberately distinct from `fresh`: "we do not know"
    /// and "it is fine" are different answers and only one of them is reassuring.
    Unknown,
    Fresh,
    Stale,
    /// Fresh, and stale at the previous evaluation. A transition rather than a
    /// resting place: the next evaluation reports it as `fresh`. It exists
    /// because "it is fine" and "it is fine AGAIN" are different things to tell
    /// someone who was paged about it (#304).
    Recovered,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetFreshness {
    pub asset: String,
    pub state: State,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// The ownership rule's tags, carried so an alert rule can route on them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// RFC3339 of the newest complete successful write.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_written_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<i64>,
    /// The declared limit, as authored (e.g. "36h").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum_age: Option<String>,
    /// When this asset first went stale, carried across evaluations so the
    /// answer to "how long has this been broken" does not reset every tick.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
}

/// Parse a duration written the way an operator writes one: `90m`, `36h`, `2d`.
///
/// Plain seconds are accepted too, but the suffixed forms are what appears in a
/// file a human maintains, and rejecting `36h` because it is not a number would
/// make the feature annoying enough to go unused.
pub fn parse_duration(s: &str) -> Option<i64> {
    let t = s.trim().to_ascii_lowercase();
    if t.is_empty() {
        return None;
    }
    let (num, mult) = match t.chars().last() {
        Some('s') => (&t[..t.len() - 1], 1),
        Some('m') => (&t[..t.len() - 1], 60),
        Some('h') => (&t[..t.len() - 1], 3600),
        Some('d') => (&t[..t.len() - 1], 86_400),
        Some(c) if c.is_ascii_digit() => (t.as_str(), 1),
        _ => return None,
    };
    let n: i64 = num.trim().parse().ok()?;
    if n < 0 {
        return None;
    }
    Some(n * mult)
}

/// Where one asset stands, given its declared limit and how old it is.
///
/// The single definition of "stale". The clock check and the catalog view both
/// need the answer and they must not each work it out: a screen that called an
/// asset fresh while the alerting called it stale would be the worst of both,
/// and the disagreement would be invisible until someone compared them.
///
/// `age_seconds` is None when the asset has never been successfully written.
pub fn verdict(maximum_age: Option<&str>, age_seconds: Option<i64>) -> State {
    match (maximum_age.and_then(parse_duration), age_seconds) {
        (Some(limit), Some(a)) if a > limit => State::Stale,
        (Some(_), Some(_)) => State::Fresh,
        // Declared but never written is stale, not unknown: the SLA says it
        // should exist by now and it does not.
        (Some(_), None) => State::Stale,
        _ => State::Unknown,
    }
}

/// Whether a schedule-relative deadline has passed (#304).
///
/// `expectedAfterSchedule: 4h` means "written within 4h of when it was due",
/// which is a moving deadline rather than a guess at the longest gap between
/// runs - the thing an absolute `maximumAge` forces you to pick.
///
/// Stated forward rather than backward, because it needs no "previous
/// occurrence" primitive and reads the same way the failure does:
///
/// ```text
/// last successful write W
///   -> F = the first time the schedule fires after W
///   -> if now is past F + grace, a run was due and did not deliver
/// ```
///
/// Returns None when the question cannot be asked at all - no producer, no
/// schedule for it, a schedule that is not a cron, or an unparseable
/// expression - so the caller falls back rather than inventing a verdict.
/// A schedule that exists and is DISABLED returns Some(true): that is one of
/// the failure modes this exists for, and a deadline taken from a schedule
/// nobody is running would excuse exactly the outage it should catch.
fn schedule_deadline_passed(
    workspace: &Path,
    producer: Option<&str>,
    grace: &str,
    last_written_at: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<bool> {
    let grace = parse_duration(grace)?;
    let producer = producer?;
    let schedules = crate::schedules::load(workspace).ok()?;
    let sched = schedules.iter().find(|s| s.pipeline_id == producer)?;
    if !sched.enabled {
        return Some(true);
    }
    let expr = match &sched.kind {
        crate::schedules::ScheduleKind::Cron { expr } => expr.clone(),
        // An interval or a file watch has no civil-time occurrence to be late
        // against. Answering None sends the caller to `maximumAge`, which is
        // the honest fallback rather than a deadline invented here.
        _ => return None,
    };
    let zone = crate::cronzone::resolve_zone(sched.timezone.as_deref()).ok()?;
    // Anchored at the last successful write. With none, anchor at the grace
    // window before now, so a never-written asset is late as soon as one
    // occurrence has been missed rather than immediately.
    let anchor = last_written_at
        .and_then(|w| chrono::DateTime::parse_from_rfc3339(w).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
        .unwrap_or(now - chrono::Duration::seconds(grace));
    let (occurrence, _) = crate::cronzone::next_after(&expr, &zone, anchor).ok()?;
    let due = occurrence?.at;
    Some(now > due + chrono::Duration::seconds(grace))
}

/// Judge every asset that declares a maximum age.
///
/// `now` is a parameter so the evaluation is testable without waiting.
pub fn evaluate(workspace: &Path, now: chrono::DateTime<chrono::Utc>) -> Vec<AssetFreshness> {
    let owners = crate::catalog::load_owners(workspace).unwrap_or_default();
    let fresh = crate::catalog::freshness(workspace);
    let mut out: Vec<AssetFreshness> = Vec::new();
    // Every asset that has ever been written, plus every asset a rule names
    // explicitly - an asset that has NEVER been written is exactly the case an
    // SLA is meant to catch, so it cannot be discovered from run history alone.
    let mut assets: BTreeMap<String, ()> = fresh.keys().map(|k| (k.clone(), ())).collect();
    for rule in &owners.assets {
        if !rule.pattern.contains('*') {
            assets.insert(rule.pattern.clone(), ());
        }
    }
    for asset in assets.keys() {
        let rule = owners.assets.iter().find(|r| {
            // A pattern that will not compile matches nothing, exactly as the
            // catalog's own ownership lookup treats it.
            glob::Pattern::new(&r.pattern).map(|p| p.matches(asset)).unwrap_or(false)
        });
        let maximum_age = rule.and_then(|r| r.maximum_age.clone());
        let f = fresh.get(asset);
        let age = f.and_then(|f| {
            chrono::DateTime::parse_from_rfc3339(&f.last_written_at)
                .ok()
                .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds())
        });
        // A schedule-relative deadline wins when one is declared and can be
        // answered: it is the more specific statement, and it is the one that
        // moves with the schedule. Where it cannot be answered - no producer,
        // an interval schedule, an unparseable expression - it falls back to
        // the absolute limit rather than inventing a verdict.
        let by_schedule = rule.and_then(|r| r.expected_after_schedule.as_deref()).and_then(|g| {
            schedule_deadline_passed(
                workspace,
                f.map(|f| f.pipeline_id.as_str()),
                g,
                f.map(|f| f.last_written_at.as_str()),
                now,
            )
        });
        let state = match by_schedule {
            Some(true) => State::Stale,
            Some(false) => State::Fresh,
            None => verdict(maximum_age.as_deref(), age),
        };
        out.push(AssetFreshness {
            asset: asset.clone(),
            state,
            owner: rule.map(|r| r.owner.clone()),
            tags: rule.map(|r| r.tags.clone()).unwrap_or_default(),
            last_written_at: f.map(|f| f.last_written_at.clone()),
            age_seconds: age,
            maximum_age,
            stale_since: None,
            producer: f.map(|f| f.pipeline_id.clone()),
        });
    }
    out
}

/// What the previous evaluation concluded, per asset.
///
/// #304 asks for `recovered` and for "stale since", and neither can be answered
/// by a function that only looks at now. This is the smallest thing that makes
/// them answerable: the prior verdict, and when it started.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Memory {
    #[serde(default)]
    assets: BTreeMap<String, Remembered>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Remembered {
    state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    since: Option<String>,
}

fn memory_path(workspace: &Path) -> std::path::PathBuf {
    workspace.join(".duckle").join("freshness.json")
}

fn load_memory(workspace: &Path) -> Memory {
    std::fs::read_to_string(memory_path(workspace))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn save_memory(workspace: &Path, m: &Memory) {
    let path = memory_path(workspace);
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    // Temp then rename, like every other durable file here: a reader must see
    // the previous complete memory or the new one, never a half-written map.
    let tmp = path.with_extension("json.tmp");
    if serde_json::to_string_pretty(m)
        .ok()
        .and_then(|body| std::fs::write(&tmp, body).ok())
        .is_some()
    {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// Judge every asset, remember the verdict, and say what CHANGED.
///
/// The difference between this and [`evaluate`] is memory. `evaluate` answers
/// "is this stale now", which cannot distinguish an asset that has been broken
/// for a week from one that broke a minute ago, and cannot report an all-clear
/// at all. This carries the previous verdict forward, so `fresh -> stale` and
/// `stale -> fresh` are visible and `stale_since` survives across evaluations.
///
/// Returns the full picture, not only the changes: a caller showing a freshness
/// panel wants every asset, and a caller raising alerts filters.
pub fn check(workspace: &Path, now: chrono::DateTime<chrono::Utc>) -> Vec<AssetFreshness> {
    let mut memory = load_memory(workspace);
    let mut out = evaluate(workspace, now);
    let stamp = now.to_rfc3339();
    for a in out.iter_mut() {
        let prior = memory.assets.get(&a.asset);
        let was_stale = prior.map(|p| p.state == "stale").unwrap_or(false);
        match a.state {
            State::Stale => {
                // The clock starts when it first went stale, not when it was
                // last looked at.
                a.stale_since = prior
                    .and_then(|p| p.since.clone())
                    .filter(|_| was_stale)
                    .or(Some(stamp.clone()));
            }
            State::Fresh if was_stale => {
                a.state = State::Recovered;
                // Kept on the record for this one evaluation, so the all-clear
                // can say how long it was out.
                a.stale_since = prior.and_then(|p| p.since.clone());
            }
            _ => {}
        }
        // `recovered` is remembered as `fresh`: it describes a transition, and
        // remembering it would make the NEXT evaluation report a recovery from
        // a state that was already fine.
        let remembered = match a.state {
            State::Stale => "stale",
            State::Fresh | State::Recovered => "fresh",
            State::Unknown => "unknown",
        };
        memory.assets.insert(
            a.asset.clone(),
            Remembered {
                state: remembered.to_string(),
                since: match a.state {
                    State::Stale => a.stale_since.clone(),
                    _ => None,
                },
            },
        );
    }
    save_memory(workspace, &memory);
    out
}

/// Evaluate, remember, and tell whoever asked to be told.
///
/// Returns how many alerts were sent. Alerting never fails the check: a broken
/// webhook must not stop freshness being evaluated, for the same reason it must
/// not change the outcome of a run.
pub fn check_and_alert(workspace: &Path, now: chrono::DateTime<chrono::Utc>) -> (Vec<AssetFreshness>, usize) {
    let assets = check(workspace, now);
    let mut sent = 0usize;
    for a in &assets {
        let (event, text) = match a.state {
            // Raised on every evaluation while stale, not only on the
            // transition: the rule's own cooldown is what makes that "once,
            // then not again for a while", which is the same contract a
            // failing pipeline's alerts have.
            State::Stale => (
                crate::alerts::Event::Stale,
                format!(
                    "Duckle: {} is STALE{}{}",
                    a.asset,
                    a.maximum_age.as_deref().map(|m| format!(" (limit {m})")).unwrap_or_default(),
                    match (&a.last_written_at, a.age_seconds) {
                        (Some(w), Some(age)) => {
                            format!(", last written {w} ({:.1}h ago)", age as f64 / 3600.0)
                        }
                        _ => ", and has never been written".to_string(),
                    }
                ),
            ),
            State::Recovered => (
                crate::alerts::Event::Refreshed,
                format!(
                    "Duckle: {} was written again{}",
                    a.asset,
                    a.stale_since.as_deref().map(|s| format!(", stale since {s}")).unwrap_or_default()
                ),
            ),
            _ => continue,
        };
        // Routed by who owns it and how it is tagged, not only by its path:
        // one team's datasets live under several prefixes and one prefix holds
        // several teams', which a glob cannot express and the ownership rule
        // already knows.
        let routing = crate::alerts::Routing { owner: a.owner.clone(), tags: a.tags.clone() };
        sent += crate::alerts::notify_subject(workspace, &a.asset, event, &text, &routing);
    }
    (assets, sent)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_with(records: &str, owners: &str) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("runs")).unwrap();
        std::fs::write(tmp.path().join("runs").join("daily.json"), records).unwrap();
        std::fs::write(tmp.path().join("owners.json"), owners).unwrap();
        tmp
    }

    fn at(hours_ago: i64) -> String {
        (chrono::Utc::now() - chrono::Duration::hours(hours_ago)).to_rfc3339()
    }

    /// The bug this found: a failed run never refreshed an asset, but an
    /// INCOMPLETE one did. An incomplete run stopped at a ceiling, so its rows
    /// are correct and are not all of them - and unlike a failure, it looks
    /// healthy.
    #[test]
    fn a_partial_publish_does_not_make_an_asset_fresh() {
        let records = format!(
            r#"[{{"at":"{}","status":"ok","duration_ms":1,"rows":10,"node_count":1,
                  "trigger":"manual","incomplete":true,
                  "assets":[{{"id":"/lake/orders","direction":"write","rows":10}}]}}]"#,
            at(1)
        );
        let owners = r#"{"assets":[{"match":"/lake/orders","owner":"data-eng","maximumAge":"36h"}]}"#;
        let ws = workspace_with(&records, owners);

        let r = evaluate(ws.path(), chrono::Utc::now());
        let orders = r.iter().find(|a| a.asset == "/lake/orders").expect("the asset");
        assert_eq!(
            orders.state,
            State::Stale,
            "an incomplete run published part of a dataset; that is not a refresh"
        );
        assert!(orders.last_written_at.is_none(), "and it is not a last-written time");
    }

    /// A complete run inside the window is fresh, and carries who to tell.
    #[test]
    fn a_complete_recent_run_is_fresh_and_names_its_owner() {
        let records = format!(
            r#"[{{"at":"{}","status":"ok","duration_ms":1,"rows":10,"node_count":1,
                  "trigger":"manual",
                  "assets":[{{"id":"/lake/orders","direction":"write","rows":10}}]}}]"#,
            at(2)
        );
        let owners = r#"{"assets":[{"match":"/lake/*","owner":"data-eng","maximumAge":"36h"}]}"#;
        let ws = workspace_with(&records, owners);
        let r = evaluate(ws.path(), chrono::Utc::now());
        let o = r.iter().find(|a| a.asset == "/lake/orders").unwrap();
        assert_eq!(o.state, State::Fresh);
        assert_eq!(o.owner.as_deref(), Some("data-eng"), "a stale asset needs someone to tell");
        assert_eq!(o.producer.as_deref(), Some("daily"));
    }

    /// The case the issue is actually about: nothing failed, nothing ran, and
    /// the asset quietly aged past its limit.
    #[test]
    fn an_asset_past_its_limit_is_stale_without_any_failed_run() {
        let records = format!(
            r#"[{{"at":"{}","status":"ok","duration_ms":1,"rows":10,"node_count":1,
                  "trigger":"scheduled",
                  "assets":[{{"id":"/lake/orders","direction":"write","rows":10}}]}}]"#,
            at(50)
        );
        let owners = r#"{"assets":[{"match":"/lake/orders","owner":"data-eng","maximumAge":"36h"}]}"#;
        let ws = workspace_with(&records, owners);
        let r = evaluate(ws.path(), chrono::Utc::now());
        let o = r.iter().find(|a| a.asset == "/lake/orders").unwrap();
        assert_eq!(o.state, State::Stale);
        assert!(o.age_seconds.unwrap() > 36 * 3600);
    }

    /// No SLA is UNKNOWN, not fresh. "We do not know" and "it is fine" are
    /// different answers and only one of them is reassuring.
    #[test]
    fn an_asset_with_no_declared_limit_is_unknown_rather_than_fresh() {
        let records = format!(
            r#"[{{"at":"{}","status":"ok","duration_ms":1,"rows":1,"node_count":1,
                  "trigger":"manual",
                  "assets":[{{"id":"/lake/other","direction":"write","rows":1}}]}}]"#,
            at(500)
        );
        let ws = workspace_with(&records, r#"{"assets":[]}"#);
        let r = evaluate(ws.path(), chrono::Utc::now());
        assert_eq!(r.iter().find(|a| a.asset == "/lake/other").unwrap().state, State::Unknown);
    }

    /// An asset that a rule names but that has never been written is stale, not
    /// missing: the SLA says it should be there by now.
    #[test]
    fn a_declared_asset_that_never_arrived_is_stale() {
        let ws = workspace_with(
            "[]",
            r#"{"assets":[{"match":"/lake/never","owner":"eng","maximumAge":"1h"}]}"#,
        );
        let r = evaluate(ws.path(), chrono::Utc::now());
        let o = r.iter().find(|a| a.asset == "/lake/never").expect("named by a rule");
        assert_eq!(o.state, State::Stale);
        assert!(o.last_written_at.is_none());
    }

    #[test]
    fn durations_are_written_the_way_an_operator_writes_them() {
        assert_eq!(parse_duration("90m"), Some(5400));
        assert_eq!(parse_duration("36h"), Some(129_600));
        assert_eq!(parse_duration("2d"), Some(172_800));
        assert_eq!(parse_duration("45"), Some(45), "bare seconds still work");
        assert_eq!(parse_duration("  12h "), Some(43_200));
        assert_eq!(parse_duration("later"), None);
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("-1h"), None, "a negative age is not a limit");
    }

    fn write_records(ws: &std::path::Path, hours_ago: i64) {
        let records = format!(
            r#"[{{"at":"{}","status":"ok","duration_ms":1,"rows":10,"node_count":1,
                  "trigger":"manual",
                  "assets":[{{"id":"/lake/orders","direction":"write","rows":10}}]}}]"#,
            (chrono::Utc::now() - chrono::Duration::hours(hours_ago)).to_rfc3339()
        );
        std::fs::write(ws.join("runs").join("daily.json"), records).unwrap();
    }

    /// #304's four states, walked in order, because three of them only exist
    /// relative to the evaluation before.
    #[test]
    fn an_asset_goes_stale_then_recovers_then_settles() {
        let owners =
            r#"{"assets":[{"match":"/lake/orders","owner":"data-eng","maximumAge":"12h"}]}"#;
        let ws = workspace_with("[]", owners);
        let of = |v: &Vec<AssetFreshness>| {
            v.iter().find(|a| a.asset == "/lake/orders").expect("the asset").clone()
        };

        // Written 40 hours ago against a 12h limit.
        write_records(ws.path(), 40);
        let first = of(&check(ws.path(), chrono::Utc::now()));
        assert_eq!(first.state, State::Stale);
        let since = first.stale_since.clone().expect("stale since is recorded");

        // Still stale at the next evaluation, and the clock did NOT restart -
        // "how long has this been broken" must not reset every tick.
        let again = of(&check(ws.path(), chrono::Utc::now()));
        assert_eq!(again.state, State::Stale);
        assert_eq!(again.stale_since.as_deref(), Some(since.as_str()), "the clock restarted");

        // Written again: recovered, not merely fresh, and it still says how
        // long it was out.
        write_records(ws.path(), 0);
        let back = of(&check(ws.path(), chrono::Utc::now()));
        assert_eq!(back.state, State::Recovered, "an all-clear is not the same as 'it is fine'");
        assert_eq!(back.stale_since.as_deref(), Some(since.as_str()));

        // And it settles: recovered describes a transition, so the evaluation
        // after it is plain fresh rather than a second all-clear.
        let settled = of(&check(ws.path(), chrono::Utc::now()));
        assert_eq!(settled.state, State::Fresh, "recovery was reported twice");
        assert_eq!(settled.stale_since, None);
    }

    /// An asset nobody declared a limit for stays Unknown across evaluations
    /// and never produces a transition, so memory cannot invent an alert.
    #[test]
    fn an_undeclared_asset_never_transitions() {
        let ws = workspace_with("[]", r#"{"assets":[]}"#);
        write_records(ws.path(), 100);
        for _ in 0..3 {
            let r = check(ws.path(), chrono::Utc::now());
            let a = r.iter().find(|a| a.asset == "/lake/orders").expect("the asset");
            assert_eq!(a.state, State::Unknown, "no limit means no verdict");
            assert_eq!(a.stale_since, None);
        }
    }

}

#[cfg(test)]
mod schedule_relative {
    use super::*;

    fn ws_with(owners: &str, schedules: &str, written_hours_ago: Option<i64>) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("runs")).unwrap();
        let records = match written_hours_ago {
            Some(h) => format!(
                r#"[{{"at":"{}","status":"ok","duration_ms":1,"rows":1,"node_count":1,
                      "trigger":"schedule",
                      "assets":[{{"id":"/lake/orders","direction":"write","rows":1}}]}}]"#,
                (chrono::Utc::now() - chrono::Duration::hours(h)).to_rfc3339()
            ),
            None => "[]".to_string(),
        };
        std::fs::write(tmp.path().join("runs").join("daily.json"), records).unwrap();
        std::fs::write(tmp.path().join("owners.json"), owners).unwrap();
        std::fs::write(tmp.path().join("schedules.json"), schedules).unwrap();
        tmp
    }

    const HOURLY: &str = r#"[{"id":"s1","pipeline_id":"daily","name":"hourly","enabled":true,
                              "kind":{"type":"cron","expr":"0 * * * *"}}]"#;

    fn state_of(ws: &std::path::Path) -> State {
        evaluate(ws, chrono::Utc::now())
            .into_iter()
            .find(|a| a.asset == "/lake/orders")
            .expect("the asset")
            .state
    }

    /// #304: "written within 4h of when it was due" rather than a guess at the
    /// longest acceptable gap. On an hourly schedule, a write 10 hours old has
    /// missed several occurrences.
    #[test]
    fn an_asset_that_missed_its_schedule_is_stale() {
        let owners = r#"{"assets":[{"match":"/lake/orders","owner":"o","expectedAfterSchedule":"4h"}]}"#;
        let ws = ws_with(owners, HOURLY, Some(10));
        assert_eq!(state_of(ws.path()), State::Stale);
    }

    /// And one written a moment ago is fresh, on the same rule - so the test
    /// above is not passing because everything is stale.
    #[test]
    fn an_asset_written_on_time_is_fresh() {
        let owners = r#"{"assets":[{"match":"/lake/orders","owner":"o","expectedAfterSchedule":"4h"}]}"#;
        let ws = ws_with(owners, HOURLY, Some(0));
        assert_eq!(state_of(ws.path()), State::Fresh);
    }

    /// The failure mode the issue names first. A deadline taken from a schedule
    /// nobody is running would excuse exactly the outage it should catch, so a
    /// disabled schedule is stale however recently the asset was written.
    #[test]
    fn a_disabled_schedule_makes_its_asset_stale() {
        let owners = r#"{"assets":[{"match":"/lake/orders","owner":"o","expectedAfterSchedule":"4h"}]}"#;
        let off = r#"[{"id":"s1","pipeline_id":"daily","name":"hourly","enabled":false,
                       "kind":{"type":"cron","expr":"0 * * * *"}}]"#;
        let ws = ws_with(owners, off, Some(0));
        assert_eq!(
            state_of(ws.path()),
            State::Stale,
            "a switched-off schedule excused its own asset"
        );
    }

    /// An interval schedule has no civil-time occurrence to be late against, so
    /// the question falls back to the absolute limit rather than being answered
    /// with a deadline invented here.
    #[test]
    fn an_interval_schedule_falls_back_to_the_absolute_limit() {
        let owners = r#"{"assets":[{"match":"/lake/orders","owner":"o",
                                    "expectedAfterSchedule":"1h","maximumAge":"100h"}]}"#;
        let interval = r#"[{"id":"s1","pipeline_id":"daily","name":"every","enabled":true,
                            "kind":{"type":"interval","seconds":600}}]"#;
        let ws = ws_with(owners, interval, Some(10));
        assert_eq!(
            state_of(ws.path()),
            State::Fresh,
            "10h old against a 100h absolute limit is fresh; the schedule rule should not apply"
        );
    }
}
