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
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetFreshness {
    pub asset: String,
    pub state: State,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// RFC3339 of the newest complete successful write.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_written_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<i64>,
    /// The declared limit, as authored (e.g. "36h").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum_age: Option<String>,
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
        let state = match (maximum_age.as_deref().and_then(parse_duration), age) {
            (Some(limit), Some(a)) if a > limit => State::Stale,
            (Some(_), Some(_)) => State::Fresh,
            // Declared but never written is stale, not unknown: the SLA says it
            // should exist by now and it does not.
            (Some(_), None) => State::Stale,
            _ => State::Unknown,
        };
        out.push(AssetFreshness {
            asset: asset.clone(),
            state,
            owner: rule.map(|r| r.owner.clone()),
            last_written_at: f.map(|f| f.last_written_at.clone()),
            age_seconds: age,
            maximum_age,
            producer: f.map(|f| f.pipeline_id.clone()),
        });
    }
    out
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
}
