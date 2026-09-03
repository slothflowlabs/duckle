//! #325: run a pipeline when the data it reads is published.
//!
//! A consumer that says "after the producer" through a clock is guessing. It
//! either runs too early and reads yesterday's data, or too late and wastes the
//! gap - and when the producer is delayed it does both. The publication is
//! already recorded ([`crate::materialize`]); this is the part that subscribes
//! to it.
//!
//! ## Its own store, not a schedule kind
//!
//! A subscription is not a clock, and the two schedulers cannot express one:
//! the serve-side scheduler reads a flattened JSON projection of the schedule
//! store rather than the typed one, so a non-clock variant would be invisible
//! to it. Adding a variant there would mean a trigger that exists in the type
//! and never fires, which is worse than one that lives somewhere honest.
//!
//! So subscriptions live in `subscriptions.json` and are consumed by a pump
//! that is deliberately separate - the shape Louis asked for on the issue. The
//! run it creates still goes through the ordinary durable run path, so nothing
//! about pools, policy, receipts or logs is bypassed.
//!
//! ## Three states, not one silence
//!
//! ```text
//! producer materialized     -> the event exists
//! delivery failed           -> a delivery record with an error
//! consumer run failed       -> a normal failed run, reachable by its run id
//! ```
//!
//! Without the middle one, "the downstream did not run" and "the downstream ran
//! and failed" look identical from outside, and only one of them is a bug in
//! this machinery.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A standing request to run a pipeline when something it reads is published.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub id: String,
    /// The pipeline to run when this fires.
    pub pipeline_id: String,
    /// Globs over asset ids. ANY match delivers - a consumer reading three
    /// tables wants to run when any of them is republished, not when all three
    /// happen to be published by one run.
    pub assets: Vec<String>,
    /// Only publications by this pipeline count. Absent means any producer,
    /// which is usually what is wanted: the subscription is to the DATA, and
    /// which pipeline happens to produce it is the catalog's business.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    #[serde(default = "yes")]
    pub enabled: bool,
}

fn yes() -> bool {
    true
}

/// Where one delivery stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryState {
    /// Matched, not yet run.
    Pending,
    /// The consumer run was started and finished cleanly.
    Delivered,
    /// The consumer could not be started, or its run failed. `last_error` says
    /// which, because they call for different responses.
    Failed,
}

/// One subscription's response to one publication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    /// `hash(subscription + event)`, so the same publication delivered to the
    /// same subscriber is the same delivery however many times it is
    /// considered. Two ids rather than one widened key, per the issue: "has
    /// this work been done" and "has this subscriber been told" are different
    /// questions, and a subscriber added later must get its own deliveries for
    /// events that already happened without touching the events.
    pub delivery_id: String,
    pub subscription_id: String,
    pub event_id: String,
    /// The consumer.
    pub pipeline_id: String,
    pub state: DeliveryState,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// The consumer's run, so a failure is reachable from the delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub at: String,
}

pub fn delivery_id(subscription_id: &str, event_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for part in [subscription_id, event_id] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    format!("dlv-{}", &hex[..16])
}

pub fn store_path(workspace: &Path) -> PathBuf {
    workspace.join("subscriptions.json")
}

pub fn deliveries_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("deliveries.json")
}

/// The subscriptions as authored. A missing file is none, not an error.
pub fn load(workspace: &Path) -> Result<Vec<Subscription>, String> {
    let p = store_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", p.display()))
}

pub fn deliveries(workspace: &Path) -> BTreeMap<String, Delivery> {
    std::fs::read_to_string(deliveries_path(workspace))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

/// Write the delivery ledger, atomically.
///
/// Temp then rename, never unlink first: a reader must see the previous
/// complete ledger or the new one, and a ledger that is briefly absent is one
/// the pump would treat as "nothing has ever been delivered".
pub fn save_deliveries(workspace: &Path, all: &BTreeMap<String, Delivery>) -> Result<(), String> {
    let path = deliveries_path(workspace);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let body = serde_json::to_string_pretty(all).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
}

/// Whether this subscription wants this publication.
///
/// A subscription to an asset the producer did not write is not a match, and
/// neither is a self-subscription: a pipeline that consumed its own publication
/// would publish again and run forever. Refused here rather than detected
/// later, because the later detection is a storm.
pub fn wants(sub: &Subscription, event: &crate::materialize::Event) -> bool {
    if !sub.enabled {
        return false;
    }
    if sub.pipeline_id == event.pipeline_id {
        return false;
    }
    if let Some(p) = &sub.producer {
        if p != &event.pipeline_id {
            return false;
        }
    }
    sub.assets.iter().any(|pattern| {
        // Through the catalog's own normaliser rather than a second rule: it is
        // what decides that a backslash path with an upper-case drive letter
        // and a forward-slash one with a lower-case drive letter are the same
        // file. A subscription is written by a person reading a path off their
        // screen, and without this the glob silently matched nothing - the
        // consumer never ran, with no error, because "no subscription matched"
        // and "nothing was published" look identical from outside.
        let matcher = glob::Pattern::new(&crate::catalog::normalise_path(pattern));
        event.assets.iter().any(|a| match &matcher {
            Ok(m) => m.matches(&crate::catalog::normalise_path(a)),
            // A pattern that will not compile matches nothing, the same way the
            // catalog's ownership lookup treats one. Matching everything would
            // turn a typo into a workspace-wide trigger.
            Err(_) => false,
        })
    })
}

/// Deliveries that should exist and do not: one per (subscription, event) pair
/// nobody has recorded yet.
///
/// Derived rather than queued, so a subscriber added today receives the
/// publications that already happened rather than only future ones - which is
/// what the two-id design buys, and what a queue written at publish time could
/// not give.
pub fn pending(workspace: &Path, now: &str) -> Vec<Delivery> {
    let subs = load(workspace).unwrap_or_default();
    if subs.is_empty() {
        return Vec::new();
    }
    let known = deliveries(workspace);
    let mut out = Vec::new();
    for event in crate::materialize::read(workspace) {
        for sub in &subs {
            if !wants(sub, &event) {
                continue;
            }
            let id = delivery_id(&sub.id, &event.event_id);
            if known.contains_key(&id) {
                continue;
            }
            out.push(Delivery {
                delivery_id: id,
                subscription_id: sub.id.clone(),
                event_id: event.event_id.clone(),
                pipeline_id: sub.pipeline_id.clone(),
                state: DeliveryState::Pending,
                attempts: 0,
                last_error: None,
                run_id: None,
                at: now.to_string(),
            });
        }
    }
    out
}

/// Record what happened to a delivery.
pub fn record(workspace: &Path, delivery: Delivery) -> Result<(), String> {
    let mut all = deliveries(workspace);
    all.insert(delivery.delivery_id.clone(), delivery);
    save_deliveries(workspace, &all)
}

/// Deliveries that failed and can be tried again.
pub fn failed(workspace: &Path) -> Vec<Delivery> {
    deliveries(workspace)
        .into_values()
        .filter(|d| d.state == DeliveryState::Failed)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materialize::Event;

    fn event(pipeline: &str, assets: &[&str]) -> Event {
        Event {
            event_id: format!("mat-{pipeline}-{}", assets.join("_")),
            pipeline_id: pipeline.into(),
            run_id: Some("run-1".into()),
            release_id: None,
            partition_key: None,
            trigger: "scheduled".into(),
            committed_at: "2026-09-03T10:00:00Z".into(),
            assets: assets.iter().map(|a| (*a).to_string()).collect(),
        }
    }

    fn sub(id: &str, pipeline: &str, assets: &[&str]) -> Subscription {
        Subscription {
            id: id.into(),
            pipeline_id: pipeline.into(),
            assets: assets.iter().map(|a| (*a).to_string()).collect(),
            producer: None,
            enabled: true,
        }
    }

    #[test]
    fn a_subscription_matches_any_of_its_assets() {
        let s = sub("s1", "downstream", &["/lake/orders", "/lake/customers"]);
        assert!(wants(&s, &event("nightly", &["/lake/orders"])));
        assert!(wants(&s, &event("nightly", &["/lake/customers", "/lake/other"])));
        assert!(!wants(&s, &event("nightly", &["/lake/other"])));
    }

    #[test]
    fn a_glob_covers_a_prefix_and_a_bad_one_covers_nothing() {
        assert!(wants(&sub("s", "d", &["/lake/raw/*"]), &event("p", &["/lake/raw/orders"])));
        assert!(!wants(&sub("s", "d", &["/lake/raw/*"]), &event("p", &["/lake/gold/orders"])));
        assert!(
            !wants(&sub("s", "d", &["/lake/[bad"]), &event("p", &["/lake/[bad"])),
            "an uncompilable pattern must match nothing, not everything"
        );
    }

    /// A pipeline consuming its own publication would publish again and run
    /// forever. Refused rather than detected later, because the later detection
    /// is a storm.
    #[test]
    fn a_pipeline_cannot_subscribe_to_itself() {
        let s = sub("s1", "nightly", &["/lake/orders"]);
        assert!(!wants(&s, &event("nightly", &["/lake/orders"])));
    }

    #[test]
    fn a_disabled_subscription_and_a_wrong_producer_do_not_match() {
        let mut off = sub("s1", "d", &["/lake/orders"]);
        off.enabled = false;
        assert!(!wants(&off, &event("nightly", &["/lake/orders"])));

        let mut pinned = sub("s2", "d", &["/lake/orders"]);
        pinned.producer = Some("nightly".into());
        assert!(wants(&pinned, &event("nightly", &["/lake/orders"])));
        assert!(
            !wants(&pinned, &event("someone-else", &["/lake/orders"])),
            "a producer-pinned subscription fired for another producer"
        );
    }

    /// The same publication delivered to the same subscriber is one delivery,
    /// however many times it is considered - which is what stops a pump that
    /// runs every minute from running the consumer every minute.
    #[test]
    fn a_delivery_is_created_once_and_only_once() {
        let ws = tempfile::tempdir().unwrap();
        std::fs::write(
            store_path(ws.path()),
            serde_json::to_string(&vec![sub("s1", "downstream", &["/lake/orders"])]).unwrap(),
        )
        .unwrap();
        crate::materialize::append(
            ws.path(),
            "nightly",
            &publication("run-1", "/lake/orders"),
        )
        .unwrap();

        let first = pending(ws.path(), "now");
        assert_eq!(first.len(), 1, "the publication was not matched");
        record(ws.path(), first[0].clone()).unwrap();

        assert!(
            pending(ws.path(), "now").is_empty(),
            "the same publication was offered to the same subscriber twice"
        );
    }

    /// And the two-id design paying off: a subscriber added after the fact
    /// receives what already happened, because deliveries are derived rather
    /// than queued at publish time.
    #[test]
    fn a_subscriber_added_later_receives_earlier_publications() {
        let ws = tempfile::tempdir().unwrap();
        crate::materialize::append(ws.path(), "nightly", &publication("run-1", "/lake/orders"))
            .unwrap();
        assert!(pending(ws.path(), "now").is_empty(), "no subscribers yet");

        std::fs::write(
            store_path(ws.path()),
            serde_json::to_string(&vec![sub("late", "downstream", &["/lake/orders"])]).unwrap(),
        )
        .unwrap();
        assert_eq!(
            pending(ws.path(), "now").len(),
            1,
            "a subscription added after the publication got nothing"
        );
    }

    fn publication(run: &str, asset: &str) -> crate::history::RunRecord {
        crate::history::RunRecord {
            run_id: Some(run.into()),
            at: "2026-09-03T10:00:00Z".into(),
            status: "ok".into(),
            duration_ms: 1,
            rows: 1,
            node_count: 1,
            trigger: "scheduled".into(),
            error: None,
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            category: None,
            assets: vec![crate::history::AssetTouch {
                id: asset.into(),
                direction: "write".into(),
                rows: Some(1),
            }],
        }
    }
}

#[cfg(test)]
mod path_shapes {
    use super::*;
    use crate::materialize::Event;

    /// The trap this hit in a live run: the catalog stores an asset with a
    /// lower-case drive letter and forward slashes, and a person writes the
    /// path the way their file manager shows it. The glob matched nothing, so
    /// the consumer never ran - and there was no error to see, because "no
    /// subscription matched" and "nothing was published" look identical.
    #[test]
    fn a_subscription_matches_however_the_path_was_typed() {
        let event = Event {
            event_id: "mat-1".into(),
            pipeline_id: "producer".into(),
            run_id: None,
            release_id: None,
            partition_key: None,
            trigger: "manual".into(),
            committed_at: "2026-09-03T10:00:00Z".into(),
            assets: vec!["c:/data/lake/orders.parquet".into()],
        };
        for written_as in [
            "c:/data/lake/orders.parquet",
            "C:/data/lake/orders.parquet",
            r"C:\data\lake\orders.parquet",
            "C:/data/lake/*.parquet",
        ] {
            let s = Subscription {
                id: "s".into(),
                pipeline_id: "consumer".into(),
                assets: vec![written_as.to_string()],
                producer: None,
                enabled: true,
            };
            assert!(wants(&s, &event), "a subscription written as {written_as:?} matched nothing");
        }
    }
}
