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

/// Which pipelines a publication by `producer` would set running.
///
/// Decided by [`wants`], not by a second matching rule. Static validation that
/// disagreed with runtime delivery would be worse than none: it would refuse
/// configurations that work and permit ones that loop. Reusing the predicate
/// makes the two agree by construction, and means the producer filter and the
/// self-subscription refusal are inherited rather than restated.
fn triggered_by(producer: &str, writes: &[String], subs: &[Subscription]) -> Vec<String> {
    // A stand-in publication: what a run of `producer` that wrote everything it
    // can write would look like. Only `pipeline_id` and `assets` are read by
    // `wants`; the rest is filled to make a well-formed event.
    let event = crate::materialize::Event {
        event_id: String::new(),
        pipeline_id: producer.to_string(),
        run_id: None,
        release_id: None,
        partition_key: None,
        trigger: "static-check".to_string(),
        committed_at: String::new(),
        assets: writes.to_vec(),
    };
    let mut out: Vec<String> =
        subs.iter().filter(|s| wants(s, &event)).map(|s| s.pipeline_id.clone()).collect();
    out.sort();
    out.dedup();
    out
}

/// Every asset each pipeline WRITES, from the catalog.
///
/// Writes only. A read does not start anything, so a read edge cannot be part
/// of a trigger loop, and including one would refuse the ordinary arrangement
/// where two pipelines share an input.
fn writes_by_pipeline(catalog: &crate::catalog::Catalog) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for t in &catalog.touches {
        if t.direction == crate::catalog::Direction::Write {
            out.entry(t.pipeline_id.clone()).or_default().push(t.asset.clone());
        }
    }
    for v in out.values_mut() {
        v.sort();
        v.dedup();
    }
    out
}

/// A trigger loop, as the pipelines around it.
///
/// #325 criterion 7. `wants` already refuses `A -> A`, and that is the easy
/// half: `A -> B -> C -> A` is invisible to a per-subscription rule because no
/// single subscription is wrong. It only exists in the combined graph, and it
/// is only cheap to see BEFORE it starts running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cycle {
    /// The pipelines in trigger order, the first repeated at the end so the
    /// loop reads as a loop: `a -> b -> c -> a`.
    pub path: Vec<String>,
}

impl Cycle {
    pub fn describe(&self) -> String {
        self.path.join(" -> ")
    }
}

/// Every trigger loop in the combined asset and subscription graph.
///
/// The graph is pipelines joined by "publishing this would run that": a
/// pipeline's writes from the catalog, matched against the subscriptions. A
/// cycle in it is a set of runs that would each cause the next forever.
///
/// Depth-first with an explicit stack of the path being explored, so what comes
/// back is the loop itself rather than the fact that one exists. "There is a
/// cycle somewhere in your workspace" is not an error anybody can act on.
pub fn cycles(catalog: &crate::catalog::Catalog, subs: &[Subscription]) -> Vec<Cycle> {
    let writes = writes_by_pipeline(catalog);
    let edges: BTreeMap<String, Vec<String>> = writes
        .iter()
        .map(|(p, assets)| (p.clone(), triggered_by(p, assets, subs)))
        .collect();

    let mut found: Vec<Cycle> = Vec::new();
    let mut done: std::collections::BTreeSet<String> = Default::default();
    for start in edges.keys() {
        if done.contains(start) {
            continue;
        }
        // Iterative rather than recursive: a workspace is operator-authored and
        // a deep chain is legitimate, and a stack overflow inside a validation
        // check would be a worse failure than the thing it validates.
        let mut path: Vec<String> = vec![start.clone()];
        let mut cursor: Vec<usize> = vec![0];
        let mut on_path: std::collections::BTreeSet<String> = [start.clone()].into();
        while let Some(depth) = cursor.len().checked_sub(1) {
            let here = path[depth].clone();
            let next = edges.get(&here).and_then(|v| v.get(cursor[depth]).cloned());
            cursor[depth] += 1;
            let Some(next) = next else {
                on_path.remove(&here);
                done.insert(here);
                path.pop();
                cursor.pop();
                continue;
            };
            if on_path.contains(&next) {
                // Report from where the loop closes, not from where the walk
                // began: `a -> b -> c -> b` names a as a participant it is not.
                let at = path.iter().position(|p| p == &next).unwrap_or(0);
                let mut loop_path = path[at..].to_vec();
                loop_path.push(next);
                found.push(Cycle { path: loop_path });
                continue;
            }
            if done.contains(&next) {
                continue;
            }
            on_path.insert(next.clone());
            path.push(next);
            cursor.push(0);
        }
    }
    found.sort_by(|a, b| a.path.cmp(&b.path));
    found.dedup();
    found
}

/// Refuse a subscription that would close a trigger loop.
///
/// Checked against the graph the subscription WOULD make, so the answer is
/// about the change being made rather than about the workspace in general: a
/// loop that already exists is not this edit's fault and is not reported here,
/// or an operator could never fix one subscription at a time.
pub fn check_addition(
    catalog: &crate::catalog::Catalog,
    existing: &[Subscription],
    candidate: &Subscription,
) -> Result<(), String> {
    let before = cycles(catalog, existing);
    let mut after_subs: Vec<Subscription> =
        existing.iter().filter(|s| s.id != candidate.id).cloned().collect();
    after_subs.push(candidate.clone());
    let after = cycles(catalog, &after_subs);

    let new: Vec<&Cycle> = after.iter().filter(|c| !before.contains(c)).collect();
    if new.is_empty() {
        return Ok(());
    }
    Err(format!(
        "subscription {} would make {} run in a loop: {}. A publication by any of these would \
         start the next one forever. Narrow the assets it matches, or set a producer, so the \
         chain does not come back to where it started.",
        candidate.id,
        candidate.pipeline_id,
        new.iter().map(|c| c.describe()).collect::<Vec<_>>().join("; ")
    ))
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

/// #325 criterion 7: trigger loops in the combined graph.
#[cfg(test)]
mod trigger_cycles {
    use super::*;

    /// #325 criterion 7: an indirect loop that no single subscription reveals.
    fn catalog_of(writes: &[(&str, &str)]) -> crate::catalog::Catalog {
        crate::catalog::Catalog {
            touches: writes
                .iter()
                .map(|(pipeline, asset)| crate::catalog::Touch {
                    pipeline_id: (*pipeline).into(),
                    node_id: "n1".into(),
                    component_id: "snk.parquet".into(),
                    asset: (*asset).into(),
                    direction: crate::catalog::Direction::Write,
                })
                .collect(),
            ..Default::default()
        }
    }

    fn sub(id: &str, pipeline: &str, asset: &str) -> Subscription {
        Subscription {
            id: id.into(),
            pipeline_id: pipeline.into(),
            assets: vec![asset.into()],
            producer: None,
            enabled: true,
        }
    }

    #[test]
    fn an_indirect_trigger_loop_is_found() {
        // a writes A, b subscribes to A and writes B, c subscribes to B and
        // writes C, and a subscribes to C. No subscription is wrong on its own.
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B"), ("c", "/data/C")]);
        let subs = vec![
            sub("s1", "b", "/data/A"),
            sub("s2", "c", "/data/B"),
            sub("s3", "a", "/data/C"),
        ];
        let found = cycles(&catalog, &subs);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].describe(), "a -> b -> c -> a");
    }

    #[test]
    fn a_chain_that_does_not_come_back_is_allowed() {
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B"), ("c", "/data/C")]);
        let subs = vec![sub("s1", "b", "/data/A"), sub("s2", "c", "/data/B")];
        assert!(cycles(&catalog, &subs).is_empty(), "a -> b -> c is a pipeline, not a loop");
    }

    /// Reading is not triggering. Two pipelines sharing an input is ordinary.
    #[test]
    fn a_read_edge_is_not_a_trigger_edge() {
        let mut catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B")]);
        catalog.touches.push(crate::catalog::Touch {
            pipeline_id: "a".into(),
            node_id: "n2".into(),
            component_id: "src.parquet".into(),
            asset: "/data/B".into(),
            direction: crate::catalog::Direction::Read,
        });
        // b runs when A is published and writes B, which a merely READS.
        let subs = vec![sub("s1", "b", "/data/A")];
        assert!(cycles(&catalog, &subs).is_empty(), "a reads B, it is not run by it");
    }

    #[test]
    fn the_loop_is_reported_from_where_it_closes() {
        // a -> b -> c -> b. `a` starts the walk and is not in the loop.
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B"), ("c", "/data/C")]);
        let subs = vec![
            sub("s1", "b", "/data/A"),
            sub("s2", "c", "/data/B"),
            sub("s3", "b", "/data/C"),
        ];
        let found = cycles(&catalog, &subs);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].describe(), "b -> c -> b", "a is not a participant");
    }

    #[test]
    fn a_subscription_that_would_close_a_loop_is_refused() {
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B"), ("c", "/data/C")]);
        let existing = vec![sub("s1", "b", "/data/A"), sub("s2", "c", "/data/B")];
        // Adding it is fine while the chain stays open.
        assert!(check_addition(&catalog, &existing, &sub("s9", "c", "/data/A")).is_ok());
        // Closing it is not.
        let err = check_addition(&catalog, &existing, &sub("s3", "a", "/data/C"))
            .expect_err("that closes the loop");
        assert!(err.contains("a -> b -> c -> a"), "it names the loop: {err}");
        assert!(err.contains("Narrow the assets"), "it says what to do: {err}");
    }

    /// A loop that is already there is not this edit's fault.
    #[test]
    fn an_existing_loop_does_not_block_an_unrelated_subscription() {
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B"), ("d", "/data/D")]);
        let existing = vec![sub("s1", "b", "/data/A"), sub("s2", "a", "/data/B")];
        assert_eq!(cycles(&catalog, &existing).len(), 1, "a and b already loop");
        // Editing something else must still be possible, or the workspace is
        // unrecoverable one subscription at a time.
        assert!(check_addition(&catalog, &existing, &sub("s3", "d", "/data/A")).is_ok());
    }

    /// A producer filter is a legitimate way OUT of a loop, and it is honoured
    /// because the check asks `wants` rather than re-deciding matching.
    #[test]
    fn a_producer_filter_breaks_the_loop() {
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B")]);
        let looping = vec![sub("s1", "b", "/data/A"), sub("s2", "a", "/data/B")];
        assert_eq!(cycles(&catalog, &looping).len(), 1);
        let mut narrowed = looping.clone();
        narrowed[1].producer = Some("someone-else".into());
        assert!(cycles(&catalog, &narrowed).is_empty(), "a is no longer run by b");
    }

    #[test]
    fn a_disabled_subscription_is_not_an_edge() {
        let catalog = catalog_of(&[("a", "/data/A"), ("b", "/data/B")]);
        let mut subs = vec![sub("s1", "b", "/data/A"), sub("s2", "a", "/data/B")];
        assert_eq!(cycles(&catalog, &subs).len(), 1);
        subs[0].enabled = false;
        assert!(cycles(&catalog, &subs).is_empty());
    }
}
