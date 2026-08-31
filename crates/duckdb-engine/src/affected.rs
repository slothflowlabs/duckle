//! #308: which pipelines a change reaches, and why.
//!
//! A change to one pipeline is rarely contained to it. The producer of a table
//! is edited, and three pipelines that read that table now compile against a
//! shape nobody checked. CI needs to know which ones without running the whole
//! workspace, and an operator promoting a release needs the same answer with a
//! reason they can argue with.
//!
//! ## Every result carries why it is here
//!
//! A selection with no explanation is not reviewable: the reader either trusts
//! it completely or ignores it completely, and both are wrong. So each selected
//! pipeline carries the chain that reached it - `orders -> canonical_orders ->
//! serving_orders` - and a reviewer can point at the hop they disagree with.
//!
//! ## Uncertain is a result, not a gap
//!
//! A source whose target the catalog could not name might read anything, so
//! nothing downstream of it can be ruled out. Dropping those silently is the
//! failure mode that makes a gate untrustworthy - it reports "2 affected" and
//! is quietly wrong. They are always listed; `include_uncertain` decides only
//! whether they are also *selected*.
//!
//! ## Two kinds of edge
//!
//! The asset graph is one: a pipeline writes a table, another reads it. The
//! other is `pipelineRef` - a parent invoking a child - and it runs the other
//! way: the CHILD changing affects the PARENT. Following only the asset graph
//! misses every sub-pipeline edit, which is exactly the change most likely to
//! be assumed harmless.

use crate::catalog::{Catalog, Direction};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

/// Bumped when the emitted document changes shape, so CI that pins a version
/// fails loudly rather than parsing a document it half understands (#308.4).
pub const SCHEMA_VERSION: u32 = 1;

/// Why one pipeline is in the selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Reason {
    /// Did not exist at the base revision.
    Added,
    /// Its own definition differs from the base revision.
    Changed,
    /// Reached through the asset graph. The chain alternates pipeline, asset,
    /// pipeline, starting at the pipeline that actually changed.
    Downstream { path: Vec<String> },
    /// Invokes a changed child pipeline through `pipelineRef`.
    Child { path: Vec<String>, node: String },
    /// A shared input it depends on changed: a context value, a lock file.
    Shared { input: String, key: Option<String> },
    /// Has a dependency the catalog could not name, so it cannot be ruled out.
    Uncertain { node: String, why: String },
}

/// One pipeline, and every reason it was reached.
///
/// Several reasons rather than the first: a pipeline that both changed and sits
/// downstream of another change is a different review than one that only
/// changed, and collapsing them hides that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Selected {
    pub pipeline: String,
    pub reasons: Vec<Reason>,
    /// True when at least one reason is [`Reason::Uncertain`].
    pub uncertain: bool,
}

/// A dependency the catalog could not resolve, reported whether or not the
/// pipeline holding it was selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Uncertainty {
    pub pipeline: String,
    pub node: String,
    pub why: String,
}

/// A changed input that is not itself a pipeline.
///
/// `keys` is what changed inside it. Empty means the input is opaque - a lock
/// file, a shared template - and every pipeline is treated as depending on it,
/// because narrowing without evidence is how a gate misses the one that
/// mattered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedChange {
    pub input: String,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Selection {
    pub schema_version: u32,
    pub base: String,
    pub head: String,
    pub selected: Vec<Selected>,
    /// Pipelines that existed at the base and no longer do. They cannot be run,
    /// but whatever read what they wrote is affected, so they still seed the
    /// walk.
    pub removed: Vec<String>,
    /// The selected pipelines in an order that runs producers before consumers
    /// and children before parents.
    pub order: Vec<String>,
    /// Pipelines the order could not place because they depend on each other.
    /// Listed rather than silently ordered arbitrarily: an arbitrary order that
    /// looks topological is worse than an admitted cycle.
    pub cycles: Vec<String>,
    pub uncertain: Vec<Uncertainty>,
}

/// Canvas geometry, dropped before comparing.
///
/// Dragging a node changes the file and changes nothing that runs. A gate that
/// fires on every drag is a gate people learn to skip, and then it is not
/// protecting anything. Only `position` is dropped: a label is user-visible and
/// reaches run receipts, so calling it cosmetic is not this module's call to
/// make.
fn without_geometry(doc: &Value) -> Value {
    let mut doc = doc.clone();
    if let Some(nodes) = doc.get_mut("nodes").and_then(Value::as_array_mut) {
        for node in nodes.iter_mut() {
            if let Some(object) = node.as_object_mut() {
                object.remove("position");
            }
        }
    }
    doc
}

/// A `pipelineRef` resolves by file stem, the way the runner resolves it, so a
/// reference written as a bare name, a filename or a path all reach the same
/// pipeline.
fn stem(reference: &str) -> String {
    std::path::Path::new(reference)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| reference.to_string())
}

/// Every `(node_id, child_stem)` a document invokes.
fn child_refs(doc: &Value) -> Vec<(String, String)> {
    let Some(nodes) = doc.get("nodes").and_then(Value::as_array) else { return Vec::new() };
    nodes
        .iter()
        .filter_map(|node| {
            let id = node.get("id")?.as_str()?.to_string();
            let r = node.get("data")?.get("properties")?.get("pipelineRef")?.as_str()?;
            Some((id, stem(r)))
        })
        .collect()
}

/// Whether a document mentions `key`, at word boundaries.
///
/// Over the raw document rather than a parsed one, like everything else here.
/// Parsing would introduce a failure case - a document this build cannot read -
/// whose only safe answer is "affected", and a check that returns "affected"
/// for every pipeline whenever a field is unfamiliar is not a check.
///
/// Word boundaries are shared with #302 rather than re-derived, so `region`
/// does not match `region_code` here and there for different reasons.
fn mentions(doc: &Value, key: &str) -> bool {
    fn walk(v: &Value, key: &str) -> bool {
        match v {
            Value::String(s) => crate::contracts::contains_word(s, key),
            Value::Array(a) => a.iter().any(|x| walk(x, key)),
            Value::Object(m) => m.values().any(|x| walk(x, key)),
            _ => false,
        }
    }
    doc.get("nodes")
        .and_then(Value::as_array)
        .map(|nodes| nodes.iter().filter_map(|n| n.get("data")).any(|d| walk(d, key)))
        .unwrap_or(false)
}

pub fn select(
    base: &[(String, Value)],
    head: &[(String, Value)],
    catalog: &Catalog,
    shared: &[SharedChange],
    include_uncertain: bool,
) -> Selection {
    let base_by_id: HashMap<&str, &Value> = base.iter().map(|(i, d)| (i.as_str(), d)).collect();
    let head_by_id: HashMap<&str, &Value> = head.iter().map(|(i, d)| (i.as_str(), d)).collect();

    let mut reasons: BTreeMap<String, Vec<Reason>> = BTreeMap::new();

    // ---- what changed on its own terms ---------------------------------
    for (id, doc) in head {
        match base_by_id.get(id.as_str()) {
            None => reasons.entry(id.clone()).or_default().push(Reason::Added),
            Some(before) => {
                if without_geometry(before) != without_geometry(doc) {
                    reasons.entry(id.clone()).or_default().push(Reason::Changed);
                }
            }
        }
    }
    let removed: Vec<String> = base
        .iter()
        .map(|(id, _)| id)
        .filter(|id| !head_by_id.contains_key(id.as_str()))
        .cloned()
        .collect();

    // ---- shared inputs --------------------------------------------------
    for change in shared {
        for (id, doc) in head {
            if change.keys.is_empty() {
                reasons.entry(id.clone()).or_default().push(Reason::Shared {
                    input: change.input.clone(),
                    key: None,
                });
                continue;
            }
            for key in &change.keys {
                if mentions(doc, key) {
                    reasons.entry(id.clone()).or_default().push(Reason::Shared {
                        input: change.input.clone(),
                        key: Some(key.clone()),
                    });
                }
            }
        }
    }

    // ---- the two edge indices -------------------------------------------
    let mut writes: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut readers: HashMap<&str, Vec<&str>> = HashMap::new();
    // A deleted pipeline writes nothing in the HEAD graph, so the head graph
    // alone says deleting a producer affects nobody - which is the opposite of
    // the truth, and the one change most worth catching.
    let base_graph = crate::catalog::build_from_documents(base);
    for touch in base_graph.touches.iter().filter(|t| removed.iter().any(|r| *r == t.pipeline_id)) {
        if touch.direction == Direction::Write {
            writes.entry(touch.pipeline_id.as_str()).or_default().push(touch.asset.as_str());
        }
    }
    for touch in &catalog.touches {
        match touch.direction {
            Direction::Write => {
                writes.entry(&touch.pipeline_id).or_default().push(&touch.asset)
            }
            Direction::Read => readers.entry(&touch.asset).or_default().push(&touch.pipeline_id),
        }
    }
    // Child -> the parents that invoke it. Built from HEAD, because the
    // question is what would run now, not what used to.
    let mut parents: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (id, doc) in head {
        for (node, child) in child_refs(doc) {
            parents.entry(child).or_default().push((id.clone(), node));
        }
    }

    // ---- the walk --------------------------------------------------------
    // Breadth-first, so the first chain to reach a pipeline is the shortest -
    // which is also the one a reviewer will find easiest to check.
    let mut chain: BTreeMap<String, Vec<String>> = BTreeMap::new();
    // Separate from `chain`, because a pipeline that changed on its own AND
    // sits downstream of another change is a different review from one that
    // only changed. `chain` decides what to walk; this decides what to say,
    // once, with the shortest chain that reached it.
    let mut explained: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    for seed in reasons.keys().cloned().chain(removed.iter().cloned()) {
        if chain.insert(seed.clone(), vec![seed.clone()]).is_none() {
            queue.push_back(seed);
        }
    }
    while let Some(current) = queue.pop_front() {
        let here = chain.get(&current).cloned().unwrap_or_default();
        let mut assets: Vec<&str> = writes.get(current.as_str()).cloned().unwrap_or_default();
        assets.sort_unstable();
        assets.dedup();
        for asset in assets {
            let mut downstream: Vec<&str> = readers.get(asset).cloned().unwrap_or_default();
            downstream.sort_unstable();
            downstream.dedup();
            for next in downstream {
                // A pipeline that reads back what it writes is a normal
                // incremental pattern, not an edge to follow.
                if next == current {
                    continue;
                }
                let mut path = here.clone();
                path.push(asset.to_string());
                path.push(next.to_string());
                if !chain.contains_key(next) {
                    chain.insert(next.to_string(), path.clone());
                    queue.push_back(next.to_string());
                }
                if explained.insert(next.to_string()) {
                    reasons.entry(next.to_string()).or_default().push(Reason::Downstream { path });
                }
            }
        }
        let mut invoking = parents.get(&current).cloned().unwrap_or_default();
        invoking.sort();
        for (parent, node) in invoking {
            if parent == current {
                continue;
            }
            let mut path = here.clone();
            path.push(parent.clone());
            if !chain.contains_key(&parent) {
                chain.insert(parent.clone(), path.clone());
                queue.push_back(parent.clone());
            }
            if explained.insert(parent.clone()) {
                reasons.entry(parent).or_default().push(Reason::Child { path, node });
            }
        }
    }

    // ---- uncertainty ------------------------------------------------------
    // Reported for every pipeline that has one, selected or not: the point is
    // that the reader sees what the answer could not account for.
    let mut uncertain: Vec<Uncertainty> = catalog
        .unresolved
        .iter()
        .map(|u| Uncertainty {
            pipeline: u.pipeline_id.clone(),
            node: format!("{} ({})", u.node_id, u.component_id),
            why: u.reason.clone(),
        })
        .collect();
    // A path the catalog DID name but that still holds a `${...}` is the more
    // common case, and the more dangerous one: the graph gives it a confident
    // id, and that id is not what the run will read. Two pipelines naming
    // `${DIR}/x.parquet` may touch different files, and a pipeline naming it
    // may touch a file another pipeline names literally. Neither the edge nor
    // its absence can be trusted, so it is uncertain even though nothing failed
    // to resolve.
    for touch in &catalog.touches {
        if !touch.asset.contains("${") {
            continue;
        }
        uncertain.push(Uncertainty {
            pipeline: touch.pipeline_id.clone(),
            node: format!("{} ({})", touch.node_id, touch.component_id),
            why: format!(
                "{} is decided at run time, so what it {} cannot be known here",
                touch.asset,
                match touch.direction {
                    Direction::Read => "reads",
                    Direction::Write => "writes",
                }
            ),
        });
    }
    uncertain.dedup_by(|a, b| a.pipeline == b.pipeline && a.node == b.node && a.why == b.why);
    uncertain.sort_by(|a, b| (&a.pipeline, &a.node).cmp(&(&b.pipeline, &b.node)));
    if include_uncertain {
        for u in &uncertain {
            if head_by_id.contains_key(u.pipeline.as_str()) {
                reasons
                    .entry(u.pipeline.clone())
                    .or_default()
                    .push(Reason::Uncertain { node: u.node.clone(), why: u.why.clone() });
            }
        }
    }

    // A removed pipeline seeded the walk but cannot be run.
    for id in &removed {
        reasons.remove(id);
    }

    let selected: Vec<Selected> = reasons
        .into_iter()
        .map(|(pipeline, reasons)| Selected {
            uncertain: reasons.iter().any(|r| matches!(r, Reason::Uncertain { .. })),
            pipeline,
            reasons,
        })
        .collect();

    let ids: BTreeSet<&str> = selected.iter().map(|s| s.pipeline.as_str()).collect();
    let (order, cycles) = topological(&ids, &writes, &readers, &parents);

    Selection {
        schema_version: SCHEMA_VERSION,
        base: String::new(),
        head: String::new(),
        selected,
        removed,
        order,
        cycles,
        uncertain,
    }
}

/// Producers before consumers, children before parents.
///
/// Kahn's algorithm over the selected set only: a dependency outside the
/// selection is not being run, so it constrains nothing. Whatever is left when
/// no node has an empty in-degree is a cycle, and it is returned as one rather
/// than flushed into the order, because an arbitrary order that looks
/// topological is worse than an admitted cycle.
fn topological(
    ids: &BTreeSet<&str>,
    writes: &HashMap<&str, Vec<&str>>,
    readers: &HashMap<&str, Vec<&str>>,
    parents: &HashMap<String, Vec<(String, String)>>,
) -> (Vec<String>, Vec<String>) {
    let mut after: BTreeMap<&str, BTreeSet<&str>> = ids.iter().map(|i| (*i, BTreeSet::new())).collect();
    let mut before: BTreeMap<&str, BTreeSet<&str>> = after.clone();
    let mut edge = |from: &str, to: &str| {
        if from == to {
            return;
        }
        if let (Some(f), Some(t)) = (ids.get(from), ids.get(to)) {
            after.get_mut(*f).map(|s| s.insert(*t));
            before.get_mut(*t).map(|s| s.insert(*f));
        }
    };
    for (producer, assets) in writes {
        for asset in assets {
            for consumer in readers.get(*asset).into_iter().flatten() {
                edge(producer, consumer);
            }
        }
    }
    for (child, invoking) in parents {
        for (parent, _) in invoking {
            edge(child, parent);
        }
    }

    let mut order: Vec<String> = Vec::new();
    let mut ready: VecDeque<&str> =
        before.iter().filter(|(_, d)| d.is_empty()).map(|(i, _)| *i).collect();
    let mut placed: BTreeSet<&str> = ready.iter().copied().collect();
    while let Some(id) = ready.pop_front() {
        order.push(id.to_string());
        for next in after.get(id).cloned().unwrap_or_default() {
            let deps = before.get_mut(next).expect("edge target is in the set");
            deps.remove(id);
            if deps.is_empty() && placed.insert(next) {
                ready.push_back(next);
            }
        }
    }
    let cycles: Vec<String> =
        ids.iter().filter(|i| !placed.contains(*i)).map(|i| i.to_string()).collect();
    (order, cycles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;

    fn doc(name: &str, nodes: Value) -> Value {
        serde_json::json!({ "name": name, "nodes": nodes, "edges": [] })
    }

    fn reader(asset: &str) -> Value {
        serde_json::json!([{ "id": "s", "type": "source",
            "data": { "componentId": "src.parquet", "properties": { "path": asset } } }])
    }

    fn writer(asset: &str) -> Value {
        serde_json::json!([{ "id": "w", "type": "sink",
            "data": { "componentId": "snk.parquet", "properties": { "path": asset } } }])
    }

    fn reader_writer(read: &str, write: &str) -> Value {
        serde_json::json!([
            { "id": "s", "type": "source",
              "data": { "componentId": "src.parquet", "properties": { "path": read } } },
            { "id": "w", "type": "sink",
              "data": { "componentId": "snk.parquet", "properties": { "path": write } } }
        ])
    }

    fn graph(docs: &[(String, Value)]) -> catalog::Catalog {
        catalog::build_from_documents(docs)
    }

    fn ids(s: &Selection) -> Vec<&str> {
        s.selected.iter().map(|x| x.pipeline.as_str()).collect()
    }

    #[test]
    fn a_changed_producer_reaches_its_transitive_consumers() {
        let base = vec![
            ("produce".into(), doc("produce", writer("a.parquet"))),
            ("middle".into(), doc("middle", reader_writer("a.parquet", "b.parquet"))),
            ("serve".into(), doc("serve", reader("b.parquet"))),
            ("elsewhere".into(), doc("elsewhere", reader("z.parquet"))),
        ];
        let mut head = base.clone();
        head[0].1 = doc("produce v2", writer("a.parquet"));
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert_eq!(ids(&sel), vec!["middle", "produce", "serve"]);
        // and it says why, as a chain someone can check hop by hop
        let serve = sel.selected.iter().find(|s| s.pipeline == "serve").unwrap();
        assert_eq!(
            serve.reasons,
            vec![Reason::Downstream {
                path: vec![
                    "produce".into(),
                    "a.parquet".into(),
                    "middle".into(),
                    "b.parquet".into(),
                    "serve".into()
                ]
            }]
        );
    }

    #[test]
    fn producers_run_before_the_pipelines_that_read_them() {
        let base = vec![
            ("produce".into(), doc("produce", writer("a.parquet"))),
            ("middle".into(), doc("middle", reader_writer("a.parquet", "b.parquet"))),
            ("serve".into(), doc("serve", reader("b.parquet"))),
        ];
        let mut head = base.clone();
        head[0].1 = doc("produce v2", writer("a.parquet"));
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert_eq!(sel.order, vec!["produce", "middle", "serve"]);
        assert!(sel.cycles.is_empty());
    }

    #[test]
    fn a_cycle_is_admitted_rather_than_ordered_arbitrarily() {
        // Two pipelines that each read what the other writes.
        let base = vec![
            ("left".into(), doc("left", reader_writer("b.parquet", "a.parquet"))),
            ("right".into(), doc("right", reader_writer("a.parquet", "b.parquet"))),
        ];
        let mut head = base.clone();
        head[0].1 = doc("left v2", reader_writer("b.parquet", "a.parquet"));
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert_eq!(ids(&sel), vec!["left", "right"]);
        assert!(sel.order.is_empty(), "neither can go first");
        assert_eq!(sel.cycles, vec!["left", "right"]);
    }

    #[test]
    fn moving_a_node_on_the_canvas_is_not_a_change() {
        let one = serde_json::json!({ "name": "p", "edges": [], "nodes": [
            { "id": "w", "type": "sink", "position": { "x": 0, "y": 0 },
              "data": { "componentId": "snk.parquet", "properties": { "path": "a.parquet" } } }]});
        let mut two = one.clone();
        two["nodes"][0]["position"] = serde_json::json!({ "x": 400, "y": 120 });
        let base = vec![("p".to_string(), one)];
        let head = vec![("p".to_string(), two)];
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert!(sel.selected.is_empty(), "a drag is not a change: {:?}", sel.selected);
    }

    #[test]
    fn a_changed_child_pipeline_affects_the_parent_that_invokes_it() {
        // The asset graph has no edge here at all: the parent's only link to
        // the child is a pipelineRef.
        let parent = doc(
            "parent",
            serde_json::json!([{ "id": "fe", "type": "transform",
                "data": { "componentId": "ctl.foreach",
                          "properties": { "pipelineRef": "child" } } }]),
        );
        let base = vec![
            ("child".into(), doc("child", writer("a.parquet"))),
            ("parent".into(), parent.clone()),
        ];
        let mut head = base.clone();
        head[0].1 = doc("child v2", writer("a.parquet"));
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert_eq!(ids(&sel), vec!["child", "parent"]);
        let p = sel.selected.iter().find(|s| s.pipeline == "parent").unwrap();
        assert_eq!(
            p.reasons,
            vec![Reason::Child {
                path: vec!["child".into(), "parent".into()],
                node: "fe".into()
            }]
        );
        // and the child runs first
        assert_eq!(sel.order, vec!["child", "parent"]);
    }

    #[test]
    fn a_removed_pipeline_is_not_run_but_its_consumers_are_affected() {
        let base = vec![
            ("produce".into(), doc("produce", writer("a.parquet"))),
            ("serve".into(), doc("serve", reader("a.parquet"))),
        ];
        let head = vec![("serve".to_string(), doc("serve", reader("a.parquet")))];
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert_eq!(sel.removed, vec!["produce"]);
        assert_eq!(ids(&sel), vec!["serve"], "the deleted one cannot be run");
    }

    #[test]
    fn an_unresolved_dependency_is_always_visible_and_only_selected_on_request() {
        // A source whose path is a run-time value the catalog cannot name.
        let vague = doc(
            "vague",
            serde_json::json!([{ "id": "s", "type": "source",
                "data": { "componentId": "src.parquet", "properties": {} } }]),
        );
        let base = vec![("vague".to_string(), vague.clone())];
        let head = base.clone();
        let g = graph(&head);
        assert!(!g.unresolved.is_empty(), "fixture must actually be unresolvable");

        let quiet = select(&base, &head, &g, &[], false);
        assert!(quiet.selected.is_empty(), "nothing changed");
        assert_eq!(quiet.uncertain.len(), 1, "but it is still reported");

        let conservative = select(&base, &head, &g, &[], true);
        assert_eq!(ids(&conservative), vec!["vague"]);
        assert!(conservative.selected[0].uncertain);
    }

    #[test]
    fn a_run_time_path_is_uncertain_even_though_the_catalog_named_it() {
        // The catalog resolves this to the literal id `${DIR}/in.parquet`,
        // which looks like a perfectly good asset and is not one.
        let dynamic = doc("dynamic", reader_writer("${DIR}/in.parquet", "out.parquet"));
        let base = vec![("dynamic".to_string(), dynamic.clone())];
        let head = base.clone();
        let g = graph(&head);
        assert!(g.unresolved.is_empty(), "the catalog does NOT flag this itself");

        let sel = select(&base, &head, &g, &[], false);
        assert_eq!(sel.uncertain.len(), 1, "but the selection must: {:?}", sel.uncertain);
        assert_eq!(sel.uncertain[0].pipeline, "dynamic");
        assert!(sel.selected.is_empty(), "still not selected unless asked");

        let conservative = select(&base, &head, &g, &[], true);
        assert_eq!(ids(&conservative), vec!["dynamic"]);
    }

    #[test]
    fn a_changed_context_key_selects_only_the_pipelines_that_use_it() {
        let uses = doc(
            "uses",
            serde_json::json!([{ "id": "s", "type": "source",
                "data": { "componentId": "src.parquet",
                          "properties": { "path": "${region}/a.parquet" } } }]),
        );
        let base = vec![
            ("uses".into(), uses.clone()),
            ("ignores".into(), doc("ignores", reader("z.parquet"))),
        ];
        let head = base.clone();
        let shared =
            [SharedChange { input: "contexts/prod.json".into(), keys: vec!["region".into()] }];
        let sel = select(&base, &head, &graph(&head), &shared, false);
        assert_eq!(ids(&sel), vec!["uses"]);
        assert_eq!(
            sel.selected[0].reasons,
            vec![Reason::Shared {
                input: "contexts/prod.json".into(),
                key: Some("region".into())
            }]
        );
    }

    #[test]
    fn an_opaque_shared_input_selects_everything() {
        let base = vec![
            ("one".into(), doc("one", writer("a.parquet"))),
            ("two".into(), doc("two", reader("z.parquet"))),
        ];
        let head = base.clone();
        let shared = [SharedChange { input: "duckle.lock".into(), keys: vec![] }];
        let sel = select(&base, &head, &graph(&head), &shared, false);
        assert_eq!(ids(&sel), vec!["one", "two"]);
    }

    #[test]
    fn a_pipeline_that_both_changed_and_sits_downstream_keeps_both_reasons() {
        let base = vec![
            ("produce".into(), doc("produce", writer("a.parquet"))),
            ("serve".into(), doc("serve", reader("a.parquet"))),
        ];
        let mut head = base.clone();
        head[0].1 = doc("produce v2", writer("a.parquet"));
        head[1].1 = doc("serve v2", reader("a.parquet"));
        let sel = select(&base, &head, &graph(&head), &[], false);
        let serve = sel.selected.iter().find(|s| s.pipeline == "serve").unwrap();
        assert_eq!(
            serve.reasons,
            vec![
                Reason::Changed,
                Reason::Downstream {
                    path: vec!["produce".into(), "a.parquet".into(), "serve".into()]
                }
            ]
        );
    }

    #[test]
    fn a_document_reordered_but_not_edited_is_not_a_change() {
        let one = serde_json::json!({ "name": "p", "nodes": [], "edges": [] });
        let two = serde_json::json!({ "edges": [], "nodes": [], "name": "p" });
        let base = vec![("p".to_string(), one)];
        let head = vec![("p".to_string(), two)];
        let sel = select(&base, &head, &graph(&head), &[], false);
        assert!(sel.selected.is_empty());
    }
}
