//! Reusable sub-flows (a Talend joblet, a Hop mapping): a pipeline whose rows
//! come in through `ctl.subflow.in` nodes and leave through `ctl.subflow.out`
//! nodes, used in another pipeline as one `ctl.subflow` node. Before a pipeline
//! is planned, every `ctl.subflow` node is replaced by the sub-flow's own
//! nodes, so the plan, its SQL and every run surface see one ordinary graph.
//!
//! A boundary node names its port (`port`, "main" unless set); the caller wires
//! each of its links to a port of that name. An inner node keeps its id behind
//! the call's (`<call>__<inner>`), and a sub-flow with a "main" output keeps
//! the call's own id as a pass-through of it, so the caller's canvas still
//! shows the call's rows.

use std::collections::{HashMap, HashSet};

use duckle_metadata::{PipelineEdge, PipelineNode};

use crate::plan::PipelineDoc;
use crate::EngineError;

pub const CALL: &str = "ctl.subflow";
pub const INPUT: &str = "ctl.subflow.in";
pub const OUTPUT: &str = "ctl.subflow.out";
/// What a call leaves under its own id: a pass-through of its "main" output.
/// Not a boundary, so a sub-flow that calls a sub-flow keeps one way out.
pub const STAND_IN: &str = "ctl.subflow.call";

/// How deep sub-flows may call sub-flows: far past any real design, and well
/// short of the stack.
const MAX_DEPTH: usize = 16;

fn component(n: &PipelineNode) -> &str {
    n.data.component_id.as_deref().unwrap_or("")
}

fn prop<'a>(n: &'a PipelineNode, key: &str) -> Option<&'a str> {
    n.data.properties.as_ref()?.get(key)?.as_str()
}

/// A boundary node's port: its `port`, "main" when unset.
pub fn port_of(n: &PipelineNode) -> String {
    prop(n, "port")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .unwrap_or("main")
        .to_string()
}

/// The sample file an input reads when its sub-flow runs on its own.
pub fn sample_path(n: &PipelineNode) -> Option<String> {
    prop(n, "samplePath").map(str::trim).filter(|p| !p.is_empty()).map(str::to_string)
}

/// Refuse a run that would read an input with no sample: a sub-flow run on its
/// own, with nothing to feed that input. Checked against the stages the run
/// will execute, so running only a branch the input does not feed is fine.
/// Not a compile error, because a sub-flow is a whole pipeline that every
/// check reading pipelines without running them takes like any other.
pub fn refuse_unfed_inputs(doc: &PipelineDoc, stages: &[crate::plan::Stage]) -> Result<(), EngineError> {
    for stage in stages.iter().filter(|s| s.component_id == INPUT) {
        let fed = doc.nodes.iter().any(|n| n.id == stage.node_id && sample_path(n).is_some());
        if !fed {
            return Err(EngineError::Config(format!(
                "{} `{}`: a sub-flow's input has rows only when a pipeline runs the sub-flow \
                 through a Sub-flow node. To run it on its own, give the input a sample file \
                 (samplePath).",
                INPUT, stage.node_id
            )));
        }
    }
    Ok(())
}

fn handle(h: &Option<String>) -> &str {
    h.as_deref().filter(|h| !h.is_empty()).unwrap_or("main")
}

/// Reads a sub-flow from the reference a Sub-flow node holds: where it was read
/// from, and the document.
pub type Loader<'a> = dyn Fn(&str) -> Result<(String, PipelineDoc), EngineError> + 'a;

/// `doc` with every sub-flow call expanded; None when it makes none. `subs` are
/// the variables the run was handed (a For Each child's, a batch item's): they
/// reach a sub-flow's steps as they reach the run's own.
///
/// Each sub-flow is read the way a Run Job's child is: found by path or id, the
/// workspace's variables and the run's put in, saved connections applied. A run
/// resolves only the document it was handed, and a sub-flow is not it.
pub fn expand(doc: &PipelineDoc, subs: &HashMap<String, String>) -> Result<Option<PipelineDoc>, EngineError> {
    expand_with(doc, &|reference| crate::connectors::load_child_pipeline(reference, subs))
}

/// [`expand`], reading each sub-flow with `load`: a release's gate reads the
/// copy the release stored, not the workspace's.
pub fn expand_with(doc: &PipelineDoc, load: &Loader) -> Result<Option<PipelineDoc>, EngineError> {
    if !doc.nodes.iter().any(|n| component(n) == CALL) {
        return Ok(None);
    }
    let mut out = doc.clone();
    expand_calls(&mut out, load, &mut Vec::new())?;
    Ok(Some(out))
}

/// `stack` holds the path of each sub-flow being expanded.
fn expand_calls(doc: &mut PipelineDoc, load: &Loader, stack: &mut Vec<String>) -> Result<(), EngineError> {
    let calls: Vec<PipelineNode> = doc.nodes.iter().filter(|n| component(n) == CALL).cloned().collect();
    for call in calls {
        let reference = prop(&call, "subflowRef")
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "ctl.subflow `{}`: pick the sub-flow it runs (subflowRef)",
                    call.id
                ))
            })?
            .to_string();
        let (path, mut child) = load(&reference).map_err(|e| {
            let why = match e {
                EngineError::Config(why) => why,
                other => other.to_string(),
            };
            EngineError::Config(format!("ctl.subflow `{}`: {}", call.id, why))
        })?;
        if stack.contains(&path) {
            return Err(EngineError::Config(format!(
                "ctl.subflow `{}`: the sub-flow {} calls itself (through {})",
                call.id,
                path,
                stack.join(" -> ")
            )));
        }
        if stack.len() >= MAX_DEPTH {
            return Err(EngineError::Config(format!(
                "ctl.subflow `{}`: sub-flows nest more than {} deep ({})",
                call.id,
                MAX_DEPTH,
                stack.join(" -> ")
            )));
        }
        stack.push(path.clone());
        expand_calls(&mut child, load, stack)?;
        stack.pop();
        splice(doc, &call, &child, &path)?;
    }
    Ok(())
}

/// Replace `call` in `doc` with the nodes of `child`, rewiring the call's links
/// to the ports they name.
fn splice(
    doc: &mut PipelineDoc,
    call: &PipelineNode,
    child: &PipelineDoc,
    path: &str,
) -> Result<(), EngineError> {
    let refuse = |what: String| EngineError::Config(format!("ctl.subflow `{}`: {}", call.id, what));
    let inner_id = |id: &str| format!("{}__{}", call.id, id);

    let inputs: Vec<&PipelineNode> = child.nodes.iter().filter(|n| component(n) == INPUT).collect();
    let outputs: Vec<&PipelineNode> = child.nodes.iter().filter(|n| component(n) == OUTPUT).collect();
    for (side, nodes) in [("input", &inputs), ("output", &outputs)] {
        let mut seen = HashSet::new();
        for n in nodes.iter() {
            if !seen.insert(port_of(n)) {
                return Err(refuse(format!("{} has two {}s called \"{}\"", path, side, port_of(n))));
            }
        }
    }
    let boundary: HashSet<&str> = inputs.iter().chain(outputs.iter()).map(|n| n.id.as_str()).collect();

    let into: Vec<PipelineEdge> = doc.edges.iter().filter(|e| e.target == call.id).cloned().collect();
    let out_of: Vec<PipelineEdge> = doc.edges.iter().filter(|e| e.source == call.id).cloned().collect();
    for e in &into {
        let port = handle(&e.target_handle);
        if !inputs.iter().any(|n| port_of(n) == port) {
            return Err(refuse(format!("{} has no input \"{}\"", path, port)));
        }
    }
    for e in &out_of {
        let port = handle(&e.source_handle);
        if !outputs.iter().any(|n| port_of(n) == port) {
            return Err(refuse(format!("{} has no output \"{}\"", path, port)));
        }
    }

    // What feeds each input node: the one caller link wired to its port.
    let mut feeds: HashMap<&str, &PipelineEdge> = HashMap::new();
    for n in &inputs {
        let port = port_of(n);
        let wired: Vec<&PipelineEdge> = into.iter().filter(|e| handle(&e.target_handle) == port).collect();
        match wired.as_slice() {
            [one] => {
                feeds.insert(n.id.as_str(), *one);
            }
            [] => return Err(refuse(format!("nothing is wired into its input \"{}\"", port))),
            many => {
                return Err(refuse(format!(
                    "its input \"{}\" has {} links into it; it takes one",
                    port,
                    many.len()
                )))
            }
        }
    }

    // Where each output's rows come from, in the expanded graph.
    let mut origins: HashMap<String, (String, Option<String>)> = HashMap::new();
    for n in &outputs {
        let port = port_of(n);
        let wired: Vec<&PipelineEdge> = child.edges.iter().filter(|e| e.target == n.id).collect();
        let e = match wired.as_slice() {
            [one] => *one,
            [] => return Err(refuse(format!("in {}, nothing is wired into its output \"{}\"", path, port))),
            many => {
                return Err(refuse(format!(
                    "in {}, its output \"{}\" has {} links into it; it takes one",
                    path,
                    port,
                    many.len()
                )))
            }
        };
        let origin = match feeds.get(e.source.as_str()) {
            Some(feed) => (feed.source.clone(), feed.source_handle.clone()),
            None => (inner_id(&e.source), e.source_handle.clone()),
        };
        origins.insert(port, origin);
    }

    let mut nodes: Vec<PipelineNode> = doc.nodes.iter().filter(|n| n.id != call.id).cloned().collect();
    let mut edges: Vec<PipelineEdge> = doc
        .edges
        .iter()
        .filter(|e| e.target != call.id && e.source != call.id)
        .cloned()
        .collect();

    for n in child.nodes.iter().filter(|n| !boundary.contains(n.id.as_str())) {
        let mut inner = n.clone();
        inner.id = inner_id(&n.id);
        inner.data.label = format!("{} / {}", call.data.label, n.data.label);
        nodes.push(inner);
    }
    for e in &child.edges {
        if outputs.iter().any(|n| n.id == e.target) {
            continue; // stands in as an origin
        }
        let mut inner = e.clone();
        inner.id = inner_id(&e.id);
        inner.target = inner_id(&e.target);
        match feeds.get(e.source.as_str()) {
            Some(feed) => {
                inner.source = feed.source.clone();
                inner.source_handle = feed.source_handle.clone();
            }
            None => inner.source = inner_id(&e.source),
        }
        edges.push(inner);
    }

    // A "main" output keeps the call's id, as a pass-through of what leaves the
    // sub-flow there, so the caller's links from it and its preview still work.
    if let Some((source, source_handle)) = origins.get("main") {
        let mut stand_in = call.clone();
        stand_in.data.component_id = Some(STAND_IN.to_string());
        stand_in.data.properties = Some(serde_json::json!({}));
        nodes.push(stand_in);
        edges.push(PipelineEdge {
            id: inner_id("main"),
            source: source.clone(),
            target: call.id.clone(),
            source_handle: source_handle.clone(),
            target_handle: Some("main".to_string()),
            edge_type: None,
            data: None,
        });
    }
    for e in &out_of {
        let port = handle(&e.source_handle);
        if port == "main" {
            edges.push(e.clone());
            continue;
        }
        let (source, source_handle) = origins[port].clone();
        let mut rewired = e.clone();
        rewired.source = source;
        rewired.source_handle = source_handle;
        edges.push(rewired);
    }

    doc.nodes = nodes;
    doc.edges = edges;
    Ok(())
}
