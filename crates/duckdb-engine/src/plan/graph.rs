//! Graph compilation: topological sort, input-port mapping, schema
//! propagation, and column-reference validation. Extracted from plan/mod.rs.

use super::*;

/// Reject-port outputs are named "<node>__reject"; the schema map keys
/// on the unsuffixed node id.
pub(crate) fn strip_reject_suffix(s: &str) -> &str {
    s.strip_suffix(REJECT_SUFFIX).unwrap_or(s)
}

/// Compute the output column set a node exposes to its consumers,
/// given its own declared schema (if any) and its upstream's set.
///
/// Returns None when we don't know - either the upstream is unknown
/// or the component transforms columns in ways the planner doesn't
/// model (project, join, aggregation, etc). None disables column
/// validation for downstream nodes that read this output.
pub(crate) fn derive_output_columns(
    component_id: Option<&str>,
    props: Option<&JsonValue>,
    declared: Option<&[duckle_metadata::Column]>,
    upstream: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    // A source contributes its declared schema (if the user set one
    // via Autodetect / hand-typed Schema panel).
    if let Some(cols) = declared {
        if !cols.is_empty() {
            return Some(cols.iter().map(|c| c.name.clone()).collect());
        }
    }
    let component = match component_id {
        Some(c) => c,
        None => return None,
    };
    // TRUE pass-through transforms: output column set is exactly the
    // upstream's (they filter / reorder / retype rows, never add or
    // rename a column). Safe to propagate the upstream set so downstream
    // column-reference validation stays exact.
    if matches!(
        component,
        "xf.filter"
            | "xf.distinct"
            | "xf.sort"
            | "xf.limit"
            | "xf.topn"
            | "xf.sample"
            | "xf.skip"
            | "xf.log"
            | "xf.fill_forward"
            | "xf.fill_backward"
            | "xf.fill_constant"
            | "xf.cast"
            | "xf.rank.filter"
    ) {
        return upstream.cloned();
    }
    // Column-ADDING transforms (window functions, row_hash, audit, uuid,
    // ...) output the upstream columns PLUS one or more new ones whose
    // names we don't track here. Returning the upstream set would make
    // downstream validation falsely reject references to the column they
    // add (e.g. xf.rownum adds "row_num", then a downstream xf.distinct
    // on "row_num" looked "not found"). Return None = "schema unknown"
    // so downstream validation is skipped rather than wrong.
    if matches!(
        component,
        "xf.uuid"
            | "xf.audit"
            | "xf.row_hash"
            | "xf.rownum"
            | "xf.rank"
            | "xf.denserank"
            | "xf.lead"
            | "xf.lag"
            | "xf.first"
            | "xf.last"
            | "xf.ntile"
            | "xf.cumulative"
            | "xf.aggwin"
    ) {
        return None;
    }
    // xf.drop subtracts; xf.rename renames. Both decodeable from props.
    if component == "xf.drop" {
        let mut set = upstream.cloned()?;
        if let Some(p) = props {
            let drops = columns_from_props(p, "columns").unwrap_or_default();
            for d in drops {
                set.remove(&d);
            }
        }
        return Some(set);
    }
    if component == "xf.rename" {
        let mut set = upstream.cloned()?;
        if let Some(p) = props {
            // Use the same pair extraction build_rename uses, so the
            // derived schema reflects the renames regardless of which
            // prop shape the UI saved (renames/columns array OR mapping).
            for (from, to) in rename_pairs(p) {
                set.remove(&from);
                set.insert(to);
            }
        }
        return Some(set);
    }
    // xf.project narrows to the listed columns (or keep list).
    if component == "xf.project" {
        if let Some(p) = props {
            let cols = columns_from_props(p, "columns")
                .or_else(|| columns_from_props(p, "keep"))
                .unwrap_or_default();
            if !cols.is_empty() {
                return Some(cols.into_iter().collect());
            }
        }
    }
    // Everything else (joins, aggregations, projects with custom SQL,
    // sources without a declared schema, custom code blocks): unknown.
    None
}

/// Lightweight column-reference checks for transforms whose props
/// name an input column. Runs before stage compilation so the error
/// surfaces as a clear "column X not found in upstream" at the right
/// node, instead of DuckDB's run-time "Binder Error: column not found"
/// two stages later.
pub(crate) fn validate_column_refs(
    component_id: &str,
    props: Option<&JsonValue>,
    cols: &HashSet<String>,
) -> Result<(), String> {
    let p = match props {
        Some(p) => p,
        None => return Ok(()),
    };
    let check = |col: &str| -> Result<(), String> {
        let c = col.trim();
        if c.is_empty() {
            return Ok(()); // empty handled by per-component validation
        }
        if cols.contains(c) {
            return Ok(());
        }
        // If there's a case-insensitive match, that's almost always the
        // intended column (hand-typed case mismatch) - point straight at
        // it. Otherwise list the columns that ARE available so the user
        // can see the mismatch instead of guessing (e.g. an order_id
        // reference against a customers file).
        if let Some(k) = cols.iter().find(|k| k.eq_ignore_ascii_case(c)) {
            return Err(format!(
                "column '{}' not found in upstream (did you mean '{}'?)",
                c, k
            ));
        }
        let mut available: Vec<&str> = cols.iter().map(String::as_str).collect();
        available.sort_unstable();
        let shown = if available.len() > 15 {
            format!("{}, ...", available[..15].join(", "))
        } else {
            available.join(", ")
        };
        Err(format!(
            "column '{}' not found in upstream. Available columns: {}",
            c, shown
        ))
    };
    // Helper for components whose props expose a single "column" key.
    let check_single_col = |p: &JsonValue| -> Result<(), String> {
        if let Some(c) = p.get("column").and_then(JsonValue::as_str) {
            let c = c.trim();
            if !c.is_empty() {
                check(c)?;
            }
        }
        Ok(())
    };
    let check_list = |key: &str| -> Result<(), String> {
        for c in columns_list(p, key) {
            let c = c.trim();
            if !c.is_empty() {
                check(c)?;
            }
        }
        Ok(())
    };
    match component_id {
        "xf.fill_forward" | "xf.fill_backward" | "xf.fill_constant" => {
            check_single_col(p)?;
        }
        "xf.cast" => {
            // Multi-row form
            if let Some(arr) = p.get("casts").or_else(|| p.get("columns")).and_then(JsonValue::as_array) {
                for entry in arr {
                    if let Some(c) = entry.get("column").and_then(JsonValue::as_str) {
                        let c = c.trim();
                        if !c.is_empty() {
                            check(c)?;
                        }
                    }
                }
            }
            check_single_col(p)?;
        }
        "xf.distinct" | "xf.drop" | "xf.keep" | "xf.unpivot" | "xf.row_hash" => {
            check_list("columns")?;
        }
        "xf.project" => {
            check_list("columns")?;
            check_list("keep")?;
        }
        "xf.sort" => {
            // orderBy is either an array of column-name strings or
            // an array of {column, direction} objects. Validate both.
            if let Some(arr) = p.get("orderBy").and_then(JsonValue::as_array) {
                for entry in arr {
                    let c = entry
                        .as_str()
                        .map(|s| s.to_string())
                        .or_else(|| {
                            entry
                                .get("column")
                                .and_then(JsonValue::as_str)
                                .map(|s| s.to_string())
                        });
                    if let Some(c) = c {
                        let c = c.trim();
                        if !c.is_empty() {
                            check(c)?;
                        }
                    }
                }
            }
        }
        "xf.rename" => {
            // Validate the old (from) names against the upstream schema,
            // across every prop shape (renames/columns array OR mapping).
            for (from, _to) in rename_pairs(p) {
                let c = from.trim();
                if !c.is_empty() {
                    check(c)?;
                }
            }
        }
        "xf.aggregate" => {
            check_list("groupBy")?;
            // aggregateColumns: [{column, fn}, ...] - check the column field.
            if let Some(arr) = p.get("aggregateColumns").and_then(JsonValue::as_array) {
                for entry in arr {
                    if let Some(c) = entry.get("column").and_then(JsonValue::as_str) {
                        let c = c.trim();
                        if !c.is_empty() {
                            check(c)?;
                        }
                    }
                }
            }
        }
        "xf.pivot" => {
            check_single_col(p)?;
            for key in ["pivotColumn", "valueColumn", "valuesColumn"] {
                if let Some(c) = p.get(key).and_then(JsonValue::as_str) {
                    let c = c.trim();
                    if !c.is_empty() {
                        check(c)?;
                    }
                }
            }
            check_list("groupBy")?;
        }
        "xf.url.parse" | "xf.ip.parse" => {
            check_single_col(p)?;
        }
        "xf.cdc.scd1" | "xf.cdc.scd2" | "xf.cdc.compare" => {
            check_list("naturalKey")?;
            check_list("compareColumns")?;
        }
        // Window family: partitionBy + orderBy are upstream columns.
        // `column` is the column the function operates on (lead/lag/
        // first/last) - present on a subset.
        "xf.window"
        | "xf.rownum"
        | "xf.rank"
        | "xf.denserank"
        | "xf.lead"
        | "xf.lag"
        | "xf.first"
        | "xf.last"
        | "xf.ntile"
        | "xf.rank.filter"
        | "xf.cumulative"
        | "xf.aggwin" => {
            check_list("partitionBy")?;
            check_list("orderBy")?;
            check_single_col(p)?;
        }
        // Join keys on the left side. Right-side keys reference the
        // lookup input, whose columns we don't currently propagate
        // through the planner; skip those rather than emit a false
        // positive.
        "xf.join"
        | "xf.join.left"
        | "xf.join.right"
        | "xf.join.full"
        | "xf.join.cross"
        | "xf.semi"
        | "xf.anti" => {
            if let Some(s) = p.get("leftKey").and_then(JsonValue::as_str) {
                for k in s.split(',') {
                    let k = k.trim();
                    if !k.is_empty() {
                        check(k)?;
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct NodeInputs {
    /// canonical port -> ordered list of upstream node ids.
    pub(crate) ports: BTreeMap<String, Vec<String>>,
}

impl NodeInputs {
    pub(crate) fn main(&self) -> Option<&str> {
        self.ports.get("main").and_then(|v| v.first()).map(|s| s.as_str())
    }

    /// Inputs across the `main` and `main_N` ports (used by set ops,
    /// whose handles are main_1 / main_2 / main_3).
    pub(crate) fn all_main_ports(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for (key, refs) in &self.ports {
            if key == "main" || key.starts_with("main_") {
                out.extend(refs.iter().map(|s| s.as_str()));
            }
        }
        out
    }

    #[allow(dead_code)]
    pub(crate) fn lookup(&self, idx: usize) -> Option<&str> {
        let key = if idx == 0 {
            "lookup".to_string()
        } else {
            format!("lookup_{}", idx + 1)
        };
        self.ports.get(&key).and_then(|v| v.first()).map(|s| s.as_str())
    }

    pub(crate) fn first_lookup(&self) -> Option<&str> {
        for (k, v) in &self.ports {
            if k.starts_with("lookup") {
                if let Some(first) = v.first() {
                    return Some(first.as_str());
                }
            }
        }
        None
    }
}

/// Suffix for a node's secondary "reject" output table.
pub(crate) const REJECT_SUFFIX: &str = "__reject";

/// Which materialized table an edge reads, based on the source node's
/// OUTPUT handle. Reject/filter outputs read the node's `__reject`
/// table; everything else reads its main table.
pub(crate) fn output_table_ref(source_id: &str, source_handle: Option<&str>) -> String {
    match source_handle.map(canonical_port) {
        Some("reject") | Some("filter") => format!("{}{}", source_id, REJECT_SUFFIX),
        // Switch / conditional split: each case + default port reads
        // from its own `<node>__<handle>` table that build_switch
        // materializes.
        Some(h) if h.starts_with("case_") || h == "default" => {
            format!("{}__{}", source_id, h)
        }
        _ => source_id.to_string(),
    }
}

/// SQL for a `ctl.*` node that exposes its single upstream unchanged under
/// its own name (wait, throttle, barrier, checkpoint, runpipeline, iterate,
/// try, trigger, foreach, gate, ...). Their real effect (delay, sub-pipeline
/// run, assertion, durable copy) happens in the Rust executor; the SQL is
/// purely a rename. A VIEW is correct and far cheaper than a TABLE: DuckDB
/// inlines it into whatever reads it, with no row copy. The old
/// `CREATE TABLE x AS SELECT * FROM upstream` copied every upstream row to
/// disk for nothing - ~12s for a 10M-row dataset flowing through a single
/// control node, versus ~5ms as a view.
pub(crate) fn passthrough_view_sql(node_id: &str, upstream: &str) -> String {
    format!(
        "CREATE OR REPLACE VIEW {} AS SELECT * FROM {}",
        quote_ident(node_id),
        quote_ident(upstream)
    )
}

/// Empty-result placeholder for a control node used as a pure driver with
/// no upstream (e.g. `ctl.iterate` running a sub-pipeline N times, or a
/// `ctl.trigger` with nothing wired in). A view over a constant-false
/// select is enough; nothing reads its rows.
pub(crate) fn passthrough_placeholder_sql(node_id: &str, marker: &str) -> String {
    format!(
        "CREATE OR REPLACE VIEW {} AS SELECT '{}' AS status WHERE 1=0",
        quote_ident(node_id),
        marker.replace('\'', "''")
    )
}

/// Extract the independent downstream branches of a ctl.parallelize node into
/// self-contained sub-pipeline docs (one per output branch) for concurrent
/// execution, plus the set of branch node ids to exclude from the main plan.
///
/// Each branch sub-doc gets an injected `src.parquet` source whose id equals
/// the parallelize node's id (so the branch's edges from it resolve) reading a
/// `${__PSNAP__}` snapshot placeholder the executor fills at run time. Branches
/// must be independent: a branch node may not also be fed from outside the
/// branches, nor feed a node outside them (output composition isn't supported).
pub(crate) fn build_parallelize_branches(
    p_node: &PipelineNode,
    nodes: &[PipelineNode],
    data_edges: &[&PipelineEdge],
) -> Result<(ParallelizeSpec, Vec<String>), EngineError> {
    let p_id = p_node.id.as_str();
    // Branch roots: every node directly downstream of the parallelize node.
    // Branches are defined by their INDEPENDENT downstream subgraphs, not by
    // which output handle the edge leaves from. The canvas may serialize all
    // fan-out edges on the same "main" handle (handles are a UI affordance),
    // but each independent downstream chain is still its own concurrent
    // branch - so we group by reachability, never by handle id.
    let mut roots: Vec<String> = Vec::new();
    {
        let mut seen_root: HashSet<&str> = HashSet::new();
        for e in data_edges {
            if e.source == p_id && e.target != p_id && seen_root.insert(e.target.as_str()) {
                roots.push(e.target.clone());
            }
        }
    }
    if roots.is_empty() {
        return Err(EngineError::Config(format!(
            "ctl.parallelize ({}): wire at least one branch to an output before running",
            p_id
        )));
    }

    let node_by_id: HashMap<&str, &PipelineNode> =
        nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let mut downstream: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in data_edges {
        downstream
            .entry(e.source.as_str())
            .or_default()
            .push(e.target.as_str());
    }

    let mut all_branch: HashSet<String> = HashSet::new();
    let mut branches: Vec<String> = Vec::new();

    for root in &roots {
        // A root already pulled into an earlier branch's subgraph (two outputs
        // feeding the same chain) belongs to that branch, not a new one.
        if all_branch.contains(root.as_str()) {
            continue;
        }
        // BFS the independent downstream subgraph rooted here.
        let mut order: Vec<String> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        let mut queue: Vec<&str> = vec![root.as_str()];
        while let Some(n) = queue.pop() {
            if n == p_id || !seen.insert(n) {
                continue;
            }
            if all_branch.contains(n) {
                return Err(EngineError::Config(format!(
                    "ctl.parallelize ({}): node '{}' is reachable from more than one branch; branches must be independent",
                    p_id, n
                )));
            }
            order.push(n.to_string());
            if let Some(succ) = downstream.get(n) {
                for &t in succ {
                    if t != p_id {
                        queue.push(t);
                    }
                }
            }
        }
        let branch_set: HashSet<&str> = order.iter().map(|s| s.as_str()).collect();
        // Sub-doc nodes: injected snapshot source (id = parallelize id) + branch nodes.
        let mut sub_nodes: Vec<serde_json::Value> = vec![serde_json::json!({
            "id": p_id,
            "position": { "x": 0, "y": 0 },
            "data": {
                "label": "snapshot",
                "componentId": "src.parquet",
                "properties": { "path": "${__PSNAP__}" }
            }
        })];
        for n in &order {
            let node = node_by_id.get(n.as_str()).ok_or_else(|| {
                EngineError::Config(format!("ctl.parallelize ({}): unknown branch node '{}'", p_id, n))
            })?;
            sub_nodes.push(
                serde_json::to_value(node)
                    .map_err(|e| EngineError::Config(format!("ctl.parallelize: serialize node: {}", e)))?,
            );
        }
        let mut sub_edges: Vec<serde_json::Value> = Vec::new();
        for e in data_edges {
            let src_in = e.source == p_id || branch_set.contains(e.source.as_str());
            if src_in && branch_set.contains(e.target.as_str()) {
                sub_edges.push(
                    serde_json::to_value(e)
                        .map_err(|e| EngineError::Config(format!("ctl.parallelize: serialize edge: {}", e)))?,
                );
            }
        }
        let sub_doc = serde_json::json!({ "nodes": sub_nodes, "edges": sub_edges });
        branches.push(
            serde_json::to_string(&sub_doc)
                .map_err(|e| EngineError::Config(format!("ctl.parallelize: serialize branch: {}", e)))?,
        );
        for n in order {
            all_branch.insert(n);
        }
    }

    // Independence checks: no branch node may exchange data with a node outside
    // the branches (that would require composing branch output back).
    for e in data_edges {
        if all_branch.contains(e.target.as_str())
            && e.source != p_id
            && !all_branch.contains(e.source.as_str())
        {
            return Err(EngineError::Config(format!(
                "ctl.parallelize ({}): branch node '{}' also receives data from '{}' outside the parallel branches; branches must be independent",
                p_id, e.target, e.source
            )));
        }
        if all_branch.contains(e.source.as_str()) && !all_branch.contains(e.target.as_str()) {
            return Err(EngineError::Config(format!(
                "ctl.parallelize ({}): branch node '{}' feeds '{}' outside the parallel branches; branch outputs can't be composed back (end each branch in its own sink)",
                p_id, e.source, e.target
            )));
        }
    }

    let max_concurrency = p_node
        .data
        .properties
        .as_ref()
        .and_then(|p| p.get("maxConcurrency"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    Ok((
        ParallelizeSpec {
            branches,
            max_concurrency,
        },
        all_branch.into_iter().collect(),
    ))
}

pub(crate) fn canonical_port(p: &str) -> &str {
    // Collapse port handle ids to canonical names. The frontend uses
    // 'main', 'lookup_1', 'lookup_2', 'lookup_3', 'reject', 'filter',
    // 'iterate'. Triggers don't carry data so we never see them here.
    if p.is_empty() {
        return "main";
    }
    p
}

/// Components that legitimately accept more than one edge on the `main`
/// port (they read every upstream via all_main_ports, not just the
/// first). Everything else is single-input and must reject fan-in.
pub(crate) fn is_multi_main_component(component_id: &str) -> bool {
    matches!(
        component_id,
        "xf.union" | "xf.unionall" | "xf.intersect" | "xf.except"
    )
}

pub(crate) fn is_data_edge(edge: &PipelineEdge) -> bool {
    match edge.data.as_ref() {
        Some(d) => matches!(
            d.connection_type.as_str(),
            "main" | "lookup" | "reject" | "filter"
        ),
        None => true,
    }
}

pub(crate) fn topological_sort(
    nodes: &[PipelineNode],
    edges: &[&PipelineEdge],
) -> Result<Vec<String>, EngineError> {
    let mut in_degree: HashMap<String, usize> =
        nodes.iter().map(|n| (n.id.clone(), 0_usize)).collect();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        if !in_degree.contains_key(&edge.source) || !in_degree.contains_key(&edge.target) {
            continue;
        }
        adjacency
            .entry(edge.source.clone())
            .or_default()
            .push(edge.target.clone());
        *in_degree.entry(edge.target.clone()).or_insert(0) += 1;
    }
    let mut queue: Vec<String> = in_degree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(k, _)| k.clone())
        .collect();
    // Stabilize order so generated SQL is reproducible.
    queue.sort();
    let mut order = Vec::with_capacity(nodes.len());
    while let Some(id) = queue.pop() {
        order.push(id.clone());
        if let Some(children) = adjacency.get(&id) {
            for child in children {
                let entry = in_degree.entry(child.clone()).or_insert(0);
                if *entry > 0 {
                    *entry -= 1;
                    if *entry == 0 {
                        queue.push(child.clone());
                        queue.sort();
                    }
                }
            }
        }
    }
    if order.len() != nodes.len() {
        return Err(EngineError::Config(
            "Pipeline contains a cycle in the data-flow edges".into(),
        ));
    }
    Ok(order)
}
