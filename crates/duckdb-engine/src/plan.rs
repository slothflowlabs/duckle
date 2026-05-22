//! Pipeline → DuckDB SQL compiler.
//!
//! Lowers a Duckle pipeline document (the same JSON the frontend
//! produces) into an ordered list of SQL statements. Each non-sink node
//! becomes a `CREATE OR REPLACE TEMP VIEW "<node_id>" AS (...)` so
//! downstream nodes can reference it by name. Sinks become standalone
//! `COPY (...) TO '...' (FORMAT ...)` statements.

use crate::sql_escape;
use crate::EngineError;
use duckle_metadata::{PipelineEdge, PipelineNode};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Pipeline payload sent from the frontend. Just the nodes + edges
/// directly — no wrapping metadata required for a run.
#[derive(Debug, Deserialize)]
pub struct PipelineDoc {
    pub nodes: Vec<PipelineNode>,
    #[serde(default)]
    pub edges: Vec<PipelineEdge>,
}

#[derive(Debug)]
pub struct Stage {
    pub node_id: String,
    pub component_id: String,
    pub label: String,
    pub sql: String,
    pub kind: StageKind,
    /// For sinks: the upstream object name they read from, so the
    /// executor can report a row count.
    pub from: Option<String>,
    /// For sinks: the output path + write mode, so the executor can
    /// enforce "error if exists" before writing.
    pub sink_path: Option<String>,
    pub sink_mode: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StageKind {
    /// Non-sink node — emitted as a `CREATE OR REPLACE TEMP VIEW`.
    View,
    /// Sink — emitted as a `COPY (...) TO '...' (FORMAT ...)`.
    Sink,
}

#[derive(Debug)]
pub struct CompiledPipeline {
    pub stages: Vec<Stage>,
    /// Node IDs that have no downstream consumer — used to fetch
    /// preview rows when there's no sink.
    pub leaves: Vec<String>,
}

/// Compile only the subgraph upstream of (and including) `target_id`.
/// Sinks downstream of the target are dropped — the target becomes the
/// new "leaf" whose preview the caller can fetch. Used by the
/// "Run from here" right-click action.
pub fn compile_partial(
    pipeline: &PipelineDoc,
    target_id: &str,
) -> Result<CompiledPipeline, EngineError> {
    // Make sure the target actually exists.
    if !pipeline.nodes.iter().any(|n| n.id == target_id) {
        return Err(EngineError::Config(format!(
            "Target node '{}' not found",
            target_id
        )));
    }
    // BFS backwards from target along data edges.
    let mut keep: std::collections::HashSet<String> = std::collections::HashSet::new();
    keep.insert(target_id.to_string());
    let mut frontier = vec![target_id.to_string()];
    while let Some(id) = frontier.pop() {
        for edge in pipeline.edges.iter().filter(|e| is_data_edge(e) && e.target == id) {
            if keep.insert(edge.source.clone()) {
                frontier.push(edge.source.clone());
            }
        }
    }
    let filtered = PipelineDoc {
        nodes: pipeline
            .nodes
            .iter()
            .filter(|n| keep.contains(&n.id))
            .cloned()
            .collect(),
        edges: pipeline
            .edges
            .iter()
            .filter(|e| keep.contains(&e.source) && keep.contains(&e.target))
            .cloned()
            .collect(),
    };
    compile(&filtered)
}

pub fn compile(pipeline: &PipelineDoc) -> Result<CompiledPipeline, EngineError> {
    let node_index: HashMap<&str, &PipelineNode> = pipeline
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    let data_edges: Vec<&PipelineEdge> = pipeline
        .edges
        .iter()
        .filter(|e| is_data_edge(e))
        .collect();

    let order = topological_sort(&pipeline.nodes, &data_edges)?;

    // Build inputs map: node_id -> port_id -> Vec<source_node_id>
    let mut inputs: HashMap<&str, NodeInputs> = HashMap::new();
    for edge in &data_edges {
        let port = edge
            .target_handle
            .as_deref()
            .unwrap_or("main");
        let port_key = canonical_port(port);
        // Resolve which materialized table this edge actually reads, based
        // on the SOURCE node's output handle (main vs reject).
        let source_ref = output_table_ref(&edge.source, edge.source_handle.as_deref());
        inputs
            .entry(edge.target.as_str())
            .or_default()
            .ports
            .entry(port_key.to_string())
            .or_default()
            .push(source_ref);
    }

    let mut stages = Vec::with_capacity(order.len());
    for node_id in &order {
        let node = node_index
            .get(node_id.as_str())
            .ok_or_else(|| EngineError::Config(format!("Unknown node: {}", node_id)))?;
        let component_id = node
            .data
            .component_id
            .as_deref()
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "Node '{}' has no componentId; can't execute",
                    node_id
                ))
            })?;
        if node.data.disabled.unwrap_or(false) {
            continue;
        }
        let empty = NodeInputs::default();
        let node_inputs = inputs.get(node_id.as_str()).unwrap_or(&empty);
        let stage = build_stage(node, component_id, node_inputs)?;
        stages.push(stage);
    }

    // Leaves = data-flow nodes that nothing else consumes from
    let has_downstream: HashSet<&str> = data_edges.iter().map(|e| e.source.as_str()).collect();
    let leaves: Vec<String> = order
        .iter()
        .filter(|id| !has_downstream.contains(id.as_str()))
        .cloned()
        .collect();

    Ok(CompiledPipeline { stages, leaves })
}

#[derive(Debug, Default)]
struct NodeInputs {
    /// canonical port -> ordered list of upstream node ids.
    ports: BTreeMap<String, Vec<String>>,
}

impl NodeInputs {
    fn main(&self) -> Option<&str> {
        self.ports.get("main").and_then(|v| v.first()).map(|s| s.as_str())
    }

    /// Inputs across the `main` and `main_N` ports (used by set ops,
    /// whose handles are main_1 / main_2 / main_3).
    fn all_main_ports(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for (key, refs) in &self.ports {
            if key == "main" || key.starts_with("main_") {
                out.extend(refs.iter().map(|s| s.as_str()));
            }
        }
        out
    }

    #[allow(dead_code)]
    fn lookup(&self, idx: usize) -> Option<&str> {
        let key = if idx == 0 {
            "lookup".to_string()
        } else {
            format!("lookup_{}", idx + 1)
        };
        self.ports.get(&key).and_then(|v| v.first()).map(|s| s.as_str())
    }

    fn first_lookup(&self) -> Option<&str> {
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
const REJECT_SUFFIX: &str = "__reject";

/// Which materialized table an edge reads, based on the source node's
/// OUTPUT handle. Reject/filter outputs read the node's `__reject`
/// table; everything else reads its main table.
fn output_table_ref(source_id: &str, source_handle: Option<&str>) -> String {
    match source_handle.map(canonical_port) {
        Some("reject") | Some("filter") => format!("{}{}", source_id, REJECT_SUFFIX),
        _ => source_id.to_string(),
    }
}

fn canonical_port(p: &str) -> &str {
    // Collapse port handle ids to canonical names. The frontend uses
    // 'main', 'lookup_1', 'lookup_2', 'lookup_3', 'reject', 'filter',
    // 'iterate'. Triggers don't carry data so we never see them here.
    if p.is_empty() {
        return "main";
    }
    p
}

fn is_data_edge(edge: &PipelineEdge) -> bool {
    match edge.data.as_ref() {
        Some(d) => matches!(
            d.connection_type.as_str(),
            "main" | "lookup" | "reject" | "filter"
        ),
        None => true,
    }
}

fn topological_sort(
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

fn build_stage(
    node: &PipelineNode,
    component_id: &str,
    inputs: &NodeInputs,
) -> Result<Stage, EngineError> {
    let props = node
        .data
        .properties
        .as_ref()
        .cloned()
        .unwrap_or(JsonValue::Null);
    let mut sink_path: Option<String> = None;
    let mut sink_mode: Option<String> = None;
    let (sql, kind, from) = if component_id.starts_with("snk.") {
        let from_view = inputs
            .main()
            .ok_or_else(|| missing_input(node, "main"))?;
        sink_path = string_prop(&props, "path").filter(|s| !s.is_empty());
        sink_mode = string_prop(&props, "mode").filter(|s| !s.is_empty());
        (
            build_sink_sql(component_id, &props, from_view)?,
            StageKind::Sink,
            Some(from_view.to_string()),
        )
    } else {
        let body = build_view_sql(component_id, &props, inputs).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })?;
        // Materialize as a real table so the result persists across the
        // separate CLI invocations the executor uses per stage.
        let mut sql = format!(
            "CREATE OR REPLACE TABLE {} AS {}",
            quote_ident(&node.id),
            body
        );
        // Components that split rows (filter, quality validators) also
        // materialize a `<node>__reject` table for their reject port.
        if let Some(reject_body) = build_reject_sql(component_id, &props, inputs).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })? {
            let reject_table = format!("{}{}", node.id, REJECT_SUFFIX);
            sql.push_str(&format!(
                "; CREATE OR REPLACE TABLE {} AS {}",
                quote_ident(&reject_table),
                reject_body
            ));
        }
        (sql, StageKind::View, None)
    };
    Ok(Stage {
        node_id: node.id.clone(),
        component_id: component_id.to_string(),
        label: node.data.label.clone(),
        sql,
        kind,
        from,
        sink_path,
        sink_mode,
    })
}

/// The `SELECT * FROM <reader>` SQL for a source format — used by the
/// engine's inspect path to DESCRIBE / sample without materializing.
pub fn source_select_for_format(format: &str, props: &JsonValue) -> Option<String> {
    Some(match format {
        "csv" => build_csv_source(props),
        "tsv" => build_tsv_source(props),
        "parquet" => build_parquet_source(props),
        "json" | "jsonl" | "ndjson" => build_json_source(props),
        "sqlite" => build_sqlite_source(props),
        "duckdb" => build_duckdb_source(props),
        "s3" | "gcs" | "azureblob" | "http" | "https" => build_cloud_source(props),
        _ => return None,
    })
}

fn missing_input(node: &PipelineNode, port: &str) -> EngineError {
    EngineError::Config(format!(
        "{} ({}) is missing its '{}' input",
        node.data.label, node.id, port
    ))
}

// ---- View SQL (sources + transforms) ------------------------------------

fn build_view_sql(
    component_id: &str,
    props: &JsonValue,
    inputs: &NodeInputs,
) -> Result<String, String> {
    match component_id {
        // Sources
        "src.csv" => Ok(build_csv_source(props)),
        "src.tsv" => Ok(build_tsv_source(props)),
        "src.parquet" => Ok(build_parquet_source(props)),
        "src.json" | "src.jsonl" => Ok(build_json_source(props)),
        "src.sqlite" => Ok(build_sqlite_source(props)),
        "src.duckdb" => Ok(build_duckdb_source(props)),
        "src.s3" | "src.gcs" | "src.azureblob" | "src.http" => {
            Ok(build_cloud_source(props))
        }
        // Pass-through transforms
        "xf.filter" => build_filter(inputs, props),
        // Log Rows — pass data through unchanged; its rows surface in the
        // Output / Preview so you can inspect mid-pipeline (like tLogRow).
        "xf.log" => build_passthrough_op(inputs, "SELECT *"),
        "xf.project" => build_project(inputs, props),
        "xf.distinct" => build_distinct(inputs, props),
        "xf.limit" => build_limit(inputs, props),
        "xf.sort" => build_sort(inputs, props),
        "xf.agg" | "xf.groupby" => build_aggregate(inputs, props, GroupMode::Plain),
        "xf.rollup" => build_aggregate(inputs, props, GroupMode::Rollup),
        "xf.cube" => build_aggregate(inputs, props, GroupMode::Cube),
        "xf.union" => build_union(inputs, true),
        "xf.unionall" => build_union(inputs, false),
        "xf.intersect" => build_setop(inputs, "INTERSECT"),
        "xf.except" => build_setop(inputs, "EXCEPT"),
        "xf.addcol" | "xf.coalesce" => build_addcol(inputs, props),
        "xf.rownum" | "xf.rank" | "xf.denserank" | "xf.lead" | "xf.lag" | "xf.first"
        | "xf.last" | "xf.ntile" => build_window(inputs, props, component_id),
        "xf.pivot" => build_pivot(inputs, props),
        // Data-quality validators — the PASS rows. Failures go to the
        // node's __reject table (see build_reject_sql).
        "qa.notnull" | "qa.range" | "qa.regex" | "qa.unique" => {
            build_quality(inputs, props, component_id, false)
        }
        "xf.reorder" => build_reorder(inputs, props),
        "xf.count" => build_count(inputs),
        "xf.join.cross" => build_cross_join(inputs),
        "xf.regex" | "xf.trim" | "xf.case" | "xf.length" | "xf.substring" | "xf.concat"
        | "xf.split" | "xf.format" => build_string(inputs, props, component_id),
        "xf.num.round" | "xf.num.abs" | "xf.num.mod" | "xf.num.power" | "xf.num.sqrt"
        | "xf.num.log" => build_numeric(inputs, props, component_id),
        "xf.dt.parse" | "xf.dt.format" | "xf.dt.extract" | "xf.dt.trunc" | "xf.dt.tz" => {
            build_datetime(inputs, props, component_id)
        }
        "xf.dt.add" => build_date_add(inputs, props),
        "xf.dt.diff" => build_date_diff(inputs, props),
        "xf.json.parse" | "xf.json.stringify" | "xf.json.path" => {
            build_json(inputs, props, component_id)
        }
        "xf.json.flatten" => build_json_flatten(inputs, props),
        "xf.json.merge" => build_json_merge(inputs, props),
        "xf.arr.element" | "xf.arr.distinct" | "xf.arr.explode" => {
            build_array(inputs, props, component_id)
        }
        "xf.arr.collect" => build_arr_collect(inputs, props),
        "xf.arr.contains" => build_arr_contains(inputs, props),
        "xf.cast" => build_cast(inputs, props),
        "xf.rename" => build_rename(inputs, props),
        "xf.drop" | "xf.dropcol" => build_drop(inputs, props),
        "xf.map" => build_mapper(inputs, props),
        "xf.join.inner" | "xf.join" => build_join(inputs, props, "INNER"),
        "xf.join.left" => build_join(inputs, props, "LEFT"),
        "xf.join.right" => build_join(inputs, props, "RIGHT"),
        "xf.join.full" | "xf.join.outer" => build_join(inputs, props, "FULL OUTER"),
        "xf.lookup" | "xf.lookup.outer" => build_join(inputs, props, "LEFT"),
        "xf.semi" | "xf.semi.join" => build_semi(inputs, props, false),
        "xf.anti" | "xf.anti.join" => build_semi(inputs, props, true),
        "xf.topn" => build_take(inputs, props, TakeKind::Limit),
        "xf.skip" => build_take(inputs, props, TakeKind::Offset),
        "xf.sample" => build_take(inputs, props, TakeKind::Sample),
        // Custom SQL — runs the user's SELECT as a real stage, with the
        // upstream exposed as `input`. Makes SQL routines executable too.
        "code.sql" | "code.sqltemplate" => build_custom_sql(inputs, props),
        // Control-flow nodes don't transform data — pass it through.
        other if other.starts_with("ctl.") => {
            let upstream = inputs.main().ok_or_else(|| missing_input_msg(other))?;
            Ok(format!("SELECT * FROM {}", quote_ident(upstream)))
        }
        // Everything else isn't executable yet. Fail loudly rather than
        // silently passing data through unchanged (which would look like
        // success while doing nothing).
        other => Err(format!(
            "'{}' isn't executable on the DuckDB engine yet — it's a preview component.",
            other
        )),
    }
}

fn build_passthrough_op(inputs: &NodeInputs, op: &str) -> Result<String, String> {
    let upstream = inputs
        .main()
        .ok_or_else(|| "missing main input".to_string())?;
    Ok(format!("{} FROM {}", op, quote_ident(upstream)))
}

fn build_filter(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    // The predicate is usually a structured object carrying compiled
    // `sql`; it may also be a raw string (legacy / raw-SQL mode).
    let predicate = filter_predicate_sql(props.get("predicate"))
        .or_else(|| {
            props
                .get("filterSql")
                .and_then(JsonValue::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    let predicate = predicate.trim();
    let predicate = if predicate.is_empty() { "TRUE" } else { predicate };
    Ok(format!(
        "SELECT * FROM {} WHERE {}",
        quote_ident(upstream),
        predicate
    ))
}

/// Extract the effective SQL from a filter predicate value, which may be
/// a plain string or the structured FilterPredicate object the visual
/// builder writes ({ mode, conditions, rawSql, sql }).
fn filter_predicate_sql(v: Option<&JsonValue>) -> Option<String> {
    match v {
        Some(JsonValue::String(s)) => Some(s.clone()),
        Some(JsonValue::Object(o)) => o
            .get("sql")
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                if o.get("mode").and_then(JsonValue::as_str) == Some("raw") {
                    o.get("rawSql").and_then(JsonValue::as_str).map(str::to_string)
                } else {
                    None
                }
            }),
        _ => None,
    }
}

fn build_project(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let columns = columns_from_props(props, "columns").or_else(|| columns_from_props(props, "keep"));
    let cols = match columns {
        Some(cs) if !cs.is_empty() => cs
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "*".to_string(),
    };
    Ok(format!("SELECT {} FROM {}", cols, quote_ident(upstream)))
}

fn build_drop(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let columns = columns_from_props(props, "columns")
        .or_else(|| columns_from_props(props, "drop"))
        .unwrap_or_default();
    if columns.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    let except_list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT * EXCLUDE ({}) FROM {}",
        except_list,
        quote_ident(upstream)
    ))
}

fn build_limit(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let limit = props
        .get("limit")
        .and_then(JsonValue::as_u64)
        .or_else(|| props.get("rows").and_then(JsonValue::as_u64))
        .unwrap_or(100);
    Ok(format!(
        "SELECT * FROM {} LIMIT {}",
        quote_ident(upstream),
        limit
    ))
}

enum TakeKind {
    Limit,
    Offset,
    Sample,
}

fn build_take(inputs: &NodeInputs, props: &JsonValue, kind: TakeKind) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let n = props
        .get("count")
        .and_then(JsonValue::as_u64)
        .or_else(|| props.get("limit").and_then(JsonValue::as_u64))
        .unwrap_or(100);
    let from = quote_ident(upstream);
    Ok(match kind {
        TakeKind::Limit => format!("SELECT * FROM {} LIMIT {}", from, n),
        TakeKind::Offset => format!("SELECT * FROM {} OFFSET {}", from, n),
        TakeKind::Sample => format!("SELECT * FROM {} USING SAMPLE {} ROWS", from, n),
    })
}

/// Custom SQL stage. The upstream table is exposed as a CTE named
/// `input`, so a node's SQL like `SELECT * FROM input WHERE x > 1`
/// just works. With no upstream, the SQL stands alone (e.g. a source
/// SELECT). build_stage wraps the result in CREATE OR REPLACE TABLE.
fn build_custom_sql(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let sql = string_prop(props, "sql")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Custom SQL is empty — write a SELECT or pick a SQL routine".to_string())?;
    Ok(match inputs.main() {
        Some(upstream) => {
            format!("WITH input AS (SELECT * FROM {}) {}", quote_ident(upstream), sql)
        }
        None => sql,
    })
}

fn build_distinct(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        Ok(format!("SELECT DISTINCT * FROM {}", quote_ident(upstream)))
    } else {
        let on = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        Ok(format!(
            "SELECT DISTINCT ON ({}) * FROM {}",
            on,
            quote_ident(upstream)
        ))
    }
}

fn build_sort(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let sort_keys: Vec<String> = props
        .get("orderBy")
        .and_then(JsonValue::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    if let Some(s) = v.as_str() {
                        Some(s.to_string())
                    } else if let Some(obj) = v.as_object() {
                        let col = obj.get("column").and_then(JsonValue::as_str)?;
                        let dir = obj
                            .get("direction")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("asc");
                        Some(format!("{} {}", quote_ident(col), dir.to_uppercase()))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let mut sort_keys = sort_keys;
    // The Sort form writes a single sortColumn + direction + nullsLast.
    if sort_keys.is_empty() {
        if let Some(col) = string_prop(props, "sortColumn").filter(|s| !s.is_empty()) {
            let dir = if string_prop(props, "direction").as_deref() == Some("desc") {
                "DESC"
            } else {
                "ASC"
            };
            let nulls = if props.get("nullsLast").and_then(JsonValue::as_bool).unwrap_or(true) {
                " NULLS LAST"
            } else {
                " NULLS FIRST"
            };
            sort_keys.push(format!("{} {}{}", quote_ident(&col), dir, nulls));
        }
    }
    if sort_keys.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT * FROM {} ORDER BY {}",
        quote_ident(upstream),
        sort_keys.join(", ")
    ))
}

enum GroupMode {
    Plain,
    Rollup,
    Cube,
}

fn build_aggregate(
    inputs: &NodeInputs,
    props: &JsonValue,
    mode: GroupMode,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    // The Group By form writes `groupKeys`; accept `groupBy` too.
    let group_by: Vec<String> = columns_from_props(props, "groupKeys")
        .or_else(|| columns_from_props(props, "groupBy"))
        .unwrap_or_default();
    let aggregations = props
        .get("aggregations")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let mut select_terms: Vec<String> = group_by.iter().map(|c| quote_ident(c)).collect();
    for agg in &aggregations {
        let column = agg.get("column").and_then(JsonValue::as_str).unwrap_or("*");
        // The UI's AggregationsField stores { column, func, output };
        // accept the function/alias spellings too for robustness.
        let func = agg
            .get("function")
            .or_else(|| agg.get("func"))
            .and_then(JsonValue::as_str)
            .unwrap_or("count")
            .to_uppercase();
        let alias = agg
            .get("alias")
            .or_else(|| agg.get("output"))
            .and_then(JsonValue::as_str)
            .map(String::from)
            .unwrap_or_else(|| format!("{}_{}", func.to_lowercase(), column.replace('*', "all")));
        let column_expr = if column == "*" {
            "*".to_string()
        } else {
            quote_ident(column)
        };
        let agg_expr = match func.as_str() {
            "COUNT_DISTINCT" => format!("COUNT(DISTINCT {})", column_expr),
            _ => format!("{}({})", func, column_expr),
        };
        select_terms.push(format!("{} AS {}", agg_expr, quote_ident(&alias)));
    }
    if select_terms.is_empty() {
        select_terms.push("COUNT(*) AS row_count".to_string());
    }
    let group_clause = if group_by.is_empty() {
        String::new()
    } else {
        let cols = group_by
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        match mode {
            GroupMode::Plain => format!(" GROUP BY {}", cols),
            GroupMode::Rollup => format!(" GROUP BY ROLLUP ({})", cols),
            GroupMode::Cube => format!(" GROUP BY CUBE ({})", cols),
        }
    };
    let having = string_prop(props, "havingClause")
        .or_else(|| string_prop(props, "having"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|h| format!(" HAVING {}", h))
        .unwrap_or_default();
    Ok(format!(
        "SELECT {} FROM {}{}{}",
        select_terms.join(", "),
        quote_ident(upstream),
        group_clause,
        having
    ))
}

fn interval_unit(unit: &str) -> &'static str {
    match unit.to_lowercase().as_str() {
        "year" | "years" => "YEAR",
        "quarter" | "quarters" => "QUARTER",
        "month" | "months" => "MONTH",
        "week" | "weeks" => "WEEK",
        "hour" | "hours" => "HOUR",
        "minute" | "minutes" => "MINUTE",
        "second" | "seconds" => "SECOND",
        _ => "DAY",
    }
}

fn build_date_add(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.add"))?;
    let column = require_column(props)?;
    let amount = props.get("amount").and_then(JsonValue::as_i64).unwrap_or(1);
    let unit = string_prop(props, "unit").unwrap_or_else(|| "day".into());
    // amount * INTERVAL 1 unit handles negatives cleanly.
    let expr = format!(
        "{} + ({} * INTERVAL 1 {})",
        quote_ident(&column),
        amount,
        interval_unit(&unit)
    );
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_date_diff(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.diff"))?;
    let start = string_prop(props, "startColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Date diff needs a start column".to_string())?;
    let end = string_prop(props, "endColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Date diff needs an end column".to_string())?;
    let unit = string_prop(props, "unit").unwrap_or_else(|| "day".into());
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "date_diff".into());
    Ok(format!(
        "SELECT *, date_diff('{}', {}, {}) AS {} FROM {}",
        sql_escape(&unit),
        quote_ident(&start),
        quote_ident(&end),
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

fn build_json_flatten(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.json.flatten"))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    // Expand a STRUCT column's fields to top-level columns.
    Ok(format!(
        "SELECT * EXCLUDE ({}), {}.* FROM {}",
        col,
        col,
        quote_ident(upstream)
    ))
}

fn build_json_merge(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.json.merge"))?;
    let a = require_column(props)?;
    let b = string_prop(props, "secondColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Merge needs a second column".to_string())?;
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "merged".into());
    Ok(format!(
        "SELECT *, json_merge_patch(CAST({} AS JSON), CAST({} AS JSON)) AS {} FROM {}",
        quote_ident(&a),
        quote_ident(&b),
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

fn build_arr_collect(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.arr.collect"))?;
    let value = string_prop(props, "valueColumn")
        .or_else(|| string_prop(props, "column"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Collect needs a value column".to_string())?;
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "items".into());
    let group = columns_list(props, "groupBy");
    if group.is_empty() {
        Ok(format!(
            "SELECT list({}) AS {} FROM {}",
            quote_ident(&value),
            quote_ident(&out),
            quote_ident(upstream)
        ))
    } else {
        let g = group.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        Ok(format!(
            "SELECT {}, list({}) AS {} FROM {} GROUP BY {}",
            g,
            quote_ident(&value),
            quote_ident(&out),
            quote_ident(upstream),
            g
        ))
    }
}

fn build_arr_contains(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.arr.contains"))?;
    let column = require_column(props)?;
    let value = string_prop(props, "value").unwrap_or_default();
    let lit = if value.trim().parse::<f64>().is_ok() {
        value.trim().to_string()
    } else {
        format!("'{}'", sql_escape(&value))
    };
    let expr = format!("list_contains({}, {})", quote_ident(&column), lit);
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_contains", column));
    Ok(format!(
        "SELECT *, {} AS {} FROM {}",
        expr,
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

fn build_union(inputs: &NodeInputs, distinct: bool) -> Result<String, String> {
    let mains = inputs.all_main_ports();
    if mains.is_empty() {
        return Err("Union needs at least one input".into());
    }
    let op = if distinct { " UNION " } else { " UNION ALL " };
    Ok(mains
        .iter()
        .map(|id| format!("SELECT * FROM {}", quote_ident(id)))
        .collect::<Vec<_>>()
        .join(op))
}

fn build_setop(inputs: &NodeInputs, op: &str) -> Result<String, String> {
    let mains = inputs.all_main_ports();
    if mains.len() < 2 {
        return Err(format!("{} needs two inputs", op));
    }
    let sep = format!(" {} ", op);
    Ok(mains
        .iter()
        .map(|id| format!("SELECT * FROM {}", quote_ident(id)))
        .collect::<Vec<_>>()
        .join(&sep))
}

fn build_window(
    inputs: &NodeInputs,
    props: &JsonValue,
    component_id: &str,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "window: missing main input".to_string())?;
    let func = string_prop(props, "function")
        .unwrap_or_else(|| component_id.rsplit('.').next().unwrap_or("rownum").to_string());
    let target = string_prop(props, "targetColumn").filter(|s| !s.is_empty());
    let offset = props.get("offset").and_then(JsonValue::as_u64).unwrap_or(1);
    let need_target = |f: &str| -> Result<String, String> {
        target
            .clone()
            .map(|c| quote_ident(&c))
            .ok_or_else(|| format!("Window function '{}' needs a target column", f))
    };
    let call = match func.as_str() {
        "rownum" => "ROW_NUMBER()".to_string(),
        "rank" => "RANK()".to_string(),
        "denserank" => "DENSE_RANK()".to_string(),
        "lead" => format!("LEAD({}, {})", need_target("lead")?, offset),
        "lag" => format!("LAG({}, {})", need_target("lag")?, offset),
        "first" => format!("FIRST_VALUE({})", need_target("first")?),
        "last" => format!("LAST_VALUE({})", need_target("last")?),
        "ntile" => format!("NTILE({})", offset.max(1)),
        other => return Err(format!("Unknown window function '{}'", other)),
    };
    let partition = columns_list(props, "partitionBy");
    let order = columns_list(props, "orderBy");
    let mut over = String::new();
    if !partition.is_empty() {
        over.push_str(&format!(
            "PARTITION BY {}",
            partition.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    if !order.is_empty() {
        if !over.is_empty() {
            over.push(' ');
        }
        over.push_str(&format!(
            "ORDER BY {}",
            order.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    let out_name = string_prop(props, "outputName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| func.clone());
    Ok(format!(
        "SELECT *, {} OVER ({}) AS {} FROM {}",
        call,
        over,
        quote_ident(&out_name),
        quote_ident(upstream)
    ))
}

fn build_pivot(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "pivot: missing main input".to_string())?;
    let pivot_col = string_prop(props, "pivotColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Pivot needs a pivot column".to_string())?;
    let value_col = string_prop(props, "valueColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Pivot needs a value column".to_string())?;
    let agg = string_prop(props, "aggregation").unwrap_or_else(|| "sum".into());
    let mut sql = format!(
        "PIVOT (SELECT * FROM {}) ON {} USING {}({})",
        quote_ident(upstream),
        quote_ident(&pivot_col),
        agg,
        quote_ident(&value_col)
    );
    let group = columns_list(props, "groupBy");
    if !group.is_empty() {
        sql.push_str(&format!(
            " GROUP BY {}",
            group.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(sql)
}

fn missing_input_msg(component: &str) -> String {
    format!("{} is missing its input connection", component)
}

/// Emit a per-row column expression: add it as `output` if given, else
/// replace the source column in place.
fn apply_col_expr(upstream: &str, column: &str, expr: String, output: Option<String>) -> String {
    match output.filter(|s| !s.trim().is_empty()) {
        Some(out) => format!(
            "SELECT *, {} AS {} FROM {}",
            expr,
            quote_ident(out.trim()),
            quote_ident(upstream)
        ),
        None => format!(
            "SELECT * REPLACE ({} AS {}) FROM {}",
            expr,
            quote_ident(column),
            quote_ident(upstream)
        ),
    }
}

fn require_column(props: &JsonValue) -> Result<String, String> {
    string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "This transform needs a column".to_string())
}

fn build_string(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let pattern = string_prop(props, "pattern").unwrap_or_default();
    let replacement = string_prop(props, "replacement").unwrap_or_default();
    let expr = match component_id {
        "xf.regex" => format!(
            "regexp_replace(CAST({} AS VARCHAR), '{}', '{}', 'g')",
            col,
            sql_escape(&pattern),
            sql_escape(&replacement)
        ),
        "xf.trim" => format!("trim(CAST({} AS VARCHAR))", col),
        "xf.case" => match pattern.to_lowercase().as_str() {
            "lower" => format!("lower(CAST({} AS VARCHAR))", col),
            "title" | "initcap" | "proper" => format!("initcap(CAST({} AS VARCHAR))", col),
            _ => format!("upper(CAST({} AS VARCHAR))", col),
        },
        "xf.length" => format!("length(CAST({} AS VARCHAR))", col),
        "xf.substring" => {
            let start = pattern.trim().parse::<i64>().unwrap_or(1).max(1);
            match replacement.trim().parse::<i64>() {
                Ok(len) => format!("substring(CAST({} AS VARCHAR), {}, {})", col, start, len),
                Err(_) => format!("substring(CAST({} AS VARCHAR), {})", col, start),
            }
        }
        "xf.concat" => format!("concat(CAST({} AS VARCHAR), '{}')", col, sql_escape(&pattern)),
        "xf.split" => format!("string_split(CAST({} AS VARCHAR), '{}')", col, sql_escape(&pattern)),
        "xf.format" => format!("printf('{}', {})", sql_escape(&pattern), col),
        other => return Err(format!("String op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_numeric(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let arg = num_prop(props, "argument");
    let expr = match component_id {
        "xf.num.round" => format!("round({}, {})", col, arg.unwrap_or_else(|| "0".into())),
        "xf.num.abs" => format!("abs({})", col),
        "xf.num.mod" => format!("{} % {}", col, arg.ok_or("Modulo needs a divisor argument")?),
        "xf.num.power" => format!("power({}, {})", col, arg.unwrap_or_else(|| "2".into())),
        "xf.num.sqrt" => format!("sqrt({})", col),
        "xf.num.log" => match arg {
            Some(base) => format!("log({}, {})", base, col),
            None => format!("ln({})", col),
        },
        other => return Err(format!("Numeric op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_datetime(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let fmt = string_prop(props, "format").unwrap_or_else(|| "%Y-%m-%d".into());
    let unit = string_prop(props, "unit").unwrap_or_else(|| "day".into());
    let tz = string_prop(props, "timezone").unwrap_or_default();
    let expr = match component_id {
        "xf.dt.parse" => format!("strptime(CAST({} AS VARCHAR), '{}')", col, sql_escape(&fmt)),
        "xf.dt.format" => format!("strftime({}, '{}')", col, sql_escape(&fmt)),
        "xf.dt.extract" => format!("date_part('{}', {})", sql_escape(&unit), col),
        "xf.dt.trunc" => format!("date_trunc('{}', {})", sql_escape(&unit), col),
        "xf.dt.tz" => {
            if tz.is_empty() {
                return Err("Timezone convert needs a timezone".into());
            }
            format!("{} AT TIME ZONE '{}'", col, sql_escape(&tz))
        }
        other => return Err(format!("Date/time op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_json(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let path = string_prop(props, "path").unwrap_or_default();
    let expr = match component_id {
        "xf.json.parse" => format!("CAST({} AS JSON)", col),
        "xf.json.stringify" => format!("CAST({} AS VARCHAR)", col),
        "xf.json.path" => {
            if path.is_empty() {
                return Err("JSONPath extract needs a path".into());
            }
            format!("json_extract({}, '{}')", col, sql_escape(&path))
        }
        other => return Err(format!("JSON op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_array(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    if component_id == "xf.arr.explode" {
        // One row per element, keeping the other columns.
        return Ok(format!(
            "SELECT unnest({}) AS {}, * EXCLUDE ({}) FROM {}",
            col,
            col,
            col,
            quote_ident(upstream)
        ));
    }
    let expr = match component_id {
        "xf.arr.element" => {
            let idx = props.get("index").and_then(JsonValue::as_i64).unwrap_or(1);
            format!("{}[{}]", col, idx)
        }
        "xf.arr.distinct" => format!("list_distinct({})", col),
        other => return Err(format!("Array op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_reorder(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.reorder"))?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    let listed = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    // Listed columns first, everything else after — never drops a column.
    Ok(format!(
        "SELECT {}, * EXCLUDE ({}) FROM {}",
        listed,
        listed,
        quote_ident(upstream)
    ))
}

fn build_count(inputs: &NodeInputs) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.count"))?;
    Ok(format!("SELECT count(*) AS row_count FROM {}", quote_ident(upstream)))
}

fn build_cross_join(inputs: &NodeInputs) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "Cross join needs a main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "Cross join needs a lookup input".to_string())?;
    Ok(format!(
        "SELECT * FROM {} CROSS JOIN {}",
        quote_ident(left),
        quote_ident(right)
    ))
}

/// Data-quality validators. `reject = false` yields the passing rows;
/// `reject = true` yields the failing rows for the node's reject port.
fn build_quality(
    inputs: &NodeInputs,
    props: &JsonValue,
    component_id: &str,
    reject: bool,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "validator: missing main input".to_string())?;
    let from = quote_ident(upstream);
    if component_id == "qa.unique" {
        let keys = columns_list(props, "columns");
        if keys.is_empty() {
            return Err("Uniqueness check needs key columns".into());
        }
        let partition = keys.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let cmp = if reject { ">" } else { "=" };
        return Ok(format!(
            "SELECT * EXCLUDE (__dq_rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {}) AS __dq_rn FROM {}) WHERE __dq_rn {} 1",
            partition, from, cmp
        ));
    }
    let predicate = quality_pass_predicate(component_id, props)?;
    Ok(if reject {
        format!("SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)", from, predicate)
    } else {
        format!("SELECT * FROM {} WHERE COALESCE(({}), FALSE)", from, predicate)
    })
}

fn quality_pass_predicate(component_id: &str, props: &JsonValue) -> Result<String, String> {
    match component_id {
        "qa.notnull" => {
            let cols = columns_list(props, "columns");
            if cols.is_empty() {
                return Ok("TRUE".into());
            }
            Ok(cols
                .iter()
                .map(|c| format!("{} IS NOT NULL", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(" AND "))
        }
        "qa.range" => {
            let col = string_prop(props, "column")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Range check needs a column".to_string())?;
            let c = quote_ident(&col);
            let inclusive = props.get("inclusive").and_then(JsonValue::as_bool).unwrap_or(true);
            let (ge, le) = if inclusive { (">=", "<=") } else { (">", "<") };
            let mut parts = Vec::new();
            if let Some(min) = num_prop(props, "min") {
                parts.push(format!("{} {} {}", c, ge, min));
            }
            if let Some(max) = num_prop(props, "max") {
                parts.push(format!("{} {} {}", c, le, max));
            }
            Ok(if parts.is_empty() { "TRUE".into() } else { parts.join(" AND ") })
        }
        "qa.regex" => {
            let col = string_prop(props, "column")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Regex check needs a column".to_string())?;
            let pat = string_prop(props, "pattern")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Regex check needs a pattern".to_string())?;
            Ok(format!(
                "regexp_full_match(CAST({} AS VARCHAR), '{}')",
                quote_ident(&col),
                sql_escape(&pat)
            ))
        }
        other => Err(format!("Validator '{}' is not yet implemented", other)),
    }
}

/// Reject-port SQL for components that split rows. None = no reject table.
fn build_reject_sql(
    component_id: &str,
    props: &JsonValue,
    inputs: &NodeInputs,
) -> Result<Option<String>, String> {
    match component_id {
        "xf.filter" => {
            let upstream = inputs.main().ok_or_else(|| "filter: missing main input".to_string())?;
            let predicate = filter_predicate_sql(props.get("predicate")).unwrap_or_default();
            let predicate = predicate.trim();
            let predicate = if predicate.is_empty() { "TRUE" } else { predicate };
            Ok(Some(format!(
                "SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)",
                quote_ident(upstream),
                predicate
            )))
        }
        "qa.notnull" | "qa.range" | "qa.regex" | "qa.unique" => {
            Ok(Some(build_quality(inputs, props, component_id, true)?))
        }
        _ => Ok(None),
    }
}

fn columns_list(props: &JsonValue, key: &str) -> Vec<String> {
    props
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

/// A numeric property as a SQL literal — only if it's actually numeric,
/// so it can't smuggle arbitrary SQL into a comparison.
fn num_prop(props: &JsonValue, key: &str) -> Option<String> {
    match props.get(key) {
        Some(JsonValue::Number(n)) => Some(n.to_string()),
        Some(JsonValue::String(s)) => {
            let t = s.trim();
            t.parse::<f64>().ok().map(|_| t.to_string())
        }
        _ => None,
    }
}

fn build_addcol(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let columns = props
        .get("columns")
        .or_else(|| props.get("additions"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let mut additions: Vec<String> = Vec::new();
    for col in &columns {
        let name = col.get("name").and_then(JsonValue::as_str).unwrap_or("col");
        let expr = col
            .get("expression")
            .or_else(|| col.get("expr"))
            .and_then(JsonValue::as_str)
            .unwrap_or("NULL");
        additions.push(format!("{} AS {}", expr, quote_ident(name)));
    }
    // The Add-Column / Coalesce form is single: { name, expression }.
    if additions.is_empty() {
        let name = string_prop(props, "name").filter(|s| !s.is_empty());
        let expr = string_prop(props, "expression").or_else(|| string_prop(props, "expr"));
        if let (Some(name), Some(expr)) = (name, expr) {
            if !expr.trim().is_empty() {
                additions.push(format!("{} AS {}", expr, quote_ident(&name)));
            }
        }
    }
    if additions.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT *, {} FROM {}",
        additions.join(", "),
        quote_ident(upstream)
    ))
}

fn build_cast(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let casts = props
        .get("casts")
        .or_else(|| props.get("columns"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    // Use REPLACE so we keep other columns. e.g.
    //   SELECT * REPLACE (CAST(amount AS DECIMAL(10,2)) AS amount) FROM x
    let mut replacements: Vec<String> = Vec::new();
    for c in &casts {
        let column = c.get("column").and_then(JsonValue::as_str).unwrap_or("");
        let target = c
            .get("targetType")
            .or_else(|| c.get("type"))
            .and_then(JsonValue::as_str)
            .unwrap_or("VARCHAR");
        if column.is_empty() {
            continue;
        }
        let target_sql = duckle_type_to_duckdb(target);
        replacements.push(format!(
            "CAST({} AS {}) AS {}",
            quote_ident(column),
            target_sql,
            quote_ident(column)
        ));
    }
    // The Cast form is single-column: { column, targetType }.
    if replacements.is_empty() {
        if let Some(column) = string_prop(props, "column").filter(|s| !s.is_empty()) {
            let target = string_prop(props, "targetType")
                .or_else(|| string_prop(props, "type"))
                .unwrap_or_else(|| "string".into());
            replacements.push(format!(
                "CAST({} AS {}) AS {}",
                quote_ident(&column),
                duckle_type_to_duckdb(&target),
                quote_ident(&column)
            ));
        }
    }
    if replacements.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT * REPLACE ({}) FROM {}",
        replacements.join(", "),
        quote_ident(upstream)
    ))
}

fn build_rename(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let renames = props
        .get("renames")
        .or_else(|| props.get("columns"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    // RENAME via SELECT * REPLACE — keeps unrelated columns intact.
    // DuckDB doesn't support * REPLACE for renames directly; we use
    // SELECT *, col AS new_col then DROP not possible without listing.
    // Cleanest: enumerate explicit aliases. Need to know all columns.
    // For now, emit a CTE that selects everything then renames each
    // listed pair using a fresh wrapper.
    //   SELECT x.* EXCLUDE (a,b), x.a AS new_a, x.b AS new_b FROM up x
    let mut excludes = Vec::new();
    let mut aliases = Vec::new();
    for r in &renames {
        let from = r
            .get("from")
            .or_else(|| r.get("source"))
            .and_then(JsonValue::as_str);
        let to = r
            .get("to")
            .or_else(|| r.get("target"))
            .and_then(JsonValue::as_str);
        if let (Some(from), Some(to)) = (from, to) {
            excludes.push(quote_ident(from));
            aliases.push(format!(
                "{}.{} AS {}",
                quote_ident(upstream),
                quote_ident(from),
                quote_ident(to)
            ));
        }
    }
    // The Rename form writes `mapping` as key-value pairs: old -> new.
    if aliases.is_empty() {
        if let Some(pairs) = props.get("mapping").and_then(JsonValue::as_array) {
            for kv in pairs {
                let old = kv.get("key").and_then(JsonValue::as_str);
                let new = kv.get("value").and_then(JsonValue::as_str);
                if let (Some(old), Some(new)) = (old, new) {
                    if !old.is_empty() && !new.is_empty() {
                        excludes.push(quote_ident(old));
                        aliases.push(format!(
                            "{}.{} AS {}",
                            quote_ident(upstream),
                            quote_ident(old),
                            quote_ident(new)
                        ));
                    }
                }
            }
        }
    }
    if aliases.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT {}.* EXCLUDE ({}), {} FROM {}",
        quote_ident(upstream),
        excludes.join(", "),
        aliases.join(", "),
        quote_ident(upstream)
    ))
}

fn build_mapper(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "mapper: missing main input".to_string())?;
    // The Map form writes `expressions` as key-value pairs:
    // output column name -> SQL expression.
    if let Some(pairs) = props.get("expressions").and_then(JsonValue::as_array) {
        let terms: Vec<String> = pairs
            .iter()
            .filter_map(|kv| {
                let name = kv.get("key").and_then(JsonValue::as_str)?.trim();
                let expr = kv.get("value").and_then(JsonValue::as_str)?.trim();
                if name.is_empty() || expr.is_empty() {
                    return None;
                }
                Some(format!("{} AS {}", strip_port_prefixes(expr), quote_ident(name)))
            })
            .collect();
        if !terms.is_empty() {
            return Ok(format!("SELECT {} FROM {}", terms.join(", "), quote_ident(upstream)));
        }
    }
    let mapper = props.get("mapper");
    let outputs = mapper
        .and_then(|m| m.get("outputs"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    if outputs.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    let mut select_terms = Vec::new();
    for o in &outputs {
        let name = o.get("name").and_then(JsonValue::as_str).unwrap_or("col");
        let expr_raw = o
            .get("expression")
            .or_else(|| o.get("expr"))
            .and_then(JsonValue::as_str)
            .unwrap_or("NULL");
        // The visual mapper emits references like `main.col` or
        // `lookup_1.col`. Those don't exist as DuckDB alias prefixes
        // in our generated SQL, so we strip them to bare column refs.
        let expr = strip_port_prefixes(expr_raw);
        select_terms.push(format!("{} AS {}", expr, quote_ident(name)));
    }
    let filter = mapper
        .and_then(|m| m.get("filter"))
        .and_then(JsonValue::as_str)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let mut sql = format!(
        "SELECT {} FROM {}",
        select_terms.join(", "),
        quote_ident(upstream)
    );
    if let Some(predicate) = filter {
        sql.push_str(" WHERE ");
        sql.push_str(predicate);
    }
    Ok(sql)
}

fn strip_port_prefixes(expr: &str) -> String {
    // Replace `<word>.<word>` where the leading word is a known port
    // alias the mapper used, leaving the column reference untouched.
    let mut out = String::with_capacity(expr.len());
    for token in expr.split_inclusive(|c: char| !c.is_alphanumeric() && c != '_' && c != '.') {
        // For each token, if it looks like main.col / lookup_N.col,
        // drop the prefix.
        let (alpha, rest) = split_leading_token(token);
        if !alpha.is_empty() && (alpha == "main" || alpha.starts_with("lookup")) {
            if let Some(stripped) = rest.strip_prefix('.') {
                out.push_str(stripped);
                continue;
            }
        }
        out.push_str(token);
    }
    out
}

fn split_leading_token(s: &str) -> (&str, &str) {
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_alphanumeric() || c == '_' {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    (&s[..end], &s[end..])
}

fn build_join(inputs: &NodeInputs, props: &JsonValue, kind: &str) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "join: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "join: missing lookup input".to_string())?;
    let left_key = props
        .get("leftKey")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "join: leftKey property required".to_string())?;
    let right_key = props
        .get("rightKey")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "join: rightKey property required".to_string())?;
    // The form's joinType, if set, overrides the component-id default so
    // changing it in the UI actually takes effect.
    let kind = match string_prop(props, "joinType").as_deref() {
        Some("inner") => "INNER",
        Some("left") => "LEFT",
        Some("right") => "RIGHT",
        Some("full") | Some("outer") => "FULL OUTER",
        _ => kind,
    };
    Ok(format!(
        "SELECT m.*, r.* FROM {} m {} JOIN {} r ON m.{} = r.{}",
        quote_ident(left),
        kind,
        quote_ident(right),
        quote_ident(left_key),
        quote_ident(right_key)
    ))
}

fn build_semi(inputs: &NodeInputs, props: &JsonValue, anti: bool) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "semi: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "semi: missing lookup input".to_string())?;
    let left_key = props
        .get("leftKey")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "semi: leftKey required".to_string())?;
    let right_key = props
        .get("rightKey")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "semi: rightKey required".to_string())?;
    let op = if anti { "NOT IN" } else { "IN" };
    Ok(format!(
        "SELECT * FROM {} WHERE {} {} (SELECT {} FROM {})",
        quote_ident(left),
        quote_ident(left_key),
        op,
        quote_ident(right_key),
        quote_ident(right)
    ))
}

// ---- Sources ------------------------------------------------------------

fn build_csv_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let has_header = props
        .get("hasHeader")
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    let delim = string_prop(props, "delimiter");
    let quote = string_prop(props, "quoteChar");
    let null_val = string_prop(props, "nullValue");
    let mut args = vec![format!("'{}'", sql_escape(&path))];
    args.push(format!("header={}", has_header));
    if let Some(d) = delim.as_deref().filter(|s| !s.is_empty()) {
        args.push(format!("delim='{}'", sql_escape(d)));
    }
    if let Some(q) = quote.as_deref().filter(|s| !s.is_empty()) {
        args.push(format!("quote='{}'", sql_escape(q)));
    }
    if let Some(n) = null_val.as_deref().filter(|s| !s.is_empty()) {
        args.push(format!("nullstr='{}'", sql_escape(n)));
    }
    if let Some(skip) = props.get("skipLines").and_then(JsonValue::as_u64) {
        if skip > 0 {
            args.push(format!("skip={}", skip));
        }
    }
    if let Some(enc) = string_prop(props, "encoding").filter(|s| !s.is_empty()) {
        args.push(format!("encoding='{}'", sql_escape(&enc)));
    }
    format!("SELECT * FROM read_csv_auto({})", args.join(", "))
}

fn build_tsv_source(props: &JsonValue) -> String {
    // TSV is just CSV with delim='\t'. Force it.
    let mut p = props.clone();
    if let Some(obj) = p.as_object_mut() {
        obj.insert(
            "delimiter".into(),
            JsonValue::String("\t".into()),
        );
    }
    build_csv_source(&p)
}

fn build_parquet_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // Optional projection: comma-separated column list pushed into the read.
    let select = string_prop(props, "columns")
        .filter(|s| !s.trim().is_empty())
        .map(|c| {
            c.split(',')
                .map(|s| quote_ident(s.trim()))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "*".into());
    format!("SELECT {} FROM read_parquet('{}')", select, sql_escape(&path))
}

fn build_json_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!(
        "SELECT * FROM read_json_auto('{}')",
        sql_escape(&path)
    )
}

fn build_sqlite_source(props: &JsonValue) -> String {
    let database = string_prop(props, "database").unwrap_or_default();
    let table = string_prop(props, "tableName").unwrap_or_default();
    let sql = string_prop(props, "sql");
    let from_arg = sql
        .filter(|s| !s.is_empty())
        .unwrap_or(table);
    format!(
        "SELECT * FROM sqlite_scan('{}', '{}')",
        sql_escape(&database),
        sql_escape(&from_arg)
    )
}

fn build_duckdb_source(props: &JsonValue) -> String {
    // For DuckDB sources the runtime ATTACHes the DB in execute. Here
    // we just produce a SELECT that references the attached schema.
    let sql = string_prop(props, "sql").unwrap_or_else(|| "SELECT 1 AS placeholder".into());
    format!("SELECT * FROM ({})", sql)
}

/// Cloud sources (S3 / GCS / Azure Blob / HTTP). DuckDB's httpfs +
/// azure extensions let us read these directly via the same
/// read_csv_auto / read_parquet / read_json_auto family of functions.
/// Format is inferred from the URL extension unless the user picks one.
fn build_cloud_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path")
        .or_else(|| string_prop(props, "url"))
        .unwrap_or_default();
    let override_fmt = string_prop(props, "format");
    let lower = path.to_ascii_lowercase();
    let chosen = override_fmt.filter(|s| !s.is_empty()).unwrap_or_else(|| {
        if lower.ends_with(".parquet") || lower.ends_with(".pq") {
            "parquet".into()
        } else if lower.ends_with(".json")
            || lower.ends_with(".jsonl")
            || lower.ends_with(".ndjson")
        {
            "json".into()
        } else if lower.ends_with(".tsv") {
            "tsv".into()
        } else {
            "csv".into()
        }
    });
    match chosen.as_str() {
        "parquet" => format!("SELECT * FROM read_parquet('{}')", sql_escape(&path)),
        "json" => format!("SELECT * FROM read_json_auto('{}')", sql_escape(&path)),
        "tsv" => format!(
            "SELECT * FROM read_csv_auto('{}', header=true, delim='\\t')",
            sql_escape(&path)
        ),
        _ => format!(
            "SELECT * FROM read_csv_auto('{}', header=true)",
            sql_escape(&path)
        ),
    }
}

// ---- Sinks --------------------------------------------------------------

fn build_sink_sql(
    component_id: &str,
    props: &JsonValue,
    from_view: &str,
) -> Result<String, EngineError> {
    match component_id {
        "snk.csv" => Ok(build_csv_sink(props, from_view)),
        "snk.tsv" => {
            let mut p = props.clone();
            if let Some(obj) = p.as_object_mut() {
                obj.insert("delimiter".into(), JsonValue::String("\t".into()));
            }
            Ok(build_csv_sink(&p, from_view))
        }
        "snk.parquet" => Ok(build_parquet_sink(props, from_view)),
        "snk.json" | "snk.jsonl" => Ok(build_json_sink(props, from_view)),
        "snk.s3" | "snk.gcs" | "snk.azureblob" => Ok(build_cloud_sink(props, from_view)),
        other => Err(EngineError::Unsupported(format!(
            "Sink '{}' is not yet implemented",
            other
        ))),
    }
}

/// Cloud sink — COPY a view out to an s3:// / gs:// / az:// URL.
/// DuckDB's httpfs handles the upload; credentials come from the
/// SECRET wired up in execute_pipeline_with_events. Format is inferred
/// from the URL extension unless overridden.
fn build_cloud_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path")
        .or_else(|| string_prop(props, "url"))
        .unwrap_or_default();
    let override_fmt = string_prop(props, "format").filter(|s| !s.is_empty());
    let lower = path.to_ascii_lowercase();
    let chosen = override_fmt.unwrap_or_else(|| {
        if lower.ends_with(".parquet") || lower.ends_with(".pq") {
            "parquet".into()
        } else if lower.ends_with(".json") || lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
            "json".into()
        } else {
            "csv".into()
        }
    });
    let options = match chosen.as_str() {
        "parquet" => "FORMAT PARQUET, COMPRESSION 'ZSTD'".to_string(),
        "json" => "FORMAT JSON".to_string(),
        _ => "FORMAT CSV, HEADER true".to_string(),
    };
    format!(
        "COPY (SELECT * FROM {}) TO '{}' ({})",
        quote_ident(from_view),
        sql_escape(&path),
        options
    )
}

fn build_csv_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // The sink form writes `writeHeader`; the source uses `hasHeader`.
    let header = props
        .get("writeHeader")
        .or_else(|| props.get("hasHeader"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    let delim = string_prop(props, "delimiter").unwrap_or_else(|| ",".into());
    let null_val = string_prop(props, "nullValue").unwrap_or_default();
    let mut options = vec![
        "FORMAT CSV".to_string(),
        format!("HEADER {}", header),
        format!("DELIM '{}'", sql_escape(&delim)),
    ];
    if !null_val.is_empty() {
        options.push(format!("NULLSTR '{}'", sql_escape(&null_val)));
    }
    format!(
        "COPY (SELECT * FROM {}) TO '{}' ({})",
        quote_ident(from_view),
        sql_escape(&path),
        options.join(", ")
    )
}

fn build_parquet_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let compression = string_prop(props, "compression").unwrap_or_else(|| "ZSTD".into());
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET, COMPRESSION '{}')",
        quote_ident(from_view),
        sql_escape(&path),
        sql_escape(&compression)
    )
}

fn build_json_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let array = string_prop(props, "format")
        .map(|f| f.eq_ignore_ascii_case("array"))
        .unwrap_or(false);
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT JSON, ARRAY {})",
        quote_ident(from_view),
        sql_escape(&path),
        if array { "true" } else { "false" }
    )
}

// ---- Helpers ------------------------------------------------------------

fn columns_from_props(props: &JsonValue, key: &str) -> Option<Vec<String>> {
    props
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
}

fn string_prop(props: &JsonValue, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(JsonValue::as_str)
        .map(String::from)
}

pub(crate) fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn duckle_type_to_duckdb(t: &str) -> String {
    match t.to_lowercase().as_str() {
        "string" | "varchar" | "text" => "VARCHAR".into(),
        "int32" | "int" | "integer" => "INTEGER".into(),
        "int64" | "bigint" => "BIGINT".into(),
        "float32" | "real" | "float" => "REAL".into(),
        "float64" | "double" => "DOUBLE".into(),
        "bool" | "boolean" => "BOOLEAN".into(),
        "date" => "DATE".into(),
        "timestamp" => "TIMESTAMP".into(),
        "time" => "TIME".into(),
        "decimal" => "DECIMAL(18,4)".into(),
        "json" => "JSON".into(),
        "binary" | "blob" => "BLOB".into(),
        other => other.to_uppercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline_from_json(s: &str) -> PipelineDoc {
        serde_json::from_str(s).expect("valid pipeline JSON")
    }

    #[test]
    fn compiles_csv_filter_parquet() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"status = 'paid'"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Parquet","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        assert_eq!(compiled.stages.len(), 3);
        assert_eq!(compiled.stages[0].node_id, "s1");
        assert!(compiled.stages[0]
            .sql
            .contains("read_csv_auto('/tmp/orders.csv'"));
        assert!(compiled.stages[1].sql.contains("WHERE status = 'paid'"));
        assert_eq!(compiled.stages[2].kind, StageKind::Sink);
        assert!(compiled.stages[2]
            .sql
            .contains("TO '/tmp/out.parquet' (FORMAT PARQUET"));
    }

    #[test]
    fn rejects_cycles() {
        let p = pipeline_from_json(
            r#"{
              "nodes":[
                {"id":"a","position":{"x":0,"y":0},"data":{"label":"A","componentId":"xf.filter","properties":{}}},
                {"id":"b","position":{"x":0,"y":0},"data":{"label":"B","componentId":"xf.filter","properties":{}}}
              ],
              "edges":[
                {"id":"e1","source":"a","target":"b","data":{"connectionType":"main"}},
                {"id":"e2","source":"b","target":"a","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        assert!(compile(&p).is_err());
    }
}
