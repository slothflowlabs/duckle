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
/// directly - no wrapping metadata required for a run.
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
    /// For relational-DB sinks in upsert mode: the planner can't
    /// enumerate the upstream's non-key columns up front, so it leaves
    /// `sql` empty and the executor introspects the materialized
    /// upstream (DESCRIBE) before assembling the final INSERT ... ON
    /// CONFLICT statement.
    pub upsert: Option<UpsertSpec>,
}

#[derive(Debug, Clone)]
pub struct UpsertSpec {
    pub family: UpsertFamily,
    /// INSTALL/LOAD/ATTACH preamble; ends with a trailing space.
    pub attach: String,
    /// Fully qualified target inside the ATTACHed DB
    /// (e.g. `duckle_dst."public"."orders"`).
    pub target: String,
    /// The upstream materialized table name in the temp DB.
    pub from_view: String,
    /// Columns the user declared as the conflict key.
    pub conflict_cols: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum UpsertFamily {
    /// `ON CONFLICT (key) DO UPDATE SET col = EXCLUDED.col` (Postgres, Cockroach).
    Postgres,
    /// `ON DUPLICATE KEY UPDATE col = VALUES(col)` (MySQL, MariaDB).
    MySql,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StageKind {
    /// Non-sink node - emitted as a `CREATE OR REPLACE TEMP VIEW`.
    View,
    /// Sink - emitted as a `COPY (...) TO '...' (FORMAT ...)`.
    Sink,
}

#[derive(Debug)]
pub struct CompiledPipeline {
    pub stages: Vec<Stage>,
    /// Node IDs that have no downstream consumer - used to fetch
    /// preview rows when there's no sink.
    pub leaves: Vec<String>,
}

/// Compile only the subgraph upstream of (and including) `target_id`.
/// Sinks downstream of the target are dropped - the target becomes the
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
        // Switch / conditional split: each case + default port reads
        // from its own `<node>__<handle>` table that build_switch
        // materializes.
        Some(h) if h.starts_with("case_") || h == "default" => {
            format!("{}__{}", source_id, h)
        }
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
    let mut upsert: Option<UpsertSpec> = None;
    // ATTACH statements for external-DB nodes (DuckDB/SQLite). Each stage
    // runs in its own CLI process, so fixed aliases are collision-free.
    let attach = attach_prelude(component_id, &props);
    let (sql, kind, from) = if component_id.starts_with("snk.") {
        let from_view = inputs
            .main()
            .ok_or_else(|| missing_input(node, "main"))?;
        sink_path = string_prop(&props, "path").filter(|s| !s.is_empty());
        sink_mode = string_prop(&props, "mode").filter(|s| !s.is_empty());
        // Relational DB upsert is the only sink mode whose SQL the
        // planner can't fully generate up front: the SET clause needs
        // the upstream's non-key column list, which the executor reads
        // via DESCRIBE before assembling the final INSERT.
        if sink_mode.as_deref() == Some("upsert")
            && matches!(
                component_id,
                "snk.postgres" | "snk.cockroach" | "snk.mysql" | "snk.mariadb"
            )
        {
            let conflict_cols = columns_list(&props, "conflictColumns");
            if conflict_cols.is_empty() {
                return Err(EngineError::Config(format!(
                    "{}: upsert mode needs at least one column in Conflict columns",
                    component_id
                )));
            }
            let table = string_prop(&props, "tableName")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    EngineError::Config(format!("{}: table name is required", component_id))
                })?;
            let schema = string_prop(&props, "schemaName").filter(|s| !s.is_empty());
            let target = relational_qualified(
                "duckle_dst",
                component_id,
                schema.as_deref(),
                &table,
            );
            let family = if component_id == "snk.postgres" || component_id == "snk.cockroach" {
                UpsertFamily::Postgres
            } else {
                UpsertFamily::MySql
            };
            upsert = Some(UpsertSpec {
                family,
                attach: attach.clone(),
                target,
                from_view: from_view.to_string(),
                conflict_cols,
            });
            (String::new(), StageKind::Sink, Some(from_view.to_string()))
        } else {
            (
                format!("{}{}", attach, build_sink_sql(component_id, &props, from_view)?),
                StageKind::Sink,
                Some(from_view.to_string()),
            )
        }
    } else if component_id == "ctl.switch" {
        // Switch materializes one table per case + default; it has no
        // main output table, so the count_rows fallback in the executor
        // (which would target node.id) just returns None for it.
        let sql = build_switch(&node.id, inputs, &props).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })?;
        (format!("{}{}", attach, sql), StageKind::View, None)
    } else {
        let body = build_view_sql(component_id, &props, inputs).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })?;
        // Materialize as a real table so the result persists across the
        // separate CLI invocations the executor uses per stage.
        let mut sql = format!(
            "{}CREATE OR REPLACE TABLE {} AS {}",
            attach,
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
        upsert,
    })
}

/// The `SELECT * FROM <reader>` SQL for a source format - used by the
/// engine's inspect path to DESCRIBE / sample without materializing.
pub fn source_select_for_format(format: &str, props: &JsonValue) -> Option<String> {
    Some(match format {
        "csv" => build_csv_source(props),
        "tsv" => build_tsv_source(props),
        "parquet" => build_parquet_source(props),
        "json" | "jsonl" | "ndjson" => build_json_source(props),
        "sqlite" => build_sqlite_source(props),
        "duckdb" => build_duckdb_source(props),
        "s3" | "gcs" | "azureblob" | "http" | "https" => build_cloud_source(format, props),
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
        "src.s3" | "src.gcs" | "src.azureblob" | "src.http"
        | "src.minio" | "src.r2" | "src.b2" => {
            // MinIO / R2 / B2 are S3-compatible; the endpoint lives in
            // the SECRET created by the runtime, so the URL itself is
            // just s3://bucket/key.
            let s = component_id.strip_prefix("src.").unwrap_or(component_id);
            let scheme = if matches!(s, "minio" | "r2" | "b2") { "s3" } else { s };
            Ok(build_cloud_source(scheme, props))
        }
        "src.postgres" | "src.cockroach" | "src.mysql" | "src.mariadb"
        | "src.motherduck" => build_relational_source(component_id, props),
        "src.avro" => Ok(build_avro_source(props)),
        "src.excel" => Ok(build_excel_source(props)),
        "src.iceberg" => Ok(build_iceberg_source(props)),
        "src.delta" => Ok(build_delta_source(props)),
        "src.spatial" => Ok(build_spatial_source(props)),
        // Pass-through transforms
        "xf.filter" => build_filter(inputs, props),
        // Log Rows - pass data through unchanged; its rows surface in the
        // Output / Preview so you can inspect mid-pipeline (like tLogRow).
        "xf.log" => build_passthrough_op(inputs, "SELECT *"),
        "xf.project" => build_project(inputs, props),
        "xf.distinct" => build_distinct(inputs, props),
        "xf.limit" => build_limit(inputs, props),
        "xf.sort" => build_sort(inputs, props),
        "xf.agg" | "xf.groupby" => build_aggregate(inputs, props, GroupMode::Plain),
        "xf.rollup" => build_aggregate(inputs, props, GroupMode::Rollup),
        "xf.cube" => build_aggregate(inputs, props, GroupMode::Cube),
        "xf.aggwin" => build_window_aggregate(inputs, props),
        "xf.union" => build_union(inputs, true),
        "xf.unionall" => build_union(inputs, false),
        "xf.intersect" => build_setop(inputs, "INTERSECT"),
        "xf.except" => build_setop(inputs, "EXCEPT"),
        "xf.addcol" | "xf.coalesce" => build_addcol(inputs, props),
        "xf.rownum" | "xf.rank" | "xf.denserank" | "xf.lead" | "xf.lag" | "xf.first"
        | "xf.last" | "xf.ntile" => build_window(inputs, props, component_id),
        "xf.pivot" => build_pivot(inputs, props),
        "xf.unpivot" => build_unpivot(inputs, props),
        "xf.denorm" => build_denormalize(inputs, props),
        "xf.norm" => build_normalize(inputs, props),
        "xf.transpose" => build_transpose(inputs),
        "xf.cdc.diff" => build_cdc_diff(inputs, props),
        "xf.cdc.scd2" => build_scd2(inputs, props),
        "xf.cdc.scd1" => build_scd1(inputs, props),
        "xf.cdc.upsert" => build_upsert(inputs, props),
        "xf.ai.vector_search" => build_vector_search(inputs, props),
        // Data-quality validators - the PASS rows. Failures go to the
        // node's __reject table (see build_reject_sql).
        "qa.notnull" | "qa.range" | "qa.regex" | "qa.unique" | "qa.schemavalidate" => {
            build_quality(inputs, props, component_id, false)
        }
        "qa.profile" => build_profile(inputs, props),
        "qa.describe" => build_describe(inputs),
        "qa.histogram" => build_histogram(inputs, props),
        "qa.standardize" => build_standardize(inputs, props),
        "qa.dedupe" => build_fuzzy_dedupe(inputs, props),
        "qa.match" => build_record_match(inputs, props),
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
        // Custom SQL - runs the user's SELECT as a real stage, with the
        // upstream exposed as `input`. Makes SQL routines executable too.
        "code.sql" | "code.sqltemplate" => build_custom_sql(inputs, props),
        // Routing: replicate is a passthrough (the graph already lets
        // multiple downstream edges read the same materialized table);
        // merge concatenates multiple input streams with UNION ALL.
        "ctl.replicate" => {
            let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.replicate"))?;
            Ok(format!("SELECT * FROM {}", quote_ident(upstream)))
        }
        "ctl.merge" => build_union(inputs, false),
        // Everything else isn't executable yet. Fail loudly rather than
        // silently passing data through unchanged (which would look like
        // success while doing nothing).
        other => Err(format!(
            "'{}' isn't executable on the DuckDB engine yet - it's a preview component.",
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
        .ok_or_else(|| "Custom SQL is empty - write a SELECT or pick a SQL routine".to_string())?;
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
    // Listed columns first, everything else after - never drops a column.
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

/// Window aggregate: an aggregate computed over a window, keeping every
/// row (unlike Group By, which collapses them).
fn build_window_aggregate(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.aggwin"))?;
    let func = string_prop(props, "function").unwrap_or_else(|| "sum".into()).to_uppercase();
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "*".into());
    let call = if column == "*" {
        format!("{}(*)", func)
    } else {
        format!("{}({})", func, quote_ident(&column))
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
    let out = string_prop(props, "outputName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", func.to_lowercase(), column.replace('*', "all")));
    Ok(format!(
        "SELECT *, {} OVER ({}) AS {} FROM {}",
        call,
        over,
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

/// CDC Diff Detect: compare a 'new' input (main) against a 'previous'
/// input (lookup) on a natural key and tag each row inserted / deleted /
/// updated / unchanged. Updates are detected from the compare columns;
/// unchanged rows are dropped unless the user keeps them.
fn build_cdc_diff(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs
        .main()
        .ok_or_else(|| "Diff Detect needs a 'new' input on the main port".to_string())?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "Diff Detect needs a 'previous' input (connect it to the previous port)".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("Diff Detect needs natural key columns".to_string());
    }
    let compares = columns_list(props, "compareColumns");
    let reject_unchanged = props
        .get("rejectUnchanged")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let coalesced = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("COALESCE(cur.{q}, prev.{q}) AS {q}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let excl = keys
        .iter()
        .map(|k| quote_ident(k))
        .collect::<Vec<_>>()
        .join(", ");
    let join_on = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("cur.{q} = prev.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let first_key = quote_ident(&keys[0]);
    let updated = if compares.is_empty() {
        String::new()
    } else {
        let diff = compares
            .iter()
            .map(|c| {
                let q = quote_ident(c);
                format!("cur.{q} IS DISTINCT FROM prev.{q}")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        format!("WHEN ({diff}) THEN 'updated' ")
    };
    let inner = format!(
        "SELECT {coalesced}, cur.* EXCLUDE ({excl}), \
         CASE WHEN prev.{first_key} IS NULL THEN 'inserted' \
         WHEN cur.{first_key} IS NULL THEN 'deleted' \
         {updated}ELSE 'unchanged' END AS change_type \
         FROM {cur} cur FULL OUTER JOIN {prev} prev ON {join_on}",
        cur = quote_ident(cur),
        prev = quote_ident(prev),
    );
    if reject_unchanged {
        Ok(format!(
            "SELECT * FROM ({inner}) WHERE change_type != 'unchanged'"
        ))
    } else {
        Ok(inner)
    }
}

/// Denormalize: collapse many rows per group into one, joining the
/// chosen columns into a single delimited cell with string_agg.
fn build_denormalize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.denorm"))?;
    let group_by = columns_list(props, "groupBy");
    if group_by.is_empty() {
        return Err("Denormalize needs group-by columns".to_string());
    }
    let agg_cols = columns_list(props, "aggregateColumns");
    if agg_cols.is_empty() {
        return Err("Denormalize needs columns to aggregate".to_string());
    }
    let sep = string_prop(props, "separator").unwrap_or_else(|| ", ".into());
    let sep_sql = sep.replace('\'', "''");
    let group_list = group_by
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let aggs = agg_cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("string_agg(CAST({q} AS VARCHAR), '{sep_sql}') AS {q}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT {group_list}, {aggs} FROM {} GROUP BY {group_list}",
        quote_ident(upstream)
    ))
}

/// Normalize: explode a delimited string (or array) column into one row
/// per element, keeping the other columns.
fn build_normalize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.norm"))?;
    let col = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Normalize needs a column to split".to_string())?;
    let q = quote_ident(&col);
    let sep = string_prop(props, "separator").unwrap_or_else(|| ",".into());
    let value_expr = if sep.is_empty() {
        // Empty separator means the column is already an array; just unnest.
        format!("unnest({q})")
    } else {
        let sep_sql = sep.replace('\'', "''");
        format!("unnest(string_split(CAST({q} AS VARCHAR), '{sep_sql}'))")
    };
    Ok(format!(
        "SELECT * EXCLUDE ({q}), {value_expr} AS {q} FROM {}",
        quote_ident(upstream)
    ))
}

/// Transpose: swap the input's rows and columns. The output has one row
/// per original column (named `colname`) and one value column per
/// original row, named `r1`, `r2`, ... The "r" prefix keeps the column
/// names valid identifiers and parsable as a CSV header (a pure-numeric
/// header would not auto-detect). Requires the input's columns to share
/// a compatible type (UNPIVOT cannot mix unrelated types).
fn build_transpose(inputs: &NodeInputs) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.transpose"))?;
    Ok(format!(
        "SELECT * FROM (PIVOT (FROM (SELECT *, \
         'r' || CAST(ROW_NUMBER() OVER () AS VARCHAR) AS _row FROM {up}) \
         UNPIVOT (val FOR colname IN (COLUMNS(* EXCLUDE _row)))) \
         ON _row USING first(val) GROUP BY colname)",
        up = quote_ident(upstream)
    ))
}

/// Switch / Conditional Split. Routes rows to case_1 ... case_N output
/// ports based on the form's `branches` (a key-value of branch name
/// -> boolean SQL expression). First-match-wins: a row that satisfied
/// branch i is excluded from branches i+1..N and from default. Up to
/// 3 cases (matching the fixed port set) plus a default for the
/// remainder. The form's branch object preserves insertion order
/// because the workspace enables serde_json's preserve_order feature.
fn build_switch(node_id: &str, inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.switch"))?;
    let mut conds: Vec<String> = Vec::new();
    if let Some(obj) = props.get("branches").and_then(|v| v.as_object()) {
        for (_name, val) in obj {
            if let Some(c) = val.as_str().filter(|s| !s.trim().is_empty()) {
                conds.push(c.to_string());
            }
            if conds.len() >= 3 {
                break;
            }
        }
    }
    if conds.is_empty() {
        return Err("Switch needs at least one branch condition".to_string());
    }
    let up = quote_ident(upstream);
    let mut stmts: Vec<String> = Vec::new();
    let mut prior: Vec<String> = Vec::new();
    for (i, cond) in conds.iter().enumerate() {
        let case_table = format!("{}__case_{}", node_id, i + 1);
        let where_clause = if prior.is_empty() {
            format!("({})", cond)
        } else {
            let neg = prior
                .iter()
                .map(|p| format!("NOT ({})", p))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({}) AND {}", cond, neg)
        };
        stmts.push(format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM {} WHERE {}",
            quote_ident(&case_table),
            up,
            where_clause
        ));
        prior.push(cond.clone());
    }
    // Default: rows that no branch matched.
    let default_table = format!("{}__default", node_id);
    let default_where = prior
        .iter()
        .map(|p| format!("NOT ({})", p))
        .collect::<Vec<_>>()
        .join(" AND ");
    stmts.push(format!(
        "CREATE OR REPLACE TABLE {} AS SELECT * FROM {} WHERE {}",
        quote_ident(&default_table),
        up,
        default_where
    ));
    Ok(stmts.join("; "))
}

/// SCD Type 1: overwrite-in-place. Output is the resolved current
/// state: every row from `current`, plus rows from `previous` whose
/// key isn't in current (so unrelated history isn't dropped). Both
/// inputs must have the same column schema.
fn build_scd1(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.scd1"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "SCD1 needs a 'previous' input on the lookup port".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("SCD1 needs natural key columns".to_string());
    }
    let key_eq = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("p.{q} = c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "SELECT * FROM {cur} \
         UNION ALL \
         SELECT * FROM {prev} p WHERE NOT EXISTS (SELECT 1 FROM {cur} c WHERE {key_eq})",
        cur = quote_ident(cur),
        prev = quote_ident(prev),
    ))
}

/// Merge / Upsert: output the delta to write into a target -  the
/// rows in `current` that are either a new key or a changed value.
/// Unchanged rows are skipped (the target already has them). Deletes
/// are NOT emitted; use Diff Detect when you need them.
fn build_upsert(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.upsert"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "Upsert needs a 'previous' input on the lookup port".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("Upsert needs natural key columns".to_string());
    }
    let compares = columns_list(props, "compareColumns");
    let key_eq = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("cur.{q} = p.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let first_key = quote_ident(&keys[0]);
    let change_clause = if compares.is_empty() {
        // No compare columns means we only flag new keys; everything
        // already in previous (regardless of value) is skipped.
        String::new()
    } else {
        let cmp_diff = compares
            .iter()
            .map(|c| {
                let q = quote_ident(c);
                format!("cur.{q} IS DISTINCT FROM p.{q}")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        format!(" OR ({cmp_diff})")
    };
    Ok(format!(
        "SELECT cur.* FROM {cur} cur LEFT JOIN {prev} p ON {key_eq} \
         WHERE p.{first_key} IS NULL{change_clause}",
        cur = quote_ident(cur),
        prev = quote_ident(prev),
    ))
}

/// SCD Type 2: maintain versioned history. Reads `current` on main and
/// `previous` on the lookup port; the previous input must already carry
/// the SCD columns (valid_from, valid_to, is_current) at the end of its
/// schema. Output is the new history table: closed records get their
/// valid_to + is_current updated, unchanged records pass through, and
/// new / changed keys land as fresh current versions. Compare columns
/// drive the change detection.
fn build_scd2(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.scd2"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "SCD2 needs a 'previous' input on the lookup port (the current history table)".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("SCD2 needs natural key columns".to_string());
    }
    let compares = columns_list(props, "compareColumns");
    if compares.is_empty() {
        return Err("SCD2 needs at least one compare column to detect changes".to_string());
    }
    let valid_from = string_prop(props, "validFromColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "valid_from".into());
    let valid_to = string_prop(props, "validToColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "valid_to".into());
    let is_current = string_prop(props, "isCurrentColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "is_current".into());

    let key_eq = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("p.{q} = c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let cmp_diff = compares
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("p.{q} IS DISTINCT FROM c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let cmp_same = compares
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("p.{q} IS NOT DISTINCT FROM c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let first_key = quote_ident(&keys[0]);
    let vf = quote_ident(&valid_from);
    let vt = quote_ident(&valid_to);
    let ic = quote_ident(&is_current);
    let cur_q = quote_ident(cur);
    let prev_q = quote_ident(prev);

    Ok(format!(
        "WITH prev_current AS (SELECT * FROM {prev_q} WHERE {ic}), \
              prev_history AS (SELECT * FROM {prev_q} WHERE NOT {ic}), \
              to_close AS (SELECT p.* FROM prev_current p LEFT JOIN {cur_q} c ON {key_eq} \
                           WHERE c.{first_key} IS NULL OR ({cmp_diff})), \
              to_keep AS (SELECT p.* FROM prev_current p INNER JOIN {cur_q} c ON {key_eq} \
                          WHERE {cmp_same}), \
              to_insert AS (SELECT c.* FROM {cur_q} c LEFT JOIN prev_current p ON {key_eq} \
                            WHERE p.{first_key} IS NULL OR ({cmp_diff})) \
         SELECT * FROM prev_history \
         UNION ALL SELECT * FROM to_keep \
         UNION ALL SELECT * REPLACE (CURRENT_TIMESTAMP AS {vt}, FALSE AS {ic}) FROM to_close \
         UNION ALL SELECT *, CURRENT_TIMESTAMP AS {vf}, NULL::TIMESTAMP AS {vt}, TRUE AS {ic} FROM to_insert"
    ))
}

/// Unpivot: turn a set of columns into name/value rows (wide to long).
fn build_unpivot(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.unpivot"))?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Err("Unpivot needs the columns to unpivot".to_string());
    }
    let name_col = string_prop(props, "nameColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "name".into());
    let value_col = string_prop(props, "valueColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "value".into());
    let on = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    Ok(format!(
        "SELECT * FROM (UNPIVOT (SELECT * FROM {}) ON {} INTO NAME {} VALUE {})",
        quote_ident(upstream),
        on,
        quote_ident(&name_col),
        quote_ident(&value_col)
    ))
}

/// Column Profile: one summary-stats row per column, via DuckDB
/// SUMMARIZE (count, null %, approx distinct, min/max, quartiles).
fn build_profile(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.profile"))?;
    let cols = columns_list(props, "columns");
    let projection = if cols.is_empty() {
        "*".to_string()
    } else {
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Ok(format!(
        "SELECT * FROM (SUMMARIZE SELECT {} FROM {})",
        projection,
        quote_ident(upstream)
    ))
}

/// Describe: the column names and types of the input.
fn build_describe(inputs: &NodeInputs) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.describe"))?;
    Ok(format!(
        "SELECT * FROM (DESCRIBE SELECT * FROM {})",
        quote_ident(upstream)
    ))
}

/// Histogram: value frequencies for one column, most frequent first.
fn build_histogram(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.histogram"))?;
    let col = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Histogram needs a column".to_string())?;
    let q = quote_ident(&col);
    Ok(format!(
        "SELECT {q} AS value, COUNT(*) AS frequency FROM {} GROUP BY {q} ORDER BY frequency DESC, value",
        quote_ident(upstream)
    ))
}

/// Standardize: trim, case-normalize, and collapse internal whitespace in
/// the chosen text columns, in place.
fn build_standardize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.standardize"))?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Err("Standardize needs at least one column".to_string());
    }
    let case = string_prop(props, "case").unwrap_or_else(|| "none".into());
    let trim = props.get("trim").and_then(|v| v.as_bool()).unwrap_or(true);
    let collapse = props
        .get("collapseWhitespace")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let replacements = cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            let mut expr = format!("CAST({} AS VARCHAR)", q);
            expr = match case.as_str() {
                "upper" => format!("UPPER({})", expr),
                "lower" => format!("LOWER({})", expr),
                "title" => format!("INITCAP({})", expr),
                _ => expr,
            };
            if collapse {
                expr = format!("regexp_replace({}, '\\s+', ' ', 'g')", expr);
            }
            if trim {
                expr = format!("TRIM({})", expr);
            }
            format!("{} AS {}", expr, q)
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT * REPLACE ({}) FROM {}",
        replacements,
        quote_ident(upstream)
    ))
}

/// Lowercased comparison key from the chosen columns, for fuzzy
/// matching. Errors if no columns are given.
fn match_key(props: &JsonValue) -> Result<String, String> {
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Err("needs at least one compare column".to_string());
    }
    Ok(format!(
        "lower(concat_ws(' ', {}))",
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// A 0..1 similarity score expression over a._key / b._key, plus the
/// configured threshold. Unknown algorithms fall back to Jaro-Winkler.
fn similarity(props: &JsonValue) -> (String, f64) {
    let algo = string_prop(props, "algorithm").unwrap_or_else(|| "jaro-winkler".into());
    let threshold = props
        .get("threshold")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.85);
    let score = match algo.as_str() {
        "levenshtein" => "(1.0 - levenshtein(a._key, b._key)::DOUBLE \
             / GREATEST(length(a._key), length(b._key), 1))"
            .to_string(),
        _ => "jaro_winkler_similarity(a._key, b._key)".to_string(),
    };
    (score, threshold)
}

/// Fuzzy Deduplicate: keep the first row of each near-duplicate cluster,
/// where rows are duplicates when their key similarity meets the
/// threshold.
fn build_fuzzy_dedupe(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.dedupe"))?;
    let key = match_key(props).map_err(|e| format!("Fuzzy Deduplicate {e}"))?;
    let (score, threshold) = similarity(props);
    Ok(format!(
        "WITH ranked AS MATERIALIZED (SELECT *, {key} AS _key, \
         ROW_NUMBER() OVER (ORDER BY {key}) AS _rn FROM {up}) \
         SELECT a.* EXCLUDE (_key, _rn) FROM ranked a \
         WHERE NOT EXISTS (SELECT 1 FROM ranked b \
         WHERE b._rn < a._rn AND {score} >= {threshold})",
        up = quote_ident(upstream)
    ))
}

/// Record Match: self-join the input and emit each pair of rows whose key
/// similarity meets the threshold, with a match score (record linkage
/// within one dataset).
fn build_record_match(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.match"))?;
    let key = match_key(props).map_err(|e| format!("Record Match {e}"))?;
    let (score, threshold) = similarity(props);
    Ok(format!(
        "WITH k AS MATERIALIZED (SELECT *, {key} AS _key, ROW_NUMBER() OVER () AS _rn FROM {up}) \
         SELECT a.* EXCLUDE (_key, _rn), b._key AS matched_key, round({score}, 4) AS match_score \
         FROM k a JOIN k b ON a._rn < b._rn AND {score} >= {threshold}",
        up = quote_ident(upstream)
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
        "qa.notnull" | "qa.schemavalidate" => {
            // Schema Validate reuses the not-null predicate against the
            // form's expectedColumns list (the columns the user said the
            // input must have populated). Any row missing a value in any
            // of those columns is rejected.
            let key = if component_id == "qa.schemavalidate" {
                "expectedColumns"
            } else {
                "columns"
            };
            let cols = columns_list(props, key);
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
        "qa.notnull" | "qa.range" | "qa.regex" | "qa.unique" | "qa.schemavalidate" => {
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

/// A numeric property as a SQL literal - only if it's actually numeric,
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
    // RENAME via SELECT * REPLACE - keeps unrelated columns intact.
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
    // The DuckDB file is ATTACHed as `duckle_src` (READ_ONLY) by the
    // stage / inspect prelude; we read from it qualified by that alias.
    if let Some(table) = string_prop(props, "tableName").filter(|s| !s.is_empty()) {
        match string_prop(props, "schema").filter(|s| !s.is_empty()) {
            Some(schema) => format!(
                "SELECT * FROM duckle_src.{}.{}",
                quote_ident(&schema),
                quote_ident(&table)
            ),
            None => format!("SELECT * FROM duckle_src.{}", quote_ident(&table)),
        }
    } else if let Some(sql) = string_prop(props, "sql").filter(|s| !s.trim().is_empty()) {
        // Advanced: a custom query. Reference tables as duckle_src.<table>.
        format!("({})", sql)
    } else {
        "SELECT 1 AS placeholder LIMIT 0".into()
    }
}

/// ATTACH statements for external-database nodes. The aliases are fixed
/// (`duckle_src` / `duckle_dst`) - safe because each stage is its own
/// CLI process.
fn attach_prelude(component_id: &str, props: &JsonValue) -> String {
    // Network DBs use host/port + libpq-style fields, not the
    // file-style `database` path the file-based ATTACH connectors use.
    // Cockroach speaks PG wire so it rides the postgres extension;
    // MariaDB speaks MySQL wire so it rides the mysql extension.
    match component_id {
        "src.postgres" | "src.cockroach" => return db_attach(props, "postgres", 5432, true),
        "snk.postgres" | "snk.cockroach" => return db_attach(props, "postgres", 5432, false),
        "src.mysql" | "src.mariadb" => return db_attach(props, "mysql", 3306, true),
        "snk.mysql" | "snk.mariadb" => return db_attach(props, "mysql", 3306, false),
        "src.motherduck" => return md_attach(props),
        // Extensions are pre-installed (desktop: the first-launch
        // installer; CI: a dedicated pre-install step). Each fresh
        // DuckDB process still needs LOAD. Concurrent INSTALL would
        // race on the cached extension file and intermittently fail.
        "src.avro" => return "LOAD avro; ".into(),
        "src.excel" => return "LOAD excel; ".into(),
        "src.iceberg" => return "LOAD iceberg; ".into(),
        "src.delta" => return "LOAD delta; ".into(),
        // Vector Similarity Search uses the vss extension's array_*
        // distance functions; LOAD before the SELECT runs.
        "xf.ai.vector_search" => return "LOAD vss; ".into(),
        // Spatial is GDAL-backed and ~50 MB; deliberately kept out of
        // the first-launch DUCKDB_EXTENSIONS pre-fetch so the install
        // stays small. INSTALL runs lazily on first use, then LOAD on
        // every subsequent run.
        "src.spatial" => return "INSTALL spatial; LOAD spatial; ".into(),
        _ => {}
    }
    let db = match string_prop(props, "database").filter(|s| !s.is_empty()) {
        Some(d) => d,
        None => return String::new(),
    };
    match component_id {
        "src.duckdb" => format!("ATTACH '{}' AS duckle_src (READ_ONLY); ", sql_escape(&db)),
        "snk.sqlite" => format!("ATTACH '{}' AS duckle_dst (TYPE SQLITE); ", sql_escape(&db)),
        "snk.duckdb" => format!("ATTACH '{}' AS duckle_dst; ", sql_escape(&db)),
        _ => String::new(),
    }
}

/// ATTACH a network relational database through a DuckDB extension
/// (postgres or mysql). The connection string is built libpq-style from
/// host / port / database / user / password; the extension-specific key
/// for the database name (`dbname` for libpq/Postgres, `database` for
/// the MySQL driver) is handled here. INSTALL+LOAD is prepended so a
/// fresh user without the extension cache still attaches successfully,
/// though the first-launch installer already pre-fetches both.
fn db_attach(props: &JsonValue, extension: &str, default_port: u64, read_only: bool) -> String {
    let host = string_prop(props, "host").unwrap_or_default();
    if host.is_empty() {
        return String::new();
    }
    let port = props
        .get("port")
        .and_then(|v| v.as_u64())
        .filter(|p| *p > 0)
        .unwrap_or(default_port);
    let db_key = if extension == "postgres" { "dbname" } else { "database" };
    let mut parts = vec![format!("host={}", host), format!("port={}", port)];
    if let Some(db) = string_prop(props, "database").filter(|s| !s.is_empty()) {
        parts.push(format!("{}={}", db_key, db));
    }
    if let Some(u) = string_prop(props, "user").filter(|s| !s.is_empty()) {
        parts.push(format!("user={}", u));
    }
    if let Some(p) = string_prop(props, "password").filter(|s| !s.is_empty()) {
        parts.push(format!("password={}", p));
    }
    let connstr = parts.join(" ");
    let (alias, mode) = if read_only {
        ("duckle_src", ", READ_ONLY")
    } else {
        ("duckle_dst", "")
    };
    let type_name = extension.to_uppercase();
    format!(
        "LOAD {ext}; ATTACH '{conn}' AS {alias} (TYPE {type_name}{mode}); ",
        ext = extension,
        conn = sql_escape(&connstr),
        alias = alias,
        type_name = type_name,
        mode = mode
    )
}

/// Source for a network relational DB (Postgres / Cockroach via the
/// postgres extension; MySQL / MariaDB via the mysql extension). Reads
/// from `duckle_src` qualified by the right depth: Postgres uses
/// catalog.schema.table (default schema `public`); MySQL uses
/// catalog.table (the database is selected at ATTACH time).
fn build_relational_source(component_id: &str, props: &JsonValue) -> Result<String, String> {
    let mode = string_prop(props, "mode").unwrap_or_else(|| "table".into());
    if mode == "sql" {
        let sql = string_prop(props, "sql")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| format!("{}: SQL query is empty", component_id))?;
        return Ok(format!("({})", sql));
    }
    if mode == "incremental" {
        return Err(format!(
            "{}: incremental read mode isn't implemented yet",
            component_id
        ));
    }
    let table = string_prop(props, "tableName")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{}: table name is required", component_id))?;
    let schema = string_prop(props, "schemaName").filter(|s| !s.is_empty());
    Ok(format!(
        "SELECT * FROM {}",
        relational_qualified("duckle_src", component_id, schema.as_deref(), &table)
    ))
}

/// Sink for a network relational DB (Postgres / Cockroach / MySQL /
/// MariaDB). Only `overwrite` (DROP + CREATE) is wired today; append /
/// upsert / truncate / error-if-exists error loudly rather than
/// pretending to apply. Writes inside the ATTACHed `duckle_dst` DB.
fn build_relational_sink(
    component_id: &str,
    props: &JsonValue,
    from_view: &str,
) -> Result<String, EngineError> {
    let table = string_prop(props, "tableName")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| EngineError::Config(format!("{}: table name is required", component_id)))?;
    let schema = string_prop(props, "schemaName").filter(|s| !s.is_empty());
    let mode = string_prop(props, "mode").unwrap_or_else(|| "overwrite".into());
    let qual = relational_qualified("duckle_dst", component_id, schema.as_deref(), &table);
    match mode.as_str() {
        "overwrite" => Ok(format!(
            "DROP TABLE IF EXISTS {q}; CREATE TABLE {q} AS (SELECT * FROM {from})",
            q = qual,
            from = quote_ident(from_view)
        )),
        // Append inserts into an existing table; the table must already
        // exist (create-if-missing isn't wired yet because we don't know
        // the upstream's column types ahead of time without inspecting).
        "append" => Ok(format!(
            "INSERT INTO {q} SELECT * FROM {from}",
            q = qual,
            from = quote_ident(from_view)
        )),
        // Truncate keeps the table's existing schema (and any indexes /
        // grants on it) and replaces just the rows. Useful when the
        // table is referenced by downstream views or foreign keys.
        "truncate" => Ok(format!(
            "TRUNCATE TABLE {q}; INSERT INTO {q} SELECT * FROM {from}",
            q = qual,
            from = quote_ident(from_view)
        )),
        other => Err(EngineError::Config(format!(
            "{}: write mode '{}' isn't implemented yet (use 'overwrite', 'append', or 'truncate')",
            component_id, other
        ))),
    }
}

/// Qualify a table reference under the right naming depth for each
/// network DB family. Postgres / Cockroach use catalog.schema.table
/// (default schema `public`); MotherDuck is DuckDB-native and uses
/// catalog.schema.table with default schema `main`; MySQL / MariaDB
/// use catalog.table (the MySQL database is selected at ATTACH time,
/// though we honour an explicit schemaName as a 3-level qualifier).
fn relational_qualified(alias: &str, component_id: &str, schema: Option<&str>, table: &str) -> String {
    let default_schema: Option<&str> = if component_id.ends_with(".postgres")
        || component_id.ends_with(".cockroach")
    {
        Some("public")
    } else if component_id.ends_with(".motherduck") {
        Some("main")
    } else {
        None // MySQL / MariaDB: skip the schema layer unless given
    };
    match (schema, default_schema) {
        (Some(s), _) => format!("{}.{}.{}", alias, quote_ident(s), quote_ident(table)),
        (None, Some(d)) => format!("{}.{}.{}", alias, quote_ident(d), quote_ident(table)),
        (None, None) => format!("{}.{}", alias, quote_ident(table)),
    }
}

/// MotherDuck ATTACH. MotherDuck support is built into DuckDB itself
/// (no extension to install), so this just builds an `md:` URL with
/// an optional inline `motherduck_token` query parameter. If the token
/// isn't in the form, MotherDuck falls back to the MOTHERDUCK_TOKEN env
/// var, which lets a user keep credentials out of saved pipelines.
fn md_attach(props: &JsonValue) -> String {
    let db = match string_prop(props, "database").filter(|s| !s.is_empty()) {
        Some(d) => d,
        None => return String::new(),
    };
    let token = string_prop(props, "token").filter(|s| !s.is_empty());
    let url = match token {
        Some(t) => format!("md:{}?motherduck_token={}", db, t),
        None => format!("md:{}", db),
    };
    format!("ATTACH '{}' AS duckle_src (READ_ONLY); ", sql_escape(&url))
}

/// SQLite / DuckDB sink - write the upstream into a table inside the
/// ATTACHed `duckle_dst` database. DROP+CREATE works for both writers
/// (the SQLite writer doesn't support CREATE OR REPLACE).
fn build_db_sink(props: &JsonValue, from_view: &str) -> String {
    let table = string_prop(props, "tableName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "output".into());
    let t = quote_ident(&table);
    format!(
        "DROP TABLE IF EXISTS duckle_dst.{}; CREATE TABLE duckle_dst.{} AS (SELECT * FROM {})",
        t,
        t,
        quote_ident(from_view)
    )
}

/// Avro source. The `avro` DuckDB community extension exposes
/// `read_avro` (read-only); the LOAD is in the stage prelude so the
/// function is available before the SELECT runs.
fn build_avro_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM read_avro('{}')", sql_escape(&path))
}

/// Vector Similarity Search via the DuckDB vss extension. Adds a
/// similarity score column to each upstream row (against a fixed query
/// vector) and optionally returns only the top-K most similar rows.
/// The vector column is CAST to FLOAT[dim] so vss accepts it; the
/// target vector is embedded as an array literal (validated as a JSON
/// array of numbers at plan time).
fn build_vector_search(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs
        .main()
        .ok_or_else(|| missing_input_msg("xf.ai.vector_search"))?;
    let column = string_prop(props, "vectorColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Vector Search needs a vector column".to_string())?;
    let target = string_prop(props, "targetVector")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Vector Search needs a target vector (JSON array of floats)".to_string())?;
    let dim = props
        .get("dimension")
        .and_then(|v| v.as_u64())
        .filter(|d| *d > 0)
        .ok_or_else(|| "Vector Search needs a positive dimension".to_string())?;
    let metric = string_prop(props, "distanceMetric").unwrap_or_else(|| "cosine".into());
    let top_k = props
        .get("topK")
        .and_then(|v| v.as_u64())
        .filter(|k| *k > 0);
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "similarity_score".into());

    let vec_vals: Vec<f64> = serde_json::from_str(&target)
        .map_err(|e| format!("Vector Search: targetVector must be a JSON array of numbers ({})", e))?;
    if vec_vals.len() as u64 != dim {
        return Err(format!(
            "Vector Search: target vector has {} elements but dimension is {}",
            vec_vals.len(),
            dim
        ));
    }
    let target_literal = format!(
        "[{}]::FLOAT[{}]",
        vec_vals
            .iter()
            .map(|f| format!("{}", f))
            .collect::<Vec<_>>()
            .join(","),
        dim
    );
    let col_cast = format!("CAST({} AS FLOAT[{}])", quote_ident(&column), dim);
    let (fn_name, order_dir) = match metric.as_str() {
        "l2" | "distance" => ("array_distance", "ASC"),
        "inner_product" | "dot" => ("array_inner_product", "DESC"),
        _ => ("array_cosine_similarity", "DESC"),
    };
    let score_expr = format!("{fn_name}({col_cast}, {target_literal})");
    let mut sql = format!(
        "SELECT *, {score} AS {out} FROM {up}",
        score = score_expr,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    );
    if let Some(k) = top_k {
        sql = format!(
            "{sql} ORDER BY {out} {dir} LIMIT {k}",
            out = quote_ident(&output),
            dir = order_dir
        );
    }
    Ok(sql)
}

/// Geospatial source via the DuckDB spatial extension. ST_Read is
/// GDAL-backed, so the same builder handles GeoJSON, Shapefile,
/// GeoPackage, KML, GPX, and many more (format auto-detected by file
/// extension). The geometry column comes through as binary; downstream
/// transforms (e.g. ST_AsText) can convert it.
fn build_spatial_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM ST_Read('{}')", sql_escape(&path))
}

/// Iceberg source via the DuckDB iceberg extension's `iceberg_scan`.
/// The `path` is the iceberg table location (a local directory or an
/// `s3://...` URL backed by a cloud SECRET created elsewhere).
fn build_iceberg_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM iceberg_scan('{}')", sql_escape(&path))
}

/// Delta Lake source via the DuckDB delta extension's `delta_scan`.
fn build_delta_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM delta_scan('{}')", sql_escape(&path))
}

/// Excel (.xlsx) source via DuckDB v1.2+ `read_xlsx`. Supports an
/// optional `sheet` form field (omitted defaults to the first sheet)
/// and a `hasHeader` toggle.
fn build_excel_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let mut args = vec![format!("'{}'", sql_escape(&path))];
    if let Some(sheet) = string_prop(props, "sheet").filter(|s| !s.is_empty()) {
        args.push(format!("sheet = '{}'", sql_escape(&sheet)));
    }
    if let Some(has_header) = props.get("hasHeader").and_then(JsonValue::as_bool) {
        args.push(format!("header = {}", has_header));
    }
    format!("SELECT * FROM read_xlsx({})", args.join(", "))
}

/// Cloud sources (S3 / GCS / Azure Blob / HTTP). DuckDB's httpfs +
/// azure extensions let us read these directly via the same
/// read_csv_auto / read_parquet / read_json_auto family of functions.
/// Format is inferred from the URL extension unless the user picks one.
fn build_cloud_source(scheme: &str, props: &JsonValue) -> String {
    let path = string_prop(props, "path")
        .or_else(|| string_prop(props, "url"))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // The storage form supplies bucket + key rather than a full
            // URL; assemble one using the connector's scheme.
            let bucket = string_prop(props, "bucket").filter(|s| !s.is_empty())?;
            let key = string_prop(props, "key").unwrap_or_default();
            let prefix = match scheme {
                "s3" => "s3://",
                "gcs" => "gs://",
                "azureblob" => "az://",
                _ => "https://",
            };
            Some(format!("{}{}/{}", prefix, bucket, key.trim_start_matches('/')))
        })
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
        "snk.sqlite" | "snk.duckdb" => Ok(build_db_sink(props, from_view)),
        "snk.postgres" | "snk.cockroach" | "snk.mysql" | "snk.mariadb" => {
            build_relational_sink(component_id, props, from_view)
        }
        other => Err(EngineError::Unsupported(format!(
            "Sink '{}' is not yet implemented",
            other
        ))),
    }
}

/// Cloud sink - COPY a view out to an s3:// / gs:// / az:// URL.
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
