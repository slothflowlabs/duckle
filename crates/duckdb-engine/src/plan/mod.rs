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
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Pipeline payload sent from the frontend. Just the nodes + edges
/// directly - no wrapping metadata required for a run.
#[derive(Debug, Deserialize, Serialize)]
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
    /// Single runtime action this stage performs beyond plain DuckDB SQL:
    /// a driver source/sink, an HTTP/AI/code transform, or a control-flow
    /// side effect. None means the stage is pure SQL. Replacing the former
    /// ~61 Option<Spec> fields with one enum makes impossible states
    /// unrepresentable and keeps is_pure_sql from silently drifting.
    pub runtime: Option<RuntimeSpec>,
    /// Milliseconds the executor sleeps before running this stage.
    /// Set by ctl.wait and ctl.throttle. None = no delay.
    pub wait_ms: Option<u64>,
    /// Advanced-settings retry: total attempts (1 = no retry). The
    /// executor sleeps `retry_backoff_ms` (with linear scaling) between
    /// attempts and only retries on engine errors, not on cancellation.
    pub retry_attempts: u32,
    pub retry_backoff_ms: u64,
    /// PRAGMA memory_limit prepended to the stage SQL when set. Lets a
    /// user cap a heavy aggregation without touching the whole pipeline.
    pub memory_limit_mb: Option<u32>,
    /// True when this is a duck-family source the user set to Materialize=View.
    /// compile() upgrades it from the safe materialized TABLE to a real lazy
    /// VIEW (so a downstream WHERE / projection pushes down into the source
    /// scan) when the whole pipeline runs in one batched session and it is the
    /// sole `duckle_src` ATTACH; otherwise it stays a TABLE (#76).
    pub attach_view: bool,
}

impl Stage {
    /// True when the stage's `sql` field is the full unit of work - the
    /// executor would run it via the bare `duckdb.exe -c` branch with no
    /// pre/post Rust-side helper. Used by the batched executor to decide
    /// whether a pipeline can be collapsed into a single CLI spawn.
    ///
    /// Keep this in sync with the spec/hook fields above: any new
    /// driver-based source or sink should add itself here so it forces
    /// the per-stage path.
    pub fn is_pure_sql(&self) -> bool {
        self.runtime.is_none()
    }
}

/// The single non-SQL action a Stage performs (or None for pure SQL).
/// Terminal variants (sources / sinks / transforms) replace the stage's
/// SQL run in the executor; control-flow variants (RunJob / Iterate /
/// Foreach / InstallFallback) run as a side effect and then fall through to
/// the stage's pass-through SQL.
#[derive(Debug)]
pub enum RuntimeSpec {
    Upsert(UpsertSpec),
    TextSearch(TextSearchSpec),
    /// Parent -> child job call (ctl.runpipeline / ctl.trigger / ctl.runjob).
    /// `vars` are substituted as ${KEY} into the child before it runs.
    RunJob {
        path: String,
        vars: Vec<(String, String)>,
    },
    InstallFallback(String),
    Iterate { path: String, count: u64 },
    /// Run `path` once per upstream row. `concurrency` > 1 runs the per-row
    /// children in bounded concurrent waves (each in its own temp DB); 1 is
    /// the default sequential behaviour.
    Foreach { path: String, concurrency: usize },
    Parallelize(ParallelizeSpec),
    /// ctl.log / ctl.warn: emit a log line at `level` ("info" / "warn")
    /// then pass the upstream through. `{rows}` in the message is replaced
    /// with the upstream row count.
    Log { level: String, message: String },
    /// ctl.die: fail the run with `message` when `condition` holds against
    /// the upstream row count ("always" / "has-rows" / "no-rows").
    Die { message: String, condition: String },
    /// xf.incremental: watermark-based incremental load (see IncrementalSpec).
    Incremental(IncrementalSpec),
    /// src.ducklake.changes: DuckLake change-data-feed source (see DuckLakeCdcSpec).
    DuckLakeCdc(DuckLakeCdcSpec),
    Webhook(WebhookSpec),
    SnowflakeSink(SnowflakeSinkSpec),
    DatabricksSink(DatabricksSinkSpec),
    SnowflakeSource(SnowflakeSourceSpec),
    DatabricksSource(DatabricksSourceSpec),
    RestSource(RestSourceSpec),
    ElasticSource(ElasticSourceSpec),
    MongoSink(MongoSinkSpec),
    MongoSource(MongoSourceSpec),
    ClickhouseSink(ClickHouseSinkSpec),
    ClickhouseSource(ClickHouseSourceSpec),
    SqlserverSink(SqlServerSinkSpec),
    SqlserverSource(SqlServerSourceSpec),
    CassandraSink(CassandraSinkSpec),
    CassandraSource(CassandraSourceSpec),
    OracleSink(OracleSinkSpec),
    OracleSource(OracleSourceSpec),
    AdbcSource(AdbcSourceSpec),
    AttachParquetSource(AttachParquetSourceSpec),
    /// materialize = "duckdb"/"duckdbfile": persist the stage into a DuckDB file.
    MaterializeDuckDb(MaterializeDuckDbSpec),
    RedisSink(RedisSinkSpec),
    RedisSource(RedisSourceSpec),
    QdrantSource(QdrantSourceSpec),
    WeaviateSource(WeaviateSourceSpec),
    MilvusSource(MilvusSourceSpec),
    FormatSource(FormatFileSourceSpec),
    FormatSink(FormatFileSinkSpec),
    KafkaSink(KafkaSinkSpec),
    KafkaSource(KafkaSourceSpec),
    AvroSource(AvroSourceSpec),
    NatsSink(NatsSinkSpec),
    NatsSource(NatsSourceSpec),
    PubsubSink(PubSubSinkSpec),
    PubsubSource(PubSubSourceSpec),
    XmlSource(XmlSourceSpec),
    XmlSink(XmlSinkSpec),
    AvroSink(AvroSinkSpec),
    RabbitSink(RabbitSinkSpec),
    RabbitSource(RabbitSourceSpec),
    GitSource(GitSourceSpec),
    Shell(ShellSpec),
    /// xf.dbt: run a dbt Core project against the run database (see DbtSpec).
    Dbt(DbtSpec),
    FtpSource(FtpSourceSpec),
    SftpSource(SftpSourceSpec),
    FtpSink(FtpSinkSpec),
    SftpSink(SftpSinkSpec),
    ClipboardSource(ClipboardSourceSpec),
    EmailSource(EmailSourceSpec),
    EmailSink(EmailSinkSpec),
    WebhookSource(WebhookSourceSpec),
    DynamodbSource(DynamoDbSourceSpec),
    KinesisSource(KinesisSourceSpec),
    AiEmbed(AiEmbedSpec),
    Wasm(WasmSpec),
    Javascript(JavaScriptSpec),
    AiChunk(AiChunkSpec),
    AiPii(AiPiiSpec),
    AiLlm(AiLlmSpec),
    AiClassify(AiClassifySpec),
    AiDedupe(AiDedupeSpec),
}

// Connector / transform spec type definitions live in plan/specs.rs and
// are re-exported here so the rest of the planner (and lib.rs) keep using
// plain `plan::XxxSpec` paths.
mod specs;
pub use specs::*;

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

/// Remote / catalog sources that, when exactly one stage consumes them, take
/// the COPY-to-parquet fast path instead of a run-db table insert (see
/// build_stage). At module scope so the consumer-count pass can avoid
/// penalising them: their rows are already materialized once to a local
/// parquet, so a reject-split downstream re-reads that cheap file, not the
/// remote, and must not count as two consumers.
const ATTACH_PARQUET_SOURCES: &[&str] = &[
    "src.postgres",
    "src.cockroach",
    "src.pgvector",
    "src.redshift",
    "src.mysql",
    "src.mariadb",
    "src.motherduck",
    "src.bigquery",
    "src.quack",
    "src.ducklake",
    "src.iceberg",
    "src.delta",
];

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
    // Also count consumers per (source_node, source_handle) so we know
    // when it's safe to emit a CREATE VIEW (lazy) vs CREATE TABLE
    // (materialized). A node with exactly one downstream consumer can
    // be a view: DuckDB inlines it into the single downstream query,
    // gets predicate / projection pushdown into the source read, and
    // skips an intermediate materialize-to-disk. A node with multiple
    // consumers gets materialized so each consumer reads it once
    // instead of re-evaluating the chain.
    // A node whose reject output is wired (a filter / quality validator with
    // its reject port connected) reads its main input TWICE: once for the pass
    // body (`... WHERE pred`) and once for the reject body (`... WHERE NOT
    // pred`) - see build_quality / build_filter. Count such a consumer as two
    // so the upstream materializes as a TABLE and an expensive source (e.g.
    // read_json_auto) is scanned once instead of re-evaluated for each side.
    let mut reject_wired: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for edge in &data_edges {
        if matches!(edge.source_handle.as_deref(), Some("reject") | Some("filter")) {
            reject_wired.insert(edge.source.as_str());
        }
    }
    let mut consumer_count: HashMap<String, usize> = HashMap::new();
    for edge in &data_edges {
        let port = edge
            .target_handle
            .as_deref()
            .unwrap_or("main");
        let port_key = canonical_port(port);
        // Resolve which materialized table this edge actually reads, based
        // on the SOURCE node's output handle (main vs reject).
        let source_ref = output_table_ref(&edge.source, edge.source_handle.as_deref());
        // Don't double-count an attach-parquet source: its rows are already
        // materialized once to a local parquet, so a reject-split downstream
        // re-reads that cheap file (not the remote). Counting it as two would
        // only knock it off the COPY-to-parquet fast path for no read savings.
        let upstream_is_attach_parquet = node_index
            .get(edge.source.as_str())
            .and_then(|n| n.data.component_id.as_deref())
            .map(|cid| ATTACH_PARQUET_SOURCES.contains(&cid))
            .unwrap_or(false);
        let weight = if port_key == "main"
            && reject_wired.contains(edge.target.as_str())
            && !upstream_is_attach_parquet
        {
            2
        } else {
            1
        };
        *consumer_count.entry(source_ref.clone()).or_insert(0) += weight;
        inputs
            .entry(edge.target.as_str())
            .or_default()
            .ports
            .entry(port_key.to_string())
            .or_default()
            .push(source_ref);
    }

    // Propagate "known output columns" through the DAG so passthrough
    // transforms (filter, sort, limit, fill, cast itself) can validate
    // their column references at planner time. Sources contribute their
    // declared schema (only present when the user ran Autodetect or
    // hand-typed a Schema panel). Transforms that don't change the
    // column set propagate the parent set as-is; transforms that do
    // (project, rename, drop, joins, aggregations) reset the set to
    // None so downstream nodes don't validate against stale info.
    //
    // Validation degrades gracefully: if upstream schema is unknown we
    // skip the check and let DuckDB raise its native "column not
    // found" at run time. Worst case is the user's old experience -
    // no regression.
    let mut known_columns: HashMap<String, Option<HashSet<String>>> = HashMap::new();
    for node_id in &order {
        let node = match node_index.get(node_id.as_str()) {
            Some(n) => *n,
            None => continue,
        };
        let upstream_set = inputs
            .get(node_id.as_str())
            .and_then(|ni| ni.main())
            .and_then(|src| {
                // src looks like "node_id" or "node_id__reject" - the
                // known_columns map keys by node id directly.
                let src_node = strip_reject_suffix(src);
                known_columns.get(src_node).cloned()
            })
            .flatten();
        let derived = derive_output_columns(
            node.data.component_id.as_deref(),
            node.data.properties.as_ref(),
            node.data.schema.as_deref(),
            upstream_set.as_ref(),
        );
        known_columns.insert(node.id.clone(), derived);
    }

    // ctl.parallelize: extract each node's independent downstream branches
    // into sub-pipelines that run concurrently, and exclude those branch
    // nodes from the main (sequential) plan so they don't also run inline.
    let mut excluded: HashSet<String> = HashSet::new();
    let mut parallelize_specs: HashMap<String, ParallelizeSpec> = HashMap::new();
    for node in &pipeline.nodes {
        if node.data.component_id.as_deref() == Some("ctl.parallelize")
            && !node.data.disabled.unwrap_or(false)
        {
            let (spec, branch_nodes) =
                build_parallelize_branches(node, &pipeline.nodes, &data_edges)?;
            for bn in branch_nodes {
                if !excluded.insert(bn.clone()) {
                    return Err(EngineError::Config(format!(
                        "node '{}' belongs to more than one ctl.parallelize",
                        bn
                    )));
                }
            }
            parallelize_specs.insert(node.id.clone(), spec);
        }
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
        // Nodes pulled into a ctl.parallelize branch run inside that branch's
        // sub-pipeline, not in the main sequential plan.
        if excluded.contains(node_id.as_str()) {
            continue;
        }
        let empty = NodeInputs::default();
        let node_inputs = inputs.get(node_id.as_str()).unwrap_or(&empty);
        // Validate column references against the upstream's known set.
        // Errors here propagate as compile errors with a clear stage-
        // tagged message - no need to wait for DuckDB's runtime error.
        let upstream_cols = node_inputs
            .main()
            .map(strip_reject_suffix)
            .and_then(|src| known_columns.get(src).and_then(|x| x.as_ref()));
        if let Some(cols) = upstream_cols {
            validate_column_refs(component_id, node.data.properties.as_ref(), cols)
                .map_err(|msg| {
                    EngineError::Config(format!(
                        "{} ({} / {}): {}",
                        node.data.label, component_id, node.id, msg
                    ))
                })?;
        }
        // Fail loud on fan-in to a single input port. Every component
        // except Union / set ops reads its primary input via .main()
        // (which only ever sees the first edge), so a second edge wired
        // into the same `main` port is silently dropped - real data loss.
        // Union / intersect / except legitimately take multiple `main`
        // edges (all_main_ports), so they're exempt.
        if !is_multi_main_component(component_id) {
            if let Some(mains) = node_inputs.ports.get("main") {
                if mains.len() > 1 {
                    return Err(EngineError::Config(format!(
                        "{} ({} / {}): {} inputs are wired into this node's single input port, but only one is read - the rest would be silently dropped. Insert a Union to merge upstreams, or use a Join/Diff lookup port.",
                        node.data.label, component_id, node.id, mains.len()
                    )));
                }
            }
        }
        // Same data-loss guard for lookup ports: join / diff / scd / upsert read
        // a single lookup via first_lookup(), so a second lookup edge would be
        // silently dropped. xf.map (tMap) is exempt - it reads every configured
        // lookup port.
        if !is_multi_lookup_component(component_id) {
            let lookups: usize = node_inputs
                .ports
                .iter()
                .filter(|(k, _)| k.starts_with("lookup"))
                .map(|(_, v)| v.len())
                .sum();
            if lookups > 1 {
                return Err(EngineError::Config(format!(
                    "{} ({} / {}): {} inputs are wired into this node's lookup port, but only one is read - the rest would be silently dropped. Union them first, or use a Map node for multiple lookups.",
                    node.data.label, component_id, node.id, lookups
                )));
            }
        }
        let mut stage = build_stage(node, component_id, node_inputs, &consumer_count)?;
        if let Some(spec) = parallelize_specs.remove(node_id) {
            stage.runtime = Some(RuntimeSpec::Parallelize(spec));
        }
        stages.push(stage);
    }

    // #76: a duck-family source set to Materialize=View becomes a real lazy
    // VIEW so a downstream WHERE / projection pushes down into the source scan
    // (the whole point of choosing View) instead of being materialized.
    //
    // A VIEW over the process-local `duckle_src` alias only survives when (a)
    // every stage runs in one batched single-session invocation and (b) the
    // alias is not detached/reused between stages. So upgrade ONLY when the
    // pipeline is provably batchable AND this is the sole duckle_src ATTACH;
    // otherwise the source stays the safe materialized TABLE it was built as.
    // The batchable condition here is a strict subset of the executor's
    // `batchable` check (compile() is the no-target path), so whenever we
    // upgrade, the executor is guaranteed to take the single-session path.
    if stages.iter().any(|s| s.attach_view) {
        let would_batch = stages.len() >= 2
            && stages.iter().all(|s| {
                s.is_pure_sql()
                    && s.retry_attempts <= 1
                    && s.wait_ms.is_none()
                    && s.memory_limit_mb.is_none()
                    && s.sink_mode.as_deref() != Some("error")
            });
        let duckle_src_sources = stages
            .iter()
            .filter(|s| s.sql.contains("AS duckle_src"))
            .count();
        if would_batch && duckle_src_sources == 1 {
            for s in stages.iter_mut().filter(|s| s.attach_view) {
                // TABLE -> VIEW so the consumer inlines it and pushes predicates
                // into the ducklake / duckdb / postgres scan.
                if let Some(p) = s.sql.find("CREATE OR REPLACE TABLE ") {
                    s.sql
                        .replace_range(p..p + "CREATE OR REPLACE TABLE ".len(), "CREATE OR REPLACE VIEW ");
                }
                // Keep duckle_src ATTACHed for the downstream stage: drop the
                // trailing "DETACH duckle_src;" the source appended, or the view
                // would dangle when the consumer reads it.
                if let Some(d) = s.sql.rfind("DETACH duckle_src") {
                    s.sql.truncate(d);
                    while s.sql.ends_with(' ') || s.sql.ends_with(';') {
                        s.sql.pop();
                    }
                }
            }
        }
    }

    // Leaves = data-flow nodes that nothing else (still in the plan) consumes
    // from. Edges into excluded parallelize-branch nodes don't count, so a
    // parallelize node whose only consumers are its branches stays a leaf.
    let has_downstream: HashSet<&str> = data_edges
        .iter()
        .filter(|e| !excluded.contains(e.target.as_str()))
        .map(|e| e.source.as_str())
        .collect();
    let leaves: Vec<String> = order
        .iter()
        .filter(|id| !excluded.contains(id.as_str()) && !has_downstream.contains(id.as_str()))
        .cloned()
        .collect();

    Ok(CompiledPipeline { stages, leaves })
}

mod graph;
use graph::*;

/// Key columns for a sink's "upsert" write mode, or empty for plain insert.
/// Driver sinks (SQL Server / Oracle / Snowflake / Databricks) MERGE on these
/// when the form sets `mode = "upsert"` and supplies `conflictColumns`.
fn upsert_keys_from(props: &JsonValue) -> Vec<String> {
    if string_prop(props, "mode").as_deref() == Some("upsert") {
        columns_list(props, "conflictColumns")
    } else {
        Vec::new()
    }
}

/// Delete-propagation control column for a sink's "upsert" write mode. When
/// the form sets `mode = "upsert"` and a `deleteColumn`, rows whose value in
/// that column equals `deleteValue` are removed from the target by key instead
/// of being upserted - this is how CDC deletes (xf.cdc.diff change_type /
/// DuckLake CDC) flow through. Returns None outside upsert mode or when unset.
fn delete_column_from(props: &JsonValue) -> Option<String> {
    if string_prop(props, "mode").as_deref() == Some("upsert") {
        string_prop(props, "deleteColumn").filter(|s| !s.is_empty())
    } else {
        None
    }
}

/// The value in `deleteColumn` that marks a row for deletion (default
/// "delete", matching xf.cdc.diff's change_type tag).
fn delete_value_from(props: &JsonValue) -> String {
    string_prop(props, "deleteValue")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "delete".into())
}

fn build_stage(
    node: &PipelineNode,
    component_id: &str,
    inputs: &NodeInputs,
    consumer_count: &HashMap<String, usize>,
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
    let mut text_search: Option<TextSearchSpec> = None;
    let mut webhook: Option<WebhookSpec> = None;
    let mut run_job: Option<(String, Vec<(String, String)>)> = None;
    let mut install_fallback_path: Option<String> = None;
    let mut iterate_pipeline_path: Option<String> = None;
    let mut iterate_count: Option<u64> = None;
    let mut foreach_pipeline_path: Option<String> = None;
    let mut foreach_concurrency: usize = 1;
    // (level, message) for ctl.log / ctl.warn; (message, condition) for ctl.die.
    let mut log_spec: Option<(String, String)> = None;
    let mut die_spec: Option<(String, String)> = None;
    let mut incremental: Option<IncrementalSpec> = None;
    let mut ducklake_cdc: Option<DuckLakeCdcSpec> = None;
    let mut snowflake_sink: Option<SnowflakeSinkSpec> = None;
    let mut databricks_sink: Option<DatabricksSinkSpec> = None;
    let mut snowflake_source: Option<SnowflakeSourceSpec> = None;
    let mut databricks_source: Option<DatabricksSourceSpec> = None;
    let mut rest_source: Option<RestSourceSpec> = None;
    let mut elastic_source: Option<ElasticSourceSpec> = None;
    let mut mongo_sink: Option<MongoSinkSpec> = None;
    let mut mongo_source: Option<MongoSourceSpec> = None;
    let mut clickhouse_sink: Option<ClickHouseSinkSpec> = None;
    let mut clickhouse_source: Option<ClickHouseSourceSpec> = None;
    let mut sqlserver_sink: Option<SqlServerSinkSpec> = None;
    let mut sqlserver_source: Option<SqlServerSourceSpec> = None;
    let mut cassandra_sink: Option<CassandraSinkSpec> = None;
    let mut cassandra_source: Option<CassandraSourceSpec> = None;
    let mut oracle_sink: Option<OracleSinkSpec> = None;
    let mut oracle_source: Option<OracleSourceSpec> = None;
    let mut adbc_source: Option<AdbcSourceSpec> = None;
    let mut attach_parquet_source: Option<AttachParquetSourceSpec> = None;
    let mut materialize_duckdb: Option<MaterializeDuckDbSpec> = None;
    let mut redis_sink: Option<RedisSinkSpec> = None;
    let mut redis_source: Option<RedisSourceSpec> = None;
    let mut qdrant_source: Option<QdrantSourceSpec> = None;
    let mut weaviate_source: Option<WeaviateSourceSpec> = None;
    let mut milvus_source: Option<MilvusSourceSpec> = None;
    let mut format_source: Option<FormatFileSourceSpec> = None;
    let mut format_sink: Option<FormatFileSinkSpec> = None;
    let mut kafka_sink: Option<KafkaSinkSpec> = None;
    let mut kafka_source: Option<KafkaSourceSpec> = None;
    let mut avro_source: Option<AvroSourceSpec> = None;
    let mut nats_sink: Option<NatsSinkSpec> = None;
    let mut nats_source: Option<NatsSourceSpec> = None;
    let mut pubsub_sink: Option<PubSubSinkSpec> = None;
    let mut pubsub_source: Option<PubSubSourceSpec> = None;
    let mut xml_source: Option<XmlSourceSpec> = None;
    let mut xml_sink: Option<XmlSinkSpec> = None;
    let mut avro_sink: Option<AvroSinkSpec> = None;
    let mut rabbit_sink: Option<RabbitSinkSpec> = None;
    let mut rabbit_source: Option<RabbitSourceSpec> = None;
    let mut git_source: Option<GitSourceSpec> = None;
    let mut shell: Option<ShellSpec> = None;
    let mut dbt: Option<DbtSpec> = None;
    let mut ftp_source: Option<FtpSourceSpec> = None;
    let mut sftp_source: Option<SftpSourceSpec> = None;
    let mut ftp_sink: Option<FtpSinkSpec> = None;
    let mut sftp_sink: Option<SftpSinkSpec> = None;
    let mut clipboard_source: Option<ClipboardSourceSpec> = None;
    let mut email_source: Option<EmailSourceSpec> = None;
    let mut email_sink: Option<EmailSinkSpec> = None;
    let mut webhook_source: Option<WebhookSourceSpec> = None;
    let mut dynamodb_source: Option<DynamoDbSourceSpec> = None;
    let mut kinesis_source: Option<KinesisSourceSpec> = None;
    let mut ai_embed: Option<AiEmbedSpec> = None;
    let mut wasm: Option<WasmSpec> = None;
    let mut javascript: Option<JavaScriptSpec> = None;
    let mut ai_chunk: Option<AiChunkSpec> = None;
    let mut ai_pii: Option<AiPiiSpec> = None;
    let mut ai_llm: Option<AiLlmSpec> = None;
    let mut ai_classify: Option<AiClassifySpec> = None;
    let mut ai_dedupe: Option<AiDedupeSpec> = None;
    let mut wait_ms: Option<u64> = None;
    // Advanced settings (universal across components, written by the
    // Properties Panel's Advanced tab). Engine honours them per stage.
    let retry_attempts = props
        .get("retryAttempts")
        .and_then(|v| v.as_u64())
        .map(|n| n.max(1) as u32)
        .unwrap_or(1);
    let retry_backoff_ms = props
        .get("retryBackoffMs")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let memory_limit_mb = props
        .get("memoryLimitMb")
        .and_then(|v| v.as_u64())
        .filter(|n| *n > 0)
        .map(|n| n as u32);
    // ATTACH statements for external-DB nodes (DuckDB/SQLite/relational).
    // The prelude uses fixed aliases (duckle_src / duckle_dst). In batched
    // mode every pure-SQL stage shares ONE DuckDB connection, so two
    // attach-backed stages would each ATTACH the same alias and the second
    // fails with `database with name "duckle_src" already exists`. Each
    // attach-backed stage copies its rows into <node> (downstream never
    // reads the alias - see the materialize-as-TABLE note below), so we
    // DETACH the alias at the end of the stage (further down) to free it for
    // the next stage's ATTACH.
    let attach = attach_prelude(component_id, &props);
    let attach_alias: Option<&str> = if attach.contains("AS duckle_src") {
        Some("duckle_src")
    } else if attach.contains("AS duckle_dst") {
        Some("duckle_dst")
    } else {
        None
    };
    // #76: set by the generic source/view branch below when this is a
    // single-consumer attach-backed source the user marked Materialize=View,
    // making it eligible for the lazy-VIEW upgrade in compile().
    let mut attach_view = false;
    let (mut sql, kind, from) = if component_id == "snk.graphql" {
        // GraphQL mutation: POST one request per row with the row's
        // JSON as `variables`. Rides the WebhookSpec pipeline.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let url = string_prop(&props, "url")
            .or_else(|| string_prop(&props, "endpoint"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required (GraphQL endpoint)", component_id)))?;
        let mutation = string_prop(&props, "mutation")
            .or_else(|| string_prop(&props, "query"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: mutation (GraphQL document) required", component_id)))?;
        let mut headers = headers_from_props(&props);
        push_rest_auth(&mut headers, &props);
        // body_extras puts the mutation alongside the variables (batch
        // mode wraps the row array as 'variables').
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method: "POST".into(),
            headers,
            body_shape: "batch".into(),
            body_wrap: Some("variables".into()),
            body_extras: vec![("query".into(), serde_json::Value::String(mutation))],
            bulk_action: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.webhook" || component_id == "snk.rest" {
        // HTTP sink. Stage SQL stays empty; the executor materializes
        // the upstream view, then dispatches one ureq request per row
        // (body_shape='row') or one batched request (body_shape='batch').
        let from_view = inputs
            .main()
            .ok_or_else(|| missing_input(node, "main"))?;
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        let method = string_prop(&props, "method")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "POST".into())
            .to_uppercase();
        // Prefer bodyShape (engine-native), fall back to batchMode
        // (form-native): 'one' -> per-row, 'array' -> batched.
        let body_shape = string_prop(&props, "bodyShape")
            .filter(|s| !s.is_empty())
            .or_else(|| {
                string_prop(&props, "batchMode").map(|m| match m.as_str() {
                    "array" => "batch".into(),
                    _ => "row".into(),
                })
            })
            .unwrap_or_else(|| if component_id == "snk.webhook" { "row".into() } else { "batch".into() });
        let mut headers = headers_from_props(&props);
        // Translate the form's authType + authToken into a header so
        // the executor doesn't need to know about auth shapes.
        push_rest_auth(&mut headers, &props);
        let body_wrap = string_prop(&props, "bodyWrap").filter(|s| !s.is_empty());
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method,
            headers,
            body_shape,
            body_wrap,
            body_extras: Vec::new(),
            bulk_action: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.pinecone" {
        // Pinecone vector upsert. Form fields: indexHost (e.g.
        // 'idx-abc123.svc.us-east1-gcp.pinecone.io'), apiKey, vectorColumn,
        // idColumn. The engine builds the {vectors: [...]} body that the
        // /vectors/upsert endpoint expects and sets the Api-Key header.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let host = string_prop(&props, "indexHost")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: indexHost required (e.g. 'idx-abc123.svc.us-east1-gcp.pinecone.io')", component_id)))?;
        let api_key = string_prop(&props, "apiKey").unwrap_or_default();
        let url = format!("https://{}/vectors/upsert", host.trim_start_matches("https://"));
        let mut headers = headers_from_props(&props);
        if !api_key.is_empty() {
            headers.push(("Api-Key".into(), api_key));
        }
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method: "POST".into(),
            headers,
            body_shape: "batch".into(),
            body_wrap: Some("vectors".into()),
            body_extras: Vec::new(),
            bulk_action: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.qdrant" {
        // Qdrant points upsert. Form fields: clusterUrl (e.g.
        // 'https://xyz-east1.aws.cloud.qdrant.io:6333'), collection,
        // apiKey. Body shape: {points: [...]}; upsert is PUT to
        // /collections/{collection}/points.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let cluster = string_prop(&props, "clusterUrl")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: clusterUrl required", component_id)))?;
        let collection = string_prop(&props, "collection")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: collection required", component_id)))?;
        let api_key = string_prop(&props, "apiKey").unwrap_or_default();
        let url = format!(
            "{}/collections/{}/points",
            cluster.trim_end_matches('/'),
            collection
        );
        let mut headers = headers_from_props(&props);
        if !api_key.is_empty() {
            headers.push(("api-key".into(), api_key));
        }
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method: "PUT".into(),
            headers,
            body_shape: "batch".into(),
            body_wrap: Some("points".into()),
            body_extras: Vec::new(),
            bulk_action: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.weaviate" {
        // Weaviate batch objects endpoint:
        //   POST {endpoint}/v1/batch/objects
        //   { "objects": [ { class, properties, vector }, ... ] }
        // Auth via Bearer token (apiKey) when supplied.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required (e.g. 'https://my-cluster.weaviate.network')", component_id)))?;
        let api_key = string_prop(&props, "apiKey").unwrap_or_default();
        let url = format!("{}/v1/batch/objects", endpoint.trim_end_matches('/'));
        let mut headers = headers_from_props(&props);
        if !api_key.is_empty() {
            headers.push(("Authorization".into(), format!("Bearer {}", api_key)));
        }
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method: "POST".into(),
            headers,
            body_shape: "batch".into(),
            body_wrap: Some("objects".into()),
            body_extras: Vec::new(),
            bulk_action: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.milvus" {
        // Milvus REST insert:
        //   POST {endpoint}/v1/vector/insert
        //   { "collectionName": "...", "data": [ {id, vector, ...}, ... ] }
        // body_extras puts the collectionName next to data.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required", component_id)))?;
        let collection = string_prop(&props, "collection")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: collection required", component_id)))?;
        let api_key = string_prop(&props, "apiKey").unwrap_or_default();
        let url = format!("{}/v1/vector/insert", endpoint.trim_end_matches('/'));
        let mut headers = headers_from_props(&props);
        if !api_key.is_empty() {
            headers.push(("Authorization".into(), format!("Bearer {}", api_key)));
        }
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method: "POST".into(),
            headers,
            body_shape: "batch".into(),
            body_wrap: Some("data".into()),
            body_extras: vec![(
                "collectionName".into(),
                serde_json::Value::String(collection),
            )],
            bulk_action: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.databricks" {
        // Databricks SQL Statement Execution API sink. PAT Bearer auth
        // (standard for Databricks). Engine batches into multi-row
        // INSERTs at batchSize rows each, identifiers backtick-quoted.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let workspace = string_prop(&props, "workspace")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: workspace required (e.g. 'dbc-xxxx.cloud.databricks.com')", component_id)))?;
        let pat = string_prop(&props, "pat")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: pat (Personal Access Token) required", component_id)))?;
        let warehouse_id = string_prop(&props, "warehouseId")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: warehouseId required", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        databricks_sink = Some(DatabricksSinkSpec {
            from_view: from_view.to_string(),
            workspace,
            endpoint: string_prop(&props, "endpoint").filter(|s| !s.is_empty()),
            pat,
            warehouse_id,
            catalog: string_prop(&props, "catalog").filter(|s| !s.is_empty()),
            schema: string_prop(&props, "schema").filter(|s| !s.is_empty()),
            table,
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
            wait_timeout_seconds: props
                .get("waitTimeoutSeconds")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n <= 50) // Databricks max is 50s
                .unwrap_or(30),
            upsert_keys: upsert_keys_from(&props),
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.oracle" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let connect = string_prop(&props, "connect")
            .or_else(|| string_prop(&props, "connectionString"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: connect required (host:port/service_name)", component_id)))?;
        let user = string_prop(&props, "user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: user required", component_id)))?;
        let password = string_prop(&props, "password").unwrap_or_default();
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        oracle_sink = Some(OracleSinkSpec {
            from_view: from_view.to_string(),
            connect,
            user,
            password,
            schema: string_prop(&props, "schema").filter(|s| !s.is_empty()),
            table,
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000) as usize,
            upsert_keys: upsert_keys_from(&props),
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.redis" {
        // Redis SET sink. keyColumn picks the column whose value
        // becomes the Redis key; valueColumn (optional) picks the
        // payload column; if absent, the whole row is JSON-stringified
        // as the value. Optional ttlSeconds adds an EXPIRE.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let url = string_prop(&props, "url")
            .or_else(|| string_prop(&props, "connectionString"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required (e.g. redis://default:pass@host:6379/0)", component_id)))?;
        let key_column = string_prop(&props, "keyColumn")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: keyColumn required", component_id)))?;
        redis_sink = Some(RedisSinkSpec {
            from_view: from_view.to_string(),
            url,
            key_column,
            value_column: string_prop(&props, "valueColumn").unwrap_or_default(),
            ttl_seconds: props.get("ttlSeconds").and_then(|v| v.as_u64()).unwrap_or(0),
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.cassandra" || component_id == "snk.scylla" {
        // ScyllaDB shares CQL with Cassandra; same driver, same executor.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let contact_points = string_prop(&props, "contactPoints")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: contactPoints required (comma-separated host:port)", component_id)))?;
        let keyspace = string_prop(&props, "keyspace")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: keyspace required", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        cassandra_sink = Some(CassandraSinkSpec {
            from_view: from_view.to_string(),
            contact_points,
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            keyspace,
            table,
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.sqlserver" || component_id == "snk.synapse" {
        // Synapse rides the SQL Server wire; same tiberius path.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let user = string_prop(&props, "user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: user required", component_id)))?;
        let password = string_prop(&props, "password").unwrap_or_default();
        let database = string_prop(&props, "database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: database required", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        sqlserver_sink = Some(SqlServerSinkSpec {
            from_view: from_view.to_string(),
            host,
            // Range-check before the u16 cast like the other port parsers; a
            // value >= 65536 would otherwise wrap (e.g. 70000 -> 4464) and dial
            // the wrong port. Out-of-range falls back to the 1433 default.
            port: props
                .get("port")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n < 65536)
                .map(|n| n as u16)
                .unwrap_or(1433),
            user,
            password,
            database,
            schema: string_prop(&props, "schema").unwrap_or_else(|| "dbo".into()),
            table,
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000) as usize,
            trust_cert: props.get("trustCert").and_then(|v| v.as_bool()).unwrap_or(false),
            upsert_keys: upsert_keys_from(&props),
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.clickhouse" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required (e.g. 'http://localhost:8123')", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        clickhouse_sink = Some(ClickHouseSinkSpec {
            from_view: from_view.to_string(),
            endpoint,
            database: string_prop(&props, "database").filter(|s| !s.is_empty()),
            table,
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(10000) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.mongodb" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let uri = string_prop(&props, "uri")
            .or_else(|| string_prop(&props, "connectionString"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: uri required (mongodb://...)", component_id)))?;
        let database = string_prop(&props, "database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: database required", component_id)))?;
        let collection = string_prop(&props, "collection")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: collection required", component_id)))?;
        mongo_sink = Some(MongoSinkSpec {
            from_view: from_view.to_string(),
            uri,
            database,
            collection,
            mode: string_prop(&props, "mode").unwrap_or_else(|| "insert".into()),
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
            upsert_keys: upsert_keys_from(&props),
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.snowflake" {
        // Snowflake SQL API sink. Supports two auth modes:
        //   - 'pat': Bearer Personal Access Token (simple, modern)
        //   - 'jwt': RS256-signed JWT from a PEM private key (older standard)
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let account = string_prop(&props, "account")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: account required (e.g. 'xy12345.us-east-1')", component_id)))?;
        let auth_type = string_prop(&props, "authType").unwrap_or_else(|| "pat".into());
        let auth = match auth_type.as_str() {
            "jwt" => {
                let user = string_prop(&props, "user")
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| EngineError::Config(format!("{}: user required for JWT auth", component_id)))?;
                let pem = string_prop(&props, "privateKeyPem")
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        string_prop(&props, "privateKeyPath")
                            .filter(|s| !s.is_empty())
                            .and_then(|p| std::fs::read_to_string(&p).ok())
                    })
                    .ok_or_else(|| EngineError::Config(format!("{}: privateKeyPem or privateKeyPath required for JWT auth", component_id)))?;
                SnowflakeAuth::Jwt { user, private_key_pem: pem }
            }
            _ => {
                let token = string_prop(&props, "pat")
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| EngineError::Config(format!("{}: pat (Personal Access Token) required for PAT auth", component_id)))?;
                SnowflakeAuth::Pat { token }
            }
        };
        let database = string_prop(&props, "database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: database required", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        snowflake_sink = Some(SnowflakeSinkSpec {
            from_view: from_view.to_string(),
            account,
            endpoint: string_prop(&props, "endpoint").filter(|s| !s.is_empty()),
            auth,
            database,
            schema: string_prop(&props, "schema").filter(|s| !s.is_empty()),
            warehouse: string_prop(&props, "warehouse").filter(|s| !s.is_empty()),
            role: string_prop(&props, "role").filter(|s| !s.is_empty()),
            table,
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
            upsert_keys: upsert_keys_from(&props),
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.elastic" || component_id == "snk.opensearch" {
        // Elasticsearch / OpenSearch bulk API:
        //   POST {host}/{index}/_bulk
        //   action_line\n
        //   document_line\n
        //   ... (repeated, NDJSON, no trailing comma)
        // Content-Type: application/x-ndjson.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let host = string_prop(&props, "endpoint")
            .or_else(|| string_prop(&props, "host"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required", component_id)))?;
        let index = string_prop(&props, "index")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: index required", component_id)))?;
        let api_key = string_prop(&props, "apiKey").unwrap_or_default();
        let url = format!("{}/_bulk", host.trim_end_matches('/'));
        let mut headers = headers_from_props(&props);
        headers.push(("Content-Type".into(), "application/x-ndjson".into()));
        if !api_key.is_empty() {
            headers.push(("Authorization".into(), format!("ApiKey {}", api_key)));
        }
        // index action template: {"index": {"_index": "<index>"}}
        let action_line = format!("{{\"index\":{{\"_index\":\"{}\"}}}}", index.replace('"', "\\\""));
        webhook = Some(WebhookSpec {
            from_view: from_view.to_string(),
            url,
            method: "POST".into(),
            headers,
            body_shape: "ndjson_bulk".into(),
            body_wrap: None,
            body_extras: Vec::new(),
            bulk_action: Some(action_line),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.email" {
        // SMTP per-row send via lettre. host required; user/password
        // optional (for relay servers that don't require auth).
        // to/subject/body all from per-row columns so one stage can
        // send N personalized messages.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let from_address = string_prop(&props, "fromAddress")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: fromAddress required", component_id)))?;
        email_sink = Some(EmailSinkSpec {
            from_view: from_view.to_string(),
            host,
            port: props
                .get("port")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n < 65536)
                .map(|n| n as u16)
                .unwrap_or(587),
            user: string_prop(&props, "user").unwrap_or_default(),
            password: string_prop(&props, "password").unwrap_or_default(),
            from_address,
            to_column: string_prop(&props, "toColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "to".into()),
            subject_column: string_prop(&props, "subjectColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "subject".into()),
            body_column: string_prop(&props, "bodyColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "body".into()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.rabbit" {
        // RabbitMQ publisher. exchange='' means the default direct
        // exchange (route to queue named by routingKey). exchange
        // non-empty + routingKey = standard exchange routing.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        let routing_key = string_prop(&props, "routingKey")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: routingKey required", component_id)))?;
        rabbit_sink = Some(RabbitSinkSpec {
            from_view: from_view.to_string(),
            url,
            exchange: string_prop(&props, "exchange").unwrap_or_default(),
            routing_key,
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(500) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.xml" {
        // XML wrapper-element writer. Default shape:
        //   <root><row><col>val</col>...</row>...</root>
        // Custom rootElement / rowElement override the wrapper names.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        xml_sink = Some(XmlSinkSpec {
            from_view: from_view.to_string(),
            path,
            root_element: string_prop(&props, "rootElement")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "root".into()),
            row_element: string_prop(&props, "rowElement")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "row".into()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.ftp" {
        // File-transfer sink (write-side mirror of src.ftp). The Protocol
        // dropdown selects FTP, FTPS, or SFTP. The upstream view is COPY-ed to
        // a local temp file in `format`, then uploaded to `remotePath` (a full
        // remote path including filename). FTP / FTPS go through suppaftp; SFTP
        // (a different, SSH-based protocol) goes through russh + russh-sftp.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let protocol = string_prop(&props, "protocol")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let user = string_prop(&props, "user")
            .or_else(|| string_prop(&props, "username"))
            .filter(|s| !s.is_empty());
        let remote_path = string_prop(&props, "remotePath")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!("{}: remotePath required", component_id))
            })?;
        let format = string_prop(&props, "format")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "csv".into())
            .to_ascii_lowercase();
        let port = props
            .get("port")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0 && *n < 65536)
            .map(|n| n as u16);
        if protocol == "sftp" {
            sftp_sink = Some(SftpSinkSpec {
                from_view: from_view.to_string(),
                host,
                port: port.unwrap_or(22),
                user: user.ok_or_else(|| {
                    EngineError::Config(format!("{}: user required for SFTP", component_id))
                })?,
                password: string_prop(&props, "password").filter(|s| !s.is_empty()),
                private_key: string_prop(&props, "privateKey")
                    .or_else(|| {
                        string_prop(&props, "privateKeyPath")
                            .and_then(|p| std::fs::read_to_string(&p).ok())
                    })
                    .filter(|s| !s.is_empty()),
                key_passphrase: string_prop(&props, "keyPassphrase").filter(|s| !s.is_empty()),
                remote_path,
                format,
                host_fingerprint: string_prop(&props, "hostFingerprint").filter(|s| !s.is_empty()),
            });
        } else {
            ftp_sink = Some(FtpSinkSpec {
                from_view: from_view.to_string(),
                host,
                port: port.unwrap_or(21),
                user: user.unwrap_or_else(|| "anonymous".into()),
                password: string_prop(&props, "password").unwrap_or_else(|| "anonymous@".into()),
                secure: protocol == "ftps"
                    || props.get("secure").and_then(|v| v.as_bool()).unwrap_or(false),
                remote_path,
                format,
            });
        }
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.avro" {
        // Avro container-file writer. Schema either inferred from
        // the first row's columns (long / double / string / boolean)
        // or supplied verbatim as a JSON Avro schema via the
        // schemaJson field.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        avro_sink = Some(AvroSinkSpec {
            from_view: from_view.to_string(),
            path,
            schema_json: string_prop(&props, "schemaJson").unwrap_or_default(),
            record_name: string_prop(&props, "recordName")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Row".into()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.nats" {
        // NATS publisher. urls (comma-separated nats:// URLs) +
        // subject + optional subjectSuffixColumn (row column whose
        // value becomes a per-row subject suffix - subject.value).
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let urls = string_prop(&props, "urls")
            .or_else(|| string_prop(&props, "servers"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: urls required (nats://host:port,...)", component_id)))?;
        let subject = string_prop(&props, "subject")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: subject required", component_id)))?;
        nats_sink = Some(NatsSinkSpec {
            from_view: from_view.to_string(),
            urls,
            subject,
            subject_suffix_column: string_prop(&props, "subjectSuffixColumn").unwrap_or_default(),
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(500) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.pubsub" {
        // GCP Pub/Sub publish via REST. accessToken is a pre-fetched
        // OAuth2 Bearer token; sidesteps the JWT-minting + refresh
        // worker that the official client would do.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let project = string_prop(&props, "project")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: project required", component_id)))?;
        let topic = string_prop(&props, "topic")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: topic required", component_id)))?;
        let access_token = string_prop(&props, "accessToken")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: accessToken required (OAuth2 Bearer; use `gcloud auth print-access-token` to mint one)", component_id)))?;
        pubsub_sink = Some(PubSubSinkSpec {
            from_view: from_view.to_string(),
            project,
            topic,
            access_token,
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if matches!(component_id, "snk.kafka" | "snk.redpanda") {
        // Kafka producer (Redpanda speaks the Kafka wire protocol so
        // it's a pure alias). Bootstrap servers + topic + optional
        // keyColumn + partitionId. Must come before the
        // starts_with("snk.") catch-all below.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let bootstrap = string_prop(&props, "brokers")
            .or_else(|| string_prop(&props, "bootstrapServers"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: brokers required (comma-separated host:port)", component_id)))?;
        let topic = string_prop(&props, "topic")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: topic required", component_id)))?;
        kafka_sink = Some(KafkaSinkSpec {
            from_view: from_view.to_string(),
            bootstrap_servers: bootstrap,
            topic,
            partition_id: props.get("partitionId").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            key_column: string_prop(&props, "keyColumn").unwrap_or_default(),
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(500) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if matches!(component_id, "snk.yaml" | "snk.toml") {
        // Single-file YAML / TOML writer. SELECT the upstream view's
        // rows, serialize as a single doc. YAML emits a top-level
        // array; TOML wraps in a `rows` key (TOML disallows a bare
        // top-level array). MUST come before the `starts_with("snk.")`
        // catch-all below since that arm routes to build_sink_sql which
        // doesn't know these formats.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        format_sink = Some(FormatFileSinkSpec {
            from_view: from_view.to_string(),
            path,
            format: if component_id == "snk.yaml" {
                FormatKind::Yaml
            } else {
                FormatKind::Toml
            },
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id.starts_with("snk.") {
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
                raw_schema: schema.clone(),
                raw_table: table.clone(),
                conflict_cols,
                delete_column: delete_column_from(&props),
                delete_value: delete_value_from(&props),
            });
            (String::new(), StageKind::Sink, Some(from_view.to_string()))
        } else {
            // The sink's input column names (from the propagated schema) feed
            // the "merge" write mode's MERGE INTO column lists (issue #39).
            let sink_cols: Vec<String> = node
                .data
                .schema
                .as_deref()
                .map(|s| s.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();
            (
                format!("{}{}", attach, build_sink_sql(component_id, &props, from_view, &sink_cols)?),
                StageKind::Sink,
                Some(from_view.to_string()),
            )
        }
    } else if component_id == "ctl.iterate" {
        // Run a pipeline file N times. ${ITER_INDEX} in the sub-pipeline
        // gets substituted to the iteration number (0..N-1). Side-effect
        // model; sub-pipeline output isn't composed into the parent.
        let path = string_prop(&props, "pipelineRef")
            .or_else(|| string_prop(&props, "iteratePipelineRef"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: pipelineRef required", component_id)))?;
        let count = props
            .get("count")
            .or_else(|| props.get("iterations"))
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .ok_or_else(|| EngineError::Config(format!("{}: count (positive integer) required", component_id)))?;
        iterate_pipeline_path = Some(path);
        iterate_count = Some(count);
        let sql = match inputs.main() {
            Some(from_view) => passthrough_view_sql(&node.id, from_view),
            None => passthrough_placeholder_sql(&node.id, "iterated"),
        };
        (sql, StageKind::View, None)
    } else if component_id == "ctl.foreach" {
        // Run a pipeline file once per upstream row. ${ITER_ITEM_<FIELD>}
        // (uppercased) substitutes to the row's value for each field;
        // ${ITER_INDEX} is the row index. We pass the upstream view
        // name through `from` so the executor can SELECT from it
        // *before* our own pass-through SQL materializes the node.
        let path = string_prop(&props, "pipelineRef")
            .or_else(|| string_prop(&props, "foreachPipelineRef"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: pipelineRef required", component_id)))?;
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        foreach_pipeline_path = Some(path);
        // Optional: run the per-row children concurrently. Default 1 keeps the
        // existing sequential behaviour. Accepts a JSON number or a numeric
        // string (the form stores it as text).
        foreach_concurrency = props
            .get("concurrency")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            })
            .unwrap_or(1)
            .max(1) as usize;
        let sql = passthrough_view_sql(&node.id, from_view);
        (sql, StageKind::View, Some(from_view.to_string()))
    } else if component_id == "ctl.try" {
        // Side-effect fallback installer: pass through upstream
        // unchanged; on any subsequent stage failure, the engine
        // runs the fallback pipeline as a side effect before the
        // original error surfaces. Not the full block-scoped try
        // with continuation - that needs the DAG-engine refactor
        // (see docs/dag-block-refactor.md).
        let path = string_prop(&props, "fallbackPipelineRef")
            .or_else(|| string_prop(&props, "fallbackPath"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: fallbackPipelineRef (path to a recovery pipeline) required", component_id)))?;
        install_fallback_path = Some(path);
        let sql = match inputs.main() {
            Some(from_view) => passthrough_view_sql(&node.id, from_view),
            None => passthrough_placeholder_sql(&node.id, "try-installed"),
        };
        (sql, StageKind::View, None)
    } else if component_id == "ctl.runpipeline"
        || component_id == "ctl.trigger"
        || component_id == "ctl.runjob"
    {
        // Parent -> child job call (Run Job). Reads + executes the
        // referenced pipeline file as a side effect before passing this
        // node's upstream view through. `pipelineRef` is the child path;
        // optional context variables (key-value) are substituted as ${VAR}
        // into the child before it runs - same mechanism as ctl.iterate /
        // ctl.foreach. Side-effect model: the child runs in its own temp DB
        // and its output is not composed back into the parent (full
        // composition needs the DAG-block refactor noted in the README).
        // Without an upstream input the stage emits an empty placeholder so
        // downstream wiring still has a target ('master job' orchestration).
        let path = string_prop(&props, "pipelineRef")
            .or_else(|| string_prop(&props, "path"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: pipelineRef (path to a pipeline file) required", component_id)))?;
        let mut vars = kv_pairs(&props, "contextVariables");
        if vars.is_empty() {
            vars = kv_pairs(&props, "parameters");
        }
        run_job = Some((path, vars));
        let sql = match inputs.main() {
            Some(from_view) => passthrough_view_sql(&node.id, from_view),
            None => passthrough_placeholder_sql(&node.id, "triggered"),
        };
        (sql, StageKind::View, None)
    } else if component_id == "ctl.parallelize" {
        // The branch sub-pipelines + concurrency are attached by compile() as
        // RuntimeSpec::Parallelize. Here we just set `from` so the executor
        // knows which upstream to snapshot, and pass the input through as a
        // view so the node stays previewable.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let sql = passthrough_view_sql(&node.id, from_view);
        (sql, StageKind::View, Some(from_view.to_string()))
    } else if component_id == "ctl.log" || component_id == "ctl.warn" {
        // Emit a log line as a side effect, then pass the upstream through.
        // The executor substitutes {rows} with the upstream count and emits
        // a PipelineEvent::Log (also written to the workspace run log).
        let message = string_prop(&props, "message")
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| {
                if component_id == "ctl.warn" { "warning".into() } else { "log".into() }
            });
        let level = if component_id == "ctl.warn" { "warn" } else { "info" };
        log_spec = Some((level.to_string(), message));
        // `from` carries the upstream view so the executor can count its rows.
        let (sql, from) = match inputs.main() {
            Some(from_view) => (
                passthrough_view_sql(&node.id, from_view),
                Some(from_view.to_string()),
            ),
            None => (passthrough_placeholder_sql(&node.id, "logged"), None),
        };
        (sql, StageKind::View, from)
    } else if component_id == "ctl.die" {
        // Fail the run with a message when the condition holds against the
        // upstream row count. Pass-through otherwise so the node previews.
        let message = string_prop(&props, "message")
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Pipeline stopped by Die".into());
        let condition = string_prop(&props, "condition")
            .or_else(|| string_prop(&props, "dieIf"))
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "always".into());
        die_spec = Some((message, condition));
        let (sql, from) = match inputs.main() {
            Some(from_view) => (
                passthrough_view_sql(&node.id, from_view),
                Some(from_view.to_string()),
            ),
            None => (passthrough_placeholder_sql(&node.id, "die"), None),
        };
        (sql, StageKind::View, from)
    } else if component_id == "src.ducklake.changes" || component_id == "xf.ducklake.cdc" {
        // DuckLake change-data-feed (CDC) source. The executor ATTACHes the
        // catalog, reads the last consumed snapshot id from workspace state,
        // materializes table_changes(table, last, current), and persists the
        // new snapshot id on run success. Placeholder SQL; the RuntimeSpec arm
        // replaces it.
        let path = string_prop(&props, "path")
            .or_else(|| string_prop(&props, "catalog"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!("{}: catalog path required", component_id))
            })?;
        let table = string_prop(&props, "table")
            .or_else(|| string_prop(&props, "tableName"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: table required", component_id)))?;
        ducklake_cdc = Some(DuckLakeCdcSpec {
            node_id: node.id.clone(),
            path,
            schema: string_prop(&props, "schema").filter(|s| !s.is_empty()),
            table,
            initial_snapshot: props.get("initialSnapshot").and_then(|v| v.as_u64()).unwrap_or(0),
            inserts_only: props.get("insertsOnly").and_then(|v| v.as_bool()).unwrap_or(false),
        });
        (
            passthrough_placeholder_sql(&node.id, "ducklake-cdc"),
            StageKind::View,
            None,
        )
    } else if component_id == "xf.incremental" {
        // Watermark incremental load. The executor reads the saved high-water
        // mark, materializes only rows past it, and persists the new mark on
        // run success - so the planner SQL here is just a placeholder the
        // RuntimeSpec arm replaces.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let column = string_prop(&props, "column")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!("{}: column required (the watermark column)", component_id))
            })?;
        let initial = string_prop(&props, "initialValue").filter(|s| !s.trim().is_empty());
        incremental = Some(IncrementalSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            column,
            initial,
        });
        (
            passthrough_view_sql(&node.id, from_view),
            StageKind::View,
            Some(from_view.to_string()),
        )
    } else if component_id == "ctl.wait" {
        // Pass-through view. Engine sleeps wait_ms before running the SQL.
        // Form writes { duration: int, unit: 'milliseconds'|'seconds'|'minutes'|'hours' }.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let dur = props.get("duration").and_then(|v| v.as_u64()).unwrap_or(0);
        let unit = string_prop(&props, "unit").unwrap_or_else(|| "seconds".into());
        let ms = match unit.as_str() {
            "milliseconds" | "ms" => dur,
            "minutes" => dur.saturating_mul(60_000),
            "hours" => dur.saturating_mul(3_600_000),
            _ => dur.saturating_mul(1_000),
        };
        if ms > 0 {
            wait_ms = Some(ms);
        }
        let sql = passthrough_view_sql(&node.id, from_view);
        (sql, StageKind::View, None)
    } else if component_id == "ctl.throttle" {
        // Same shape as ctl.wait - applies an inter-stage delay derived
        // from the requested rows-per-second. Marginal for batch
        // workloads but the hook is in place for streaming.
        // Form writes { rate: int (rows/sec) }.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let rps = props
            .get("rate")
            .and_then(|v| v.as_f64())
            .or_else(|| props.get("rowsPerSecond").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);
        if rps > 0.0 {
            wait_ms = Some((1000.0 / rps).max(1.0) as u64);
        }
        let sql = passthrough_view_sql(&node.id, from_view);
        (sql, StageKind::View, None)
    } else if component_id == "ctl.checkpoint" {
        // Pass-through view + a sidecar parquet write. The temp DB the
        // executor uses goes away after the pipeline; the parquet is
        // the durable artifact a user can read back into a future run.
        // Form writes { name, storage }.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "storage")
            .or_else(|| string_prop(&props, "path"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: checkpoint storage path required", component_id)))?;
        // Pass-through as a view, then write the durable checkpoint
        // parquet directly from upstream. The view avoids copying every
        // row into an intermediate table before the COPY reads it again.
        let sql = format!(
            "{}; COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET)",
            passthrough_view_sql(&node.id, from_view),
            quote_ident(from_view),
            sql_escape(&path)
        );
        (sql, StageKind::View, None)
    } else if component_id == "ctl.deadletter" {
        // Terminal sink for rejected rows. Same shape as snk.parquet /
        // snk.csv / snk.json - write the upstream to a file.
        // Form writes { destination: path, format: 'json'|'csv'|'parquet' }.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "destination")
            .or_else(|| string_prop(&props, "path"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: dead letter destination required", component_id)))?;
        let format = string_prop(&props, "format").unwrap_or_else(|| "json".into());
        sink_path = Some(path.clone());
        sink_mode = string_prop(&props, "mode").filter(|s| !s.is_empty());
        let copy = match format.as_str() {
            "csv" => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT CSV, HEADER true)",
                quote_ident(from_view),
                sql_escape(&path)
            ),
            "parquet" => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET, COMPRESSION 'ZSTD')",
                quote_ident(from_view),
                sql_escape(&path)
            ),
            _ => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT JSON, ARRAY false)",
                quote_ident(from_view),
                sql_escape(&path)
            ),
        };
        (copy, StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "src.elastic" || component_id == "src.opensearch" {
        // Elasticsearch / OpenSearch _search source. Form: endpoint,
        // index, apiKey, query (raw JSON DSL), size.
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required", component_id)))?;
        let index = string_prop(&props, "index")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: index required", component_id)))?;
        let pagination_mode = string_prop(&props, "paginationMode").unwrap_or_else(|| "from_size".into());
        let pagination = match pagination_mode.as_str() {
            "search_after" => {
                let sort = string_prop(&props, "sort")
                    .filter(|s| !s.trim().is_empty())
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                    .and_then(|v| v.as_array().cloned())
                    // Default sort: _shard_doc is Elasticsearch's
                    // built-in shard-stable doc id (7.12+); safe
                    // tiebreaker that works without any field choice.
                    .unwrap_or_else(|| vec![serde_json::json!({"_shard_doc": "asc"})]);
                ElasticPagination::SearchAfter { sort }
            }
            _ => ElasticPagination::FromSize,
        };
        elastic_source = Some(ElasticSourceSpec {
            node_id: node.id.clone(),
            endpoint,
            index,
            api_key: string_prop(&props, "apiKey").filter(|s| !s.is_empty()),
            query: string_prop(&props, "query").filter(|s| !s.trim().is_empty()),
            size: props
                .get("size")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000),
            max_pages: props
                .get("maxPages")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(100),
            pagination,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.oracle" {
        let connect = string_prop(&props, "connect")
            .or_else(|| string_prop(&props, "connectionString"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: connect required", component_id)))?;
        let user = string_prop(&props, "user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: user required", component_id)))?;
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName").filter(|s| !s.is_empty())?;
                let schema = string_prop(&props, "schema").filter(|s| !s.is_empty());
                let qualified = match schema {
                    Some(s) => format!("\"{}\".\"{}\"", s, table),
                    None => format!("\"{}\"", table),
                };
                Some(format!("SELECT * FROM {}", qualified))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or tableName required", component_id)))?;
        oracle_source = Some(OracleSourceSpec {
            node_id: node.id.clone(),
            connect,
            user,
            password: string_prop(&props, "password").unwrap_or_default(),
            query,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.adbc" {
        // Generic ADBC source: a prebuilt driver lib + database options +
        // a SQL query. Friendly wrappers (e.g. src.snowflake.adbc) can map
        // their own fields onto `driver`/`options` before reaching here.
        let driver = string_prop(&props, "driver")
            .or_else(|| string_prop(&props, "driverPath"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: driver (path or name) required", component_id)))?;
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: query required", component_id)))?;
        let mut options: Vec<(String, String)> = Vec::new();
        if let Some(arr) = props.get("options").and_then(JsonValue::as_array) {
            for kv in arr {
                let k = kv.get("key").and_then(|v| v.as_str()).unwrap_or("").trim();
                let v = kv.get("value").and_then(|v| v.as_str()).unwrap_or("");
                if !k.is_empty() {
                    options.push((k.to_string(), v.to_string()));
                }
            }
        }
        // Convenience: a bare `uri` prop maps to the canonical ADBC uri key.
        if let Some(uri) = string_prop(&props, "uri").filter(|s| !s.is_empty()) {
            options.push(("uri".to_string(), uri));
        }
        // At most one downstream consumer means we can expose the materialized
        // parquet as a lazy read_parquet VIEW instead of copying it into a
        // table (skips the table write; lets the consumer push projection /
        // predicate down into the parquet scan).
        let single_consumer = consumer_count
            .get(&output_table_ref(&node.id, None))
            .copied()
            .unwrap_or(0)
            <= 1;
        adbc_source = Some(AdbcSourceSpec {
            node_id: node.id.clone(),
            driver,
            entrypoint: string_prop(&props, "entrypoint").filter(|s| !s.is_empty()),
            options,
            query,
            single_consumer,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.nats" {
        // NATS subscribe-with-timeout collector. Drains up to
        // max_records messages or stops after timeout_ms wall-clock.
        let urls = string_prop(&props, "urls")
            .or_else(|| string_prop(&props, "servers"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: urls required", component_id)))?;
        let subject = string_prop(&props, "subject")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: subject required", component_id)))?;
        nats_source = Some(NatsSourceSpec {
            node_id: node.id.clone(),
            urls,
            subject,
            max_records: props.get("maxRecords").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000),
            timeout_ms: props.get("timeoutMs").and_then(|v| v.as_u64()).unwrap_or(5000),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.pubsub" {
        // GCP Pub/Sub pull. Auto-acks the pulled batch (best-fit for
        // batch ETL drains; for exactly-once you'd want manual ack
        // which is on the roadmap).
        let project = string_prop(&props, "project")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: project required", component_id)))?;
        let subscription = string_prop(&props, "subscription")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: subscription required", component_id)))?;
        let access_token = string_prop(&props, "accessToken")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: accessToken required (OAuth2 Bearer)", component_id)))?;
        pubsub_source = Some(PubSubSourceSpec {
            node_id: node.id.clone(),
            project,
            subscription,
            access_token,
            max_messages: props.get("maxMessages").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
        });
        (String::new(), StageKind::View, None)
    } else if matches!(component_id, "src.kafka" | "src.redpanda") {
        // Kafka batch-consume from a single partition. start_offset
        // negative = read from earliest available; positive = read
        // from that offset. max_records caps the batch (defaults to
        // 1000 - this is a batch ETL connector, not a streaming pump).
        let bootstrap = string_prop(&props, "brokers")
            .or_else(|| string_prop(&props, "bootstrapServers"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: brokers required", component_id)))?;
        let topic = string_prop(&props, "topic")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: topic required", component_id)))?;
        kafka_source = Some(KafkaSourceSpec {
            node_id: node.id.clone(),
            bootstrap_servers: bootstrap,
            topic,
            partition_id: props.get("partitionId").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            // The UI exposes `offset` = latest/earliest, not a numeric
            // startOffset. Map it onto the sentinel run_kafka_source reads:
            // -2 = latest tip (only new messages), -1 = earliest, >=0 = that
            // literal offset. A hand-authored numeric startOffset still wins;
            // default earliest when neither is supplied. Previously the engine
            // only read startOffset, so the UI's Initial offset was a no-op and
            // "Latest" silently behaved as "Earliest".
            start_offset: props
                .get("startOffset")
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| match string_prop(&props, "offset").as_deref() {
                    Some("latest") => -2,
                    _ => -1,
                }),
            max_records: props.get("maxRecords").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.rabbit" {
        // RabbitMQ batch consumer. queue must exist (declared by the
        // producer or the broker admin). Pulls up to max_messages or
        // until timeout_ms elapses.
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required (amqp://...)", component_id)))?;
        let queue = string_prop(&props, "queue")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: queue required", component_id)))?;
        rabbit_source = Some(RabbitSourceSpec {
            node_id: node.id.clone(),
            url,
            queue,
            max_messages: props.get("maxMessages").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000),
            timeout_ms: props.get("timeoutMs").and_then(|v| v.as_u64()).unwrap_or(5000),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.git" {
        // Local git repo reader. mode=log walks `git log`; mode=files
        // walks `git ls-tree -r`. Both shell out to the system `git`.
        let repo = string_prop(&props, "repo")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: repo required (path to local clone)", component_id)))?;
        git_source = Some(GitSourceSpec {
            node_id: node.id.clone(),
            repo,
            mode: string_prop(&props, "mode")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "log".to_string()),
            revision: string_prop(&props, "revision")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "HEAD".to_string()),
            path_filter: string_prop(&props, "pathFilter").filter(|s| !s.is_empty()),
            max_rows: props
                .get("maxRows")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "code.shell" {
        // One-shot shell exec. Emits a single row with the captured
        // stdout/stderr/exit_code/duration_ms so downstream stages can
        // branch on success / parse output. Shell defaults to the
        // platform interpreter; pass `shell` to override.
        let command = string_prop(&props, "command")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: command required", component_id)))?;
        shell = Some(ShellSpec {
            node_id: node.id.clone(),
            command,
            shell: string_prop(&props, "shell").filter(|s| !s.is_empty()),
            working_dir: string_prop(&props, "workingDir").filter(|s| !s.is_empty()),
            timeout_ms: props
                .get("timeoutMs")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.dbt" {
        // dbt Core execution node. The engine generates profiles.yml for
        // the dbt-duckdb adapter against the run database, so models read
        // upstream node tables directly and downstream nodes read the
        // built models. Upstream is optional - a project can also run
        // purely against its own sources.
        // Two authoring modes: point at an existing project (projectDir), or
        // write one model inline (model) which the engine scaffolds into an
        // ephemeral project. One of the two is required.
        let project_dir = string_prop(&props, "projectDir").filter(|s| !s.trim().is_empty());
        let inline_model = string_prop(&props, "model").filter(|s| !s.trim().is_empty());
        if project_dir.is_none() && inline_model.is_none() {
            return Err(EngineError::Config(format!(
                "{}: set either projectDir (an existing dbt project) or an inline model",
                component_id
            )));
        }
        let inline_model_name = string_prop(&props, "modelName")
            .filter(|s| !s.trim().is_empty())
            .map(|s| sanitize_dbt_model_name(&s))
            .unwrap_or_else(|| "duckle_model".into());
        // In inline mode the node's natural output is the model it just built,
        // so default outputModel to the model name when not set.
        let output_model = string_prop(&props, "outputModel")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                if project_dir.is_none() {
                    Some(inline_model_name.clone())
                } else {
                    None
                }
            });
        let from_views: Vec<String> =
            inputs.all_main_ports().iter().map(|s| s.to_string()).collect();
        let from = from_views.first().cloned();
        dbt = Some(DbtSpec {
            node_id: node.id.clone(),
            project_dir,
            inline_model,
            inline_model_name,
            command: string_prop(&props, "command")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "run".into()),
            dbt_bin: string_prop(&props, "dbtBin").filter(|s| !s.trim().is_empty()),
            database: string_prop(&props, "database").filter(|s| !s.trim().is_empty()),
            schema: string_prop(&props, "schema")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "main".into()),
            output_model,
            from_view: from.clone(),
            from_views: from_views.clone(),
            timeout_ms: props
                .get("timeoutMs")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0),
        });
        (String::new(), StageKind::View, from)
    } else if component_id == "src.kinesis" {
        // Single-shard Kinesis read. iteratorType in
        // {TRIM_HORIZON, LATEST, AT_TIMESTAMP, AT/AFTER_SEQUENCE_NUMBER};
        // we expose only the simple two-value choice for v1.
        let region = string_prop(&props, "region")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: region required", component_id)))?;
        let access_key_id = string_prop(&props, "accessKeyId")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: accessKeyId required", component_id)))?;
        let secret_access_key = string_prop(&props, "secretAccessKey")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: secretAccessKey required", component_id)))?;
        let stream_name = string_prop(&props, "streamName")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: streamName required", component_id)))?;
        kinesis_source = Some(KinesisSourceSpec {
            node_id: node.id.clone(),
            region,
            access_key_id,
            secret_access_key,
            session_token: string_prop(&props, "sessionToken").filter(|s| !s.is_empty()),
            stream_name,
            shard_index: props
                .get("shardIndex")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
            iterator_type: string_prop(&props, "iteratorType")
                .filter(|s| s == "TRIM_HORIZON" || s == "LATEST")
                .unwrap_or_else(|| "TRIM_HORIZON".into()),
            max_records: props
                .get("maxRecords")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.dynamodb" {
        // DynamoDB Scan via direct HTTP + SigV4. Pure JSON wire
        // protocol; we avoid pulling in the 300-service aws-sdk-rust
        // dep tree. region required; credentials from props
        // (env-var lookup is a follow-up via the credentials store).
        let region = string_prop(&props, "region")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: region required (e.g. us-east-1)", component_id)))?;
        let access_key_id = string_prop(&props, "accessKeyId")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: accessKeyId required", component_id)))?;
        let secret_access_key = string_prop(&props, "secretAccessKey")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: secretAccessKey required", component_id)))?;
        let table_name = string_prop(&props, "tableName")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        dynamodb_source = Some(DynamoDbSourceSpec {
            node_id: node.id.clone(),
            region,
            access_key_id,
            secret_access_key,
            session_token: string_prop(&props, "sessionToken").filter(|s| !s.is_empty()),
            table_name,
            limit_per_page: props
                .get("limitPerPage")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000),
            max_pages: props
                .get("maxPages")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(100),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.webhook" {
        // Local HTTP listener that collects N requests then closes.
        // Bound to 127.0.0.1 only; users punching through to the
        // internet should run their own tunnel (ngrok / cloudflared).
        webhook_source = Some(WebhookSourceSpec {
            node_id: node.id.clone(),
            port: props
                .get("port")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n < 65536)
                .map(|n| n as u16)
                .ok_or_else(|| EngineError::Config(format!("{}: port required", component_id)))?,
            max_requests: props
                .get("maxRequests")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1),
            timeout_ms: props
                .get("timeoutMs")
                .and_then(|v| v.as_u64())
                .unwrap_or(30000),
            path_filter: string_prop(&props, "pathFilter").filter(|s| !s.is_empty()),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.email" {
        // IMAP source. host required (e.g. imap.fastmail.com); port
        // defaults to 993 (IMAPS). mailbox defaults to INBOX.
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let user = string_prop(&props, "user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: user required", component_id)))?;
        let password = string_prop(&props, "password")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: password required", component_id)))?;
        email_source = Some(EmailSourceSpec {
            node_id: node.id.clone(),
            host,
            port: props
                .get("port")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n < 65536)
                .map(|n| n as u16)
                .unwrap_or(993),
            user,
            password,
            mailbox: string_prop(&props, "mailbox")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "INBOX".into()),
            max_messages: props
                .get("maxMessages")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(50),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.clipboard" {
        // System clipboard reader. No props - just emit current
        // clipboard content as a row (or rows, if JSON array).
        clipboard_source = Some(ClipboardSourceSpec {
            node_id: node.id.clone(),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.ftp" {
        // File-transfer source. The Protocol dropdown selects FTP, FTPS, or
        // SFTP. FTP / FTPS go through suppaftp; SFTP (SSH - a different
        // protocol) goes through russh + russh-sftp (issue #16). All three
        // list files at `directory`, filter by optional glob `pattern`,
        // download up to `maxFiles`, and emit one row per file
        // {filename, size, content_b64, modified}.
        let protocol = string_prop(&props, "protocol")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        // The form historically wrote `username` / `remotePath`; accept those
        // as fallbacks for the canonical `user` / `directory`.
        let user = string_prop(&props, "user")
            .or_else(|| string_prop(&props, "username"))
            .filter(|s| !s.is_empty());
        let directory = string_prop(&props, "directory")
            .or_else(|| string_prop(&props, "remotePath"))
            .filter(|s| !s.is_empty());
        let pattern = string_prop(&props, "pattern").filter(|s| !s.is_empty());
        let max_files = props
            .get("maxFiles")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(100);
        let port = props
            .get("port")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0 && *n < 65536)
            .map(|n| n as u16);
        if protocol == "sftp" {
            sftp_source = Some(SftpSourceSpec {
                node_id: node.id.clone(),
                host,
                port: port.unwrap_or(22),
                user: user.ok_or_else(|| {
                    EngineError::Config(format!("{}: user required for SFTP", component_id))
                })?,
                password: string_prop(&props, "password").filter(|s| !s.is_empty()),
                // Accept a pasted PEM (privateKey) or a key file (privateKeyPath).
                private_key: string_prop(&props, "privateKey")
                    .or_else(|| {
                        string_prop(&props, "privateKeyPath")
                            .and_then(|p| std::fs::read_to_string(&p).ok())
                    })
                    .filter(|s| !s.is_empty()),
                key_passphrase: string_prop(&props, "keyPassphrase").filter(|s| !s.is_empty()),
                directory: directory.unwrap_or_else(|| ".".into()),
                pattern,
                max_files,
                host_fingerprint: string_prop(&props, "hostFingerprint").filter(|s| !s.is_empty()),
            });
        } else {
            ftp_source = Some(FtpSourceSpec {
                node_id: node.id.clone(),
                host,
                port: port.unwrap_or(21),
                user: user.unwrap_or_else(|| "anonymous".into()),
                password: string_prop(&props, "password").unwrap_or_else(|| "anonymous@".into()),
                secure: protocol == "ftps"
                    || props.get("secure").and_then(|v| v.as_bool()).unwrap_or(false),
                directory: directory.unwrap_or_else(|| "/".into()),
                pattern,
                max_files,
            });
        }
        (String::new(), StageKind::View, None)
    } else if component_id == "src.xml" {
        // XML row-path source. rowPath is a slash-separated element
        // walk from the root (e.g. "library/books/book"). Each match
        // becomes a JSON object with attributes prefixed '@', text in
        // '_text', and child elements nested.
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        xml_source = Some(XmlSourceSpec {
            node_id: node.id.clone(),
            path,
            row_path: string_prop(&props, "rowPath").unwrap_or_default(),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.avro" {
        // Apache Avro container-file reader via the pure-Rust apache-avro
        // crate. Self-contained - works on every OS without DuckDB's
        // community avro extension (which only ships for a subset of
        // platform/version combos). The .avro file carries its own
        // schema in the OCF header so no schema config is needed.
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        avro_source = Some(AvroSourceSpec {
            node_id: node.id.clone(),
            path,
        });
        (String::new(), StageKind::View, None)
    } else if matches!(component_id, "src.yaml" | "src.toml") {
        // Single-file YAML / TOML reader. path is the absolute file
        // path; engine parses the doc with the relevant serde crate
        // and materializes the row array via the shared json-table
        // helper. If the doc is a top-level array, each element is
        // a row; otherwise the whole doc becomes one row.
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        format_source = Some(FormatFileSourceSpec {
            node_id: node.id.clone(),
            path,
            format: if component_id == "src.yaml" {
                FormatKind::Yaml
            } else {
                FormatKind::Toml
            },
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.qdrant" {
        // Qdrant points scroll source. clusterUrl + collection +
        // optional apiKey. with_vector defaults false (vectors are
        // big - users usually want metadata for ETL).
        let cluster = string_prop(&props, "clusterUrl")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: clusterUrl required (e.g. https://xyz.cloud.qdrant.io:6333)", component_id)))?;
        let collection = string_prop(&props, "collection")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: collection required", component_id)))?;
        qdrant_source = Some(QdrantSourceSpec {
            node_id: node.id.clone(),
            cluster_url: cluster,
            collection,
            api_key: string_prop(&props, "apiKey").unwrap_or_default(),
            page_size: props.get("pageSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
            max_pages: props.get("maxPages").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
            with_vector: props.get("withVector").and_then(|v| v.as_bool()).unwrap_or(false),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.weaviate" {
        // Weaviate object list source. endpoint + class + optional apiKey.
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required (e.g. https://my-cluster.weaviate.network)", component_id)))?;
        let class = string_prop(&props, "class")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: class required", component_id)))?;
        weaviate_source = Some(WeaviateSourceSpec {
            node_id: node.id.clone(),
            endpoint,
            class,
            api_key: string_prop(&props, "apiKey").unwrap_or_default(),
            page_size: props.get("pageSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
            max_pages: props.get("maxPages").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
            with_vector: props.get("withVector").and_then(|v| v.as_bool()).unwrap_or(false),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.milvus" {
        // Milvus query source. endpoint + collection + filter expression
        // (e.g. "id > 0") + optional outputFields (comma-separated) +
        // apiKey. Walks via offset += pageSize until a short page.
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required", component_id)))?;
        let collection = string_prop(&props, "collection")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: collection required", component_id)))?;
        let output_fields = string_prop(&props, "outputFields")
            .map(|s| s.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect::<Vec<_>>())
            .unwrap_or_default();
        milvus_source = Some(MilvusSourceSpec {
            node_id: node.id.clone(),
            endpoint,
            collection,
            api_key: string_prop(&props, "apiKey").unwrap_or_default(),
            filter: string_prop(&props, "filter").filter(|s| !s.trim().is_empty()).unwrap_or_else(|| "id > 0".into()),
            output_fields,
            page_size: props.get("pageSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
            max_pages: props.get("maxPages").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(100),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.redis" {
        // Redis SCAN+GET source. Walks keys matching keyPattern (default
        // '*') up to `limit` keys; emits {key, value} rows. Hash / list /
        // set / sorted-set value types stringify as their MULTI reply -
        // for now the simple string GET path covers the common cache
        // export use case.
        let url = string_prop(&props, "url")
            .or_else(|| string_prop(&props, "connectionString"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        redis_source = Some(RedisSourceSpec {
            node_id: node.id.clone(),
            url,
            key_pattern: string_prop(&props, "keyPattern")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "*".into()),
            limit: props.get("limit").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(10_000),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.cassandra" || component_id == "src.scylla" {
        let contact_points = string_prop(&props, "contactPoints")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: contactPoints required", component_id)))?;
        let keyspace = string_prop(&props, "keyspace").filter(|s| !s.is_empty());
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName").filter(|s| !s.is_empty())?;
                let ks = keyspace.clone()?;
                Some(format!("SELECT * FROM {}.{}", ks, table))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or (keyspace+tableName) required", component_id)))?;
        cassandra_source = Some(CassandraSourceSpec {
            node_id: node.id.clone(),
            contact_points,
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            keyspace,
            query,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.sqlserver" || component_id == "src.synapse" {
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let user = string_prop(&props, "user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: user required", component_id)))?;
        let database = string_prop(&props, "database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: database required", component_id)))?;
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName").filter(|s| !s.is_empty())?;
                let schema = string_prop(&props, "schema").unwrap_or_else(|| "dbo".into());
                Some(format!("SELECT * FROM [{}].[{}]", schema, table))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or tableName required", component_id)))?;
        sqlserver_source = Some(SqlServerSourceSpec {
            node_id: node.id.clone(),
            host,
            // Range-check before the u16 cast (see the sink path); an out-of-range
            // port would otherwise wrap and dial the wrong service.
            port: props
                .get("port")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n < 65536)
                .map(|n| n as u16)
                .unwrap_or(1433),
            user,
            password: string_prop(&props, "password").unwrap_or_default(),
            database,
            query,
            trust_cert: props.get("trustCert").and_then(|v| v.as_bool()).unwrap_or(false),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.clickhouse" {
        let endpoint = string_prop(&props, "endpoint")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required", component_id)))?;
        let database = string_prop(&props, "database").filter(|s| !s.is_empty());
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName").filter(|s| !s.is_empty())?;
                let qualified = match &database {
                    Some(d) => format!("`{}`.`{}`", d, table),
                    None => format!("`{}`", table),
                };
                Some(format!("SELECT * FROM {}", qualified))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or tableName required", component_id)))?;
        clickhouse_source = Some(ClickHouseSourceSpec {
            node_id: node.id.clone(),
            endpoint,
            database,
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            query,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.mongodb" {
        let uri = string_prop(&props, "uri")
            .or_else(|| string_prop(&props, "connectionString"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: uri required", component_id)))?;
        let database = string_prop(&props, "database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: database required", component_id)))?;
        let collection = string_prop(&props, "collection")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: collection required", component_id)))?;
        mongo_source = Some(MongoSourceSpec {
            node_id: node.id.clone(),
            uri,
            database,
            collection,
            filter: string_prop(&props, "filter").filter(|s| !s.trim().is_empty()),
            projection: string_prop(&props, "projection").filter(|s| !s.trim().is_empty()),
            limit: props.get("limit").and_then(|v| v.as_i64()).filter(|n| *n > 0),
        });
        (String::new(), StageKind::View, None)
    } else if matches!(component_id, "src.graphql" | "src.linear" | "src.monday") {
        // GraphQL source + Linear alias: POST {query, variables} to
        // the endpoint, walk the response data path. Rides
        // RestSourceSpec. Linear's API is exclusively GraphQL so the
        // alias gives users a clear-named tile.
        let url = string_prop(&props, "url")
            .or_else(|| string_prop(&props, "endpoint"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: query required", component_id)))?;
        let variables = string_prop(&props, "variables")
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                serde_json::from_str::<serde_json::Value>(&s)
                    .unwrap_or(serde_json::Value::Object(Default::default()))
            })
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let body = serde_json::json!({
            "query": query,
            "variables": variables,
        });
        let mut headers = headers_from_props(&props);
        push_rest_auth(&mut headers, &props);
        // responsePath defaults to /data which is the GraphQL convention.
        let response_path = string_prop(&props, "responsePath")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "/data".into());
        rest_source = Some(RestSourceSpec {
            node_id: node.id.clone(),
            url,
            method: "POST".into(),
            headers,
            body: Some(serde_json::to_string(&body).unwrap_or_else(|_| "{}".into())),
            response_path,
            response_format: RestResponseFormat::Json,
            pagination: RestPagination::None,
            max_pages: 1,
        });
        (String::new(), StageKind::View, None)
    } else if matches!(
        component_id,
        "src.rest"
            | "src.github"
            | "src.gitlab"
            | "src.airtable"
            | "src.notion"
            | "src.hubspot"
            | "src.jira"
            | "src.stripe"
            | "src.sendgrid"
            | "src.mailchimp"
            | "src.pipedrive"
            | "src.segment"
            | "src.salesforce"
            | "src.xero"
            | "src.quickbooks"
            | "src.zendesk"
            | "src.shopify"
            | "src.intercom"
            | "src.couchdb"
            | "src.odata"
            | "src.soap"
            | "src.asana"
            | "src.trello"
            | "src.clickup"
            | "src.slack"
            | "src.discord"
            | "src.twilio"
            | "src.telegram"
    ) {
        // Generic REST source + thin vendor aliases. Vendors share
        // the same plumbing - the palette/form pre-fills url, auth
        // scheme, and pagination for the well-known APIs so users
        // don't have to look up each vendor's quirks; the engine
        // treats them identically. Any prefilled value is overridable.
        // src.odata: defaults to responsePath=/value + nextUrl
        // pagination at /@odata.nextLink (the OData v4 contract).
        // src.soap: defaults to POST + Content-Type text/xml + XML
        // response parsing (responsePath walks element names from the
        // SOAP envelope root, e.g. Envelope/Body/Foo/Bar).
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        let method = string_prop(&props, "method")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if component_id == "src.soap" {
                    "POST".into()
                } else {
                    "GET".into()
                }
            })
            .to_uppercase();
        let body = string_prop(&props, "body").filter(|s| !s.is_empty());
        let mut headers = headers_from_props(&props);
        // SOAP needs a content-type and (often) a SOAPAction header.
        // Only set defaults if the user didn't already pass them via
        // the headers form.
        if component_id == "src.soap" {
            let has_ct = headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("Content-Type"));
            if !has_ct {
                headers.push(("Content-Type".into(), "text/xml; charset=utf-8".into()));
            }
            if let Some(action) = string_prop(&props, "soapAction").filter(|s| !s.is_empty()) {
                let has_sa = headers
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("SOAPAction"));
                if !has_sa {
                    headers.push(("SOAPAction".into(), action));
                }
            }
        }
        push_rest_auth(&mut headers, &props);
        let response_format = if component_id == "src.soap"
            || string_prop(&props, "responseFormat").as_deref() == Some("xml")
        {
            RestResponseFormat::Xml
        } else {
            RestResponseFormat::Json
        };
        let response_path = string_prop(&props, "responsePath")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if component_id == "src.odata" {
                    "/value".into()
                } else {
                    String::new()
                }
            });
        let pagination_type = string_prop(&props, "paginationType")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if component_id == "src.odata" {
                    "nextUrl".into()
                } else {
                    "none".into()
                }
            });
        let pagination = match pagination_type.as_str() {
            "cursor" => {
                let next_path = string_prop(&props, "cursorNextPath").filter(|s| !s.is_empty());
                let param = string_prop(&props, "cursorParam").filter(|s| !s.is_empty());
                match (next_path, param) {
                    (Some(n), Some(p)) => RestPagination::Cursor { next_path: n, param: p },
                    _ => RestPagination::None,
                }
            }
            "offset" => {
                let param = string_prop(&props, "offsetParam")
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "offset".into());
                let page_size = props
                    .get("pageSize")
                    .and_then(|v| v.as_u64())
                    .filter(|n| *n > 0)
                    .unwrap_or(100);
                let total_path = string_prop(&props, "totalCountPath")
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .map(|s| if s.starts_with('/') { s } else { format!("/{}", s) });
                RestPagination::Offset { offset_param: param, page_size, total_path }
            }
            "page" => {
                let param = string_prop(&props, "pageParam")
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "page".into());
                let start_page = props
                    .get("startPage")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1);
                RestPagination::Page { page_param: param, start_page }
            }
            "link" => RestPagination::Link,
            "nextUrl" => {
                let next_path = string_prop(&props, "nextUrlPath")
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| {
                        if component_id == "src.odata" {
                            "/@odata.nextLink".into()
                        } else {
                            "/next".into()
                        }
                    });
                RestPagination::NextUrl { next_path }
            }
            _ => {
                // Back-compat: if cursor_next_path is set, use cursor mode.
                let next_path = string_prop(&props, "cursorNextPath").filter(|s| !s.is_empty());
                let param = string_prop(&props, "cursorParam").filter(|s| !s.is_empty());
                match (next_path, param) {
                    (Some(n), Some(p)) => RestPagination::Cursor { next_path: n, param: p },
                    _ => RestPagination::None,
                }
            }
        };
        let max_pages = props
            .get("maxPages")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(100);
        rest_source = Some(RestSourceSpec {
            node_id: node.id.clone(),
            url,
            method,
            headers,
            body,
            response_path,
            response_format,
            pagination,
            max_pages,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.snowflake" {
        // Snowflake source. User picks PAT or JWT auth (same shape
        // as snk.snowflake) and provides either a free 'query' or
        // (database, schema, tableName) which the engine turns into
        // 'SELECT * FROM database.schema.tableName'.
        let account = string_prop(&props, "account")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: account required", component_id)))?;
        let auth_type = string_prop(&props, "authType").unwrap_or_else(|| "pat".into());
        let auth = match auth_type.as_str() {
            "jwt" => {
                let user = string_prop(&props, "user")
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| EngineError::Config(format!("{}: user required for JWT auth", component_id)))?;
                let pem = string_prop(&props, "privateKeyPem")
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        string_prop(&props, "privateKeyPath")
                            .filter(|s| !s.is_empty())
                            .and_then(|p| std::fs::read_to_string(&p).ok())
                    })
                    .ok_or_else(|| EngineError::Config(format!("{}: privateKeyPem or privateKeyPath required for JWT auth", component_id)))?;
                SnowflakeAuth::Jwt { user, private_key_pem: pem }
            }
            _ => {
                let token = string_prop(&props, "pat")
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| EngineError::Config(format!("{}: pat required for PAT auth", component_id)))?;
                SnowflakeAuth::Pat { token }
            }
        };
        let database = string_prop(&props, "database").filter(|s| !s.is_empty());
        let schema = string_prop(&props, "schema").filter(|s| !s.is_empty());
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName").filter(|s| !s.is_empty())?;
                let db = database.clone()?;
                let sch = schema.clone().unwrap_or_else(|| "PUBLIC".into());
                Some(format!(
                    "SELECT * FROM \"{}\".\"{}\".\"{}\"",
                    db, sch, table
                ))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or (database+schema+tableName) required", component_id)))?;
        snowflake_source = Some(SnowflakeSourceSpec {
            node_id: node.id.clone(),
            account,
            endpoint: string_prop(&props, "endpoint").filter(|s| !s.is_empty()),
            auth,
            database,
            schema,
            warehouse: string_prop(&props, "warehouse").filter(|s| !s.is_empty()),
            role: string_prop(&props, "role").filter(|s| !s.is_empty()),
            query,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.databricks" {
        // Databricks SQL source. Same shape as snk.databricks but reads.
        let workspace = string_prop(&props, "workspace")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: workspace required", component_id)))?;
        let pat = string_prop(&props, "pat")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: pat required", component_id)))?;
        let warehouse_id = string_prop(&props, "warehouseId")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: warehouseId required", component_id)))?;
        let catalog = string_prop(&props, "catalog").filter(|s| !s.is_empty());
        let schema = string_prop(&props, "schema").filter(|s| !s.is_empty());
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName").filter(|s| !s.is_empty())?;
                let qualified = match (&catalog, &schema) {
                    (Some(c), Some(s)) => format!("`{}`.`{}`.`{}`", c, s, table),
                    (None, Some(s)) => format!("`{}`.`{}`", s, table),
                    _ => format!("`{}`", table),
                };
                Some(format!("SELECT * FROM {}", qualified))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or (catalog+schema+tableName) required", component_id)))?;
        databricks_source = Some(DatabricksSourceSpec {
            node_id: node.id.clone(),
            workspace,
            endpoint: string_prop(&props, "endpoint").filter(|s| !s.is_empty()),
            pat,
            warehouse_id,
            catalog,
            schema,
            query,
            wait_timeout_seconds: props
                .get("waitTimeoutSeconds")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n <= 50)
                .unwrap_or(30),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "ctl.switch" {
        // Switch materializes one table per case + default; it has no
        // main output table, so the count_rows fallback in the executor
        // (which would target node.id) just returns None for it.
        let sql = build_switch(&node.id, inputs, &props, consumer_count).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })?;
        (format!("{}{}", attach, sql), StageKind::View, None)
    } else if component_id == "xf.ai.text_search" {
        // Full-Text Search runs as a two-step path in the executor (the
        // v1.5 fts PRAGMA can't see tables created in the same -c
        // invocation). The planner records the spec; sql stays empty.
        let spec = build_text_search_spec(&node.id, inputs, &props).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })?;
        text_search = Some(spec);
        (String::new(), StageKind::View, None)
    } else if component_id == "code.javascript" {
        // Per-row JS transform. Script must define a `transform`
        // function (named or assigned) that takes a row object and
        // returns one. No persistent state across rows.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let script = string_prop(&props, "script")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: script required", component_id)))?;
        javascript = Some(JavaScriptSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            script,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "code.wasm" {
        // Per-row WASM transform via wasmi. The user supplies the
        // module either as base64 bytes (inline) or as a path to a
        // .wasm file. Module contract: must export `memory` and a
        // function with signature (i32, i32) -> i64 packing
        // (out_ptr << 32) | out_len.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let wasm_bytes = if let Some(b64) = string_prop(&props, "wasmB64").filter(|s| !s.is_empty())
        {
            use base64::engine::general_purpose::STANDARD as B64;
            use base64::Engine as _;
            B64.decode(&b64)
                .map_err(|e| EngineError::Config(format!("{}: wasmB64 decode: {}", component_id, e)))?
        } else if let Some(path) = string_prop(&props, "path").filter(|s| !s.is_empty()) {
            std::fs::read(&path)
                .map_err(|e| EngineError::Config(format!("{}: read {}: {}", component_id, path, e)))?
        } else {
            return Err(EngineError::Config(format!(
                "{}: either wasmB64 or path required",
                component_id
            )));
        };
        wasm = Some(WasmSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            wasm_bytes,
            input_column: string_prop(&props, "inputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "text".into()),
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "result".into()),
            function: string_prop(&props, "function")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "transform".into()),
            reuse_instance: props
                .get("reuseInstance")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.ai.pii" {
        // Regex-based PII redaction. `types` is a comma-separated
        // subset of email,phone,ssn,credit_card; empty = all.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let input_column = string_prop(&props, "inputColumn")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "text".into());
        let types = string_prop(&props, "types")
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.split(',')
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        ai_pii = Some(AiPiiSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| input_column.clone()),
            input_column,
            types,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.ai.chunk" {
        // Text splitter. Local string ops only - no API. Default to
        // explode mode (one row per chunk) which is what RAG pipelines
        // typically want before feeding into xf.ai.embed.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        ai_chunk = Some(AiChunkSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            input_column: string_prop(&props, "inputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "text".into()),
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "chunk".into()),
            chunk_size: props
                .get("chunkSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
            chunk_overlap: props
                .get("chunkOverlap")
                .and_then(|v| v.as_u64())
                .unwrap_or(100) as usize,
            mode: string_prop(&props, "mode")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "explode".into()),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.ai.dedupe" {
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        ai_dedupe = Some(AiDedupeSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            embedding_column: string_prop(&props, "embeddingColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "embedding".into()),
            threshold: props
                .get("threshold")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.95),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.ai.classify" {
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let api_key = string_prop(&props, "apiKey")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: apiKey required", component_id)))?;
        let categories: Vec<String> = string_prop(&props, "categories")
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.split(',')
                    .map(|c| c.trim().to_string())
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        if categories.is_empty() {
            return Err(EngineError::Config(format!(
                "{}: categories required (comma-separated list)",
                component_id
            )));
        }
        ai_classify = Some(AiClassifySpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            input_column: string_prop(&props, "inputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "text".into()),
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "category".into()),
            categories,
            model: string_prop(&props, "model")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "gpt-4o-mini".into()),
            api_key,
            base_url: string_prop(&props, "baseUrl")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "https://api.openai.com".into()),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.ai.llm" {
        // Per-row LLM call. Renders promptTemplate with {col} subst.
        // Same credential pattern as xf.ai.embed.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let api_key = string_prop(&props, "apiKey")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: apiKey required", component_id)))?;
        ai_llm = Some(AiLlmSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            input_column: string_prop(&props, "inputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "text".into()),
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "completion".into()),
            model: string_prop(&props, "model")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "gpt-4o-mini".into()),
            api_key,
            base_url: string_prop(&props, "baseUrl")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "https://api.openai.com".into()),
            prompt_template: string_prop(&props, "promptTemplate").unwrap_or_default(),
            system_prompt: string_prop(&props, "systemPrompt").filter(|s| !s.is_empty()),
            temperature: props
                .get("temperature")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.ai.embed" {
        // Per-row embedding via an OpenAI-compatible API. The planner
        // resolves the upstream view name (the stage reads from it
        // during execution) and pins the API config. apiKey is
        // required - this stage will not run with an empty key.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let api_key = string_prop(&props, "apiKey")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: apiKey required (OpenAI / compatible)", component_id)))?;
        ai_embed = Some(AiEmbedSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            input_column: string_prop(&props, "inputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "text".into()),
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "embedding".into()),
            model: string_prop(&props, "model")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "text-embedding-3-small".into()),
            api_key,
            base_url: string_prop(&props, "baseUrl")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "https://api.openai.com".into()),
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(100) as usize,
        });
        (String::new(), StageKind::View, None)
    } else {
        // Is the node's reject port actually read downstream? Computed before
        // the body so CSV/TSV sources can switch to the tolerant pass/reject
        // split when (and only when) the reject port is wired (issue #15).
        let reject_ref = output_table_ref(&node.id, Some("reject"));
        let reject_consumers = consumer_count.get(&reject_ref).copied().unwrap_or(0);
        let body = build_view_sql(
            component_id,
            &props,
            inputs,
            node.data.schema.as_deref(),
            reject_consumers >= 1,
        ).map_err(|e| {
            EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
        })?;
        // Pick TABLE vs VIEW based on consumer count.
        //
        // VIEW (lazy): DuckDB inlines the view body into the downstream
        // query, gets predicate / projection pushdown into the underlying
        // source read, and skips an intermediate materialize-to-disk.
        // Safe when exactly one downstream consumer reads the result -
        // the body runs once, embedded in the consumer's plan.
        //
        // TABLE (materialized): forced when 2+ consumers reference this
        // node's main output, because a view would be re-evaluated by
        // each consumer. Also forced when the node's reject port is wired
        // (we want the pass / reject split materialized once each).
        // Sources that need external data injection (Oracle, REST etc.)
        // bypass this path entirely - they materialize via their own
        // runtime helpers and the planner stage stays empty.
        let main_ref = output_table_ref(&node.id, None);
        let main_consumers = consumer_count.get(&main_ref).copied().unwrap_or(0);
        // reject_consumers computed above (drives both the CSV split body and
        // whether we materialize the reject relation here). An unwired reject
        // port (the common plain-Filter case) skips the split entirely: it
        // otherwise materialized the whole rejected set to disk for nothing
        // (a 10M -> 2M filter wrote 8M rejected rows, ~12s of pure waste).
        let reject_sql = if reject_consumers >= 1 {
            build_reject_sql(component_id, &props, inputs, node.data.schema.as_deref()).map_err(|e| {
                EngineError::Config(format!("{} ({} / {}): {}", node.data.label, component_id, node.id, e))
            })?
        } else {
            None
        };
        // Dynamic PIVOT (pivot values extracted from the data) is not
        // allowed inside a view in DuckDB 1.5 - the parser rejects it
        // with "PIVOT statements with pivot elements extracted from
        // the data cannot be used in views." Force TABLE materialization
        // for components whose body uses dynamic PIVOT so they don't
        // hit that limit when the consumer-count path picks VIEW.
        let uses_dynamic_pivot =
            matches!(component_id, "xf.transpose" | "xf.pivot" | "xf.zip");
        // DUCKLE_FORCE_VIEWS=1 makes every eligible step a VIEW even when
        // multiple downstream nodes consume it (issue #5). The default
        // (single-consumer => VIEW, multi-consumer => TABLE) balances
        // recompute vs materialize; forcing views trades memory for
        // re-evaluation, which some users prefer to let DuckDB's
        // optimizer see the whole query.
        let force_views = std::env::var("DUCKLE_FORCE_VIEWS")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false);
        // Each output (pass + reject) independently picks VIEW vs TABLE by
        // its OWN consumer count. A view with a single consumer is inlined
        // into that consumer's query (predicate / projection pushdown, no
        // intermediate write); 2+ consumers get a table so the body runs
        // once. The reject side used to be unconditionally a TABLE, so a
        // consumed reject port wrote the whole rejected set (e.g. 8M rows)
        // to disk even when its only consumer was a sink that would just
        // COPY it straight out - turning a ~1.5s job into ~17s. And a
        // consumed reject no longer forces the pass side to a table either.
        // An ATTACH-backed source (postgres / mysql / motherduck / ...) must
        // materialize as a TABLE, never a lazy view. Its body reads the
        // process-local `duckle_src` alias created by the stage's ATTACH; a
        // single-consumer VIEW would be inlined into a *downstream* stage
        // whose separate CLI process never ran that ATTACH, failing with
        // "schema duckle_src does not exist". Materializing copies the rows
        // so downstream reads them with no attach needed - and matches how
        // the other external sources (Oracle / SQL Server / ADBC) already
        // behave. (Sinks take a different path and are unaffected.)
        let attach_backed = !attach.is_empty();
        // Per-stage materialization override (Properties > Basic > Materialize).
        // "view" forces a lazy VIEW even with several consumers (DUCKLE_FORCE_VIEWS
        // scoped to one node); "memory" forces a materialized run-db TABLE
        // (RAM-buffered, fast); "disk" streams through a temp parquet file (see
        // the disk branch below) for minimal RAM on huge intermediates. Both
        // "memory" and "disk" make an expensive source read once even when a
        // single downstream split would otherwise re-scan it. ("table" is kept as
        // an alias of "memory" for pipelines saved before the split.) "auto"
        // (default) keeps the single-consumer => VIEW, multi => TABLE policy.
        let materialize = props
            .get("materialize")
            .and_then(|v| v.as_str())
            .unwrap_or("auto");
        let forced_view = force_views || materialize == "view";
        let forced_table =
            matches!(materialize, "table" | "memory" | "disk" | "duckdb" | "duckdbfile");
        let view_ok = |consumers: usize| {
            !uses_dynamic_pivot
                && !attach_backed
                && !forced_table
                && (forced_view || consumers <= 1)
        };
        let main_kw = if view_ok(main_consumers) { "VIEW" } else { "TABLE" };
        // Remote / catalog sources that exactly one stage consumes: COPY the
        // already-typed rows to a temp parquet once and expose a read_parquet
        // VIEW instead of inserting them into the on-disk run-db table. The
        // parquet write is cheaper than the table insert, the consumer gets
        // projection / predicate pushdown, and it reads the parquet file with
        // no re-attach and no extension LOAD - the same proven path as
        // src.adbc, lossless because the rows are already typed. The executor
        // fills in the run-scoped temp path, so we hand it the prelude + body.
        //
        // Covers the relational / warehouse / catalog DBs (read via the
        // duckle_src ATTACH alias) and the lakehouse formats (read via the
        // iceberg_scan / delta_scan functions - a plain VIEW would fail
        // downstream because the consumer's process never LOADed the extension,
        // so COPY-to-parquet is what makes them lazy at all). EXCLUDED: local
        // file ATTACHes (sqlite / duckdb) and local file-scan sources (avro /
        // excel / spatial) - no scan bottleneck, so the round-trip would only
        // add overhead. 2+ consumers also stay a table (materialize once), and
        // reject-split components never take this branch.
        // ATTACH_PARQUET_SOURCES is defined at module scope (the consumer-count
        // pass also reads it to avoid double-counting these sources).
        // The auto fast-path: a single-consumer remote / catalog source COPYs
        // once to a temp parquet and exposes a read_parquet VIEW. Skipped when
        // the user explicitly chose Materialize=View - that intent is handled
        // as a real lazy VIEW over the live source in compile() (issue #76),
        // which gives true predicate pushdown into the source scan rather than
        // the eager full COPY this fast path performs.
        if attach_backed
            && main_consumers <= 1
            && reject_sql.is_none()
            && materialize != "view"
            && ATTACH_PARQUET_SOURCES.contains(&component_id)
        {
            attach_parquet_source = Some(AttachParquetSourceSpec {
                node_id: node.id.clone(),
                attach: attach.to_string(),
                body: body.to_string(),
            });
        }
        // Materialize=View on an attach-backed source is deliberately NOT routed
        // to a parquet COPY (that eagerly reads the whole table - the opposite
        // of the pushdown the user asked for). It stays a plain TABLE here, and
        // compile() upgrades it to a real lazy VIEW over the live source when
        // the pipeline batches into a single session and it is the sole
        // duckle_src ATTACH (issue #76). Only single-consumer sources qualify:
        // a multi-consumer VIEW would re-scan the source once per consumer, so
        // those stay a materialized TABLE (scan once).
        attach_view = materialize == "view"
            && attach_backed
            && main_consumers <= 1
            && reject_sql.is_none();
        // Materialize = "disk": stream this stage through a temp parquet file
        // (COPY ... TO parquet, then a read_parquet VIEW) instead of inserting
        // into the run-db table - minimal RAM, built for huge intermediates.
        // Reuses the attach-parquet executor path; works for any stage (attach
        // is empty for plain transforms). The reject-split case keeps the run-db
        // TABLE (the COPY would cover only the main body), so it is excluded.
        if materialize == "disk" && attach_parquet_source.is_none() && reject_sql.is_none() {
            attach_parquet_source = Some(AttachParquetSourceSpec {
                node_id: node.id.clone(),
                attach: attach.to_string(),
                body: body.to_string(),
            });
        }
        // Materialize = "duckdb" / "duckdbfile": persist this stage into a DuckDB
        // database file (a real table) and expose it as a normal run-db table for
        // downstream stages. "duckdb" uses a run-scoped temp file (swept at end);
        // "duckdbfile" writes a user-named persistent .duckdb (materializePath) so
        // the rows can be queried for analytics later. Excluded for reject-split
        // (the body would cover only the main side) and never overrides the
        // attach-parquet fast path.
        if (materialize == "duckdb" || materialize == "duckdbfile")
            && attach_parquet_source.is_none()
            && reject_sql.is_none()
        {
            let output_path = if materialize == "duckdbfile" {
                let p = string_prop(&props, "materializePath")
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| {
                        EngineError::Config(format!(
                            "{}: a DuckDB file path (materializePath) is required for the 'DuckDB file (persistent)' materialize target",
                            component_id
                        ))
                    })?;
                Some(p)
            } else {
                None
            };
            materialize_duckdb = Some(MaterializeDuckDbSpec {
                node_id: node.id.clone(),
                attach: attach.to_string(),
                body: body.to_string(),
                output_path,
            });
        }
        // Always build the logical CREATE TABLE as the stage SQL. When the
        // attach-parquet spec above is set the executor prefers it (the fast
        // parquet path) and ignores this sql; it is kept so the SQL export /
        // Copy-SQL view still shows - and redacts secrets in - the real source
        // statement instead of a bare placeholder.
        let mut sql = format!(
            "{}CREATE OR REPLACE {} {} AS {}",
            attach,
            main_kw,
            quote_ident(&node.id),
            body
        );
        // Components that split rows (filter, quality validators) also emit
        // a `<node>__reject` relation - but only when the reject port is
        // wired (see reject_sql above), and as a VIEW unless it has 2+
        // consumers, same as any other output.
        if let Some(reject_body) = reject_sql {
            let reject_table = format!("{}{}", node.id, REJECT_SUFFIX);
            let reject_kw = if view_ok(reject_consumers) { "VIEW" } else { "TABLE" };
            sql.push_str(&format!(
                "; CREATE OR REPLACE {} {} AS {}",
                reject_kw,
                quote_ident(&reject_table),
                reject_body
            ));
        }
        (sql, StageKind::View, None)
    };
    // Collapse the at-most-one set runtime spec into a single enum. Each
    // component sets exactly one of these, so the .or_else order is irrelevant.
    let runtime: Option<RuntimeSpec> = None
        .or_else(|| upsert.map(RuntimeSpec::Upsert))
        .or_else(|| text_search.map(RuntimeSpec::TextSearch))
        .or_else(|| run_job.map(|(path, vars)| RuntimeSpec::RunJob { path, vars }))
        .or_else(|| install_fallback_path.map(RuntimeSpec::InstallFallback))
        .or_else(|| iterate_pipeline_path
            .map(|path| RuntimeSpec::Iterate { path, count: iterate_count.unwrap_or(0) }))
        .or_else(|| foreach_pipeline_path
            .map(|path| RuntimeSpec::Foreach { path, concurrency: foreach_concurrency }))
        .or_else(|| log_spec.map(|(level, message)| RuntimeSpec::Log { level, message }))
        .or_else(|| die_spec.map(|(message, condition)| RuntimeSpec::Die { message, condition }))
        .or_else(|| incremental.map(RuntimeSpec::Incremental))
        .or_else(|| ducklake_cdc.map(RuntimeSpec::DuckLakeCdc))
        .or_else(|| webhook.map(RuntimeSpec::Webhook))
        .or_else(|| snowflake_sink.map(RuntimeSpec::SnowflakeSink))
        .or_else(|| databricks_sink.map(RuntimeSpec::DatabricksSink))
        .or_else(|| snowflake_source.map(RuntimeSpec::SnowflakeSource))
        .or_else(|| databricks_source.map(RuntimeSpec::DatabricksSource))
        .or_else(|| rest_source.map(RuntimeSpec::RestSource))
        .or_else(|| elastic_source.map(RuntimeSpec::ElasticSource))
        .or_else(|| mongo_sink.map(RuntimeSpec::MongoSink))
        .or_else(|| mongo_source.map(RuntimeSpec::MongoSource))
        .or_else(|| clickhouse_sink.map(RuntimeSpec::ClickhouseSink))
        .or_else(|| clickhouse_source.map(RuntimeSpec::ClickhouseSource))
        .or_else(|| sqlserver_sink.map(RuntimeSpec::SqlserverSink))
        .or_else(|| sqlserver_source.map(RuntimeSpec::SqlserverSource))
        .or_else(|| cassandra_sink.map(RuntimeSpec::CassandraSink))
        .or_else(|| cassandra_source.map(RuntimeSpec::CassandraSource))
        .or_else(|| oracle_sink.map(RuntimeSpec::OracleSink))
        .or_else(|| oracle_source.map(RuntimeSpec::OracleSource))
        .or_else(|| adbc_source.map(RuntimeSpec::AdbcSource))
        .or_else(|| attach_parquet_source.map(RuntimeSpec::AttachParquetSource))
        .or_else(|| materialize_duckdb.map(RuntimeSpec::MaterializeDuckDb))
        .or_else(|| redis_sink.map(RuntimeSpec::RedisSink))
        .or_else(|| redis_source.map(RuntimeSpec::RedisSource))
        .or_else(|| qdrant_source.map(RuntimeSpec::QdrantSource))
        .or_else(|| weaviate_source.map(RuntimeSpec::WeaviateSource))
        .or_else(|| milvus_source.map(RuntimeSpec::MilvusSource))
        .or_else(|| format_source.map(RuntimeSpec::FormatSource))
        .or_else(|| format_sink.map(RuntimeSpec::FormatSink))
        .or_else(|| kafka_sink.map(RuntimeSpec::KafkaSink))
        .or_else(|| kafka_source.map(RuntimeSpec::KafkaSource))
        .or_else(|| avro_source.map(RuntimeSpec::AvroSource))
        .or_else(|| nats_sink.map(RuntimeSpec::NatsSink))
        .or_else(|| nats_source.map(RuntimeSpec::NatsSource))
        .or_else(|| pubsub_sink.map(RuntimeSpec::PubsubSink))
        .or_else(|| pubsub_source.map(RuntimeSpec::PubsubSource))
        .or_else(|| xml_source.map(RuntimeSpec::XmlSource))
        .or_else(|| xml_sink.map(RuntimeSpec::XmlSink))
        .or_else(|| avro_sink.map(RuntimeSpec::AvroSink))
        .or_else(|| rabbit_sink.map(RuntimeSpec::RabbitSink))
        .or_else(|| rabbit_source.map(RuntimeSpec::RabbitSource))
        .or_else(|| git_source.map(RuntimeSpec::GitSource))
        .or_else(|| shell.map(RuntimeSpec::Shell))
        .or_else(|| dbt.map(RuntimeSpec::Dbt))
        .or_else(|| ftp_source.map(RuntimeSpec::FtpSource))
        .or_else(|| sftp_source.map(RuntimeSpec::SftpSource))
        .or_else(|| ftp_sink.map(RuntimeSpec::FtpSink))
        .or_else(|| sftp_sink.map(RuntimeSpec::SftpSink))
        .or_else(|| clipboard_source.map(RuntimeSpec::ClipboardSource))
        .or_else(|| email_source.map(RuntimeSpec::EmailSource))
        .or_else(|| email_sink.map(RuntimeSpec::EmailSink))
        .or_else(|| webhook_source.map(RuntimeSpec::WebhookSource))
        .or_else(|| dynamodb_source.map(RuntimeSpec::DynamodbSource))
        .or_else(|| kinesis_source.map(RuntimeSpec::KinesisSource))
        .or_else(|| ai_embed.map(RuntimeSpec::AiEmbed))
        .or_else(|| wasm.map(RuntimeSpec::Wasm))
        .or_else(|| javascript.map(RuntimeSpec::Javascript))
        .or_else(|| ai_chunk.map(RuntimeSpec::AiChunk))
        .or_else(|| ai_pii.map(RuntimeSpec::AiPii))
        .or_else(|| ai_llm.map(RuntimeSpec::AiLlm))
        .or_else(|| ai_classify.map(RuntimeSpec::AiClassify))
        .or_else(|| ai_dedupe.map(RuntimeSpec::AiDedupe))
        ;
    // Free the ATTACH alias so the next batched stage can re-ATTACH it (see
    // attach_alias above). Only stages that embed the ATTACH in their own SQL
    // qualify - the sql starts with the prelude. Runtime-spec sources/sinks
    // (the parquet fast-path, upsert, relational drivers, ...) run in their
    // own connection and either leave sql empty or have the executor ignore
    // it, so they are unaffected.
    if let Some(alias) = attach_alias {
        if sql.starts_with(attach.as_str()) {
            let trimmed = sql.trim_end();
            let sep = if trimmed.ends_with(';') { " " } else { "; " };
            sql = format!("{}{}DETACH {};", trimmed, sep, alias);
        }
    }
    Ok(Stage {
        node_id: node.id.clone(),
        component_id: component_id.to_string(),
        label: node.data.label.clone(),
        sql,
        kind,
        from,
        sink_path,
        sink_mode,
        runtime,
        wait_ms,
        retry_attempts,
        retry_backoff_ms,
        memory_limit_mb,
        attach_view,
    })
}

mod builders;
pub(crate) use builders::*;

#[cfg(test)]
mod tests;
