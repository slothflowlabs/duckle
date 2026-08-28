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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// Sinks in the same publish group become visible together or not at all.
    ///
    /// Scoped deliberately: one pipeline, one DuckLake catalog. There is no
    /// honest two-phase commit across a lake and a Postgres, and a guarantee
    /// that only sometimes holds is worse than none - so a combination that
    /// cannot be honoured is REFUSED rather than quietly downgraded.
    pub publish_group: Option<String>,
    /// For sinks: the output path + write mode, so the executor can
    /// enforce "error if exists" before writing.
    pub sink_path: Option<String>,
    pub sink_mode: Option<String>,
    /// For a file sink: the compression its COPY will use. A source that can
    /// write the destination itself reads this so the file it produces matches
    /// what the sink would have written.
    pub sink_compression: Option<String>,
    /// The sink opted in to letting its upstream source write the file itself,
    /// skipping the decode-and-re-encode pass. Off by default: it is faster but
    /// the source's Parquet writer does not compress as well as DuckDB's, so
    /// the file can be several times larger.
    pub sink_direct: bool,
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
    /// This stage is allowed to fail without ending the run.
    ///
    /// A real sequence mixes the two: the load must stop the run, while writing
    /// an audit row or sorting yesterday's files should not. Without a per-stage
    /// say, a pipeline has to abandon everything on a housekeeping step.
    ///
    /// The stage is still recorded as failed and the run still ends failed. It
    /// only decides whether the stages after it are attempted.
    pub continue_on_failure: bool,
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
    /// Optional user-defined SQL name for this stage's output relation (#102).
    /// When set, the executor also creates `CREATE OR REPLACE VIEW "<alias>" AS
    /// SELECT * FROM "<node_id>"`, so raw / pure SQL nodes downstream can refer
    /// to this node by a friendly name. Edge wiring still uses the node id.
    pub alias: Option<String>,
    /// True for a Pure SQL node: the stage SQL runs verbatim with no CREATE
    /// wrapper, so it does not produce a `"<node_id>"` relation. The executor
    /// skips the per-stage count + preview (and the batched count marker) for
    /// these, the same way it does for nodes that never create a plain relation.
    pub no_output_relation: bool,
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
        // snk.excel needs a Rust-side post-write pass (inject
        // xml:space="preserve" so Excel keeps whitespace-bearing cells, #141),
        // so it is not a bare SQL stage: force it onto the per-stage executor
        // where that hook runs instead of the collapsed single-CLI-spawn path.
        self.runtime.is_none() && self.component_id != "snk.excel"
    }
}

/// The relation a run variable's value is kept in.
///
/// Prefixed so it cannot be mistaken for, or collide with, a node's own relation.
/// Kafka transport security from a node's props.
///
/// `security` names the protocol the way the Kafka ecosystem does; credentials
/// come from the sasl* fields. Returns (tls, sasl). Both halves of the form
/// were read by nothing before this, so a node configured for SASL_SSL
/// connected in plaintext with no credentials and said nothing about it.
fn kafka_security(props: &JsonValue) -> (bool, Option<KafkaSasl>) {
    let protocol = string_prop(props, "security")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let tls = matches!(protocol.as_str(), "ssl" | "sasl_ssl");
    let user = string_prop(props, "saslUsername").filter(|s| !s.trim().is_empty());
    // Credentials drive SASL, not the dropdown: someone who fills them in and
    // leaves the protocol alone meant to authenticate.
    let sasl = user.map(|username| KafkaSasl {
        mechanism: string_prop(props, "saslMechanism")
            .map(|m| m.trim().to_string())
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "PLAIN".to_string()),
        username,
        password: string_prop(props, "saslPassword").unwrap_or_default(),
    });
    (tls, sasl)
}

pub(crate) fn run_var_relation(name: &str) -> String {
    format!("duckle_var__{name}")
}

/// How a run variable is read back in SQL.
fn run_var_read(name: &str) -> String {
    format!("(SELECT v FROM {})", quote_ident(&run_var_relation(name)))
}

/// Put the run variables a pipeline sets into the SQL of the steps that name them.
///
/// `${name}` stands for a VALUE, so it becomes a read of the one-row relation the
/// setting node wrote:
///
/// - standing on its own, it is replaced where it stands;
/// - written as a whole string literal, `'${name}'` - which is how a value is usually
///   spelled into a WHERE clause - the quotes come off with it, because what it stands
///   for is the value and not the eight characters of its name;
/// - written inside a longer literal, the literal is joined around it so it keeps its
///   shape.
///
/// Only names a node in THIS pipeline sets are touched. Every other `${...}` belongs to
/// some other pass and is left exactly as it is.
fn read_run_vars(sql: &str, names: &std::collections::BTreeSet<String>) -> String {
    if names.is_empty() || !sql.contains("${") {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len());
    let mut in_string = false;
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < sql.len() {
        if !sql.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let rest = &sql[i..];
        if rest.starts_with('\'') {
            in_string = !in_string;
            out.push('\'');
            i += 1;
            continue;
        }
        if rest.starts_with("${") {
            if let Some(end) = rest.find('}') {
                let name = rest[2..end].trim();
                if names.contains(name) {
                    let read = run_var_read(name);
                    // Inside a literal the literal is closed and reopened around the
                    // value, so text on either side of the name survives.
                    if in_string {
                        out.push_str(&format!("' || {read} || '"));
                    } else {
                        out.push_str(&read);
                    }
                    i += end + 1;
                    continue;
                }
            }
        }
        let ch = rest.chars().next().unwrap_or('\0');
        out.push(ch);
        i += ch.len_utf8();
        let _ = bytes;
    }
    // A name that WAS the whole literal leaves an empty piece on each side. They are
    // harmless but unreadable, and this SQL is read by people when a run goes wrong.
    out.replace("'' || ", "").replace(" || ''", "")
}

/// The names every `ctl.setvar` node in a pipeline sets.
pub(crate) fn run_var_names(doc: &PipelineDoc) -> std::collections::BTreeSet<String> {
    doc.nodes
        .iter()
        .filter(|n| n.data.component_id.as_deref() == Some("ctl.setvar"))
        .filter_map(|n| {
            let props = n.data.properties.as_ref()?;
            let name = props.get("name")?.as_str()?.trim();
            (!name.is_empty()).then(|| name.to_string())
        })
        .collect()
}

/// The single non-SQL action a Stage performs (or None for pure SQL).
/// Terminal variants (sources / sinks / transforms) replace the stage's
/// SQL run in the executor; control-flow variants (RunJob / Iterate /
/// Foreach / InstallFallback) run as a side effect and then fall through to
/// the stage's pass-through SQL.
#[derive(Debug)]
pub enum RuntimeSpec {
    Upsert(UpsertSpec),
    /// snk.execsource: run a CREATE TABLE AS on the remote server (#115).
    RemoteExec(RemoteExecSpec),
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
    /// `item_key` names the upstream COLUMN that identifies each iteration.
    /// Without it every iteration of one child is the same run, so they share
    /// one `xf.incremental` watermark - fine for 1 item, silent data loss for
    /// 400 tables. It is never inferred: keying on the row's position would
    /// tie a watermark to the order of the driving query.
    /// `queue` writes the work out as a batch file and returns, instead of
    /// running it here. That is the whole difference between one machine and
    /// several: the rows become a durable list that any number of workers can
    /// claim from, rather than thread waves inside this process.
    Foreach {
        path: String,
        concurrency: usize,
        item_key: Option<String>,
        queue: bool,
        /// Written into each queued item, so workers know how long to keep
        /// retrying it without having to read the pipeline that queued it.
        retry: Option<crate::batch::RetryPolicy>,
    },
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
    /// snk.salesforce: write rows into a Salesforce object via the sObject
    /// Collections API. See SalesforceSinkSpec / docs/salesforce-sink.
    SalesforceSink(SalesforceSinkSpec),
    /// snk.dhis2: chunked import into DHIS2 with import-summary parsing.
    /// See Dhis2SinkSpec for why snk.rest cannot stand in for this.
    Dhis2Sink(Dhis2SinkSpec),
    /// snk.salesforce.bulk: write rows into a Salesforce object via Bulk API
    /// 2.0's async job lifecycle. See SalesforceBulkSinkSpec.
    SalesforceBulkSink(SalesforceBulkSinkSpec),
    /// src.salesforce.bulk: read a SOQL result set via a Bulk API 2.0
    /// query job. See SalesforceBulkSourceSpec.
    SalesforceBulkSource(SalesforceBulkSourceSpec),
    SnowflakeSource(SnowflakeSourceSpec),
    DatabricksSource(DatabricksSourceSpec),
    RestSource(RestSourceSpec),
    ElasticSource(ElasticSourceSpec),
    MongoSink(MongoSinkSpec),
    HuggingFaceSink(HuggingFaceSinkSpec),
    MongoSource(MongoSourceSpec),
    LanceSink(LanceSinkSpec),
    LanceSource(LanceSourceSpec),
    /// #223: Pixeltable read/write, exchanged as Parquet through Python.
    PixeltableSink(PixeltableSinkSpec),
    PixeltableSource(PixeltableSourceSpec),
    VortexSink(VortexSinkSpec),
    VortexSource(VortexSourceSpec),
    ClickhouseSink(ClickHouseSinkSpec),
    ClickhouseSource(ClickHouseSourceSpec),
    SqlserverSink(SqlServerSinkSpec),
    SqlserverSource(SqlServerSourceSpec),
    CassandraSink(CassandraSinkSpec),
    CassandraSource(CassandraSourceSpec),
    OracleSink(OracleSinkSpec),
    OracleSource(OracleSourceSpec),
    AdbcSource(AdbcSourceSpec),
    AdbcSink(AdbcSinkSpec),
    TeradataSource(TeradataSourceSpec),
    TeradataSink(TeradataSinkSpec),
    SpoolSource(SpoolSourceSpec),
    ChangedSource(ChangedSourceSpec),
    ArtifactCopy(ArtifactCopySpec),
    DuckLakeMaintain(DuckLakeMaintainSpec),
    Tumble(TumbleSpec),
    Neo4jSource(Neo4jSourceSpec),
    Neo4jSink(Neo4jSinkSpec),
    TursoSource(TursoSourceSpec),
    TursoSink(TursoSinkSpec),
    Db2Source(Db2SourceSpec),
    Db2Sink(Db2SinkSpec),
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
    QvdSource(QvdSourceSpec),
    NatsSink(NatsSinkSpec),
    NatsSource(NatsSourceSpec),
    PubsubSink(PubSubSinkSpec),
    PubsubSource(PubSubSourceSpec),
    ModelCard(ModelCardSpec),
    PdfSource(PdfSourceSpec),
    HtmlSource(HtmlSourceSpec),
    XmlSource(XmlSourceSpec),
    XmlSink(XmlSinkSpec),
    AvroSink(AvroSinkSpec),
    QvdSink(QvdSinkSpec),
    GizmoSqlSource(GizmoSqlSourceSpec),
    GizmoSqlSink(GizmoSqlSinkSpec),
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
    /// src.runevents: the stages that have already failed in THIS run, as rows.
    ///
    /// Read off the run's own results at the moment the node executes, so it
    /// sees what happened before it and nothing after.
    RunEvents,
    /// ctl.file: one typed filesystem operation.
    FileOp(FileOpSpec),
    /// src.websocket / snk.websocket (issue #192): WebSocket client connectors.
    WebSocketSource(WebSocketSourceSpec),
    WebSocketSink(WebSocketSinkSpec),
    DynamodbSource(DynamoDbSourceSpec),
    KinesisSource(KinesisSourceSpec),
    AiEmbed(AiEmbedSpec),
    Wasm(WasmSpec),
    Javascript(JavaScriptSpec),
    Python(PythonSpec),
    AiChunk(AiChunkSpec),
    AiPii(AiPiiSpec),
    AiLlm(AiLlmSpec),
    AiClassify(AiClassifySpec),
    AiDedupe(AiDedupeSpec),
    Jq(JqSpec),
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
/// Where a cached stage keeps its answer.
///
/// Under the workspace so it is per-project and easy to delete: removing the folder is
/// the whole of "clear the cache".
pub(crate) fn cache_dir() -> Option<std::path::PathBuf> {
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|w| !w.is_empty())?;
    Some(std::path::Path::new(&ws).join(".duckle").join("duckle_cache"))
}

/// The key for a stage: what it computes, and what it reads.
///
/// The stage's own SQL carries its query and, for a file source, its path. Everything
/// upstream is folded in through `from`, so a change anywhere above invalidates
/// everything below it.
///
/// A local file also contributes its size and modified time, because the SQL names the
/// path and says nothing about the contents. That is a cheap check rather than a hash of
/// the file: hashing gigabytes on every run is the cost the cache exists to avoid.
///
/// NOT a cryptographic hash and not stable across builds. A collision serves stale data,
/// which is why this is opt-in per node rather than something applied on anyone's behalf.
fn cache_key(stage: &Stage, upstream: Option<&str>) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    stage.sql.hash(&mut h);
    upstream.hash(&mut h);
    for path in read_paths(&stage.sql) {
        if let Ok(meta) = std::fs::metadata(&path) {
            meta.len().hash(&mut h);
            if let Ok(t) = meta.modified() {
                if let Ok(d) = t.duration_since(std::time::UNIX_EPOCH) {
                    d.as_secs().hash(&mut h);
                }
            }
        }
    }
    format!("{:016x}", h.finish())
}

/// The local file paths a piece of SQL reads, so their state can go into the key.
fn read_paths(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    for piece in sql.split('\'').skip(1).step_by(2) {
        if piece.contains('/') && !piece.contains('*') && std::path::Path::new(piece).is_file() {
            out.push(piece.to_string());
        }
    }
    out
}

/// Materialise the stages that asked to be cached, and read back the ones already done.
///
/// The stage keeps its own relation name either way, so nothing downstream can tell the
/// difference between a fresh answer and a kept one.
fn apply_stage_cache(doc: &PipelineDoc, stages: &mut [Stage]) {
    let Some(dir) = cache_dir() else { return };
    apply_stage_cache_in(doc, stages, &dir);
}

/// The part that does not look at the environment, so a test can drive it with a folder
/// of its own instead of setting a variable every other test can see.
pub(crate) fn apply_stage_cache_in(doc: &PipelineDoc, stages: &mut [Stage], dir: &std::path::Path) {
    let wants: std::collections::BTreeSet<&str> = doc
        .nodes
        .iter()
        .filter(|n| {
            n.data
                .properties
                .as_ref()
                .and_then(|p| p.get("cache"))
                .and_then(JsonValue::as_bool)
                .unwrap_or(false)
        })
        .map(|n| n.id.as_str())
        .collect();
    if wants.is_empty() {
        return;
    }
    let mut keys: std::collections::BTreeMap<String, String> = Default::default();
    for i in 0..stages.len() {
        let key = cache_key(&stages[i], stages[i].from.as_deref().and_then(|f| keys.get(f)).map(|s| s.as_str()));
        keys.insert(stages[i].node_id.clone(), key.clone());
        // Only a plain SQL view is cached. A sink is a side effect and caching one would
        // skip the write it exists to do; anything with a runtime hook is not a query.
        if !wants.contains(stages[i].node_id.as_str())
            || stages[i].kind != StageKind::View
            || stages[i].runtime.is_some()
            || stages[i].no_output_relation
        {
            continue;
        }
        if std::fs::create_dir_all(dir).is_err() {
            return;
        }
        let safe: String = stages[i]
            .node_id
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect();
        let file = dir.join(format!("{safe}-{key}.parquet")).to_string_lossy().replace('\\', "/");
        let esc = file.replace('\'', "''");
        let view = format!(
            "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
            quote_ident(&stages[i].node_id),
            esc
        );
        stages[i].sql = if std::path::Path::new(&file).exists() {
            view
        } else {
            // The node's own SQL builds its relation as usual, then the answer is written
            // out and read back, so the SAME statement list both fills the cache and
            // leaves the relation the rest of the plan expects.
            format!(
                "{};
COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET);
{}",
                stages[i].sql,
                quote_ident(&stages[i].node_id),
                esc,
                view
            )
        };
    }
}

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
    // A publish group has to be checked against the WHOLE document, before the
    // filtering above hides the members this run does not reach. Inside
    // compile_impl the dropped members simply do not exist, so the group looks
    // complete and would publish the half a backward walk happened to include.
    let mut split: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
    for n in &pipeline.nodes {
        if n.data.component_id.as_deref() != Some("snk.ducklake") {
            continue;
        }
        let props = n.data.properties.clone().unwrap_or(JsonValue::Null);
        if let Some(g) = string_prop(&props, "publishGroup").filter(|g| !g.trim().is_empty()) {
            let e = split.entry(g.trim().to_string()).or_insert((false, Vec::new()));
            if keep.contains(&n.id) {
                e.0 = true;
            } else {
                e.1.push(n.data.label.clone());
            }
        }
    }
    if let Some((group, (_, missing))) = split.iter().find(|(_, (kept, m))| *kept && !m.is_empty()) {
        return Err(EngineError::Config(format!(
            "publish group '{}' cannot be honoured: '{}' is a member but it is not part of this run - a partial run that contains only some of a group cannot publish the group. Run the whole pipeline, or take that sink out of the group.",
            group,
            missing.join("', '")
        )));
    }

    // Partial runs never batch (the executor only batches when target.is_none()),
    // so suppress the live-VIEW upgrade - keep ATTACH-backed sources as
    // materialized TABLEs that survive across the per-stage processes (#87).
    compile_impl(&filtered, false)
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
    compile_impl(pipeline, true)
}

/// `allow_view_upgrade=false` is used by partial ("Run from here") runs: those
/// never take the single-session batched path (the executor only batches when
/// target.is_none()), so the #76 TABLE->live-VIEW upgrade below MUST be
/// suppressed - otherwise the source's `duckle_src_<node>` ATTACH/VIEW, created
/// in the source stage's process, would not exist when the next stage runs in
/// its own process, giving `Catalog "duckle_src_..." does not exist` (#87).
fn compile_impl(pipeline: &PipelineDoc, allow_view_upgrade: bool) -> Result<CompiledPipeline, EngineError> {
    let node_index: HashMap<&str, &PipelineNode> = pipeline
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    // #102: a node may carry a user alias used as its output relation's SQL
    // name. The executor exposes it as a view alongside the node-id relation, so
    // an alias must be unique and must not shadow another node's id - otherwise
    // the alias view would clash with a real relation. Validate up front so the
    // error is clear instead of a cryptic "view already exists" mid-run.
    {
        let mut seen: HashSet<String> = HashSet::new();
        for n in &pipeline.nodes {
            let alias = match n.data.alias.as_deref().map(str::trim) {
                Some(a) if !a.is_empty() && a != n.id => a,
                _ => continue,
            };
            if node_index.contains_key(alias) {
                return Err(EngineError::Config(format!(
                    "Node '{}' has SQL name '{}', which is already another node's id. Pick a different name.",
                    n.data.label, alias
                )));
            }
            if !seen.insert(alias.to_string()) {
                return Err(EngineError::Config(format!(
                    "SQL name '{}' is used by more than one node. Each node's SQL name must be unique.",
                    alias
                )));
            }
        }
    }

    let data_edges: Vec<&PipelineEdge> = pipeline
        .edges
        .iter()
        .filter(|e| is_data_edge(e))
        .collect();

    // Execution order honours ordering-only links as well as data ones. A
    // trigger edge - iterate, run-if, the subjob/component ok and error links -
    // says "after this", which constrains WHEN a node runs without claiming that
    // rows flow along it. The inputs map below is still built from `data_edges`
    // alone, so a trigger orders and does not wire.
    //
    // Sorting on data edges alone meant the canvas would draw a trigger and the
    // planner would ignore it, so an imported job's ordering-only steps ran in
    // whatever order the sort happened to produce.
    let order_edges: Vec<&PipelineEdge> = pipeline.edges.iter().collect();
    let order = topological_sort(&pipeline.nodes, &order_edges)?;

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
    // A sink in upsert mode re-evaluates its main upstream TWICE: the
    // DELETE ... WHERE (keys) IN (SELECT keys FROM up) and the INSERT ... SELECT
    // FROM up both reference it (build_db_sink / build_relational_sink). Count
    // its input as two consumers so an expensive single-consumer upstream
    // materializes ONCE as a TABLE instead of being re-planned per statement.
    // (merge mode references the source only once via MERGE INTO, so it is
    // intentionally excluded.)
    let upsert_sink_targets: std::collections::HashSet<&str> = node_index
        .iter()
        .filter_map(|(id, node)| {
            let is_upsert = node
                .data
                .properties
                .as_ref()
                .and_then(|p| string_prop(p, "mode"))
                .map(|m| m.eq_ignore_ascii_case("upsert"))
                .unwrap_or(false);
            if is_upsert {
                Some(*id)
            } else {
                None
            }
        })
        .collect();
    let mut consumer_count: HashMap<String, usize> = HashMap::new();
    // Source nodes whose rows a reject-wired filter reads twice (pass + reject
    // arm). Such a source must stay materialized once (#76): it is NOT eligible
    // for the live-VIEW upgrade, which would re-scan the source per arm.
    let mut feeds_reject: HashSet<String> = HashSet::new();
    // Each node's main upstream, to resolve a sink's input columns from the
    // nearest upstream node that declares a schema (#39: a transform between a
    // source and a merge sink leaves the sink's own schema empty).
    let mut main_input: HashMap<String, String> = HashMap::new();
    for edge in &data_edges {
        let port = edge
            .target_handle
            .as_deref()
            .unwrap_or("main");
        let port_key = canonical_port(port);
        if port_key == "main" {
            if reject_wired.contains(edge.target.as_str()) {
                feeds_reject.insert(edge.source.clone());
            }
            main_input.entry(edge.target.clone()).or_insert_with(|| edge.source.clone());
        }
        // Resolve which materialized table this edge actually reads, based
        // on the SOURCE node's output handle (main vs reject).
        let mut source_ref = output_table_ref(&edge.source, edge.source_handle.as_deref());
        // A Pure SQL producer registers only the relation its own body created
        // (named by the node's SQL name / alias), never a `"<node_id>"` one, so
        // a consumer on its main output must read the alias, not the id (#102).
        if source_ref == edge.source {
            if let Some(node) = node_index.get(edge.source.as_str()) {
                if let Some(alias) = pure_sql_alias_ref(node) {
                    source_ref = alias;
                }
            }
        }
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
            && (reject_wired.contains(edge.target.as_str())
                || upsert_sink_targets.contains(edge.target.as_str()))
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

    // Pre-flight contracts: opt-in, compile-time checks declared per node under
    // `properties.contracts`. `requireColumns` fails fast if a declared column is
    // not produced by the node. A best-effort PII guard taints `contracts.pii`
    // columns, follows them by name through each node's known output columns (a
    // qa.mask clears the columns it masks; a rename/derive that changes the name
    // ends tracking), and refuses to let a tagged column reach a sink unless that
    // sink sets `contracts.allowPii`. It is a guardrail, not a proof.
    fn contract_strings(props: Option<&serde_json::Value>, key: &str) -> Vec<String> {
        props
            .and_then(|p| p.get("contracts"))
            .and_then(|c| c.get(key))
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }
    fn contract_flag(props: Option<&serde_json::Value>, key: &str) -> bool {
        props
            .and_then(|p| p.get("contracts"))
            .and_then(|c| c.get(key))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
    fn masked_columns(props: Option<&serde_json::Value>) -> HashSet<String> {
        let mut out = HashSet::new();
        if let Some(p) = props {
            if let Some(arr) = p.get("masks").and_then(|v| v.as_array()) {
                for m in arr {
                    if let Some(c) = m.get("column").and_then(|v| v.as_str()) {
                        out.insert(c.to_string());
                    }
                }
            }
            if let Some(c) = p.get("column").and_then(|v| v.as_str()) {
                out.insert(c.to_string());
            }
        }
        out
    }
    {
        let mut tainted: HashMap<String, HashSet<String>> = HashMap::new();
        for node_id in &order {
            let node = match node_index.get(node_id.as_str()) {
                Some(n) => *n,
                None => continue,
            };
            if node.data.disabled.unwrap_or(false) {
                continue;
            }
            let props = node.data.properties.as_ref();
            let cid = node.data.component_id.as_deref().unwrap_or("");

            let required = contract_strings(props, "requireColumns");
            if !required.is_empty() {
                if let Some(Some(cols)) = known_columns.get(node_id) {
                    for r in &required {
                        if !cols.contains(r) {
                            return Err(EngineError::Config(format!(
                                "{} ({} / {}): contract requireColumns lists '{}', which this node does not produce",
                                node.data.label, cid, node.id, r
                            )));
                        }
                    }
                }
            }

            let mut taint: HashSet<String> = HashSet::new();
            for e in &data_edges {
                if e.target.as_str() == node_id.as_str() {
                    if let Some(up) = tainted.get(e.source.as_str()) {
                        taint.extend(up.iter().cloned());
                    }
                }
            }
            if let Some(Some(cols)) = known_columns.get(node_id) {
                taint.retain(|c| cols.contains(c));
            }
            if cid == "qa.mask" {
                for m in masked_columns(props) {
                    taint.remove(&m);
                }
            }
            for c in contract_strings(props, "pii") {
                taint.insert(c);
            }

            if cid.starts_with("snk.") && !taint.is_empty() && !contract_flag(props, "allowPii") {
                let mut cols: Vec<String> = taint.iter().cloned().collect();
                cols.sort();
                return Err(EngineError::Config(format!(
                    "{} ({} / {}): column(s) [{}] tagged PII reach this sink without masking. Add a qa.mask upstream, or set contracts.allowPii=true to allow it.",
                    node.data.label, cid, node.id, cols.join(", ")
                )));
            }

            tainted.insert(node_id.clone(), taint);
        }
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
        // Nearest upstream node's declared schema, used as the merge sink's
        // input column list when the sink itself has no schema (#39).
        let upstream_cols: Vec<String> = {
            let mut cur = main_input.get(node_id.as_str()).cloned();
            let mut found: Vec<String> = Vec::new();
            let mut hops = 0;
            while let Some(up) = cur {
                if let Some(cols) = node_index
                    .get(up.as_str())
                    .and_then(|n| n.data.schema.as_deref())
                    .map(|s| s.iter().map(|c| c.name.clone()).collect::<Vec<String>>())
                    .filter(|v| !v.is_empty())
                {
                    found = cols;
                    break;
                }
                cur = main_input.get(&up).cloned();
                hops += 1;
                if hops > 512 {
                    break;
                }
            }
            found
        };
        let mut stage = build_stage(node, component_id, node_inputs, &consumer_count, &feeds_reject, &upstream_cols)?;
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
    if allow_view_upgrade && stages.iter().any(|s| s.attach_view) {
        // An Auto attach-view candidate carries its parquet fast-path spec as a
        // fallback, which makes it non-pure-SQL; treat it as batch-compatible
        // here since the upgrade below clears that spec (so the executor then
        // sees a pure-SQL stage and takes the single-session path too).
        let would_batch = stages.len() >= 2
            && stages.iter().all(|s| {
                let upgradeable = s.attach_view
                    && matches!(s.runtime, Some(RuntimeSpec::AttachParquetSource(_)));
                (s.is_pure_sql() || upgradeable)
                    && s.retry_attempts <= 1
                    && s.wait_ms.is_none()
                    && s.memory_limit_mb.is_none()
                    && s.sink_mode.as_deref() != Some("error")
                    && !s.continue_on_failure
            });
        // Each attach-backed source now uses a unique alias (duckle_src_<node>),
        // so multiple duck sources can stay attached as live VIEWs at once - the
        // old "exactly one duckle_src" guard is no longer needed (#76 case 3).
        if would_batch {
            for s in stages.iter_mut().filter(|s| s.attach_view) {
                // TABLE -> VIEW so the consumer inlines it and pushes predicates
                // into the ducklake / duckdb / postgres scan.
                if let Some(p) = s.sql.find("CREATE OR REPLACE TABLE ") {
                    s.sql
                        .replace_range(p..p + "CREATE OR REPLACE TABLE ".len(), "CREATE OR REPLACE VIEW ");
                }
                // Keep the alias ATTACHed for the downstream stage: drop the
                // trailing "DETACH duckle_src_<node>;" the source appended, or the
                // view would dangle when the consumer reads it.
                if let Some(d) = s.sql.rfind("DETACH duckle_src") {
                    s.sql.truncate(d);
                    while s.sql.ends_with(' ') || s.sql.ends_with(';') {
                        s.sql.pop();
                    }
                }
                // Drop the Auto parquet fast-path spec (#76 case 2): the live
                // VIEW we just produced gives true pushdown, and clearing the
                // spec makes the stage pure SQL for the batched executor. Explicit
                // View has no spec, so this is a no-op there.
                s.runtime = None;
            }
        }
    }

    // #168: a persisted GEOMETRY column only round-trips its CRS in PROJJSON
    // form (what GeoParquet V1 requires) when the spatial extension is loaded
    // *before* the column is bound. A batched single-session run loads spatial
    // once via the source stage's prelude, so it covers the whole session and a
    // downstream snk.parquet writes GeoParquet fine. A partial ("Run from here")
    // run executes each stage in its own CLI process: the sink process never
    // loaded spatial, so DuckDB autoloads it only *after* binding the stored
    // geometry and reconstructs the CRS as WKT2, which the GeoParquet V1 writer
    // rejects ("only supports PROJJSON CRS definitions"). Load spatial in every
    // stage that geometry flows into so a per-stage run matches the batched one.
    {
        // Seed: stages whose own prelude already loads spatial (spatial-family
        // sources/transforms, ST_-referencing SQL) - geometry originates here.
        let mut geom_tainted: HashSet<String> = stages
            .iter()
            .filter(|s| s.sql.contains("LOAD spatial"))
            .map(|s| s.node_id.clone())
            .collect();
        // Propagate downstream along data edges: every consumer of a geometry
        // relation reads that stored GEOMETRY column and needs spatial too.
        loop {
            let mut grew = false;
            for e in &data_edges {
                if geom_tainted.contains(&e.source) && geom_tainted.insert(e.target.clone()) {
                    grew = true;
                }
            }
            if !grew {
                break;
            }
        }
        for s in stages.iter_mut() {
            if geom_tainted.contains(&s.node_id) && !s.sql.contains("LOAD spatial") {
                s.sql.insert_str(0, "INSTALL spatial; LOAD spatial; ");
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

    // A step that names a run variable reads the value the setting node wrote. This is
    // done on the finished SQL rather than on node properties because a name stands for
    // a value in SQL and nowhere else - a path or a file name cannot read a relation.
    let run_vars = run_var_names(pipeline);
    if !run_vars.is_empty() {
        for s in stages.iter_mut() {
            if s.component_id != "ctl.setvar" {
                s.sql = read_run_vars(&s.sql, &run_vars);
            }
        }
    }

    apply_stage_cache(pipeline, &mut stages);

    prepare_publish_groups(pipeline, &mut stages, &excluded)?;

    Ok(CompiledPipeline { stages, leaves })
}

/// A publish group promises its sinks become visible together or not at all.
///
/// Three things quietly shrink a group - a disabled node, a member pulled into a
/// `ctl.parallelize` branch, a "run from here" that starts below one of them -
/// and a shrunken group is worse than no group, because the promise is still
/// being made while a table silently drops out of it. So each of those REFUSES
/// the run and says which member went missing and why. Refusing is loud and
/// recoverable; publishing four of five tables and reporting success is not.
fn prepare_publish_groups(
    pipeline: &PipelineDoc,
    stages: &mut [Stage],
    excluded: &HashSet<String>,
) -> Result<(), EngineError> {
    // Declared in the document, whether or not it survived into the plan.
    let mut declared: BTreeMap<String, Vec<&PipelineNode>> = BTreeMap::new();
    for node in &pipeline.nodes {
        if node.data.component_id.as_deref() != Some("snk.ducklake") {
            continue;
        }
        let props = node.data.properties.clone().unwrap_or(JsonValue::Null);
        if let Some(g) = string_prop(&props, "publishGroup").filter(|g| !g.trim().is_empty()) {
            declared.entry(g.trim().to_string()).or_default().push(node);
        }
    }
    if declared.is_empty() {
        return Ok(());
    }

    let planned: HashSet<String> = stages
        .iter()
        .filter(|s| s.publish_group.is_some())
        .map(|s| s.node_id.clone())
        .collect();

    for (group, members) in &declared {
        for node in members {
            if planned.contains(&node.id) {
                continue;
            }
            let why = if node.data.disabled.unwrap_or(false) {
                "it is disabled"
            } else if excluded.contains(&node.id) {
                "it was pulled into a ctl.parallelize branch, which runs as its own sub-pipeline and cannot share this run's transaction"
            } else {
                "it is not part of this run - a partial run that contains only some of a group cannot publish the group"
            };
            return Err(EngineError::Config(format!(
                "publish group '{}' cannot be honoured: '{}' is a member but {}. Either include it or take it out of the group - publishing the rest would claim an atomicity that no longer holds.",
                group,
                node.data.label,
                why
            )));
        }
        // One transaction reaches one catalog. Two lakes are two commits, and
        // no ordering of them makes both land or neither.
        let mut lakes: Vec<(String, String)> = Vec::new();
        for node in members {
            let props = node.data.properties.clone().unwrap_or(JsonValue::Null);
            let path = string_prop(&props, "path").unwrap_or_default();
            let who = node.data.label.clone();
            lakes.push((path, who));
        }
        if let Some((first, _)) = lakes.first().cloned() {
            if let Some((other, who)) = lakes.iter().find(|(p, _)| *p != first) {
                return Err(EngineError::Config(format!(
                    "publish group '{}' spans two DuckLake catalogs ('{}' and '{}', the second on '{}'). One transaction commits to one catalog; two catalogs are two commits and cannot be made atomic together.",
                    group, first, other, who
                )));
            }
        }

        // Every sink normally attaches the lake, writes, and detaches again. Inside
        // a shared transaction that is wrong twice over: the second ATTACH collides
        // on the alias, and a DETACH with the transaction's writes still uncommitted
        // discards them. So the group attaches ONCE, on its first member, and the
        // attachment stays open until the COMMIT the executor emits after the last.
        //
        // One attachment also keeps the whole group inside a single transaction
        // participant, which is what makes the commit one snapshot rather than a
        // race between two handles on the same catalog.
        let props = members[0].data.properties.clone().unwrap_or(JsonValue::Null);
        let prelude = builders::ducklake_attach(&props, false);
        let detach = "DETACH duckle_dst;";
        let mut seen_first = false;
        for st in stages.iter_mut() {
            if st.publish_group.as_deref() != Some(group.as_str()) {
                continue;
            }
            let trimmed = st.sql.trim_end();
            if let Some(rest) = trimmed.strip_suffix(detach) {
                st.sql = rest.trim_end().to_string();
            }
            if seen_first {
                if let Some(rest) = st.sql.strip_prefix(prelude.as_str()) {
                    st.sql = rest.to_string();
                }
            }
            seen_first = true;
        }
    }

    // DuckDB allows one transaction to write to exactly ONE attached database.
    // Stages between two group members create their relations in the run
    // database, so a transaction spanning them dies with "a single transaction
    // can only write to a single attached database" - verified, it is a hard
    // error and not a warning.
    //
    // So the group's sinks are moved to the end, contiguously, keeping their
    // relative order. That is safe for every plan that can reach this point: a
    // group is only honoured on the batched path, the batched path requires
    // every stage to be pure SQL, and a pure-SQL stage that is not a sink is a
    // view definition - nothing after the group is waiting on it. The sort is
    // stable, so the stages that move keep the order the planner gave them.
    stages.sort_by_key(|s| s.publish_group.is_some());
    Ok(())
}

mod graph;
use graph::*;

/// Key columns for a sink's "upsert" write mode, or empty for plain insert.
/// Driver sinks (SQL Server / Oracle / Snowflake / Databricks) MERGE on these
/// when the form sets `mode = "upsert"` and supplies `conflictColumns`.
///
/// Asking for upsert without usable keys is refused rather than carried on
/// with. Every one of these sinks treats an empty key list as "plain insert",
/// so the run reported success while appending the whole input again on each
/// execution - a doubling that only shows up as a row count much later. If
/// the write cannot be keyed the way it was asked for, it must not happen.
fn upsert_keys_from(props: &JsonValue, component_id: &str) -> Result<Vec<String>, EngineError> {
    if string_prop(props, "mode").as_deref() != Some("upsert") {
        return Ok(Vec::new());
    }
    let keys = columns_list(props, "conflictColumns");
    if keys.is_empty() {
        return Err(EngineError::Config(format!(
            "{}: mode \"upsert\" needs conflictColumns - the column(s) identifying \
             an existing row, e.g. conflictColumns: [\"id\"]. Without them the write \
             would insert every row again on each run.",
            component_id
        )));
    }
    Ok(keys)
}

/// Write mode for snk.mongodb, checked against what the sink actually honours.
///
/// The sink drops the collection only on "replace" and otherwise inserts, so an
/// unrecognised mode used to mean "append" no matter what was asked for: a
/// pipeline set to "overwrite" doubled the collection on every run while
/// reporting success. The word is also a fair guess, since snk.duckdb,
/// snk.postgres and snk.csv all spell this "overwrite" - so accept it as an
/// alias for "replace" and reject anything genuinely unknown.
fn mongo_write_mode(props: &JsonValue, component_id: &str) -> Result<String, EngineError> {
    match string_prop(props, "mode").filter(|s| !s.is_empty()) {
        None => Ok("insert".into()),
        Some(m) => match m.as_str() {
            "insert" | "replace" | "upsert" => Ok(m),
            "overwrite" => Ok("replace".into()),
            other => Err(EngineError::Config(format!(
                "{}: unknown mode {:?} - expected \"insert\", \"replace\" (drop the \
                 collection first) or \"upsert\"",
                component_id, other
            ))),
        },
    }
}

/// Delete-propagation control column for a sink's "upsert" write mode. When
/// the form sets `mode = "upsert"` and a `deleteColumn`, rows whose value in
/// that column equals `deleteValue` are removed from the target by key instead
/// of being upserted - this is how CDC deletes (xf.cdc.diff change_type /
/// DuckLake CDC) flow through. Returns None outside upsert mode or when unset.
/// snk.snowflake `writeMode`: "append" (default) inserts, "overwrite" empties the
/// target first. "upsert" is spelled by supplying key columns, so asking for both
/// is a contradiction rather than a precedence question, and is refused.
fn snowflake_truncate_first(props: &JsonValue, component_id: &str) -> Result<bool, EngineError> {
    let mode = string_prop(props, "writeMode")
        .or_else(|| string_prop(props, "mode"))
        .filter(|s| !s.is_empty());
    match mode.as_deref() {
        // "upsert" is spelled on `mode`, which this also reads when `writeMode` is unset,
        // so the documented way of asking for one was refused as an unknown write mode
        // unless a redundant writeMode sat beside it. An upsert merges into what is
        // already there, which is the one thing it certainly does not truncate.
        None | Some("append") | Some("insert") | Some("upsert") => Ok(false),
        Some("overwrite") | Some("replace") => {
            if !upsert_keys_from(props, component_id)?.is_empty() {
                return Err(EngineError::Config(format!(
                    "{}: writeMode overwrite empties the table, and upsert keys merge into what is already there. Choose one.",
                    component_id
                )));
            }
            Ok(true)
        }
        Some(other) => Err(EngineError::Config(format!(
            "{}: unknown writeMode {:?} - expected \"append\" or \"overwrite\"",
            component_id, other
        ))),
    }
}

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

/// Parse ADBC database options from a node's `options` array (key/value pairs)
/// plus the optional bare `uri` convenience key. Shared by src.adbc-style
/// wrappers and the ADBC ingest sink.
fn adbc_db_options(props: &JsonValue) -> Vec<(String, String)> {
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
    if let Some(uri) = string_prop(props, "uri").filter(|s| !s.is_empty()) {
        options.push(("uri".to_string(), uri));
    }
    options
}

/// Build a Teradata ODBC connection string from a node's props. Precedence:
/// an explicit `connectionString` wins; otherwise a `dsn`; otherwise the
/// friendly `driver` + `host` (DBCNAME) fields. UID / PWD / DATABASE are layered
/// on in every case, and CharacterSet=UTF8 is appended so the driver returns
/// UTF-8 text. The result carries the password, so callers must never log it.
fn teradata_conn_string(props: &JsonValue) -> Result<String, EngineError> {
    if let Some(cs) = string_prop(props, "connectionString").filter(|s| !s.is_empty()) {
        return Ok(cs);
    }
    let mut parts: Vec<String> = Vec::new();
    if let Some(dsn) = string_prop(props, "dsn").filter(|s| !s.is_empty()) {
        parts.push(format!("DSN={}", dsn));
    } else {
        let driver = string_prop(props, "driver")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Teradata Database ODBC Driver 17.20".to_string());
        let host = string_prop(props, "host")
            .or_else(|| string_prop(props, "dbcName"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(
                    "teradata: host (DBCNAME), or a dsn / connectionString, is required".into(),
                )
            })?;
        parts.push(format!("DRIVER={{{}}}", driver));
        parts.push(format!("DBCNAME={}", host));
    }
    if let Some(u) = string_prop(props, "user")
        .or_else(|| string_prop(props, "username"))
        .filter(|s| !s.is_empty())
    {
        parts.push(format!("UID={}", u));
    }
    if let Some(p) = string_prop(props, "password").filter(|s| !s.is_empty()) {
        parts.push(format!("PWD={}", p));
    }
    if let Some(d) = string_prop(props, "database").filter(|s| !s.is_empty()) {
        parts.push(format!("DATABASE={}", d));
    }
    parts.push("CharacterSet=UTF8".to_string());
    Ok(parts.join(";"))
}

/// Build an IBM DB2 ODBC connection string from a node's props. Same
/// precedence as Teradata: an explicit `connectionString` wins, otherwise a
/// `dsn`, otherwise the friendly `driver` + `host` + `port` + `database`
/// fields. DB2 needs DATABASE at connect time - unlike Teradata, where it only
/// sets the default schema - so it is required in the friendly form. The
/// result carries the password, so callers must never log it.
fn db2_conn_string(props: &JsonValue) -> Result<String, EngineError> {
    if let Some(cs) = string_prop(props, "connectionString").filter(|s| !s.is_empty()) {
        return Ok(cs);
    }
    let mut parts: Vec<String> = Vec::new();
    if let Some(dsn) = string_prop(props, "dsn").filter(|s| !s.is_empty()) {
        parts.push(format!("DSN={}", dsn));
    } else {
        let driver = string_prop(props, "driver")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "IBM DB2 ODBC DRIVER".to_string());
        let host = string_prop(props, "host")
            .or_else(|| string_prop(props, "hostname"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(
                    "db2: host, or a dsn / connectionString, is required".into(),
                )
            })?;
        let database = string_prop(props, "database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(
                    "db2: database is required (DB2 selects the database at connect time)".into(),
                )
            })?;
        let port = props
            .get("port")
            .and_then(|v| v.as_u64())
            .or_else(|| string_prop(props, "port").and_then(|s| s.parse().ok()))
            .unwrap_or(50000);
        parts.push(format!("DRIVER={{{}}}", driver));
        parts.push(format!("HOSTNAME={}", host));
        parts.push(format!("PORT={}", port));
        parts.push(format!("DATABASE={}", database));
        // Without PROTOCOL the IBM driver assumes a local catalogued alias
        // rather than a TCP connection, which fails with SQL1013N.
        parts.push("PROTOCOL=TCPIP".to_string());
    }
    if let Some(u) = string_prop(props, "user")
        .or_else(|| string_prop(props, "username"))
        .filter(|s| !s.is_empty())
    {
        parts.push(format!("UID={}", u));
    }
    if let Some(pw) = string_prop(props, "password").filter(|s| !s.is_empty()) {
        parts.push(format!("PWD={}", pw));
    }
    if props.get("useSsl").and_then(|v| v.as_bool()).unwrap_or(false) {
        parts.push("SECURITY=SSL".to_string());
    }
    Ok(parts.join(";"))
}

/// Sanitize a node id into a SQL-identifier-safe alias suffix (#76 per-source
/// aliases). Non-alphanumeric chars become `_`; the `duckle_src_` prefix the
/// caller prepends guarantees it never starts with a digit.
fn alias_suffix(node_id: &str) -> String {
    let s: String = node_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if s.is_empty() { "src".to_string() } else { s }
}

/// Replace every token-boundaried occurrence of `from` in `sql` with `to`. A
/// match counts only when the char after `from` is not an identifier char, so
/// renaming the alias `duckle_src` (e.g. `AS duckle_src`, `duckle_src.tbl`,
/// `'duckle_src'`) never corrupts a longer identifier like a user table named
/// `duckle_src_x`.
fn rename_token(sql: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < sql.len() {
        if sql[i..].starts_with(from) {
            let after_ident = bytes
                .get(i + from.len())
                .map(|c| c.is_ascii_alphanumeric() || *c == b'_')
                .unwrap_or(false);
            if !after_ident {
                out.push_str(to);
                i += from.len();
                continue;
            }
        }
        let ch = sql[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// The credentials a node needs to reach an artifact URI.
///
/// One reader for every node that can open one, so `src.pdf`, `src.xml` and
/// `xf.artifact.copy` all take the same property names. Three readers would be
/// three conventions that agree until one of them is changed.
fn artifact_auth_from_props(props: &JsonValue) -> ArtifactAuth {
    ArtifactAuth {
        s3: crate::s3::S3Config::from_props(props),
        headers: builders::headers_from_props(props),
        user: string_prop(props, "user").filter(|s| !s.is_empty()),
        password: string_prop(props, "password").filter(|s| !s.is_empty()),
        private_key: string_prop(props, "privateKey").filter(|s| !s.is_empty()),
        key_passphrase: string_prop(props, "keyPassphrase").filter(|s| !s.is_empty()),
        host_fingerprint: string_prop(props, "hostFingerprint").filter(|s| !s.is_empty()),
    }
}

/// A parser's optional artifact input.
///
/// `from_view` is None when nothing is wired in, which is what keeps every
/// existing path-configured pipeline working unchanged.
fn artifact_input_from_props(props: &JsonValue, from_view: Option<&str>) -> ArtifactInput {
    ArtifactInput {
        from_view: from_view.map(str::to_string),
        uri_column: string_prop(props, "uriColumn")
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "uri".to_string()),
        sha_column: string_prop(props, "shaColumn")
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "sha256".to_string()),
        auth: artifact_auth_from_props(props),
    }
}

fn build_stage(
    node: &PipelineNode,
    component_id: &str,
    inputs: &NodeInputs,
    consumer_count: &HashMap<String, usize>,
    feeds_reject: &HashSet<String>,
    upstream_cols: &[String],
) -> Result<Stage, EngineError> {
    let props = node
        .data
        .properties
        .as_ref()
        .cloned()
        .unwrap_or(JsonValue::Null);
    let mut sink_path: Option<String> = None;
    let mut sink_compression: Option<String> = None;
    let mut sink_direct = false;
    let mut sink_mode: Option<String> = None;
    let mut upsert: Option<UpsertSpec> = None;
    let mut text_search: Option<TextSearchSpec> = None;
    let mut webhook: Option<WebhookSpec> = None;
    let mut remote_exec: Option<RemoteExecSpec> = None;
    let mut run_job: Option<(String, Vec<(String, String)>)> = None;
    let mut install_fallback_path: Option<String> = None;
    let mut iterate_pipeline_path: Option<String> = None;
    let mut iterate_count: Option<u64> = None;
    let mut foreach_pipeline_path: Option<String> = None;
    let mut foreach_concurrency: usize = 1;
    let mut foreach_item_key: Option<String> = None;
    let mut foreach_queue = false;
    let mut foreach_retry: Option<crate::batch::RetryPolicy> = None;
    // (level, message) for ctl.log / ctl.warn; (message, condition) for ctl.die.
    let mut log_spec: Option<(String, String)> = None;
    let mut die_spec: Option<(String, String)> = None;
    let mut incremental: Option<IncrementalSpec> = None;
    let mut ducklake_cdc: Option<DuckLakeCdcSpec> = None;
    let mut snowflake_sink: Option<SnowflakeSinkSpec> = None;
    let mut databricks_sink: Option<DatabricksSinkSpec> = None;
    let mut salesforce_sink: Option<SalesforceSinkSpec> = None;
    let mut dhis2_sink: Option<Dhis2SinkSpec> = None;
    let mut salesforce_bulk_sink: Option<SalesforceBulkSinkSpec> = None;
    let mut salesforce_bulk_source: Option<SalesforceBulkSourceSpec> = None;
    let mut snowflake_source: Option<SnowflakeSourceSpec> = None;
    let mut databricks_source: Option<DatabricksSourceSpec> = None;
    let mut rest_source: Option<RestSourceSpec> = None;
    let mut elastic_source: Option<ElasticSourceSpec> = None;
    let mut mongo_sink: Option<MongoSinkSpec> = None;
    let mut huggingface_sink: Option<HuggingFaceSinkSpec> = None;
    let mut mongo_source: Option<MongoSourceSpec> = None;
    let mut lance_sink: Option<LanceSinkSpec> = None;
    let mut lance_source: Option<LanceSourceSpec> = None;
    let mut pixeltable_sink: Option<PixeltableSinkSpec> = None;
    let mut pixeltable_source: Option<PixeltableSourceSpec> = None;
    let mut vortex_sink: Option<VortexSinkSpec> = None;
    let mut vortex_source: Option<VortexSourceSpec> = None;
    let mut clickhouse_sink: Option<ClickHouseSinkSpec> = None;
    let mut clickhouse_source: Option<ClickHouseSourceSpec> = None;
    let mut sqlserver_sink: Option<SqlServerSinkSpec> = None;
    let mut sqlserver_source: Option<SqlServerSourceSpec> = None;
    let mut cassandra_sink: Option<CassandraSinkSpec> = None;
    let mut cassandra_source: Option<CassandraSourceSpec> = None;
    let mut oracle_sink: Option<OracleSinkSpec> = None;
    let mut oracle_source: Option<OracleSourceSpec> = None;
    let mut adbc_source: Option<AdbcSourceSpec> = None;
    let mut adbc_sink: Option<AdbcSinkSpec> = None;
    let mut teradata_source: Option<TeradataSourceSpec> = None;
    let mut teradata_sink: Option<TeradataSinkSpec> = None;
    let mut spool_source: Option<SpoolSourceSpec> = None;
    let mut changed_source: Option<ChangedSourceSpec> = None;
    let mut artifact_copy: Option<ArtifactCopySpec> = None;
    let mut ducklake_maintain: Option<DuckLakeMaintainSpec> = None;
    let mut tumble: Option<TumbleSpec> = None;
    let mut neo4j_source: Option<Neo4jSourceSpec> = None;
    let mut neo4j_sink: Option<Neo4jSinkSpec> = None;
    let mut turso_source: Option<TursoSourceSpec> = None;
    let mut turso_sink: Option<TursoSinkSpec> = None;
    let mut db2_source: Option<Db2SourceSpec> = None;
    let mut db2_sink: Option<Db2SinkSpec> = None;
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
    let mut qvd_source: Option<QvdSourceSpec> = None;
    let mut nats_sink: Option<NatsSinkSpec> = None;
    let mut nats_source: Option<NatsSourceSpec> = None;
    let mut pubsub_sink: Option<PubSubSinkSpec> = None;
    let mut pubsub_source: Option<PubSubSourceSpec> = None;
    let mut model_card: Option<ModelCardSpec> = None;
    let mut pdf_source: Option<PdfSourceSpec> = None;
    let mut html_source: Option<HtmlSourceSpec> = None;
    let mut xml_source: Option<XmlSourceSpec> = None;
    let mut xml_sink: Option<XmlSinkSpec> = None;
    let mut avro_sink: Option<AvroSinkSpec> = None;
    let mut qvd_sink: Option<QvdSinkSpec> = None;
    let mut gizmosql_source: Option<GizmoSqlSourceSpec> = None;
    let mut gizmosql_sink: Option<GizmoSqlSinkSpec> = None;
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
    let mut websocket_source: Option<WebSocketSourceSpec> = None;
    let mut websocket_sink: Option<WebSocketSinkSpec> = None;
    let mut email_source: Option<EmailSourceSpec> = None;
    let mut email_sink: Option<EmailSinkSpec> = None;
    let mut webhook_source: Option<WebhookSourceSpec> = None;
    let mut run_events = false;
    let mut file_op: Option<FileOpSpec> = None;
    let mut dynamodb_source: Option<DynamoDbSourceSpec> = None;
    let mut kinesis_source: Option<KinesisSourceSpec> = None;
    let mut ai_embed: Option<AiEmbedSpec> = None;
    let mut wasm: Option<WasmSpec> = None;
    let mut javascript: Option<JavaScriptSpec> = None;
    let mut jq: Option<JqSpec> = None;
    let mut python: Option<PythonSpec> = None;
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
    let continue_on_failure = props
        .get("continueOnFailure")
        .and_then(|v| {
            v.as_bool()
                .or_else(|| v.as_str().map(|t| t.eq_ignore_ascii_case("true")))
        })
        .unwrap_or(false);
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
    // Set true by the Pure SQL branch: the stage runs verbatim and creates no
    // `"<node_id>"` relation, so the executor skips its count + preview (#102).
    let mut no_output_relation = false;
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
            text_template: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.websocket" {
        // WebSocket client sink (#192): connect, send each upstream row as a
        // text frame (whole row as JSON, or one column), close.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!(
                "{}: url required (ws:// or wss://)", component_id
            )))?;
        websocket_sink = Some(WebSocketSinkSpec {
            from_view: from_view.to_string(),
            url,
            message_column: string_prop(&props, "messageColumn").filter(|s| !s.is_empty()),
            headers: headers_from_props(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.dhis2" {
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config("snk.dhis2 needs an upstream input".into()))?;
        let url = string_prop(&props, "url").unwrap_or_default();
        if url.is_empty() {
            return Err(EngineError::Config(
                "snk.dhis2: set url to the import endpoint, e.g. \
                 https://<host>/api/dataValueSets or https://<host>/api/tracker"
                    .into(),
            ));
        }
        // Reuse the shared REST auth builder so ApiToken / Basic / Bearer all
        // behave exactly as they do on the source side.
        let mut auth_headers: Vec<(String, String)> = Vec::new();
        builders::push_rest_auth(&mut auth_headers, &props);
        let import_type = string_prop(&props, "importType").unwrap_or_else(|| "aggregate".into());
        if import_type != "aggregate" && import_type != "tracker" {
            return Err(EngineError::Config(format!(
                "snk.dhis2: importType must be 'aggregate' or 'tracker', got '{}'",
                import_type
            )));
        }
        let tracker_resource =
            string_prop(&props, "trackerResource").unwrap_or_else(|| "events".into());
        if import_type == "tracker"
            && !matches!(
                tracker_resource.as_str(),
                "trackedEntities" | "events" | "enrollments" | "relationships"
            )
        {
            return Err(EngineError::Config(format!(
                "snk.dhis2: trackerResource must be one of trackedEntities / events / \
                 enrollments / relationships, got '{}'",
                tracker_resource
            )));
        }
        dhis2_sink = Some(Dhis2SinkSpec {
            from_view: from_view.to_string(),
            url,
            auth_header: auth_headers.into_iter().next(),
            import_type,
            tracker_resource,
            import_strategy: string_prop(&props, "importStrategy")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "CREATE_AND_UPDATE".into()),
            chunk_size: props.get("chunkSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000) as usize,
            dry_run: props.get("dryRun").and_then(|v| v.as_bool()).unwrap_or(false),
            atomic_mode: string_prop(&props, "atomicMode")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "ALL".into()),
            fail_on_conflict: props.get("failOnConflict").and_then(|v| v.as_bool()).unwrap_or(true),
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
        // #147: a "text" body type renders each row through a template and
        // newline-joins them into one raw body (InfluxDB Line Protocol / QuestDB
        // /write and other line-oriented endpoints). Otherwise the JSON / form
        // shapes apply, keyed on bodyShape (engine-native) then batchMode
        // (form-native): 'one' -> per-row, 'array' -> batched.
        let body_type = string_prop(&props, "bodyType").unwrap_or_default();
        let text_template = if body_type == "text" {
            Some(string_prop(&props, "bodyTemplate").unwrap_or_default())
        } else {
            None
        };
        let body_shape = if body_type == "text" {
            "text".to_string()
        } else {
            string_prop(&props, "bodyShape")
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    string_prop(&props, "batchMode").map(|m| match m.as_str() {
                        "array" => "batch".into(),
                        _ => "row".into(),
                    })
                })
                .unwrap_or_else(|| {
                    if component_id == "snk.webhook" {
                        "row".into()
                    } else {
                        "batch".into()
                    }
                })
        };
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
            text_template,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.execsource" {
        // #115 in-database processing v1b: run the transform on the source
        // server itself (no round-trip). Bind the server as duckle_dst, then
        // CREATE TABLE <dest> AS <sql> via the extension's execute passthrough
        // (postgres_execute / mysql_execute). Self-contained: no upstream input.
        let engine = string_prop(&props, "engine").unwrap_or_else(|| "postgres".into());
        let (ext, port, exec_fn, default_schema) = if engine == "mysql" {
            ("mysql", 3306u64, "mysql_execute", None)
        } else {
            ("postgres", 5432u64, "postgres_execute", Some("public"))
        };
        let attach = db_attach(&props, ext, port, false);
        if attach.is_empty() {
            return Err(EngineError::Config(format!(
                "{}: connection is incomplete (host or connection string required)",
                component_id
            )));
        }
        let sql = string_prop(&props, "sql")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: SQL query is required", component_id)))?;
        let table = string_prop(&props, "destTable")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!("{}: destination table is required", component_id))
            })?;
        // Native (server-side) qualified name, quoted in the target's own
        // dialect: Postgres uses ANSI double quotes and a schema.table shape
        // (default public); MySQL uses backticks and selects the database at
        // ATTACH, so there is no schema layer.
        let dest = if ext == "mysql" {
            format!("`{}`", table.replace('`', "``"))
        } else {
            let schema = string_prop(&props, "destSchema")
                .filter(|s| !s.trim().is_empty())
                .or_else(|| default_schema.map(|s| s.to_string()));
            match schema {
                Some(s) => format!("{}.{}", quote_ident(&s), quote_ident(&table)),
                None => quote_ident(&table),
            }
        };
        let inner = sql.trim().trim_end_matches(';').trim().to_string();
        // Overwrite (default) drops first; "create" appends to a fresh table.
        let mut statements = Vec::new();
        if string_prop(&props, "mode").as_deref() != Some("create") {
            statements.push(format!("DROP TABLE IF EXISTS {dest}"));
        }
        statements.push(format!("CREATE TABLE {dest} AS {inner}"));
        remote_exec = Some(RemoteExecSpec {
            attach,
            exec_fn: exec_fn.to_string(),
            statements,
        });
        (String::new(), StageKind::Sink, None)
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
            text_template: None,
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
            text_template: None,
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
            text_template: None,
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
            text_template: None,
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
            upsert_keys: upsert_keys_from(&props, component_id)?,
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
            mode: string_prop(&props, "mode").unwrap_or_else(|| "append".into()),
            upsert_keys: upsert_keys_from(&props, component_id)?,
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
    } else if (component_id == "snk.sqlserver" || component_id == "snk.synapse")
        && !props.get("bulk").and_then(|v| v.as_bool()).unwrap_or(true)
    {
        // bulk=false: the row-by-row tiberius driver path (works offline, no
        // extension). The DEFAULT (bulk=true, #86) instead falls through to the
        // generic attach-sink path below, which ATTACHes via the DuckDB mssql
        // community extension and bulk-writes through COPY/INSERT (~1.2M rows/s).
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
            mode: string_prop(&props, "mode").unwrap_or_else(|| "append".into()),
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000) as usize,
            trust_cert: props.get("trustCert").and_then(|v| v.as_bool()).unwrap_or(false),
            encrypt: props.get("encrypt").and_then(|v| v.as_bool()).unwrap_or(true),
            upsert_keys: upsert_keys_from(&props, component_id)?,
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
            mode: mongo_write_mode(&props, component_id)?,
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
            upsert_keys: upsert_keys_from(&props, component_id)?,
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.huggingface" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let repo = string_prop(&props, "repo")
            .map(|s| {
                s.trim()
                    .trim_start_matches("hf://")
                    .trim_start_matches("datasets/")
                    .trim_matches('/')
                    .to_string()
            })
            .filter(|s| s.contains('/'))
            .ok_or_else(|| {
                EngineError::Config(format!("{}: repo required as user/dataset", component_id))
            })?;
        // A write token is mandatory - unlike the read side there is no
        // public write path on the Hub.
        let token = string_prop(&props, "token")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "{}: a write-scoped token is required to push to the Hub",
                    component_id
                ))
            })?;
        let path = string_prop(&props, "path")
            .map(|s| s.trim().trim_start_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "data/train.parquet".to_string());
        let commit_message = string_prop(&props, "commitMessage")
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("Add {}", path));
        huggingface_sink = Some(HuggingFaceSinkSpec {
            from_view: from_view.to_string(),
            repo,
            path,
            revision: string_prop(&props, "revision")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "main".into()),
            token,
            private: props.get("private").and_then(|v| v.as_bool()).unwrap_or(false),
            commit_message,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.lancedb" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let uri = string_prop(&props, "uri")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: uri required", component_id)))?;
        let table = string_prop(&props, "table")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: table required", component_id)))?;
        lance_sink = Some(LanceSinkSpec {
            from_view: from_view.to_string(),
            uri,
            table,
            mode: string_prop(&props, "mode").unwrap_or_else(|| "create".into()),
            api_key: string_prop(&props, "apiKey").filter(|s| !s.is_empty()),
            region: string_prop(&props, "region").filter(|s| !s.is_empty()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.pixeltable" {
        // #223. Mode mirrors the other sinks: `insert` appends to a table that
        // already exists, `create` builds it from the incoming rows.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let table = string_prop(&props, "table")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: table required", component_id)))?;
        pixeltable_sink = Some(PixeltableSinkSpec {
            from_view: from_view.to_string(),
            table,
            mode: string_prop(&props, "mode").unwrap_or_else(|| "insert".into()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.vortex" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        vortex_sink = Some(VortexSinkSpec {
            from_view: from_view.to_string(),
            path,
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
            upsert_keys: upsert_keys_from(&props, component_id)?,
            delete_column: delete_column_from(&props),
            delete_value: delete_value_from(&props),
            truncate_first: snowflake_truncate_first(&props, component_id)?,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.salesforce" {
        // Salesforce REST write sink (Tier 1: sObject Collections, <=200/req).
        // Auth: Bearer OAuth access token, same token as src.salesforce.
        // instance_url doubles as the endpoint base tests point at a mock.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        // #166: OAuth client-credentials mints a fresh token per run from the
        // durable clientId/clientSecret, so instanceUrl + accessToken are only
        // required in the default Bearer mode. In client-credentials mode the
        // token response supplies both the access token and the instance_url.
        let oauth = rest_oauth_from_props(&props, true)?;
        let instance_url = string_prop(&props, "instanceUrl")
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        let access_token = string_prop(&props, "accessToken").filter(|s| !s.is_empty());
        if oauth.is_none() {
            if instance_url.is_none() {
                return Err(EngineError::Config(format!(
                    "{}: instanceUrl required (e.g. https://acme.my.salesforce.com)",
                    component_id
                )));
            }
            if access_token.is_none() {
                return Err(EngineError::Config(format!(
                    "{}: accessToken required (Bearer OAuth token; use ${{ENV:SF_TOKEN}}), \
                     or set Auth mode to OAuth Client Credentials",
                    component_id
                )));
            }
        }
        let instance_url = instance_url.unwrap_or_default();
        let access_token = access_token.unwrap_or_default();
        let object = string_prop(&props, "object")
            .or_else(|| string_prop(&props, "sobject"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!(
                "{}: object required (e.g. Account)", component_id
            )))?;
        let operation = string_prop(&props, "operation")
            .unwrap_or_else(|| "insert".into())
            .to_lowercase();
        if !matches!(operation.as_str(), "insert" | "update" | "upsert" | "delete") {
            return Err(EngineError::Config(format!(
                "{}: operation must be insert|update|upsert|delete (got '{}')",
                component_id, operation
            )));
        }
        let external_id_field = string_prop(&props, "externalIdField")
            .filter(|s| !s.is_empty());
        if operation == "upsert" && external_id_field.is_none() {
            return Err(EngineError::Config(format!(
                "{}: externalIdField required when operation = upsert", component_id
            )));
        }
        // Bulk API 2.0 now has its own node rather than being a mode of this
        // one. Saved pipelines carrying the never-implemented api='bulk' get
        // pointed at it instead of silently falling back to Collections.
        let api = match string_prop(&props, "api").unwrap_or_else(|| "collections".into()).as_str() {
            "bulk" => return Err(EngineError::Config(format!(
                "{}: api='bulk' is no longer a mode of this node - use the \
                 Salesforce Bulk sink (snk.salesforce.bulk) for Bulk API 2.0, or \
                 'collections' (<=200 records/request) here. \
                 See docs/salesforce-sink/IMPLEMENTATION.md.",
                component_id
            ))),
            _ => SalesforceWriteApi::Collections,
        };
        salesforce_sink = Some(SalesforceSinkSpec {
            from_view: from_view.to_string(),
            instance_url,
            api_version: string_prop(&props, "apiVersion")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "v60.0".into()),
            access_token,
            object,
            operation,
            external_id_field,
            id_field: string_prop(&props, "idField")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Id".into()),
            // Salesforce hard-caps sObject Collections at 200.
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(200)
                .min(200) as usize,
            all_or_none: props.get("allOrNone").and_then(|v| v.as_bool()).unwrap_or(false),
            fail_on_error: props.get("failOnError").and_then(|v| v.as_bool()).unwrap_or(true),
            api,
            oauth,
            results_path: string_prop(&props, "resultsPath").filter(|s| !s.is_empty()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.salesforce.bulk" {
        // Salesforce Bulk API 2.0 write sink: async job lifecycle for
        // migration-scale loads. Same auth shape as snk.salesforce.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let oauth = rest_oauth_from_props(&props, true)?;
        let instance_url = string_prop(&props, "instanceUrl")
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        let access_token = string_prop(&props, "accessToken").filter(|s| !s.is_empty());
        if oauth.is_none() {
            if instance_url.is_none() {
                return Err(EngineError::Config(format!(
                    "{}: instanceUrl required (e.g. https://acme.my.salesforce.com)",
                    component_id
                )));
            }
            if access_token.is_none() {
                return Err(EngineError::Config(format!(
                    "{}: accessToken required (Bearer OAuth token; use ${{ENV:SF_TOKEN}}), \
                     or set Auth mode to OAuth Client Credentials",
                    component_id
                )));
            }
        }
        let instance_url = instance_url.unwrap_or_default();
        let access_token = access_token.unwrap_or_default();
        let object = string_prop(&props, "object")
            .or_else(|| string_prop(&props, "sobject"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!(
                "{}: object required (e.g. Account)", component_id
            )))?;
        // Bulk's operation names are case-sensitive on the wire ("hardDelete"),
        // so match case-insensitively but keep Salesforce's spelling.
        let operation = match string_prop(&props, "operation")
            .unwrap_or_else(|| "insert".into())
            .to_lowercase()
            .as_str()
        {
            "insert" => "insert".to_string(),
            "update" => "update".to_string(),
            "upsert" => "upsert".to_string(),
            "delete" => "delete".to_string(),
            "harddelete" => "hardDelete".to_string(),
            other => {
                return Err(EngineError::Config(format!(
                    "{}: operation must be insert|update|upsert|delete|hardDelete (got '{}')",
                    component_id, other
                )))
            }
        };
        let external_id_field = string_prop(&props, "externalIdField")
            .filter(|s| !s.is_empty());
        if operation == "upsert" && external_id_field.is_none() {
            return Err(EngineError::Config(format!(
                "{}: externalIdField required when operation = upsert", component_id
            )));
        }
        let poll_interval_secs = props
            .get("pollIntervalSecs")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(5);
        let timeout_secs = props
            .get("timeoutSecs")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(3600);
        salesforce_bulk_sink = Some(SalesforceBulkSinkSpec {
            from_view: from_view.to_string(),
            instance_url,
            api_version: string_prop(&props, "apiVersion")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "v60.0".into()),
            access_token,
            object,
            operation,
            external_id_field,
            id_field: string_prop(&props, "idField")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Id".into()),
            assignment_rule_id: string_prop(&props, "assignmentRuleId")
                .filter(|s| !s.is_empty()),
            poll_interval_secs,
            timeout_secs,
            fail_on_error: props.get("failOnError").and_then(|v| v.as_bool()).unwrap_or(true),
            oauth,
            results_path: string_prop(&props, "resultsPath").filter(|s| !s.is_empty()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "src.salesforce.bulk" {
        // Salesforce Bulk API 2.0 query source: async job lifecycle for
        // migration-scale reads. Auth mirrors snk.salesforce.bulk (sink-shaped
        // keys), NOT the REST-form src.salesforce.
        let oauth = rest_oauth_from_props(&props, true)?;
        let instance_url = string_prop(&props, "instanceUrl")
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        let access_token = string_prop(&props, "accessToken").filter(|s| !s.is_empty());
        if oauth.is_none() {
            if instance_url.is_none() {
                return Err(EngineError::Config(format!(
                    "{}: instanceUrl required (e.g. https://acme.my.salesforce.com)",
                    component_id
                )));
            }
            if access_token.is_none() {
                return Err(EngineError::Config(format!(
                    "{}: accessToken required (Bearer OAuth token; use ${{ENV:SF_TOKEN}}), \
                     or set Auth mode to OAuth Client Credentials",
                    component_id
                )));
            }
        }
        let instance_url = instance_url.unwrap_or_default();
        let access_token = access_token.unwrap_or_default();
        let query = string_prop(&props, "query")
            .or_else(|| string_prop(&props, "soql"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!(
                "{}: query required (a SOQL SELECT, e.g. SELECT Id, Name FROM Account)",
                component_id
            )))?;
        // Case-sensitive on the wire: "queryAll", not "queryall".
        let operation = match string_prop(&props, "operation")
            .unwrap_or_else(|| "query".into())
            .to_lowercase()
            .as_str()
        {
            "query" => "query".to_string(),
            "queryall" => "queryAll".to_string(),
            other => {
                return Err(EngineError::Config(format!(
                    "{}: operation must be query|queryAll (got '{}')",
                    component_id, other
                )))
            }
        };
        let poll_interval_secs = props
            .get("pollIntervalSecs")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(5);
        let timeout_secs = props
            .get("timeoutSecs")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(3600);
        salesforce_bulk_source = Some(SalesforceBulkSourceSpec {
            node_id: node.id.clone(),
            instance_url,
            api_version: string_prop(&props, "apiVersion")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "v60.0".into()),
            access_token,
            query,
            operation,
            poll_interval_secs,
            timeout_secs,
            max_records: props
                .get("maxRecords")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0),
            oauth,
            declared_schema: node.data.schema.clone(),
        });
        (String::new(), StageKind::View, None)
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
            text_template: None,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.email" {
        // SMTP per-row send via lettre. host required; user/password
        // optional (for relay servers that don't require auth).
        // to/subject/body all from per-row columns so one stage can
        // send N personalized messages.
        // No upstream means notification mode: one message from `to` / `subject`
        // / `body` rather than one per row. An ordering link into a mail step to
        // say "tell someone we got here" is ordinary, and requiring rows for it
        // meant inventing a one-row table to carry three constants.
        let fixed = if inputs.main().is_none() {
            let to = string_prop(&props, "to")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    EngineError::Config(format!(
                        "{}: with nothing wired in, `to` is required - either feed it rows or give it a message to send",
                        component_id
                    ))
                })?;
            Some((
                to,
                string_prop(&props, "subject").unwrap_or_default(),
                string_prop(&props, "body").unwrap_or_default(),
            ))
        } else {
            None
        };
        let from_view = inputs.main().unwrap_or("");
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let from_address = string_prop(&props, "fromAddress")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: fromAddress required", component_id)))?;
        email_sink = Some(EmailSinkSpec {
            fixed,
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
    } else if component_id == "snk.qvd" {
        // Qlik QVD writer via the clean-room crate::qvd encoder. Column order
        // follows the first row; the QVD carries its own schema.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        qvd_sink = Some(QvdSinkSpec {
            from_view: from_view.to_string(),
            path,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.gizmosql" {
        // GizmoSQL (Arrow Flight SQL) sink: CREATE + batched INSERT over Flight
        // SQL via the clean-room crate::gizmosql client.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let table = string_prop(&props, "table")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: table required", component_id)))?;
        gizmosql_sink = Some(GizmoSqlSinkSpec {
            from_view: from_view.to_string(),
            host,
            port: string_prop(&props, "port").and_then(|s| s.parse().ok()).unwrap_or(31337),
            username: string_prop(&props, "username").unwrap_or_default(),
            password: string_prop(&props, "password").unwrap_or_default(),
            tls: props.get("tls").and_then(|v| v.as_bool()).unwrap_or(false),
            tls_skip_verify: props.get("tlsSkipVerify").and_then(|v| v.as_bool()).unwrap_or(false),
            table,
            mode: string_prop(&props, "mode")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "append".into()),
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
            tls: kafka_security(&props).0,
            sasl: kafka_security(&props).1,
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
    } else if component_id == "snk.adbc" {
        // Generic ADBC ingest sink. COPYs the upstream view to Parquet and
        // bulk-loads it through the driver's ADBC ingest API. ADBC bulk ingest
        // is create/append/replace only, so upsert is rejected rather than
        // silently downgraded to append. MUST come before the starts_with("snk.")
        // catch-all below, which routes to build_sink_sql (no ADBC ingest path).
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let driver = string_prop(&props, "driver")
            .or_else(|| string_prop(&props, "driverPath"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: driver (path or name) required", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        let write_mode = string_prop(&props, "writeMode")
            .or_else(|| string_prop(&props, "mode"));
        if write_mode.as_deref() == Some("upsert") {
            return Err(EngineError::Config(format!(
                "{}: upsert is not supported for ADBC ingest; use writeMode append or overwrite",
                component_id
            )));
        }
        let mode = match write_mode.as_deref() {
            Some("overwrite") | Some("replace") => "overwrite",
            _ => "append",
        }
        .to_string();
        adbc_sink = Some(AdbcSinkSpec {
            from_view: from_view.to_string(),
            driver,
            entrypoint: string_prop(&props, "entrypoint").filter(|s| !s.is_empty()),
            options: adbc_db_options(&props),
            table,
            schema: string_prop(&props, "schema").filter(|s| !s.is_empty()),
            catalog: string_prop(&props, "catalog").filter(|s| !s.is_empty()),
            mode,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.teradata" {
        // Teradata sink over the Teradata ODBC driver. Reads the upstream view
        // and INSERTs the rows through ODBC. Bulk ingest is append (create the
        // table if missing) or overwrite (clear it first); upsert is rejected.
        // MUST come before the starts_with("snk.") catch-all below.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        let write_mode = string_prop(&props, "writeMode").or_else(|| string_prop(&props, "mode"));
        if write_mode.as_deref() == Some("upsert") {
            return Err(EngineError::Config(format!(
                "{}: upsert is not supported; use writeMode append or overwrite",
                component_id
            )));
        }
        let mode = match write_mode.as_deref() {
            Some("overwrite") | Some("replace") => "overwrite",
            _ => "append",
        }
        .to_string();
        teradata_sink = Some(TeradataSinkSpec {
            from_view: from_view.to_string(),
            conn_str: teradata_conn_string(&props)?,
            database: string_prop(&props, "database")
                .or_else(|| string_prop(&props, "schema"))
                .filter(|s| !s.is_empty()),
            table,
            mode,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.model" {
        // #253: the card comes from the upstream row's COLUMNS, so a training
        // stage produces it the way it produces any table.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let name = string_prop(&props, "name")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: name required (the model's name; cards land under <path>/<name>/)", component_id)))?;
        model_card = Some(ModelCardSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            dir: string_prop(&props, "path")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "models".to_string()),
            name,
        });
        (String::new(), StageKind::View, None)
    // These three sinks MUST come before the starts_with("snk.") catch-all
    // below. Placed after it they are unreachable, and the run fails with
    // "not yet implemented" for a component the palette offers.
    } else if component_id == "snk.neo4j" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let endpoint = string_prop(&props, "endpoint")
            .or_else(|| string_prop(&props, "url"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "{}: endpoint required (e.g. 'http://localhost:7474')",
                    component_id
                ))
            })?;
        let cypher = string_prop(&props, "cypher").filter(|s| !s.trim().is_empty());
        // A label names the nodes being written, so it is required unless the
        // user supplied their own Cypher that decides what to write.
        let label = match string_prop(&props, "label").filter(|s| !s.is_empty()) {
            Some(l) => l,
            None if cypher.is_some() => String::new(),
            None => {
                return Err(EngineError::Config(format!(
                    "{}: label required (or supply your own cypher)",
                    component_id
                )))
            }
        };
        neo4j_sink = Some(Neo4jSinkSpec {
            from_view: from_view.to_string(),
            endpoint,
            database: string_prop(&props, "database")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "neo4j".to_string()),
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            label,
            merge_keys: columns_list(&props, "mergeKeys"),
            cypher,
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.turso" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let url = string_prop(&props, "url")
            .or_else(|| string_prop(&props, "endpoint"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        turso_sink = Some(TursoSinkSpec {
            from_view: from_view.to_string(),
            url,
            auth_token: string_prop(&props, "authToken")
                .or_else(|| string_prop(&props, "token"))
                .filter(|s| !s.is_empty()),
            table,
            mode: string_prop(&props, "mode")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "append".to_string()),
            batch_size: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(500) as usize,
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id == "snk.db2" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        let table = string_prop(&props, "tableName")
            .or_else(|| string_prop(&props, "table"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: tableName required", component_id)))?;
        db2_sink = Some(Db2SinkSpec {
            from_view: from_view.to_string(),
            conn_str: db2_conn_string(&props)?,
            schema: string_prop(&props, "schema").filter(|s| !s.is_empty()),
            table,
            mode: string_prop(&props, "mode")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "append".to_string()),
        });
        (String::new(), StageKind::Sink, Some(from_view.to_string()))
    } else if component_id.starts_with("snk.") {
        let from_view = inputs
            .main()
            .ok_or_else(|| missing_input(node, "main"))?;
        sink_path = string_prop(&props, "path").filter(|s| !s.is_empty());
        sink_mode = string_prop(&props, "mode").filter(|s| !s.is_empty());
        sink_compression = string_prop(&props, "compression").filter(|s| !s.is_empty());
        sink_direct = props
            .get("directWrite")
            .and_then(|v| {
                v.as_bool()
                    .or_else(|| v.as_str().map(|t| t.eq_ignore_ascii_case("true")))
            })
            .unwrap_or(false);
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
                .map(|s| s.iter().map(|c| c.name.clone()).collect::<Vec<String>>())
                .filter(|v| !v.is_empty())
                // #39: fall back to the nearest upstream node's schema so a
                // transform (e.g. a sample) between source and a merge sink
                // still gives the merge its input column list.
                .unwrap_or_else(|| upstream_cols.to_vec());
            (
                format!("{}{}", attach, build_sink_sql(component_id, &props, from_view, &sink_cols, node.data.schema.as_deref())?),
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
        // Accept a numeric string as well as a number: context substitution
        // rewrites values in place and everything it produces is a string, so a
        // count supplied as ${...} arrived as "15" and failed to parse as a
        // number it plainly was.
        let count = props
            .get("count")
            .or_else(|| props.get("iterations"))
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            })
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
        // The column whose value names each iteration, for run logs and for
        // per-item incremental state. Optional: absent keeps the old shape,
        // where every iteration is the same named run.
        foreach_item_key = string_prop(&props, "itemKey").filter(|s| !s.trim().is_empty());
        // "queue" hands the rows to workers instead of running them here.
        foreach_queue = string_prop(&props, "dispatch")
            .map(|d| d.trim().eq_ignore_ascii_case("queue"))
            .unwrap_or(false);
        foreach_concurrency = props
            .get("concurrency")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            })
            .unwrap_or(1)
            .max(1) as usize;
        // How long a queued item keeps being retried. Only meaningful for
        // dispatch "queue" - an inline foreach runs each row once, in this run,
        // and there is no later pass for a retry to happen on.
        let num = |key: &str| -> u64 {
            props
                .get(key)
                .and_then(|v| {
                    v.as_u64()
                        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
                })
                .unwrap_or(0)
        };
        let max_attempts = num("maxAttempts") as u32;
        let initial_seconds = num("retryInitialSeconds");
        if foreach_queue && (max_attempts > 0 || initial_seconds > 0) {
            foreach_retry = Some(crate::batch::RetryPolicy {
                max_attempts,
                backoff: string_prop(&props, "retryBackoff")
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| "fixed".into()),
                initial_seconds,
                max_seconds: num("retryMaxSeconds"),
            });
        }
        let sql = passthrough_view_sql(&node.id, from_view);
        (sql, StageKind::View, Some(from_view.to_string()))
    } else if component_id == "src.runevents" {
        // Emits the stages that have already failed in this run. A job's
        // log-catcher is a SOURCE of error rows, not a sink for them: what it
        // emits gets mailed or written to a table downstream.
        //
        // It can only see failures the run survived, which is what
        // continueOnFailure is for. Without a soft stage ahead of it the run
        // ends at its first error and this node never executes, so wiring one
        // without marking anything soft reports nothing - correctly.
        run_events = true;
        (String::new(), StageKind::View, None)
    } else if component_id == "ctl.file" {
        // A typed filesystem operation, so staging a file does not require an
        // authored shell command that only runs on one platform.
        let op = string_prop(&props, "op")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "copy".into())
            .to_ascii_lowercase();
        if !matches!(op.as_str(), "copy" | "move" | "delete" | "archive") {
            return Err(EngineError::Config(format!(
                "{}: unknown op {:?} - expected \"copy\", \"move\", \"delete\" or \"archive\"",
                component_id, op
            )));
        }
        let source = string_prop(&props, "source")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: source required", component_id)))?;
        let destination = string_prop(&props, "destination")
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if !matches!(op.as_str(), "delete") && destination.is_empty() {
            return Err(EngineError::Config(format!(
                "{}: destination required for {}",
                component_id, op
            )));
        }
        file_op = Some(FileOpSpec {
            op,
            source,
            destination,
            overwrite: props
                .get("overwrite")
                .and_then(JsonValue::as_bool)
                .unwrap_or(true),
            fail_on_error: props
                .get("failOnError")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
        });
        no_output_relation = true;
        (String::new(), StageKind::View, None)
    } else if component_id == "ctl.anchor" {
        // Does no work. It exists so ordering links have something to attach to.
        //
        // A visual ETL tool marks the start and end of a job, and fans subjobs
        // out, with components that carry no data and no configuration: their
        // whole meaning is "the things wired to me run around here". Importing
        // one as a data component asserts a flow that does not exist, and
        // dropping it throws the ordering away with it.
        //
        // Effect-only: no `<node>` relation, so nothing downstream can read rows
        // from it, while the trigger edges into and out of it still order the
        // run (see the sort in compile()).
        no_output_relation = true;
        (
            passthrough_placeholder_sql(&node.id, "anchor"),
            StageKind::View,
            None,
        )
    } else if component_id == "ctl.setvar" {
        // Work out a value while the run is under way and let later steps ask for it
        // by name. A job routinely needs one - the date on the batch it just read, the
        // id it just wrote - and the static context cannot hold it, because nothing
        // knows it until the run has started.
        //
        // The value is kept in the run's OWN DATABASE, not in the session. A stage is
        // sometimes a separate connection to the same file (see the per-stage path in
        // lib.rs), and session state does not cross that boundary - so a session
        // variable would be there on one path and quietly missing on the other, which
        // is the worst of the two.
        let name = string_prop(&props, "name")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "{}: `name` is required - it is the name later steps write as ${{name}}",
                    component_id
                ))
            })?;
        let value = string_prop(&props, "value")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "{}: `value` is required - it is the expression that produces the value",
                    component_id
                ))
            })?;
        // Wired to rows, the expression is read against them and the first row decides,
        // so an aggregate over the whole input is written as one. Wired to nothing, it
        // stands on its own and is simply evaluated.
        let holder = run_var_relation(&name);
        let set = match inputs.main() {
            Some(from_view) => format!(
                "CREATE OR REPLACE TABLE {} AS SELECT ({}) AS v FROM {} LIMIT 1",
                quote_ident(&holder),
                value,
                quote_ident(from_view)
            ),
            None => format!(
                "CREATE OR REPLACE TABLE {} AS SELECT ({}) AS v",
                quote_ident(&holder),
                value
            ),
        };
        let (pass, from) = match inputs.main() {
            Some(from_view) => (
                passthrough_view_sql(&node.id, from_view),
                Some(from_view.to_string()),
            ),
            None => (passthrough_placeholder_sql(&node.id, "set"), None),
        };
        (format!("{set};
{pass}"), StageKind::View, from)
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
        // A child normally hands nothing back. When the parent says it wants the child's
        // rows, both sides agree on a handoff file: the parent names it, passes it down as
        // ${DUCKLE_RETURN} like any other context variable, and reads it once the child has
        // run. The process id keeps two concurrent runs of one pipeline apart.
        let returns_rows = props
            .get("returnsRows")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let handoff = returns_rows.then(|| {
            let safe: String = node
                .id
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            // A counter as well as the process id: two runs of one pipeline in the same
            // process share a node id and would otherwise race on the same file.
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::env::temp_dir()
                .join(format!(
                    "duckle-return-{}-{}-{}.parquet",
                    std::process::id(),
                    seq,
                    safe
                ))
                .to_string_lossy()
                .replace('\\', "/")
        });
        if let Some(file) = &handoff {
            vars.push(("DUCKLE_RETURN".to_string(), file.clone()));
        }
        run_job = Some((path, vars));
        let sql = match (&handoff, inputs.main()) {
            (Some(file), _) => format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
                quote_ident(&node.id),
                file.replace('\'', "''")
            ),
            (None, Some(from_view)) => passthrough_view_sql(&node.id, from_view),
            (None, None) => passthrough_placeholder_sql(&node.id, "triggered"),
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
            data_path: string_prop(&props, "dataPath").filter(|s| !s.trim().is_empty()),
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
            single_consumer: consumer_count
                .get(&output_table_ref(&node.id, None))
                .copied()
                .unwrap_or(0)
                <= 1,
            parallel_column: string_prop(&props, "parallelColumn")
                .or_else(|| string_prop(&props, "partitionColumn"))
                .filter(|s| !s.trim().is_empty()),
            parallel_degree: props
                .get("parallelDegree")
                .and_then(|v| v.as_u64())
                .or_else(|| {
                    string_prop(&props, "parallelDegree").and_then(|s| s.trim().parse().ok())
                })
                .unwrap_or(1)
                .clamp(1, 32) as usize,
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
    } else if component_id == "src.teradata" {
        // Teradata source over the Teradata ODBC driver (there is no DuckDB
        // Teradata extension or native Rust driver). Connect through the user's
        // installed ODBC driver (or a DSN / raw connection string), run the
        // query or read a whole table, and materialize with per-column typed
        // casts so numbers / decimals / dates keep their types.
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                string_prop(&props, "tableName")
                    .or_else(|| string_prop(&props, "table"))
                    .filter(|s| !s.is_empty())
                    .map(|t| format!("SELECT * FROM {}", t))
            })
            .ok_or_else(|| EngineError::Config(format!("{}: query or tableName required", component_id)))?;
        teradata_source = Some(TeradataSourceSpec {
            node_id: node.id.clone(),
            conn_str: teradata_conn_string(&props)?,
            query,
            batch_rows: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(5000) as usize,
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
        // Avro decoding needs somewhere to fetch the schema from. Saying the
        // messages are Avro and giving nowhere to look it up cannot work, so it
        // fails here rather than handing back UTF-8 mangled bytes at run time.
        let kafka_registry =
            string_prop(&props, "schemaRegistryUrl").filter(|s| !s.trim().is_empty());
        if string_prop(&props, "format").as_deref() == Some("avro") && kafka_registry.is_none() {
            return Err(EngineError::Config(format!(
                "{}: message format is Avro, so a Schema Registry URL is required to decode it",
                component_id
            )));
        }
        kafka_source = Some(KafkaSourceSpec {
            schema_registry_url: kafka_registry,
            tls: kafka_security(&props).0,
            sasl: kafka_security(&props).1,
            // Off by default: turning it on changes where a run starts reading,
            // which is not a decision to make on someone's behalf.
            track_offset: props
                .get("trackOffset")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
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
        // The GUI authors every code.* node's source under `code` (shared
        // code-node form), so read that first and keep `command` as a
        // back-compat alias for hand-authored / MCP pipelines.
        let command = string_prop(&props, "code")
            .or_else(|| string_prop(&props, "command"))
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
    } else if component_id == "src.websocket" {
        // WebSocket client source (#192): connect, optionally subscribe, read up
        // to maxMessages frames (or until the timeout), emit rows.
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!(
                "{}: url required (ws:// or wss://)", component_id
            )))?;
        websocket_source = Some(WebSocketSourceSpec {
            node_id: node.id.clone(),
            url,
            subscribe: string_prop(&props, "subscribe").filter(|s| !s.is_empty()),
            max_messages: props
                .get("maxMessages")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1),
            timeout_ms: props
                .get("timeoutMs")
                .and_then(|v| v.as_u64())
                .unwrap_or(30000),
            headers: headers_from_props(&props),
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
    } else if component_id == "src.pdf" {
        // #248: pages out of a document. Not SQL - DuckDB cannot open a PDF -
        // so this is a runtime hook that materialises the relation itself.
        //
        // #282: with something wired in, the documents are whatever the upstream
        // rows name and `path` is not required. With nothing wired in it reads
        // its configured path exactly as it always has, so every existing
        // pipeline is untouched.
        let upstream = inputs.main();
        let path = string_prop(&props, "path").filter(|s| !s.is_empty());
        if path.is_none() && upstream.is_none() {
            return Err(EngineError::Config(format!(
                "{}: needs either a path (a .pdf file, or a folder of them) or an upstream relation naming the documents to read",
                component_id
            )));
        }
        pdf_source = Some(PdfSourceSpec {
            node_id: node.id.clone(),
            path: path.unwrap_or_default(),
            input: artifact_input_from_props(&props, upstream),
            concurrency: props
                .get("concurrency")
                .and_then(|v| {
                    v.as_u64()
                        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
                })
                .unwrap_or(1)
                .max(1) as usize,
            on_error: string_prop(&props, "onError")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "fail".to_string()),
            recursive: props
                .get("recursive")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
            declared_schema: node.data.schema.clone(),
        });
        // `from` stays None even with an input wired in: this node MATERIALISES
        // its own relation, and `from` names the relation a stage READS. Setting
        // it to the upstream made the downstream sink count - and write - the
        // artifact rows instead of the pages.
        (String::new(), StageKind::View, None)
    } else if component_id == "src.html" {
        // #255: rows out of an HTML page. Not SQL - DuckDB cannot parse HTML -
        // so this is a runtime hook that materialises the relation itself, the
        // same shape as src.xml below.
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required (a file, or an http(s) URL)", component_id)))?;
        let row_selector = string_prop(&props, "rowSelector")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: rowSelector required (a CSS selector; every match is one row)", component_id)))?;
        // Two shapes, the way headers_from_props already accepts two: an array of
        // {name, selector, attr} for precision, and the object the GUI's
        // key-value editor writes, where the value is `selector` or
        // `selector@attribute`.
        let columns = match props.get("columns") {
            Some(JsonValue::Object(map)) => map
                .iter()
                .filter_map(|(name, v)| {
                    let name = name.trim().to_string();
                    let spec = v.as_str()?.trim();
                    if name.is_empty() {
                        return None;
                    }
                    // Split on the LAST '@': CSS attribute selectors are written
                    // [attr=value], so a bare '@' here is the attribute marker.
                    let (selector, attr) = match spec.rsplit_once('@') {
                        Some((sel, at)) if !at.is_empty() => (sel.trim(), Some(at.trim().to_string())),
                        _ => (spec, None),
                    };
                    Some(HtmlColumn { name, selector: selector.to_string(), attr })
                })
                .collect::<Vec<_>>(),
            _ => props
            .get("columns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| {
                        let name = c.get("name").and_then(|v| v.as_str())?.trim().to_string();
                        if name.is_empty() {
                            return None;
                        }
                        Some(HtmlColumn {
                            name,
                            selector: c
                                .get("selector")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .trim()
                                .to_string(),
                            attr: c
                                .get("attr")
                                .and_then(|v| v.as_str())
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(str::to_string),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        };
        let mut headers = headers_from_props(&props);
        push_rest_auth(&mut headers, &props);
        html_source = Some(HtmlSourceSpec {
            transport: http_transport_from_props(&props),
            node_id: node.id.clone(),
            path,
            row_selector,
            columns,
            headers,
            declared_schema: node.data.schema.clone(),
        });
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
            // The GUI historically wrote this under `rootPath` while the engine
            // only ever read `rowPath`, so GUI-configured XML never picked up
            // the path. The manifest now writes `rowPath`; accept `rootPath` as
            // a fallback so any older saved pipeline keeps working.
            row_path: string_prop(&props, "rowPath")
                .filter(|s| !s.is_empty())
                .or_else(|| string_prop(&props, "rootPath"))
                .unwrap_or_default(),
            declared_schema: node.data.schema.clone(),
            // Only consulted for an sftp:// path; same prop keys as src.ftp so a
            // pasted PEM (privateKey) or a key file (privateKeyPath) both work.
            sftp_password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            sftp_private_key: string_prop(&props, "privateKey")
                .or_else(|| {
                    string_prop(&props, "privateKeyPath")
                        .and_then(|p| std::fs::read_to_string(&p).ok())
                })
                .filter(|s| !s.is_empty()),
            sftp_key_passphrase: string_prop(&props, "keyPassphrase").filter(|s| !s.is_empty()),
            sftp_host_fingerprint: string_prop(&props, "hostFingerprint").filter(|s| !s.is_empty()),
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
    } else if component_id == "src.qvd" {
        // Qlik QVD reader (#88) via the clean-room crate::qvd decoder. The QVD
        // header carries its own schema, so path is the only required prop.
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        qvd_source = Some(QvdSourceSpec {
            node_id: node.id.clone(),
            path,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.gizmosql" {
        // GizmoSQL (Arrow Flight SQL) source via the clean-room crate::gizmosql
        // client. Result streams to Parquet + materializes like the ADBC source.
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        let query = string_prop(&props, "query")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: query required", component_id)))?;
        let single_consumer = consumer_count
            .get(&output_table_ref(&node.id, None))
            .copied()
            .unwrap_or(0)
            <= 1;
        gizmosql_source = Some(GizmoSqlSourceSpec {
            node_id: node.id.clone(),
            host,
            port: string_prop(&props, "port").and_then(|s| s.parse().ok()).unwrap_or(31337),
            username: string_prop(&props, "username").unwrap_or_default(),
            password: string_prop(&props, "password").unwrap_or_default(),
            tls: props.get("tls").and_then(|v| v.as_bool()).unwrap_or(false),
            tls_skip_verify: props.get("tlsSkipVerify").and_then(|v| v.as_bool()).unwrap_or(false),
            query,
            single_consumer,
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
            encrypt: props.get("encrypt").and_then(|v| v.as_bool()).unwrap_or(true),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.ducklake.maintain" {
        // #279: a thin surface over DuckLake's own maintenance functions. The
        // options are that function's options - nothing here invents storage
        // semantics, and an operation this build's DuckLake does not have fails
        // with DuckDB's own message rather than a guess of ours.
        let operation = string_prop(&props, "operation")
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "stats".to_string());
        const OPERATIONS: &[&str] = &[
            "compact",
            "rewrite",
            "expireSnapshots",
            "cleanupFiles",
            "deleteOrphans",
            "flushInlined",
            "stats",
        ];
        if !OPERATIONS.contains(&operation.as_str()) {
            return Err(EngineError::Config(format!(
                "{}: unknown operation '{}' - expected one of {}",
                component_id,
                operation,
                OPERATIONS.join(", ")
            )));
        }
        let dry_run = props.get("dryRun").and_then(|v| v.as_bool()).unwrap_or(false);
        // DuckLake offers dry_run on the three destructive operations only.
        // Accepting it elsewhere would let someone tick "dry run" on a
        // compaction and have it rewrite their files anyway.
        const DRY_RUNNABLE: &[&str] = &["expireSnapshots", "cleanupFiles", "deleteOrphans"];
        if dry_run && !DRY_RUNNABLE.contains(&operation.as_str()) {
            return Err(EngineError::Config(format!(
                "{}: '{}' has no dry run in DuckLake, so ticking it would be ignored and the operation would happen anyway. Dry run is available on: {}",
                component_id,
                operation,
                DRY_RUNNABLE.join(", ")
            )));
        }
        let num = |key: &str| -> Option<u64> {
            props.get(key).and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            })
        };
        let attach = builders::ducklake_attach(&props, false);
        if attach.is_empty() {
            return Err(EngineError::Config(format!(
                "{}: path required - the DuckLake catalog to maintain",
                component_id
            )));
        }
        ducklake_maintain = Some(DuckLakeMaintainSpec {
            node_id: node.id.clone(),
            attach,
            catalog_path: string_prop(&props, "path").unwrap_or_default(),
            operation,
            schema_name: string_prop(&props, "schemaName").filter(|s| !s.trim().is_empty()),
            table_name: string_prop(&props, "tableName").filter(|s| !s.trim().is_empty()),
            dry_run,
            older_than: string_prop(&props, "olderThan").filter(|s| !s.trim().is_empty()),
            versions: string_prop(&props, "versions").filter(|s| !s.trim().is_empty()),
            cleanup_all: props
                .get("cleanupAll")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            min_file_size: num("minFileSize"),
            max_file_size: num("maxFileSize"),
            max_compacted_files: num("maxCompactedFiles"),
            delete_threshold: props.get("deleteThreshold").and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
            }),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.artifact.copy" {
        // #247: land the BYTES of the artifacts named upstream somewhere durable
        // and emit a row per landed copy, so a change feed becomes a raw zone
        // without a shell stage in the middle.
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        artifact_copy = Some(ArtifactCopySpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            uri_column: string_prop(&props, "uriColumn")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "uri".to_string()),
            destination: string_prop(&props, "destination")
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    EngineError::Config(format!(
                        "{}: destination required - an s3:// prefix or a local directory",
                        component_id
                    ))
                })?,
            naming: string_prop(&props, "naming")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "keep".to_string()),
            if_exists: string_prop(&props, "ifExists")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "skip".to_string()),
            // 8 MiB: above S3's 5 MiB floor for a non-final part, and the
            // ceiling on how much of any one object is ever in memory.
            part_size_bytes: props
                .get("partSizeMb")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .map(|n| (n as usize) * 1024 * 1024)
                .unwrap_or(8 * 1024 * 1024)
                .max(5 * 1024 * 1024),
            auth: artifact_auth_from_props(&props),
        });
        (String::new(), StageKind::View, Some(from_view.to_string()))
    } else if component_id == "xf.tumble" {
        let from_view = inputs.main().ok_or_else(|| missing_input(node, "main"))?;
        tumble = Some(TumbleSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            time_column: string_prop(&props, "timeColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    EngineError::Config(format!("{}: timeColumn required", component_id))
                })?,
            size: string_prop(&props, "size")
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    EngineError::Config(format!(
                        "{}: size required, as a DuckDB interval like \"1 hour\"",
                        component_id
                    ))
                })?,
            allowed_lateness: string_prop(&props, "allowedLateness")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "0 seconds".to_string()),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.changed" {
        // Metadata-only poll. The point is not to pay for the object to find
        // out whether it was needed.
        let uri = string_prop(&props, "uri")
            .or_else(|| string_prop(&props, "url"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: uri required", component_id)))?;
        changed_source = Some(ChangedSourceSpec {
            node_id: node.id.clone(),
            uri,
            listing: props
                .get("listing")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            suffix: string_prop(&props, "suffix").filter(|s| !s.is_empty()),
            max_entries: props
                .get("maxEntries")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000) as usize,
            track_state: props
                .get("trackState")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            private_key: string_prop(&props, "privateKey").filter(|s| !s.is_empty()),
            key_passphrase: string_prop(&props, "keyPassphrase").filter(|s| !s.is_empty()),
            host_fingerprint: string_prop(&props, "hostFingerprint").filter(|s| !s.is_empty()),
            headers: headers_from_props(&props),
            // A saved connection has already been merged onto these props by
            // resolve_connection_refs, so picking a stored S3 connection and
            // typing the keys in by hand reach here identically.
            s3: crate::s3::S3Config::from_props(&props),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.spool" {
        // Append-only NDJSON tailer. Pairs with `duckle-runner listen`, which
        // keeps a webhook or WebSocket listener up and writes here, so nothing
        // is lost between pipeline runs.
        spool_source = Some(SpoolSourceSpec {
            node_id: node.id.clone(),
            path: string_prop(&props, "path")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?,
            track_offset: props
                .get("trackOffset")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            max_bytes: props
                .get("maxBytes")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(64 * 1024 * 1024),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.neo4j" {
        // Neo4j read over the HTTP Query API. Bolt would need a driver crate
        // and a second wire protocol; the Query API returns the whole result
        // set as JSON, which is what materializing a relation needs.
        let endpoint = string_prop(&props, "endpoint")
            .or_else(|| string_prop(&props, "url"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: endpoint required", component_id)))?;
        let cypher = string_prop(&props, "cypher")
            .or_else(|| string_prop(&props, "query"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: cypher required", component_id)))?;
        // Parameters may arrive as a JSON object or as a JSON string typed
        // into a textarea; accept both rather than silently ignoring the text.
        let parameters = match props.get("parameters") {
            Some(JsonValue::Object(o)) => Some(JsonValue::Object(o.clone())),
            Some(JsonValue::String(s)) if !s.trim().is_empty() => Some(
                serde_json::from_str(s).map_err(|e| {
                    EngineError::Config(format!("{}: parameters is not valid JSON: {}", component_id, e))
                })?,
            ),
            _ => None,
        };
        neo4j_source = Some(Neo4jSourceSpec {
            node_id: node.id.clone(),
            endpoint,
            database: string_prop(&props, "database")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "neo4j".to_string()),
            user: string_prop(&props, "user").filter(|s| !s.is_empty()),
            password: string_prop(&props, "password").filter(|s| !s.is_empty()),
            cypher,
            parameters,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.turso" {
        let url = string_prop(&props, "url")
            .or_else(|| string_prop(&props, "endpoint"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: url required", component_id)))?;
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName")
                    .or_else(|| string_prop(&props, "table"))
                    .filter(|s| !s.is_empty())?;
                Some(format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))
            })
            .ok_or_else(|| {
                EngineError::Config(format!("{}: query or tableName required", component_id))
            })?;
        turso_source = Some(TursoSourceSpec {
            node_id: node.id.clone(),
            url,
            auth_token: string_prop(&props, "authToken")
                .or_else(|| string_prop(&props, "token"))
                .filter(|s| !s.is_empty()),
            query,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.db2" {
        let query = string_prop(&props, "query")
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let table = string_prop(&props, "tableName")
                    .or_else(|| string_prop(&props, "table"))
                    .filter(|s| !s.is_empty())?;
                let qualified = match string_prop(&props, "schema").filter(|s| !s.is_empty()) {
                    Some(sch) => format!("{}.{}", sch, table),
                    None => table,
                };
                Some(format!("SELECT * FROM {}", qualified))
            })
            .ok_or_else(|| {
                EngineError::Config(format!("{}: query or tableName required", component_id))
            })?;
        db2_source = Some(Db2SourceSpec {
            node_id: node.id.clone(),
            conn_str: db2_conn_string(&props)?,
            query,
            batch_rows: props
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(5000) as usize,
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
            pipeline: string_prop(&props, "pipeline").filter(|s| !s.trim().is_empty()),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.lancedb" {
        let uri = string_prop(&props, "uri")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: uri required", component_id)))?;
        let table = string_prop(&props, "table")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: table required", component_id)))?;
        lance_source = Some(LanceSourceSpec {
            node_id: node.id.clone(),
            uri,
            table,
            api_key: string_prop(&props, "apiKey").filter(|s| !s.is_empty()),
            region: string_prop(&props, "region").filter(|s| !s.is_empty()),
            limit: props.get("limit").and_then(|v| v.as_i64()).filter(|n| *n > 0),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.pixeltable" {
        // #223. `table` is Pixeltable's own path form (`dir.table`, optionally
        // `:version`); we pass it through rather than parsing it, so versioned
        // reads work without this needing to know their grammar.
        let table = string_prop(&props, "table")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: table required", component_id)))?;
        pixeltable_source = Some(PixeltableSourceSpec {
            node_id: node.id.clone(),
            table,
            filter: string_prop(&props, "filter").filter(|s| !s.is_empty()),
            columns: string_prop(&props, "columns")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            limit: props.get("limit").and_then(|v| v.as_i64()).filter(|n| *n > 0),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "src.vortex" {
        let path = string_prop(&props, "path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: path required", component_id)))?;
        vortex_source = Some(VortexSourceSpec {
            node_id: node.id.clone(),
            path,
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
            .or_else(|| string_prop(&props, "jsonPath"))
            .map(|s| json_pointer_path(&s, true))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "/data".into());
        rest_source = Some(RestSourceSpec {
            transport: http_transport_from_props(&props),
            node_id: node.id.clone(),
            response_metadata: props
                .get("responseMetadata")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
            url,
            method: "POST".into(),
            headers,
            body: Some(serde_json::to_string(&body).unwrap_or_else(|_| "{}".into())),
            response_path,
            response_format: RestResponseFormat::Json,
            pagination: RestPagination::None,
            max_pages: 1,
            oauth: None,
            from_view: None,
            url_template: None,
            parent_key_column: None,
            max_requests: 0,
            declared_schema: node.data.schema.clone(),
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
            | "src.sap"
            | "src.sap.rfc"
            | "src.soap"
            | "src.asana"
            | "src.trello"
            | "src.clickup"
            | "src.slack"
            | "src.discord"
            | "src.twilio"
            | "src.telegram"
            | "src.dhis2"
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
        // #257: a child endpoint is described by a URL template instead of a
        // fixed URL, so the template stands in when `url` is not set. When both
        // are given the template wins, because it is the more specific answer.
        let rest_url_template =
            string_prop(&props, "urlTemplate").filter(|s| !s.trim().is_empty());
        let url = string_prop(&props, "url")
            .filter(|s| !s.is_empty())
            .or_else(|| rest_url_template.clone())
            .ok_or_else(|| EngineError::Config(format!("{}: url or urlTemplate required", component_id)))?;
        // SAP native aliases. src.sap = SAP OData (v2 classic Gateway or v4
        // RAP); this covers OData services and CDS views published as OData.
        // src.sap.rfc = an RFC-enabled function module exposed over SOAP
        // (native HTTP + XML, no proprietary SAP NW RFC SDK). Binary RFC via
        // the closed SDK is intentionally not shipped.
        let is_sap_odata = component_id == "src.sap";
        let is_soap = component_id == "src.soap" || component_id == "src.sap.rfc";
        let sap_odata_v2 =
            is_sap_odata && string_prop(&props, "odataVersion").as_deref() != Some("v4");
        // SAP Gateway wants $format=json (OData v2 defaults to XML/Atom) and a
        // sap-client mandate; append both to the URL when the user hasn't.
        let url = if is_sap_odata {
            let mut u = url;
            if !u.contains("$format=") {
                let sep = if u.contains('?') { '&' } else { '?' };
                u = format!("{}{}$format=json", u, sep);
            }
            if let Some(client) = string_prop(&props, "sapClient").filter(|s| !s.is_empty()) {
                if !u.contains("sap-client=") {
                    let sep = if u.contains('?') { '&' } else { '?' };
                    u = format!("{}{}sap-client={}", u, sep, client);
                }
            }
            u
        } else {
            url
        };
        let method = string_prop(&props, "method")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if is_soap {
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
        if is_soap {
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
        // #166: src.salesforce OAuth 2.0 client-credentials. When authType selects
        // client-credentials the runner mints a fresh access token per run and
        // injects the Bearer header (push_rest_auth added nothing for this mode),
        // so users stop pasting a short-lived token. Bearer stays the default.
        //
        // #195 opens the same path to every REST alias: Salesforce keeps deriving
        // its token endpoint from loginUrl, while any other source (e.g. a Xero
        // Custom Connection) supplies an explicit tokenUrl. A source that does not
        // select the client-credentials mode still resolves to None.
        let oauth = rest_oauth_from_props(&props, component_id == "src.salesforce")?;
        let response_format = if is_soap
            || string_prop(&props, "responseFormat").as_deref() == Some("xml")
        {
            RestResponseFormat::Xml
        } else {
            RestResponseFormat::Json
        };
        let response_path = string_prop(&props, "responsePath")
            // `jsonPath` is the older spelling the form still offers as
            // "Records JSONPath (legacy)". Nothing read it, so a pipeline that
            // set only that field located no rows at all. Honoured as an alias
            // rather than ignored, so those pipelines start working.
            .or_else(|| string_prop(&props, "jsonPath"))
            .map(|s| json_pointer_path(&s, response_format == RestResponseFormat::Json))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if component_id == "src.odata" {
                    "/value".into()
                } else if is_sap_odata {
                    if sap_odata_v2 {
                        "/d/results".into()
                    } else {
                        "/value".into()
                    }
                } else {
                    String::new()
                }
            });
        let pagination_type = string_prop(&props, "paginationType")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if component_id == "src.odata" || is_sap_odata {
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
                        } else if is_sap_odata {
                            if sap_odata_v2 {
                                "/d/__next".into()
                            } else {
                                "/@odata.nextLink".into()
                            }
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
        // #257: only fan out when the user actually asked for it - a URL
        // template AND an upstream to draw rows from.
        let rest_from_view = if rest_url_template.is_some() {
            inputs.main().map(|v| v.to_string())
        } else {
            None
        };
        rest_source = Some(RestSourceSpec {
            transport: http_transport_from_props(&props),
            node_id: node.id.clone(),
            response_metadata: props
                .get("responseMetadata")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
            url,
            method,
            headers,
            body,
            response_path,
            response_format,
            pagination,
            max_pages,
            oauth,
            declared_schema: node.data.schema.clone(),
            // #257: a URL template plus a main input turns this one node into a
            // request per upstream row. Both absent = unchanged behaviour, which
            // is what every existing pipeline and vendor alias relies on.
            from_view: rest_from_view,
            url_template: rest_url_template,
            parent_key_column: string_prop(&props, "parentKeyColumn").filter(|s| !s.is_empty()),
            max_requests: props
                .get("maxRequests")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1000),
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
        // The scripts-group GUI form stores the body under `code` (and the
        // routine picker inlines into `code`); accept `script` too for
        // back-compat / hand-authored pipelines, matching code.python.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let script = string_prop(&props, "code")
            .or_else(|| string_prop(&props, "script"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: code required", component_id)))?;
        javascript = Some(JavaScriptSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            script,
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "xf.jq" {
        // Per-row jq filter over a JSON column (GitHub #173), evaluated in the
        // pure-Rust jaq engine. Runs as an isolated runtime spec (no SQL) like
        // the other in-engine per-row transforms.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let column = string_prop(&props, "column")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: a JSON column is required", component_id)))?;
        let filter = string_prop(&props, "filter")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: a jq filter is required", component_id)))?;
        jq = Some(JqSpec {
            node_id: node.id.clone(),
            from_view: from_view.to_string(),
            column,
            filter,
            output_column: string_prop(&props, "outputColumn")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "jq".into()),
            on_error: string_prop(&props, "onError")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "fail".into()),
        });
        (String::new(), StageKind::View, None)
    } else if component_id == "code.python" {
        // Per-row Python transform. Script must define process(row) -> dict.
        // The scripts-group manifest stores the body under `code`; accept `script`
        // too for parity with code.javascript.
        let from_view = inputs
            .main()
            .ok_or_else(|| EngineError::Config(format!("{}: upstream input required", component_id)))?;
        let script = string_prop(&props, "code")
            .or_else(|| string_prop(&props, "script"))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: code required", component_id)))?;
        python = Some(PythonSpec {
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
                .or_else(|| string_prop(&props, "textColumn"))
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
            .or_else(|| string_prop(&props, "labels"))
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
                .or_else(|| string_prop(&props, "textColumn"))
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
            headers: headers_from_props(&props),
            endpoint_path: string_prop(&props, "endpointPath").filter(|s| !s.is_empty()),
            // #258: default 1 keeps every existing pipeline byte-identical.
            concurrency: props
                .get("concurrency")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1) as usize,
            // #258: 0 disables retrying; the default gives a rate limit three
            // chances before the stage gives up on the whole dataset.
            max_retries: props
                .get("maxRetries")
                .and_then(|v| v.as_u64())
                .unwrap_or(3) as u32,

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
            // Accept the legacy `prompt` key too: the GUI wrote `prompt` before
            // it was aligned to the engine's `promptTemplate`, so pipelines saved
            // with the old key would otherwise send an empty message (#142).
            prompt_template: string_prop(&props, "promptTemplate")
                .or_else(|| string_prop(&props, "prompt"))
                .unwrap_or_default(),
            system_prompt: string_prop(&props, "systemPrompt").filter(|s| !s.is_empty()),
            temperature: props
                .get("temperature")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            headers: headers_from_props(&props),
            endpoint_path: string_prop(&props, "endpointPath").filter(|s| !s.is_empty()),
            // #258: default 1 keeps every existing pipeline byte-identical.
            concurrency: props
                .get("concurrency")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1) as usize,
            // #258: 0 disables retrying; the default gives a rate limit three
            // chances before the stage gives up on the whole dataset.
            max_retries: props
                .get("maxRetries")
                .and_then(|v| v.as_u64())
                .unwrap_or(3) as u32,
            // #258: only sent when the user actually set it, so a pipeline
            // that never touched the field still sends no max_tokens.
            max_tokens: props
                .get("maxTokens")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .map(|n| n as u32),
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
                .or_else(|| string_prop(&props, "textColumn"))
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
            headers: headers_from_props(&props),
            endpoint_path: string_prop(&props, "endpointPath").filter(|s| !s.is_empty()),
            // #258: default 1 keeps every existing pipeline byte-identical.
            concurrency: props
                .get("concurrency")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(1) as usize,
            // #258: 0 disables retrying; the default gives a rate limit three
            // chances before the stage gives up on the whole dataset.
            max_retries: props
                .get("maxRetries")
                .and_then(|v| v.as_u64())
                .unwrap_or(3) as u32,

        });
        (String::new(), StageKind::View, None)
    } else if matches!(component_id, "code.sql" | "code.sqltemplate")
        && props.get("pureSql").and_then(JsonValue::as_bool).unwrap_or(false)
    {
        // Pure SQL (#102 follow-up): run the user's statements verbatim - no
        // `WITH input AS (...)` wrapper AND no `CREATE OR REPLACE ... AS`
        // wrapper. This is the escape hatch for advanced users who need full
        // control: multiple statements, DDL, PRAGMAs, writes into an attached
        // database, etc. - things that cannot be wrapped in a single CREATE
        // VIEW. The body is run as-is (after any extension prelude), so it does
        // NOT produce a `"<node_id>"` relation; the executor treats this as an
        // effect step and skips its count/preview. To feed rows downstream the
        // user creates the relation themselves (e.g. CREATE OR REPLACE TABLE
        // "<node_id>" AS ... - the node id / alias is shown in the panel).
        let body = build_view_sql(
            component_id,
            &props,
            inputs,
            node.data.schema.as_deref(),
            false,
        )
        .map_err(|e| {
            EngineError::Config(format!(
                "{} ({} / {}): {}",
                node.data.label, component_id, node.id, e
            ))
        })?;
        no_output_relation = true;
        (format!("{}{}", attach, body), StageKind::View, None)
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
        // #76: both explicit View and the default Auto make a single-consumer
        // attach-backed source eligible for the live-VIEW upgrade in compile()
        // (true predicate pushdown into the source scan). Auto also keeps its
        // parquet fast-path spec as the fallback for when the pipeline cannot
        // batch into one session; compile() drops that spec on upgrade.
        attach_view = matches!(materialize, "view" | "auto")
            && attach_backed
            && main_consumers <= 1
            && reject_sql.is_none()
            // A source a reject-wired filter reads twice must stay materialized
            // once (parquet / table); a live VIEW would re-scan it per arm.
            && !feeds_reject.contains(node.id.as_str());
        // #117: custom SQL against an attach-backed catalog source (ducklake,
        // postgres, motherduck, ...) references that catalog's OWN schemas
        // (e.g. `data.weights`) without the `duckle_src` prefix - exactly as the
        // query runs in the source's own CLI. Both a lazy VIEW (#76) and a bare
        // materialized TABLE re-resolve those names against the run database
        // (where the schema doesn't exist), so the read failed with "schema
        // does not exist". Fix: materialize once via COPY to a temp parquet with
        // the attached catalog on the search_path (so the unqualified names
        // resolve), then downstream reads the parquet by path - no duckle_src
        // and no re-resolution. The #76 live-VIEW pushdown is disabled for this
        // source (custom SQL is opaque to predicate pushdown anyway). Qualified
        // `duckle_src.schema.table` queries keep working (fully-qualified names
        // resolve regardless of search_path).
        let custom_sql_attach_source = attach.contains("AS duckle_src")
            && ATTACH_PARQUET_SOURCES.contains(&component_id)
            && reject_sql.is_none()
            && (matches!(string_prop(&props, "mode").as_deref(), Some("sql"))
                || string_prop(&props, "sql").map(|s| !s.trim().is_empty()).unwrap_or(false)
                || string_prop(&props, "query").map(|s| !s.trim().is_empty()).unwrap_or(false));
        if custom_sql_attach_source {
            attach_view = false;
            attach_parquet_source = Some(AttachParquetSourceSpec {
                node_id: node.id.clone(),
                attach: format!("{}SET search_path='duckle_src'; ", attach),
                body: body.to_string(),
            });
        }
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
        // #154: emit the user "SQL name" (alias) into the COMPILED plan, not only
        // as the view the executor injects at run time, so it shows in Plan view /
        // SQL export and downstream nodes can reference it in every execution path.
        // Edge wiring still keys off node.id; this is just an extra alias view over
        // the node-id relation. Uniqueness / no-shadow is enforced up front (alias
        // validation above), so it cannot clash with a real relation. The executor
        // still injects the same view for runtime-source stages whose Stage.sql it
        // ignores; CREATE OR REPLACE makes the overlap idempotent.
        if let Some(alias) = node
            .data
            .alias
            .as_deref()
            .map(str::trim)
            .filter(|a| !a.is_empty() && *a != node.id)
        {
            sql.push_str(&format!(
                "; CREATE OR REPLACE VIEW {} AS SELECT * FROM {}",
                quote_ident(alias),
                quote_ident(&node.id)
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
            .map(|path| RuntimeSpec::Foreach {
                path,
                concurrency: foreach_concurrency,
                item_key: foreach_item_key.clone(),
                queue: foreach_queue,
                retry: foreach_retry.clone(),
            }))
        .or_else(|| log_spec.map(|(level, message)| RuntimeSpec::Log { level, message }))
        .or_else(|| die_spec.map(|(message, condition)| RuntimeSpec::Die { message, condition }))
        .or_else(|| incremental.map(RuntimeSpec::Incremental))
        .or_else(|| ducklake_cdc.map(RuntimeSpec::DuckLakeCdc))
        .or_else(|| webhook.map(RuntimeSpec::Webhook))
        .or_else(|| remote_exec.map(RuntimeSpec::RemoteExec))
        .or_else(|| snowflake_sink.map(RuntimeSpec::SnowflakeSink))
        .or_else(|| databricks_sink.map(RuntimeSpec::DatabricksSink))
        .or_else(|| salesforce_sink.map(RuntimeSpec::SalesforceSink))
        .or_else(|| dhis2_sink.map(RuntimeSpec::Dhis2Sink))
        .or_else(|| salesforce_bulk_sink.map(RuntimeSpec::SalesforceBulkSink))
        .or_else(|| salesforce_bulk_source.map(RuntimeSpec::SalesforceBulkSource))
        .or_else(|| snowflake_source.map(RuntimeSpec::SnowflakeSource))
        .or_else(|| databricks_source.map(RuntimeSpec::DatabricksSource))
        .or_else(|| rest_source.map(RuntimeSpec::RestSource))
        .or_else(|| elastic_source.map(RuntimeSpec::ElasticSource))
        .or_else(|| mongo_sink.map(RuntimeSpec::MongoSink))
        .or_else(|| huggingface_sink.map(RuntimeSpec::HuggingFaceSink))
        .or_else(|| mongo_source.map(RuntimeSpec::MongoSource))
        .or_else(|| lance_sink.map(RuntimeSpec::LanceSink))
        .or_else(|| lance_source.map(RuntimeSpec::LanceSource))
        .or_else(|| pixeltable_sink.map(RuntimeSpec::PixeltableSink))
        .or_else(|| pixeltable_source.map(RuntimeSpec::PixeltableSource))
        .or_else(|| vortex_sink.map(RuntimeSpec::VortexSink))
        .or_else(|| vortex_source.map(RuntimeSpec::VortexSource))
        .or_else(|| clickhouse_sink.map(RuntimeSpec::ClickhouseSink))
        .or_else(|| clickhouse_source.map(RuntimeSpec::ClickhouseSource))
        .or_else(|| sqlserver_sink.map(RuntimeSpec::SqlserverSink))
        .or_else(|| sqlserver_source.map(RuntimeSpec::SqlserverSource))
        .or_else(|| cassandra_sink.map(RuntimeSpec::CassandraSink))
        .or_else(|| cassandra_source.map(RuntimeSpec::CassandraSource))
        .or_else(|| oracle_sink.map(RuntimeSpec::OracleSink))
        .or_else(|| oracle_source.map(RuntimeSpec::OracleSource))
        .or_else(|| adbc_source.map(RuntimeSpec::AdbcSource))
        .or_else(|| adbc_sink.map(RuntimeSpec::AdbcSink))
        .or_else(|| teradata_source.map(RuntimeSpec::TeradataSource))
        .or_else(|| teradata_sink.map(RuntimeSpec::TeradataSink))
        .or_else(|| spool_source.map(RuntimeSpec::SpoolSource))
        .or_else(|| changed_source.map(RuntimeSpec::ChangedSource))
        .or_else(|| artifact_copy.map(RuntimeSpec::ArtifactCopy))
        .or_else(|| ducklake_maintain.map(RuntimeSpec::DuckLakeMaintain))
        .or_else(|| tumble.map(RuntimeSpec::Tumble))
        .or_else(|| neo4j_source.map(RuntimeSpec::Neo4jSource))
        .or_else(|| neo4j_sink.map(RuntimeSpec::Neo4jSink))
        .or_else(|| turso_source.map(RuntimeSpec::TursoSource))
        .or_else(|| turso_sink.map(RuntimeSpec::TursoSink))
        .or_else(|| db2_source.map(RuntimeSpec::Db2Source))
        .or_else(|| db2_sink.map(RuntimeSpec::Db2Sink))
        .or_else(|| attach_parquet_source.map(RuntimeSpec::AttachParquetSource))
        .or_else(|| materialize_duckdb.map(RuntimeSpec::MaterializeDuckDb))
        .or_else(|| redis_sink.map(RuntimeSpec::RedisSink))
        .or_else(|| redis_source.map(RuntimeSpec::RedisSource))
        .or_else(|| qdrant_source.map(RuntimeSpec::QdrantSource))
        .or_else(|| weaviate_source.map(RuntimeSpec::WeaviateSource))
        .or_else(|| milvus_source.map(RuntimeSpec::MilvusSource))
        .or_else(|| format_source.map(RuntimeSpec::FormatSource))
        .or_else(|| format_sink.map(RuntimeSpec::FormatSink))
        .or_else(|| websocket_source.map(RuntimeSpec::WebSocketSource))
        .or_else(|| websocket_sink.map(RuntimeSpec::WebSocketSink))
        .or_else(|| kafka_sink.map(RuntimeSpec::KafkaSink))
        .or_else(|| kafka_source.map(RuntimeSpec::KafkaSource))
        .or_else(|| avro_source.map(RuntimeSpec::AvroSource))
        .or_else(|| qvd_source.map(RuntimeSpec::QvdSource))
        .or_else(|| gizmosql_source.map(RuntimeSpec::GizmoSqlSource))
        .or_else(|| gizmosql_sink.map(RuntimeSpec::GizmoSqlSink))
        .or_else(|| nats_sink.map(RuntimeSpec::NatsSink))
        .or_else(|| nats_source.map(RuntimeSpec::NatsSource))
        .or_else(|| pubsub_sink.map(RuntimeSpec::PubsubSink))
        .or_else(|| pubsub_source.map(RuntimeSpec::PubsubSource))
        .or_else(|| model_card.map(RuntimeSpec::ModelCard))
        .or_else(|| pdf_source.map(RuntimeSpec::PdfSource))
        .or_else(|| html_source.map(RuntimeSpec::HtmlSource))
        .or_else(|| xml_source.map(RuntimeSpec::XmlSource))
        .or_else(|| xml_sink.map(RuntimeSpec::XmlSink))
        .or_else(|| avro_sink.map(RuntimeSpec::AvroSink))
        .or_else(|| qvd_sink.map(RuntimeSpec::QvdSink))
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
        .or_else(|| run_events.then_some(RuntimeSpec::RunEvents))
        .or_else(|| file_op.map(RuntimeSpec::FileOp))
        .or_else(|| dynamodb_source.map(RuntimeSpec::DynamodbSource))
        .or_else(|| kinesis_source.map(RuntimeSpec::KinesisSource))
        .or_else(|| ai_embed.map(RuntimeSpec::AiEmbed))
        .or_else(|| wasm.map(RuntimeSpec::Wasm))
        .or_else(|| javascript.map(RuntimeSpec::Javascript))
        .or_else(|| python.map(RuntimeSpec::Python))
        .or_else(|| ai_chunk.map(RuntimeSpec::AiChunk))
        .or_else(|| ai_pii.map(RuntimeSpec::AiPii))
        .or_else(|| ai_llm.map(RuntimeSpec::AiLlm))
        .or_else(|| ai_classify.map(RuntimeSpec::AiClassify))
        .or_else(|| ai_dedupe.map(RuntimeSpec::AiDedupe))
        .or_else(|| jq.map(RuntimeSpec::Jq))
        ;
    // Free the ATTACH alias so the next batched stage can re-ATTACH it (see
    // attach_alias above). Only stages that embed the ATTACH in their own SQL
    // qualify - the sql starts with the prelude. Runtime-spec sources/sinks
    // (the parquet fast-path, upsert, relational drivers, ...) run in their
    // own connection and either leave sql empty or have the executor ignore
    // it, so they are unaffected.
    if let Some(alias) = attach_alias {
        // #76: give each attach-backed SOURCE a unique alias (duckle_src_<node>)
        // so several duck sources coexist as live VIEWs in one batched session
        // without colliding on the shared name. Only the batched stage SQL is
        // renamed; isolated runtime specs (DuckLake CDC, drivers, duckdb-file
        // materialize) run in their own connection where the shared name is safe,
        // so they keep `duckle_src`. Sinks (duckle_dst) attach/write/detach
        // sequentially and are never kept open, so they keep the shared alias.
        let batched_source = alias == "duckle_src"
            && matches!(runtime, None | Some(RuntimeSpec::AttachParquetSource(_)));
        let (effective, prelude) = if batched_source {
            let uniq = format!("duckle_src_{}", alias_suffix(&node.id));
            sql = rename_token(&sql, "duckle_src", &uniq);
            let renamed_prelude = rename_token(&attach, "duckle_src", &uniq);
            (uniq, renamed_prelude)
        } else {
            (alias.to_string(), attach.clone())
        };
        if sql.starts_with(&prelude) {
            let trimmed = sql.trim_end();
            let sep = if trimmed.ends_with(';') { " " } else { "; " };
            sql = format!("{}{}DETACH {};", trimmed, sep, effective);
        }
    }
    Ok(Stage {
        node_id: node.id.clone(),
        component_id: component_id.to_string(),
        label: node.data.label.clone(),
        sql,
        kind,
        from,
        publish_group: if component_id == "snk.ducklake" {
            string_prop(&props, "publishGroup").filter(|s| !s.trim().is_empty())
        } else {
            None
        },
        sink_path,
        sink_mode,
        sink_compression,
        sink_direct,
        runtime,
        wait_ms,
        retry_attempts,
        continue_on_failure,
        retry_backoff_ms,
        memory_limit_mb,
        attach_view,
        // A user alias names the node's output relation. Pure SQL nodes create no
        // such relation, so they carry no alias view. An alias equal to the node
        // id is redundant (the relation already has that name), so drop it.
        alias: if no_output_relation {
            None
        } else {
            node.data
                .alias
                .as_deref()
                .map(str::trim)
                .filter(|a| !a.is_empty() && *a != node.id)
                .map(str::to_string)
        },
        no_output_relation,
    })
}

mod builders;
pub(crate) use builders::*;

#[cfg(test)]
mod tests;
