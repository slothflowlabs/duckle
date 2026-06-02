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
    /// For xf.ai.text_search: in DuckDB v1.5.x the fts PRAGMA can't see
    /// tables created in the same -c invocation. The planner records
    /// the spec; the executor runs two CLI calls (stage then index +
    /// query) so the PRAGMA sees committed state. Works unchanged on
    /// v1.4 too.
    pub text_search: Option<TextSearchSpec>,
    /// ctl.runpipeline: when set, the executor reads + runs another
    /// pipeline file as a side effect *before* propagating this
    /// stage's pass-through view. Lets a parent pipeline trigger
    /// helper pipelines (refresh dimension tables, kick off a cleanup,
    /// etc.) without the full block-scope DAG refactor that
    /// ctl.iterate / ctl.foreach / ctl.try need.
    pub run_pipeline_path: Option<String>,
    /// ctl.try: when set, the executor installs this path as a
    /// fallback pipeline. If any subsequent stage in the same
    /// execution fails, the fallback runs as a side effect before
    /// the original error surfaces. Useful for notifications,
    /// rollbacks, cleanup. NOT a true block-scoped try with
    /// continuation - that needs the DAG refactor.
    pub install_fallback_path: Option<String>,
    /// ctl.iterate: when set, the executor runs the referenced
    /// pipeline N times. ${ITER_INDEX} in the sub-pipeline file
    /// gets substituted to the iteration number (0..N-1). Side-effect
    /// model - sub-pipeline output isn't composed into the parent.
    pub iterate_pipeline_path: Option<String>,
    pub iterate_count: Option<u64>,
    /// ctl.foreach: when set, the executor reads upstream rows and
    /// runs the referenced pipeline once per row. ${ITER_INDEX} is
    /// the row index; ${ITER_ITEM_<FIELD>} (uppercased) is the row's
    /// value for each top-level field. Side-effect model.
    pub foreach_pipeline_path: Option<String>,
    /// HTTP per-row sink (snk.webhook / snk.rest). When set, the
    /// executor materializes the upstream view and dispatches requests
    /// via ureq; stage SQL is empty (no DuckDB write).
    pub webhook: Option<WebhookSpec>,
    /// Snowflake SQL API sink. When set, the executor builds multi-row
    /// INSERT statements and POSTs them in batches to the configured
    /// account; stage SQL is empty.
    pub snowflake_sink: Option<SnowflakeSinkSpec>,
    /// Databricks SQL Statement Execution API sink. Same pattern as
    /// Snowflake; stage SQL stays empty.
    pub databricks_sink: Option<DatabricksSinkSpec>,
    /// Snowflake SQL API source. When set, the executor POSTs the
    /// SELECT, parses the response, writes it as JSON to a temp file,
    /// then materializes node_id from the file via read_json_auto.
    pub snowflake_source: Option<SnowflakeSourceSpec>,
    /// Databricks SQL Statement Execution API source. Same shape.
    pub databricks_source: Option<DatabricksSourceSpec>,
    /// Generic HTTP REST source. Fetches a URL (with optional cursor
    /// pagination), parses the response, and materializes the row
    /// objects as a DuckDB table.
    pub rest_source: Option<RestSourceSpec>,
    /// Elasticsearch / OpenSearch _search source. Paginated via
    /// from+size; rows come from hits.hits[]._source.
    pub elastic_source: Option<ElasticSourceSpec>,
    /// MongoDB insert_many sink (official driver + tokio block_on).
    pub mongo_sink: Option<MongoSinkSpec>,
    /// MongoDB find() source (official driver + tokio block_on).
    pub mongo_source: Option<MongoSourceSpec>,
    /// ClickHouse HTTP-API sink (POST INSERT INTO ... FORMAT JSONEachRow).
    pub clickhouse_sink: Option<ClickHouseSinkSpec>,
    /// ClickHouse HTTP-API source (POST SELECT ... FORMAT JSON).
    pub clickhouse_source: Option<ClickHouseSourceSpec>,
    /// SQL Server / Synapse INSERT via tiberius (multi-row VALUES).
    pub sqlserver_sink: Option<SqlServerSinkSpec>,
    /// SQL Server / Synapse SELECT via tiberius.
    pub sqlserver_source: Option<SqlServerSourceSpec>,
    /// Cassandra / ScyllaDB INSERT via the scylla CQL driver.
    pub cassandra_sink: Option<CassandraSinkSpec>,
    /// Cassandra / ScyllaDB SELECT via the scylla CQL driver.
    pub cassandra_source: Option<CassandraSourceSpec>,
    /// Oracle INSERT (opt-in behind `oracle` feature; spec always
    /// present so the planner can validate the config either way).
    pub oracle_sink: Option<OracleSinkSpec>,
    /// Oracle SELECT (opt-in behind `oracle` feature).
    pub oracle_source: Option<OracleSourceSpec>,
    /// ADBC (Arrow Database Connectivity) SELECT: loads a prebuilt ADBC
    /// driver at runtime and materializes its Arrow result via Parquet.
    pub adbc_source: Option<AdbcSourceSpec>,
    /// Redis SET batch via the redis sync client.
    pub redis_sink: Option<RedisSinkSpec>,
    /// Redis SCAN + GET via the redis sync client.
    pub redis_source: Option<RedisSourceSpec>,
    /// Qdrant points scroll source (paginates /collections/{id}/points/scroll).
    pub qdrant_source: Option<QdrantSourceSpec>,
    /// Weaviate object list source (paginates /v1/objects?class=&after=).
    pub weaviate_source: Option<WeaviateSourceSpec>,
    /// Milvus vector query source (paginates /v1/vector/query via offset).
    pub milvus_source: Option<MilvusSourceSpec>,
    /// YAML / TOML config-format reader: parse the file with the relevant
    /// serde crate, then materialize the rows via the shared json-table
    /// helper. One spec, two formats picked by the FormatKind field.
    pub format_source: Option<FormatFileSourceSpec>,
    /// YAML / TOML config-format writer: SELECT the upstream view, serialize
    /// the row array with the relevant serde crate, write to disk.
    pub format_sink: Option<FormatFileSinkSpec>,
    /// Kafka producer (also handles Redpanda - same wire protocol).
    pub kafka_sink: Option<KafkaSinkSpec>,
    /// Kafka consumer (also handles Redpanda).
    pub kafka_source: Option<KafkaSourceSpec>,
    /// Apache Avro container-file reader (pure-Rust apache-avro crate).
    pub avro_source: Option<AvroSourceSpec>,
    /// NATS / JetStream publisher.
    pub nats_sink: Option<NatsSinkSpec>,
    /// NATS / JetStream subscriber-with-timeout (batch collector).
    pub nats_source: Option<NatsSourceSpec>,
    /// GCP Pub/Sub publish via REST.
    pub pubsub_sink: Option<PubSubSinkSpec>,
    /// GCP Pub/Sub pull via REST.
    pub pubsub_source: Option<PubSubSourceSpec>,
    /// XML row-path reader (walks path -> JSON object per match).
    pub xml_source: Option<XmlSourceSpec>,
    /// XML wrapper-element writer (root/row shape).
    pub xml_sink: Option<XmlSinkSpec>,
    /// Avro container-file writer (schema inferred from first row).
    pub avro_sink: Option<AvroSinkSpec>,
    /// RabbitMQ / AMQP 0.9.1 publisher.
    pub rabbit_sink: Option<RabbitSinkSpec>,
    /// RabbitMQ / AMQP 0.9.1 batch consumer.
    pub rabbit_source: Option<RabbitSourceSpec>,
    /// Local git repo reader (shells out to system `git`).
    pub git_source: Option<GitSourceSpec>,
    /// Shell-exec stage (one-shot std::process::Command).
    pub shell: Option<ShellSpec>,
    /// FTP / FTPS file downloader.
    pub ftp_source: Option<FtpSourceSpec>,
    /// System clipboard reader.
    pub clipboard_source: Option<ClipboardSourceSpec>,
    /// IMAP mailbox reader.
    pub email_source: Option<EmailSourceSpec>,
    /// SMTP per-row sender.
    pub email_sink: Option<EmailSinkSpec>,
    /// Local webhook listener (binds a TCP port, collects N requests).
    pub webhook_source: Option<WebhookSourceSpec>,
    /// DynamoDB Scan via direct HTTP + SigV4 (no AWS SDK).
    pub dynamodb_source: Option<DynamoDbSourceSpec>,
    /// Kinesis single-shard read via direct HTTP + SigV4.
    pub kinesis_source: Option<KinesisSourceSpec>,
    /// xf.ai.embed (per-row embedding).
    pub ai_embed: Option<AiEmbedSpec>,
    /// code.wasm (per-row WebAssembly transform).
    pub wasm: Option<WasmSpec>,
    /// code.javascript (per-row JS transform via boa interpreter).
    pub javascript: Option<JavaScriptSpec>,
    /// xf.ai.chunk (text splitter for RAG).
    pub ai_chunk: Option<AiChunkSpec>,
    /// xf.ai.pii (regex-based PII redaction).
    pub ai_pii: Option<AiPiiSpec>,
    /// xf.ai.llm (per-row chat completion).
    pub ai_llm: Option<AiLlmSpec>,
    /// xf.ai.classify (LLM-backed classification).
    pub ai_classify: Option<AiClassifySpec>,
    /// xf.ai.dedupe (embedding-based semantic dedupe).
    pub ai_dedupe: Option<AiDedupeSpec>,
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
        self.upsert.is_none()
            && self.text_search.is_none()
            && self.run_pipeline_path.is_none()
            && self.install_fallback_path.is_none()
            && self.iterate_pipeline_path.is_none()
            && self.foreach_pipeline_path.is_none()
            && self.webhook.is_none()
            && self.snowflake_sink.is_none()
            && self.databricks_sink.is_none()
            && self.snowflake_source.is_none()
            && self.databricks_source.is_none()
            && self.rest_source.is_none()
            && self.elastic_source.is_none()
            && self.mongo_sink.is_none()
            && self.mongo_source.is_none()
            && self.clickhouse_sink.is_none()
            && self.clickhouse_source.is_none()
            && self.sqlserver_sink.is_none()
            && self.sqlserver_source.is_none()
            && self.cassandra_sink.is_none()
            && self.cassandra_source.is_none()
            && self.oracle_sink.is_none()
            && self.oracle_source.is_none()
            && self.adbc_source.is_none()
            && self.redis_sink.is_none()
            && self.redis_source.is_none()
            && self.qdrant_source.is_none()
            && self.weaviate_source.is_none()
            && self.milvus_source.is_none()
            && self.format_source.is_none()
            && self.format_sink.is_none()
            && self.kafka_sink.is_none()
            && self.kafka_source.is_none()
            && self.avro_source.is_none()
            && self.nats_sink.is_none()
            && self.nats_source.is_none()
            && self.pubsub_sink.is_none()
            && self.pubsub_source.is_none()
            && self.xml_source.is_none()
            && self.xml_sink.is_none()
            && self.avro_sink.is_none()
            && self.rabbit_sink.is_none()
            && self.rabbit_source.is_none()
            && self.git_source.is_none()
            && self.shell.is_none()
            && self.ftp_source.is_none()
            && self.clipboard_source.is_none()
            && self.email_source.is_none()
            && self.email_sink.is_none()
            && self.webhook_source.is_none()
            && self.dynamodb_source.is_none()
            && self.kinesis_source.is_none()
            && self.ai_embed.is_none()
            && self.wasm.is_none()
            && self.javascript.is_none()
            && self.ai_chunk.is_none()
            && self.ai_pii.is_none()
            && self.ai_llm.is_none()
            && self.ai_classify.is_none()
            && self.ai_dedupe.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct TextSearchSpec {
    pub from_view: String,
    pub id_col: String,
    pub text_cols: Vec<String>,
    pub query: String,
    pub top_k: Option<u64>,
    pub output_col: String,
    /// Sanitized staging table name (so PRAGMA can reference a valid
    /// SQL identifier even when the node id has special characters).
    pub staging_table: String,
}

/// Snowflake auth mode. PAT (Personal Access Token) is a simple
/// Bearer-token flow; JWT (RS256) is the older standard - the
/// executor reads a PEM-encoded private key, derives the public-key
/// fingerprint, and signs Snowflake-shaped claims (iss/sub/iat/exp).
#[derive(Debug, Clone)]
pub enum SnowflakeAuth {
    Pat { token: String },
    Jwt {
        user: String,
        private_key_pem: String,
    },
}

/// snk.snowflake: SQL API insert. The executor reads upstream rows,
/// chunks them into batch_size groups, and POSTs one multi-row INSERT
/// per chunk to the account's /api/v2/statements endpoint.
#[derive(Debug, Clone)]
pub struct SnowflakeSinkSpec {
    pub from_view: String,
    /// Full Snowflake account identifier (e.g. "xy12345.us-east-1").
    /// Used to build https://<account>.snowflakecomputing.com/api/v2/statements
    /// unless `endpoint` overrides it (handy for tests + private link).
    pub account: String,
    /// Optional explicit endpoint override, e.g. http://127.0.0.1:8080/api/v2/statements.
    pub endpoint: Option<String>,
    pub auth: SnowflakeAuth,
    pub database: String,
    pub schema: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub table: String,
    pub batch_size: usize,
}

/// src.snowflake: SQL API read. Sends a SELECT (either user-provided
/// `query` or `SELECT * FROM <database>.<schema>.<table>` when only
/// the table info is given). The executor materializes the response
/// as a DuckDB table via read_json_auto.
#[derive(Debug, Clone)]
pub struct SnowflakeSourceSpec {
    pub node_id: String,
    pub account: String,
    pub endpoint: Option<String>,
    pub auth: SnowflakeAuth,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub query: String,
}

/// snk.oracle: Oracle INSERT via the official `oracle` crate. Behind
/// the `oracle` Cargo feature - the dep links against Oracle Instant
/// Client which is a separate install. Without the feature the plan
/// branch surfaces a clear "rebuild with --features oracle" error so
/// the configuration is at least diagnosable.
#[derive(Debug, Clone)]
pub struct OracleSinkSpec {
    pub from_view: String,
    /// Oracle Easy Connect string (host:port/service_name) or full URL.
    pub connect: String,
    pub user: String,
    pub password: String,
    pub schema: Option<String>,
    pub table: String,
    pub batch_size: usize,
}

/// src.oracle: Oracle SELECT via the oracle crate. Same feature gate.
#[derive(Debug, Clone)]
pub struct OracleSourceSpec {
    pub node_id: String,
    pub connect: String,
    pub user: String,
    pub password: String,
    pub query: String,
}

/// src.adbc: read via a prebuilt ADBC (Arrow Database Connectivity) driver
/// loaded at runtime. The driver returns Arrow batches which the executor
/// streams to a Parquet temp file and materializes via DuckDB read_parquet.
#[derive(Debug, Clone)]
pub struct AdbcSourceSpec {
    pub node_id: String,
    /// Path to the driver shared library (preferred) or a bare driver name.
    pub driver: String,
    /// Custom init entrypoint; defaults to AdbcDriverInit when None.
    pub entrypoint: Option<String>,
    /// ADBC database options (uri, username, password, driver-specific keys).
    pub options: Vec<(String, String)>,
    pub query: String,
}

/// snk.redis: SET each input row's keyColumn -> valueColumn into Redis
/// via the sync redis client. Optional TTL via EXPIRE. If valueColumn
/// is not set, the entire row gets JSON-stringified as the value.
#[derive(Debug, Clone)]
pub struct RedisSinkSpec {
    pub from_view: String,
    /// Standard redis:// or rediss:// URI (with credentials inline).
    pub url: String,
    pub key_column: String,
    /// Empty = JSON-stringify the whole row as the value.
    pub value_column: String,
    /// 0 = no TTL.
    pub ttl_seconds: u64,
    pub batch_size: usize,
}

/// src.redis: SCAN keys matching keyPattern, GET each, emit rows of
/// {key, value}. Limit caps the SCAN walk so a huge keyspace doesn't
/// take forever. Uses the sync redis client.
#[derive(Debug, Clone)]
pub struct RedisSourceSpec {
    pub node_id: String,
    pub url: String,
    pub key_pattern: String,
    pub limit: u64,
}

/// src.qdrant: paginate /collections/{collection}/points/scroll. Each
/// page returns `result.points: [{id, payload, vector?}]` plus
/// `result.next_page_offset` (null when done). Engine flattens each
/// point into {id, ...payload[, vector]}.
#[derive(Debug, Clone)]
pub struct QdrantSourceSpec {
    pub node_id: String,
    pub cluster_url: String,
    pub collection: String,
    pub api_key: String,
    pub page_size: u64,
    pub max_pages: u64,
    pub with_vector: bool,
}

/// src.weaviate: paginate GET /v1/objects?class=&limit=&after=. Each
/// page returns `objects: [{id, class, properties, vector?}]`; the
/// cursor is the last object's id, passed back as `after` on the
/// next request. Engine flattens each object into {id, ...properties[, vector]}.
#[derive(Debug, Clone)]
pub struct WeaviateSourceSpec {
    pub node_id: String,
    pub endpoint: String,
    pub class: String,
    pub api_key: String,
    pub page_size: u64,
    pub max_pages: u64,
    pub with_vector: bool,
}

/// src.milvus: paginate POST /v1/vector/query with {collectionName,
/// filter, outputFields, limit, offset}. Each page returns
/// `data: [...]`; engine walks offset += limit until a short page.
#[derive(Debug, Clone)]
pub struct MilvusSourceSpec {
    pub node_id: String,
    pub endpoint: String,
    pub collection: String,
    pub api_key: String,
    pub filter: String,
    pub output_fields: Vec<String>,
    pub page_size: u64,
    pub max_pages: u64,
}

/// Which config-data format a FormatFileSource/Sink uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatKind {
    Yaml,
    Toml,
}

/// src.yaml / src.toml: parse a single file with the relevant serde
/// crate. If the document is an array, each element becomes a row;
/// otherwise the whole document is one row. Suits config-data /
/// IaC-style imports where each YAML/TOML doc is small.
#[derive(Debug, Clone)]
pub struct FormatFileSourceSpec {
    pub node_id: String,
    pub path: String,
    pub format: FormatKind,
}

/// snk.yaml / snk.toml: serialize the upstream's rows as a single
/// document. Default shape is a top-level array of objects; for TOML
/// this means each row becomes a [[row]] table entry under a `rows`
/// key (TOML's top-level grammar disallows a bare array). YAML is
/// emitted as a clean `- key: value` array.
#[derive(Debug, Clone)]
pub struct FormatFileSinkSpec {
    pub from_view: String,
    pub path: String,
    pub format: FormatKind,
}

/// snk.kafka / snk.redpanda: bulk-produce one Kafka record per
/// upstream row. Record key = optional keyColumn value; record value
/// = JSON-stringified row. Records are produced into a single
/// partition (partitionId, default 0) - parallel multi-partition
/// produce is a follow-up.
#[derive(Debug, Clone)]
pub struct KafkaSinkSpec {
    pub from_view: String,
    /// Comma-separated list of "host:port" entries.
    pub bootstrap_servers: String,
    pub topic: String,
    pub partition_id: i32,
    /// Empty = no record key.
    pub key_column: String,
    /// Records per produce batch. Defaults to 500 - bigger means
    /// fewer broker round-trips but more memory.
    pub batch_size: usize,
}

/// src.kafka / src.redpanda: batch-consume up to `max_records`
/// messages from a single partition starting at `start_offset`
/// (negative = read from earliest). Emits {offset, key, value, timestamp_ms}
/// rows; value is the raw byte string (no schema unpacking, no Avro).
#[derive(Debug, Clone)]
pub struct KafkaSourceSpec {
    pub node_id: String,
    pub bootstrap_servers: String,
    pub topic: String,
    pub partition_id: i32,
    pub start_offset: i64,
    pub max_records: u64,
}

/// src.avro: read an Apache Avro container file (.avro / .ocf) via
/// the pure-Rust apache-avro crate. Each Avro record becomes one
/// row; complex types (records / maps / arrays) are flattened to
/// JSON values which DuckDB handles natively. No schema config -
/// the container file carries its own schema in the header.
#[derive(Debug, Clone)]
pub struct AvroSourceSpec {
    pub node_id: String,
    pub path: String,
}

/// snk.nats: publish each upstream row as one NATS message to the
/// configured subject. value = JSON-stringified row. Optional
/// per-message subject suffix from a row column (e.g. tenant key).
#[derive(Debug, Clone)]
pub struct NatsSinkSpec {
    pub from_view: String,
    /// Comma-separated NATS URLs (nats://host:port,...).
    pub urls: String,
    pub subject: String,
    /// Optional column whose value becomes a suffix on the subject
    /// per-row (subject + "." + value). Empty = single subject.
    pub subject_suffix_column: String,
    pub batch_size: usize,
}

/// src.nats: subscribe to a subject for up to timeout_ms or until
/// max_records messages arrive. Emits {subject, payload, headers}
/// rows. Best-fit for "snapshot a queue" and "drain a topic" patterns;
/// continuous streaming is a separate engine workstream.
#[derive(Debug, Clone)]
pub struct NatsSourceSpec {
    pub node_id: String,
    pub urls: String,
    pub subject: String,
    pub max_records: u64,
    /// Total wall-clock wait cap. Loop exits when this elapses even
    /// if max_records isn't reached.
    pub timeout_ms: u64,
}

/// snk.pubsub: publish via POST /v1/projects/{project}/topics/{topic}:publish.
/// Auth: pre-fetched OAuth Bearer access token (the same one
/// `gcloud auth print-access-token` mints) - sidesteps the
/// service-account-JWT-minting + token-refresh worker that the full
/// Google client needs. Body: {messages: [{data: base64, attributes: {}}]}.
#[derive(Debug, Clone)]
pub struct PubSubSinkSpec {
    pub from_view: String,
    pub project: String,
    pub topic: String,
    pub access_token: String,
    pub batch_size: usize,
}

/// src.pubsub: pull via POST /v1/projects/{project}/subscriptions/{sub}:pull.
/// Auto-acknowledges the batch (acknowledge endpoint). Emits
/// {message_id, publish_time, data} rows. Same Bearer-token auth.
#[derive(Debug, Clone)]
pub struct PubSubSourceSpec {
    pub node_id: String,
    pub project: String,
    pub subscription: String,
    pub access_token: String,
    pub max_messages: u64,
}

/// src.xml: walk an XML document, find every element matching a
/// slash-separated path (e.g. "library/books/book"), and emit each
/// match as a JSON object. Attributes prefix with '@'; text content
/// goes to '_text'; nested elements become nested objects (or arrays
/// when the same tag repeats inside a parent).
#[derive(Debug, Clone)]
pub struct XmlSourceSpec {
    pub node_id: String,
    pub path: String,
    /// Slash-separated element names from the root. Empty = take
    /// every immediate child of the root.
    pub row_path: String,
}

/// snk.xml: write rows as
///   <root>
///     <row><col>val</col>...</row>
///     ...
///   </root>
/// rootElement and rowElement are user-configurable. Values are
/// XML-escaped; complex (object / array) values are JSON-encoded
/// inside CDATA - schema-aware nested XML emission would need
/// substantial design work.
#[derive(Debug, Clone)]
pub struct XmlSinkSpec {
    pub from_view: String,
    pub path: String,
    pub root_element: String,
    pub row_element: String,
}

/// snk.avro: write upstream rows as an Apache Avro container file.
/// Schema is inferred from the first row's columns - long for
/// integers, double for floats, string for text, boolean for bool,
/// "string nullable" via union [null, string] when the first
/// non-null example is a string but other rows have nulls. For
/// fully-typed pipelines users can supply a JSON Avro schema via
/// the schemaJson field which bypasses inference.
#[derive(Debug, Clone)]
pub struct AvroSinkSpec {
    pub from_view: String,
    pub path: String,
    /// Optional - if non-empty, parsed as a JSON Avro schema and
    /// used directly. Otherwise the engine infers from the first row.
    pub schema_json: String,
    /// Record name to use when inferring (Avro requires a name).
    pub record_name: String,
}

/// snk.rabbit: publish one AMQP message per upstream row to
/// (exchange, routing_key) via the pure-Rust lapin driver. value =
/// JSON-stringified row. Persistent delivery mode (= survives broker
/// restart). amqp:// URI carries the credentials inline.
#[derive(Debug, Clone)]
pub struct RabbitSinkSpec {
    pub from_view: String,
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
    pub batch_size: usize,
}

/// src.rabbit: pull up to max_messages from a queue, with a
/// per-poll timeout. Emits {payload, routing_key, exchange,
/// delivery_tag} rows. Auto-acks each message; if you need
/// requeue-on-failure semantics use a downstream stage that
/// errors-on-bad-shape and retries.
#[derive(Debug, Clone)]
pub struct RabbitSourceSpec {
    pub node_id: String,
    pub url: String,
    pub queue: String,
    pub max_messages: u64,
    pub timeout_ms: u64,
}

/// src.git: read either commit log or tracked-file list from a local
/// git working copy by shelling out to the system `git` CLI. mode=log
/// emits {hash, short_hash, author_name, author_email, date, subject}
/// rows; mode=files emits {mode, type, hash, size, path} rows. Useful
/// for engineering-analytics pipelines, repo audits, and CI dashboards.
#[derive(Debug, Clone)]
pub struct GitSourceSpec {
    pub node_id: String,
    pub repo: String,
    pub mode: String,
    pub revision: String,
    pub path_filter: Option<String>,
    pub max_rows: u64,
}

/// code.shell: run a single shell command and emit one row with
/// {stdout, stderr, exit_code, duration_ms}. Uses the platform's
/// default interpreter (cmd.exe /C on Windows, /bin/sh -c on Unix);
/// override with `shell` if needed. Cancellation kills the child.
#[derive(Debug, Clone)]
pub struct ShellSpec {
    pub node_id: String,
    pub command: String,
    pub shell: Option<String>,
    pub working_dir: Option<String>,
    pub timeout_ms: Option<u64>,
}

/// src.ftp: download files from an FTP / FTPS server and emit one row
/// per file with {filename, size, content, modified}. Synchronous
/// connection via the suppaftp crate. SFTP is a separate protocol
/// (SSH-based) and a separate component.
#[derive(Debug, Clone)]
pub struct FtpSourceSpec {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub secure: bool,
    pub directory: String,
    pub pattern: Option<String>,
    pub max_files: u64,
}

/// src.clipboard: read the system clipboard. If the text parses as
/// JSON-array-of-objects, the array becomes rows directly; otherwise
/// a single row {text, length} is emitted. Desktop-only by definition;
/// fails clearly on headless systems where no display is reachable.
#[derive(Debug, Clone)]
pub struct ClipboardSourceSpec {
    pub node_id: String,
}

/// src.email: connect to an IMAP server, select a mailbox, fetch up
/// to max_messages most recent. Emits {uid, from, to, subject, date,
/// body_text}. TLS via rustls (default port 993). Basic auth -
/// OAuth is on the roadmap for gmail / o365.
#[derive(Debug, Clone)]
pub struct EmailSourceSpec {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub mailbox: String,
    pub max_messages: u64,
}

/// snk.email: per-row SMTP send via lettre. Per-row to/subject/body
/// columns let one stage send N personalized messages.
#[derive(Debug, Clone)]
pub struct EmailSinkSpec {
    pub from_view: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub from_address: String,
    pub to_column: String,
    pub subject_column: String,
    pub body_column: String,
}

/// src.dynamodb: DynamoDB Scan via direct HTTP + SigV4 signing.
/// Unwraps DynamoDB's typed-attribute format ({"S": "x"}, {"N": "5"})
/// into plain JSON values. Pagination via ExclusiveStartKey -
/// follows up to max_pages page calls (safety net against runaway).
#[derive(Debug, Clone)]
pub struct DynamoDbSourceSpec {
    pub node_id: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub table_name: String,
    pub limit_per_page: u64,
    pub max_pages: u64,
}

/// src.kinesis: read records from a single Kinesis shard via direct
/// HTTP + SigV4. ListShards -> GetShardIterator(TRIM_HORIZON or
/// LATEST) -> GetRecords loop until max_records or no more data.
/// Each record's Data is base64-decoded; if the decoded payload is
/// valid JSON object, that object is the row; otherwise we emit
/// {partition_key, sequence_number, data}. Multi-shard parallelism
/// deferred to a follow-up.
#[derive(Debug, Clone)]
pub struct KinesisSourceSpec {
    pub node_id: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub stream_name: String,
    pub shard_index: usize,
    pub iterator_type: String,
    pub max_records: u64,
}

/// src.webhook: bind 127.0.0.1:port, accept up to `max_requests`
/// inbound HTTP requests with a global `timeout_ms` deadline, parse
/// each request body as JSON (or fall back to a {body, method, path,
/// headers} row), close the listener. Useful for local webhook
/// receivers - dev tunnels (ngrok / cloudflared) point at our port.
#[derive(Debug, Clone)]
pub struct WebhookSourceSpec {
    pub node_id: String,
    pub port: u16,
    pub max_requests: u64,
    pub timeout_ms: u64,
    /// Optional path filter - only requests whose URL starts with
    /// this string count toward max_requests. Other requests get a
    /// 404 but don't count.
    pub path_filter: Option<String>,
}

/// xf.ai.embed: per-row embedding transform. Reads `input_column`
/// from each upstream row, batches up to `batch_size`, POSTs to
/// `{base_url}/v1/embeddings` with Bearer `api_key`, adds the
/// returned vector to each row under `output_column` (DOUBLE[]).
/// Works with any OpenAI-compatible provider (Cohere, Voyage,
/// llama.cpp embedding server, etc) - just change base_url.
#[derive(Debug, Clone)]
pub struct AiEmbedSpec {
    pub node_id: String,
    pub from_view: String,
    pub input_column: String,
    pub output_column: String,
    pub model: String,
    pub api_key: String,
    pub base_url: String,
    pub batch_size: usize,
}

/// code.wasm: per-row WASM transform. The user supplies bytes (via
/// `wasm_b64`, base64-encoded) or a `path` to a .wasm file. The
/// module must export memory and a function `transform(i32, i32)
/// -> i64` where the i64 packs (out_ptr << 32) | out_len. For each
/// upstream row, the engine writes the input text into module memory,
/// calls transform, reads the result back. Modules run sandboxed -
/// no imports allowed.
#[derive(Debug, Clone)]
pub struct WasmSpec {
    pub node_id: String,
    pub from_view: String,
    pub wasm_bytes: Vec<u8>,
    pub input_column: String,
    pub output_column: String,
    pub function: String,
}

/// code.javascript: per-row JS transform via boa_engine (pure-Rust
/// JS interpreter). The user supplies a `script` that ends with a
/// `transform` function expression, e.g.
///   `(row) => ({ ...row, total: row.qty * row.price })`
/// The engine evaluates the script once, then calls transform(row)
/// for each upstream row passing the row as a JS object. The
/// returned object replaces the row. Sandboxed - no globals, no
/// fetch, no fs, no setTimeout.
#[derive(Debug, Clone)]
pub struct JavaScriptSpec {
    pub node_id: String,
    pub from_view: String,
    pub script: String,
}

/// xf.ai.chunk: text splitter for RAG / embedding pipelines. No API
/// call - pure local string slicing. Two modes:
/// - "explode": one row per chunk with chunk_index + chunk_count;
///   non-text columns preserved from the source row.
/// - "array": chunks stored as a JSON array in `output_column`;
///   one row per source row.
#[derive(Debug, Clone)]
pub struct AiChunkSpec {
    pub node_id: String,
    pub from_view: String,
    pub input_column: String,
    pub output_column: String,
    pub chunk_size: usize,
    pub chunk_overlap: usize,
    pub mode: String,
}

/// xf.ai.pii: regex-based PII redaction. Detects emails, phone
/// numbers, SSNs, and credit card patterns; replaces each match
/// with `[REDACTED-EMAIL]` (etc) in the output column. Output column
/// defaults to overwriting the input column. LLM-based redaction is
/// a follow-up that would share the xf.ai.embed credential pattern.
#[derive(Debug, Clone)]
pub struct AiPiiSpec {
    pub node_id: String,
    pub from_view: String,
    pub input_column: String,
    pub output_column: String,
    /// Subset of {"email","phone","ssn","credit_card"}. Empty = all.
    pub types: Vec<String>,
}

/// xf.ai.llm: per-row chat completion. POSTs to {base_url}/v1/chat/
/// completions with Bearer api_key. Prompt is rendered from
/// `prompt_template` with {column_name} substitution; if empty, the
/// row's `input_column` text is sent as the user message verbatim.
/// Optional `system_prompt`. Result lands in `output_column`.
#[derive(Debug, Clone)]
pub struct AiLlmSpec {
    pub node_id: String,
    pub from_view: String,
    pub input_column: String,
    pub output_column: String,
    pub model: String,
    pub api_key: String,
    pub base_url: String,
    pub prompt_template: String,
    pub system_prompt: Option<String>,
    pub temperature: f64,
}

/// xf.ai.classify: per-row LLM-backed classifier. Pins each row's
/// input_column text into one of `categories`. Builds a constrained
/// classification prompt and sends to the same chat completions
/// endpoint as xf.ai.llm. Result is the chosen category name in
/// output_column (or "UNKNOWN" if the model returns something
/// not in the category list).
#[derive(Debug, Clone)]
pub struct AiClassifySpec {
    pub node_id: String,
    pub from_view: String,
    pub input_column: String,
    pub output_column: String,
    pub categories: Vec<String>,
    pub model: String,
    pub api_key: String,
    pub base_url: String,
}

/// xf.ai.dedupe: semantic dedupe via cosine similarity over a
/// pre-computed embedding column (typically from xf.ai.embed). Keeps
/// the first occurrence; drops any subsequent row whose embedding is
/// within `threshold` cosine similarity of a kept row. No API call -
/// pure local math. O(N^2) per stage - fine for ETL-scale datasets.
#[derive(Debug, Clone)]
pub struct AiDedupeSpec {
    pub node_id: String,
    pub from_view: String,
    pub embedding_column: String,
    pub threshold: f64,
}

/// snk.cassandra / snk.scylla: CQL INSERT via the scylla driver
/// (pure Rust, speaks CQL to both Cassandra + ScyllaDB).
#[derive(Debug, Clone)]
pub struct CassandraSinkSpec {
    pub from_view: String,
    /// Comma-separated list of contact points (host:port).
    pub contact_points: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub keyspace: String,
    pub table: String,
    pub batch_size: usize,
}

/// src.cassandra / src.scylla: CQL SELECT via scylla.
#[derive(Debug, Clone)]
pub struct CassandraSourceSpec {
    pub node_id: String,
    pub contact_points: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub keyspace: Option<String>,
    pub query: String,
}

/// snk.sqlserver / snk.synapse: TDS INSERT via tiberius. Synapse
/// rides the same wire. Multi-row VALUES batched at 1000 rows (the
/// SQL Server max per INSERT).
#[derive(Debug, Clone)]
pub struct SqlServerSinkSpec {
    pub from_view: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub batch_size: usize,
    /// If true, skip TLS cert verification - useful for self-signed
    /// dev servers. Production users leave this off.
    pub trust_cert: bool,
}

/// src.sqlserver / src.synapse: TDS SELECT via tiberius.
#[derive(Debug, Clone)]
pub struct SqlServerSourceSpec {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub query: String,
    pub trust_cert: bool,
}

/// snk.clickhouse: HTTP INSERT to a ClickHouse table.
///   POST {endpoint}/?query=INSERT INTO {db}.{table} FORMAT JSONEachRow
///   Body: NDJSON lines (one row per line)
///   Auth: X-ClickHouse-User / X-ClickHouse-Key headers.
/// No new deps - rides the existing ureq.
#[derive(Debug, Clone)]
pub struct ClickHouseSinkSpec {
    pub from_view: String,
    /// Full endpoint like "http://localhost:8123" or "https://...".
    pub endpoint: String,
    pub database: Option<String>,
    pub table: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub batch_size: usize,
}

/// src.clickhouse: HTTP SELECT against ClickHouse.
///   POST {endpoint}/ with body "SELECT ... FORMAT JSON"
///   Response: { "meta": [...], "data": [...], "rows": N }
#[derive(Debug, Clone)]
pub struct ClickHouseSourceSpec {
    pub node_id: String,
    pub endpoint: String,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Either a free SQL `query` or (table) which becomes SELECT * FROM table.
    pub query: String,
}

/// snk.mongodb: bulk-insert documents into a MongoDB collection via
/// the official Rust driver. Async-under-the-hood; the executor runs
/// it on a small tokio runtime via block_on.
#[derive(Debug, Clone)]
pub struct MongoSinkSpec {
    pub from_view: String,
    /// Standard mongodb:// URI (with credentials inline).
    pub uri: String,
    pub database: String,
    pub collection: String,
    /// 'insert' = insert_many; 'replace' = drop the collection first
    /// then insert; 'upsert' is a follow-up commit (needs key column).
    pub mode: String,
    pub batch_size: usize,
}

/// src.mongodb: find() against a MongoDB collection with an optional
/// filter (JSON-encoded). Cursor is drained eagerly and materialized
/// as a DuckDB table via read_json_auto.
#[derive(Debug, Clone)]
pub struct MongoSourceSpec {
    pub node_id: String,
    pub uri: String,
    pub database: String,
    pub collection: String,
    /// Optional filter as JSON; empty / None = match-all.
    pub filter: Option<String>,
    /// Optional projection as JSON.
    pub projection: Option<String>,
    /// Hard cap on the cursor result count. None = unbounded.
    pub limit: Option<i64>,
}

/// Elasticsearch / OpenSearch pagination strategy.
#[derive(Debug, Clone)]
pub enum ElasticPagination {
    /// Classic from+size. Bounded by index.max_result_window (10k
    /// default). Simpler but stops working at scale.
    FromSize,
    /// search_after with a sort + last-hit cursor. Unbounded by
    /// max_result_window. Requires a consistent sort with a
    /// tiebreaker; defaults to [{"_shard_doc": "asc"}] (Elasticsearch
    /// 7.12+) or whatever the user supplies via `sort`.
    SearchAfter { sort: Vec<serde_json::Value> },
}

/// src.elastic / src.opensearch: read from Elasticsearch-compatible
/// _search APIs. Both vendors share the same wire protocol, so they
/// ride one executor. Pagination mode is either from+size (default)
/// or search_after - the latter lifts the 10k max_result_window cap.
#[derive(Debug, Clone)]
pub struct ElasticSourceSpec {
    pub node_id: String,
    /// Cluster endpoint, e.g. "https://my-cluster.es.cloud.es.io".
    pub endpoint: String,
    /// Index pattern (single index, comma-separated list, or wildcard).
    pub index: String,
    /// Optional API key for `Authorization: ApiKey <key>`.
    pub api_key: Option<String>,
    /// Raw Elasticsearch query DSL. Empty / None = `{"match_all": {}}`.
    pub query: Option<String>,
    /// Page size (default 1000).
    pub size: u64,
    pub max_pages: u64,
    /// Which pagination to use.
    pub pagination: ElasticPagination,
}

/// Pagination style for src.rest.
#[derive(Debug, Clone)]
pub enum RestPagination {
    /// Single-shot fetch; no follow-up requests.
    None,
    /// Extract a cursor token from `next_path` in each response,
    /// append as `?<param>=<cursor>` until the cursor is empty.
    Cursor { next_path: String, param: String },
    /// Increment `?<offset_param>=N` by `page_size` each call until a
    /// page returns fewer than `page_size` rows.
    Offset { offset_param: String, page_size: u64 },
    /// Increment `?<page_param>=N` starting at `start_page` (default 1)
    /// until a page returns 0 rows.
    Page { page_param: String, start_page: u64 },
    /// Follow RFC 5988 `Link` response header with rel="next".
    Link,
    /// Take the value at `next_path` from the response body and use it
    /// directly as the next URL (no token-append step). This is the
    /// OData / Microsoft Graph style: `@odata.nextLink` is already a
    /// complete URL including all query params for the next page.
    NextUrl { next_path: String },
}

/// Response body parser for src.rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestResponseFormat {
    /// Parse as JSON; walk `response_path` JSON pointer to find rows.
    Json,
    /// Parse as XML; walk `response_path` as an element-name path
    /// (e.g. `Envelope/Body/GetTickersResponse/Tickers/Ticker`) and
    /// emit one row per match. Used by src.soap and other XML APIs.
    /// Pagination is forced to None for XML (SOAP doesn't define a
    /// cross-envelope pagination convention).
    Xml,
}

/// src.rest: generic HTTP-API source. Fetches a URL, parses the JSON
/// response, optionally walks a JSON pointer (`response_path`) to
/// extract the array of row objects, and optionally follows
/// pagination via cursor / offset / page-number / Link header.
/// Materializes the accumulated rows as a DuckDB table via read_json_auto.
#[derive(Debug, Clone)]
pub struct RestSourceSpec {
    pub node_id: String,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    /// JSON pointer (RFC 6901) for JSON responses, or slash-separated
    /// element-name walk for XML responses. Empty string = the
    /// response root IS the row container.
    pub response_path: String,
    /// JSON (default) or XML body parsing.
    pub response_format: RestResponseFormat,
    /// How to walk subsequent pages.
    pub pagination: RestPagination,
    /// Hard cap on pages fetched (safety net against runaway loops).
    pub max_pages: u64,
}

/// src.databricks: SQL Statement Execution API read. Same shape as
/// the Snowflake source - sends a SELECT, materializes the response.
#[derive(Debug, Clone)]
pub struct DatabricksSourceSpec {
    pub node_id: String,
    pub workspace: String,
    pub endpoint: Option<String>,
    pub pat: String,
    pub warehouse_id: String,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub query: String,
    pub wait_timeout_seconds: u64,
}

/// snk.databricks: Databricks SQL Statement Execution API insert.
/// Same shape as Snowflake (multi-row INSERT per batch, Bearer PAT
/// auth), but the body fields and identifier quoting are different:
///   - URL: https://<workspace>/api/2.0/sql/statements/
///   - body: { statement, warehouse_id, catalog?, schema?, wait_timeout,
///     on_wait_timeout: "CONTINUE" }
///   - identifiers quoted with backticks (`name`) instead of double quotes
#[derive(Debug, Clone)]
pub struct DatabricksSinkSpec {
    pub from_view: String,
    /// Workspace host (e.g. "dbc-xxxx.cloud.databricks.com"), used to
    /// build https://<workspace>/api/2.0/sql/statements/.
    pub workspace: String,
    /// Optional endpoint override (full URL) - used by tests.
    pub endpoint: Option<String>,
    pub pat: String,
    pub warehouse_id: String,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
    pub batch_size: usize,
    pub wait_timeout_seconds: u64,
}

/// snk.webhook / snk.rest / vendor HTTP sinks: one HTTP POST/PUT
/// per row, or a single batched request whose body is the entire
/// result as a JSON array or NDJSON bulk doc set. ureq keeps the
/// per-stage CLI shape we already use; no tokio required.
#[derive(Debug, Clone)]
pub struct WebhookSpec {
    pub from_view: String,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    /// Body shape:
    ///   'row'         - one POST per row, body = row JSON
    ///   'batch'       - single POST, body = entire result as JSON array
    ///   'ndjson_bulk' - single POST, NDJSON pairs (action + doc per row)
    ///                   for Elasticsearch / OpenSearch bulk APIs.
    pub body_shape: String,
    /// Optional batch-mode wrap: when set, the array body is wrapped
    /// in {body_wrap: [...]} so vendors like Pinecone ('vectors'),
    /// Qdrant ('points'), or Weaviate ('objects') get the shape they
    /// expect without the user hand-building the JSON.
    pub body_wrap: Option<String>,
    /// Extra static fields injected into the wrapped object alongside
    /// the array. Used by Milvus ({collectionName: ..., data: [...]})
    /// and other vendors whose body has metadata + the array side by
    /// side.
    pub body_extras: Vec<(String, serde_json::Value)>,
    /// NDJSON bulk only: the action line emitted before each row.
    /// E.g. `{"index":{"_index":"docs"}}` for Elasticsearch bulk.
    pub bulk_action: Option<String>,
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
    // Also count consumers per (source_node, source_handle) so we know
    // when it's safe to emit a CREATE VIEW (lazy) vs CREATE TABLE
    // (materialized). A node with exactly one downstream consumer can
    // be a view: DuckDB inlines it into the single downstream query,
    // gets predicate / projection pushdown into the source read, and
    // skips an intermediate materialize-to-disk. A node with multiple
    // consumers gets materialized so each consumer reads it once
    // instead of re-evaluating the chain.
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
        *consumer_count.entry(source_ref.clone()).or_insert(0) += 1;
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
        let stage = build_stage(node, component_id, node_inputs, &consumer_count)?;
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

/// Reject-port outputs are named "<node>__reject"; the schema map keys
/// on the unsuffixed node id.
fn strip_reject_suffix(s: &str) -> &str {
    s.strip_suffix(REJECT_SUFFIX).unwrap_or(s)
}

/// Compute the output column set a node exposes to its consumers,
/// given its own declared schema (if any) and its upstream's set.
///
/// Returns None when we don't know - either the upstream is unknown
/// or the component transforms columns in ways the planner doesn't
/// model (project, join, aggregation, etc). None disables column
/// validation for downstream nodes that read this output.
fn derive_output_columns(
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
fn validate_column_refs(
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

/// SQL for a `ctl.*` node that exposes its single upstream unchanged under
/// its own name (wait, throttle, barrier, checkpoint, runpipeline, iterate,
/// try, trigger, foreach, gate, ...). Their real effect (delay, sub-pipeline
/// run, assertion, durable copy) happens in the Rust executor; the SQL is
/// purely a rename. A VIEW is correct and far cheaper than a TABLE: DuckDB
/// inlines it into whatever reads it, with no row copy. The old
/// `CREATE TABLE x AS SELECT * FROM upstream` copied every upstream row to
/// disk for nothing - ~12s for a 10M-row dataset flowing through a single
/// control node, versus ~5ms as a view.
fn passthrough_view_sql(node_id: &str, upstream: &str) -> String {
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
fn passthrough_placeholder_sql(node_id: &str, marker: &str) -> String {
    format!(
        "CREATE OR REPLACE VIEW {} AS SELECT '{}' AS status WHERE 1=0",
        quote_ident(node_id),
        marker.replace('\'', "''")
    )
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

/// Components that legitimately accept more than one edge on the `main`
/// port (they read every upstream via all_main_ports, not just the
/// first). Everything else is single-input and must reject fan-in.
fn is_multi_main_component(component_id: &str) -> bool {
    matches!(
        component_id,
        "xf.union" | "xf.unionall" | "xf.intersect" | "xf.except"
    )
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
    let mut run_pipeline_path: Option<String> = None;
    let mut install_fallback_path: Option<String> = None;
    let mut iterate_pipeline_path: Option<String> = None;
    let mut iterate_count: Option<u64> = None;
    let mut foreach_pipeline_path: Option<String> = None;
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
    let mut ftp_source: Option<FtpSourceSpec> = None;
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
    // ATTACH statements for external-DB nodes (DuckDB/SQLite). Each stage
    // runs in its own CLI process, so fixed aliases are collision-free.
    let attach = attach_prelude(component_id, &props);
    let (sql, kind, from) = if component_id == "snk.graphql" {
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
        let auth_type = string_prop(&props, "authType").unwrap_or_else(|| "none".into());
        let auth_token = string_prop(&props, "authToken").unwrap_or_default();
        if !auth_token.is_empty() {
            match auth_type.as_str() {
                "bearer" => headers.push(("Authorization".into(), format!("Bearer {}", auth_token))),
                "apikey" => headers.push(("X-API-Key".into(), auth_token)),
                _ => {}
            }
        }
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
        let auth_type = string_prop(&props, "authType").unwrap_or_else(|| "none".into());
        let auth_token = string_prop(&props, "authToken").unwrap_or_default();
        if !auth_token.is_empty() {
            match auth_type.as_str() {
                "bearer" => headers.push((
                    "Authorization".into(),
                    format!("Bearer {}", auth_token),
                )),
                "apikey" => headers.push(("X-API-Key".into(), auth_token)),
                _ => {}
            }
        }
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
            port: props.get("port").and_then(|v| v.as_u64()).unwrap_or(1433) as u16,
            user,
            password,
            database,
            schema: string_prop(&props, "schema").unwrap_or_else(|| "dbo".into()),
            table,
            batch_size: props.get("batchSize").and_then(|v| v.as_u64()).filter(|n| *n > 0).unwrap_or(1000) as usize,
            trust_cert: props.get("trustCert").and_then(|v| v.as_bool()).unwrap_or(false),
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
    } else if component_id == "ctl.runpipeline" || component_id == "ctl.trigger" {
        // Side-effect: read + execute the referenced pipeline file
        // before passing this node's upstream view through. Form
        // writes `pipelineRef` (path to a .json pipeline doc) +
        // optional `waitForCompletion` (currently always true; async
        // fire-and-forget would need scheduler integration).
        // Without an upstream input, the stage emits an empty table
        // so downstream nodes can still chain off it as a 'trigger
        // happened' signal.
        let path = string_prop(&props, "pipelineRef")
            .or_else(|| string_prop(&props, "path"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: pipelineRef (path to a pipeline file) required", component_id)))?;
        run_pipeline_path = Some(path);
        // Pass-through view: use main input if present, otherwise
        // synthesize an empty placeholder so downstream wiring still
        // has a target.
        let sql = match inputs.main() {
            Some(from_view) => passthrough_view_sql(&node.id, from_view),
            None => passthrough_placeholder_sql(&node.id, "triggered"),
        };
        (sql, StageKind::View, None)
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
        adbc_source = Some(AdbcSourceSpec {
            node_id: node.id.clone(),
            driver,
            entrypoint: string_prop(&props, "entrypoint").filter(|s| !s.is_empty()),
            options,
            query,
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
            start_offset: props.get("startOffset").and_then(|v| v.as_i64()).unwrap_or(-1),
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
        // FTP / FTPS list+download. List files at `directory`, filter
        // by optional glob `pattern` (* and ? wildcards), download
        // each up to `maxFiles`. Each file becomes a row with the
        // bytes as a base64 string in `content` (so the row is JSON-
        // serializable and round-trips through DuckDB cleanly).
        let host = string_prop(&props, "host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(format!("{}: host required", component_id)))?;
        ftp_source = Some(FtpSourceSpec {
            node_id: node.id.clone(),
            host,
            port: props
                .get("port")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0 && *n < 65536)
                .map(|n| n as u16)
                .unwrap_or(21),
            user: string_prop(&props, "user").unwrap_or_else(|| "anonymous".into()),
            password: string_prop(&props, "password").unwrap_or_else(|| "anonymous@".into()),
            secure: props
                .get("secure")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            directory: string_prop(&props, "directory")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "/".into()),
            pattern: string_prop(&props, "pattern").filter(|s| !s.is_empty()),
            max_files: props
                .get("maxFiles")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .unwrap_or(100),
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
            port: props.get("port").and_then(|v| v.as_u64()).unwrap_or(1433) as u16,
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
        let auth_type = string_prop(&props, "authType").unwrap_or_else(|| "none".into());
        let auth_token = string_prop(&props, "authToken").unwrap_or_default();
        if !auth_token.is_empty() {
            match auth_type.as_str() {
                "bearer" => headers.push(("Authorization".into(), format!("Bearer {}", auth_token))),
                "apikey" => headers.push(("X-API-Key".into(), auth_token)),
                _ => {}
            }
        }
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
        let auth_type = string_prop(&props, "authType").unwrap_or_else(|| "none".into());
        let auth_token = string_prop(&props, "authToken").unwrap_or_default();
        if !auth_token.is_empty() {
            match auth_type.as_str() {
                "bearer" => headers.push(("Authorization".into(), format!("Bearer {}", auth_token))),
                "apikey" => headers.push(("X-API-Key".into(), auth_token)),
                _ => {}
            }
        }
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
                RestPagination::Offset { offset_param: param, page_size }
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
        let body = build_view_sql(
            component_id,
            &props,
            inputs,
            node.data.schema.as_deref(),
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
        // Only build the reject split when a downstream node actually reads
        // the reject port. An unwired reject port (the common plain-Filter
        // case) otherwise materialized the entire rejected set to disk for
        // nothing: on a 10M-row -> 2M-pass filter that wrote the 8M rejected
        // rows to a temp table, which dominated the stage's runtime (~12s).
        let reject_ref = output_table_ref(&node.id, Some("reject"));
        let reject_consumers = consumer_count.get(&reject_ref).copied().unwrap_or(0);
        let reject_sql = if reject_consumers >= 1 {
            build_reject_sql(component_id, &props, inputs).map_err(|e| {
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
            matches!(component_id, "xf.transpose" | "xf.pivot");
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
        let view_ok = |consumers: usize| {
            !uses_dynamic_pivot && !attach_backed && (force_views || consumers <= 1)
        };
        let main_kw = if view_ok(main_consumers) { "VIEW" } else { "TABLE" };
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
        text_search,
        run_pipeline_path,
        install_fallback_path,
        iterate_pipeline_path,
        iterate_count,
        foreach_pipeline_path,
        webhook,
        snowflake_sink,
        databricks_sink,
        snowflake_source,
        databricks_source,
        rest_source,
        elastic_source,
        mongo_sink,
        mongo_source,
        clickhouse_sink,
        clickhouse_source,
        sqlserver_sink,
        sqlserver_source,
        cassandra_sink,
        cassandra_source,
        oracle_sink,
        oracle_source,
        adbc_source,
        redis_sink,
        redis_source,
        qdrant_source,
        weaviate_source,
        milvus_source,
        format_source,
        format_sink,
        kafka_sink,
        kafka_source,
        avro_source,
        nats_sink,
        nats_source,
        pubsub_sink,
        pubsub_source,
        xml_source,
        xml_sink,
        avro_sink,
        rabbit_sink,
        rabbit_source,
        git_source,
        shell,
        ftp_source,
        clipboard_source,
        ai_embed,
        wasm,
        javascript,
        ai_chunk,
        ai_pii,
        ai_llm,
        ai_classify,
        ai_dedupe,
        email_source,
        email_sink,
        webhook_source,
        dynamodb_source,
        kinesis_source,
        wait_ms,
        retry_attempts,
        retry_backoff_ms,
        memory_limit_mb,
    })
}

/// The `SELECT * FROM <reader>` SQL for a source format - used by the
/// engine's inspect path to DESCRIBE / sample without materializing.
pub fn source_select_for_format(format: &str, props: &JsonValue) -> Option<String> {
    Some(match format {
        "csv" => build_csv_source(props, None),
        "tsv" => build_tsv_source(props, None),
        "parquet" => build_parquet_source(props),
        "json" | "jsonl" | "ndjson" => build_json_source(props),
        "sqlite" => build_sqlite_source(props),
        "duckdb" => build_duckdb_source(props),
        "s3" | "gcs" | "azureblob" | "http" | "https" => build_cloud_source(format, props, None),
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
    declared: Option<&[duckle_metadata::Column]>,
) -> Result<String, String> {
    match component_id {
        // Sources - declared schema is consulted only by formats that
        // accept a `columns = {...}` override (CSV / TSV today). Other
        // sources auto-infer and ignore `declared`.
        "src.csv" => Ok(build_csv_source(props, declared)),
        "src.tsv" => Ok(build_tsv_source(props, declared)),
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
            Ok(build_cloud_source(scheme, props, declared))
        }
        "src.postgres" | "src.cockroach" | "src.mysql" | "src.mariadb"
        | "src.motherduck" | "src.ducklake" | "src.pgvector"
        | "src.redshift" | "src.bigquery" | "src.quack" => build_relational_source(component_id, props),
        "src.avro" => Ok(build_avro_source(props)),
        "src.excel" => Ok(build_excel_source(props)),
        "src.iceberg" => Ok(build_iceberg_source(props)),
        "src.delta" => Ok(build_delta_source(props)),
        "src.spatial" => Ok(build_spatial_source(props)),
        "src.fixedwidth" => build_fixedwidth_source(props),
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
        "xf.approx.quantile" => build_approx_quantile(inputs, props),
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
        "xf.join.spatial" => build_spatial_join(inputs, props),
        "xf.regex" | "xf.regex.extract" | "xf.regex.match" | "xf.trim" | "xf.case"
        | "xf.length" | "xf.substring" | "xf.concat" | "xf.split" | "xf.format" => {
            build_string(inputs, props, component_id)
        }
        "xf.url.parse" => build_url_parse(inputs, props),
        "xf.assert" => build_assert(inputs, props),
        "xf.hash" => build_hash(inputs, props),
        "xf.ip.parse" => build_ip_parse(inputs, props),
        "xf.geo.distance" => build_geo_distance(inputs, props),
        "xf.geo.buffer" => build_geo_buffer(inputs, props),
        "xf.geo.intersects" => build_geo_intersects(inputs, props),
        "xf.num.round" | "xf.num.abs" | "xf.num.mod" | "xf.num.power" | "xf.num.sqrt"
        | "xf.num.log" => build_numeric(inputs, props, component_id),
        "xf.num.bucketize" => build_bucketize(inputs, props),
        "xf.num.zscore" => build_zscore(inputs, props),
        "xf.num.clamp" => build_clamp(inputs, props),
        "xf.num.sign" => build_sign(inputs, props),
        "xf.rank.filter" => build_rank_filter(inputs, props),
        "xf.fill_forward" => build_fill_forward(inputs, props),
        "xf.fill_backward" => build_fill_backward(inputs, props),
        "xf.fill_constant" => build_fill_constant(inputs, props),
        "xf.row_hash" => build_row_hash(inputs, props),
        "xf.audit" => build_audit(inputs, props),
        "xf.cumulative" => build_cumulative(inputs, props),
        "xf.dt.bin" => build_dt_bin(inputs, props),
        "xf.arr.length" => build_arr_length(inputs, props),
        "xf.uuid" => build_uuid(inputs, props),
        "xf.dt.parse" | "xf.dt.format" | "xf.dt.extract" | "xf.dt.trunc" | "xf.dt.tz" => {
            build_datetime(inputs, props, component_id)
        }
        "xf.dt.add" => build_date_add(inputs, props),
        "xf.dt.diff" => build_date_diff(inputs, props),
        "xf.dt.now" => build_dt_now(inputs, props),
        "xf.dt.epoch" => build_dt_epoch(inputs, props),
        "xf.json.parse" | "xf.json.stringify" | "xf.json.path" => {
            build_json(inputs, props, component_id)
        }
        "xf.json.flatten" => build_json_flatten(inputs, props),
        "xf.json.merge" => build_json_merge(inputs, props),
        "xf.json.array_agg" => build_json_array_agg(inputs, props),
        "xf.text.similarity" => build_text_similarity(inputs, props),
        "xf.text.base64" => build_base64(inputs, props),
        "xf.text.padding" => build_padding(inputs, props),
        "xf.text.match" => build_text_match(inputs, props),
        "xf.text.reverse" => build_text_reverse(inputs, props),
        "xf.text.repeat" => build_text_repeat(inputs, props),
        "xf.text.replace" => build_text_replace(inputs, props),
        "xf.text.slug" => build_text_slug(inputs, props),
        "xf.text.strip_html" => build_text_strip_html(inputs, props),
        "xf.compare" => build_compare(inputs, props),
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
        // Retry wrapper: passthrough view. Retries are read off the
        // form's Advanced tab as retry_attempts/retry_backoff_ms on
        // THIS stage. Useful as an explicit marker in the DAG saying
        // "retry up to this point in the pipeline on transient
        // failure"; semantically equivalent to setting Advanced.retry
        // on the next downstream stage, but more visually obvious.
        "ctl.retry" => {
            let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.retry"))?;
            Ok(format!("SELECT * FROM {}", quote_ident(upstream)))
        }
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
    // Optional `orderBy` (comma-separated columns) makes LIMIT / OFFSET
    // deterministic. A bare LIMIT/OFFSET picks an arbitrary slice under
    // preserve_insertion_order=false whenever an upstream operator
    // reorders rows, so xf.skip/xf.topn/xf.limit could skip or keep a
    // different set run-to-run (audit B4). We do NOT auto-inject an
    // ordering (it would change both which rows survive and their order
    // for every existing node, plus cost a full sort) and do NOT require
    // it (would break existing nodes); it's opt-in.
    let order_by = {
        let cols = columns_list(props, "orderBy");
        if cols.is_empty() {
            String::new()
        } else {
            format!(
                " ORDER BY {}",
                cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
            )
        }
    };
    Ok(match kind {
        TakeKind::Limit => format!("SELECT * FROM {}{} LIMIT {}", from, order_by, n),
        TakeKind::Offset => format!("SELECT * FROM {}{} OFFSET {}", from, order_by, n),
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
        // DISTINCT ON keeps the first row per group in ORDER BY order; with
        // no ORDER BY the surviving non-key columns are nondeterministic
        // (worse under preserve_insertion_order=false).
        //
        // Default ORDER BY ALL breaks ties across every column, so the kept
        // row is the deterministic per-group minimum - but it forces a full
        // sort on every column (audit B10: ~1.6s vs ~0.01s on 10M rows, a
        // >100x cost). An optional `orderBy` prop sorts only the key columns
        // plus the chosen tiebreak columns, keeping determinism at a
        // fraction of the cost. The default is unchanged (ORDER BY ALL) so
        // existing pipelines keep their exact current survivor + ordering.
        let tiebreak = columns_list(props, "orderBy");
        let order_clause = if tiebreak.is_empty() {
            "ORDER BY ALL".to_string()
        } else {
            // DISTINCT ON requires its keys to lead the ORDER BY; append the
            // tiebreak columns after them for a deterministic survivor.
            let tb = tiebreak.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
            format!("ORDER BY {}, {}", on, tb)
        };
        Ok(format!(
            "SELECT DISTINCT ON ({}) * FROM {} {}",
            on,
            quote_ident(upstream),
            order_clause
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
                        // Allowlist the direction: an unexpected token spliced
                        // raw would make a malformed ORDER BY / parser error
                        // (audit B5). Map asc/desc explicitly; anything else
                        // falls back to ASC, matching the single-column branch.
                        let dir_kw = match dir.trim().to_ascii_lowercase().as_str() {
                            "desc" => "DESC",
                            _ => "ASC",
                        };
                        Some(format!("{} {}", quote_ident(col), dir_kw))
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
            "APPROX_COUNT_DISTINCT" => format!("approx_count_distinct({})", column_expr),
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
    // Only emit a bare numeric literal for a FINITE number. Rust's f64
    // parse also accepts "inf"/"nan"/"infinity"/"1e999"(->inf), none of
    // which are valid DuckDB numeric tokens - emitting them bare caused a
    // hard parse/binder error. Treat those as string search values.
    let lit = match value.trim().parse::<f64>() {
        Ok(n) if n.is_finite() => value.trim().to_string(),
        _ => format!("'{}'", sql_escape(&value)),
    };
    // COALESCE wrap: list_contains returns NULL when the array column
    // itself is NULL (not just missing the value). Without this, any
    // downstream `WHERE _contains` would silently drop NULL-array rows -
    // same class of bug as the IN/NOT IN gotcha we fixed in semi/anti.
    // Empty array correctly returns FALSE; only the NULL-array case
    // needs the COALESCE shield.
    let expr = format!(
        "COALESCE(list_contains({}, {}), FALSE)",
        quote_ident(&column),
        lit
    );
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
    // Default to `UNION [ALL] BY NAME` - DuckDB-specific syntax that
    // matches columns by name across inputs, padding missing columns
    // with NULL on each side. The standard SQL `UNION [ALL]` matches
    // by POSITION and silently produces garbage if columns are reordered
    // or one input has an extra column. ETL users almost always expect
    // by-name semantics; legacy positional behavior is still reachable
    // by reordering / projecting columns upstream.
    let op = if distinct {
        " UNION BY NAME "
    } else {
        " UNION ALL BY NAME "
    };
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
    // Every function build_window handles is order-sensitive: ROW_NUMBER,
    // RANK, DENSE_RANK, LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTILE all
    // produce nonsense (or DuckDB errors) without ORDER BY. Catch it at
    // compile time with a clear message instead of letting DuckDB raise
    // "OVER clause requires ORDER BY" two stages later.
    if order.is_empty() {
        return Err(format!(
            "Window function '{}' needs at least one Order By column (otherwise the result has no defined order)",
            func
        ));
    }
    let mut over = String::new();
    if !partition.is_empty() {
        over.push_str(&format!(
            "PARTITION BY {}",
            partition.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    if !over.is_empty() {
        over.push(' ');
    }
    over.push_str(&format!(
        "ORDER BY {}",
        order.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
    ));
    let out_name = string_prop(props, "outputName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| func.clone());
    // FIRST_VALUE / LAST_VALUE need an explicit full-partition frame. With
    // an ORDER BY present (always, above) the default window frame is RANGE
    // BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so LAST_VALUE returns the
    // CURRENT row's value, not the partition's last - a silent wrong result.
    // Span the whole partition so "last"/"first" mean what the user picked.
    let frame = match func.as_str() {
        "first" | "last" => " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        _ => "",
    };
    Ok(format!(
        "SELECT *, {} OVER ({}{}) AS {} FROM {}",
        call,
        over,
        frame,
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

/// Escape stray literal `%` in an xf.format pattern so printf does not
/// mis-parse them as conversion specifiers. A bare `%` not beginning a
/// valid spec corrupts the output (audit B5: '100% done' -> '100 5one').
/// Each `%` that does NOT start a valid printf conversion (optional
/// flags/width/precision then a conversion char, or `%%`) is doubled;
/// intended specifiers like %s, %d, %.2f, %% are left untouched.
fn escape_stray_printf_percents(pattern: &str) -> String {
    let bytes = pattern.as_bytes();
    let mut out = String::with_capacity(pattern.len() + 4);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            let ch = pattern[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        let mut j = i + 1;
        if j < bytes.len() && bytes[j] == b'%' {
            out.push_str("%%");
            i = j + 1;
            continue;
        }
        // printf flags, EXCLUDING space: a space after % almost always
        // means a literal percent followed by prose ("50% off"), not the
        // C space-flag. Including it made "% o"/"% d" in ordinary text
        // parse as a spec and skip escaping (audit B5 test).
        while j < bytes.len() && matches!(bytes[j], b'-' | b'+' | b'0' | b'#') {
            j += 1;
        }
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'.' {
            j += 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
        }
        let is_spec = j < bytes.len()
            && matches!(
                bytes[j],
                b's' | b'd' | b'i' | b'u' | b'f' | b'F' | b'g' | b'G' | b'e' | b'E'
                    | b'x' | b'X' | b'o' | b'c' | b'b'
            );
        if is_spec {
            out.push_str(&pattern[i..=j]);
            i = j + 1;
        } else {
            out.push_str("%%");
            i += 1;
        }
    }
    out
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
        "xf.regex.extract" => {
            let group_idx = props
                .get("groupIndex")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                .max(0);
            format!(
                "regexp_extract(CAST({} AS VARCHAR), '{}', {})",
                col,
                sql_escape(&pattern),
                group_idx
            )
        }
        "xf.regex.match" => format!(
            "regexp_matches(CAST({} AS VARCHAR), '{}')",
            col,
            sql_escape(&pattern)
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
        "xf.format" => format!("printf('{}', {})", sql_escape(&escape_stray_printf_percents(&pattern)), col),
        other => return Err(format!("String op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

fn build_numeric(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let arg = num_prop(props, "argument");
    // num_prop accepts any f64-parseable string, including 'inf'/'nan'/
    // 'infinity', which it then emits BARE as an operand. DuckDB parses
    // those tokens as column references, not float literals, so the stage
    // fails with a confusing "column not found" binder error (audit B5,
    // verified). Reject a non-finite numeric argument with a clear planner
    // error. Overflow literals like 1e400 stay allowed - DuckDB accepts
    // them - so only the literal inf/nan spellings are guarded.
    if let Some(a) = arg.as_deref() {
        let low = a.trim().to_ascii_lowercase();
        if matches!(
            low.as_str(),
            "inf" | "-inf" | "+inf" | "infinity" | "-infinity" | "+infinity" | "nan" | "-nan" | "+nan"
        ) {
            return Err(format!(
                "{}: numeric argument must be a finite number (got '{}')",
                component_id, a
            ));
        }
    }
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
        // try_strptime returns NULL on a value that doesn't match the
        // format, instead of strptime's hard error that aborts the entire
        // run on the first unparseable row (one bad date killing a whole
        // pipeline). Matches the TRY_CAST philosophy used elsewhere.
        "xf.dt.parse" => format!("try_strptime(CAST({} AS VARCHAR), '{}')", col, sql_escape(&fmt)),
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
        // One row per element, keeping the other columns. Outer-style: a
        // NULL or empty array yields one row with a NULL element instead
        // of being silently dropped. Plain unnest() of NULL/[] produces
        // zero rows, which loses the row's other columns entirely - real
        // data loss for sparse arrays. The CASE injects a single NULL
        // element so the row survives; untyped [NULL] unifies with any
        // array element type.
        return Ok(format!(
            "SELECT unnest(CASE WHEN {c} IS NULL OR length({c}) = 0 THEN [NULL] ELSE {c} END) AS {c}, * EXCLUDE ({c}) FROM {up}",
            c = col,
            up = quote_ident(upstream)
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

/// Approximate Quantile via DuckDB's t-digest. Single-row aggregate
/// (or one row per group, if `groupBy` is set). Picks `quantile` from
/// 0..1 (default 0.5 = median). approx_quantile uses fixed memory
/// regardless of cardinality, so it's the right tool for "what's the
/// p95 latency over 10B rows" instead of an exact quantile() call
/// that would need to sort the whole input.
fn build_approx_quantile(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.approx.quantile"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Approx Quantile needs a column".to_string())?;
    let q = props.get("quantile").and_then(|v| v.as_f64()).unwrap_or(0.5);
    let q = if (0.0..=1.0).contains(&q) { q } else { 0.5 };
    let group_by: Vec<String> = columns_from_props(props, "groupBy").unwrap_or_default();
    let alias = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_q{}", column, (q * 100.0).round() as i64));
    let select_extra = group_by
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let select = if group_by.is_empty() {
        format!("approx_quantile({}, {}) AS {}", quote_ident(&column), q, quote_ident(&alias))
    } else {
        format!(
            "{}, approx_quantile({}, {}) AS {}",
            select_extra,
            quote_ident(&column),
            q,
            quote_ident(&alias)
        )
    };
    let group_clause = if group_by.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", select_extra)
    };
    Ok(format!(
        "SELECT {} FROM {}{}",
        select,
        quote_ident(upstream),
        group_clause
    ))
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
    // Require compareColumns: with none, the `updated` CASE arm below is
    // empty, so every matched-key row - changed or not - falls through to
    // 'unchanged' and is dropped by the default rejectUnchanged=true,
    // silently losing all updates (audit B3, HIGH). This guard always
    // fires (unlike the schema-gated check_list path in compile()).
    if compares.is_empty() {
        return Err(
            "Diff Detect needs compare columns (the columns to check for changes); \
             without them every changed row would be dropped as 'unchanged'"
                .to_string(),
        );
    }
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
    // Outer-style unnest: a NULL (or empty) array/string yields one row
    // with a NULL element rather than being silently dropped (plain
    // unnest of NULL/[] produces zero rows, losing the row's other
    // columns). Matches the xf.arr.explode behavior.
    let value_expr = if sep.is_empty() {
        // Empty separator means the column is already an array.
        format!("unnest(CASE WHEN {q} IS NULL OR length({q}) = 0 THEN [NULL] ELSE {q} END)")
    } else {
        let sep_sql = sep.replace('\'', "''");
        format!(
            "unnest(CASE WHEN {q} IS NULL THEN [NULL] ELSE string_split(CAST({q} AS VARCHAR), '{sep_sql}') END)"
        )
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
         UNPIVOT INCLUDE NULLS (val FOR colname IN (COLUMNS(* EXCLUDE _row)))) \
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
fn build_switch(
    node_id: &str,
    inputs: &NodeInputs,
    props: &JsonValue,
    consumer_count: &HashMap<String, usize>,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.switch"))?;
    // `branches` is a key-value field. The UI saves it as an ARRAY of
    // {key,value} (which also preserves branch order = case_1, case_2, ...);
    // older docs may have an object. Accept both, mirroring
    // headers_from_props. The value is the branch condition; the key is
    // just the branch label.
    let mut conds: Vec<String> = Vec::new();
    let raw = props.get("branches");
    if let Some(arr) = raw.and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(c) = item
                .get("value")
                .and_then(|x| x.as_str())
                .filter(|s| !s.trim().is_empty())
            {
                conds.push(c.to_string());
            }
        }
    } else if let Some(obj) = raw.and_then(|v| v.as_object()) {
        for (_name, val) in obj {
            if let Some(c) = val.as_str().filter(|s| !s.trim().is_empty()) {
                conds.push(c.to_string());
            }
        }
    }
    conds.truncate(3);
    if conds.is_empty() {
        return Err("Switch needs at least one branch condition".to_string());
    }
    // Each branch/default port picks VIEW vs TABLE by its OWN downstream
    // consumer count, matching the main/reject policy (audit B9): a single
    // consumer -> lazy VIEW (DuckDB inlines it, no row copy), 2+ -> TABLE.
    // A case port with ZERO consumers is skipped entirely - but its
    // condition is STILL pushed into the negation chain (`prior`), or
    // first-match-wins routing would break and later branches/default would
    // wrongly claim its rows. DUCKLE_FORCE_VIEWS forces views as elsewhere.
    let force_views = std::env::var("DUCKLE_FORCE_VIEWS")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false);
    let kw = |relation: &str| -> &'static str {
        let consumers = consumer_count.get(relation).copied().unwrap_or(0);
        if force_views || consumers <= 1 { "VIEW" } else { "TABLE" }
    };
    let up = quote_ident(upstream);
    let mut stmts: Vec<String> = Vec::new();
    let mut prior: Vec<String> = Vec::new();
    // Guard every condition with COALESCE(..., FALSE): a row whose
    // condition evaluates to NULL (e.g. comparing a NULL column) is
    // neither TRUE for its branch nor caught by the default's NOT(...)
    // chain (NOT NULL = NULL), so without this it falls through every
    // case AND the default and is silently lost. COALESCE makes NULL
    // behave as "did not match", routing the row to the default branch.
    for (i, cond) in conds.iter().enumerate() {
        let case_rel = format!("{}__case_{}", node_id, i + 1);
        let positive = format!("COALESCE(({}), FALSE)", cond);
        let where_clause = if prior.is_empty() {
            positive
        } else {
            let neg = prior
                .iter()
                .map(|p| format!("NOT COALESCE(({}), FALSE)", p))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("{} AND {}", positive, neg)
        };
        // Skip a dead (unwired) branch port, but ALWAYS extend the negation
        // chain below so first-match-wins for later branches stays correct.
        let consumers = consumer_count.get(&case_rel).copied().unwrap_or(0);
        if consumers >= 1 || force_views {
            stmts.push(format!(
                "CREATE OR REPLACE {} {} AS SELECT * FROM {} WHERE {}",
                kw(&case_rel),
                quote_ident(&case_rel),
                up,
                where_clause
            ));
        }
        prior.push(cond.clone());
    }
    // Default: rows that no branch matched (including NULL-condition rows).
    // Always emitted so the stage SQL is never empty even if every case
    // port is unwired. Lazy VIEW unless 2+ consumers.
    let default_rel = format!("{}__default", node_id);
    let default_where = prior
        .iter()
        .map(|p| format!("NOT COALESCE(({}), FALSE)", p))
        .collect::<Vec<_>>()
        .join(" AND ");
    stmts.push(format!(
        "CREATE OR REPLACE {} {} AS SELECT * FROM {} WHERE {}",
        kw(&default_rel),
        quote_ident(&default_rel),
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
    // UNION ALL BY NAME (not positional): the retained unmatched-previous
    // rows must align to `current` by column NAME. Positional UNION ALL
    // silently swaps values when the two inputs present columns in a
    // different order (audit B3, DuckDB-verified). SCD1's documented
    // precondition is that both inputs share a schema; BY NAME additionally
    // tolerates column-order differences instead of corrupting them.
    Ok(format!(
        "SELECT * FROM {cur} \
         UNION ALL BY NAME \
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
    // INCLUDE NULLS: DuckDB's UNPIVOT defaults to EXCLUDE NULLS, which
    // silently drops every row whose unpivoted value is NULL - on sparse
    // wide data that's real data loss. The SQL-standard form is the only
    // one that accepts INCLUDE NULLS (the parenthesized statement form
    // rejects it), so emit that: `... UNPIVOT INCLUDE NULLS (value FOR
    // name IN (cols))`.
    Ok(format!(
        "SELECT * FROM {} UNPIVOT INCLUDE NULLS ({} FOR {} IN ({}))",
        quote_ident(upstream),
        quote_ident(&value_col),
        quote_ident(&name_col),
        on
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
        // ROW_NUMBER() with no ORDER BY picks an arbitrary survivor per
        // duplicate group, which is non-deterministic under
        // preserve_insertion_order=false + multi-threading: the same input
        // can keep a different row run-to-run (audit B4). An optional
        // `tieBreak` prop (comma-separated columns) makes the survivor
        // deterministic. We do NOT impose a default ordering - that would
        // change which row currently survives for every existing qa.unique
        // node, and there's no safe all-column default (breaks on
        // LIST/STRUCT/MAP). Per-port row COUNTS are unchanged regardless;
        // the prop only fixes WHICH row of each group is kept.
        let order = columns_list(props, "tieBreak");
        let window = if order.is_empty() {
            format!("ROW_NUMBER() OVER (PARTITION BY {})", partition)
        } else {
            let ob = order.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
            format!("ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {})", partition, ob)
        };
        return Ok(format!(
            "SELECT * EXCLUDE (__dq_rn) FROM (SELECT *, {} AS __dq_rn FROM {}) WHERE __dq_rn {} 1",
            window, from, cmp
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
    // Optional declared `type`: when the form picks a type for the new
    // column, wrap the expression in a cast so the column actually has that
    // type. Use TRY_CAST by default (mirrors build_cast): a hard CAST aborts
    // the whole run on the first value the expression can't coerce - one bad
    // row killing the pipeline. TRY_CAST nulls the bad cell instead. The
    // onError prop opts into the strict path (onError=='fail').
    let cast_fn = match string_prop(props, "onError").as_deref() {
        Some("fail") => "CAST",
        _ => "TRY_CAST",
    };
    let typed_expr = |expr: &str, ty: Option<&str>| -> String {
        match ty.map(str::trim).filter(|s| !s.is_empty()) {
            Some(t) => format!("{}(({}) AS {})", cast_fn, expr, duckle_type_to_duckdb(t)),
            None => expr.to_string(),
        }
    };
    let mut additions: Vec<String> = Vec::new();
    for col in &columns {
        let name = col.get("name").and_then(JsonValue::as_str).unwrap_or("col");
        let expr = col
            .get("expression")
            .or_else(|| col.get("expr"))
            .and_then(JsonValue::as_str)
            .unwrap_or("NULL");
        let ty = col.get("type").and_then(JsonValue::as_str);
        additions.push(format!("{} AS {}", typed_expr(expr, ty), quote_ident(name)));
    }
    // The Add-Column / Coalesce form is single: { name, type, expression }.
    if additions.is_empty() {
        let name = string_prop(props, "name").filter(|s| !s.is_empty());
        let expr = string_prop(props, "expression").or_else(|| string_prop(props, "expr"));
        if let (Some(name), Some(expr)) = (name, expr) {
            if !expr.trim().is_empty() {
                let ty = string_prop(props, "type");
                additions.push(format!(
                    "{} AS {}",
                    typed_expr(expr.trim(), ty.as_deref()),
                    quote_ident(&name)
                ));
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
    let provided_casts = !casts.is_empty();
    let mut skipped_empty = 0_usize;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    // The Cast form's "On conversion error" control:
    //   null (default) -> TRY_CAST, bad values become NULL
    //   reject         -> TRY_CAST too (row-level rejection isn't wired
    //                     for cast yet; NULL-on-error is the safe,
    //                     non-failing approximation)
    //   fail           -> CAST, a bad value aborts the run
    // Previously this prop was ignored and we always emitted CAST, so a
    // default-configured cast of dirty data crashed the pipeline instead
    // of nulling the bad cells like the UI promised.
    let cast_fn = match string_prop(props, "onError").as_deref() {
        Some("fail") => "CAST",
        _ => "TRY_CAST",
    };
    // Use REPLACE so we keep other columns. e.g.
    //   SELECT * REPLACE (TRY_CAST(amount AS DECIMAL(10,2)) AS amount) FROM x
    let mut replacements: Vec<String> = Vec::new();
    for c in &casts {
        let column = c.get("column").and_then(JsonValue::as_str).unwrap_or("").trim();
        let target = c
            .get("targetType")
            .or_else(|| c.get("type"))
            .and_then(JsonValue::as_str)
            .unwrap_or("VARCHAR");
        if column.is_empty() {
            skipped_empty += 1;
            continue;
        }
        if !seen.insert(column.to_string()) {
            // Duplicate cast for the same column - silently letting the
            // later definition win used to surprise users who'd added
            // two casts for the same field by accident. Loud error.
            return Err(format!(
                "Cast: column '{}' appears in two cast entries; remove one",
                column
            ));
        }
        let target_sql = duckle_type_to_duckdb(target);
        replacements.push(format!(
            "{}({} AS {}) AS {}",
            cast_fn,
            quote_ident(column),
            target_sql,
            quote_ident(column)
        ));
    }
    // The Cast form is single-column: { column, targetType }.
    if replacements.is_empty() {
        if let Some(column) = string_prop(props, "column").filter(|s| !s.trim().is_empty()) {
            let column = column.trim();
            let target = string_prop(props, "targetType")
                .or_else(|| string_prop(props, "type"))
                .unwrap_or_else(|| "string".into());
            replacements.push(format!(
                "{}({} AS {}) AS {}",
                cast_fn,
                quote_ident(column),
                duckle_type_to_duckdb(&target),
                quote_ident(column)
            ));
        }
    }
    // If the user supplied cast entries but every one was empty / blank,
    // the SELECT * REPLACE clause would be empty - the cast becomes a
    // silent no-op and the user wonders why their column type didn't
    // change. Catch it loudly here.
    if replacements.is_empty() {
        if provided_casts && skipped_empty > 0 {
            return Err(format!(
                "Cast: {} cast entr{} had no column name - pick a column or remove the row",
                skipped_empty,
                if skipped_empty == 1 { "y" } else { "ies" }
            ));
        }
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT * REPLACE ({}) FROM {}",
        replacements.join(", "),
        quote_ident(upstream)
    ))
}

/// All (old, new) rename pairs a Rename node carries, across every prop
/// shape the UI / older docs use: a `renames` or `columns` array of
/// {from|source, to|target}, OR the current Rename form's `mapping`
/// array of {key=old, value=new}. Shared by build_rename, the schema
/// derivation, and validation so they never disagree about which column
/// names exist downstream (a mismatch made the validator reject the new
/// name and accept the renamed-away old one).
fn rename_pairs(props: &JsonValue) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(arr) = props
        .get("renames")
        .or_else(|| props.get("columns"))
        .and_then(JsonValue::as_array)
    {
        for r in arr {
            let from = r.get("from").or_else(|| r.get("source")).and_then(JsonValue::as_str);
            let to = r.get("to").or_else(|| r.get("target")).and_then(JsonValue::as_str);
            if let (Some(f), Some(t)) = (from, to) {
                if !f.is_empty() && !t.is_empty() {
                    out.push((f.to_string(), t.to_string()));
                }
            }
        }
    }
    // The current Rename form writes `mapping` as key-value pairs
    // (old -> new); only consulted when the array shapes are absent,
    // matching build_rename's precedence.
    if out.is_empty() {
        if let Some(arr) = props.get("mapping").and_then(JsonValue::as_array) {
            for kv in arr {
                let old = kv.get("key").and_then(JsonValue::as_str);
                let new = kv.get("value").and_then(JsonValue::as_str);
                if let (Some(o), Some(n)) = (old, new) {
                    if !o.is_empty() && !n.is_empty() {
                        out.push((o.to_string(), n.to_string()));
                    }
                }
            }
        }
    }
    out
}

fn build_rename(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let pairs = rename_pairs(props);
    let mut excludes = Vec::new();
    let mut aliases = Vec::new();
    for (from, to) in &pairs {
        excludes.push(quote_ident(from));
        aliases.push(format!(
            "{}.{} AS {}",
            quote_ident(upstream),
            quote_ident(from),
            quote_ident(to)
        ));
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

/// A configured lookup join on a Map (tMap-style) node.
struct MapLookup {
    port: String,
    view: String,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    kind: &'static str,
}

fn build_mapper(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "mapper: missing main input".to_string())?;

    // Collect the output (name, raw expression) pairs. The Map form writes
    // either `expressions` (key-value: out name -> SQL) or a structured
    // `mapper.outputs` array ({name, expression}). Both are accepted.
    let mut outputs: Vec<(String, String)> = Vec::new();
    if let Some(pairs) = props.get("expressions").and_then(JsonValue::as_array) {
        for kv in pairs {
            let name = kv.get("key").and_then(JsonValue::as_str).unwrap_or("").trim();
            let expr = kv.get("value").and_then(JsonValue::as_str).unwrap_or("").trim();
            if !name.is_empty() && !expr.is_empty() {
                outputs.push((name.to_string(), expr.to_string()));
            }
        }
    }
    if outputs.is_empty() {
        if let Some(outs) = props.get("mapper").and_then(|m| m.get("outputs")).and_then(JsonValue::as_array) {
            for o in outs {
                let name = o.get("name").and_then(JsonValue::as_str).unwrap_or("").trim();
                let expr = o
                    .get("expression")
                    .or_else(|| o.get("expr"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("")
                    .trim();
                if !name.is_empty() && !expr.is_empty() {
                    outputs.push((name.to_string(), expr.to_string()));
                }
            }
        }
    }

    // Optional output filter (WHERE), from either `filter` or `mapper.filter`.
    let filter = string_prop(props, "filter")
        .or_else(|| props.get("mapper").and_then(|m| m.get("filter")).and_then(JsonValue::as_str).map(String::from))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Parse the lookup join config: props.lookups = [{port, leftKey,
    // rightKey, joinType}]. Each port must be wired as an actual input
    // (read by exact handle name - NodeInputs::lookup(idx) does NOT map to
    // lookup_1/2/3, see plan.rs ~1776).
    let mut lookups: Vec<MapLookup> = Vec::new();
    if let Some(arr) = props.get("lookups").and_then(JsonValue::as_array) {
        for entry in arr {
            let port = entry
                .get("port")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Map: each lookup needs a 'port' (e.g. lookup_1)".to_string())?;
            let view = inputs
                .ports
                .get(port)
                .and_then(|v| v.first())
                .ok_or_else(|| format!(
                    "Map: lookup config references port '{}' but no input is wired into it",
                    port
                ))?
                .clone();
            let left_keys = parse_key_list(
                entry.get("leftKey").and_then(JsonValue::as_str).unwrap_or(""),
            );
            let right_keys = parse_key_list(
                entry.get("rightKey").and_then(JsonValue::as_str).unwrap_or(""),
            );
            if left_keys.is_empty() || right_keys.is_empty() {
                return Err(format!(
                    "Map: lookup '{}' needs leftKey and rightKey",
                    port
                ));
            }
            if left_keys.len() != right_keys.len() {
                return Err(format!(
                    "Map: lookup '{}' leftKey and rightKey must have the same number of columns (got {} vs {})",
                    port, left_keys.len(), right_keys.len()
                ));
            }
            let kind = match entry.get("joinType").and_then(JsonValue::as_str) {
                Some("inner") => "INNER",
                Some("left") | None => "LEFT",
                Some(other) => {
                    return Err(format!(
                        "Map: lookup '{}' joinType must be 'inner' or 'left' (got '{}')",
                        port, other
                    ))
                }
            };
            lookups.push(MapLookup { port: port.to_string(), view, left_keys, right_keys, kind });
        }
    }

    // Validate every lookup port referenced in an expression / filter is
    // either configured above or at least wired - otherwise the generated
    // SQL would reference an unknown relation. This replaces the old blanket
    // "Map can't join" refusal with a precise, actionable error.
    let configured: std::collections::BTreeSet<&str> =
        lookups.iter().map(|l| l.port.as_str()).collect();
    let mut referenced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (_, expr) in &outputs {
        referenced.extend(referenced_lookup_ports(expr));
    }
    if let Some(f) = &filter {
        referenced.extend(referenced_lookup_ports(f));
    }
    for port in &referenced {
        if !configured.contains(port.as_str()) {
            return Err(format!(
                "Map: an expression references lookup port '{}', but it is not configured in 'lookups' (add a lookup with join keys for it)",
                port
            ));
        }
    }

    // No lookups configured AND nothing references one: behave exactly like
    // the original single-input mapper (strip the `main.` prefix off
    // expressions). Preserves prior behavior + tests.
    if lookups.is_empty() {
        if outputs.is_empty() {
            return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
        }
        let terms: Vec<String> = outputs
            .iter()
            .map(|(name, expr)| format!("{} AS {}", strip_port_prefixes(expr), quote_ident(name)))
            .collect();
        let mut sql = format!("SELECT {} FROM {}", terms.join(", "), quote_ident(upstream));
        if let Some(predicate) = &filter {
            sql.push_str(" WHERE ");
            sql.push_str(&strip_port_prefixes(predicate));
        }
        return Ok(sql);
    }

    // Join path. Alias each input by its (unique) view name, quoted.
    // main -> "<upstream>", lookup_1 -> "<view1>", etc.
    let mut aliases: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    aliases.insert("main".to_string(), quote_ident(upstream));
    for l in &lookups {
        aliases.insert(l.port.clone(), quote_ident(&l.view));
    }

    if outputs.is_empty() {
        return Err("Map: define at least one output expression when using lookups".to_string());
    }
    let terms: Vec<String> = outputs
        .iter()
        .map(|(name, expr)| format!("{} AS {}", qualify_port_refs(expr, &aliases), quote_ident(name)))
        .collect();

    // FROM main JOIN lookup_1 ON main.k = lookup_1.k [AND ...] JOIN ...
    // Left keys qualify against main; right keys against the lookup view.
    let main_alias = aliases.get("main").cloned().unwrap_or_else(|| quote_ident(upstream));
    let mut from = quote_ident(upstream);
    for l in &lookups {
        let look_alias = aliases.get(&l.port).cloned().unwrap_or_else(|| quote_ident(&l.view));
        let on = l
            .left_keys
            .iter()
            .zip(l.right_keys.iter())
            .map(|(lk, rk)| {
                format!("{}.{} = {}.{}", main_alias, quote_ident(lk), look_alias, quote_ident(rk))
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        from.push_str(&format!(" {} JOIN {} ON {}", l.kind, look_alias, on));
    }

    let mut sql = format!("SELECT {} FROM {}", terms.join(", "), from);
    if let Some(predicate) = &filter {
        sql.push_str(" WHERE ");
        sql.push_str(&qualify_port_refs(predicate, &aliases));
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

/// Collect the set of `lookup_N` port names an expression references
/// (e.g. `lookup_1.name + lookup_2.code` -> {lookup_1, lookup_2}). Used to
/// validate that every referenced lookup is actually configured/wired.
/// String literals are skipped so `'lookup_9.x'` inside a quoted string is
/// not treated as a reference.
fn referenced_lookup_ports(expr: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let bytes = expr.as_bytes();
    let mut i = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if c == '\'' {
                // '' is an escaped quote, stays in the string.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            in_str = true;
            i += 1;
            continue;
        }
        // Start of an identifier (not preceded by an identifier char, so we
        // don't match the tail of `my_lookup_1`).
        let prev_ident = i > 0 && {
            let p = bytes[i - 1] as char;
            p.is_alphanumeric() || p == '_'
        };
        if !prev_ident && (c.is_ascii_alphabetic() || c == '_') {
            let start = i;
            while i < bytes.len() {
                let ch = bytes[i] as char;
                if ch.is_alphanumeric() || ch == '_' {
                    i += 1;
                } else {
                    break;
                }
            }
            let ident = &expr[start..i];
            if ident.starts_with("lookup") && i < bytes.len() && bytes[i] == b'.' {
                out.insert(ident.to_string());
            }
            continue;
        }
        i += 1;
    }
    out
}

/// Rewrite `main.col` / `lookup_N.col` references in an expression to
/// quoted, aliased column references (e.g. `"orders"."id"`), using the
/// alias map (port -> already-quoted view name). String literals are left
/// untouched, so an expression like `'http://main.x'` is not corrupted -
/// this is the key difference from strip_port_prefixes, which is not
/// string-aware and is only safe on the no-lookup single-input path.
fn qualify_port_refs(
    expr: &str,
    aliases: &std::collections::BTreeMap<String, String>,
) -> String {
    let bytes = expr.as_bytes();
    let mut out = String::with_capacity(expr.len() + 16);
    let mut i = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            out.push(c);
            if c == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            in_str = true;
            out.push(c);
            i += 1;
            continue;
        }
        let prev_ident = i > 0 && {
            let p = bytes[i - 1] as char;
            p.is_alphanumeric() || p == '_'
        };
        if !prev_ident && (c.is_ascii_alphabetic() || c == '_') {
            let start = i;
            while i < bytes.len() {
                let ch = bytes[i] as char;
                if ch.is_alphanumeric() || ch == '_' {
                    i += 1;
                } else {
                    break;
                }
            }
            let ident = &expr[start..i];
            // A `<port>.<col>` reference: rewrite to alias + quoted column.
            if i < bytes.len() && bytes[i] == b'.' {
                if let Some(alias) = aliases.get(ident) {
                    // Consume the dot + the following column identifier.
                    let mut j = i + 1;
                    let col_start = j;
                    while j < bytes.len() {
                        let ch = bytes[j] as char;
                        if ch.is_alphanumeric() || ch == '_' {
                            j += 1;
                        } else {
                            break;
                        }
                    }
                    if j > col_start {
                        let col = &expr[col_start..j];
                        out.push_str(alias);
                        out.push('.');
                        out.push_str(&quote_ident(col));
                        i = j;
                        continue;
                    }
                }
            }
            out.push_str(ident);
            continue;
        }
        out.push(c);
        i += 1;
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

/// Parse a key string into a list of column names. Accepts a single
/// column (`"id"`) or comma-separated composite keys (`"customer_id,
/// order_date"`). Whitespace around commas is stripped.
fn parse_key_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn build_join(inputs: &NodeInputs, props: &JsonValue, kind: &str) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "join: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "join: missing lookup input".to_string())?;
    let left_keys = parse_key_list(
        props
            .get("leftKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "join: leftKey property required".to_string())?,
    );
    let right_keys = parse_key_list(
        props
            .get("rightKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "join: rightKey property required".to_string())?,
    );
    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("join: leftKey and rightKey cannot be empty".into());
    }
    if left_keys.len() != right_keys.len() {
        return Err(format!(
            "join: leftKey and rightKey must have the same number of columns (got {} vs {})",
            left_keys.len(),
            right_keys.len()
        ));
    }
    // The form's joinType, if set, overrides the component-id default so
    // changing it in the UI actually takes effect.
    let kind = match string_prop(props, "joinType").as_deref() {
        Some("inner") => "INNER",
        Some("left") => "LEFT",
        Some("right") => "RIGHT",
        Some("full") | Some("outer") => "FULL OUTER",
        _ => kind,
    };
    // Two-shaped output:
    // - If the keys have the same names on both sides (common with
    //   well-modeled data), USING(...) gives a clean single copy of
    //   the join columns - no "ambiguous reference" downstream.
    // - If the names differ, ON + EXCLUDE the right-side keys still
    //   dedupes the join columns. Other shared columns (e.g., both
    //   tables have `created_at`) still need the user to project
    //   them via xf.rename or xf.project upstream, but at minimum
    //   the join keys themselves no longer collide.
    let same_names = left_keys == right_keys;
    if same_names {
        let key_list = left_keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT * FROM {l} m {k} JOIN {r} r USING ({keys})",
            l = quote_ident(left),
            k = kind,
            r = quote_ident(right),
            keys = key_list
        ))
    } else {
        let on_clause = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(l, r)| format!("m.{} = r.{}", quote_ident(l), quote_ident(r)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // Project each key as COALESCE(left, right) under the left key
        // name, and EXCLUDE the key columns from BOTH sides. The previous
        // `m.*, r.* EXCLUDE(right_keys)` kept the LEFT key column and
        // dropped the right one - fine for INNER/LEFT, but for RIGHT/FULL
        // a right-only row has m.* all NULL, so the join key showed up as
        // NULL even though the right side had a value (data corruption +
        // the key effectively lost). COALESCE recovers the key value from
        // whichever side is present.
        let coalesced = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(l, r)| {
                format!(
                    "COALESCE(m.{}, r.{}) AS {}",
                    quote_ident(l),
                    quote_ident(r),
                    quote_ident(l)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let left_excl = left_keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        let right_excl = right_keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT {coalesced}, m.* EXCLUDE ({lexcl}), r.* EXCLUDE ({rexcl}) FROM {l} m {k} JOIN {r} r ON {on}",
            coalesced = coalesced,
            lexcl = left_excl,
            rexcl = right_excl,
            l = quote_ident(left),
            k = kind,
            r = quote_ident(right),
            on = on_clause
        ))
    }
}

fn build_semi(inputs: &NodeInputs, props: &JsonValue, anti: bool) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "semi: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "semi: missing lookup input".to_string())?;
    let left_keys = parse_key_list(
        props
            .get("leftKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "semi: leftKey required".to_string())?,
    );
    let right_keys = parse_key_list(
        props
            .get("rightKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "semi: rightKey required".to_string())?,
    );
    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("semi: keys cannot be empty".into());
    }
    if left_keys.len() != right_keys.len() {
        return Err(format!(
            "semi: leftKey and rightKey must have the same number of columns (got {} vs {})",
            left_keys.len(),
            right_keys.len()
        ));
    }
    // EXISTS / NOT EXISTS replaces IN / NOT IN to fix the classic SQL
    // NULL gotcha: `x NOT IN (subquery)` returns UNKNOWN (treated as
    // false) the moment the subquery yields a single NULL, which makes
    // anti-join silently drop every row. EXISTS evaluates the subquery
    // as a correlated boolean - NULL right-side keys simply don't
    // match and don't break the predicate. Composite keys ride the
    // same construction.
    let prefix = if anti { "NOT " } else { "" };
    let correlated = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| format!("m.{} = r.{}", quote_ident(l), quote_ident(r)))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "SELECT * FROM {l} m WHERE {pre}EXISTS (SELECT 1 FROM {r} r WHERE {on})",
        l = quote_ident(left),
        pre = prefix,
        r = quote_ident(right),
        on = correlated
    ))
}

// ---- Sources ------------------------------------------------------------

fn build_csv_source(props: &JsonValue, declared: Option<&[duckle_metadata::Column]>) -> String {
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
    // Explicit date / timestamp parsing format. DuckDB's strptime tokens
    // (%d, %m, %Y, etc.) - the most common pain point is dd/mm/yyyy which
    // DuckDB otherwise mis-detects as mm/dd/yyyy. Setting this keeps the
    // column as a proper DATE / TIMESTAMP instead of forcing VARCHAR via
    // the Schema panel (which is the other workaround we added for #3).
    if let Some(df) = string_prop(props, "dateFormat").filter(|s| !s.is_empty()) {
        args.push(format!("dateformat='{}'", sql_escape(&df)));
    }
    if let Some(tf) = string_prop(props, "timestampFormat").filter(|s| !s.is_empty()) {
        args.push(format!("timestampformat='{}'", sql_escape(&tf)));
    }
    // If the user declared a schema (Schema panel in PropertiesPanel),
    // honor it via DuckDB's `types` argument, which overrides the inferred
    // type for the NAMED columns and auto-detects the rest. This is how a
    // user forces a `dd/mm/yy` date column to stay as VARCHAR instead of
    // being misparsed as `yyyy-mm-dd`. See issue #3.
    //
    // `types` (name-match), NOT `columns` (positional full-schema):
    // `columns` requires the declaration to list EVERY column in the file,
    // so a PARTIAL Schema-panel declaration (the common case - declare only
    // the few columns you care about) hard-failed with a cryptic sniffer
    // "Schema mismatch ... expected N columns" error. `types` accepts a
    // partial map, binds by NAME, and errors only when a declared name is
    // genuinely absent from the file (the correct, loud failure).
    // DuckDB 1.5.3 verified: types={'amt':'VARCHAR'} over a 3-col CSV keeps
    // id=BIGINT (auto) + amt=VARCHAR (forced); a bogus name errors clearly.
    //
    // Per-column multi-format workaround (issue #10): DuckDB has only a
    // single global `dateformat`/`timestampformat`, so to parse several
    // DATE/TIMESTAMP columns each with its OWN format on one read, force
    // those columns to VARCHAR in `types=` (raw text) and re-parse each via
    // try_strptime in a `SELECT * REPLACE (...)` wrap. try_strptime yields
    // NULL (not an error) on a value the format can't parse.
    if let Some(cols) = declared.filter(|c| !c.is_empty()) {
        use duckle_metadata::DataType;
        let mut pairs = Vec::with_capacity(cols.len());
        let mut replaces = Vec::new();
        for c in cols {
            let fmt = c.format.as_deref().filter(|s| !s.is_empty());
            let datey = matches!(c.data_type, DataType::Date | DataType::Timestamp);
            match (fmt, datey) {
                (Some(fmt), true) => {
                    // Read raw, re-parse with the column's own format.
                    pairs.push(format!("'{}': 'VARCHAR'", sql_escape(&c.name)));
                    let ident = quote_ident(&c.name);
                    let cast = match c.data_type {
                        DataType::Date => "DATE",
                        _ => "TIMESTAMP",
                    };
                    replaces.push(format!(
                        "try_strptime({id}, '{f}')::{cast} AS {id}",
                        id = ident,
                        f = sql_escape(fmt),
                        cast = cast
                    ));
                }
                _ => pairs.push(format!(
                    "'{}': '{}'",
                    sql_escape(&c.name),
                    data_type_to_duckdb_sql(&c.data_type)
                )),
            }
        }
        args.push(format!("types = {{{}}}", pairs.join(", ")));
        if !replaces.is_empty() {
            return format!(
                "SELECT * REPLACE ({}) FROM read_csv_auto({})",
                replaces.join(", "),
                args.join(", ")
            );
        }
    }
    format!("SELECT * FROM read_csv_auto({})", args.join(", "))
}

/// Map Duckle's DataType enum to a DuckDB SQL type string suitable for
/// read_csv_auto's `columns = {...}` argument. "string" -> VARCHAR is
/// the key one here: it stops DuckDB from trying (and usually failing)
/// to auto-parse dd/mm/yy and other non-ISO date formats.
fn data_type_to_duckdb_sql(t: &duckle_metadata::DataType) -> &'static str {
    use duckle_metadata::DataType as D;
    match t {
        D::String => "VARCHAR",
        D::Int32 => "INTEGER",
        D::Int64 => "BIGINT",
        D::Float32 => "FLOAT",
        D::Float64 => "DOUBLE",
        D::Bool => "BOOLEAN",
        D::Date => "DATE",
        D::Timestamp => "TIMESTAMP",
        D::Time => "TIME",
        D::Decimal => "DECIMAL",
        D::Json => "JSON",
        D::Binary => "BLOB",
    }
}

fn build_tsv_source(props: &JsonValue, declared: Option<&[duckle_metadata::Column]>) -> String {
    // TSV is just CSV with delim='\t'. Force it.
    let mut p = props.clone();
    if let Some(obj) = p.as_object_mut() {
        obj.insert(
            "delimiter".into(),
            JsonValue::String("\t".into()),
        );
    }
    build_csv_source(&p, declared)
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
        "src.postgres" | "src.cockroach" | "src.pgvector" | "src.redshift" => {
            // Redshift speaks the Postgres wire protocol with a different
            // default port (5439). The DuckDB postgres extension is happy
            // pointed at any pg-compatible endpoint.
            let default_port = if component_id == "src.redshift" { 5439 } else { 5432 };
            return db_attach(props, "postgres", default_port, true);
        }
        "snk.postgres" | "snk.cockroach" | "snk.pgvector" | "snk.redshift" => {
            let default_port = if component_id == "snk.redshift" { 5439 } else { 5432 };
            return db_attach(props, "postgres", default_port, false);
        }
        "src.mysql" | "src.mariadb" => return db_attach(props, "mysql", 3306, true),
        "snk.mysql" | "snk.mariadb" => return db_attach(props, "mysql", 3306, false),
        "src.motherduck" => return md_attach(props, true),
        "snk.motherduck" => return md_attach(props, false),
        "src.quack" => return quack_attach(props, true),
        "snk.quack" => return quack_attach(props, false),
        "src.ducklake" => return ducklake_attach(props, true),
        "snk.ducklake" => return ducklake_attach(props, false),
        // BigQuery via the duckdb-bigquery community extension. The
        // user's prop 'project' becomes the BigQuery project ID; the
        // ATTACH alias is the standard duckle_src / duckle_dst.
        "src.bigquery" => return bigquery_attach(props, true),
        "snk.bigquery" => return bigquery_attach(props, false),
        // snk.excel COPYs through the DuckDB excel extension; LOAD is
        // enough since the install paths pre-fetched it.
        "snk.excel" => return "LOAD excel; ".into(),
        // Extensions are pre-installed (desktop: the first-launch
        // installer; CI: a dedicated pre-install step). Each fresh
        // DuckDB process still needs LOAD. Concurrent INSTALL would
        // race on the cached extension file and intermittently fail.
        "src.avro" => return "LOAD avro; ".into(),
        "src.excel" => return "LOAD excel; ".into(),
        "src.iceberg" | "snk.iceberg" => return "LOAD iceberg; ".into(),
        "src.delta" => return "LOAD delta; ".into(),
        // Vector Similarity Search uses the vss extension's array_*
        // distance functions; LOAD before the SELECT runs.
        "xf.ai.vector_search" => return "LOAD vss; ".into(),
        // Full-Text Search uses the fts extension's match_bm25.
        "xf.ai.text_search" => return "LOAD fts; ".into(),
        // Spatial is GDAL-backed and ~50 MB; deliberately kept out of
        // the first-launch DUCKDB_EXTENSIONS pre-fetch so the install
        // stays small. INSTALL runs lazily on first use, then LOAD on
        // every subsequent run.
        "src.spatial"
        | "snk.spatial"
        | "xf.geo.distance"
        | "xf.geo.buffer"
        | "xf.geo.intersects"
        | "xf.join.spatial" => {
            return "INSTALL spatial; LOAD spatial; ".into();
        }
        // inet is a small built-in extension. INSTALL is a no-op once
        // the extension is bundled, but keeping it explicit means a
        // fresh CLI cache still works without the first-launch fetch.
        "xf.ip.parse" => return "INSTALL inet; LOAD inet; ".into(),
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
        || component_id.ends_with(".pgvector")
        || component_id.ends_with(".redshift")
    {
        Some("public")
    } else if component_id.ends_with(".motherduck") || component_id.ends_with(".ducklake") {
        Some("main")
    } else if component_id.ends_with(".bigquery") {
        // BigQuery's first level is a "dataset" - same shape as schema.
        // Caller can supply dataset via either prop name; we leave the
        // default empty so the ATTACH-time default dataset takes over
        // when unqualified.
        None
    } else {
        None // MySQL / MariaDB: skip the schema layer unless given
    };
    match (schema, default_schema) {
        (Some(s), _) => format!("{}.{}.{}", alias, quote_ident(s), quote_ident(table)),
        (None, Some(d)) => format!("{}.{}.{}", alias, quote_ident(d), quote_ident(table)),
        (None, None) => format!("{}.{}", alias, quote_ident(table)),
    }
}

/// DuckLake ATTACH. DuckLake is DuckDB's own lakehouse format (a
/// catalog stored in a DuckDB file or Postgres pointing at parquet
/// data files). The form's `path` is the catalog path.
fn ducklake_attach(props: &JsonValue, read_only: bool) -> String {
    let path = match string_prop(props, "path").filter(|s| !s.is_empty()) {
        Some(p) => p,
        None => return String::new(),
    };
    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };
    format!(
        "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:{}' AS {}{}; ",
        sql_escape(&path),
        alias,
        mode
    )
}

/// MotherDuck ATTACH. MotherDuck support is built into DuckDB itself
/// (no extension to install), so this just builds an `md:` URL with
/// an optional inline `motherduck_token` query parameter. If the token
/// isn't in the form, MotherDuck falls back to the MOTHERDUCK_TOKEN env
/// var, which lets a user keep credentials out of saved pipelines.
/// BigQuery via the duckdb-bigquery community extension. ATTACHes a
/// project by ID; auth uses the standard GCP credential discovery
/// (GOOGLE_APPLICATION_CREDENTIALS env var, gcloud default, etc).
/// User points the extension at a project via the 'project' prop;
/// optional 'dataset' fills in the default dataset for unqualified
/// table names.
fn bigquery_attach(props: &JsonValue, read_only: bool) -> String {
    let project = match string_prop(props, "project").filter(|s| !s.is_empty()) {
        Some(p) => p,
        None => return String::new(),
    };
    let dataset = string_prop(props, "dataset").filter(|s| !s.is_empty());
    let attach_target = match dataset {
        Some(d) => format!("project={} dataset={}", project, d),
        None => format!("project={}", project),
    };
    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };
    // INSTALL/LOAD the community extension. The community: tag tells
    // DuckDB to fetch from the community-extensions repo.
    format!(
        "INSTALL bigquery FROM community; LOAD bigquery; ATTACH '{}' AS {} (TYPE bigquery{}); ",
        attach_target, alias, mode
    )
}

fn md_attach(props: &JsonValue, read_only: bool) -> String {
    let db = match string_prop(props, "database").filter(|s| !s.is_empty()) {
        Some(d) => d,
        None => return String::new(),
    };
    let token = string_prop(props, "token").filter(|s| !s.is_empty());
    let url = match token {
        Some(t) => format!("md:{}?motherduck_token={}", db, t),
        None => format!("md:{}", db),
    };
    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };
    format!("ATTACH '{}' AS {}{}; ", sql_escape(&url), alias, mode)
}

/// Quack remote protocol (DuckDB 2.0+, May 2026). The remote DuckDB
/// instance runs `quack_serve(...)` on port 9494 by default and exposes
/// its database to multiple concurrent clients over HTTP using a
/// custom `application/duckdb` MIME type. Client side: a SECRET
/// carries the auth token, then ATTACH names the URL.
///
/// Requires DuckDB built with quack support; older builds will surface
/// a clear error at runtime ("Unknown ATTACH option 'TYPE'" or
/// similar) without any Duckle-side breakage.
fn quack_attach(props: &JsonValue, read_only: bool) -> String {
    let host = match string_prop(props, "host").filter(|s| !s.is_empty()) {
        Some(h) => h,
        None => return String::new(),
    };
    let port = props
        .get("port")
        .and_then(|v| v.as_u64())
        .filter(|p| *p > 0)
        .unwrap_or(9494);
    let token = string_prop(props, "token").filter(|s| !s.is_empty());

    // If the host already carries an explicit :port, respect it; otherwise
    // append the default 9494.
    let url = if host.contains(':') && !host.starts_with('[') {
        format!("quack:{}", host)
    } else {
        format!("quack:{}:{}", host, port)
    };

    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };

    let secret = match token {
        Some(t) => format!(
            "CREATE OR REPLACE SECRET duckle_quack_secret (TYPE QUACK, TOKEN '{}'); ",
            sql_escape(&t)
        ),
        None => String::new(),
    };

    format!("{}ATTACH '{}' AS {}{}; ", secret, sql_escape(&url), alias, mode)
}

/// Excel sink: COPY ... TO '<path>' (FORMAT 'xlsx'). The form's
/// `hasHeader` toggle becomes HEADER true/false. v1.2+ ships native
/// xlsx writer in the excel extension.
fn build_excel_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let header = props
        .get("hasHeader")
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT 'xlsx', HEADER {})",
        quote_ident(from_view),
        sql_escape(&path),
        header
    )
}

/// Iceberg sink: COPY ... TO '<path>' (FORMAT 'iceberg'). DuckDB
/// v1.5+ writes a full Iceberg table (data/ + metadata/) at the
/// given path. Read-back via src.iceberg.
fn build_iceberg_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT 'iceberg')",
        quote_ident(from_view),
        sql_escape(&path)
    )
}

/// Geospatial sink via the spatial extension's GDAL writer. The form's
/// `driver` picks the OGR driver (GeoJSON / GeoPackage / Shapefile /
/// KML / GPX). Most drivers expect a geometry column called `geom`.
fn build_spatial_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let driver = string_prop(props, "driver")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "GeoJSON".into());
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT GDAL, DRIVER '{}')",
        quote_ident(from_view),
        sql_escape(&path),
        sql_escape(&driver)
    )
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

/// Validate the text-search form and produce the spec the executor
/// uses to run the two CLI calls (stage table -> index + final query).
fn build_text_search_spec(node_id: &str, inputs: &NodeInputs, props: &JsonValue) -> Result<TextSearchSpec, String> {
    let upstream = inputs
        .main()
        .ok_or_else(|| missing_input_msg("xf.ai.text_search"))?;
    let id_col = string_prop(props, "idColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Search needs an id column (unique per row)".to_string())?;
    let text_cols = columns_list(props, "textColumns");
    if text_cols.is_empty() {
        return Err("Text Search needs at least one text column to index".to_string());
    }
    let query = string_prop(props, "query")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Search needs a query string".to_string())?;
    let top_k = props
        .get("topK")
        .and_then(|v| v.as_u64())
        .filter(|k| *k > 0);
    let output_col = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "score".into());
    let suffix: String = node_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let staging_table = format!("_fts_{}", suffix);
    Ok(TextSearchSpec {
        from_view: upstream.to_string(),
        id_col,
        text_cols,
        query,
        top_k,
        output_col,
        staging_table,
    })
}

/// Spatial Distance: add a column with the distance from each row's
/// geometry to a fixed target point (WKT). Uses the spatial extension's
/// ST_Distance over CAST geometries. Units come from the SRS of the
/// input geometry (degrees for plain WGS84, metres for projected SRS).
fn build_geo_distance(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.distance"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Distance needs a geometry column".to_string())?;
    let target = string_prop(props, "targetWkt")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Geo Distance needs a target geometry (WKT, e.g. 'POINT(0 0)')".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "distance".into());
    Ok(format!(
        "SELECT *, ST_Distance(CAST({col} AS GEOMETRY), ST_GeomFromText('{target}')) AS {out} FROM {up}",
        col = quote_ident(&column),
        target = target.replace('\'', "''"),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Spatial Buffer: add a column with ST_Buffer(geom, distance) - the
/// area within `distance` of each row's geometry.
fn build_geo_buffer(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.buffer"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Buffer needs a geometry column".to_string())?;
    let distance = props
        .get("distance")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Geo Buffer needs a distance".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "buffer".into());
    Ok(format!(
        "SELECT *, ST_Buffer(CAST({col} AS GEOMETRY), {distance}) AS {out} FROM {up}",
        col = quote_ident(&column),
        distance = distance,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Base64: encode a column to base64 text, or decode a base64 text
/// column back to bytes (returned as VARCHAR for downstream
/// compatibility - the actual underlying type is BLOB).
fn build_base64(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.base64"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Base64 needs a column".to_string())?;
    let mode = string_prop(props, "mode").unwrap_or_else(|| "encode".into());
    let qcol = quote_ident(&column);
    // Use encode()/decode() for the VARCHAR<->BLOB bridge, NOT CAST. CAST
    // VARCHAR->BLOB hard-errors on any non-ASCII byte ("Invalid byte ... All
    // non-ascii characters must be escaped"), crashing the whole run; and
    // CAST BLOB->VARCHAR hex-escapes non-ASCII bytes ("caf\xC3\xA9"),
    // silently corrupting decoded UTF-8. encode() does a clean UTF-8
    // VARCHAR->BLOB and decode() a clean BLOB->VARCHAR.
    let expr = if mode == "decode" {
        format!("decode(from_base64(CAST({} AS VARCHAR)))", qcol)
    } else {
        format!("base64(encode({}))", qcol)
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, mode));
    Ok(format!(
        "SELECT *, {expr} AS {out} FROM {up}",
        expr = expr,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Z-Score: per-row standardized value computed against the whole
/// input via window aggregates. (value - mean) / stddev_samp. Useful
/// for outlier detection and feature scaling. Single SQL pass; no
/// extra stage. If stddev is 0 (all values equal), the result is NULL
/// rather than divide-by-zero.
fn build_zscore(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.zscore"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Z-Score needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_zscore", column));
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT *, CASE WHEN stddev_samp(CAST({col} AS DOUBLE)) OVER () = 0 THEN NULL ELSE (CAST({col} AS DOUBLE) - avg(CAST({col} AS DOUBLE)) OVER ()) / stddev_samp(CAST({col} AS DOUBLE)) OVER () END AS {out} FROM {up}",
        col = qcol,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Literal Replace: DuckDB replace(string, search, replacement).
/// Different from xf.regex - this is a literal substring swap, no
/// regex metacharacters.
fn build_text_replace(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.replace"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Replace needs a column".to_string())?;
    let search = string_prop(props, "search")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Replace needs a search string".to_string())?;
    let replacement = string_prop(props, "replacement").unwrap_or_default();
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| column.clone());
    let qcol = quote_ident(&column);
    let expr = format!(
        "replace(CAST({} AS VARCHAR), '{}', '{}')",
        qcol,
        sql_escape(&search),
        sql_escape(&replacement)
    );
    if output == column {
        Ok(format!(
            "SELECT * REPLACE ({} AS {}) FROM {}",
            expr,
            qcol,
            quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {} AS {} FROM {}",
            expr,
            quote_ident(&output),
            quote_ident(upstream)
        ))
    }
}

/// URL Slug: lowercase + strip non-alphanumerics + collapse runs of
/// whitespace into single hyphens + trim leading/trailing hyphens.
/// "Hello, World!" -> "hello-world".
fn build_text_slug(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.slug"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Slug needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_slug", column));
    let qcol = quote_ident(&column);
    // Lower, replace any run of non-alphanumerics with a single hyphen,
    // then trim leading/trailing hyphens.
    let expr = format!(
        "trim(regexp_replace(lower(CAST({} AS VARCHAR)), '[^a-z0-9]+', '-', 'g'), '-')",
        qcol
    );
    Ok(format!(
        "SELECT *, {} AS {} FROM {}",
        expr,
        quote_ident(&output),
        quote_ident(upstream)
    ))
}

/// Strip HTML: remove all <...> tag spans via regex. Leaves the text
/// content. Standard newsletter / scrape-cleanup helper.
fn build_text_strip_html(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.strip_html"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Strip HTML needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| column.clone());
    let qcol = quote_ident(&column);
    let expr = format!(
        "regexp_replace(CAST({} AS VARCHAR), '<[^>]+>', '', 'g')",
        qcol
    );
    if output == column {
        Ok(format!(
            "SELECT * REPLACE ({} AS {}) FROM {}",
            expr,
            qcol,
            quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {} AS {} FROM {}",
            expr,
            quote_ident(&output),
            quote_ident(upstream)
        ))
    }
}

/// Text Reverse: reverse the characters in a string column.
/// DuckDB reverse() function.
fn build_text_reverse(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.reverse"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Reverse needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_reversed", column));
    Ok(format!(
        "SELECT *, reverse(CAST({col} AS VARCHAR)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Text Repeat: repeat a string column N times via DuckDB repeat().
fn build_text_repeat(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.repeat"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Repeat needs a column".to_string())?;
    let count = props
        .get("count")
        .and_then(|v| v.as_i64())
        .filter(|n| *n >= 0)
        .unwrap_or(2);
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_repeated", column));
    Ok(format!(
        "SELECT *, repeat(CAST({col} AS VARCHAR), {n}) AS {out} FROM {up}",
        col = quote_ident(&column),
        n = count,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Compare: produce a boolean column from a comparison of two
/// upstream columns. op = eq / neq / lt / le / gt / ge. Useful for
/// flagging mismatches between expected/actual columns.
fn build_compare(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.compare"))?;
    let left = string_prop(props, "leftColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Compare needs a left column".to_string())?;
    let right = string_prop(props, "rightColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Compare needs a right column".to_string())?;
    let op = string_prop(props, "op").unwrap_or_else(|| "eq".into());
    let sql_op = match op.as_str() {
        "neq" => "!=",
        "lt" => "<",
        "le" => "<=",
        "gt" => ">",
        "ge" => ">=",
        _ => "=",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}_{}", left, op, right));
    Ok(format!(
        "SELECT *, ({} {} {}) AS {} FROM {}",
        quote_ident(&left),
        sql_op,
        quote_ident(&right),
        quote_ident(&output),
        quote_ident(upstream)
    ))
}

/// Text Match: boolean substring / prefix / suffix predicate via
/// DuckDB's contains / starts_with / ends_with. Adds a boolean
/// column - pair with Filter Rows downstream to keep only matches.
fn build_text_match(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.match"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Match needs a column".to_string())?;
    let needle = string_prop(props, "needle")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Match needs a search term".to_string())?;
    let mode = string_prop(props, "mode").unwrap_or_else(|| "contains".into());
    let fn_name = match mode.as_str() {
        "starts_with" => "starts_with",
        "ends_with" => "ends_with",
        _ => "contains",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, mode));
    Ok(format!(
        "SELECT *, {fn}(CAST({col} AS VARCHAR), '{n}') AS {out} FROM {up}",
        fn = fn_name,
        col = quote_ident(&column),
        n = sql_escape(&needle),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Sign: -1 for negative, 0 for zero, +1 for positive. DuckDB's
/// sign() function on a DOUBLE input.
fn build_sign(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.sign"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Sign needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_sign", column));
    Ok(format!(
        "SELECT *, sign(CAST({col} AS DOUBLE)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Clamp: clip numeric values to a [low, high] range via LEAST +
/// GREATEST. Values below low become low; above high become high.
/// Useful for capping outliers before downstream stats.
fn build_clamp(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.clamp"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Clamp needs a column".to_string())?;
    let low = props
        .get("low")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Clamp needs a low bound".to_string())?;
    let high = props
        .get("high")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Clamp needs a high bound".to_string())?;
    if high < low {
        return Err("Clamp needs high >= low".to_string());
    }
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (LEAST(GREATEST(CAST({col} AS DOUBLE), {low}), {high}) AS {col}) FROM {up}",
        col = qcol,
        low = low,
        high = high,
        up = quote_ident(upstream)
    ))
}

/// String Padding: pad a string column to a fixed length on the left
/// or right with a fill character. Default fills with space, mode
/// 'left' (lpad) is the classic 'zero-pad numeric IDs' pattern.
fn build_padding(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.padding"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Padding needs a column".to_string())?;
    let length = props
        .get("length")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .ok_or_else(|| "Padding needs a positive target length".to_string())?;
    let fill = string_prop(props, "fill")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| " ".into());
    let side = string_prop(props, "side").unwrap_or_else(|| "left".into());
    let fn_name = if side == "right" { "rpad" } else { "lpad" };
    let qcol = quote_ident(&column);
    let fill_escaped = sql_escape(&fill);
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| column.clone());
    if output == column {
        Ok(format!(
            "SELECT * REPLACE ({fn}(CAST({col} AS VARCHAR), {n}, '{f}') AS {col}) FROM {up}",
            fn = fn_name,
            col = qcol,
            n = length,
            f = fill_escaped,
            up = quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {fn}(CAST({col} AS VARCHAR), {n}, '{f}') AS {out} FROM {up}",
            fn = fn_name,
            col = qcol,
            n = length,
            f = fill_escaped,
            out = quote_ident(&output),
            up = quote_ident(upstream)
        ))
    }
}

/// Date/Time Epoch: convert a TIMESTAMP column to Unix epoch seconds
/// (mode 'to') or epoch seconds back to TIMESTAMP (mode 'from').
/// Both directions use DuckDB core functions, no extension needed.
fn build_dt_epoch(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.epoch"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Epoch needs a column".to_string())?;
    let mode = string_prop(props, "mode").unwrap_or_else(|| "to".into());
    let qcol = quote_ident(&column);
    let expr = if mode == "from" {
        // Stay in pure TIMESTAMP space - to_timestamp() returns
        // TIMESTAMPTZ which round-trips wrong on non-UTC sessions.
        format!(
            "(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL '1 second' * CAST({} AS BIGINT))",
            qcol
        )
    } else {
        format!("epoch(CAST({} AS TIMESTAMP))", qcol)
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if mode == "from" {
                format!("{}_timestamp", column)
            } else {
                format!("{}_epoch", column)
            }
        });
    Ok(format!(
        "SELECT *, {expr} AS {out} FROM {up}",
        expr = expr,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Current Timestamp: add a column holding the time at which the
/// pipeline runs - the standard 'loaded_at' / 'processed_at' /
/// 'ingested_at' stamp every ETL output usually carries. Cast to
/// plain TIMESTAMP - current_timestamp returns TIMESTAMPTZ which
/// serializes with a session-timezone offset and confuses
/// downstream readers.
fn build_dt_now(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.now"))?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "loaded_at".into());
    Ok(format!(
        "SELECT *, CAST(current_timestamp AS TIMESTAMP) AS {out} FROM {up}",
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// UUID: add a freshly-generated UUID v4 to every row. Standard
/// 'surrogate row id' pattern, especially handy before upserts into
/// systems that need a non-business primary key.
fn build_uuid(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.uuid"))?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "row_id".into());
    Ok(format!(
        "SELECT *, uuid() AS {out} FROM {up}",
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Cumulative: running aggregate over an ordered window
/// (sum / avg / count / min / max), optionally per-group. Classic
/// reporting pattern - 'running total of sales', 'cumulative count
/// of users per region'. Uses the standard ROWS BETWEEN UNBOUNDED
/// PRECEDING AND CURRENT ROW frame so the value at each row reflects
/// everything seen so far in scan order.
fn build_cumulative(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.cumulative"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Cumulative needs a column".to_string())?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Cumulative needs an orderBy column".to_string())?;
    let func = string_prop(props, "function").unwrap_or_else(|| "sum".into()).to_lowercase();
    let fn_name = match func.as_str() {
        "avg" => "avg",
        "count" => "count",
        "min" => "min",
        "max" => "max",
        _ => "sum",
    };
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_running_{}", column, fn_name));
    Ok(format!(
        "SELECT *, {fn}({col}) OVER ({part}ORDER BY {ord} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {out} FROM {up}",
        fn = fn_name,
        col = quote_ident(&column),
        part = partition_clause,
        ord = quote_ident(&order_col),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Time Bin: round a timestamp column down to the nearest multiple of
/// the chosen interval (e.g. 5-minute, 1-hour, 1-day buckets) for
/// time-series grouping. Done via epoch math so any (unit, count)
/// combination works, not just the standard date_trunc units.
fn build_dt_bin(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.bin"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Time Bin needs a timestamp column".to_string())?;
    let unit = string_prop(props, "unit").unwrap_or_else(|| "minute".into());
    let count = props
        .get("count")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .unwrap_or(5);
    let seconds_per = match unit.to_lowercase().as_str() {
        "second" | "seconds" => 1_i64,
        "minute" | "minutes" => 60,
        "hour" | "hours" => 3_600,
        "day" | "days" => 86_400,
        _ => 60,
    };
    let bucket_seconds = seconds_per * count;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_bin", column));
    let qcol = quote_ident(&column);
    // Subtract the timestamp's remainder seconds past its bucket boundary.
    // Stays inside the TIMESTAMP type the whole way - to_timestamp() would
    // return TIMESTAMPTZ which then serializes with a timezone offset and
    // round-trips wrong on non-UTC session timezones (tests failed on IST).
    Ok(format!(
        "SELECT *, CAST({col} AS TIMESTAMP) - (INTERVAL '1 second' * (((CAST(epoch(CAST({col} AS TIMESTAMP)) AS BIGINT) % {bucket}) + {bucket}) % {bucket})) AS {out} FROM {up}",
        col = qcol,
        bucket = bucket_seconds,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Array Length: scalar length of an array / list column.
fn build_arr_length(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.arr.length"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Array Length needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_length", column));
    Ok(format!(
        "SELECT *, length({col}) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Rank Filter: keep the top N rows per group, ordered by a column.
/// Common reporting pattern: 'top 3 spenders per region', 'most
/// recent 5 orders per customer'. Computes ROW_NUMBER over the
/// (partitionBy, orderBy DESC|ASC) window in a subquery, then
/// WHERE filters to rank <= N. desc defaults to true (top N).
fn build_rank_filter(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.rank.filter"))?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Rank Filter needs an orderBy column".to_string())?;
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let n = props
        .get("n")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .unwrap_or(10);
    let desc = props
        .get("desc")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let direction = if desc { "DESC" } else { "ASC" };
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    Ok(format!(
        "SELECT * EXCLUDE (_duckle_rank) FROM (SELECT u.*, row_number() OVER ({part}ORDER BY {ord} {dir}) AS _duckle_rank FROM {up} u) WHERE _duckle_rank <= {n}",
        part = partition_clause,
        ord = quote_ident(&order_col),
        dir = direction,
        n = n,
        up = quote_ident(upstream)
    ))
}

/// Forward-fill: replace NULL values with the most recent non-null
/// value within a group, ordered by a sort column. The classic
/// time-series gap-fill: missing readings get the previous reading.
/// Uses last_value(col IGNORE NULLS) over an unbounded preceding
/// window - DuckDB evaluates this in one pass.
fn build_fill_forward(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.fill_forward"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Forward Fill needs a column".to_string())?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Forward Fill needs an orderBy column".to_string())?;
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (last_value({col} IGNORE NULLS) OVER ({part}ORDER BY {ord} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {col}) FROM {up}",
        col = qcol,
        part = partition_clause,
        ord = quote_ident(&order_col),
        up = quote_ident(upstream)
    ))
}

/// Row hash: append a stable fingerprint column computed over N
/// other columns. The classic CDC primitive - hash a tuple's
/// content so downstream you can answer "did this row's value
/// change?" without comparing every column.
///
/// SQL: SELECT *, {algo}(concat_ws('||', col1::VARCHAR, col2::VARCHAR, ...)) AS _row_hash
///
/// Concat separator is '||' (a pipe sequence that won't appear in
/// typical data and that keeps multi-column distinguishable - "a"
/// + "bc" != "ab" + "c" when the boundary marker is present).
/// NULLs are coerced to the empty string via concat_ws's default
/// NULL-skipping, which means rows with the same non-null values
/// hash equal regardless of which optional fields were missing -
/// usually what you want for change detection.
fn build_row_hash(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.row_hash"))?;
    let cols: Vec<String> = columns_from_props(props, "columns").unwrap_or_default();
    if cols.is_empty() {
        return Err("Row Hash needs at least one column".to_string());
    }
    let algo = string_prop(props, "algorithm")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "md5".into());
    let algo_fn = match algo.as_str() {
        "md5" => "md5",
        "sha1" => "sha1",
        "sha256" => "sha256",
        other => return Err(format!("Row Hash: unknown algorithm '{}'", other)),
    };
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "_row_hash".into());
    let parts = cols
        .iter()
        .map(|c| format!("CAST({} AS VARCHAR)", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT *, {algo}(concat_ws('||', {parts})) AS {out} FROM {up}",
        algo = algo_fn,
        parts = parts,
        out = quote_ident(&out),
        up = quote_ident(upstream)
    ))
}

/// Audit columns: stamp every row with provenance + load metadata.
/// The classic warehouse pattern - downstream you can answer "when
/// did this row land?", "from which pipeline?", "which batch?"
/// without joining back to a runs table.
///
/// All four columns are independently toggleable. Strings (`source`,
/// `batchId`) are emitted as literals so context variables resolve
/// at compile time. Use Duckle's `{{ context.foo }}` interpolation
/// in the form to wire a per-run batch ID.
fn build_audit(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.audit"))?;
    let mut adds: Vec<String> = Vec::new();
    let loaded_at = props.get("loadedAt").and_then(JsonValue::as_bool).unwrap_or(true);
    if loaded_at {
        adds.push("current_timestamp AS _loaded_at".to_string());
    }
    if props.get("loadedDate").and_then(JsonValue::as_bool).unwrap_or(false) {
        adds.push("current_date AS _loaded_date".to_string());
    }
    if let Some(s) = string_prop(props, "source").filter(|s| !s.is_empty()) {
        adds.push(format!("'{}' AS _source", sql_escape(&s)));
    }
    if let Some(b) = string_prop(props, "batchId").filter(|s| !s.is_empty()) {
        adds.push(format!("'{}' AS _batch_id", sql_escape(&b)));
    }
    if adds.is_empty() {
        return Err("Audit: enable at least one audit column".to_string());
    }
    Ok(format!(
        "SELECT *, {extra} FROM {up}",
        extra = adds.join(", "),
        up = quote_ident(upstream)
    ))
}

/// Constant-fill: replace NULLs in a column with a user-supplied
/// literal. Rounds out the fill family (forward / backward / constant).
/// String literals are auto-quoted so the user types `unknown`, not
/// `'unknown'`. A value that parses as a finite number passes through
/// raw - lets the same prop handle numeric columns without making the
/// user know SQL quoting rules. The COALESCE expression takes the
/// column's type from the column itself, so numeric vs text doesn't
/// need a separate type hint.
fn build_fill_constant(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.fill_constant"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Fill Constant needs a column".to_string())?;
    // Accept either a string `value` (most common) or a number.
    let literal = match props.get("value") {
        Some(JsonValue::String(s)) => {
            let trimmed = s.trim();
            // If the user typed a bare FINITE number (e.g. `0`, `-1.5`),
            // pass it through unquoted so DuckDB sees a numeric literal.
            // Otherwise quote it as a string. The is_finite guard matters:
            // Rust's f64 parse also accepts "inf"/"nan"/"infinity"/"1e999",
            // which are not valid DuckDB numeric tokens and would make the
            // COALESCE fail - those are almost certainly intended as the
            // literal string fill value.
            match trimmed.parse::<f64>() {
                Ok(n) if n.is_finite() => trimmed.to_string(),
                _ => format!("'{}'", sql_escape(trimmed)),
            }
        }
        Some(JsonValue::Number(n)) => n.to_string(),
        Some(JsonValue::Bool(b)) => b.to_string(),
        _ => return Err("Fill Constant needs a value".to_string()),
    };
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (COALESCE({col}, {lit}) AS {col}) FROM {up}",
        col = qcol,
        lit = literal,
        up = quote_ident(upstream)
    ))
}

/// Backward-fill: replace NULL values with the next non-null value
/// within a group, ordered by a sort column. Pandas-style bfill /
/// "fill up" - useful when the first readings of a series are missing
/// and you'd rather impute from the future than leave them null.
/// Uses first_value(col IGNORE NULLS) over an unbounded following
/// window so the current row sees the nearest non-null ahead of it.
fn build_fill_backward(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.fill_backward"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Backward Fill needs a column".to_string())?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Backward Fill needs an orderBy column".to_string())?;
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (first_value({col} IGNORE NULLS) OVER ({part}ORDER BY {ord} ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS {col}) FROM {up}",
        col = qcol,
        part = partition_clause,
        ord = quote_ident(&order_col),
        up = quote_ident(upstream)
    ))
}

/// Numeric Bucketize: bin a numeric column into N equal-width
/// buckets between low and high. Output is 1..N for in-range values,
/// 0 for below-low, N+1 for above-high (PostgreSQL width_bucket
/// semantics). DuckDB core doesn't ship width_bucket as a scalar
/// function (only the Postgres extension defines it), so we expand
/// to the explicit floor((v - low) / step) + 1 form, which works on
/// every DuckDB build.
fn build_bucketize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.bucketize"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Bucketize needs a column".to_string())?;
    let low = props
        .get("low")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Bucketize needs a low bound".to_string())?;
    let high = props
        .get("high")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Bucketize needs a high bound".to_string())?;
    if high <= low {
        return Err("Bucketize needs high > low".to_string());
    }
    let buckets = props
        .get("buckets")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .unwrap_or(10);
    let step = (high - low) / buckets as f64;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_bucket", column));
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT *, CASE WHEN CAST({col} AS DOUBLE) < {low} THEN 0 WHEN CAST({col} AS DOUBLE) >= {high} THEN {overflow} ELSE CAST(floor((CAST({col} AS DOUBLE) - {low}) / {step}) AS INTEGER) + 1 END AS {out} FROM {up}",
        col = qcol,
        low = low,
        high = high,
        step = step,
        overflow = buckets + 1,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// JSON Array Agg: collapse multiple rows into a JSON array per group
/// via json_group_array. With no groupBy, produces one row with the
/// whole input as a single array.
fn build_json_array_agg(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.json.array_agg"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "JSON Array Agg needs a column".to_string())?;
    let group_by: Vec<String> = columns_from_props(props, "groupBy").unwrap_or_default();
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_array", column));
    let agg = format!("json_group_array({}) AS {}", quote_ident(&column), quote_ident(&output));
    if group_by.is_empty() {
        Ok(format!("SELECT {} FROM {}", agg, quote_ident(upstream)))
    } else {
        let cols = group_by
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT {cols}, {agg} FROM {up} GROUP BY {cols}",
            cols = cols,
            agg = agg,
            up = quote_ident(upstream)
        ))
    }
}

/// Text Similarity: pairwise string similarity between two columns
/// via levenshtein (edit distance), damerau_levenshtein (also counts
/// transpositions), jaccard (set similarity of trigrams), or
/// jaro_winkler_similarity (0..1, weighted toward shared prefixes).
/// The first two are integer distances (lower = more similar); the
/// last two are normalized similarities (higher = more similar).
fn build_text_similarity(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.similarity"))?;
    let left_col = string_prop(props, "leftColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Similarity needs a left column".to_string())?;
    let right_col = string_prop(props, "rightColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Similarity needs a right column".to_string())?;
    let algo = string_prop(props, "algorithm").unwrap_or_else(|| "levenshtein".into());
    let fn_name = match algo.as_str() {
        "damerau_levenshtein" => "damerau_levenshtein",
        "jaccard" => "jaccard",
        "jaro_winkler" => "jaro_winkler_similarity",
        _ => "levenshtein",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}_{}_score", left_col, right_col, fn_name));
    let l = quote_ident(&left_col);
    let r = quote_ident(&right_col);
    // jaccard() raises "argument too short!" on an empty-string input,
    // which aborts the whole run on the first empty row. Guard it: an
    // empty (or NULL) value on either side yields a NULL score instead.
    // The other algorithms handle empty/short strings fine.
    let expr = if fn_name == "jaccard" {
        format!(
            "CASE WHEN CAST({l} AS VARCHAR) = '' OR CAST({r} AS VARCHAR) = '' THEN NULL \
             ELSE jaccard(CAST({l} AS VARCHAR), CAST({r} AS VARCHAR)) END"
        )
    } else {
        format!("{fn_name}(CAST({l} AS VARCHAR), CAST({r} AS VARCHAR))")
    };
    Ok(format!(
        "SELECT *, {expr} AS {out} FROM {up}",
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Spatial Join: a two-input join whose predicate is a spatial
/// relationship between left.geom and right.geom (intersects /
/// contains / within / touches / crosses / overlaps / equals).
/// Different from xf.geo.intersects which is a one-input enrichment
/// against a fixed target. The classic "orders inside delivery zone"
/// example is `left=orders.point JOIN right=zones.polygon ON
/// ST_Within(orders.point, zones.polygon)`.
fn build_spatial_join(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let left = inputs
        .main()
        .ok_or_else(|| "Spatial Join needs a driving input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "Spatial Join needs a lookup input".to_string())?;
    let left_col = string_prop(props, "leftGeomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Spatial Join needs leftGeomColumn".to_string())?;
    let right_col = string_prop(props, "rightGeomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Spatial Join needs rightGeomColumn".to_string())?;
    let relation = string_prop(props, "relation").unwrap_or_else(|| "intersects".into());
    let fn_name = match relation.as_str() {
        "contains" => "ST_Contains",
        "within" => "ST_Within",
        "touches" => "ST_Touches",
        "crosses" => "ST_Crosses",
        "overlaps" => "ST_Overlaps",
        "equals" => "ST_Equals",
        _ => "ST_Intersects",
    };
    let kind = match string_prop(props, "joinType").as_deref() {
        Some("left") => "LEFT",
        _ => "INNER",
    };
    Ok(format!(
        "SELECT m.*, r.* FROM {} m {} JOIN {} r ON {}(CAST(m.{} AS GEOMETRY), CAST(r.{} AS GEOMETRY))",
        quote_ident(left),
        kind,
        quote_ident(right),
        fn_name,
        quote_ident(&left_col),
        quote_ident(&right_col)
    ))
}

/// Spatial Intersects: add a boolean column with ST_Intersects(geom,
/// target). Pair with xf.filter downstream to keep only the rows that
/// overlap a polygon (e.g. "orders inside a delivery zone"). Two-input
/// spatial joins land later as xf.join.spatial.
fn build_geo_intersects(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.intersects"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Spatial Intersects needs a geometry column".to_string())?;
    let target = string_prop(props, "targetWkt")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Spatial Intersects needs a target geometry (WKT)".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "intersects".into());
    Ok(format!(
        "SELECT *, ST_Intersects(CAST({col} AS GEOMETRY), ST_GeomFromText('{target}')) AS {out} FROM {up}",
        col = quote_ident(&column),
        target = target.replace('\'', "''"),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Hash: add a column with the md5 / sha1 / sha256 digest (or a
/// DuckDB `hash()` int64) of an input column. Useful for deterministic
/// IDs from natural keys, one-way PII masking, and fingerprinting.
fn build_hash(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.hash"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Hash needs a column".to_string())?;
    let algo = string_prop(props, "algorithm").unwrap_or_else(|| "md5".into());
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_hash", column));
    let fn_name = match algo.as_str() {
        "sha1" => "sha1",
        "sha256" => "sha256",
        "hash" => "hash",
        _ => "md5",
    };
    Ok(format!(
        "SELECT *, {fn_name}(CAST({col} AS VARCHAR)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Assert: hard-fail the pipeline if any row violates the given SQL
/// predicate. Unlike qa.* validators which route bad rows to a reject
/// port, this stops the whole pipeline so a downstream sink never
/// sees a partial result. Rows pass through unchanged. The CASE
/// invokes DuckDB's error() in the ELSE branch; the error surfaces
/// as the stage's failure with the user's message. The outer
/// EXCLUDE strips the temporary marker column so downstream stages
/// see the original schema.
fn build_assert(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.assert"))?;
    let predicate = string_prop(props, "predicate")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Assert needs a SQL predicate (e.g. amount >= 0)".to_string())?;
    let raw_msg = string_prop(props, "message")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("Assertion violated: {}", predicate));
    let msg = sql_escape(&raw_msg);
    // Aggregate the predicate into a single boolean across the whole
    // input via bool_and, then evaluate one CASE in a MATERIALIZED CTE.
    // This pattern (rather than a per-row CASE in the projection) is the
    // only shape DuckDB reliably keeps - the optimizer prunes unused
    // projection columns even when their CASE has error() in the ELSE,
    // which on some platforms (notably Windows release builds in CI)
    // means the assertion silently never fires. The aggregate has no
    // such hiding place; bool_and is forced to scan every row, and the
    // outer SELECT uses the CTE's value in WHERE so the CTE is
    // genuinely materialized. COALESCE(..., TRUE) treats an empty
    // input as a pass (vacuously true).
    Ok(format!(
        "WITH _duckle_assert AS MATERIALIZED (SELECT CASE WHEN COALESCE(bool_and(CAST(({pred}) AS BOOLEAN)), TRUE) THEN 'ok' ELSE error('{msg}') END AS result FROM {up}) SELECT u.* FROM {up} u WHERE (SELECT result FROM _duckle_assert) IS NOT NULL",
        pred = predicate,
        msg = msg,
        up = quote_ident(upstream)
    ))
}

/// URL Parse: pull a single component out of a URL string column via
/// a fixed regex. Picks one of scheme / host / port / path / query /
/// fragment with the `kind` prop, mirrors xf.ip.parse's shape.
fn build_url_parse(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.url.parse"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "URL Parse needs an input column".to_string())?;
    let kind = string_prop(props, "kind").unwrap_or_else(|| "host".into());
    // Single regex with named groups for every URL component. The
    // expression intentionally accepts URLs with and without a scheme.
    let url_re = "^(?:([a-zA-Z][a-zA-Z0-9+.-]*)://)?([^:/?#]*)(?::([0-9]+))?(/[^?#]*)?(?:\\?([^#]*))?(?:#(.*))?$";
    let group_idx: i64 = match kind.as_str() {
        "scheme" => 1,
        "host" => 2,
        "port" => 3,
        "path" => 4,
        "query" => 5,
        "fragment" => 6,
        _ => 2,
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, kind));
    Ok(format!(
        "SELECT *, regexp_extract(CAST({col} AS VARCHAR), '{re}', {idx}) AS {out} FROM {up}",
        col = quote_ident(&column),
        re = sql_escape(url_re),
        idx = group_idx,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// IP Parse: CAST a text/IP column to INET and extract a single
/// component via the inet extension. `kind` picks which piece comes
/// out (host / family / broadcast / netmask / hostmask / masklen /
/// network), so one row gives one output column and the upstream
/// schema is untouched. The CAST handles both bare addresses
/// (1.2.3.4 / ::1) and CIDR notation (10.0.0.0/8).
fn build_ip_parse(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.ip.parse"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "IP Parse needs an input column".to_string())?;
    let kind = string_prop(props, "kind").unwrap_or_else(|| "host".into());
    let fn_name = match kind.as_str() {
        "family" => "family",
        "broadcast" => "broadcast",
        "netmask" => "netmask",
        "hostmask" => "hostmask",
        "masklen" => "masklen",
        "network" => "network",
        _ => "host",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, fn_name));
    Ok(format!(
        "SELECT *, {fn_name}(CAST({col} AS INET)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
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

/// Fixed-width / positional source. The form gives a `columns` array
/// of `{name, start (1-based), width}` entries; the engine builds a
/// SELECT that walks each line and pulls the substring at the right
/// offset. The whole-file-as-one-column trick uses read_csv with a
/// delimiter that can't appear in plain text (chr(7) - the BEL) so
/// every line becomes a single string the SUBSTR projections can chew.
/// Trims trailing whitespace by default (the standard for fixed-width
/// dumps where every field is padded to its column width).
fn build_fixedwidth_source(props: &JsonValue) -> Result<String, String> {
    let path = string_prop(props, "path")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Fixed-width source: path required".to_string())?;
    let cols = props
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            "Fixed-width source: columns array required ({name, start, width} each)".to_string()
        })?;
    if cols.is_empty() {
        return Err("Fixed-width source: at least one column required".into());
    }
    let trim = props
        .get("trim")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let projections: Vec<String> = cols
        .iter()
        .map(|c| {
            let name = c
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("col")
                .to_string();
            let start = c.get("start").and_then(|v| v.as_i64()).unwrap_or(1);
            let width = c.get("width").and_then(|v| v.as_i64()).unwrap_or(1);
            let raw = format!("substr(line, {}, {})", start, width);
            let expr = if trim {
                format!("rtrim({})", raw)
            } else {
                raw
            };
            format!("{} AS {}", expr, quote_ident(&name))
        })
        .collect();
    // chr(7) (BEL) is virtually never present in real text; using it as
    // the read_csv delimiter forces every line to land as one column.
    // all_varchar=true keeps the line string-typed regardless of what
    // it happens to start with (numbers, dates, etc).
    Ok(format!(
        "WITH _lines AS (SELECT column0 AS line FROM read_csv_auto('{}', delim = chr(7), header = false, all_varchar = true)) SELECT {} FROM _lines",
        sql_escape(&path),
        projections.join(", ")
    ))
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
fn build_cloud_source(
    scheme: &str,
    props: &JsonValue,
    declared: Option<&[duckle_metadata::Column]>,
) -> String {
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
    // Delegate to the LOCAL format builders with the resolved cloud path
    // injected into a cloned props, so a cloud (s3/gcs/azure/http) source
    // gets the same treatment as its local counterpart: parquet column
    // projection and CSV declared-schema (`types=`) + delimiter / header /
    // quote / null / date options. Previously this re-derived a minimal
    // read with none of those, silently dropping issue-#3 type enforcement
    // and every CSV option once the file lived in the cloud (audit B1). The
    // local builders read props["path"], so inject the assembled bucket/key
    // path here.
    let mut local = props.clone();
    if let Some(obj) = local.as_object_mut() {
        obj.insert("path".into(), JsonValue::String(path.clone()));
    }
    match chosen.as_str() {
        "parquet" => build_parquet_source(&local),
        "json" => format!("SELECT * FROM read_json_auto('{}')", sql_escape(&path)),
        "tsv" => build_tsv_source(&local, declared),
        _ => build_csv_source(&local, declared),
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
        "snk.postgres" | "snk.cockroach" | "snk.mysql" | "snk.mariadb"
        | "snk.motherduck" | "snk.ducklake" | "snk.pgvector"
        | "snk.redshift" | "snk.bigquery" | "snk.quack" => build_relational_sink(component_id, props, from_view),
        "snk.excel" => Ok(build_excel_sink(props, from_view)),
        "snk.spatial" => Ok(build_spatial_sink(props, from_view)),
        "snk.iceberg" => Ok(build_iceberg_sink(props, from_view)),
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
    // Delegate to the LOCAL sink builders with the resolved cloud path
    // injected, so a cloud sink honors the same compression / delimiter /
    // null-value / header options as its local counterpart (audit B1).
    // Previously it emitted a fixed option set and ignored all of them.
    //
    // partitionBy is intentionally NOT forwarded: a partitioned directory
    // write over httpfs (s3/gs/azure) behaves very differently from a
    // single-object COPY and isn't validated against a live target, so
    // cloud sinks keep writing a single object as before. The `format` prop
    // selects the format family here (not build_json_sink's array toggle),
    // so it's stripped before the JSON delegation to preserve the current
    // NDJSON-always cloud-json behavior.
    let mut local = props.clone();
    if let Some(obj) = local.as_object_mut() {
        obj.insert("path".into(), JsonValue::String(path.clone()));
        obj.remove("partitionBy");
    }
    match chosen.as_str() {
        "csv" => build_csv_sink(&local, from_view),
        "json" | "jsonl" | "ndjson" => {
            if let Some(obj) = local.as_object_mut() {
                obj.remove("format");
            }
            build_json_sink(&local, from_view)
        }
        _ => build_parquet_sink(&local, from_view),
    }
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
    let partition = columns_from_props(props, "partitionBy").unwrap_or_default();
    if !partition.is_empty() {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        options.push(format!("PARTITION_BY ({})", cols));
        options.push("OVERWRITE_OR_IGNORE".to_string());
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
    let partition = columns_from_props(props, "partitionBy").unwrap_or_default();
    let mut options = vec![
        "FORMAT PARQUET".to_string(),
        format!("COMPRESSION '{}'", sql_escape(&compression)),
    ];
    if !partition.is_empty() {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        options.push(format!("PARTITION_BY ({})", cols));
        // DuckDB refuses to write into an existing partition directory
        // unless one of these is set; OVERWRITE_OR_IGNORE matches what
        // most ETL pipelines want (rewrite the slice we just emitted,
        // leave untouched siblings alone).
        options.push("OVERWRITE_OR_IGNORE".to_string());
    }
    format!(
        "COPY (SELECT * FROM {}) TO '{}' ({})",
        quote_ident(from_view),
        sql_escape(&path),
        options.join(", ")
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

/// Reads the `headers` key-value pairs from a HTTP connector's props.
/// Forms write them as either an object ({k: v}) or an array of
/// {key, value} entries; accept both shapes.
fn headers_from_props(props: &JsonValue) -> Vec<(String, String)> {
    let raw = match props.get("headers") {
        Some(v) => v,
        None => return Vec::new(),
    };
    if let Some(obj) = raw.as_object() {
        return obj
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
    }
    if let Some(arr) = raw.as_array() {
        return arr
            .iter()
            .filter_map(|item| {
                let k = item.get("key").and_then(|x| x.as_str())?;
                let v = item.get("value").and_then(|x| x.as_str())?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
    }
    Vec::new()
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

    fn map_sql(doc: &PipelineDoc) -> String {
        compile(doc)
            .unwrap()
            .stages
            .iter()
            .find(|s| s.node_id == "m")
            .unwrap()
            .sql
            .clone()
    }

    #[test]
    fn map_with_lookups_emits_join_chain() {
        // tMap-style: main CSV + two lookup CSVs, joined, with expressions
        // referencing each input and a filter referencing a lookup.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{"label":"cust","componentId":"src.csv","properties":{"path":"/tmp/c.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{"label":"region","componentId":"src.csv","properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "lookups":[
                    {"port":"lookup_1","leftKey":"customer_id","rightKey":"cust_id","joinType":"left"},
                    {"port":"lookup_2","leftKey":"region_code","rightKey":"code","joinType":"inner"}
                  ],
                  "expressions":[
                    {"key":"order_id","value":"main.id"},
                    {"key":"customer_name","value":"lookup_1.name"},
                    {"key":"region_name","value":"lookup_2.label"},
                    {"key":"net","value":"main.amount * 1.08"}
                  ],
                  "filter":"lookup_2.active = true"
                }}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"m","targetHandle":"lookup_1","data":{"connectionType":"lookup"}},
                {"id":"e3","source":"r","target":"m","targetHandle":"lookup_2","data":{"connectionType":"lookup"}},
                {"id":"e4","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = map_sql(&doc);
        assert!(sql.contains("LEFT JOIN \"c\" ON \"o\".\"customer_id\" = \"c\".\"cust_id\""), "left join: {}", sql);
        assert!(sql.contains("INNER JOIN \"r\" ON \"o\".\"region_code\" = \"r\".\"code\""), "inner join: {}", sql);
        assert!(sql.contains("\"o\".\"id\" AS \"order_id\""), "main expr: {}", sql);
        assert!(sql.contains("\"c\".\"name\" AS \"customer_name\""), "lookup_1 expr: {}", sql);
        assert!(sql.contains("\"o\".\"amount\" * 1.08 AS \"net\""), "arithmetic expr: {}", sql);
        assert!(sql.contains("WHERE \"r\".\"active\" = true"), "filter qualified: {}", sql);
    }

    #[test]
    fn map_without_lookups_is_unchanged() {
        // No lookups + no lookup refs: behaves like the original mapper.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "expressions":[{"key":"net","value":"main.amount * 1.08"}]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = map_sql(&doc);
        assert!(sql.contains("amount * 1.08 AS \"net\""), "strip-prefix path: {}", sql);
        assert!(!sql.contains("JOIN"), "no join when no lookups: {}", sql);
    }

    #[test]
    fn map_unconfigured_lookup_ref_errors() {
        // Referencing lookup_1 without a lookups[] entry for it must error
        // clearly, not emit broken SQL.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "expressions":[{"key":"x","value":"lookup_1.name"}]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&doc).unwrap_err().to_string();
        assert!(err.contains("lookup_1") && err.contains("lookups"), "clear error: {}", err);
    }

    #[test]
    fn map_string_literal_with_dot_prefix_not_corrupted() {
        // A string literal containing 'main.' / 'lookup_1.' must be left
        // untouched by qualification (the qualifier is string-aware).
        let aliases: std::collections::BTreeMap<String, String> = [
            ("main".to_string(), "\"o\"".to_string()),
            ("lookup_1".to_string(), "\"c\"".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            qualify_port_refs("main.id || 'see lookup_1.x or main.y'", &aliases),
            "\"o\".\"id\" || 'see lookup_1.x or main.y'"
        );
        // Escaped quotes inside the literal don't end it early.
        assert_eq!(
            qualify_port_refs("'it''s main.x' || main.id", &aliases),
            "'it''s main.x' || \"o\".\"id\""
        );
    }

    #[test]
    fn cast_honors_on_error_try_vs_hard_cast() {
        // Default "Set to NULL" must emit TRY_CAST (bad values -> NULL);
        // "Fail pipeline" must emit a hard CAST. Previously onError was
        // ignored and the engine always emitted CAST, crashing the run on
        // dirty data even though the UI default promised NULLs.
        let try_doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"amount","targetType":"int64","onError":"null"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"c","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&try_doc).unwrap().stages.iter()
            .find(|s| s.node_id == "c").unwrap().sql.clone();
        assert!(sql.contains("TRY_CAST"), "default onError should TRY_CAST: {}", sql);

        let fail_doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"amount","targetType":"int64","onError":"fail"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"c","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&fail_doc).unwrap().stages.iter()
            .find(|s| s.node_id == "c").unwrap().sql.clone();
        assert!(sql.contains("CAST") && !sql.contains("TRY_CAST"),
            "onError=fail should hard CAST: {}", sql);
    }

    #[test]
    fn addcol_wraps_expression_in_declared_type() {
        // The Add-Column form's type selector must actually type the new
        // column (CAST the expression), not be cosmetic.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"Add","componentId":"xf.addcol",
                  "properties":{"name":"total","type":"int64","expression":"qty * price"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"a","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.iter()
            .find(|s| s.node_id == "a").unwrap().sql.clone();
        assert!(sql.contains("CAST((qty * price) AS BIGINT)"),
            "addcol should cast expr to declared type: {}", sql);
    }

    #[test]
    fn downstream_ref_to_window_added_column_is_not_rejected() {
        // Regression: xf.rownum ADDS a column ("row_num"). A downstream
        // transform referencing that added column must NOT be falsely
        // rejected by the column-existence validator. Column-adding
        // transforms report "schema unknown" so downstream validation
        // is skipped rather than wrong. (Reported as "most transforms
        // erroneous" - the validator over-fired on column-adder chains.)
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/in.csv","hasHeader":true},
                  "schema":[{"name":"amount","type":"int64","nullable":true}]}},
                {"id":"rn","position":{"x":0,"y":0},"data":{
                  "label":"Row Number","componentId":"xf.rownum",
                  "properties":{"outputColumn":"row_num","orderBy":["amount"]}}},
                {"id":"d1","position":{"x":0,"y":0},"data":{
                  "label":"Distinct","componentId":"xf.distinct",
                  "properties":{"columns":["row_num"]}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"rn",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"rn","target":"d1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        // Must compile cleanly - the distinct on the rownum-added column
        // must not trip the validator.
        assert!(compile(&p).is_ok(), "rownum-added column must not be rejected downstream");
    }

    #[test]
    fn distinct_on_missing_column_errors_with_available_list() {
        // The genuine error case (issue screenshot): a customers CSV has
        // no order_id column, so xf.distinct on order_id must fail at
        // planner time with a message that lists the real columns.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/c.csv","hasHeader":true},
                  "schema":[
                    {"name":"Index","type":"int64","nullable":true},
                    {"name":"Customer Id","type":"string","nullable":true}
                  ]}},
                {"id":"d1","position":{"x":0,"y":0},"data":{
                  "label":"Distinct","componentId":"xf.distinct",
                  "properties":{"columns":["order_id"]}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"d1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).unwrap_err().to_string();
        assert!(err.contains("order_id"), "got: {}", err);
        assert!(
            err.contains("Available columns") && err.contains("Customer Id"),
            "error should list available columns, got: {}",
            err
        );
    }

    #[test]
    fn pure_sql_pipeline_marks_every_stage_batchable() {
        // CSV -> filter -> Parquet has no driver-based stages and no
        // ctl.* hooks, so every stage must report is_pure_sql() = true.
        // The batched executor uses exactly this predicate to decide
        // whether to collapse the pipeline into one CLI spawn.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/in.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"x > 0"}}},
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
        for stage in &compiled.stages {
            assert!(
                stage.is_pure_sql(),
                "stage {} ({}) should be batchable",
                stage.node_id,
                stage.component_id
            );
        }
    }

    #[test]
    fn rest_source_pipeline_is_not_batchable() {
        // src.rest hits the Rust-side ureq driver mid-pipeline, so
        // its stage must report is_pure_sql() = false. Any single
        // false stage forces the per-stage execution path.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"REST","componentId":"src.rest",
                  "properties":{"url":"https://example.com/users",
                                "responsePath":"data"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let any_non_batchable = compiled.stages.iter().any(|s| !s.is_pure_sql());
        assert!(
            any_non_batchable,
            "src.rest pipeline must contain at least one non-pure stage"
        );
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
        // Perf regression guard: a filter whose reject port is unwired must
        // compile to a lazy VIEW (so DuckDB pushes the predicate into the
        // source read) and must NOT materialize the rejected rows. The old
        // behaviour wrote every rejected row to a `__reject` table - on a
        // 10M-row source that dominated the whole run (~16s).
        assert!(
            compiled.stages[1].sql.contains("CREATE OR REPLACE VIEW \"f1\""),
            "unwired-reject filter must be a VIEW, got: {}",
            compiled.stages[1].sql
        );
        assert!(
            !compiled.stages[1].sql.contains("__reject"),
            "unwired-reject filter must not materialize a reject table, got: {}",
            compiled.stages[1].sql
        );
        assert_eq!(compiled.stages[2].kind, StageKind::Sink);
        assert!(compiled.stages[2]
            .sql
            .contains("TO '/tmp/out.parquet' (FORMAT PARQUET"));
    }

    #[test]
    fn filter_with_single_consumer_reject_is_a_lazy_view() {
        // When the reject port is consumed by exactly one downstream node,
        // it must be a lazy VIEW (inlined into that consumer), NOT a
        // materialized table. The old code always made reject a TABLE, which
        // wrote the entire rejected set to disk (8M rows on a 10M source)
        // even when its only consumer was a sink that would just COPY it.
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
                  "label":"Pass","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/pass.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"Rejected","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/rej.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1",
                  "data":{"connectionType":"main"}},
                {"id":"e3","source":"f1","sourceHandle":"reject","target":"k2",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let filter = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "f1")
            .expect("filter stage");
        assert!(
            filter.sql.contains("CREATE OR REPLACE VIEW \"f1__reject\""),
            "single-consumer reject must be a lazy VIEW, got: {}",
            filter.sql
        );
        assert!(
            !filter.sql.contains("CREATE OR REPLACE TABLE \"f1__reject\""),
            "single-consumer reject must not materialize a table, got: {}",
            filter.sql
        );
        // The pass side is also single-consumer, so it stays a lazy view too.
        assert!(
            filter.sql.contains("CREATE OR REPLACE VIEW \"f1\""),
            "single-consumer pass must be a lazy VIEW, got: {}",
            filter.sql
        );
    }

    #[test]
    fn filter_with_multi_consumer_reject_materializes_table() {
        // 2+ consumers of the reject port -> materialize it once as a TABLE
        // so the body isn't re-evaluated per consumer.
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
                  "label":"R1","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/r1.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"R2","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/r2.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","sourceHandle":"reject","target":"k1",
                  "data":{"connectionType":"main"}},
                {"id":"e3","source":"f1","sourceHandle":"reject","target":"k2",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let filter = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "f1")
            .expect("filter stage");
        assert!(
            filter.sql.contains("CREATE OR REPLACE TABLE \"f1__reject\""),
            "multi-consumer reject must materialize a table, got: {}",
            filter.sql
        );
    }

    #[test]
    fn cdc_diff_requires_compare_columns() {
        // Regression (audit B3): without compareColumns, build_cdc_diff's
        // `updated` arm is empty so every changed row is tagged 'unchanged'
        // and dropped by rejectUnchanged. compile() must reject it.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"cur","position":{"x":0,"y":0},"data":{
                  "label":"cur","componentId":"src.csv",
                  "properties":{"path":"/tmp/cur.csv","hasHeader":true}}},
                {"id":"prev","position":{"x":0,"y":0},"data":{
                  "label":"prev","componentId":"src.csv",
                  "properties":{"path":"/tmp/prev.csv","hasHeader":true}}},
                {"id":"d","position":{"x":0,"y":0},"data":{
                  "label":"Diff","componentId":"xf.cdc.diff",
                  "properties":{"naturalKey":["id"]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"cur","target":"d","data":{"connectionType":"main"}},
                {"id":"e2","source":"prev","sourceHandle":"main","target":"d","targetHandle":"lookup","data":{"connectionType":"lookup"}},
                {"id":"e3","source":"d","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).expect_err("cdc.diff without compareColumns must fail");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("compare columns"),
            "error should name compare columns, got: {}",
            msg
        );
    }

    #[test]
    fn scd1_uses_union_all_by_name() {
        // Regression (audit B3): SCD1 retains unmatched-previous rows via
        // UNION ALL, which must align cur/prev by column NAME. Positional
        // UNION ALL silently swaps values when the two inputs present
        // columns in a different order.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"cur","position":{"x":0,"y":0},"data":{
                  "label":"cur","componentId":"src.csv",
                  "properties":{"path":"/tmp/cur.csv","hasHeader":true}}},
                {"id":"prev","position":{"x":0,"y":0},"data":{
                  "label":"prev","componentId":"src.csv",
                  "properties":{"path":"/tmp/prev.csv","hasHeader":true}}},
                {"id":"scd","position":{"x":0,"y":0},"data":{
                  "label":"SCD1","componentId":"xf.cdc.scd1",
                  "properties":{"naturalKey":["id"]}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"cur","target":"scd",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"prev","sourceHandle":"main","target":"scd","targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"scd","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let scd = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "scd")
            .expect("scd1 stage");
        assert!(
            scd.sql.contains("UNION ALL BY NAME"),
            "SCD1 must align by name, got: {}",
            scd.sql
        );
    }

    #[test]
    fn printf_escapes_stray_percent_but_keeps_specs() {
        // audit B5: a literal % not forming a spec must be doubled so
        // printf prints it; real conversion specs are preserved.
        assert_eq!(escape_stray_printf_percents("100% done"), "100%% done");
        assert_eq!(escape_stray_printf_percents("%s"), "%s");
        assert_eq!(escape_stray_printf_percents("%.2f"), "%.2f");
        assert_eq!(escape_stray_printf_percents("val %s (100%%)"), "val %s (100%%)");
        assert_eq!(escape_stray_printf_percents("50% off %d items"), "50%% off %d items");
        assert_eq!(escape_stray_printf_percents("no percents"), "no percents");
    }

    #[test]
    fn numeric_rejects_non_finite_argument() {
        // audit B5: 'inf'/'nan' as a numeric op argument bind as columns
        // in DuckDB -> confusing binder error. Reject at plan time.
        for bad in ["inf", "Infinity", "nan", "-inf"] {
            let p = pipeline_from_json(&format!(
                r#"{{
                  "nodes": [
                    {{"id":"s","position":{{"x":0,"y":0}},"data":{{
                      "label":"CSV","componentId":"src.csv",
                      "properties":{{"path":"/tmp/x.csv","hasHeader":true}}}}}},
                    {{"id":"n","position":{{"x":0,"y":0}},"data":{{
                      "label":"Pow","componentId":"xf.num.power",
                      "properties":{{"column":"v","argument":"{}"}}}}}},
                    {{"id":"k","position":{{"x":0,"y":0}},"data":{{
                      "label":"out","componentId":"snk.parquet",
                      "properties":{{"path":"/tmp/o.parquet"}}}}}}
                  ],
                  "edges": [
                    {{"id":"e1","source":"s","target":"n","data":{{"connectionType":"main"}}}},
                    {{"id":"e2","source":"n","target":"k","data":{{"connectionType":"main"}}}}
                  ]
                }}"#,
                bad
            ));
            assert!(
                compile(&p).is_err(),
                "numeric op with argument '{}' should be rejected",
                bad
            );
        }
    }

    #[test]
    fn addcol_typed_expr_defaults_to_try_cast() {
        // audit B5: a typed Add-Column should TRY_CAST by default so one
        // bad value nulls the cell instead of aborting the run.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"Add","componentId":"xf.addcol",
                  "properties":{"name":"n","type":"int64","expression":"v"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"a","data":{"connectionType":"main"}},
                {"id":"e2","source":"a","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let add = compiled.stages.iter().find(|s| s.node_id == "a").unwrap();
        assert!(
            add.sql.contains("TRY_CAST((v) AS BIGINT)"),
            "typed addcol should TRY_CAST by default, got: {}",
            add.sql
        );
    }

    #[test]
    fn qa_unique_tiebreak_makes_survivor_deterministic() {
        // audit B4: with a tieBreak prop, qa.unique's ROW_NUMBER gets an
        // ORDER BY so the kept duplicate is deterministic. Without it, no
        // ORDER BY (unchanged behavior).
        let with_tb = build_quality(
            &{
                let mut ni = NodeInputs::default();
                ni.ports.insert("main".into(), vec!["up".into()]);
                ni
            },
            &serde_json::json!({"columns": ["k"], "tieBreak": ["ts"]}),
            "qa.unique",
            false,
        )
        .unwrap();
        assert!(
            with_tb.contains("PARTITION BY \"k\" ORDER BY \"ts\""),
            "tieBreak should add ORDER BY, got: {}",
            with_tb
        );
        let without = build_quality(
            &{
                let mut ni = NodeInputs::default();
                ni.ports.insert("main".into(), vec!["up".into()]);
                ni
            },
            &serde_json::json!({"columns": ["k"]}),
            "qa.unique",
            false,
        )
        .unwrap();
        assert!(
            !without.contains("ORDER BY"),
            "no tieBreak should not add ORDER BY, got: {}",
            without
        );
    }

    #[test]
    fn skip_orderby_makes_offset_deterministic() {
        // audit B4: xf.skip with an orderBy prop emits ORDER BY before
        // OFFSET so the skipped slice is repeatable.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_take(&ni, &serde_json::json!({"count": 5, "orderBy": ["id"]}), TakeKind::Offset).unwrap();
        assert!(
            sql.contains("ORDER BY \"id\" OFFSET 5"),
            "skip with orderBy should sort before offset, got: {}",
            sql
        );
    }

    #[test]
    fn distinct_orderby_prop_replaces_order_by_all() {
        // audit B10: keyed DISTINCT defaults to ORDER BY ALL (deterministic
        // but a full sort, >100x slower). An `orderBy` prop sorts only the
        // keys + tiebreak columns; default is unchanged.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let default_sql = build_distinct(&ni, &serde_json::json!({"columns": ["status"]})).unwrap();
        assert!(
            default_sql.contains("ORDER BY ALL"),
            "default keyed distinct must keep ORDER BY ALL, got: {}",
            default_sql
        );
        let fast_sql = build_distinct(
            &ni,
            &serde_json::json!({"columns": ["status"], "orderBy": ["amount"]}),
        )
        .unwrap();
        assert!(
            fast_sql.contains("ORDER BY \"status\", \"amount\"") && !fast_sql.contains("ORDER BY ALL"),
            "orderBy prop must sort keys+tiebreak, not ALL, got: {}",
            fast_sql
        );
    }

    #[test]
    fn csv_declared_schema_overrides_autodetect() {
        // Regression for issue #3: when the user sets a column to
        // VARCHAR in the Schema panel (typical fix for dd/mm/yy dates
        // that DuckDB would otherwise misparse as yyyy-mm-dd), the
        // generated read_csv_auto must include `types = {...}` so
        // DuckDB uses the requested types instead of inferring them.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/dates.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"event_date","type":"string","nullable":true}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            src_sql.contains("types = {"),
            "missing types= clause: {}",
            src_sql
        );
        assert!(
            src_sql.contains("'event_date': 'VARCHAR'"),
            "date column not forced to VARCHAR: {}",
            src_sql
        );
        assert!(
            src_sql.contains("'id': 'BIGINT'"),
            "int64 not mapped to BIGINT: {}",
            src_sql
        );
    }

    #[test]
    fn csv_date_format_passes_through_to_reader() {
        // Follow-up to #3: a user with dd/mm/yyyy dates can now keep
        // the column as a real DATE instead of forcing VARCHAR, by
        // setting the dateFormat prop. The generated SQL must include
        // dateformat='%d/%m/%Y' so DuckDB picks the right parser.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true,
                                "dateFormat":"%d/%m/%Y",
                                "timestampFormat":"%d/%m/%Y %H:%M:%S"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(sql.contains("dateformat='%d/%m/%Y'"), "missing dateformat: {}", sql);
        assert!(sql.contains("timestampformat='%d/%m/%Y %H:%M:%S'"), "missing timestampformat: {}", sql);
    }

    #[test]
    fn csv_per_column_format_wraps_with_try_strptime() {
        // Issue #10: two date/timestamp columns with DIFFERENT formats on
        // one read. Each is forced to VARCHAR in types= and re-parsed with
        // its own format via try_strptime inside a SELECT * REPLACE wrap.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[
                    {"name":"d1","type":"date","format":"%d/%m/%Y"},
                    {"name":"ts","type":"timestamp","format":"%Y-%m-%d %H:%M:%S"}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(sql.contains("SELECT * REPLACE ("), "missing REPLACE wrap: {}", sql);
        assert!(
            sql.contains("try_strptime(\"d1\", '%d/%m/%Y')::DATE AS \"d1\""),
            "missing d1 strptime: {}",
            sql
        );
        assert!(
            sql.contains("try_strptime(\"ts\", '%Y-%m-%d %H:%M:%S')::TIMESTAMP AS \"ts\""),
            "missing ts strptime: {}",
            sql
        );
        assert!(sql.contains("'d1': 'VARCHAR'"), "d1 not forced VARCHAR: {}", sql);
        assert!(sql.contains("'ts': 'VARCHAR'"), "ts not forced VARCHAR: {}", sql);
        assert!(sql.contains("FROM read_csv_auto("), "missing reader: {}", sql);
    }

    #[test]
    fn csv_date_column_without_format_keeps_native_type() {
        // A DATE column with no format (or empty format) must NOT trigger
        // the REPLACE wrap; its declared type goes straight into types=.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[{"name":"d","type":"date","format":""}]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(!sql.contains("REPLACE ("), "should not wrap without format: {}", sql);
        assert!(sql.contains("'d': 'DATE'"), "date type not preserved: {}", sql);
    }

    #[test]
    fn csv_mixed_format_and_plain_columns() {
        // One formatted date column + one plain int column: only the date
        // is rewritten; the int keeps its type and is carried through *.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[
                    {"name":"d","type":"date","format":"%d/%m/%Y"},
                    {"name":"n","type":"int64"}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(sql.contains("SELECT * REPLACE ("), "missing REPLACE wrap: {}", sql);
        assert!(sql.contains("try_strptime(\"d\", '%d/%m/%Y')::DATE AS \"d\""), "missing d: {}", sql);
        assert!(!sql.contains("\"n\")") && !sql.contains("AS \"n\""), "n should not be rewritten: {}", sql);
        assert!(sql.contains("'n': 'BIGINT'"), "int type not preserved: {}", sql);
    }

    #[test]
    fn csv_per_column_format_quotes_identifier() {
        // A formatted date column whose name needs quoting: both the
        // try_strptime arg and the AS alias must be double-quoted.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[{"name":"Order Date","type":"date","format":"%d/%m/%Y"}]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(
            sql.contains("try_strptime(\"Order Date\", '%d/%m/%Y')::DATE AS \"Order Date\""),
            "identifier not quoted: {}",
            sql
        );
    }

    #[test]
    fn cast_referencing_unknown_column_errors_at_planner() {
        // When the upstream source has a declared schema (Autodetect
        // or hand-typed), downstream xf.cast that references a column
        // not in the schema errors at compile time instead of waiting
        // for DuckDB's runtime "column not found".
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"name","type":"string","nullable":true}
                  ]}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"NAME","targetType":"VARCHAR"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'NAME'"), "should name the bad column: {}", msg);
        assert!(
            msg.contains("did you mean 'name'"),
            "should suggest the case-insensitive match: {}",
            msg
        );
    }

    #[test]
    fn cast_referencing_truly_missing_column_errors_without_hint() {
        // No close match: error still surfaces but no "did you mean".
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false}
                  ]}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"price","targetType":"DOUBLE"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'price'"), "should name the bad column: {}", msg);
        assert!(msg.contains("not found"), "should say not found: {}", msg);
    }

    #[test]
    fn fill_forward_with_unknown_column_errors() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"reading","type":"float64","nullable":true},
                    {"name":"ts","type":"timestamp","nullable":false}
                  ]}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Fill","componentId":"xf.fill_forward",
                  "properties":{"column":"Reading","orderBy":"ts"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"f",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'Reading'"), "should name the bad column: {}", msg);
        assert!(
            msg.contains("did you mean 'reading'"),
            "should suggest the close match: {}",
            msg
        );
    }

    #[test]
    fn cast_with_valid_column_in_schema_compiles() {
        // The positive case: with a declared schema and a valid column
        // reference, compile succeeds and emits the cast SQL.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"amount","type":"string","nullable":true}
                  ]}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"amount","targetType":"DOUBLE"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).expect("should compile cleanly");
        let cast_sql = compiled.stages.iter().find(|s| s.node_id == "c").unwrap().sql.as_str();
        assert!(cast_sql.contains("CAST(\"amount\" AS DOUBLE)"), "wrong cast SQL: {}", cast_sql);
    }

    #[test]
    fn cast_with_all_empty_columns_errors_loudly() {
        // Used to silently emit `SELECT * FROM upstream` (no-op) when
        // every cast entry had an empty column - the user wondered
        // why their column type didn't change.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"casts":[
                    {"column":"","targetType":"INTEGER"},
                    {"column":"   ","targetType":"DOUBLE"}
                  ]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("Cast:"), "should mention Cast: {}", msg);
        assert!(msg.contains("no column name"), "should mention the empty-column gap: {}", msg);
    }

    #[test]
    fn cast_with_duplicate_columns_errors_loudly() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"casts":[
                    {"column":"amount","targetType":"INTEGER"},
                    {"column":"amount","targetType":"DOUBLE"}
                  ]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'amount'"), "should name the duplicate column: {}", msg);
    }

    #[test]
    fn window_without_order_by_errors_clearly() {
        // xf.rank / xf.lead / xf.lag / etc. all need ORDER BY. DuckDB's
        // native error for missing ORDER BY arrives two stages later
        // and reads as "Binder Error: OVER clause requires ORDER BY";
        // we want a planner-side error mentioning the function name and
        // pointing at the right form field.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"Rank","componentId":"xf.rank",
                  "properties":{"partitionBy":["dept"]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"r",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err from missing ORDER BY");
        let msg = format!("{:?}", err);
        assert!(
            msg.to_lowercase().contains("order by"),
            "error should mention Order By: {}",
            msg
        );
        assert!(
            msg.contains("rank"),
            "error should mention the window function name: {}",
            msg
        );
    }

    #[test]
    fn union_uses_by_name_to_dodge_positional_silent_corruption() {
        // ETL users almost always expect by-name semantics. Standard SQL
        // UNION matches by position - reordering columns in one input
        // silently produces garbage with no error. DuckDB's UNION BY NAME
        // matches column names + pads missing columns with NULL.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"A","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"b","position":{"x":0,"y":0},"data":{
                  "label":"B","componentId":"src.csv",
                  "properties":{"path":"/tmp/b.csv","hasHeader":true}}},
                {"id":"u","position":{"x":0,"y":0},"data":{
                  "label":"Union","componentId":"xf.unionall","properties":{}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"a","target":"u",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"b","target":"u",
                  "data":{"connectionType":"main"}},
                {"id":"e3","source":"u","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let union_sql = compiled.stages.iter().find(|s| s.node_id == "u").unwrap().sql.as_str();
        assert!(union_sql.contains("UNION ALL BY NAME"), "expected BY NAME variant: {}", union_sql);
    }

    #[test]
    fn arr_contains_is_null_safe() {
        // list_contains(NULL_array, x) returns NULL. Without the COALESCE
        // shield, downstream WHERE _contains would silently drop the row.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Contains","componentId":"xf.arr.contains",
                  "properties":{"column":"tags","value":"red"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = compiled.stages.iter().find(|s| s.node_id == "c").unwrap().sql.as_str();
        assert!(sql.contains("COALESCE(list_contains"), "missing COALESCE shield: {}", sql);
        assert!(sql.contains(", FALSE)"), "missing FALSE fallback: {}", sql);
    }

    #[test]
    fn join_with_same_key_name_uses_using_clause() {
        // When leftKey == rightKey, USING() dedupes the join column
        // and downstream `SELECT id FROM joined` is unambiguous.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Join","componentId":"xf.join.inner",
                  "properties":{"leftKey":"customer_id","rightKey":"customer_id"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(join_sql.contains("USING (\"customer_id\")"), "missing USING clause: {}", join_sql);
        assert!(!join_sql.contains("m.\"customer_id\" = r.\"customer_id\""), "should have used USING not ON: {}", join_sql);
    }

    #[test]
    fn join_with_different_key_names_excludes_right_key() {
        // Different key names: ON + EXCLUDE the right-side key so the
        // join column isn't duplicated in the output.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Join","componentId":"xf.join.left",
                  "properties":{"leftKey":"customer_id","rightKey":"cust_id"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(join_sql.contains("EXCLUDE (\"cust_id\")"), "missing EXCLUDE: {}", join_sql);
        assert!(join_sql.contains("m.\"customer_id\" = r.\"cust_id\""), "missing ON clause: {}", join_sql);
        assert!(join_sql.contains("LEFT JOIN"), "wrong kind: {}", join_sql);
    }

    #[test]
    fn join_composite_keys_two_columns() {
        // Composite keys via comma-separated input. Both sides must
        // have the same arity or compile fails loudly.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Join","componentId":"xf.join.inner",
                  "properties":{"leftKey":"customer_id, order_date","rightKey":"customer_id, order_date"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(
            join_sql.contains("USING (\"customer_id\", \"order_date\")"),
            "composite USING wrong: {}",
            join_sql
        );
    }

    #[test]
    fn semi_join_uses_exists_not_in() {
        // Anti-join was silently dropping all rows when the right side
        // had any NULL key, because `x NOT IN (subq with NULL)` evaluates
        // to UNKNOWN. NOT EXISTS doesn't have that quirk.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Anti","componentId":"xf.anti",
                  "properties":{"leftKey":"id","rightKey":"id"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(join_sql.contains("NOT EXISTS"), "anti should use NOT EXISTS: {}", join_sql);
        assert!(!join_sql.contains("NOT IN"), "should not emit NOT IN: {}", join_sql);
    }

    #[test]
    fn row_hash_emits_concat_ws_with_casts() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"h","position":{"x":0,"y":0},"data":{
                  "label":"Hash","componentId":"xf.row_hash",
                  "properties":{"columns":["id","email","status"],"algorithm":"sha256","outputColumn":"fp"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"h",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"h","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("sha256("), "wrong algorithm: {}", sql);
        assert!(sql.contains("concat_ws('||'"), "wrong separator: {}", sql);
        assert!(sql.contains("CAST(\"id\" AS VARCHAR)"), "id not cast: {}", sql);
        assert!(sql.contains("CAST(\"email\" AS VARCHAR)"), "email not cast: {}", sql);
        assert!(sql.contains(" AS \"fp\""), "custom output column not honoured: {}", sql);
    }

    #[test]
    fn row_hash_default_algorithm_is_md5() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"h","position":{"x":0,"y":0},"data":{
                  "label":"Hash","componentId":"xf.row_hash",
                  "properties":{"columns":["id"]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"h",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"h","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("md5("), "default should be md5: {}", sql);
        assert!(sql.contains(" AS \"_row_hash\""), "default output column wrong: {}", sql);
    }

    #[test]
    fn audit_emits_selected_columns_only() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"Audit","componentId":"xf.audit",
                  "properties":{"loadedAt":true,"loadedDate":false,"source":"orders_etl","batchId":"2026-05-27"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"a",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"a","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("current_timestamp AS _loaded_at"), "loaded_at missing: {}", sql);
        assert!(!sql.contains("_loaded_date"), "loaded_date should be off: {}", sql);
        assert!(sql.contains("'orders_etl' AS _source"), "source literal missing: {}", sql);
        assert!(sql.contains("'2026-05-27' AS _batch_id"), "batch_id missing: {}", sql);
    }

    #[test]
    fn fill_constant_string_value_quoted() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Fill","componentId":"xf.fill_constant",
                  "properties":{"column":"status","value":"unknown"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"f",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("COALESCE(\"status\", 'unknown')"), "string literal not quoted: {}", sql);
    }

    #[test]
    fn fill_constant_numeric_value_unquoted() {
        // Bare numbers (`0`, `-1.5`) pass through unquoted so DuckDB
        // sees a numeric literal and doesn't try to cast a string.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Fill","componentId":"xf.fill_constant",
                  "properties":{"column":"qty","value":"0"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"f",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("COALESCE(\"qty\", 0)"), "numeric literal got quoted: {}", sql);
    }

    #[test]
    fn csv_without_declared_schema_uses_autodetect() {
        // Inverse check: no schema -> no columns clause, so DuckDB
        // falls back to its normal autodetect.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        assert!(
            !compiled.stages[0].sql.contains("types = {"),
            "should not emit types clause without a declared schema: {}",
            compiled.stages[0].sql
        );
    }

    #[test]
    fn cloud_parquet_source_projects_declared_columns() {
        // audit B1: a cloud parquet source must honor the `columns`
        // projection like the local builder (delegation), not read SELECT *.
        let sql = build_cloud_source(
            "s3",
            &serde_json::json!({"format": "parquet", "path": "s3://b/k.parquet", "columns": "id, amount"}),
            None,
        );
        assert!(
            sql.contains("SELECT \"id\", \"amount\" FROM read_parquet('s3://b/k.parquet')"),
            "cloud parquet must project declared columns, got: {}",
            sql
        );
    }

    #[test]
    fn cloud_csv_source_threads_declared_schema() {
        // audit B1: a cloud CSV source must honor a Schema-panel declaration
        // via types= (issue #3 parity), not a bare read_csv_auto.
        let cols = vec![duckle_metadata::Column {
            name: "amt".into(),
            data_type: duckle_metadata::DataType::String,
            nullable: true,
            primary_key: None,
            format: None,
        }];
        let sql = build_cloud_source(
            "s3",
            &serde_json::json!({"format": "csv", "path": "s3://b/k.csv", "hasHeader": true}),
            Some(&cols),
        );
        assert!(
            sql.contains("types = {") && sql.contains("'amt': 'VARCHAR'"),
            "cloud csv must thread declared schema via types=, got: {}",
            sql
        );
    }

    #[test]
    fn cloud_csv_sink_honors_options_but_not_partitionby() {
        // audit B1: a cloud CSV sink must honor delimiter/nullValue (ignored
        // before), but must NOT emit PARTITION_BY (unvalidated over httpfs).
        let sql = build_cloud_sink(
            &serde_json::json!({
                "format": "csv", "path": "s3://b/out.csv",
                "delimiter": "|", "nullValue": "NA", "partitionBy": "id"
            }),
            "v",
        );
        assert!(
            sql.contains("FORMAT CSV") && sql.contains("DELIM '|'") && sql.contains("NULLSTR 'NA'"),
            "cloud csv sink must honor options, got: {}",
            sql
        );
        assert!(
            !sql.contains("PARTITION_BY"),
            "cloud sink must not emit PARTITION_BY, got: {}",
            sql
        );
        assert!(sql.contains("'s3://b/out.csv'"), "must write to the cloud path, got: {}", sql);
    }

    #[test]
    fn csv_partial_declared_schema_uses_types_not_columns() {
        // Regression (audit B2): a Schema-panel declaration that covers only
        // SOME of a wider file's columns must emit `types = {...}` (name-
        // match, partial-ok), NOT `columns = {...}` (positional, requires
        // the full schema). The old `columns` emission made read_csv_auto
        // hard-fail with a sniffer arity error for the common "declare just
        // the column I care about" case.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/wide.csv","hasHeader":true},
                  "schema":[
                    {"name":"amt","type":"string","nullable":true}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            src_sql.contains("types = {") && src_sql.contains("'amt': 'VARCHAR'"),
            "partial declaration must emit types= with the declared column: {}",
            src_sql
        );
        assert!(
            !src_sql.contains("columns = {"),
            "partial declaration must NOT emit columns= (positional, full-schema): {}",
            src_sql
        );
    }

    #[test]
    fn quack_source_emits_attach_with_secret() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"Quack","componentId":"src.quack",
                  "properties":{"host":"duck.example.com","port":9494,
                                "token":"super_secret","tableName":"orders"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            src_sql.contains("CREATE OR REPLACE SECRET duckle_quack_secret"),
            "missing SECRET creation: {}",
            src_sql
        );
        assert!(src_sql.contains("TYPE QUACK"), "wrong SECRET type: {}", src_sql);
        assert!(src_sql.contains("'super_secret'"), "token not in SECRET: {}", src_sql);
        assert!(
            src_sql.contains("ATTACH 'quack:duck.example.com:9494'"),
            "wrong ATTACH URL: {}",
            src_sql
        );
        assert!(src_sql.contains("AS duckle_src"), "wrong alias: {}", src_sql);
        assert!(src_sql.contains("READ_ONLY"), "missing READ_ONLY: {}", src_sql);
        assert!(
            src_sql.contains("SELECT * FROM duckle_src"),
            "missing SELECT from alias: {}",
            src_sql
        );
    }

    #[test]
    fn quack_source_omits_secret_when_no_token() {
        // Unauthenticated test servers: leave the SECRET off entirely
        // rather than emitting an empty TOKEN clause.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"Quack","componentId":"src.quack",
                  "properties":{"host":"localhost","tableName":"t"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            !src_sql.contains("CREATE OR REPLACE SECRET"),
            "should not emit empty SECRET: {}",
            src_sql
        );
        // Default port 9494 is appended when host has no explicit port.
        assert!(
            src_sql.contains("'quack:localhost:9494'"),
            "missing default port: {}",
            src_sql
        );
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
