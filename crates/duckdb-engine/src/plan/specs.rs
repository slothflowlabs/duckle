//! Connector / transform / control runtime spec types.
//!
//! Pure data definitions extracted from the planner so plan/mod.rs stays
//! focused on graph compilation and SQL generation. Re-exported via
//! `pub use specs::*` from the parent module, so existing `plan::XxxSpec`
//! paths are unchanged.

/// ctl.parallelize: run the independent downstream branches concurrently.
/// Each branch is a self-contained sub-pipeline doc (JSON) whose source is an
/// injected src.parquet reading the `${__PSNAP__}` snapshot placeholder; the
/// executor snapshots the upstream once, substitutes the real snapshot path,
/// and runs each branch in its own temp DB on a worker thread.
#[derive(Debug, Clone)]
pub struct ParallelizeSpec {
    pub branches: Vec<String>,
    /// Max branches running at once; 0 = all at once.
    pub max_concurrency: usize,
}

/// xf.incremental: watermark-based incremental load. Only rows whose
/// `column` is greater than the last successful run's high-water mark are
/// passed through; the new mark is persisted to workspace state after the
/// whole run succeeds, so the next run resumes from there.
#[derive(Debug, Clone)]
pub struct IncrementalSpec {
    pub node_id: String,
    pub from_view: String,
    pub column: String,
    /// Starting watermark for the very first run (before any state exists).
    /// None loads everything on the first run.
    pub initial: Option<String>,
}

/// src.ducklake.changes: DuckLake change-data-feed (CDC) source. ATTACHes a
/// DuckLake catalog, reads the last consumed snapshot id from workspace state
/// (same mechanism as xf.incremental), and materializes
/// `table_changes(table, last, current)` - the row-level insert / delete /
/// update_preimage / update_postimage deltas, with the change_type column
/// preserved. The new snapshot id is persisted only on run success.
#[derive(Debug, Clone)]
pub struct DuckLakeCdcSpec {
    pub node_id: String,
    /// DuckLake catalog path (a local `.ducklake` file or a metadata DB DSN).
    pub path: String,
    /// Where the lake's data files live. Required by DuckLake when the catalog
    /// is a sqlite/postgres/mysql DSN rather than a local file and the lake does
    /// not exist yet; ignored for an existing lake, which stores its own.
    pub data_path: Option<String>,
    /// DuckLake schema; default "main".
    pub schema: Option<String>,
    pub table: String,
    /// Snapshot id to start from on the very first run (0 = from the start).
    pub initial_snapshot: u64,
    /// Keep only `insert` change rows when true; otherwise all change types.
    pub inserts_only: bool,
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
    /// Non-empty in "upsert" write mode: the key columns to MERGE on.
    /// Empty means plain INSERT.
    pub upsert_keys: Vec<String>,
    /// Upsert delete propagation: when set, rows whose `delete_column`
    /// equals `delete_value` are removed from the target (matched by key)
    /// instead of being inserted/updated. Drives CDC deletes (xf.cdc.diff
    /// / DuckLake CDC change_type='delete'). None disables it.
    pub delete_column: Option<String>,
    pub delete_value: String,
    /// "overwrite" write mode: empty the target before inserting, so it holds
    /// this run's rows and nothing older. TRUNCATE rather than drop-and-recreate,
    /// which would discard the table's grants and column types.
    ///
    /// Only applied when there are rows to write. A run that produced nothing
    /// leaves the target alone rather than emptying it on the strength of an
    /// upstream that may simply have failed to produce.
    pub truncate_first: bool,
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
    /// Write mode: "append"/"overwrite" (create + insert) or "truncate"
    /// (TRUNCATE TABLE then insert). "upsert" is driven by upsert_keys.
    pub mode: String,
    /// Non-empty in "upsert" write mode: the key columns to MERGE on.
    /// Empty means plain INSERT.
    pub upsert_keys: Vec<String>,
    /// Upsert delete propagation (see SnowflakeSinkSpec). None disables it.
    pub delete_column: Option<String>,
    pub delete_value: String,
}

/// src.oracle: Oracle SELECT via the oracle crate. Same feature gate.
#[derive(Debug, Clone)]
pub struct OracleSourceSpec {
    pub node_id: String,
    pub connect: String,
    pub user: String,
    pub password: String,
    pub query: String,
    /// True when at most one downstream stage reads this source. The Arrow fast
    /// path then exposes the temp parquet as a lazy read_parquet VIEW instead of
    /// copying it into a table, which skips the whole decode-and-store pass and
    /// lets the consumer push projection / predicate into the parquet scan.
    pub single_consumer: bool,
    /// Numeric or date column to split the extract on, so several sessions can
    /// fetch disjoint ranges at once. Empty means a single-session read.
    /// Should be indexed and reasonably evenly distributed - ranges are cut at
    /// equal width between MIN and MAX, so a heavily skewed column just makes
    /// one session do most of the work.
    pub parallel_column: Option<String>,
    /// How many sessions to read with. 1 (the default) means no parallelism.
    pub parallel_degree: usize,
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
    /// True when at most one downstream stage reads this source. The executor
    /// then exposes the materialized parquet as a lazy read_parquet VIEW
    /// (skipping the table copy + enabling projection / predicate pushdown);
    /// 2+ consumers get a real TABLE so the rows are decoded once.
    pub single_consumer: bool,
}

/// snk.adbc: write the upstream view into a target table through a prebuilt
/// ADBC driver loaded at runtime. The executor COPYs the upstream to a Parquet
/// temp file and bulk-ingests it via the ADBC bind_stream + ingest API (no
/// per-row round-trips, no in-process DuckDB write).
#[derive(Debug, Clone)]
pub struct AdbcSinkSpec {
    pub from_view: String,
    /// Path to the driver shared library (preferred) or a bare driver name.
    pub driver: String,
    /// Custom init entrypoint; defaults to AdbcDriverInit when None.
    pub entrypoint: Option<String>,
    /// ADBC database options (uri, username, password, driver-specific keys).
    pub options: Vec<(String, String)>,
    /// Target table to ingest into.
    pub table: String,
    /// Optional target schema (ADBC TargetDbSchema ingest option).
    pub schema: Option<String>,
    /// Optional target catalog (ADBC TargetCatalog ingest option).
    pub catalog: Option<String>,
    /// "append" (create-if-missing then append) or "overwrite" (replace).
    pub mode: String,
}

/// src.teradata: read from Teradata over its free ODBC driver (there is no
/// DuckDB Teradata extension and no native Rust driver). The executor connects
/// through the user's installed Teradata ODBC driver, runs the query, and
/// materializes the result with per-column typed casts (read all text, then
/// TRY_CAST each column to its DuckDB-equivalent type) so numbers / decimals /
/// dates / timestamps keep their types.
#[derive(Debug, Clone)]
pub struct TeradataSourceSpec {
    pub node_id: String,
    /// Full ODBC connection string (built from the friendly fields, a DSN, or
    /// supplied verbatim). Carries the password, so it is never logged.
    pub conn_str: String,
    pub query: String,
    /// Rows fetched per ODBC batch.
    pub batch_rows: usize,
}

/// snk.teradata: write the upstream view into a Teradata table over ODBC. The
/// executor reads the upstream rows and INSERTs them through the Teradata ODBC
/// driver (one INSERT per row, the dialect-safe form; large loads should use
/// Teradata's bulk utilities). Append creates the table if missing; overwrite
/// clears it first. No upsert (rejected at plan time).
#[derive(Debug, Clone)]
pub struct TeradataSinkSpec {
    pub from_view: String,
    /// Full ODBC connection string. Carries the password, so it is never logged.
    pub conn_str: String,
    /// Optional target database the table lives in (qualifies the table name).
    pub database: Option<String>,
    pub table: String,
    /// "append" (create-if-missing then append) or "overwrite" (clear first).
    pub mode: String,
}

/// Single-consumer network-DB source (postgres / mysql / mariadb / cockroach /
/// redshift) read via DuckDB's ATTACH extensions. Instead of inserting the
/// rows into an on-disk run-db TABLE, the executor COPYs the already-typed
/// result to a temp parquet once and exposes a lazy read_parquet VIEW - the
/// parquet write is cheaper than the table insert and the consumer gets
/// projection / predicate pushdown. Same proven path as src.adbc, and lossless
/// because the rows are already typed (unlike the read_json_auto sources).
/// Only built when exactly one stage consumes the source; 2+ consumers keep
/// the plain CREATE TABLE so the rows are materialized once.
#[derive(Debug, Clone)]
pub struct AttachParquetSourceSpec {
    pub node_id: String,
    /// INSTALL/LOAD/ATTACH preamble (ends with a trailing space); creates the
    /// process-local `duckle_src` alias the body reads from.
    pub attach: String,
    /// The source SELECT body (e.g. `SELECT * FROM duckle_src."orders"`).
    pub body: String,
}

/// materialize = "duckdb" / "duckdbfile": persist this stage into a DuckDB
/// database file (a real table, not parquet), then expose it to the run as a
/// normal table so downstream stages read it unchanged. `output_path = None`
/// is a temporary file (swept at run end); `Some(path)` is a user-named,
/// persistent `.duckdb` the rows stay in so they can be queried for analytics
/// later without re-running the pipeline.
#[derive(Debug, Clone)]
pub struct MaterializeDuckDbSpec {
    pub node_id: String,
    /// Same INSTALL/LOAD/ATTACH preamble the plain stage uses (empty for a
    /// local transform); the body reads from whatever it sets up.
    pub attach: String,
    /// The stage's SELECT body.
    pub body: String,
    /// Target `.duckdb` path; `None` = a run-scoped temp file.
    pub output_path: Option<String>,
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

/// SASL credentials for a Kafka broker.
///
/// The GUI has offered these fields since the connector shipped while nothing
/// read them, so a user who filled them in got an unauthenticated connection
/// and no indication of it.
#[derive(Debug, Clone)]
pub struct KafkaSasl {
    /// PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. Anything else is refused at plan
    /// time rather than silently downgraded.
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

/// snk.kafka / snk.redpanda: bulk-produce one Kafka record per
/// upstream row. Record key = optional keyColumn value; record value
/// = JSON-stringified row. Records are produced into a single
/// partition (partitionId, default 0) - parallel multi-partition
/// produce is a follow-up.
#[derive(Debug, Clone)]
pub struct KafkaSinkSpec {
    /// Connect over TLS. Set from the Security protocol field (SSL / SASL_SSL).
    pub tls: bool,
    /// SASL credentials, when the node supplies them.
    pub sasl: Option<KafkaSasl>,
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
    /// Confluent Schema Registry base URL. When set, a message carrying the
    /// Confluent framing (a zero byte, then a big-endian schema id) is decoded
    /// against the schema that id names, instead of being handed back as text.
    pub schema_registry_url: Option<String>,
    /// Connect over TLS. Set from the Security protocol field (SSL / SASL_SSL).
    pub tls: bool,
    /// SASL credentials, when the node supplies them.
    pub sasl: Option<KafkaSasl>,
    pub node_id: String,
    pub bootstrap_servers: String,
    pub topic: String,
    pub partition_id: i32,
    pub start_offset: i64,
    pub max_records: u64,
    /// Remember where this node got to, and resume there next run.
    ///
    /// Without it a scheduled read either re-reads the whole backlog (an
    /// `earliest` start) or skips everything that arrived since the last run (a
    /// `latest` start), so repeated runs cannot be stitched into a stream. The
    /// resume point is written only when the whole run succeeded, so a failure
    /// after the read re-delivers rather than loses: at-least-once.
    pub track_offset: bool,
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

/// src.qvd (#88): Qlik QVD reader via the clean-room `crate::qvd` decoder. The
/// QVD header carries its own schema, so no config beyond the path is needed.
#[derive(Debug, Clone)]
pub struct QvdSourceSpec {
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

/// snk.model: register a trained model's card (#253).
///
/// Deliberately NOT a model store. The engine never touches the model bytes:
/// the training script writes those wherever it likes and reports the URI as a
/// column, and this records the card describing them. What the engine adds is
/// the part a convention cannot give you - the card is written only if the run
/// actually succeeded, and a `latest` pointer moves with it so downstream
/// pipelines are not edited on every retrain.
#[derive(Debug, Clone)]
pub struct ModelCardSpec {
    pub node_id: String,
    pub from_view: String,
    /// Directory the registry lives in, usually `${workspace}/models`.
    pub dir: String,
    /// Model name. Cards land under `<dir>/<name>/`.
    pub name: String,
}

/// src.pdf: one row per page of a PDF (#248).
///
/// The text layer a document already carries, plus page geometry and the Info
/// dictionary, so a page is a row like any other. No OCR: a scanned page comes
/// back with `has_text_layer` false, which is what makes it routable.
#[derive(Debug, Clone)]
pub struct PdfSourceSpec {
    pub node_id: String,
    /// A .pdf file, or a directory of them. Ignored when `input` names an
    /// upstream relation.
    pub path: String,
    /// #282: read the documents named by an upstream artifact relation instead
    /// of a configured path.
    pub input: ArtifactInput,
    /// How many documents to parse at once. Sequential by default: rendering
    /// and parsing a document can take a lot of memory, and the bound that
    /// matters is one artifact times this.
    pub concurrency: usize,
    /// What to do with a document that cannot be parsed: "fail" the run,
    /// "skip" it, or "reject" it down the reject port.
    pub on_error: String,
    /// Descend into sub-directories when `path` is a directory.
    pub recursive: bool,
    /// Optional declared output schema (the node's Schema tab).
    pub declared_schema: Option<Vec<duckle_metadata::Column>>,
}

/// #255: one column extracted from a matched row element.
///
/// `selector` is evaluated RELATIVE to the row element; empty means the row
/// element itself. `attr` takes an attribute instead of the text, which is how
/// a link's href or a data- value is read.
#[derive(Debug, Clone)]
pub struct HtmlColumn {
    pub name: String,
    pub selector: String,
    pub attr: Option<String>,
}

/// src.html: rows out of an HTML page, by CSS selector.
///
/// A lot of public data is only published as HTML - registries, filing pages,
/// results tables - and getting at it used to mean shelling out to Python
/// before anything could enter a pipeline. HTML is not XML: real pages carry
/// unclosed tags and unquoted attributes that the strict XML reader rejects
/// outright, so this parses with a tolerant HTML parser instead.
#[derive(Debug, Clone)]
pub struct HtmlSourceSpec {
    /// #256: per-node transport (proxy, timeouts, User-Agent), usually filled
    /// from a saved `http` connection. None uses the shared default agent.
    pub transport: Option<crate::tls::HttpTransport>,
    pub node_id: String,
    /// A local filesystem path, or an `http(s)://` URL fetched through the
    /// shared proxy- and CA-aware agent.
    pub path: String,
    /// CSS selector: every match is one row.
    pub row_selector: String,
    /// How to fill each column. Empty means table mode: the row selector names
    /// a table, its `th` cells become the column names and each `tr` a row.
    pub columns: Vec<HtmlColumn>,
    /// Request headers for the http(s) case, including any auth the REST
    /// helpers build.
    pub headers: Vec<(String, String)>,
    /// Optional declared output schema (the node's Schema tab). When set, the
    /// result is pinned to exactly these columns and types, so a daily scrape
    /// keeps a stable shape even on a day the page renders a column empty.
    pub declared_schema: Option<Vec<duckle_metadata::Column>>,
}

/// src.xml: walk an XML document, find every element matching a
/// slash-separated path (e.g. "library/books/book"), and emit each
/// match as a JSON object. Attributes prefix with '@'; text content
/// goes to '_text'; nested elements become nested objects (or arrays
/// when the same tag repeats inside a parent).
#[derive(Debug, Clone)]
pub struct XmlSourceSpec {
    pub node_id: String,
    /// A local filesystem path, or a remote URI: `http(s)://` (streamed via the
    /// shared HTTP agent) or `sftp://[user@]host[:port]/remote/path` (streamed
    /// over SSH). Remote inputs never land on disk or in RAM whole (issue #186).
    pub path: String,
    /// Slash-separated element names from the root. Empty = take
    /// every immediate child of the root.
    pub row_path: String,
    /// Optional declared output schema (the node's Schema tab). When set, the
    /// result is pinned to exactly these columns and types (VARCHAR read +
    /// TRY_CAST), so a daily run's table shape stays stable regardless of that
    /// day's data; when None the schema is inferred from every row.
    pub declared_schema: Option<Vec<duckle_metadata::Column>>,
    /// SFTP credentials, used only when `path` is an `sftp://` URI. Host / port
    /// / user come from the URI; these secrets come from the node props (and may
    /// be `${ENV:...}` placeholders).
    pub sftp_password: Option<String>,
    pub sftp_private_key: Option<String>,
    pub sftp_key_passphrase: Option<String>,
    pub sftp_host_fingerprint: Option<String>,
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

/// snk.qvd (#88): write upstream rows to a Qlik QVD file via the clean-room
/// `crate::qvd` writer. Column order follows the first row; no schema config.
#[derive(Debug, Clone)]
pub struct QvdSinkSpec {
    pub from_view: String,
    pub path: String,
}

/// src.gizmosql: read from a GizmoSQL (Arrow Flight SQL) server via the
/// clean-room `crate::gizmosql` client. Result is streamed to Parquet and
/// materialized with DuckDB read_parquet, like the ADBC source.
#[derive(Debug, Clone)]
pub struct GizmoSqlSourceSpec {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub tls: bool,
    pub tls_skip_verify: bool,
    pub query: String,
    pub single_consumer: bool,
}

/// snk.gizmosql: write upstream rows to a table on a GizmoSQL (Arrow Flight SQL)
/// server via CREATE + batched INSERT over the Flight SQL protocol.
#[derive(Debug, Clone)]
pub struct GizmoSqlSinkSpec {
    pub from_view: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub tls: bool,
    pub tls_skip_verify: bool,
    pub table: String,
    /// "append" | "overwrite" | "create".
    pub mode: String,
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

/// xf.dbt: run a dbt Core project through the dbt-duckdb adapter. The
/// engine generates a profiles.yml pointing dbt at the run's working
/// database (or `database` when set), so dbt models see every upstream
/// node table and their output tables are readable downstream. The
/// upstream table name is passed to dbt as the `duckle_input` var. With
/// `output_model` set the node's output is that model's rows; otherwise
/// it is a per-model summary parsed from target/run_results.json.
/// Requires a user-installed dbt with the duckdb adapter (pip/pipx
/// install dbt-duckdb); `dbt_bin` overrides the executable path.
#[derive(Debug, Clone)]
pub struct DbtSpec {
    pub node_id: String,
    /// Directory containing dbt_project.yml. None = inline mode: the engine
    /// scaffolds an ephemeral one-model project from `inline_model`.
    pub project_dir: Option<String>,
    /// Inline model SQL (UI authoring, no external project). Scaffolded as
    /// models/<inline_model_name>.sql in a temp project when project_dir is None.
    pub inline_model: Option<String>,
    /// Name of the inline model (and its output table). Default "duckle_model".
    pub inline_model_name: String,
    /// dbt subcommand + args, e.g. "run --select staging". Default "run".
    pub command: String,
    /// dbt executable override; otherwise DUCKLE_DBT_BIN / bundled / PATH.
    pub dbt_bin: Option<String>,
    /// Target DuckDB file; default = the run's working database.
    pub database: Option<String>,
    /// Schema for the generated profile. Default "main".
    pub schema: String,
    /// Model/table to read back as this node's output rows.
    pub output_model: Option<String>,
    /// First upstream node table, exposed to dbt as var("duckle_input").
    pub from_view: Option<String>,
    /// All upstream node tables (by node id), exposed to dbt as the list
    /// var("duckle_inputs") so a multi-source inline model can reference them
    /// all. Each is also a real table dbt can read via sources.
    pub from_views: Vec<String>,
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

/// src.sftp: download files from an SFTP (SSH) server, one row per file
/// {filename, size, content_b64, modified}. Distinct from FTP/FTPS - SSH
/// transport via russh + russh-sftp on the ring backend (async, wrapped in
/// block_on by the executor). Auth by password or an OpenSSH private key;
/// the server's host key is verified against an optional SHA256 fingerprint
/// pin (the reporter's "Host Fingerprint" ask, issue #16).
#[derive(Debug, Clone)]
pub struct SftpSourceSpec {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub key_passphrase: Option<String>,
    pub directory: String,
    pub pattern: Option<String>,
    pub max_files: u64,
    /// Expected server host-key fingerprint, e.g. "SHA256:abc123...". When set,
    /// the connection is refused unless the server key matches. When empty,
    /// the key is accepted on trust (trust-on-first-use, logged).
    pub host_fingerprint: Option<String>,
}

/// snk.ftp: upload pipeline output to an FTP / FTPS server. The view is
/// first COPY-ed to a local temp file in the chosen `format`, then the file
/// is uploaded via suppaftp `put_file` to `remote_path` (a full remote path
/// including filename). SFTP is a separate protocol and is handled by
/// SftpSinkSpec.
#[derive(Debug, Clone)]
pub struct FtpSinkSpec {
    pub from_view: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub secure: bool,
    /// Full remote path including filename, e.g. /out/orders.csv.
    pub remote_path: String,
    /// csv | parquet | json | jsonl (default csv).
    pub format: String,
}

/// snk.ftp (SFTP): upload pipeline output to an SFTP (SSH) server. The view
/// is COPY-ed to a local temp file in the chosen `format`, then uploaded via
/// russh + russh-sftp `create` + `write_all`. Auth by password or an OpenSSH
/// private key; the server host key is verified against an optional SHA256
/// fingerprint pin (mirrors SftpSourceSpec).
#[derive(Debug, Clone)]
pub struct SftpSinkSpec {
    pub from_view: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub key_passphrase: Option<String>,
    /// Full remote path including filename, e.g. /out/orders.csv.
    pub remote_path: String,
    /// csv | parquet | json | jsonl (default csv).
    pub format: String,
    /// Expected server host-key fingerprint, e.g. "SHA256:abc123...". When set,
    /// the connection is refused unless the server key matches. When empty,
    /// the key is accepted on trust (trust-on-first-use).
    pub host_fingerprint: Option<String>,
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
/// ctl.file: one typed filesystem operation, staged around a run.
///
/// The only filesystem-capable component before this ran a shell command, which
/// meant one authored pipeline could not serve both platforms (cmd.exe on one,
/// /bin/sh on the other) and returned an exit code rather than doing a named
/// thing. Staging a file is ordinary batch work and deserves to be typed.
pub struct FileOpSpec {
    /// "copy", "move" or "delete".
    pub op: String,
    pub source: String,
    /// Empty for "delete".
    pub destination: String,
    /// Overwrite an existing destination. Off means an existing file is an error.
    pub overwrite: bool,
    /// Off means a missing source (or a failed operation) is reported and the
    /// stage still succeeds, which is what housekeeping usually wants.
    pub fail_on_error: bool,
}

#[derive(Debug, Clone)]
pub struct EmailSinkSpec {
    /// The relation whose rows become one email each. Empty in notification
    /// mode, where there is no upstream and `fixed` carries the whole message.
    pub from_view: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub from_address: String,
    pub to_column: String,
    pub subject_column: String,
    pub body_column: String,
    /// Set when the node has no upstream: send exactly one message with this
    /// (to, subject, body) instead of reading columns.
    ///
    /// A notification is not a row. Wiring an ordering link into a mail step to
    /// say "tell someone we got here" is ordinary, and requiring rows for it
    /// meant inventing a one-row table just to carry three constants.
    pub fixed: Option<(String, String, String)>,
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

/// src.websocket (issue #192): connect to a WebSocket URL (ws:// or wss://),
/// optionally send one subscribe frame on connect, then read up to
/// `max_messages` messages (or until the `timeout_ms` deadline), parse each as
/// JSON (object -> one row, array -> a row per element, anything else ->
/// `{message: text}`), and close. A client connector for live feeds (market
/// data, sensor streams, chat). tungstenite answers server pings automatically.
#[derive(Debug, Clone)]
pub struct WebSocketSourceSpec {
    pub node_id: String,
    pub url: String,
    /// Optional frame sent immediately after connect, e.g. a subscription like
    /// `{"type":"subscribe","channel":"trades"}`.
    pub subscribe: Option<String>,
    pub max_messages: u64,
    pub timeout_ms: u64,
    /// Extra request headers (e.g. `Authorization`) applied to the handshake.
    pub headers: Vec<(String, String)>,
}

/// snk.websocket (issue #192): connect to a WebSocket URL (ws:// or wss://) and
/// send each upstream row as a text frame - the whole row as JSON, or one
/// column's value when `message_column` is set - then close.
#[derive(Debug, Clone)]
pub struct WebSocketSinkSpec {
    pub from_view: String,
    pub url: String,
    /// When set, send this column's value as the frame; otherwise send the whole
    /// row serialized as JSON.
    pub message_column: Option<String>,
    pub headers: Vec<(String, String)>,
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
    /// #142: custom request headers (e.g. gateway auth) applied before the
    /// default Authorization/Content-Type. Empty = default OpenAI-compatible path.
    pub headers: Vec<(String, String)>,
    /// #142: override the request path (default `/v1/embeddings`) for custom
    /// OpenAI-compatible gateways. None = default path.
    pub endpoint_path: Option<String>,
    /// #258: at most this many requests in flight. 1 = sequential, byte for
    /// byte what this stage did before. Results are written back BY INDEX, so
    /// the output row order never depends on which request finishes first.
    pub concurrency: usize,
    /// #258: retries for a single request on HTTP 429 and 5xx, honouring
    /// Retry-After. A rate limit at row 400,000 must not discard the 399,999
    /// rows already paid for.
    pub max_retries: u32,
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
    /// When true, one module instance is reused across all rows (faster, but
    /// linear memory persists between rows). Default false gives a fresh
    /// instance per row so module state cannot leak - safer for untrusted
    /// modules.
    pub reuse_instance: bool,
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

/// xf.jq: apply a jq filter to a JSON column per row (GitHub #173). The filter
/// is compiled once with the pure-Rust `jaq` engine (no C libjq, no subprocess)
/// and evaluated against each row's `column` value. Row count is preserved 1:1:
/// the filter's output stream is folded into the `output_column` as a single
/// value when it yields one result, a JSON array when it yields several, and
/// null when it yields none. On a parse/eval error `on_error` decides whether
/// the stage fails or the row's output is null.
#[derive(Debug, Clone)]
pub struct JqSpec {
    pub node_id: String,
    pub from_view: String,
    pub column: String,
    pub filter: String,
    pub output_column: String,
    /// "fail" (default) aborts the stage on a bad row; "null" emits null instead.
    pub on_error: String,
}

/// code.python: per-row transform via a real Python 3 interpreter (shelled out,
/// so the user gets the full language + installed packages). The script defines
/// `process(row)` returning a dict (the output row); returning None drops the
/// row. The engine passes rows in/out as JSON, so it carries no Python runtime.
#[derive(Debug, Clone)]
pub struct PythonSpec {
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
    /// #142: custom request headers (gateway auth etc.) applied before defaults.
    pub headers: Vec<(String, String)>,
    /// #142: override the request path (default `/v1/chat/completions`).
    pub endpoint_path: Option<String>,
    /// #258: at most this many requests in flight. 1 = sequential, byte for
    /// byte what this stage did before. Results are written back BY INDEX, so
    /// the output row order never depends on which request finishes first.
    pub concurrency: usize,
    /// #258: retries for a single request on HTTP 429 and 5xx, honouring
    /// Retry-After. A rate limit at row 400,000 must not discard the 399,999
    /// rows already paid for.
    pub max_retries: u32,
    /// #258: OpenAI `max_tokens`. The GUI has offered this field since #142
    /// while the request body never carried it, so an unbounded reply was
    /// billed on every row. None = send no max_tokens, exactly as before.
    pub max_tokens: Option<u32>,
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
    /// #142: custom request headers (gateway auth etc.) applied before defaults.
    pub headers: Vec<(String, String)>,
    /// #142: override the request path (default `/v1/chat/completions`).
    pub endpoint_path: Option<String>,
    /// #258: at most this many requests in flight. 1 = sequential, byte for
    /// byte what this stage did before. Results are written back BY INDEX, so
    /// the output row order never depends on which request finishes first.
    pub concurrency: usize,
    /// #258: retries for a single request on HTTP 429 and 5xx, honouring
    /// Retry-After. A rate limit at row 400,000 must not discard the 399,999
    /// rows already paid for.
    pub max_retries: u32,
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
    /// Write mode: "append"/"overwrite" (create + insert) or "truncate"
    /// (TRUNCATE TABLE then insert). "upsert" is driven by upsert_keys.
    pub mode: String,
    /// If true, skip TLS cert verification - useful for self-signed
    /// dev servers. Production users leave this off.
    pub trust_cert: bool,
    /// If false, disable TLS entirely (tiberius EncryptionLevel::NotSupported)
    /// for legacy servers that cannot negotiate TLS 1.2+. Defaults to true.
    pub encrypt: bool,
    /// Non-empty in "upsert" write mode: the key columns to MERGE on.
    /// Empty means plain INSERT (append / create).
    pub upsert_keys: Vec<String>,
    /// Upsert delete propagation (see SnowflakeSinkSpec). None disables it.
    pub delete_column: Option<String>,
    pub delete_value: String,
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
    /// If false, disable TLS entirely (tiberius EncryptionLevel::NotSupported)
    /// for legacy servers (e.g. SQL Server 2014 and older) that cannot
    /// negotiate the TLS 1.2+ that rustls requires. Defaults to true.
    pub encrypt: bool,
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
    /// then insert; 'upsert' = replace_one(upsert) keyed on `upsert_keys`.
    pub mode: String,
    pub batch_size: usize,
    /// Non-empty in "upsert" mode: the document fields that form the match
    /// filter for replace_one(upsert=true). Empty falls back to insert.
    pub upsert_keys: Vec<String>,
    /// Upsert delete propagation: documents whose `delete_column` equals
    /// `delete_value` are delete_one'd (matched by key) instead of upserted.
    pub delete_column: Option<String>,
    pub delete_value: String,
}

/// snk.huggingface: push the upstream (materialized to a local Parquet)
/// to a Hugging Face Hub dataset repo over plain HTTP. DuckDB's hf:// is
/// read-only, so the write cannot go through SQL; the executor runs the
/// Hub API flow (create repo, preupload, git-LFS batch + PUT, NDJSON
/// commit). A write-scoped token is required.
#[derive(Debug, Clone)]
pub struct HuggingFaceSinkSpec {
    pub from_view: String,
    /// Bare dataset id "user/dataset" (no hf:// or datasets/ prefix).
    pub repo: String,
    /// Path of the file inside the repo, e.g. "data/train.parquet".
    pub path: String,
    /// Branch to commit to (default "main").
    pub revision: String,
    /// Write-scoped Hugging Face token. Redacted in exported SQL.
    pub token: String,
    /// Create the repo private if it does not exist yet.
    pub private: bool,
    pub commit_message: String,
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
    /// Optional aggregation pipeline as a JSON array of stages, e.g.
    /// [{"$match":...},{"$lookup":...},{"$group":...}] (#106). When set, the
    /// source runs aggregate() instead of find(); filter/projection/limit are
    /// ignored. Enables $lookup cross-collection joins and server-side grouping.
    pub pipeline: Option<String>,
}

/// src.pixeltable: read a Pixeltable table (#223).
///
/// Pixeltable is a Python library with no Rust client and no wire protocol, so
/// the exchange runs through Parquet, the one format both sides already do
/// well: a short Python program calls `pxt.io.export_parquet` and the engine
/// ingests the file with `read_parquet`. Same shape as the Lance sidecar above,
/// with `python` in place of the sidecar binary.
#[derive(Debug, Clone)]
pub struct PixeltableSourceSpec {
    pub node_id: String,
    /// Pixeltable table path, e.g. `myapp.media` or a versioned `myapp.media:3`.
    pub table: String,
    /// Optional `where` expression evaluated by Pixeltable, e.g. `t.score > 0.8`.
    pub filter: Option<String>,
    /// Columns to export; empty means all of them.
    pub columns: Vec<String>,
    pub limit: Option<i64>,
}

/// snk.pixeltable: insert the upstream rows into a Pixeltable table (#223).
///
/// The engine COPYs the upstream view to a Parquet temp file and Pixeltable
/// reads it directly, since `Table.insert` accepts a Parquet path. Nothing is
/// serialised row by row across the process boundary.
#[derive(Debug, Clone)]
pub struct PixeltableSinkSpec {
    pub from_view: String,
    pub table: String,
    /// `insert` into an existing table, or `create` it from the incoming rows.
    pub mode: String,
}

/// src.lancedb: read a Lance table via the duckle-lance sidecar (which owns the
/// lancedb crate); the sidecar writes a Parquet file the engine ingests through
/// read_parquet, so lancedb's deps never enter the engine.
#[derive(Debug, Clone)]
pub struct LanceSourceSpec {
    pub node_id: String,
    /// Dataset URI: a local dir, db:// (LanceDB Cloud), or s3:// / gs:// / az://.
    pub uri: String,
    pub table: String,
    pub api_key: Option<String>,
    pub region: Option<String>,
    pub limit: Option<i64>,
}

/// snk.lancedb: write the upstream rows to a Lance table via the sidecar (the
/// engine COPYs the upstream view to a Parquet temp file the sidecar reads).
#[derive(Debug, Clone)]
pub struct LanceSinkSpec {
    pub from_view: String,
    pub uri: String,
    pub table: String,
    /// "create" (overwrite the table) or "append".
    pub mode: String,
    pub api_key: Option<String>,
    pub region: Option<String>,
}

/// src.vortex: read a Vortex file (#111) via the duckle-lance sidecar, which owns
/// the vortex crate and bridges through a Parquet temp file the engine ingests.
#[derive(Debug, Clone)]
pub struct VortexSourceSpec {
    pub node_id: String,
    /// Path to the .vortex file.
    pub path: String,
}

/// snk.vortex: write the upstream rows to a Vortex file via the sidecar (the
/// engine COPYs the upstream view to a Parquet temp file the sidecar reads).
#[derive(Debug, Clone)]
pub struct VortexSinkSpec {
    pub from_view: String,
    pub path: String,
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
    /// page returns fewer than `page_size` rows. When `total_path` is set
    /// (a JSON pointer to a total-row count in the body, e.g. Redmine's
    /// `/total_count`), also stop once `offset + page_size >= total`, since
    /// such APIs return HTTP 200 with an empty array past the end and the
    /// status code cannot signal the end (issue #41).
    Offset { offset_param: String, page_size: u64, total_path: Option<String> },
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
    /// #256: per-node transport (proxy, timeouts, User-Agent), usually filled
    /// from a saved `http` connection. None uses the shared default agent.
    pub transport: Option<crate::tls::HttpTransport>,
    pub node_id: String,
    /// #257: the upstream relation whose rows each drive one request. None is
    /// exactly one endpoint, which is every pipeline that existed before.
    pub from_view: Option<String>,
    /// #257: a URL carrying `{column}` placeholders resolved from each upstream
    /// row, so a parent endpoint can feed a child endpoint
    /// (`/companies` then `/companies/{id}/officers`). The same `{column}`
    /// syntax xf.ai.llm prompts use, deliberately not `${...}`, which belongs
    /// to run variables and workspace context and is resolved before this.
    pub url_template: Option<String>,
    /// #257: an upstream column stamped onto every row the child returns, so
    /// child rows can be joined back to the parent that produced them.
    pub parent_key_column: Option<String>,
    /// #257: cap on how many upstream rows may each fire a request, so a
    /// careless upstream cannot turn into an unbounded request storm. Only
    /// applies when fanning out.
    pub max_requests: u64,
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
    /// Stamp each row with where it came from: the exact URL, the status the
    /// server answered with, and when it was fetched.
    ///
    /// Parsed rows on their own cannot answer "did this change because the
    /// source changed or because the parser did", and an API that quietly
    /// starts paginating differently looks identical downstream.
    pub response_metadata: bool,
    /// #166: when set (src.salesforce with OAuth client-credentials auth), the
    /// runner mints a fresh access token per run and injects
    /// `Authorization: Bearer <token>` before the request loop, overriding any
    /// static auth header. None for every other REST source.
    pub oauth: Option<RestOAuth>,
    /// #170: the node's declared output schema, when the user declared one. Used
    /// ONLY to type an EMPTY result set (a REST / SaaS query that matched no
    /// records) so downstream SQL sees the real columns instead of a single
    /// `json` column. None -> an empty result is a clear source-level error.
    pub declared_schema: Option<Vec<duckle_metadata::Column>>,
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
    /// Non-empty in "upsert" write mode: the key columns to MERGE on.
    pub upsert_keys: Vec<String>,
    /// Upsert delete propagation (see SnowflakeSinkSpec). None disables it.
    pub delete_column: Option<String>,
    pub delete_value: String,
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
    /// Plain-text / raw body (issue #147): when body_shape == "text" each row is
    /// rendered through this template (`${column}` placeholders) and the rows
    /// are newline-joined into one request body, sent as text/plain unless the
    /// user set a Content-Type header. Enables InfluxDB Line Protocol writes
    /// (QuestDB /write, InfluxDB) and other line-oriented endpoints. None for
    /// the JSON / form / bulk shapes.
    pub text_template: Option<String>,
}

/// Which Salesforce write API a SalesforceSinkSpec uses. Bulk API 2.0 is not a
/// variant here: its config diverges far enough (job polling, a 100MB upload
/// cap, hardDelete, no allOrNone) that it is its own node, snk.salesforce.bulk
/// / SalesforceBulkSinkSpec, rather than a mode of this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SalesforceWriteApi {
    /// sObject Collections / Composite: up to 200 records per request via a
    /// single synchronous round-trip. Fits the existing ureq per-stage model.
    /// `/composite/sobjects` (insert/update/delete) and
    /// `/composite/sobjects/{sobject}/{extIdField}` (upsert).
    Collections,
}

/// How client credentials are presented to the token endpoint.
#[derive(Debug, Clone, PartialEq)]
pub enum OAuthClientAuth {
    /// `client_id` / `client_secret` as form fields in the POST body. This is
    /// what Salesforce expects and stays the default.
    Body,
    /// HTTP Basic `Authorization` header. Xero's identity service and many
    /// OIDC providers require this form.
    Basic,
}

/// OAuth 2.0 client-credentials config for REST-shaped connectors.
///
/// Added for Salesforce (#166) and generalized in #195 to any REST source that
/// supplies its own token endpoint (e.g. a Xero Custom Connection). When
/// present on a source/sink spec the engine mints a fresh short-lived access
/// token per run by POSTing `grant_type=client_credentials` to `token_url`,
/// replacing a pre-minted Bearer token the user would otherwise re-paste.
///
/// `token_url` is the fully resolved endpoint: for Salesforce the builder
/// derives it as `{loginUrl}/services/oauth2/token`, and that response also
/// carries the `instance_url` the API calls target.
#[derive(Debug, Clone)]
pub struct RestOAuth {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    /// Optional `scope` form field, required by some providers.
    pub scope: Option<String>,
    pub client_auth: OAuthClientAuth,
}

/// snk.salesforce: write upstream rows into a Salesforce object via the REST
/// write APIs. Tier 1 targets the sObject Collections API (<=200 records per
/// request). Auth is a Bearer OAuth access token, same token flow as
/// src.salesforce; supply it via `${ENV:SF_TOKEN}` so no secret lands in the
/// pipeline JSON, or set `oauth` to mint a fresh token per run (#166).
/// `instance_url` is the org base (e.g. https://acme.my.salesforce.com) and
/// doubles as the endpoint override tests point at a mock server.
#[derive(Debug, Clone)]
pub struct SalesforceSinkSpec {
    pub from_view: String,
    /// Org base URL, e.g. https://acme.my.salesforce.com. No trailing slash.
    pub instance_url: String,
    /// REST API version segment, e.g. "v60.0".
    pub api_version: String,
    /// Bearer OAuth access token.
    pub access_token: String,
    /// sObject API name, e.g. "Account", "Contact", "MyObject__c".
    pub object: String,
    /// "insert" | "update" | "upsert" | "delete".
    pub operation: String,
    /// Required when operation == "upsert": the external-id field the upsert
    /// keys on (e.g. "External_Id__c"). Ignored otherwise.
    pub external_id_field: Option<String>,
    /// For operation == "update"/"delete": the row column holding the
    /// Salesforce record Id (default "Id").
    pub id_field: String,
    /// Records per request. Salesforce caps sObject Collections at 200; the
    /// planner clamps to that.
    pub batch_size: usize,
    /// allOrNone flag sent to Salesforce. When true, any failing record rolls
    /// back the whole request (Salesforce-side). When false, partial success.
    pub all_or_none: bool,
    /// When true, the stage errors if any record fails; when false it logs the
    /// per-record errors and continues. A first-class reject/error output
    /// stream is Tier 2 work (see IMPLEMENTATION.md).
    pub fail_on_error: bool,
    /// Which write API to use (Tier 1 = Collections).
    pub api: SalesforceWriteApi,
    /// When set, mint a fresh access token per run via OAuth client-credentials
    /// (#166) instead of using the static Bearer `access_token`. The minted
    /// `instance_url` from the token response overrides `instance_url` when the
    /// latter is empty.
    pub oauth: Option<RestOAuth>,
    /// When set, a directory that receives Data-Loader-style per-record result
    /// files after every run (#166), stamped with the job + run time so runs
    /// accumulate: `{object}_{operation}_{utc}_success.csv` (input columns +
    /// `sf__Id`) and `..._error.csv` (input columns + `sf__StatusCode` +
    /// `sf__Message`). Both files are always written - header-only when a side
    /// is empty - and they land even when the stage errors (failOnError or an
    /// HTTP failure), so the reject stream survives an aborted run. Records in
    /// chunks that were never attempted (a preceding chunk aborted the run)
    /// appear in neither file.
    pub results_path: Option<String>,
}

#[derive(Debug, Clone)]
/// snk.dhis2: import rows into a DHIS2 instance.
///
/// This is not expressible as a `snk.rest` config for two reasons, both of
/// which silently lose data rather than failing loudly:
///
///  * `snk.rest` serialises every upstream row into a single request body, so
///    a real import becomes one enormous POST.
///  * `snk.rest` discards the response body on success, and DHIS2 puts its
///    import summary there. DHIS2 answers HTTP 200 even when it rejected every
///    record, so a generic sink reports a green run having written nothing.
pub struct Dhis2SinkSpec {
    pub from_view: String,
    /// Full endpoint URL, e.g. https://play.dhis2.org/api/dataValueSets
    /// or https://play.dhis2.org/api/tracker.
    pub url: String,
    /// Pre-built Authorization header value, e.g. "ApiToken d2pat_..." or
    /// "Basic <base64>". Built by the planner from the shared REST auth props.
    pub auth_header: Option<(String, String)>,
    /// "aggregate" (POST /api/dataValueSets) or "tracker" (POST /api/tracker).
    /// These have completely different payload wrappers AND completely
    /// different response schemas, so the parser branches on this.
    pub import_type: String,
    /// Tracker only: the collection key the rows are wrapped in, one of
    /// trackedEntities / events / enrollments / relationships. DHIS2 rejects a
    /// bare array, and the key must match the resource type.
    pub tracker_resource: String,
    /// CREATE | UPDATE | CREATE_AND_UPDATE | DELETE. DHIS2 has no separate
    /// upsert flag: CREATE_AND_UPDATE *is* the upsert, and is its own default.
    /// Sent explicitly because the published tracker docs claim a default of
    /// CREATE while the source says CREATE_AND_UPDATE.
    pub import_strategy: String,
    /// Rows per request. Chunking is the whole reason this sink exists.
    pub chunk_size: usize,
    /// Sends dryRun=true (aggregate) / importMode=VALIDATE (tracker), so DHIS2
    /// validates and reports without committing.
    pub dry_run: bool,
    /// Tracker atomicMode: "ALL" rolls the whole request back on any error,
    /// "OBJECT" commits what it can. Relevant to retry safety: a 409 under
    /// OBJECT means part of the data landed.
    pub atomic_mode: String,
    /// When true (default) any conflict, error report, or non-zero `ignored`
    /// count fails the stage. When false they are reported and the run
    /// continues, which is only safe if something downstream reconciles.
    pub fail_on_conflict: bool,
}

/// snk.salesforce.bulk: write upstream rows into a Salesforce object via Bulk
/// API 2.0 - the migration-scale path, where snk.salesforce (sObject
/// Collections, <=200 records per round-trip) would mean one HTTP call per 200
/// rows. Bulk trades latency for throughput: DuckDB COPYs the upstream view
/// straight to CSV on disk, and each part is uploaded as an async job
/// (create -> upload -> UploadComplete -> poll -> fetch result sets), so a
/// multi-GB load never lands in memory.
///
/// Auth is identical to snk.salesforce (Bearer token or `oauth` client
/// credentials minted per run), which is what keeps org-A-read ->
/// org-B-write working across both node families.
#[derive(Debug, Clone)]
pub struct SalesforceBulkSinkSpec {
    pub from_view: String,
    /// Org base URL, e.g. https://acme.my.salesforce.com. No trailing slash.
    pub instance_url: String,
    /// REST API version segment, e.g. "v60.0".
    pub api_version: String,
    /// Bearer OAuth access token.
    pub access_token: String,
    /// sObject API name, e.g. "Account", "Contact", "MyObject__c".
    pub object: String,
    /// "insert" | "update" | "upsert" | "delete" | "hardDelete". hardDelete is
    /// Bulk-only (it bypasses the Recycle Bin) and needs the "Bulk API Hard
    /// Delete" user permission.
    pub operation: String,
    /// Required when operation == "upsert": the external-id field the upsert
    /// keys on (e.g. "External_Id__c"). Sent as `externalIdFieldName`.
    pub external_id_field: Option<String>,
    /// Upstream column holding the Salesforce record Id (default "Id"). Used only
    /// for delete / hardDelete, where Bulk API 2.0 requires a CSV of exactly one
    /// column named `Id`; the sink projects this column aliased to `Id`.
    pub id_field: String,
    /// Optional assignment rule Id applied to Case / Lead inserts.
    pub assignment_rule_id: Option<String>,
    /// Seconds between job-status polls.
    pub poll_interval_secs: u64,
    /// Give up after this many seconds waiting for a job to reach a terminal
    /// state; the in-flight job is aborted and the run fails naming the job Id.
    /// Bulk jobs legitimately run for minutes to hours, so this is the only
    /// thing standing between a stuck job and a pipeline that hangs forever.
    pub timeout_secs: u64,
    /// When true, the stage errors if any record failed.
    pub fail_on_error: bool,
    /// When set, mint a fresh access token per run via OAuth client-credentials
    /// (#166) instead of using the static Bearer `access_token`.
    pub oauth: Option<RestOAuth>,
    /// When set, a directory receiving the result sets Salesforce returns for
    /// each job, stamped like snk.salesforce's (#166):
    /// `{object}_{operation}_{utc}_success.csv`, `..._error.csv` and - Bulk
    /// only - `..._unprocessed.csv` for records a failed job never reached.
    /// Salesforce returns these already CSV-shaped (input columns plus `sf__Id`
    /// / `sf__Error`), so they are streamed to disk verbatim rather than
    /// re-serialised.
    pub results_path: Option<String>,
}

/// src.salesforce.bulk: read a SOQL result set via a Bulk API 2.0 query job -
/// the migration-scale read, where src.salesforce (the REST query endpoint,
/// ~2000 records per page) would mean thousands of paginated round-trips.
/// The async lifecycle mirrors the Bulk sink's: create query job -> poll to
/// JobComplete -> walk the paged CSV result sets to a temp file -> DuckDB
/// read_csv materializes it, so a multi-GB result never lands in memory.
///
/// Auth is identical to snk.salesforce.bulk (sink-shaped keys: authMode /
/// instanceUrl / accessToken, or `oauth` client credentials minted per run).
#[derive(Debug, Clone)]
pub struct SalesforceBulkSourceSpec {
    pub node_id: String,
    /// Org base URL, e.g. https://acme.my.salesforce.com. No trailing slash.
    pub instance_url: String,
    /// REST API version segment, e.g. "v60.0".
    pub api_version: String,
    /// Bearer OAuth access token.
    pub access_token: String,
    /// The SOQL statement. Bulk 2.0 queries don't support GROUP BY / OFFSET /
    /// TYPEOF / aggregates / parent-to-child subqueries; compound fields must
    /// be queried by component. Salesforce rejects those at job creation.
    pub query: String,
    /// "query" (non-deleted rows) | "queryAll" (includes deleted/archived).
    pub operation: String,
    /// Seconds between job-status polls.
    pub poll_interval_secs: u64,
    /// Abort the query job and fail the run after this many seconds.
    pub timeout_secs: u64,
    /// Optional page size for result fetches (the `maxRecords` query param);
    /// None lets the server pick. Pages are walked via the Sforce-Locator
    /// header either way, so this only tunes round-trip granularity.
    pub max_records: Option<u64>,
    /// When set, mint a fresh access token per run via OAuth client-credentials
    /// (#166) instead of using the static Bearer `access_token`.
    pub oauth: Option<RestOAuth>,
    /// The node's declared schema. A 0-record query materializes as a typed
    /// empty relation from these columns (the #170 contract); with rows they
    /// pin the output columns/types instead of leaving both to CSV inference.
    pub declared_schema: Option<Vec<duckle_metadata::Column>>,
}

/// snk.execsource "Execute in Source" (#115 in-database processing v1b): run a
/// statement (typically CREATE TABLE ... AS SELECT) directly on the attached
/// remote server via the scanner extension's passthrough (postgres_execute /
/// mysql_execute), so the transform runs in-database with no round-trip through
/// DuckDB. `attach` is the LOAD + ATTACH prelude binding the server as
/// duckle_dst; `exec_fn` is postgres_execute / mysql_execute; `statement` is
/// the SQL run on the remote, one CALL each (the mysql extension rejects a
/// multi-statement batch, so DROP + CREATE arrive as separate statements).
#[derive(Debug, Clone)]
pub struct RemoteExecSpec {
    pub attach: String,
    pub exec_fn: String,
    pub statements: Vec<String>,
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
    /// Raw (unquoted) target schema + table. `target` is pre-quoted with
    /// DuckDB's double-quote convention, which is correct for Postgres but
    /// wrong for MySQL (backticks); the native-SQL builder re-quotes per
    /// family from these raw names.
    pub raw_schema: Option<String>,
    pub raw_table: String,
    /// Columns the user declared as the conflict key.
    pub conflict_cols: Vec<String>,
    /// Upsert delete propagation: rows whose `delete_column` equals
    /// `delete_value` are DELETEd from the target by key and excluded from
    /// the INSERT. None keeps the plain ON CONFLICT / ON DUPLICATE KEY path.
    pub delete_column: Option<String>,
    pub delete_value: String,
}

#[derive(Debug, Clone, Copy)]
pub enum UpsertFamily {
    /// `ON CONFLICT (key) DO UPDATE SET col = EXCLUDED.col` (Postgres, Cockroach).
    Postgres,
    /// `ON DUPLICATE KEY UPDATE col = VALUES(col)` (MySQL, MariaDB).
    MySql,
}

/// src.neo4j: Cypher read over the HTTP Query API.
///   POST {endpoint}/db/{database}/query/v2
///   Body:     { "statement": "MATCH ...", "parameters": {...} }
///   Response: { "data": { "fields": [...], "values": [[...]] } }
#[derive(Debug, Clone)]
pub struct Neo4jSourceSpec {
    pub node_id: String,
    pub endpoint: String,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub cypher: String,
    /// Optional Cypher parameters, passed through as `$name` bindings.
    pub parameters: Option<serde_json::Value>,
}

/// snk.neo4j: write rows as nodes over the same Query API. Rows go up as the
/// `$rows` parameter and are expanded server side with UNWIND.
#[derive(Debug, Clone)]
pub struct Neo4jSinkSpec {
    pub from_view: String,
    pub endpoint: String,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Node label to write.
    pub label: String,
    /// When non-empty, MERGE on these properties instead of CREATE, so a
    /// re-run updates the matched nodes rather than duplicating them.
    pub merge_keys: Vec<String>,
    /// Full override: a Cypher statement that consumes `$rows` itself.
    pub cypher: Option<String>,
    pub batch_size: usize,
}

/// src.turso: SQL read over the libSQL HTTP pipeline API.
///   POST {url}/v2/pipeline
///   Body:     { "requests": [ {"type":"execute","stmt":{"sql":...}}, {"type":"close"} ] }
///   Response: { "results": [ { "response": { "result": { "cols":[], "rows":[[]] } } } ] }
#[derive(Debug, Clone)]
pub struct TursoSourceSpec {
    pub node_id: String,
    /// The database URL. `libsql://` is accepted and normalized to https.
    pub url: String,
    pub auth_token: Option<String>,
    pub query: String,
}

/// snk.turso: INSERT rows over the libSQL HTTP pipeline API.
#[derive(Debug, Clone)]
pub struct TursoSinkSpec {
    pub from_view: String,
    pub url: String,
    pub auth_token: Option<String>,
    pub table: String,
    /// "append" (default) or "overwrite", which clears the table first.
    pub mode: String,
    pub batch_size: usize,
}

/// src.db2: IBM DB2 read over the IBM Data Server ODBC driver. Same transport
/// as Teradata - DB2 ships no DuckDB extension and no native Rust driver.
#[derive(Debug, Clone)]
pub struct Db2SourceSpec {
    pub node_id: String,
    pub conn_str: String,
    pub query: String,
    pub batch_rows: usize,
}

/// snk.db2: auto-create the target table from the upstream column types, then
/// INSERT over ODBC.
#[derive(Debug, Clone)]
pub struct Db2SinkSpec {
    pub from_view: String,
    pub conn_str: String,
    /// Optional schema qualifier; DB2 defaults to the connecting user's schema.
    pub schema: Option<String>,
    pub table: String,
    /// "append" (default) or "overwrite", which clears the table first.
    pub mode: String,
}

/// src.spool: tail an append-only NDJSON file from where the last successful
/// run stopped.
///
/// The half of push-source support that makes it lossless. A webhook or
/// WebSocket listener that lives inside a pipeline run can only collect while
/// that run is executing - between runs the port is closed and arriving
/// requests are refused. `duckle-runner listen` keeps the listener up and
/// appends here instead, so arrival is decoupled from processing and a batch
/// boundary costs nothing.
///
/// Position is a BYTE offset, which works because the file is append-only:
/// there is no reader/writer race to lose to, and nothing has to be deleted.
#[derive(Debug, Clone)]
pub struct SpoolSourceSpec {
    pub node_id: String,
    pub path: String,
    /// Remember where this run stopped, so the next one resumes there.
    pub track_offset: bool,
    /// Most bytes to take in one pass. Bounds a batch when a listener has been
    /// running unattended for a long time, so the first run after a backlog
    /// does not try to materialize the whole thing at once.
    pub max_bytes: u64,
}

/// xf.tumble: event-time tumbling windows that survive across runs.
///
/// Rows are assigned to fixed-size buckets by their event time, held until the
/// bucket CLOSES, then emitted. Closing is decided by a watermark - the
/// greatest event time seen so far, across runs - rather than by wall clock,
/// so replaying yesterday's data produces yesterday's windows instead of
/// closing all of them at once.
///
/// The state that has to survive between runs is the rows in windows that are
/// still open, plus the watermark. Both are swapped in through the deferred
/// flush, so a run that fails downstream leaves the previous state in place
/// and its rows are re-processed rather than lost. That is the same guarantee
/// every source position gets, and it is the part SQLFlow's equivalent does
/// not have: there, a collect and a delete are separate lock acquisitions
/// around a sink write, each evaluating `now()` on its own, so a window can be
/// deleted after being collected but before being written.
#[derive(Debug, Clone)]
pub struct TumbleSpec {
    pub node_id: String,
    pub from_view: String,
    /// The event-time column. Windows are cut on this, never on arrival time.
    pub time_column: String,
    /// Window size as a DuckDB interval, e.g. `1 hour`, `5 minutes`.
    pub size: String,
    /// How far past a window's end the watermark must reach before the window
    /// closes. Buys time for out-of-order arrivals at the cost of latency.
    pub allowed_lateness: String,
}

/// src.changed: poll a remote source's METADATA and emit only what changed.
///
/// Two patterns, one component, because they are the same question asked of a
/// different number of objects:
///
/// - **object**: one URI replaced periodically. Emits one row when its
///   fingerprint differs from the last successfully processed one, and nothing
///   when it does not.
/// - **listing**: a directory or prefix of immutable files. Lists it, compares
///   each entry against what has been processed, and emits the new and changed
///   ones as ordinary rows for a ForEach or an artifact copy downstream.
///
/// The point is not to pay for the object to find out whether it was needed.
/// A HEAD or a stat is cheap; a 30 GB download is not.
///
/// Fingerprints are conservative by design. None of the signals are
/// guarantees: ETag can be absent, can weaken under compression, and on S3 is
/// a digest-of-digests for a multipart upload rather than the object's hash;
/// Last-Modified has one-second resolution; SFTP realistically offers mtime
/// and size. So a missing or unreadable signal reads as CHANGED. Re-reading
/// something unnecessarily costs compute; skipping something that did change
/// loses data.
#[derive(Debug, Clone)]
pub struct ChangedSourceSpec {
    pub node_id: String,
    /// `https://...` or `sftp://[user@]host[:port]/path`.
    pub uri: String,
    /// True to list a directory/prefix rather than probe one object.
    pub listing: bool,
    /// Only list entries whose name ends with this (listing mode).
    pub suffix: Option<String>,
    /// Most entries to emit in one run, so a first run against a directory
    /// with years of drops does not try to process all of it at once.
    pub max_entries: usize,
    /// Remember what was processed, so the next run only sees what is new.
    /// Off means every run treats everything as changed.
    pub track_state: bool,
    // SFTP auth, ignored for https.
    pub user: Option<String>,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub key_passphrase: Option<String>,
    pub host_fingerprint: Option<String>,
    /// Extra request headers for https (an API key on a metadata endpoint).
    pub headers: Vec<(String, String)>,
    /// Credentials for an `s3://` uri. None for every other scheme, and also
    /// for an S3 URI with no credentials on the node - which is an error worth
    /// reporting rather than an anonymous request that 403s.
    pub s3: Option<crate::s3::S3Config>,
}

/// How to reach an artifact, whatever scheme its URI is written in.
///
/// #282: the artifact boundary is only composable if every parser reaches a URI
/// the SAME way. Giving `src.pdf`, `src.xml` and `src.html` each their own
/// credential fields would produce three conventions that agree until the day
/// one of them does not, so the auth lives here once and each of them holds one.
#[derive(Debug, Clone, Default)]
pub struct ArtifactAuth {
    /// Credentials for `s3://`.
    pub s3: Option<crate::s3::S3Config>,
    /// Extra request headers for `https://`.
    pub headers: Vec<(String, String)>,
    // SFTP.
    pub user: Option<String>,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub key_passphrase: Option<String>,
    pub host_fingerprint: Option<String>,
}

/// A parser's optional artifact input: the upstream relation naming what to
/// read, and which of its columns holds the URI.
///
/// Absent means the node reads its configured path, exactly as it always has -
/// every existing pipeline keeps working, which is the only acceptable way to
/// add this.
#[derive(Debug, Clone)]
pub struct ArtifactInput {
    /// The upstream relation. None when the node has no input wired.
    pub from_view: Option<String>,
    /// Column holding each artifact's URI. `uri` by default, which is what
    /// `src.changed`, `src.artifact` and `xf.artifact.copy` all emit.
    pub uri_column: String,
    /// Column holding the hash of those bytes, carried through to the parsed
    /// rows. Not recomputed: the copy that landed the artifact already hashed
    /// exactly those bytes, and re-hashing would both cost a second full read
    /// and produce the hash of whatever is at that URI NOW.
    pub sha_column: String,
    pub auth: ArtifactAuth,
}

impl Default for ArtifactInput {
    fn default() -> Self {
        ArtifactInput {
            from_view: None,
            uri_column: "uri".into(),
            sha_column: "sha256".into(),
            auth: ArtifactAuth::default(),
        }
    }
}

/// `xf.archive.extract`: turn one archive artifact into one artifact per member.
///
/// #284: bulk data is published as archives far more often than as readable
/// files, and unpacking one used to mean a shell stage. As an ARTIFACT
/// operation rather than something built into each parser, a ZIP of CSVs, a TAR
/// of JSON and a GZIP of NDJSON all land the same way and each member then
/// flows into whichever parser suits it.
#[derive(Debug, Clone)]
pub struct ArchiveExtractSpec {
    pub node_id: String,
    /// The archives to open, named by an upstream relation.
    pub input: ArtifactInput,
    /// Where members land: an `s3://` prefix or a local directory.
    pub destination: String,
    /// "preserve" the member's path inside the archive, "flat" for its file
    /// name only, or "hash" for a content-addressed name.
    pub naming: String,
    /// "skip" (the default), "replace" or "error" when a member is already at
    /// the destination.
    pub if_exists: String,
    pub part_size_bytes: usize,
    /// Only extract members matching one of these globs. Empty means all.
    pub include: Vec<String>,
    /// Never extract members matching one of these, applied after `include`.
    pub exclude: Vec<String>,
    /// Most members to take out of one archive.
    pub max_members: usize,
    /// Refuse an archive that expands past this. A ZIP is a compression format,
    /// so a small one can expand to fill a disk - an archive from an external
    /// publisher is untrusted input and this is the bound that says so.
    pub max_uncompressed_bytes: u64,
    /// What to do with an archive that cannot be opened: "fail" or "skip".
    pub on_error: String,
}

/// `src.ducklake.maintain`: run one of DuckLake's own maintenance operations
/// and emit what it did as an ordinary relation.
///
/// #279 asks for a THIN surface over what the installed DuckLake supports,
/// rather than a lakehouse optimiser of our own. So every operation here is one
/// DuckLake function, its options are that function's options, and its output
/// is that function's own result rows - which means a quality gate or an alert
/// can read a compaction the same way it reads anything else.
#[derive(Debug, Clone)]
pub struct DuckLakeMaintainSpec {
    pub node_id: String,
    /// The ATTACH prelude for the catalog, built the same way every other
    /// DuckLake node builds it, so one saved lake is described once.
    pub attach: String,
    /// For the message and the lock: which catalog this is.
    pub catalog_path: String,
    /// compact | rewrite | expireSnapshots | cleanupFiles | deleteOrphans |
    /// flushInlined | stats
    pub operation: String,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    /// Only meaningful where DuckLake offers it: expireSnapshots,
    /// cleanupFiles, deleteOrphans. Elsewhere it is refused rather than
    /// ignored, because a dry run that silently deleted things is the worst
    /// possible outcome for this component.
    pub dry_run: bool,
    /// The retention boundary. DuckLake expires NOTHING without one, which is
    /// the right default and is surfaced rather than replaced.
    pub older_than: Option<String>,
    pub versions: Option<String>,
    pub cleanup_all: bool,
    pub min_file_size: Option<u64>,
    pub max_file_size: Option<u64>,
    pub max_compacted_files: Option<u64>,
    pub delete_threshold: Option<f64>,
}

/// `xf.artifact.copy`: take artifact rows in, land the BYTES somewhere durable,
/// and emit a row per landed copy.
///
/// This is the piece that turns a change feed into a raw zone. The bytes are
/// streamed, never held: the whole point of an artifact being a reference is
/// that a 40GB model file does not become 40GB of memory on the way past.
#[derive(Debug, Clone)]
pub struct ArtifactCopySpec {
    pub node_id: String,
    /// The relation whose rows name the artifacts to copy.
    pub from_view: String,
    /// Column holding the source URI. Defaults to `uri`, which is what
    /// `src.changed` and `src.artifact` both emit.
    pub uri_column: String,
    /// Where the copies land: `s3://bucket/prefix/` or a local directory.
    pub destination: String,
    /// "keep" the source's file name, "hash" for a content-addressed name, or
    /// "path" to preserve the source's directory structure under the prefix.
    pub naming: String,
    /// What to do when the destination key already holds something: "skip"
    /// (the default, and the right one for an immutable raw zone), "replace",
    /// or "error".
    pub if_exists: String,
    /// Bytes per multipart part when writing to S3. Also the ceiling on memory
    /// used per object, which is why it is a knob at all.
    pub part_size_bytes: usize,
    /// Credentials for whichever side is `s3://`, and for the other schemes.
    pub auth: ArtifactAuth,
}
