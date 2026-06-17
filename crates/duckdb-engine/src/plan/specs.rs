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
