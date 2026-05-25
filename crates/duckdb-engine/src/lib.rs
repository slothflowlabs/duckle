//! Duckle DuckDB engine adapter - CLI-driven.
//!
//! Rather than statically linking libduckdb (which bloats the binary to
//! tens of MB and makes builds glacial), this drives the official DuckDB
//! **CLI** that Duckle downloads into the app-data dir on first launch.
//! The engine shells out to `duckdb -json -c "<sql>"` and parses the
//! JSON it prints. SQL generation lives in `plan.rs` and is unchanged;
//! only execution + inspection talk to the CLI here.
//!
//! Execution model: a temp on-disk `.duckdb` file. Each non-sink stage
//! materializes a `CREATE OR REPLACE TABLE` (so it persists across the
//! separate CLI invocations); sinks `COPY` from the upstream table.
//! Cancellation kills the in-flight child process.

use duckle_metadata::{Column, DataType};
use duckle_plugin_sdk::{Inspection, InspectError};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

pub mod history;
pub mod plan;
pub use history::{append_run_record, load_run_history, RunRecord};
pub use plan::{CompiledPipeline, PipelineDoc, Stage, StageKind};
use plan::{
    AvroSinkSpec, AvroSourceSpec, CassandraSinkSpec, CassandraSourceSpec, ClickHouseSinkSpec,
    ClickHouseSourceSpec, DatabricksSinkSpec, DatabricksSourceSpec, ElasticSourceSpec,
    FormatFileSinkSpec, FormatFileSourceSpec, FormatKind, GitSourceSpec, KafkaSinkSpec,
    KafkaSourceSpec, MilvusSourceSpec, MongoSinkSpec, MongoSourceSpec, NatsSinkSpec,
    NatsSourceSpec, OracleSinkSpec, OracleSourceSpec, PubSubSinkSpec, PubSubSourceSpec,
    QdrantSourceSpec, RabbitSinkSpec, RabbitSourceSpec, RedisSinkSpec, RedisSourceSpec,
    RestPagination, RestSourceSpec, ShellSpec, SnowflakeAuth, SnowflakeSinkSpec,
    SnowflakeSourceSpec, SqlServerSinkSpec, SqlServerSourceSpec, WeaviateSourceSpec, WebhookSpec,
    XmlSinkSpec, XmlSourceSpec,
};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("config: {0}")]
    Config(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("query: {0}")]
    Query(String),
    #[error("cancelled")]
    Cancelled,
    #[error("{0}")]
    Other(String),
}

impl From<EngineError> for InspectError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::Config(m) => InspectError::Config(m),
            EngineError::Unsupported(m) => InspectError::Unsupported(m),
            other => InspectError::Other(other.to_string()),
        }
    }
}

/// Rows sampled alongside the schema for the Preview tab.
const PREVIEW_LIMIT: usize = 8;
/// Rows captured per stage during a run (shown in the node Preview tab).
const PREVIEW_ROW_LIMIT: usize = 100;

/// Drives the downloaded DuckDB CLI. Cheap to clone; holds only the
/// binary path and a shared cancel flag.
#[derive(Clone)]
pub struct DuckdbEngine {
    bin: PathBuf,
    cancel: Arc<AtomicBool>,
}

impl std::fmt::Debug for DuckdbEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckdbEngine")
            .field("bin", &self.bin)
            .finish()
    }
}

impl DuckdbEngine {
    /// Construct an engine pointing at a DuckDB CLI binary. The binary
    /// need not exist yet - calls fail with a clear error if it's
    /// missing, and the first-run setup installs it.
    pub fn new(bin: PathBuf) -> Self {
        Self {
            bin,
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn binary(&self) -> &Path {
        &self.bin
    }

    pub fn is_available(&self) -> bool {
        self.bin.exists()
    }

    /// Signal any in-flight run to stop. The polling loop in `run` sees
    /// the flag and kills the active CLI child, so even a long query
    /// returns promptly.
    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    pub fn clear_cancel(&self) {
        self.cancel.store(false, Ordering::Relaxed);
    }

    /// Returns Err(Cancelled) if a cancel has been requested. Used at
    /// the top of pagination / batch loops in source + sink runners so
    /// a long HTTP scan can be interrupted between pages rather than
    /// waiting for the whole walk to finish.
    fn check_cancelled(&self) -> Result<(), EngineError> {
        if self.cancel.load(Ordering::Relaxed) {
            Err(EngineError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Run SQL through the CLI against an optional db file. Returns raw
    /// stdout. Cancellation-aware: polls the child and kills it if a
    /// cancel was requested.
    fn run(&self, db: Option<&Path>, sql: &str, json: bool) -> Result<String, EngineError> {
        if !self.bin.exists() {
            return Err(EngineError::Config(format!(
                "DuckDB engine isn't installed (expected at {}). Open Setup to install it.",
                self.bin.display()
            )));
        }
        let mut cmd = std::process::Command::new(&self.bin);
        match db {
            Some(p) => {
                cmd.arg(p);
            }
            None => {
                cmd.arg(":memory:");
            }
        }
        if json {
            cmd.arg("-json");
        }
        cmd.arg("-bail").arg("-c").arg(sql);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // No console flash on Windows for the per-stage spawns.
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| EngineError::Other(format!("could not start duckdb: {}", e)))?;

        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    if self.cancel.load(Ordering::Relaxed) {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(EngineError::Cancelled);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(40));
                }
                Err(e) => return Err(EngineError::Other(e.to_string())),
            }
        }

        let out = child
            .wait_with_output()
            .map_err(|e| EngineError::Other(e.to_string()))?;
        if !out.status.success() {
            let mut msg = String::from_utf8_lossy(&out.stderr).trim().to_string();
            if msg.is_empty() {
                msg = String::from_utf8_lossy(&out.stdout).trim().to_string();
            }
            if msg.is_empty() {
                msg = "DuckDB CLI exited with an error".into();
            }
            return Err(EngineError::Query(msg));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Run SQL and return the first JSON array of rows it printed
    /// (DESCRIBE / SELECT produce one array; preludes produce none).
    fn run_rows(&self, db: Option<&Path>, sql: &str) -> Result<Vec<JsonValue>, EngineError> {
        let out = self.run(db, sql, true)?;
        Ok(parse_json_arrays(&out).into_iter().next().unwrap_or_default())
    }

    // ---- Inspection ----------------------------------------------------

    /// Inspect a source for its schema and a small preview. `format` is
    /// the string the frontend ships (`"csv"`, `"parquet"`, `"s3"`, ...).
    pub fn inspect(&self, format: &str, options: JsonValue) -> Result<Inspection, EngineError> {
        let select = plan::source_select_for_format(format, &options).ok_or_else(|| {
            EngineError::Unsupported(format!("Format '{}' is not supported", format))
        })?;
        let prelude = self.source_prelude(format, &options);

        let describe_sql = format!("{}DESCRIBE {};", prelude, select);
        let cols = self.run_rows(None, &describe_sql)?;
        let schema: Vec<Column> = cols.iter().filter_map(parse_describe_row).collect();

        let sample_sql = format!("{}{} LIMIT {};", prelude, select, PREVIEW_LIMIT);
        let rows = self.run_rows(None, &sample_sql).unwrap_or_default();

        Ok(Inspection {
            schema,
            sample_rows: rows,
        })
    }

    /// Statements that must run before a source query: cloud credentials,
    /// the azure extension, or ATTACH for a DuckDB file.
    fn source_prelude(&self, format: &str, options: &JsonValue) -> String {
        let mut p = String::new();
        if let Some(secret) = secret_statement(format, "duckle_inspect", options) {
            p.push_str(&secret);
            p.push(' ');
        }
        if format == "azureblob" {
            p.push_str("INSTALL azure; LOAD azure; ");
        }
        if format == "duckdb" {
            if let Some(db) = options.get("database").and_then(JsonValue::as_str) {
                p.push_str(&format!(
                    "ATTACH '{}' AS duckle_src (READ_ONLY); ",
                    sql_escape(db)
                ));
            }
        }
        p
    }

    // ---- Execution -----------------------------------------------------

    pub fn execute_pipeline(&self, doc: &PipelineDoc) -> RunResult {
        self.execute_pipeline_with_events(doc, None::<&str>, |_| {})
    }

    /// Execute a pipeline, optionally only the subgraph upstream of
    /// `target`, streaming [`PipelineEvent`]s through `on_event`.
    pub fn execute_pipeline_with_events<F>(
        &self,
        doc: &PipelineDoc,
        target: Option<&str>,
        mut on_event: F,
    ) -> RunResult
    where
        F: FnMut(PipelineEvent),
    {
        let total_start = Instant::now();
        self.clear_cancel();

        if !self.bin.exists() {
            return RunResult::failed(
                total_start,
                "DuckDB engine isn't installed yet. Open Setup to install it.".into(),
            );
        }

        let compiled = match target {
            Some(t) => plan::compile_partial(doc, t),
            None => plan::compile(doc),
        };
        let compiled = match compiled {
            Ok(c) => c,
            Err(e) => return RunResult::failed(total_start, e.to_string()),
        };

        on_event(PipelineEvent::Started {
            total_stages: compiled.stages.len() as u32,
        });

        // Temp on-disk DB for this run. The atomic counter guarantees a
        // unique path even when several runs start in the same process at
        // the same clock tick (parallel tests, or concurrent scheduled
        // runs), which would otherwise collide and fight over the file.
        let db_path = std::env::temp_dir().join(format!(
            "duckle_run_{}_{}_{}.duckdb",
            std::process::id(),
            now_nanos(),
            RUN_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _guard = TempDbGuard(db_path.clone());

        // Cloud credentials, prefixed to every stage invocation (each is
        // a fresh CLI session).
        let secrets = collect_pipeline_secrets(doc);
        let secret_prefix = if secrets.is_empty() {
            String::new()
        } else {
            format!("{} ", secrets.join(" "))
        };

        let mut nodes: std::collections::BTreeMap<String, NodeRunStatus> = Default::default();
        let mut overall_error: Option<String> = None;
        // ctl.try installs a fallback path here. On any subsequent
        // stage failure, the engine runs it as a side effect before
        // surfacing the original error. Cleared/replaced by the most
        // recent ctl.try (no stacked nesting yet - DAG block refactor
        // would add that).
        let mut installed_fallback: Option<String> = None;
        let mut was_cancelled = false;
        let mut preview: Vec<NodePreview> = Vec::new();

        for stage in &compiled.stages {
            if self.cancel.load(Ordering::Relaxed) {
                was_cancelled = true;
                on_event(PipelineEvent::Cancelled);
                break;
            }
            let kind_label = match stage.kind {
                StageKind::Sink => "sink",
                StageKind::View => "view",
            };
            on_event(PipelineEvent::StageStarted {
                node_id: stage.node_id.clone(),
                label: stage.label.clone(),
                kind: kind_label.into(),
            });

            // ctl.wait / ctl.throttle inject an inter-stage delay
            // before running the SQL. Done in the executor so the
            // planner stays declarative.
            if let Some(ms) = stage.wait_ms {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
            let started = Instant::now();
            // Advanced settings: memoryLimitMb prepends a PRAGMA so heavy
            // aggregations can be capped per stage. The PRAGMA only lives
            // for the duration of this CLI invocation.
            let memory_pragma = match stage.memory_limit_mb {
                Some(mb) => format!("PRAGMA memory_limit='{}MB'; ", mb),
                None => String::new(),
            };
            // Enforce "error if exists" before writing a local file sink.
            let sql = format!("{}{}{}", secret_prefix, memory_pragma, stage.sql);
            // Retry loop: retry_attempts >= 1; with the default of 1 we
            // call run() exactly once. Retries sleep retry_backoff_ms
            // (linearly scaled by attempt index) between attempts.
            // Cancellation is caught at the start of the *next* stage,
            // so the retry loop can complete its backoff naturally.
            let mut result = Err(EngineError::Query("stage did not run".into()));
            for attempt in 0..stage.retry_attempts {
                if attempt > 0 && stage.retry_backoff_ms > 0 {
                    let delay = stage.retry_backoff_ms.saturating_mul(attempt as u64);
                    std::thread::sleep(std::time::Duration::from_millis(delay));
                }
                // ctl.runpipeline / ctl.trigger: read + execute the
                // referenced pipeline file as a side effect *before*
                // the stage's own pass-through SQL. Failure retries
                // the whole stage (sub-pipeline + SQL together).
                // Sub-pipelines run in their own temp DB via the
                // engine's normal execute_pipeline; their output
                // isn't composed into the parent (the side-effect /
                // trigger model). Full block-scope composition needs
                // the DAG-engine refactor noted in the README.
                if let Some(ref sub_path) = stage.run_pipeline_path {
                    if let Err(e) = self.run_subpipeline(sub_path) {
                        result = Err(EngineError::Query(format!(
                            "ctl.runpipeline({}): {}",
                            sub_path, e
                        )));
                        continue;
                    }
                }
                // ctl.iterate: run the sub-pipeline N times, substituting
                // ${ITER_INDEX} into the pipeline JSON before each call.
                if let (Some(ref iter_path), Some(count)) =
                    (stage.iterate_pipeline_path.as_ref(), stage.iterate_count)
                {
                    let mut iter_err: Option<String> = None;
                    for i in 0..count {
                        let mut subs = std::collections::HashMap::new();
                        subs.insert("ITER_INDEX".to_string(), i.to_string());
                        if let Err(e) = self.run_subpipeline_with_subs(iter_path, &subs) {
                            iter_err = Some(format!(
                                "ctl.iterate({})[iteration {}]: {}",
                                iter_path, i, e
                            ));
                            break;
                        }
                    }
                    if let Some(e) = iter_err {
                        result = Err(EngineError::Query(e));
                        continue;
                    }
                }
                // ctl.foreach: read upstream rows, run the sub-pipeline
                // once per row with ${ITER_ITEM_<FIELD>} substitutions.
                if let Some(ref each_path) = stage.foreach_pipeline_path {
                    // Materialize upstream first if it isn't already
                    // (the stage's own pass-through SQL runs *after*
                    // these hooks, so the upstream view is what we
                    // read - which is the parent's last stage output).
                    let select = match &stage.from {
                        Some(f) => format!("SELECT * FROM {}", plan::quote_ident(f)),
                        None => format!("SELECT * FROM {}", plan::quote_ident(&stage.node_id)),
                    };
                    let rows = match self.run_rows(Some(&db_path), &select) {
                        Ok(r) => r,
                        Err(e) => {
                            result = Err(EngineError::Query(format!(
                                "ctl.foreach({}): can't read upstream: {}",
                                each_path, e
                            )));
                            continue;
                        }
                    };
                    let mut each_err: Option<String> = None;
                    for (i, row) in rows.iter().enumerate() {
                        let mut subs = std::collections::HashMap::new();
                        subs.insert("ITER_INDEX".to_string(), i.to_string());
                        if let Some(obj) = row.as_object() {
                            for (k, v) in obj {
                                let val_str = v
                                    .as_str()
                                    .map(String::from)
                                    .unwrap_or_else(|| v.to_string());
                                subs.insert(
                                    format!("ITER_ITEM_{}", k.to_uppercase()),
                                    val_str,
                                );
                            }
                        }
                        if let Err(e) = self.run_subpipeline_with_subs(each_path, &subs) {
                            each_err = Some(format!(
                                "ctl.foreach({})[row {}]: {}",
                                each_path, i, e
                            ));
                            break;
                        }
                    }
                    if let Some(e) = each_err {
                        result = Err(EngineError::Query(e));
                        continue;
                    }
                }
                result = if let Some(spec) = stage.webhook.as_ref() {
                    // HTTP sink (snk.webhook / snk.rest): materialize the
                    // upstream as JSON via DuckDB, then dispatch one
                    // request per row or one batched request via ureq.
                    self.run_webhook(&db_path, &secret_prefix, spec)
                } else if let Some(spec) = stage.snowflake_sink.as_ref() {
                    // Snowflake SQL API: multi-row INSERT statements
                    // batched at spec.batch_size and POSTed to /api/v2/
                    // statements with Bearer PAT auth.
                    self.run_snowflake_sink(&db_path, &secret_prefix, spec)
                } else if let Some(spec) = stage.databricks_sink.as_ref() {
                    // Databricks SQL Statement Execution API: same shape
                    // as Snowflake, different body keys + backtick quoting.
                    self.run_databricks_sink(&db_path, &secret_prefix, spec)
                } else if let Some(spec) = stage.snowflake_source.as_ref() {
                    // Snowflake source: POST SELECT, parse response,
                    // materialize as node_id via read_json_auto.
                    self.run_snowflake_source(&db_path, spec)
                } else if let Some(spec) = stage.databricks_source.as_ref() {
                    self.run_databricks_source(&db_path, spec)
                } else if let Some(spec) = stage.rest_source.as_ref() {
                    // Generic HTTP source: fetch URL, walk response_path,
                    // follow cursor pagination, materialize as table.
                    self.run_rest_source(&db_path, spec)
                } else if let Some(spec) = stage.elastic_source.as_ref() {
                    // Elasticsearch / OpenSearch _search source with
                    // from+size pagination.
                    self.run_elastic_source(&db_path, spec)
                } else if let Some(spec) = stage.mongo_sink.as_ref() {
                    // MongoDB insert_many via official async driver +
                    // a tokio block_on per stage.
                    self.run_mongo_sink(&db_path, spec)
                } else if let Some(spec) = stage.mongo_source.as_ref() {
                    self.run_mongo_source(&db_path, spec)
                } else if let Some(spec) = stage.clickhouse_sink.as_ref() {
                    // ClickHouse HTTP sink: POST INSERT ... FORMAT JSONEachRow.
                    self.run_clickhouse_sink(&db_path, spec)
                } else if let Some(spec) = stage.clickhouse_source.as_ref() {
                    self.run_clickhouse_source(&db_path, spec)
                } else if let Some(spec) = stage.sqlserver_sink.as_ref() {
                    self.run_sqlserver_sink(&db_path, spec)
                } else if let Some(spec) = stage.sqlserver_source.as_ref() {
                    self.run_sqlserver_source(&db_path, spec)
                } else if let Some(spec) = stage.cassandra_sink.as_ref() {
                    self.run_cassandra_sink(&db_path, spec)
                } else if let Some(spec) = stage.cassandra_source.as_ref() {
                    self.run_cassandra_source(&db_path, spec)
                } else if let Some(spec) = stage.oracle_sink.as_ref() {
                    self.run_oracle_sink(&db_path, spec)
                } else if let Some(spec) = stage.oracle_source.as_ref() {
                    self.run_oracle_source(&db_path, spec)
                } else if let Some(spec) = stage.redis_sink.as_ref() {
                    self.run_redis_sink(&db_path, spec)
                } else if let Some(spec) = stage.redis_source.as_ref() {
                    self.run_redis_source(&db_path, spec)
                } else if let Some(spec) = stage.qdrant_source.as_ref() {
                    self.run_qdrant_source(&db_path, spec)
                } else if let Some(spec) = stage.weaviate_source.as_ref() {
                    self.run_weaviate_source(&db_path, spec)
                } else if let Some(spec) = stage.milvus_source.as_ref() {
                    self.run_milvus_source(&db_path, spec)
                } else if let Some(spec) = stage.format_source.as_ref() {
                    self.run_format_source(&db_path, spec)
                } else if let Some(spec) = stage.format_sink.as_ref() {
                    self.run_format_sink(&db_path, spec)
                } else if let Some(spec) = stage.kafka_sink.as_ref() {
                    self.run_kafka_sink(&db_path, spec)
                } else if let Some(spec) = stage.kafka_source.as_ref() {
                    self.run_kafka_source(&db_path, spec)
                } else if let Some(spec) = stage.avro_source.as_ref() {
                    self.run_avro_source(&db_path, spec)
                } else if let Some(spec) = stage.nats_sink.as_ref() {
                    self.run_nats_sink(&db_path, spec)
                } else if let Some(spec) = stage.nats_source.as_ref() {
                    self.run_nats_source(&db_path, spec)
                } else if let Some(spec) = stage.pubsub_sink.as_ref() {
                    self.run_pubsub_sink(&db_path, spec)
                } else if let Some(spec) = stage.pubsub_source.as_ref() {
                    self.run_pubsub_source(&db_path, spec)
                } else if let Some(spec) = stage.xml_source.as_ref() {
                    self.run_xml_source(&db_path, spec)
                } else if let Some(spec) = stage.xml_sink.as_ref() {
                    self.run_xml_sink(&db_path, spec)
                } else if let Some(spec) = stage.avro_sink.as_ref() {
                    self.run_avro_sink(&db_path, spec)
                } else if let Some(spec) = stage.rabbit_sink.as_ref() {
                    self.run_rabbit_sink(&db_path, spec)
                } else if let Some(spec) = stage.rabbit_source.as_ref() {
                    self.run_rabbit_source(&db_path, spec)
                } else if let Some(spec) = stage.git_source.as_ref() {
                    self.run_git_source(&db_path, spec)
                } else if let Some(spec) = stage.shell.as_ref() {
                    self.run_shell(&db_path, spec)
                } else if let Some(spec) = stage.upsert.as_ref() {
                    // Relational-DB upsert: DESCRIBE the upstream first to
                    // get the column list, then assemble INSERT ... ON
                    // CONFLICT (Postgres) or ON DUPLICATE KEY UPDATE (MySQL).
                    self.run_upsert(&db_path, &secret_prefix, spec)
                } else if let Some(spec) = stage.text_search.as_ref() {
                    // FTS in DuckDB v1.5+ can't see tables created in the
                    // same -c invocation, so we stage in one CLI call then
                    // index + query in a second.
                    self.run_text_search(&db_path, &secret_prefix, &stage.node_id, spec)
                } else if stage.sink_mode.as_deref() == Some("error")
                    && stage
                        .sink_path
                        .as_deref()
                        .map(is_local_path)
                        .unwrap_or(false)
                    && std::path::Path::new(stage.sink_path.as_deref().unwrap()).exists()
                {
                    Err(EngineError::Query(format!(
                        "Output file already exists: {} (write mode is 'Error if exists')",
                        stage.sink_path.as_deref().unwrap()
                    )))
                } else {
                    self.run(Some(&db_path), &sql, false)
                };
                if result.is_ok() {
                    break;
                }
            }
            let elapsed_ms = started.elapsed().as_millis() as u64;

            match result {
                Ok(_) => {
                    let rows_opt = match stage.kind {
                        StageKind::Sink => stage
                            .from
                            .as_ref()
                            .and_then(|f| self.count_rows(&db_path, f).ok()),
                        StageKind::View => self.count_rows(&db_path, &stage.node_id).ok(),
                    };
                    nodes.insert(
                        stage.node_id.clone(),
                        NodeRunStatus {
                            status: "ok".into(),
                            kind: Some(kind_label.into()),
                            rows: rows_opt,
                            duration_ms: Some(elapsed_ms),
                            error: None,
                        },
                    );
                    on_event(PipelineEvent::StageFinished {
                        node_id: stage.node_id.clone(),
                        kind: kind_label.into(),
                        status: "ok".into(),
                        rows: rows_opt,
                        duration_ms: elapsed_ms,
                        error: None,
                    });
                    if stage.kind == StageKind::View {
                        if let Ok(p) = self.preview_table(&db_path, &stage.node_id) {
                            preview.push(p);
                        }
                    }
                }
                Err(EngineError::Cancelled) => {
                    was_cancelled = true;
                    on_event(PipelineEvent::Cancelled);
                    break;
                }
                Err(err) => {
                    let msg = err.to_string();
                    nodes.insert(
                        stage.node_id.clone(),
                        NodeRunStatus {
                            status: "error".into(),
                            kind: Some(kind_label.into()),
                            rows: None,
                            duration_ms: Some(elapsed_ms),
                            error: Some(msg.clone()),
                        },
                    );
                    on_event(PipelineEvent::StageFinished {
                        node_id: stage.node_id.clone(),
                        kind: kind_label.into(),
                        status: "error".into(),
                        rows: None,
                        duration_ms: elapsed_ms,
                        error: Some(msg.clone()),
                    });
                    // ctl.try fallback: if an upstream ctl.try installed
                    // a recovery pipeline, run it as a side effect before
                    // we surface the original error. Take() so we only
                    // fire once per fallback installation.
                    if let Some(fallback) = installed_fallback.take() {
                        if let Err(fe) = self.run_subpipeline(&fallback) {
                            overall_error.get_or_insert(format!(
                                "{}: {} (and fallback '{}' also failed: {})",
                                stage.label, msg, fallback, fe
                            ));
                            break;
                        }
                        // Fallback ran cleanly; still propagate the
                        // original error - this is "side-effect" semantics.
                    }
                    overall_error.get_or_insert(format!("{}: {}", stage.label, msg));
                    break;
                }
            }
            // ctl.try sets install_fallback_path on the stage itself;
            // after a successful run, install it for subsequent stages.
            if let Some(ref p) = stage.install_fallback_path {
                installed_fallback = Some(p.clone());
            }
        }

        let final_status = if was_cancelled {
            "cancelled"
        } else if overall_error.is_some() {
            "error"
        } else {
            "ok"
        };
        on_event(PipelineEvent::Finished {
            status: final_status.into(),
            duration_ms: total_start.elapsed().as_millis() as u64,
        });

        RunResult {
            status: final_status.into(),
            duration_ms: total_start.elapsed().as_millis() as u64,
            nodes,
            preview,
            error: overall_error,
        }
    }

    /// Relational-DB upsert. DuckDB's ATTACH doesn't propagate the
    /// target's UNIQUE / PRIMARY KEY constraints, so a native DuckDB
    /// INSERT ... ON CONFLICT fails to bind. Instead we stage the
    /// upstream into the target DB via ATTACH and then run the real
    /// ON CONFLICT (Postgres) / ON DUPLICATE KEY UPDATE (MySQL) INSERT
    /// directly on the underlying connection through the extension's
    /// passthrough function (postgres_execute / mysql_execute).
    fn run_upsert(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::UpsertSpec,
    ) -> Result<String, EngineError> {
        let desc_sql = format!("DESCRIBE {};", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &desc_sql)?;
        let all_cols: Vec<String> = rows
            .iter()
            .filter_map(|r| {
                r.get("column_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        if all_cols.is_empty() {
            return Err(EngineError::Query(format!(
                "Upsert: couldn't read columns from '{}'",
                spec.from_view
            )));
        }
        let key_set: std::collections::HashSet<&str> =
            spec.conflict_cols.iter().map(|s| s.as_str()).collect();
        let set_cols: Vec<&String> = all_cols
            .iter()
            .filter(|c| !key_set.contains(c.as_str()))
            .collect();

        // Sanitized staging table name (suffix from upstream node id).
        let suffix: String = spec
            .from_view
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let target_native = spec
            .target
            .strip_prefix("duckle_dst.")
            .unwrap_or(&spec.target)
            .to_string();
        let staging_unqualified = format!("duckle_upsert_staging_{}", suffix);

        // Step 1: stage the rows in the target DB (via ATTACH).
        // Default schema differs per family (public for PG/Cockroach;
        // for MySQL the database is selected at ATTACH, no schema layer).
        let staging_native = match spec.family {
            plan::UpsertFamily::Postgres => format!("public.{}", staging_unqualified),
            plan::UpsertFamily::MySql => staging_unqualified.clone(),
        };
        let staging_duckle = format!("duckle_dst.{}", staging_native);
        let stage_sql = format!(
            "{secret}{attach}DROP TABLE IF EXISTS {sd}; \
             CREATE TABLE {sd} AS SELECT * FROM {from} WHERE 1=0; \
             INSERT INTO {sd} SELECT * FROM {from};",
            secret = secret_prefix,
            attach = spec.attach,
            sd = staging_duckle,
            from = plan::quote_ident(&spec.from_view)
        );
        self.run(Some(db), &stage_sql, false)?;

        // Step 2: assemble the real upsert SQL, run it on the native
        // connection so the constraint check sees the real schema.
        let native_sql = build_native_upsert_sql(spec, &set_cols, &target_native, &staging_native);
        let exec_fn = match spec.family {
            plan::UpsertFamily::Postgres => "postgres_execute",
            plan::UpsertFamily::MySql => "mysql_execute",
        };
        let exec_sql = format!(
            "{secret}{attach}CALL {fn_name}('duckle_dst', '{sql}');",
            secret = secret_prefix,
            attach = spec.attach,
            fn_name = exec_fn,
            sql = native_sql.replace('\'', "''")
        );
        self.run(Some(db), &exec_sql, false)
    }

    /// HTTP sink (snk.webhook / snk.rest). Materializes the upstream
    /// view via DuckDB's -json output, then either
    ///   - row mode: one ureq request per row, body = row JSON
    ///   - batch mode: a single request with body = entire array JSON
    ///
    /// Returns a synthetic 'sent N rows' report on success; aggregates
    /// per-row HTTP errors into a single Err for the run feedback layer.
    fn run_webhook(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &WebhookSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        let method = if spec.method.is_empty() {
            "POST".to_string()
        } else {
            spec.method.to_uppercase()
        };
        let dispatch = |body: String, default_ct: &str| -> Result<(), EngineError> {
            let mut req = ureq::request(&method, &spec.url);
            let has_ct = spec
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
            for (k, v) in &spec.headers {
                req = req.set(k, v);
            }
            if !has_ct {
                req = req.set("content-type", default_ct);
            }
            match req.send_string(&body) {
                Ok(_) => Ok(()),
                Err(ureq::Error::Status(code, response)) => {
                    let body = response.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "HTTP {} from {}: {}",
                        code,
                        spec.url,
                        body.chars().take(200).collect::<String>()
                    )))
                }
                Err(e) => Err(EngineError::Query(format!(
                    "HTTP transport error to {}: {}",
                    spec.url, e
                ))),
            }
        };
        match spec.body_shape.as_str() {
            "batch" => {
                // Wrap the rows array in {body_wrap: [...]} when set,
                // and merge any body_extras (e.g. Milvus's collectionName).
                let body = if spec.body_wrap.is_some() || !spec.body_extras.is_empty() {
                    let mut obj = serde_json::Map::new();
                    if let Some(wrap_key) = &spec.body_wrap {
                        obj.insert(
                            wrap_key.clone(),
                            serde_json::Value::Array(rows.clone()),
                        );
                    }
                    for (k, v) in &spec.body_extras {
                        obj.insert(k.clone(), v.clone());
                    }
                    serde_json::to_string(&serde_json::Value::Object(obj))
                        .unwrap_or_else(|_| "{}".into())
                } else {
                    serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into())
                };
                dispatch(body, "application/json")?;
                Ok(format!("sent 1 batch ({} rows) to {}", rows.len(), spec.url))
            }
            "ndjson_bulk" => {
                // Each row produces TWO lines: an action then the doc.
                // The action template lives in spec.bulk_action (set by
                // snk.elastic / snk.opensearch with the index name baked in).
                let action = spec
                    .bulk_action
                    .as_deref()
                    .unwrap_or("{\"index\":{}}");
                let mut body = String::new();
                for row in &rows {
                    body.push_str(action);
                    body.push('\n');
                    let doc = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
                    body.push_str(&doc);
                    body.push('\n');
                }
                dispatch(body, "application/x-ndjson")?;
                Ok(format!("bulk-indexed {} docs to {}", rows.len(), spec.url))
            }
            _ => {
                let mut sent = 0_usize;
                for row in &rows {
                    let body = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
                    dispatch(body, "application/json")?;
                    sent += 1;
                }
                Ok(format!("sent {} rows to {}", sent, spec.url))
            }
        }
    }

    /// Snowflake SQL API sink. Reads the upstream view as JSON,
    /// chunks rows into spec.batch_size groups, builds one multi-row
    /// INSERT per chunk, and POSTs to /api/v2/statements with Bearer
    /// PAT auth. Failures surface as a single Err for the run feedback.
    fn run_snowflake_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &SnowflakeSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("snowflake: 0 rows to insert into {}", spec.table));
        }
        // Take column order from the first row (DuckDB CLI -json output
        // preserves the SELECT order, which is the upstream view's order).
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => return Err(EngineError::Query("snowflake: upstream rows aren't JSON objects".into())),
        };
        let schema_name = spec.schema.as_deref().unwrap_or("PUBLIC");
        let qualified = format!(
            "{}.{}.{}",
            sf_quote_ident(&spec.database),
            sf_quote_ident(schema_name),
            sf_quote_ident(&spec.table)
        );
        let cols_list = cols
            .iter()
            .map(|c| sf_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let url = spec.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://{}.snowflakecomputing.com/api/v2/statements",
                spec.account
            )
        });
        // Compute the Authorization header once per stage. JWT lifetime
        // is 1 hour; PAT is the token verbatim. Either way it gets
        // reused across every chunk's POST.
        let auth_header = build_snowflake_auth_header(&spec.account, &spec.auth)?;
        let mut total_inserted = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_obj = row.as_object();
                    let vals: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let v = row_obj
                                .and_then(|o| o.get(c))
                                .unwrap_or(&JsonValue::Null);
                            json_to_sql_literal(v)
                        })
                        .collect();
                    format!("({})", vals.join(", "))
                })
                .collect();
            let stmt = format!(
                "INSERT INTO {} ({}) VALUES {}",
                qualified,
                cols_list,
                values.join(", ")
            );
            let mut body_obj = serde_json::Map::new();
            body_obj.insert("statement".into(), JsonValue::String(stmt));
            body_obj.insert("timeout".into(), JsonValue::Number(60.into()));
            body_obj.insert("database".into(), JsonValue::String(spec.database.clone()));
            body_obj.insert("schema".into(), JsonValue::String(schema_name.into()));
            if let Some(wh) = &spec.warehouse {
                body_obj.insert("warehouse".into(), JsonValue::String(wh.clone()));
            }
            if let Some(role) = &spec.role {
                body_obj.insert("role".into(), JsonValue::String(role.clone()));
            }
            let body = serde_json::to_string(&JsonValue::Object(body_obj))
                .unwrap_or_else(|_| "{}".into());
            let mut req = ureq::post(&url)
                .set("Authorization", &auth_header)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            // Snowflake's JWT auth needs this header so the server
            // routes the bearer through the keypair JWT validator
            // instead of the OAuth / PAT one.
            if matches!(spec.auth, SnowflakeAuth::Jwt { .. }) {
                req = req.set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
            }
            match req.send_string(&body) {
                Ok(_) => total_inserted += chunk.len(),
                Err(ureq::Error::Status(code, response)) => {
                    let body = response.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "Snowflake HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "Snowflake HTTP transport to {}: {}",
                        url, e
                    )));
                }
            }
        }
        Ok(format!(
            "snowflake: inserted {} rows into {}",
            total_inserted, spec.table
        ))
    }

    /// Oracle sink behind the `oracle` Cargo feature. Without the
    /// feature this returns a clear error so the user knows what to
    /// rebuild with. With the feature, builds multi-row INSERT ALL ...
    /// SELECT * FROM dual statements (Oracle's idiom for multi-row
    /// insert) in batches.
    #[cfg(feature = "oracle")]
    fn run_oracle_sink(
        &self,
        db: &Path,
        spec: &OracleSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("oracle: 0 rows to insert into {}", spec.table));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "oracle: upstream rows aren't JSON objects".into(),
                ));
            }
        };
        let qualified = match &spec.schema {
            Some(s) => format!("\"{}\".\"{}\"", s, spec.table),
            None => format!("\"{}\"", spec.table),
        };
        let cols_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let conn = oracle::Connection::connect(&spec.user, &spec.password, &spec.connect)
            .map_err(|e| EngineError::Query(format!("oracle connect: {}", e)))?;
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            // Oracle's "INSERT ALL" syntax:
            //   INSERT ALL
            //     INTO tbl (cols) VALUES (...)
            //     INTO tbl (cols) VALUES (...)
            //   SELECT 1 FROM dual;
            let mut sql = String::from("INSERT ALL");
            for row in chunk {
                let row_obj = row.as_object();
                let vals: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        let v = row_obj
                            .and_then(|o| o.get(c))
                            .unwrap_or(&JsonValue::Null);
                        json_to_sql_literal(v)
                    })
                    .collect();
                sql.push_str(&format!(
                    " INTO {} ({}) VALUES ({})",
                    qualified,
                    cols_list,
                    vals.join(", ")
                ));
            }
            sql.push_str(" SELECT 1 FROM dual");
            conn.execute(&sql, &[])
                .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
            conn.commit()
                .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
            total += chunk.len();
        }
        Ok(format!(
            "oracle: inserted {} rows into {}",
            total, qualified
        ))
    }

    #[cfg(not(feature = "oracle"))]
    fn run_oracle_sink(
        &self,
        _db: &Path,
        _spec: &OracleSinkSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "snk.oracle: this Duckle binary was built without the default \
             `oracle` feature. Default builds include Oracle support; if \
             you're seeing this, rebuild with `cargo build --release` (no \
             --no-default-features). At runtime users still need Oracle \
             Instant Client (libclntsh.so / OCI.dll / libclntsh.dylib) on \
             the library path."
                .into(),
        ))
    }

    /// Oracle source behind the `oracle` Cargo feature. Same gating
    /// model as the sink.
    #[cfg(feature = "oracle")]
    fn run_oracle_source(
        &self,
        db: &Path,
        spec: &OracleSourceSpec,
    ) -> Result<String, EngineError> {
        let conn = oracle::Connection::connect(&spec.user, &spec.password, &spec.connect)
            .map_err(|e| EngineError::Query(format!("oracle connect: {}", e)))?;
        let stmt = conn
            .statement(&spec.query)
            .build()
            .map_err(|e| EngineError::Query(format!("oracle prepare: {}", e)))?;
        let rs = conn
            .query(&spec.query, &[])
            .map_err(|e| EngineError::Query(format!("oracle query: {}", e)))?;
        let _ = stmt; // suppress unused warning; rs owns the statement.
        let cols: Vec<String> = rs
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        let mut rows: Vec<JsonValue> = Vec::new();
        for row_res in rs {
            let row = row_res.map_err(|e| EngineError::Query(format!("oracle row: {}", e)))?;
            let mut obj = serde_json::Map::new();
            for (i, name) in cols.iter().enumerate() {
                let v: JsonValue = if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    JsonValue::String(s)
                } else if let Ok(Some(n)) = row.get::<usize, Option<i64>>(i) {
                    JsonValue::from(n)
                } else if let Ok(Some(f)) = row.get::<usize, Option<f64>>(i) {
                    serde_json::Number::from_f64(f)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null)
                } else {
                    JsonValue::Null
                };
                obj.insert(name.clone(), v);
            }
            rows.push(JsonValue::Object(obj));
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "oracle: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    #[cfg(not(feature = "oracle"))]
    fn run_oracle_source(
        &self,
        _db: &Path,
        _spec: &OracleSourceSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "src.oracle: this Duckle binary was built without the default \
             `oracle` feature. Default builds include Oracle support."
                .into(),
        ))
    }

    /// Cassandra / ScyllaDB sink via the scylla CQL driver. Each row
    /// becomes one INSERT statement (CQL doesn't support multi-row
    /// VALUES). Values are interpolated as literals; bind parameters
    /// would need per-column type detection which the scylla 0.13
    /// generic API makes painful.
    fn run_cassandra_sink(
        &self,
        db: &Path,
        spec: &CassandraSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!(
                "cassandra: 0 rows to insert into {}.{}",
                spec.keyspace, spec.table
            ));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "cassandra: upstream rows aren't JSON objects".into(),
                ))
            }
        };
        let cols_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let qualified = format!("{}.{}", spec.keyspace, spec.table);
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("cassandra: tokio rt: {}", e)))?;
        let total = rt
            .block_on(async {
                let mut builder = scylla::SessionBuilder::new();
                for cp in spec.contact_points.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    builder = builder.known_node(cp);
                }
                if let (Some(u), Some(p)) = (&spec.user, &spec.password) {
                    builder = builder.user(u, p);
                }
                let session = builder
                    .build()
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                let mut total = 0_usize;
                for row in &rows {
                    if cancel.load(Ordering::Relaxed) {
                        return Err("cancelled".to_string());
                    }
                    let row_obj = row.as_object();
                    let vals: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let v = row_obj
                                .and_then(|o| o.get(c))
                                .unwrap_or(&JsonValue::Null);
                            json_to_sql_literal(v)
                        })
                        .collect();
                    let stmt = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        qualified,
                        cols_list,
                        vals.join(", ")
                    );
                    session
                        .query(stmt, &[])
                        .await
                        .map_err(|e| format!("insert: {}", e))?;
                    total += 1;
                }
                Ok::<usize, String>(total)
            })
            .map_err(|e| if e == "cancelled" {
                EngineError::Cancelled
            } else {
                EngineError::Query(format!("cassandra sink: {}", e))
            })?;
        Ok(format!(
            "cassandra: inserted {} rows into {}.{}",
            total, spec.keyspace, spec.table
        ))
    }

    /// Cassandra / ScyllaDB source via scylla. Best-effort CqlValue ->
    /// JsonValue conversion for the common types (numbers, text, bool,
    /// uuid, blob-as-base64).
    fn run_cassandra_source(
        &self,
        db: &Path,
        spec: &CassandraSourceSpec,
    ) -> Result<String, EngineError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("cassandra: tokio rt: {}", e)))?;
        let rows: Vec<JsonValue> = rt
            .block_on(async {
                let mut builder = scylla::SessionBuilder::new();
                for cp in spec.contact_points.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    builder = builder.known_node(cp);
                }
                if let (Some(u), Some(p)) = (&spec.user, &spec.password) {
                    builder = builder.user(u, p);
                }
                if let Some(ks) = &spec.keyspace {
                    builder = builder.use_keyspace(ks, false);
                }
                let session = builder
                    .build()
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                let result = session
                    .query(spec.query.clone(), &[])
                    .await
                    .map_err(|e| format!("query: {}", e))?;
                let cols: Vec<String> = result
                    .col_specs
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                let rows = result.rows.unwrap_or_default();
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut obj = serde_json::Map::new();
                    for (i, name) in cols.iter().enumerate() {
                        let v = row
                            .columns
                            .get(i)
                            .and_then(|cv| cv.as_ref())
                            .map(cql_value_to_json)
                            .unwrap_or(JsonValue::Null);
                        obj.insert(name.clone(), v);
                    }
                    out.push(JsonValue::Object(obj));
                }
                Ok::<Vec<JsonValue>, String>(out)
            })
            .map_err(|e| EngineError::Query(format!("cassandra source: {}", e)))?;
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "cassandra: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Redis SET sink via the sync redis client. For each upstream row,
    /// SET <keyColumn> <valueColumn|json(row)> [EX <ttl>]. Pipelined in
    /// chunks of batch_size to amortize the round-trip cost.
    fn run_redis_sink(
        &self,
        db: &Path,
        spec: &RedisSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("redis: 0 rows to SET (from {})", spec.from_view));
        }
        let client = redis::Client::open(spec.url.as_str())
            .map_err(|e| EngineError::Query(format!("redis: client open: {}", e)))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| EngineError::Query(format!("redis: connect: {}", e)))?;
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            let mut pipe = redis::pipe();
            for row in chunk {
                let Some(obj) = row.as_object() else {
                    return Err(EngineError::Query(
                        "redis: upstream rows aren't JSON objects".into(),
                    ));
                };
                let key = obj
                    .get(&spec.key_column)
                    .map(|v| match v {
                        JsonValue::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .ok_or_else(|| {
                        EngineError::Query(format!(
                            "redis: keyColumn '{}' not in row",
                            spec.key_column
                        ))
                    })?;
                let value = if spec.value_column.is_empty() {
                    serde_json::to_string(row).unwrap_or_default()
                } else {
                    obj.get(&spec.value_column)
                        .map(|v| match v {
                            JsonValue::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                        .unwrap_or_default()
                };
                if spec.ttl_seconds > 0 {
                    pipe.cmd("SETEX")
                        .arg(&key)
                        .arg(spec.ttl_seconds)
                        .arg(&value)
                        .ignore();
                } else {
                    pipe.cmd("SET").arg(&key).arg(&value).ignore();
                }
            }
            redis::Pipeline::query::<()>(&pipe, &mut conn)
                .map_err(|e| EngineError::Query(format!("redis: SET batch: {}", e)))?;
            total += chunk.len();
        }
        Ok(format!("redis: SET {} key(s)", total))
    }

    /// Redis SCAN+GET source. Walks keys matching key_pattern via SCAN
    /// (cursor-based; safe for large keyspaces - never blocks like
    /// KEYS), then GETs each in pipelined batches of 500 and emits
    /// {key, value} rows. Limit caps the walk so a million-key DB
    /// doesn't take forever; defaults to 10_000.
    fn run_redis_source(
        &self,
        db: &Path,
        spec: &RedisSourceSpec,
    ) -> Result<String, EngineError> {
        let client = redis::Client::open(spec.url.as_str())
            .map_err(|e| EngineError::Query(format!("redis: client open: {}", e)))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| EngineError::Query(format!("redis: connect: {}", e)))?;
        let mut keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            self.check_cancelled()?;
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&spec.key_pattern)
                .arg("COUNT")
                .arg(500_u32)
                .query(&mut conn)
                .map_err(|e| EngineError::Query(format!("redis: SCAN: {}", e)))?;
            keys.extend(batch);
            if keys.len() as u64 >= spec.limit {
                keys.truncate(spec.limit as usize);
                break;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let mut rows: Vec<JsonValue> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(500) {
            self.check_cancelled()?;
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("GET").arg(k);
            }
            let values: Vec<Option<String>> = redis::Pipeline::query(&pipe, &mut conn)
                .map_err(|e| EngineError::Query(format!("redis: GET batch: {}", e)))?;
            for (k, v) in chunk.iter().zip(values) {
                let mut obj = serde_json::Map::new();
                obj.insert("key".into(), JsonValue::String(k.clone()));
                obj.insert(
                    "value".into(),
                    v.map(JsonValue::String).unwrap_or(JsonValue::Null),
                );
                rows.push(JsonValue::Object(obj));
            }
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "redis: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Qdrant scroll source. POSTs to /collections/{id}/points/scroll
    /// with {limit, offset, with_payload, with_vector}. The response
    /// puts the points in result.points[] and the next cursor in
    /// result.next_page_offset (null when done). Engine walks pages
    /// until max_pages or the cursor is null, then flattens each
    /// point into {id, ...payload[, vector]}.
    fn run_qdrant_source(
        &self,
        db: &Path,
        spec: &QdrantSourceSpec,
    ) -> Result<String, EngineError> {
        let base = spec.cluster_url.trim_end_matches('/');
        let url = format!("{}/collections/{}/points/scroll", base, spec.collection);
        let mut all_points: Vec<JsonValue> = Vec::new();
        let mut next_offset: Option<JsonValue> = None;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let mut body = serde_json::Map::new();
            body.insert("limit".into(), JsonValue::from(spec.page_size));
            body.insert("with_payload".into(), JsonValue::Bool(true));
            body.insert("with_vector".into(), JsonValue::Bool(spec.with_vector));
            if let Some(off) = &next_offset {
                body.insert("offset".into(), off.clone());
            }
            let mut req = ureq::post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if !spec.api_key.is_empty() {
                req = req.set("api-key", &spec.api_key);
            }
            let resp = match req.send_string(&serde_json::to_string(&body).unwrap_or_default()) {
                Ok(r) => r.into_json::<JsonValue>().map_err(|e| {
                    EngineError::Query(format!("qdrant: response not JSON: {}", e))
                })?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "qdrant HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "qdrant transport to {}: {}",
                        url, e
                    )));
                }
            };
            let result = resp.get("result").cloned().unwrap_or(JsonValue::Null);
            if let Some(points) = result.get("points").and_then(|v| v.as_array()) {
                for p in points {
                    let mut obj = serde_json::Map::new();
                    if let Some(id) = p.get("id") {
                        obj.insert("id".into(), id.clone());
                    }
                    if let Some(payload) = p.get("payload").and_then(|v| v.as_object()) {
                        for (k, v) in payload {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                    if spec.with_vector {
                        if let Some(v) = p.get("vector") {
                            obj.insert("vector".into(), v.clone());
                        }
                    }
                    all_points.push(JsonValue::Object(obj));
                }
            }
            match result.get("next_page_offset") {
                Some(off) if !off.is_null() => next_offset = Some(off.clone()),
                _ => break,
            }
        }
        let count = all_points.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &all_points)?;
        Ok(format!(
            "qdrant: materialized {} points into {}",
            count, spec.node_id
        ))
    }

    /// Weaviate object-list source. GET /v1/objects?class=&limit=&after=
    /// returns {objects: [{id, class, properties, vector?}]}; cursor
    /// is the last object's id, passed as `after` on the next request.
    /// Loop terminates on a short page or max_pages.
    fn run_weaviate_source(
        &self,
        db: &Path,
        spec: &WeaviateSourceSpec,
    ) -> Result<String, EngineError> {
        let base = spec.endpoint.trim_end_matches('/');
        let mut all_objects: Vec<JsonValue> = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let mut url = format!(
                "{}/v1/objects?class={}&limit={}",
                base,
                urlencode_simple(&spec.class),
                spec.page_size
            );
            if spec.with_vector {
                url.push_str("&include=vector");
            }
            if let Some(a) = &after {
                url.push_str(&format!("&after={}", urlencode_simple(a)));
            }
            let mut req = ureq::get(&url).set("Accept", "application/json");
            if !spec.api_key.is_empty() {
                req = req.set("Authorization", &format!("Bearer {}", spec.api_key));
            }
            let resp = match req.call() {
                Ok(r) => r.into_json::<JsonValue>().map_err(|e| {
                    EngineError::Query(format!("weaviate: response not JSON: {}", e))
                })?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "weaviate HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "weaviate transport to {}: {}",
                        url, e
                    )));
                }
            };
            let Some(objs) = resp.get("objects").and_then(|v| v.as_array()) else {
                break;
            };
            let page_len = objs.len();
            let mut last_id: Option<String> = None;
            for o in objs {
                let mut obj = serde_json::Map::new();
                if let Some(id) = o.get("id").and_then(|v| v.as_str()) {
                    obj.insert("id".into(), JsonValue::String(id.to_string()));
                    last_id = Some(id.to_string());
                }
                if let Some(props) = o.get("properties").and_then(|v| v.as_object()) {
                    for (k, v) in props {
                        obj.insert(k.clone(), v.clone());
                    }
                }
                if spec.with_vector {
                    if let Some(v) = o.get("vector") {
                        obj.insert("vector".into(), v.clone());
                    }
                }
                all_objects.push(JsonValue::Object(obj));
            }
            if page_len < spec.page_size as usize {
                break;
            }
            match last_id {
                Some(id) => after = Some(id),
                None => break,
            }
        }
        let count = all_objects.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &all_objects)?;
        Ok(format!(
            "weaviate: materialized {} objects into {}",
            count, spec.node_id
        ))
    }

    /// Milvus query source. POST /v1/vector/query with {collectionName,
    /// filter, outputFields, limit, offset}. Response: {data: [...]}.
    /// Walks offset += page_size until a short page or max_pages.
    fn run_milvus_source(
        &self,
        db: &Path,
        spec: &MilvusSourceSpec,
    ) -> Result<String, EngineError> {
        let base = spec.endpoint.trim_end_matches('/');
        let url = format!("{}/v1/vector/query", base);
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut offset: u64 = 0;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let mut body = serde_json::Map::new();
            body.insert(
                "collectionName".into(),
                JsonValue::String(spec.collection.clone()),
            );
            body.insert("filter".into(), JsonValue::String(spec.filter.clone()));
            if !spec.output_fields.is_empty() {
                body.insert(
                    "outputFields".into(),
                    JsonValue::Array(
                        spec.output_fields
                            .iter()
                            .map(|f| JsonValue::String(f.clone()))
                            .collect(),
                    ),
                );
            }
            body.insert("limit".into(), JsonValue::from(spec.page_size));
            body.insert("offset".into(), JsonValue::from(offset));
            let mut req = ureq::post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if !spec.api_key.is_empty() {
                req = req.set("Authorization", &format!("Bearer {}", spec.api_key));
            }
            let resp = match req.send_string(&serde_json::to_string(&body).unwrap_or_default()) {
                Ok(r) => r.into_json::<JsonValue>().map_err(|e| {
                    EngineError::Query(format!("milvus: response not JSON: {}", e))
                })?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "milvus HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "milvus transport to {}: {}",
                        url, e
                    )));
                }
            };
            let Some(arr) = resp.get("data").and_then(|v| v.as_array()) else {
                break;
            };
            let page_len = arr.len();
            for v in arr {
                all_rows.push(v.clone());
            }
            if page_len < spec.page_size as usize {
                break;
            }
            offset += spec.page_size;
        }
        let count = all_rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "milvus: materialized {} points into {}",
            count, spec.node_id
        ))
    }

    /// YAML / TOML config-format reader. Parses the whole file with
    /// the relevant serde crate, normalizes the value into a Vec of
    /// row objects (top-level array becomes one row per element;
    /// anything else becomes a single row), and materializes via the
    /// shared json-table helper. Aimed at config-data ETL (Helm
    /// values, GitHub Actions matrices, Cargo deps audits), not at
    /// streaming gigabyte logs.
    fn run_format_source(
        &self,
        db: &Path,
        spec: &FormatFileSourceSpec,
    ) -> Result<String, EngineError> {
        let raw = std::fs::read_to_string(&spec.path).map_err(|e| {
            EngineError::Query(format!("{:?} source: read {}: {}", spec.format, spec.path, e))
        })?;
        let val: JsonValue = match spec.format {
            FormatKind::Yaml => serde_yaml::from_str(&raw).map_err(|e| {
                EngineError::Query(format!("yaml parse {}: {}", spec.path, e))
            })?,
            FormatKind::Toml => {
                let t: toml::Value = toml::from_str(&raw).map_err(|e| {
                    EngineError::Query(format!("toml parse {}: {}", spec.path, e))
                })?;
                serde_json::to_value(t).map_err(|e| {
                    EngineError::Query(format!("toml -> json {}: {}", spec.path, e))
                })?
            }
        };
        let rows: Vec<JsonValue> = match val {
            JsonValue::Array(a) => a,
            other => vec![other],
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "{:?}: materialized {} rows into {}",
            spec.format, count, spec.node_id
        ))
    }

    /// YAML / TOML config-format writer. Pulls every row from the
    /// upstream view, serializes the whole batch as a single doc.
    /// YAML emits a top-level `- key: value` array. TOML wraps in a
    /// `rows` key since TOML's top-level grammar disallows a bare
    /// array (you can't write `[ { ... }, { ... } ]` at the root).
    fn run_format_sink(
        &self,
        db: &Path,
        spec: &FormatFileSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        let payload = JsonValue::Array(rows.clone());
        let text = match spec.format {
            FormatKind::Yaml => serde_yaml::to_string(&payload).map_err(|e| {
                EngineError::Query(format!("yaml serialize: {}", e))
            })?,
            FormatKind::Toml => {
                // TOML doesn't allow a top-level array; wrap.
                let mut wrap = serde_json::Map::new();
                wrap.insert("rows".into(), payload);
                let t = serde_json::to_value(JsonValue::Object(wrap)).unwrap_or(JsonValue::Null);
                toml::to_string(&t).map_err(|e| {
                    EngineError::Query(format!("toml serialize: {}", e))
                })?
            }
        };
        std::fs::write(&spec.path, text).map_err(|e| {
            EngineError::Query(format!("{:?} sink: write {}: {}", spec.format, spec.path, e))
        })?;
        Ok(format!(
            "{:?}: wrote {} rows to {}",
            spec.format,
            rows.len(),
            spec.path
        ))
    }

    /// Apache Avro container-file reader via the pure-Rust apache-avro
    /// crate. The .avro file header carries its own schema, so the
    /// engine doesn't take any schema config - it iterates records,
    /// deserializes each Value into JSON, and materializes via the
    /// shared json-table helper. Works on every OS without depending
    /// on the DuckDB community avro extension.
    fn run_avro_source(
        &self,
        db: &Path,
        spec: &AvroSourceSpec,
    ) -> Result<String, EngineError> {
        let file = std::fs::File::open(&spec.path)
            .map_err(|e| EngineError::Query(format!("avro: open {}: {}", spec.path, e)))?;
        let reader = apache_avro::Reader::new(file)
            .map_err(|e| EngineError::Query(format!("avro: open container {}: {}", spec.path, e)))?;
        let mut rows: Vec<JsonValue> = Vec::new();
        for value in reader {
            self.check_cancelled()?;
            let v = value
                .map_err(|e| EngineError::Query(format!("avro: read record: {}", e)))?;
            let j: JsonValue = apache_avro::from_value(&v)
                .map_err(|e| EngineError::Query(format!("avro: value -> json: {}", e)))?;
            rows.push(j);
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "avro: materialized {} records into {}",
            count, spec.node_id
        ))
    }

    /// XML row-path source. Walks the document, builds a serde_json
    /// tree per element, and emits every element matching the
    /// trailing components of rowPath. Attributes become "@name"
    /// keys, text content goes to "_text" (or the value directly if
    /// the element has no children), nested elements nest naturally
    /// and convert to arrays when the same tag repeats.
    fn run_xml_source(
        &self,
        db: &Path,
        spec: &XmlSourceSpec,
    ) -> Result<String, EngineError> {
        use quick_xml::events::Event;
        use quick_xml::reader::Reader;

        let content = std::fs::read_to_string(&spec.path)
            .map_err(|e| EngineError::Query(format!("xml: read {}: {}", spec.path, e)))?;
        let mut reader = Reader::from_str(&content);
        reader.config_mut().trim_text(true);

        let row_path: Vec<String> = spec
            .row_path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        // Stack frames: (element_name, attrs+children builder, text accumulator).
        let mut stack: Vec<(String, serde_json::Map<String, JsonValue>, String)> = Vec::new();
        let mut rows: Vec<JsonValue> = Vec::new();
        let mut buf = Vec::new();
        loop {
            self.check_cancelled()?;
            let event = reader
                .read_event_into(&mut buf)
                .map_err(|e| EngineError::Query(format!("xml: parse: {}", e)))?;
            match event {
                Event::Eof => break,
                Event::Start(e) => {
                    let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    let mut builder = serde_json::Map::new();
                    for attr in e.attributes().flatten() {
                        let k = format!(
                            "@{}",
                            String::from_utf8_lossy(attr.key.as_ref())
                        );
                        let v = String::from_utf8_lossy(&attr.value).to_string();
                        builder.insert(k, JsonValue::String(v));
                    }
                    stack.push((name, builder, String::new()));
                }
                Event::Empty(e) => {
                    let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    let mut builder = serde_json::Map::new();
                    for attr in e.attributes().flatten() {
                        let k = format!(
                            "@{}",
                            String::from_utf8_lossy(attr.key.as_ref())
                        );
                        let v = String::from_utf8_lossy(&attr.value).to_string();
                        builder.insert(k, JsonValue::String(v));
                    }
                    xml_close_element(
                        &mut stack,
                        &mut rows,
                        &row_path,
                        &name,
                        builder,
                        String::new(),
                    );
                }
                Event::Text(e) => {
                    let text = String::from_utf8_lossy(
                        e.unescape().unwrap_or_default().as_ref().as_bytes(),
                    )
                    .to_string();
                    if let Some(last) = stack.last_mut() {
                        last.2.push_str(&text);
                    }
                }
                Event::End(_) => {
                    if let Some((name, builder, text)) = stack.pop() {
                        xml_close_element(&mut stack, &mut rows, &row_path, &name, builder, text);
                    }
                }
                _ => {}
            }
            buf.clear();
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "xml: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// XML wrapper-element writer. Emits
    ///   <root><row><col>val</col>...</row>...</root>
    /// Values are XML-escaped via quick-xml's writer; complex types
    /// (objects, arrays) get JSON-encoded inside CDATA so the file
    /// round-trips back through src.xml losslessly.
    fn run_xml_sink(
        &self,
        db: &Path,
        spec: &XmlSinkSpec,
    ) -> Result<String, EngineError> {
        use quick_xml::events::{BytesCData, BytesEnd, BytesStart, BytesText, Event};
        use quick_xml::writer::Writer;

        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        let mut writer = Writer::new_with_indent(&mut buf, b' ', 2);
        writer
            .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
                "1.0", Some("UTF-8"), None,
            )))
            .map_err(|e| EngineError::Query(format!("xml: write decl: {}", e)))?;
        writer
            .write_event(Event::Start(BytesStart::new(spec.root_element.as_str())))
            .map_err(|e| EngineError::Query(format!("xml: write root: {}", e)))?;
        for row in &rows {
            self.check_cancelled()?;
            writer
                .write_event(Event::Start(BytesStart::new(spec.row_element.as_str())))
                .map_err(|e| EngineError::Query(format!("xml: write row: {}", e)))?;
            if let Some(obj) = row.as_object() {
                for (k, v) in obj {
                    writer
                        .write_event(Event::Start(BytesStart::new(k.as_str())))
                        .map_err(|e| EngineError::Query(format!("xml: write col {}: {}", k, e)))?;
                    match v {
                        JsonValue::String(s) => {
                            writer
                                .write_event(Event::Text(BytesText::new(s)))
                                .map_err(|e| EngineError::Query(format!("xml: write text: {}", e)))?;
                        }
                        JsonValue::Null => {}
                        JsonValue::Bool(b) => {
                            writer
                                .write_event(Event::Text(BytesText::new(if *b {
                                    "true"
                                } else {
                                    "false"
                                })))
                                .map_err(|e| EngineError::Query(format!("xml: write bool: {}", e)))?;
                        }
                        JsonValue::Number(n) => {
                            writer
                                .write_event(Event::Text(BytesText::new(&n.to_string())))
                                .map_err(|e| EngineError::Query(format!("xml: write num: {}", e)))?;
                        }
                        JsonValue::Array(_) | JsonValue::Object(_) => {
                            // Round-trip complex shapes via JSON-in-CDATA.
                            let json = serde_json::to_string(v).unwrap_or_default();
                            writer
                                .write_event(Event::CData(BytesCData::new(json)))
                                .map_err(|e| EngineError::Query(format!("xml: write cdata: {}", e)))?;
                        }
                    }
                    writer
                        .write_event(Event::End(BytesEnd::new(k.as_str())))
                        .map_err(|e| EngineError::Query(format!("xml: close col: {}", e)))?;
                }
            }
            writer
                .write_event(Event::End(BytesEnd::new(spec.row_element.as_str())))
                .map_err(|e| EngineError::Query(format!("xml: close row: {}", e)))?;
        }
        writer
            .write_event(Event::End(BytesEnd::new(spec.root_element.as_str())))
            .map_err(|e| EngineError::Query(format!("xml: close root: {}", e)))?;
        std::fs::write(&spec.path, buf)
            .map_err(|e| EngineError::Query(format!("xml: write {}: {}", spec.path, e)))?;
        Ok(format!("xml: wrote {} rows to {}", rows.len(), spec.path))
    }

    /// Avro container-file writer. Schema is inferred from the first
    /// row's column values (long / double / string / boolean / bytes /
    /// nullable-union for nulls), unless schemaJson is provided in
    /// which case it's parsed and used verbatim. Each row is written
    /// as one Avro record; the OCF format embeds the schema in the
    /// header so the file is self-describing.
    fn run_avro_sink(
        &self,
        db: &Path,
        spec: &AvroSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            // Nothing to write - leave the file untouched rather than
            // creating an empty OCF with an arbitrary schema.
            return Ok(format!("avro: 0 rows to write to {}", spec.path));
        }
        let schema = if !spec.schema_json.is_empty() {
            apache_avro::Schema::parse_str(&spec.schema_json).map_err(|e| {
                EngineError::Query(format!("avro: parse schemaJson: {}", e))
            })?
        } else {
            let Some(first) = rows[0].as_object() else {
                return Err(EngineError::Query(
                    "avro: upstream rows aren't JSON objects".into(),
                ));
            };
            let fields: Vec<serde_json::Value> = first
                .iter()
                .map(|(name, val)| {
                    let typ = infer_avro_field_type(val);
                    serde_json::json!({ "name": name, "type": typ })
                })
                .collect();
            let schema_json = serde_json::json!({
                "type": "record",
                "name": spec.record_name,
                "fields": fields,
            });
            apache_avro::Schema::parse_str(&schema_json.to_string()).map_err(|e| {
                EngineError::Query(format!("avro: parse inferred schema: {}", e))
            })?
        };
        let file = std::fs::File::create(&spec.path)
            .map_err(|e| EngineError::Query(format!("avro: create {}: {}", spec.path, e)))?;
        let mut writer = apache_avro::Writer::new(&schema, file);
        let mut total = 0_usize;
        for row in &rows {
            self.check_cancelled()?;
            // Build an Avro Record explicitly - apache_avro::to_value
            // on a JSON object returns Value::Map which the Record-
            // typed schema rejects. Record::new + put per field uses
            // the schema's known field list to coerce types.
            let Some(obj) = row.as_object() else {
                return Err(EngineError::Query(
                    "avro: upstream rows aren't JSON objects".into(),
                ));
            };
            let mut record = apache_avro::types::Record::new(&schema).ok_or_else(|| {
                EngineError::Query(
                    "avro: failed to build Record (schema is not a record type)".into(),
                )
            })?;
            for (k, v) in obj {
                record.put(k, json_to_avro_value(v));
            }
            writer
                .append(record)
                .map_err(|e| EngineError::Query(format!("avro: append: {}", e)))?;
            total += 1;
        }
        writer
            .flush()
            .map_err(|e| EngineError::Query(format!("avro: flush: {}", e)))?;
        Ok(format!("avro: wrote {} records to {}", total, spec.path))
    }

    /// RabbitMQ / AMQP 0.9.1 publisher via lapin. Each upstream row
    /// becomes one persistent-delivery-mode message on (exchange,
    /// routingKey). Payload is JSON-stringified row.
    fn run_rabbit_sink(
        &self,
        db: &Path,
        spec: &RabbitSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("rabbit: 0 rows to publish to {}", spec.routing_key));
        }
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("rabbit: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            use lapin::options::BasicPublishOptions;
            use lapin::{BasicProperties, Connection, ConnectionProperties};
            let conn = Connection::connect(&spec.url, ConnectionProperties::default())
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| format!("channel: {}", e))?;
            let props = BasicProperties::default().with_delivery_mode(2); // persistent
            let mut total = 0_usize;
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                for row in chunk {
                    let payload = serde_json::to_vec(row).unwrap_or_default();
                    channel
                        .basic_publish(
                            &spec.exchange,
                            &spec.routing_key,
                            BasicPublishOptions::default(),
                            &payload,
                            props.clone(),
                        )
                        .await
                        .map_err(|e| format!("publish: {}", e))?
                        .await
                        .map_err(|e| format!("publish confirm: {}", e))?;
                }
                total += chunk.len();
            }
            Ok(total)
        });
        match total {
            Ok(n) => Ok(format!("rabbit: published {} message(s) to {}", n, spec.routing_key)),
            Err(e) if e == "cancelled" => Err(EngineError::Cancelled),
            Err(e) => Err(EngineError::Query(format!("rabbit sink: {}", e))),
        }
    }

    /// RabbitMQ / AMQP 0.9.1 consumer via lapin. basic_get-polls
    /// the queue (one message per call) until max_messages is
    /// reached or timeout_ms total wall-clock elapses. Auto-acks
    /// each pulled message; emits {payload, routing_key, exchange,
    /// delivery_tag} rows.
    fn run_rabbit_source(
        &self,
        db: &Path,
        spec: &RabbitSourceSpec,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("rabbit: tokio rt: {}", e)))?;
        let result: Result<Vec<JsonValue>, String> = rt.block_on(async {
            use lapin::options::{BasicAckOptions, BasicGetOptions};
            use lapin::{Connection, ConnectionProperties};
            let conn = Connection::connect(&spec.url, ConnectionProperties::default())
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| format!("channel: {}", e))?;
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_millis(spec.timeout_ms);
            let mut out: Vec<JsonValue> = Vec::new();
            while (out.len() as u64) < spec.max_messages {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                let got = channel
                    .basic_get(&spec.queue, BasicGetOptions::default())
                    .await
                    .map_err(|e| format!("basic_get: {}", e))?;
                let Some(delivery) = got else {
                    // Empty queue - wait a tick and re-poll until the
                    // deadline; an explicit zero-wait poll would
                    // spin-CPU.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                };
                let payload = String::from_utf8_lossy(&delivery.data).to_string();
                let mut obj = serde_json::Map::new();
                obj.insert("payload".into(), JsonValue::String(payload));
                obj.insert(
                    "routing_key".into(),
                    JsonValue::String(delivery.routing_key.to_string()),
                );
                obj.insert(
                    "exchange".into(),
                    JsonValue::String(delivery.exchange.to_string()),
                );
                obj.insert(
                    "delivery_tag".into(),
                    JsonValue::from(delivery.delivery_tag),
                );
                out.push(JsonValue::Object(obj));
                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .map_err(|e| format!("ack: {}", e))?;
            }
            Ok(out)
        });
        let rows = match result {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("rabbit source: {}", e))),
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "rabbit: materialized {} message(s) into {}",
            count, spec.node_id
        ))
    }

    /// Local git repo reader. Shells out to the system `git` CLI -
    /// no libgit2 dependency, no extra Rust crate. mode=log captures
    /// commit history as one row per commit; mode=files captures the
    /// tracked-file tree at a revision as one row per file. NUL-record
    /// + TAB-field framing avoids the usual `|` / newline pitfalls in
    /// commit subjects.
    fn run_git_source(&self, db: &Path, spec: &GitSourceSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let mode = spec.mode.as_str();
        let max = spec.max_rows.to_string();
        let rows: Vec<JsonValue> = match mode {
            "log" => {
                let mut cmd = std::process::Command::new("git");
                cmd.arg("-C")
                    .arg(&spec.repo)
                    .arg("log")
                    .arg("-z")
                    .arg("--max-count")
                    .arg(&max)
                    .arg("--date=iso-strict")
                    .arg("--pretty=format:%H%x09%h%x09%an%x09%ae%x09%ad%x09%s")
                    .arg(&spec.revision);
                if let Some(p) = &spec.path_filter {
                    cmd.arg("--").arg(p);
                }
                let out = cmd
                    .output()
                    .map_err(|e| EngineError::Query(format!("git log: spawn: {}", e)))?;
                if !out.status.success() {
                    return Err(EngineError::Query(format!(
                        "git log exited {}: {}",
                        out.status,
                        String::from_utf8_lossy(&out.stderr)
                    )));
                }
                parse_git_log(&out.stdout)
            }
            "files" => {
                let mut cmd = std::process::Command::new("git");
                cmd.arg("-C")
                    .arg(&spec.repo)
                    .arg("ls-tree")
                    .arg("-r")
                    .arg("-z")
                    .arg("--long")
                    .arg(&spec.revision);
                if let Some(p) = &spec.path_filter {
                    cmd.arg("--").arg(p);
                }
                let out = cmd
                    .output()
                    .map_err(|e| EngineError::Query(format!("git ls-tree: spawn: {}", e)))?;
                if !out.status.success() {
                    return Err(EngineError::Query(format!(
                        "git ls-tree exited {}: {}",
                        out.status,
                        String::from_utf8_lossy(&out.stderr)
                    )));
                }
                parse_git_ls_tree(&out.stdout, spec.max_rows as usize)
            }
            other => {
                return Err(EngineError::Config(format!(
                    "src.git: mode '{}' not supported (use 'log' or 'files')",
                    other
                )))
            }
        };
        self.check_cancelled()?;
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "git ({}): materialized {} row(s) into {}",
            mode, count, spec.node_id
        ))
    }

    /// code.shell: run a single command and emit one row with the
    /// captured stdout/stderr/exit_code/duration_ms. Shell defaults to
    /// cmd.exe on Windows and /bin/sh on Unix; override per stage with
    /// `shell`. Polls a kill-on-cancel loop every 100ms while the child
    /// runs so a long-running command doesn't pin a cancelled pipeline.
    fn run_shell(&self, db: &Path, spec: &ShellSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let started = std::time::Instant::now();
        // Pick shell + argument form.
        let (shell_cmd, flag) = match spec.shell.as_deref() {
            Some(custom) => (custom.to_string(), "-c".to_string()),
            None => {
                if cfg!(windows) {
                    ("cmd.exe".to_string(), "/C".to_string())
                } else {
                    ("/bin/sh".to_string(), "-c".to_string())
                }
            }
        };
        let mut cmd = std::process::Command::new(&shell_cmd);
        cmd.arg(&flag).arg(&spec.command);
        if let Some(dir) = &spec.working_dir {
            cmd.current_dir(dir);
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| EngineError::Query(format!("shell spawn: {}", e)))?;
        // Poll: cancel kills the child; timeout kills the child; else
        // wait for natural exit.
        let deadline = spec
            .timeout_ms
            .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {}
                Err(e) => {
                    let _ = child.kill();
                    return Err(EngineError::Query(format!("shell wait: {}", e)));
                }
            }
            if self.cancel.load(Ordering::Relaxed) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(EngineError::Cancelled);
            }
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(EngineError::Query(format!(
                        "shell: timeout after {}ms",
                        spec.timeout_ms.unwrap_or(0)
                    )));
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let out = child
            .wait_with_output()
            .map_err(|e| EngineError::Query(format!("shell collect: {}", e)))?;
        let duration_ms = started.elapsed().as_millis() as i64;
        let exit_code = out.status.code().unwrap_or(-1);
        let mut row = serde_json::Map::new();
        row.insert(
            "stdout".into(),
            JsonValue::String(String::from_utf8_lossy(&out.stdout).into_owned()),
        );
        row.insert(
            "stderr".into(),
            JsonValue::String(String::from_utf8_lossy(&out.stderr).into_owned()),
        );
        row.insert("exit_code".into(), JsonValue::from(exit_code));
        row.insert("duration_ms".into(), JsonValue::from(duration_ms));
        materialize_jsonobjects_as_table(db, &spec.node_id, &[JsonValue::Object(row)])?;
        Ok(format!(
            "shell: exit {} in {}ms -> {}",
            exit_code, duration_ms, spec.node_id
        ))
    }

    /// NATS publisher via async-nats. Each upstream row becomes one
    /// NATS message published to `subject` (or to subject + "." +
    /// row[subjectSuffixColumn] for per-row routing). Payload is the
    /// JSON-stringified row.
    fn run_nats_sink(
        &self,
        db: &Path,
        spec: &NatsSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("nats: 0 rows to publish to {}", spec.subject));
        }
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("nats: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            let client = async_nats::connect(&spec.urls)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let mut total = 0_usize;
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                for row in chunk {
                    let payload = serde_json::to_vec(row).unwrap_or_default();
                    let subject = if spec.subject_suffix_column.is_empty() {
                        spec.subject.clone()
                    } else {
                        let suffix = row
                            .get(&spec.subject_suffix_column)
                            .map(|v| match v {
                                JsonValue::String(s) => s.clone(),
                                _ => v.to_string(),
                            })
                            .unwrap_or_default();
                        if suffix.is_empty() {
                            spec.subject.clone()
                        } else {
                            format!("{}.{}", spec.subject, suffix)
                        }
                    };
                    client
                        .publish(subject, payload.into())
                        .await
                        .map_err(|e| format!("publish: {}", e))?;
                }
                total += chunk.len();
            }
            client.flush().await.map_err(|e| format!("flush: {}", e))?;
            Ok(total)
        });
        match total {
            Ok(n) => Ok(format!("nats: published {} message(s) to {}", n, spec.subject)),
            Err(e) if e == "cancelled" => Err(EngineError::Cancelled),
            Err(e) => Err(EngineError::Query(format!("nats sink: {}", e))),
        }
    }

    /// NATS subscribe-with-timeout collector. Drains messages from
    /// `subject` until either max_records is reached or timeout_ms
    /// elapses (wall clock). Emits {subject, payload, headers (json)}
    /// rows. Best-fit for "snapshot a queue" and "drain a topic"
    /// batch patterns; true streaming is a separate engine workstream.
    fn run_nats_source(
        &self,
        db: &Path,
        spec: &NatsSourceSpec,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("nats: tokio rt: {}", e)))?;
        let result: Result<Vec<JsonValue>, String> = rt.block_on(async {
            use futures_util::StreamExt;
            let client = async_nats::connect(&spec.urls)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let mut sub = client
                .subscribe(spec.subject.clone())
                .await
                .map_err(|e| format!("subscribe: {}", e))?;
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_millis(spec.timeout_ms);
            let mut out: Vec<JsonValue> = Vec::new();
            while (out.len() as u64) < spec.max_records {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let next = tokio::time::timeout(remaining, sub.next()).await;
                match next {
                    Ok(Some(msg)) => {
                        let mut obj = serde_json::Map::new();
                        obj.insert(
                            "subject".into(),
                            JsonValue::String(msg.subject.to_string()),
                        );
                        obj.insert(
                            "payload".into(),
                            JsonValue::String(
                                String::from_utf8_lossy(&msg.payload).to_string(),
                            ),
                        );
                        out.push(JsonValue::Object(obj));
                    }
                    _ => break,
                }
            }
            Ok(out)
        });
        let rows = match result {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("nats source: {}", e))),
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "nats: materialized {} message(s) into {}",
            count, spec.node_id
        ))
    }

    /// GCP Pub/Sub publish via REST. POST to
    ///   /v1/projects/{project}/topics/{topic}:publish
    /// Body: {messages: [{data: base64, attributes: {}}]}.
    /// Auth: Bearer OAuth2 access token.
    fn run_pubsub_sink(
        &self,
        db: &Path,
        spec: &PubSubSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("pubsub: 0 rows to publish to {}", spec.topic));
        }
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
            spec.project, spec.topic
        );
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            use base64::Engine as _;
            let messages: Vec<JsonValue> = chunk
                .iter()
                .map(|row| {
                    let json = serde_json::to_vec(row).unwrap_or_default();
                    let data = base64::engine::general_purpose::STANDARD.encode(&json);
                    serde_json::json!({ "data": data })
                })
                .collect();
            let body = serde_json::json!({ "messages": messages });
            let resp = ureq::post(&url)
                .set("Content-Type", "application/json")
                .set("Authorization", &format!("Bearer {}", spec.access_token))
                .send_string(&serde_json::to_string(&body).unwrap_or_default());
            match resp {
                Ok(_) => total += chunk.len(),
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "pubsub HTTP {} on publish: {}",
                        code,
                        b.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "pubsub transport: {}",
                        e
                    )));
                }
            }
        }
        Ok(format!(
            "pubsub: published {} message(s) to {}",
            total, spec.topic
        ))
    }

    /// GCP Pub/Sub pull + ack via REST. POST to
    ///   /v1/projects/{project}/subscriptions/{sub}:pull
    /// with {maxMessages: N}. Auto-acks the batch via
    ///   /v1/projects/{project}/subscriptions/{sub}:acknowledge
    /// Emits {message_id, publish_time, data} rows where data is
    /// the UTF-8-decoded message payload.
    fn run_pubsub_source(
        &self,
        db: &Path,
        spec: &PubSubSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let pull_url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:pull",
            spec.project, spec.subscription
        );
        let body = serde_json::json!({ "maxMessages": spec.max_messages });
        let resp = ureq::post(&pull_url)
            .set("Content-Type", "application/json")
            .set("Authorization", &format!("Bearer {}", spec.access_token))
            .send_string(&serde_json::to_string(&body).unwrap_or_default());
        let response: JsonValue = match resp {
            Ok(r) => r
                .into_json()
                .map_err(|e| EngineError::Query(format!("pubsub: response not JSON: {}", e)))?,
            Err(ureq::Error::Status(code, r)) => {
                let b = r.into_string().unwrap_or_default();
                return Err(EngineError::Query(format!(
                    "pubsub HTTP {} on pull: {}",
                    code,
                    b.chars().take(300).collect::<String>()
                )));
            }
            Err(e) => return Err(EngineError::Query(format!("pubsub transport: {}", e))),
        };
        let received = response
            .get("receivedMessages")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut rows: Vec<JsonValue> = Vec::with_capacity(received.len());
        let mut ack_ids: Vec<String> = Vec::with_capacity(received.len());
        for item in received {
            if let Some(ack) = item.get("ackId").and_then(|v| v.as_str()) {
                ack_ids.push(ack.to_string());
            }
            let message = item.get("message").cloned().unwrap_or(JsonValue::Null);
            let mut obj = serde_json::Map::new();
            obj.insert(
                "message_id".into(),
                message.get("messageId").cloned().unwrap_or(JsonValue::Null),
            );
            obj.insert(
                "publish_time".into(),
                message.get("publishTime").cloned().unwrap_or(JsonValue::Null),
            );
            // The data field is base64-encoded - decode best-effort.
            use base64::Engine as _;
            let data_raw = message.get("data").and_then(|v| v.as_str()).unwrap_or("");
            let decoded: Option<String> = base64::engine::general_purpose::STANDARD
                .decode(data_raw)
                .ok()
                .map(|b: Vec<u8>| String::from_utf8_lossy(&b).to_string());
            obj.insert(
                "data".into(),
                decoded.map(JsonValue::String).unwrap_or(JsonValue::Null),
            );
            rows.push(JsonValue::Object(obj));
        }
        // Acknowledge the batch so messages don't redeliver. Failure
        // is non-fatal - the messages stay queued and re-deliver on
        // their visibility timeout.
        if !ack_ids.is_empty() {
            let ack_url = format!(
                "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:acknowledge",
                spec.project, spec.subscription
            );
            let ack_body = serde_json::json!({ "ackIds": ack_ids });
            let _ = ureq::post(&ack_url)
                .set("Content-Type", "application/json")
                .set("Authorization", &format!("Bearer {}", spec.access_token))
                .send_string(&serde_json::to_string(&ack_body).unwrap_or_default());
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "pubsub: materialized {} message(s) into {}",
            count, spec.node_id
        ))
    }

    /// Kafka / Redpanda producer via rskafka. Each upstream row
    /// becomes one Kafka record: key = optional keyColumn value,
    /// value = JSON-stringified row. Records go into a single
    /// partition (multi-partition fan-out is a follow-up). Async
    /// underneath; wrapped in tokio block_on like mongo / tiberius.
    fn run_kafka_sink(
        &self,
        db: &Path,
        spec: &KafkaSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("kafka: 0 rows to produce to {}", spec.topic));
        }
        let cancel = self.cancel.clone();
        let bootstrap: Vec<String> = spec
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("kafka: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            use rskafka::client::partition::{Compression, UnknownTopicHandling};
            use rskafka::client::ClientBuilder;
            use rskafka::record::Record;
            let client = ClientBuilder::new(bootstrap)
                .build()
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let pc = client
                .partition_client(&spec.topic, spec.partition_id, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| format!("partition client: {}", e))?;
            let mut total = 0_usize;
            let now = chrono::Utc::now();
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let records: Vec<Record> = chunk
                    .iter()
                    .map(|row| {
                        let key = if spec.key_column.is_empty() {
                            None
                        } else {
                            row.get(&spec.key_column).and_then(|v| match v {
                                JsonValue::String(s) => Some(s.as_bytes().to_vec()),
                                JsonValue::Null => None,
                                other => Some(other.to_string().into_bytes()),
                            })
                        };
                        let value = serde_json::to_string(row)
                            .unwrap_or_default()
                            .into_bytes();
                        Record {
                            key,
                            value: Some(value),
                            headers: std::collections::BTreeMap::new(),
                            timestamp: now,
                        }
                    })
                    .collect();
                pc.produce(records, Compression::default())
                    .await
                    .map_err(|e| format!("produce batch: {}", e))?;
                total += chunk.len();
            }
            Ok(total)
        });
        match total {
            Ok(n) => Ok(format!("kafka: produced {} record(s) to {}", n, spec.topic)),
            Err(e) if e == "cancelled" => Err(EngineError::Cancelled),
            Err(e) => Err(EngineError::Query(format!("kafka sink: {}", e))),
        }
    }

    /// Kafka / Redpanda consumer via rskafka. Batch-fetches up to
    /// max_records messages from a single partition starting at
    /// start_offset (negative = earliest available). Emits rows of
    /// {offset, key, value, timestamp_ms}. Value is the raw bytes
    /// decoded as UTF-8 (best-effort) - schema-aware decoding (Avro,
    /// Protobuf) is on the roadmap.
    fn run_kafka_source(
        &self,
        db: &Path,
        spec: &KafkaSourceSpec,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        let bootstrap: Vec<String> = spec
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("kafka: tokio rt: {}", e)))?;
        let rows: Result<Vec<JsonValue>, String> = rt.block_on(async {
            use rskafka::client::partition::UnknownTopicHandling;
            use rskafka::client::ClientBuilder;
            let client = ClientBuilder::new(bootstrap)
                .build()
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let pc = client
                .partition_client(&spec.topic, spec.partition_id, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| format!("partition client: {}", e))?;
            // Negative start_offset = read from earliest available.
            let mut next_offset = if spec.start_offset < 0 {
                pc.get_offset(rskafka::client::partition::OffsetAt::Earliest)
                    .await
                    .map_err(|e| format!("earliest offset: {}", e))?
            } else {
                spec.start_offset
            };
            let mut out: Vec<JsonValue> = Vec::new();
            while (out.len() as u64) < spec.max_records {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let (records, _hw) = pc
                    .fetch_records(next_offset, 1..1_000_000, 1_000)
                    .await
                    .map_err(|e| format!("fetch: {}", e))?;
                if records.is_empty() {
                    break;
                }
                for r in records {
                    let mut obj = serde_json::Map::new();
                    obj.insert("offset".into(), JsonValue::from(r.offset));
                    obj.insert(
                        "timestamp_ms".into(),
                        JsonValue::from(r.record.timestamp.timestamp_millis()),
                    );
                    obj.insert(
                        "key".into(),
                        r.record
                            .key
                            .as_ref()
                            .map(|b| JsonValue::String(String::from_utf8_lossy(b).to_string()))
                            .unwrap_or(JsonValue::Null),
                    );
                    obj.insert(
                        "value".into(),
                        r.record
                            .value
                            .as_ref()
                            .map(|b| JsonValue::String(String::from_utf8_lossy(b).to_string()))
                            .unwrap_or(JsonValue::Null),
                    );
                    out.push(JsonValue::Object(obj));
                    next_offset = r.offset + 1;
                    if out.len() as u64 >= spec.max_records {
                        break;
                    }
                }
            }
            Ok(out)
        });
        let rows = match rows {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("kafka source: {}", e))),
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "kafka: materialized {} record(s) into {}",
            count, spec.node_id
        ))
    }

    /// SQL Server / Synapse sink via tiberius. Builds multi-row INSERT
    /// VALUES statements batched at spec.batch_size (default 1000 -
    /// SQL Server's per-INSERT VALUES cap). Values are interpolated as
    /// SQL literals via the shared json_to_sql_literal helper - not
    /// parameterized; safe for pipeline-produced data but document
    /// users not to wire untrusted upstream into SQL Server directly.
    fn run_sqlserver_sink(
        &self,
        db: &Path,
        spec: &SqlServerSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!(
                "sqlserver: 0 rows to insert into [{}].[{}]",
                spec.schema, spec.table
            ));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "sqlserver: upstream rows aren't JSON objects".into(),
                ));
            }
        };
        let qualified = format!(
            "{}.{}.{}",
            ss_quote_ident(&spec.database),
            ss_quote_ident(&spec.schema),
            ss_quote_ident(&spec.table),
        );
        let cols_list = cols
            .iter()
            .map(|c| ss_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("sqlserver: tokio rt: {}", e)))?;
        let total = rt
            .block_on(async {
                use tokio_util::compat::TokioAsyncWriteCompatExt;
                let mut config = tiberius::Config::new();
                config.host(&spec.host);
                config.port(spec.port);
                config.authentication(tiberius::AuthMethod::sql_server(
                    &spec.user,
                    &spec.password,
                ));
                config.database(&spec.database);
                if spec.trust_cert {
                    config.trust_cert();
                }
                let tcp = tokio::net::TcpStream::connect(config.get_addr())
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                tcp.set_nodelay(true).ok();
                let mut client = tiberius::Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|e| format!("tds handshake: {}", e))?;
                let mut total = 0_usize;
                for chunk in rows.chunks(spec.batch_size) {
                    if cancel.load(Ordering::Relaxed) {
                        return Err("cancelled".to_string());
                    }
                    let values: Vec<String> = chunk
                        .iter()
                        .map(|row| {
                            let row_obj = row.as_object();
                            let vals: Vec<String> = cols
                                .iter()
                                .map(|c| {
                                    let v = row_obj
                                        .and_then(|o| o.get(c))
                                        .unwrap_or(&JsonValue::Null);
                                    json_to_sql_literal(v)
                                })
                                .collect();
                            format!("({})", vals.join(", "))
                        })
                        .collect();
                    let stmt = format!(
                        "INSERT INTO {} ({}) VALUES {}",
                        qualified,
                        cols_list,
                        values.join(", ")
                    );
                    client
                        .execute(stmt, &[])
                        .await
                        .map_err(|e| format!("execute: {}", e))?;
                    total += chunk.len();
                }
                Ok::<usize, String>(total)
            })
            .map_err(|e| if e == "cancelled" {
                EngineError::Cancelled
            } else {
                EngineError::Query(format!("sqlserver sink: {}", e))
            })?;
        Ok(format!(
            "sqlserver: inserted {} rows into [{}].[{}].[{}]",
            total, spec.database, spec.schema, spec.table
        ))
    }

    /// SQL Server / Synapse source via tiberius. Runs the query,
    /// iterates the result stream, converts each row's ColumnData
    /// to JSON, and materializes via the jsonobjects helper.
    fn run_sqlserver_source(
        &self,
        db: &Path,
        spec: &SqlServerSourceSpec,
    ) -> Result<String, EngineError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("sqlserver: tokio rt: {}", e)))?;
        let rows: Vec<JsonValue> = rt
            .block_on(async {
                use tokio_util::compat::TokioAsyncWriteCompatExt;
                let mut config = tiberius::Config::new();
                config.host(&spec.host);
                config.port(spec.port);
                config.authentication(tiberius::AuthMethod::sql_server(
                    &spec.user,
                    &spec.password,
                ));
                config.database(&spec.database);
                if spec.trust_cert {
                    config.trust_cert();
                }
                let tcp = tokio::net::TcpStream::connect(config.get_addr())
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                tcp.set_nodelay(true).ok();
                let mut client = tiberius::Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|e| format!("tds handshake: {}", e))?;
                let stream = client
                    .query(&spec.query, &[])
                    .await
                    .map_err(|e| format!("query: {}", e))?;
                let rows = stream
                    .into_first_result()
                    .await
                    .map_err(|e| format!("collect: {}", e))?;
                let mut out = Vec::with_capacity(rows.len());
                for row in rows.iter() {
                    let mut obj = serde_json::Map::new();
                    for (i, col) in row.columns().iter().enumerate() {
                        let name = col.name().to_string();
                        let v: JsonValue = if let Ok(s) = row.try_get::<&str, _>(i) {
                            s.map(|x| JsonValue::String(x.to_string()))
                                .unwrap_or(JsonValue::Null)
                        } else if let Ok(n) = row.try_get::<i64, _>(i) {
                            n.map(JsonValue::from).unwrap_or(JsonValue::Null)
                        } else if let Ok(n) = row.try_get::<i32, _>(i) {
                            n.map(JsonValue::from).unwrap_or(JsonValue::Null)
                        } else if let Ok(n) = row.try_get::<f64, _>(i) {
                            n.and_then(|x| serde_json::Number::from_f64(x).map(JsonValue::Number))
                                .unwrap_or(JsonValue::Null)
                        } else if let Ok(b) = row.try_get::<bool, _>(i) {
                            b.map(JsonValue::Bool).unwrap_or(JsonValue::Null)
                        } else {
                            JsonValue::Null
                        };
                        obj.insert(name, v);
                    }
                    out.push(JsonValue::Object(obj));
                }
                Ok::<Vec<JsonValue>, String>(out)
            })
            .map_err(|e| EngineError::Query(format!("sqlserver source: {}", e)))?;
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "sqlserver: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// ClickHouse sink: HTTP POST to `?query=INSERT INTO db.table FORMAT
    /// JSONEachRow` with NDJSON body. Batched at spec.batch_size rows.
    fn run_clickhouse_sink(
        &self,
        db: &Path,
        spec: &ClickHouseSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!(
                "clickhouse: 0 rows to insert into {}",
                spec.table
            ));
        }
        let qualified = match &spec.database {
            Some(d) => format!("`{}`.`{}`", d, spec.table),
            None => format!("`{}`", spec.table),
        };
        let base = format!(
            "{}/?query={}",
            spec.endpoint.trim_end_matches('/'),
            urlencode_simple(&format!(
                "INSERT INTO {} FORMAT JSONEachRow",
                qualified
            ))
        );
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            // NDJSON body: one row per line.
            let mut body = String::new();
            for row in chunk {
                let line = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
                body.push_str(&line);
                body.push('\n');
            }
            let mut req = ureq::post(&base)
                .set("Content-Type", "application/x-ndjson");
            if let Some(u) = &spec.user {
                req = req.set("X-ClickHouse-User", u);
            }
            if let Some(p) = &spec.password {
                req = req.set("X-ClickHouse-Key", p);
            }
            match req.send_string(&body) {
                Ok(_) => total += chunk.len(),
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "ClickHouse HTTP {} on insert into {}: {}",
                        code,
                        qualified,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "ClickHouse HTTP transport: {}",
                        e
                    )));
                }
            }
        }
        Ok(format!(
            "clickhouse: inserted {} rows into {}",
            total, qualified
        ))
    }

    /// ClickHouse source: POST the SELECT with FORMAT JSON appended; the
    /// response has a top-level `data: [{...}]` array of row objects.
    /// Materialize via the existing jsonobjects helper.
    fn run_clickhouse_source(
        &self,
        db: &Path,
        spec: &ClickHouseSourceSpec,
    ) -> Result<String, EngineError> {
        let url = format!("{}/", spec.endpoint.trim_end_matches('/'));
        let q = if spec
            .query
            .to_uppercase()
            .contains("FORMAT JSON")
        {
            spec.query.clone()
        } else {
            format!("{} FORMAT JSON", spec.query.trim())
        };
        let mut req = ureq::post(&url).set("Content-Type", "text/plain");
        if let Some(u) = &spec.user {
            req = req.set("X-ClickHouse-User", u);
        }
        if let Some(p) = &spec.password {
            req = req.set("X-ClickHouse-Key", p);
        }
        if let Some(d) = &spec.database {
            req = req.set("X-ClickHouse-Database", d);
        }
        let resp = match req.send_string(&q) {
            Ok(r) => r,
            Err(ureq::Error::Status(code, r)) => {
                let body = r.into_string().unwrap_or_default();
                return Err(EngineError::Query(format!(
                    "ClickHouse HTTP {} on query: {}",
                    code,
                    body.chars().take(300).collect::<String>()
                )));
            }
            Err(e) => {
                return Err(EngineError::Query(format!(
                    "ClickHouse HTTP transport: {}",
                    e
                )));
            }
        };
        let response: JsonValue = resp
            .into_json()
            .map_err(|e| EngineError::Query(format!("ClickHouse response not JSON: {}", e)))?;
        let rows = response
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "clickhouse: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// MongoDB sink: insert_many into the collection in batches. The
    /// async mongodb driver is wrapped in a per-stage tokio runtime
    /// (block_on) so it fits the synchronous executor model the rest
    /// of the engine uses.
    fn run_mongo_sink(
        &self,
        db: &Path,
        spec: &MongoSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("mongo: tokio runtime: {}", e)))?;
        let result: Result<String, String> = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&spec.uri)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let collection = client
                .database(&spec.database)
                .collection::<mongodb::bson::Document>(&spec.collection);
            if spec.mode == "replace" {
                if let Err(e) = collection.drop().await {
                    // Dropping a missing collection is not an error
                    // we should surface; log + continue.
                    eprintln!("mongo: drop before replace failed: {}", e);
                }
            }
            let mut total = 0_usize;
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let docs: Vec<mongodb::bson::Document> = chunk
                    .iter()
                    .filter_map(|v| mongodb::bson::to_document(v).ok())
                    .collect();
                if docs.is_empty() {
                    continue;
                }
                let inserted = docs.len();
                collection
                    .insert_many(docs)
                    .await
                    .map_err(|e| format!("insert_many: {}", e))?;
                total += inserted;
            }
            Ok(format!(
                "mongodb: inserted {} docs into {}.{}",
                total, spec.database, spec.collection
            ))
        });
        result.map_err(|e| if e == "cancelled" {
            EngineError::Cancelled
        } else {
            EngineError::Query(format!("mongodb sink: {}", e))
        })
    }

    /// MongoDB source: find() with optional filter + projection +
    /// limit. The cursor is drained eagerly and the resulting BSON
    /// documents are converted to JsonValue for materialization.
    fn run_mongo_source(
        &self,
        db: &Path,
        spec: &MongoSourceSpec,
    ) -> Result<String, EngineError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("mongo: tokio runtime: {}", e)))?;
        let docs: Result<Vec<mongodb::bson::Document>, String> = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&spec.uri)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let collection = client
                .database(&spec.database)
                .collection::<mongodb::bson::Document>(&spec.collection);
            let filter: mongodb::bson::Document = match &spec.filter {
                Some(f) => {
                    let v: serde_json::Value = serde_json::from_str(f)
                        .map_err(|e| format!("bad filter JSON: {}", e))?;
                    mongodb::bson::to_document(&v).map_err(|e| format!("filter to bson: {}", e))?
                }
                None => mongodb::bson::Document::new(),
            };
            let mut find = collection.find(filter);
            if let Some(limit) = spec.limit {
                find = find.limit(limit);
            }
            if let Some(p) = &spec.projection {
                let pv: serde_json::Value = serde_json::from_str(p)
                    .map_err(|e| format!("bad projection JSON: {}", e))?;
                let pdoc = mongodb::bson::to_document(&pv)
                    .map_err(|e| format!("projection to bson: {}", e))?;
                find = find.projection(pdoc);
            }
            let mut cursor = find.await.map_err(|e| format!("find: {}", e))?;
            let mut out = Vec::new();
            while cursor.advance().await.map_err(|e| format!("cursor: {}", e))? {
                let d = cursor
                    .deserialize_current()
                    .map_err(|e| format!("deserialize: {}", e))?;
                out.push(d);
            }
            Ok(out)
        });
        let docs = docs.map_err(|e| EngineError::Query(format!("mongodb source: {}", e)))?;
        // BSON Document -> JsonValue. Some BSON types (ObjectId, Date)
        // serialize as objects with {$oid: ...} / {$date: ...} - good
        // enough for downstream DuckDB to ingest as strings/json.
        let json_docs: Vec<JsonValue> = docs
            .iter()
            .filter_map(|d| serde_json::to_value(d).ok())
            .collect();
        let count = json_docs.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &json_docs)?;
        Ok(format!(
            "mongodb: materialized {} docs into {}",
            count, spec.node_id
        ))
    }

    /// Elasticsearch / OpenSearch _search source. POSTs the query DSL
    /// to {endpoint}/{index}/_search and follows the configured
    /// pagination mode (from+size or search_after). Extracts
    /// hits.hits[]._source per page and materializes.
    fn run_elastic_source(
        &self,
        db: &Path,
        spec: &ElasticSourceSpec,
    ) -> Result<String, EngineError> {
        use plan::ElasticPagination;
        let url = format!(
            "{}/{}/_search",
            spec.endpoint.trim_end_matches('/'),
            spec.index
        );
        let query_dsl: JsonValue = match &spec.query {
            Some(q) => serde_json::from_str(q).map_err(|e| {
                EngineError::Config(format!("elastic: invalid query JSON: {}", e))
            })?,
            None => serde_json::json!({ "match_all": {} }),
        };
        let post = |body: &JsonValue| -> Result<JsonValue, EngineError> {
            let body_str = serde_json::to_string(body).unwrap_or_else(|_| "{}".into());
            let mut req = ureq::post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if let Some(key) = &spec.api_key {
                req = req.set("Authorization", &format!("ApiKey {}", key));
            }
            match req.send_string(&body_str) {
                Ok(r) => r.into_json().map_err(|e| {
                    EngineError::Query(format!("Elastic response not JSON: {}", e))
                }),
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "Elastic HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )))
                }
                Err(e) => Err(EngineError::Query(format!(
                    "Elastic HTTP transport to {}: {}",
                    url, e
                ))),
            }
        };
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut pages = 0_u64;
        match &spec.pagination {
            ElasticPagination::FromSize => {
                let mut from = 0_u64;
                loop {
                    self.check_cancelled()?;
                    let body = serde_json::json!({
                        "query": query_dsl,
                        "size": spec.size,
                        "from": from,
                    });
                    let response = post(&body)?;
                    let hits = response
                        .pointer("/hits/hits")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let hit_count = hits.len();
                    for h in hits {
                        let source = h
                            .get("_source")
                            .cloned()
                            .unwrap_or(JsonValue::Object(Default::default()));
                        all_rows.push(source);
                    }
                    pages += 1;
                    if (hit_count as u64) < spec.size || pages >= spec.max_pages {
                        break;
                    }
                    from = from.saturating_add(spec.size);
                }
            }
            ElasticPagination::SearchAfter { sort } => {
                // search_after walks via the last hit's `sort` array.
                // Lifts the 10k max_result_window cap entirely.
                let mut last_sort: Option<JsonValue> = None;
                loop {
                    self.check_cancelled()?;
                    let mut body = serde_json::json!({
                        "query": query_dsl,
                        "size": spec.size,
                        "sort": sort,
                    });
                    if let Some(sa) = &last_sort {
                        body["search_after"] = sa.clone();
                    }
                    let response = post(&body)?;
                    let hits = response
                        .pointer("/hits/hits")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let hit_count = hits.len();
                    // Grab the last hit's sort before we move `hits`.
                    let next_after = hits
                        .last()
                        .and_then(|h| h.get("sort"))
                        .cloned();
                    for h in hits {
                        let source = h
                            .get("_source")
                            .cloned()
                            .unwrap_or(JsonValue::Object(Default::default()));
                        all_rows.push(source);
                    }
                    pages += 1;
                    if hit_count == 0 || pages >= spec.max_pages {
                        break;
                    }
                    if (hit_count as u64) < spec.size {
                        // Last page didn't fill - we're done even with
                        // search_after.
                        break;
                    }
                    last_sort = match next_after {
                        Some(s) => Some(s),
                        None => break, // server returned no sort; can't continue.
                    };
                }
            }
        }
        materialize_jsonobjects_as_table(db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "elastic: materialized {} rows ({} page(s), {}) into {}",
            all_rows.len(),
            pages,
            match &spec.pagination {
                ElasticPagination::FromSize => "from+size",
                ElasticPagination::SearchAfter { .. } => "search_after",
            },
            spec.node_id
        ))
    }

    /// Generic HTTP REST source. Fetches the URL (optionally with a
    /// JSON body for POST APIs), parses the response, walks the
    /// configured JSON pointer to find the row array, and follows
    /// cursor pagination by extracting a cursor token + appending it
    /// as a query string parameter to the next request. Stops when
    /// no cursor token is present or max_pages is hit.
    fn run_rest_source(
        &self,
        db: &Path,
        spec: &RestSourceSpec,
    ) -> Result<String, EngineError> {
        let mut url = spec.url.clone();
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut pages = 0_u64;
        // Mutable state for offset / page strategies; cursor uses
        // per-response extraction inside the loop.
        let mut offset = 0_u64;
        let mut page_no = match &spec.pagination {
            RestPagination::Page { start_page, .. } => *start_page,
            _ => 1,
        };
        loop {
            self.check_cancelled()?;
            // Build request
            let mut req = ureq::request(&spec.method, &url);
            let has_ct = spec
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
            for (k, v) in &spec.headers {
                req = req.set(k, v);
            }
            if spec.body.is_some() && !has_ct {
                req = req.set("content-type", "application/json");
            }
            let resp_result = match &spec.body {
                Some(b) => req.send_string(b),
                None => req.call(),
            };
            let response_raw = match resp_result {
                Ok(r) => r,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "REST HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "REST HTTP transport to {}: {}",
                        url, e
                    )));
                }
            };
            // Capture Link header before consuming the response body.
            let link_header = response_raw.header("link").map(String::from);
            let response: JsonValue = response_raw
                .into_json()
                .map_err(|e| EngineError::Query(format!("REST response not JSON: {}", e)))?;
            // Extract rows array
            let rows = if spec.response_path.is_empty() {
                response.as_array().cloned().unwrap_or_default()
            } else {
                response
                    .pointer(&spec.response_path)
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
            };
            let row_count = rows.len();
            all_rows.extend(rows);
            pages += 1;
            if pages >= spec.max_pages {
                break;
            }
            // Decide whether to fetch another page.
            match &spec.pagination {
                RestPagination::None => break,
                RestPagination::Cursor { next_path, param } => {
                    let next = response
                        .pointer(next_path)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(String::from);
                    match next {
                        Some(token) => {
                            let sep = if spec.url.contains('?') { '&' } else { '?' };
                            url = format!(
                                "{}{}{}={}",
                                spec.url,
                                sep,
                                param,
                                urlencode_simple(&token)
                            );
                        }
                        None => break,
                    }
                }
                RestPagination::Offset { offset_param, page_size } => {
                    // Stop when a page returns fewer rows than requested.
                    if (row_count as u64) < *page_size {
                        break;
                    }
                    offset = offset.saturating_add(*page_size);
                    let sep = if spec.url.contains('?') { '&' } else { '?' };
                    url = format!("{}{}{}={}", spec.url, sep, offset_param, offset);
                }
                RestPagination::Page { page_param, .. } => {
                    if row_count == 0 {
                        break;
                    }
                    page_no = page_no.saturating_add(1);
                    let sep = if spec.url.contains('?') { '&' } else { '?' };
                    url = format!("{}{}{}={}", spec.url, sep, page_param, page_no);
                }
                RestPagination::Link => {
                    match link_header.as_deref().and_then(parse_link_next) {
                        Some(next_url) => url = next_url,
                        None => break,
                    }
                }
                RestPagination::NextUrl { next_path } => {
                    let next = response
                        .pointer(next_path)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(String::from);
                    match next {
                        Some(next_url) => url = next_url,
                        None => break,
                    }
                }
            }
        }
        materialize_jsonobjects_as_table(db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "rest: materialized {} rows ({} page(s)) into {}",
            all_rows.len(),
            pages,
            spec.node_id
        ))
    }

    /// Read a pipeline file, parse it as a PipelineDoc, and run it
    /// inline via the engine's normal execute_pipeline. Failures
    /// surface as Err(EngineError::Query) with the sub-pipeline's
    /// error message. Used by ctl.runpipeline / ctl.trigger.
    fn run_subpipeline(&self, path: &str) -> Result<(), EngineError> {
        self.run_subpipeline_with_subs(path, &std::collections::HashMap::new())
    }

    /// Read a pipeline file, perform `${KEY}` text substitution from
    /// the supplied map, parse the result as a PipelineDoc, and run
    /// it inline. Used by ctl.iterate (${ITER_INDEX}) and ctl.foreach
    /// (${ITER_ITEM_<field>}). String substitution happens on the raw
    /// JSON text so any prop value can carry templated content; safe
    /// because we substitute INSIDE JSON strings only when the
    /// placeholder is in a string literal already.
    fn run_subpipeline_with_subs(
        &self,
        path: &str,
        subs: &std::collections::HashMap<String, String>,
    ) -> Result<(), EngineError> {
        let mut content = std::fs::read_to_string(path).map_err(|e| {
            EngineError::Config(format!("sub-pipeline: read '{}': {}", path, e))
        })?;
        for (key, val) in subs {
            let placeholder = format!("${{{}}}", key);
            if content.contains(&placeholder) {
                // JSON-escape the value before substitution so embedded
                // quotes / backslashes don't break parsing.
                let escaped: String = val
                    .chars()
                    .flat_map(|c| match c {
                        '"' => vec!['\\', '"'],
                        '\\' => vec!['\\', '\\'],
                        '\n' => vec!['\\', 'n'],
                        '\r' => vec!['\\', 'r'],
                        '\t' => vec!['\\', 't'],
                        c => vec![c],
                    })
                    .collect();
                content = content.replace(&placeholder, &escaped);
            }
        }
        let sub_doc: plan::PipelineDoc = serde_json::from_str(&content).map_err(|e| {
            EngineError::Config(format!("sub-pipeline: parse '{}': {}", path, e))
        })?;
        let result = self.execute_pipeline(&sub_doc);
        if result.status == "ok" {
            Ok(())
        } else {
            Err(EngineError::Query(
                result
                    .error
                    .unwrap_or_else(|| "sub-pipeline failed (no error message)".into()),
            ))
        }
    }

    /// Snowflake SQL API source. POSTs the SELECT, polls the
    /// statementHandle if the server returned async, then walks
    /// resultSetMetaData.partitionInfo[] fetching partitions 1..N
    /// (partition 0 ships inline in the initial response). Each
    /// partition's `data` array is concatenated and materialized
    /// into node_id via read_json_auto.
    fn run_snowflake_source(
        &self,
        db: &Path,
        spec: &SnowflakeSourceSpec,
    ) -> Result<String, EngineError> {
        let base_url = spec.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://{}.snowflakecomputing.com/api/v2/statements",
                spec.account
            )
        });
        let auth_header = build_snowflake_auth_header(&spec.account, &spec.auth)?;
        let is_jwt = matches!(spec.auth, SnowflakeAuth::Jwt { .. });
        let mut body_obj = serde_json::Map::new();
        body_obj.insert("statement".into(), JsonValue::String(spec.query.clone()));
        body_obj.insert("timeout".into(), JsonValue::Number(60.into()));
        if let Some(db) = &spec.database {
            body_obj.insert("database".into(), JsonValue::String(db.clone()));
        }
        if let Some(s) = &spec.schema {
            body_obj.insert("schema".into(), JsonValue::String(s.clone()));
        }
        if let Some(wh) = &spec.warehouse {
            body_obj.insert("warehouse".into(), JsonValue::String(wh.clone()));
        }
        if let Some(role) = &spec.role {
            body_obj.insert("role".into(), JsonValue::String(role.clone()));
        }
        let body = serde_json::to_string(&JsonValue::Object(body_obj))
            .unwrap_or_else(|_| "{}".into());
        let initial = sf_request(&base_url, "POST", &auth_header, is_jwt, Some(&body))?;
        // If the server handed us a statementHandle without data
        // (async path: 202 in HTTP terms, but ureq returns 200/202
        // both as Ok), poll until we see data.
        let mut response = if initial.get("data").is_some() {
            initial
        } else {
            let handle = initial
                .get("statementHandle")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    EngineError::Query(
                        "Snowflake response has neither data nor statementHandle".into(),
                    )
                })?
                .to_string();
            poll_snowflake_until_done(&base_url, &auth_header, is_jwt, &handle)?
        };
        let cols = response
            .pointer("/resultSetMetaData/rowType")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                EngineError::Query("Snowflake response missing resultSetMetaData.rowType".into())
            })?
            .iter()
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(String::from))
            .collect::<Vec<_>>();
        let mut all_data = response
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // Multi-partition: partitionInfo[0] is what we just ate; fetch
        // partitions 1..N. statementHandle is available even in the
        // inline case.
        let partition_count = response
            .pointer("/resultSetMetaData/partitionInfo")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(1);
        if partition_count > 1 {
            let handle = response
                .get("statementHandle")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    EngineError::Query(
                        "Snowflake paged response missing statementHandle".into(),
                    )
                })?
                .to_string();
            for i in 1..partition_count {
                self.check_cancelled()?;
                let part_url = format!("{}/{}?partition={}", base_url, handle, i);
                let part = sf_request(&part_url, "GET", &auth_header, is_jwt, None)?;
                if let Some(part_data) = part.get("data").and_then(|v| v.as_array()) {
                    all_data.extend(part_data.iter().cloned());
                }
            }
        }
        // Pretend warning to silence "response variable unused after
        // reassignment" if all_data didn't grow.
        let _ = &mut response;
        materialize_arrayrows_as_table(db, &spec.node_id, &cols, &all_data)?;
        Ok(format!(
            "snowflake: materialized {} rows ({} partition(s)) into {}",
            all_data.len(),
            partition_count,
            spec.node_id
        ))
    }

    /// Databricks SQL source. POSTs the SELECT, polls for SUCCEEDED
    /// if the server returned PENDING/RUNNING after wait_timeout, then
    /// follows result.next_chunk_internal_link until exhausted. Each
    /// chunk's data_array is concatenated and materialized.
    fn run_databricks_source(
        &self,
        db: &Path,
        spec: &DatabricksSourceSpec,
    ) -> Result<String, EngineError> {
        let base_url = spec.endpoint.clone().unwrap_or_else(|| {
            format!("https://{}/api/2.0/sql/statements/", spec.workspace)
        });
        let auth = format!("Bearer {}", spec.pat);
        let mut body_obj = serde_json::Map::new();
        body_obj.insert("statement".into(), JsonValue::String(spec.query.clone()));
        body_obj.insert(
            "warehouse_id".into(),
            JsonValue::String(spec.warehouse_id.clone()),
        );
        if let Some(c) = &spec.catalog {
            body_obj.insert("catalog".into(), JsonValue::String(c.clone()));
        }
        if let Some(s) = &spec.schema {
            body_obj.insert("schema".into(), JsonValue::String(s.clone()));
        }
        body_obj.insert(
            "wait_timeout".into(),
            JsonValue::String(format!("{}s", spec.wait_timeout_seconds)),
        );
        body_obj.insert(
            "on_wait_timeout".into(),
            JsonValue::String("CONTINUE".into()),
        );
        let body = serde_json::to_string(&JsonValue::Object(body_obj))
            .unwrap_or_else(|_| "{}".into());
        let initial = dbr_request(&base_url, "POST", &auth, Some(&body))?;
        // Poll until SUCCEEDED if we got PENDING/RUNNING back.
        let response = match initial
            .pointer("/status/state")
            .and_then(|v| v.as_str())
            .unwrap_or("SUCCEEDED")
        {
            "SUCCEEDED" => initial,
            "PENDING" | "RUNNING" => {
                let statement_id = initial
                    .get("statement_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        EngineError::Query(
                            "Databricks async response missing statement_id".into(),
                        )
                    })?
                    .to_string();
                let poll_url = format!("{}{}", base_url, statement_id);
                poll_databricks_until_done(&poll_url, &auth)?
            }
            other => {
                let err = initial
                    .pointer("/status/error/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(no message)");
                return Err(EngineError::Query(format!(
                    "Databricks statement state {}: {}",
                    other, err
                )));
            }
        };
        let cols = response
            .pointer("/manifest/schema/columns")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                EngineError::Query(
                    "Databricks response missing manifest.schema.columns".into(),
                )
            })?
            .iter()
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(String::from))
            .collect::<Vec<_>>();
        let mut all_data = response
            .pointer("/result/data_array")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // Follow next_chunk_internal_link until None. The link is a
        // path under the workspace; prepend https://workspace.
        let mut next_link: Option<String> = response
            .pointer("/result/next_chunk_internal_link")
            .and_then(|v| v.as_str())
            .map(String::from);
        let mut chunks = 1_usize;
        while let Some(link) = next_link {
            self.check_cancelled()?;
            // If endpoint override is in play (tests), prepend the
            // override's scheme+host; otherwise use the workspace host.
            let chunk_url = if let Some(ep) = &spec.endpoint {
                // Extract "scheme://host[:port]" from ep so we can
                // append the relative chunk link as-is.
                let prefix_end = ep
                    .find("://")
                    .map(|i| {
                        let after = &ep[i + 3..];
                        i + 3 + after.find('/').unwrap_or(after.len())
                    })
                    .unwrap_or(ep.len());
                format!("{}{}", &ep[..prefix_end], link)
            } else {
                format!("https://{}{}", spec.workspace, link)
            };
            let chunk = dbr_request(&chunk_url, "GET", &auth, None)?;
            if let Some(d) = chunk.get("data_array").and_then(|v| v.as_array()) {
                all_data.extend(d.iter().cloned());
                chunks += 1;
            }
            next_link = chunk
                .get("next_chunk_internal_link")
                .and_then(|v| v.as_str())
                .map(String::from);
        }
        materialize_arrayrows_as_table(db, &spec.node_id, &cols, &all_data)?;
        Ok(format!(
            "databricks: materialized {} rows ({} chunk(s)) into {}",
            all_data.len(),
            chunks,
            spec.node_id
        ))
    }

    /// Databricks SQL sink. Same multi-row INSERT batching as Snowflake;
    /// difference is the URL shape, the body field names (warehouse_id,
    /// catalog/schema, wait_timeout, on_wait_timeout), and identifier
    /// quoting uses backticks instead of double quotes.
    fn run_databricks_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &DatabricksSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("databricks: 0 rows to insert into {}", spec.table));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => return Err(EngineError::Query("databricks: upstream rows aren't JSON objects".into())),
        };
        // Build the qualified target. Catalog/schema both optional;
        // Databricks accepts 2-part (schema.table) or 3-part naming
        // (catalog.schema.table) when ambient catalog/schema is set in
        // the request body.
        let qualified = match (&spec.catalog, &spec.schema) {
            (Some(c), Some(s)) => format!(
                "{}.{}.{}",
                db_quote_ident(c),
                db_quote_ident(s),
                db_quote_ident(&spec.table)
            ),
            (None, Some(s)) => format!(
                "{}.{}",
                db_quote_ident(s),
                db_quote_ident(&spec.table)
            ),
            _ => db_quote_ident(&spec.table),
        };
        let cols_list = cols
            .iter()
            .map(|c| db_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let url = spec.endpoint.clone().unwrap_or_else(|| {
            format!("https://{}/api/2.0/sql/statements/", spec.workspace)
        });
        let mut total_inserted = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_obj = row.as_object();
                    let vals: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let v = row_obj
                                .and_then(|o| o.get(c))
                                .unwrap_or(&JsonValue::Null);
                            json_to_sql_literal(v)
                        })
                        .collect();
                    format!("({})", vals.join(", "))
                })
                .collect();
            let stmt = format!(
                "INSERT INTO {} ({}) VALUES {}",
                qualified,
                cols_list,
                values.join(", ")
            );
            let mut body_obj = serde_json::Map::new();
            body_obj.insert("statement".into(), JsonValue::String(stmt));
            body_obj.insert(
                "warehouse_id".into(),
                JsonValue::String(spec.warehouse_id.clone()),
            );
            if let Some(c) = &spec.catalog {
                body_obj.insert("catalog".into(), JsonValue::String(c.clone()));
            }
            if let Some(s) = &spec.schema {
                body_obj.insert("schema".into(), JsonValue::String(s.clone()));
            }
            body_obj.insert(
                "wait_timeout".into(),
                JsonValue::String(format!("{}s", spec.wait_timeout_seconds)),
            );
            body_obj.insert(
                "on_wait_timeout".into(),
                JsonValue::String("CONTINUE".into()),
            );
            let body = serde_json::to_string(&JsonValue::Object(body_obj))
                .unwrap_or_else(|_| "{}".into());
            let req = ureq::post(&url)
                .set("Authorization", &format!("Bearer {}", spec.pat))
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            match req.send_string(&body) {
                Ok(_) => total_inserted += chunk.len(),
                Err(ureq::Error::Status(code, response)) => {
                    let body = response.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "Databricks HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "Databricks HTTP transport to {}: {}",
                        url, e
                    )));
                }
            }
        }
        Ok(format!(
            "databricks: inserted {} rows into {}",
            total_inserted, spec.table
        ))
    }

    /// Full-Text Search runs in two CLI invocations sharing the same
    /// temp DB file. The first stages the upstream into a permanent
    /// table; the second builds the BM25 index and the final node
    /// table. The split is needed for DuckDB v1.5+ where the fts
    /// PRAGMA can't see tables created in the same -c invocation; on
    /// v1.4 it just costs one extra CLI spawn.
    fn run_text_search(
        &self,
        db: &Path,
        secret_prefix: &str,
        node_id: &str,
        spec: &plan::TextSearchSpec,
    ) -> Result<String, EngineError> {
        let staging = plan::quote_ident(&spec.staging_table);
        let upstream = plan::quote_ident(&spec.from_view);
        let node_q = plan::quote_ident(node_id);
        let id_col_q = plan::quote_ident(&spec.id_col);
        let output_q = plan::quote_ident(&spec.output_col);

        // Phase 1: stage upstream into a named table that the next CLI
        // invocation will see.
        let stage_sql = format!(
            "{secret}INSTALL fts; LOAD fts; \
             DROP TABLE IF EXISTS {staging}; \
             CREATE TABLE {staging} AS SELECT * FROM {upstream};",
            secret = secret_prefix,
            staging = staging,
            upstream = upstream,
        );
        self.run(Some(db), &stage_sql, false)?;

        // Phase 2: PRAGMA create_fts_index sees the staged table from
        // disk; the same invocation then runs the BM25 SELECT.
        let text_args = spec
            .text_cols
            .iter()
            .map(|c| format!("'{}'", c.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let index_schema = format!("fts_main_{}", spec.staging_table);
        let match_expr = format!(
            "{}.match_bm25({}, '{}')",
            index_schema,
            id_col_q,
            spec.query.replace('\'', "''")
        );
        let order_limit = match spec.top_k {
            Some(k) => format!(" ORDER BY {} DESC LIMIT {}", output_q, k),
            None => String::new(),
        };
        let index_sql = format!(
            "{secret}INSTALL fts; LOAD fts; \
             PRAGMA create_fts_index('{staging_raw}', '{id_col}', {text_args}); \
             CREATE OR REPLACE TABLE {node} AS \
               SELECT *, {match_expr} AS {output_q} FROM {staging} \
               WHERE {match_expr} IS NOT NULL{order_limit};",
            secret = secret_prefix,
            staging_raw = spec.staging_table.replace('\'', "''"),
            id_col = spec.id_col.replace('\'', "''"),
            text_args = text_args,
            node = node_q,
            match_expr = match_expr,
            output_q = output_q,
            staging = staging,
            order_limit = order_limit,
        );
        self.run(Some(db), &index_sql, false)
    }

    fn count_rows(&self, db: &Path, name: &str) -> Result<u64, EngineError> {
        let sql = format!("SELECT COUNT(*) AS n FROM {};", plan::quote_ident(name));
        let rows = self.run_rows(Some(db), &sql)?;
        let n = rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|x| x.max(0) as u64)))
            .unwrap_or(0);
        Ok(n)
    }

    fn preview_table(&self, db: &Path, name: &str) -> Result<NodePreview, EngineError> {
        let q = plan::quote_ident(name);
        let cols = self.run_rows(Some(db), &format!("DESCRIBE {};", q))?;
        let schema: Vec<Column> = cols.iter().filter_map(parse_describe_row).collect();
        let rows = self
            .run_rows(Some(db), &format!("SELECT * FROM {} LIMIT {};", q, PREVIEW_ROW_LIMIT))
            .unwrap_or_default();
        Ok(NodePreview {
            node_id: name.to_string(),
            columns: schema,
            rows,
        })
    }
}

/// Removes the temp run database (and its WAL) when dropped.
struct TempDbGuard(PathBuf);
impl Drop for TempDbGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let mut wal = self.0.clone().into_os_string();
        wal.push(".wal");
        let _ = std::fs::remove_file(PathBuf::from(wal));
    }
}

/// Per-process counter making each run's temp DB path unique even when
/// the wall clock does not advance between runs.
static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Parse an RFC 5988 Link header and return the URL of the rel="next"
/// entry, if present. Format example:
///   Link: <https://api.example.com/items?page=2>; rel="next", <...>; rel="prev"
fn parse_link_next(header: &str) -> Option<String> {
    for part in header.split(',') {
        let p = part.trim();
        if !p.starts_with('<') {
            continue;
        }
        let close = match p.find('>') {
            Some(i) => i,
            None => continue,
        };
        let url = &p[1..close];
        let rest = &p[close + 1..];
        // Look for rel="next" anywhere in the params (case-insensitive).
        let rest_lower = rest.to_ascii_lowercase();
        if rest_lower.contains("rel=\"next\"") || rest_lower.contains("rel=next") {
            return Some(url.to_string());
        }
    }
    None
}

/// URL-encode a string for use as a query parameter value.
/// Conservative escaping: alphanumerics + safe characters pass
/// through; everything else gets %XX. Avoids pulling in the `url`
/// crate just for this.
fn urlencode_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.as_bytes() {
        let c = *byte as char;
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
            out.push(c);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}

/// Materialize a Vec<JsonValue> of row objects as a DuckDB table.
/// Variant of materialize_arrayrows_as_table for sources whose
/// response is already object-shaped (no column zipping needed).
fn materialize_jsonobjects_as_table(
    db: &Path,
    node_id: &str,
    rows: &[JsonValue],
) -> Result<(), EngineError> {
    let json_text = serde_json::to_string(&JsonValue::Array(rows.to_vec()))
        .map_err(|e| EngineError::Query(format!("rest source: JSON encode: {}", e)))?;
    let tmp_path = unique_rest_tmp_path(node_id);
    std::fs::write(&tmp_path, json_text)
        .map_err(|e| EngineError::Query(format!("rest source: write tmp file: {}", e)))?;
    let sql = format!(
        "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_json_auto('{}', format='array')",
        plan::quote_ident(node_id),
        tmp_path.display().to_string().replace('\\', "/").replace('\'', "''")
    );
    rest_source_apply(db, &sql)
}

/// Unique temp path for a REST/Snowflake/Databricks source's
/// materialization. Includes node_id + process id + nanoseconds +
/// thread id so cargo test's parallel runs can't clobber each
/// other when two tests reuse the same node_id.
fn unique_rest_tmp_path(node_id: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let tid = format!("{:?}", std::thread::current().id())
        .replace(|c: char| !c.is_ascii_alphanumeric(), "");
    std::env::temp_dir().join(format!(
        "duckle-rest-{}-{}-{}-{}.json",
        node_id,
        std::process::id(),
        nanos,
        tid,
    ))
}

/// Shared helper for src.snowflake / src.databricks: take an
/// array-of-arrays response + column names, emit a JSON array of
/// row objects to a temp file, and CREATE OR REPLACE TABLE node_id
/// FROM read_json_auto('temp.json', format='array'). DuckDB infers
/// the types from the JSON content - good enough for downstream
/// stages to read the result like any other source.
fn materialize_arrayrows_as_table(
    db: &Path,
    node_id: &str,
    cols: &[String],
    rows: &[JsonValue],
) -> Result<(), EngineError> {
    let mut serialized = Vec::with_capacity(rows.len());
    for row in rows {
        let arr = row.as_array();
        let mut obj = serde_json::Map::new();
        for (i, name) in cols.iter().enumerate() {
            let v = arr
                .and_then(|a| a.get(i))
                .cloned()
                .unwrap_or(JsonValue::Null);
            obj.insert(name.clone(), v);
        }
        serialized.push(JsonValue::Object(obj));
    }
    let json_text = serde_json::to_string(&JsonValue::Array(serialized))
        .map_err(|e| EngineError::Query(format!("rest source: JSON encode: {}", e)))?;
    let tmp_path = unique_rest_tmp_path(node_id);
    std::fs::write(&tmp_path, json_text).map_err(|e| {
        EngineError::Query(format!("rest source: write tmp file: {}", e))
    })?;
    let sql = format!(
        "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_json_auto('{}', format='array')",
        plan::quote_ident(node_id),
        tmp_path.display().to_string().replace('\\', "/").replace('\'', "''")
    );
    rest_source_apply(db, &sql)
}

/// Run a single SQL statement against `db` using the CLI helper used
/// elsewhere. Tiny shim used by materialize_arrayrows_as_table to
/// avoid plumbing &self through the free helper.
fn rest_source_apply(db: &Path, sql: &str) -> Result<(), EngineError> {
    use std::process::Command;
    let binary = std::env::var("DUCKLE_DUCKDB_BIN").map_err(|_| {
        EngineError::Config("DUCKLE_DUCKDB_BIN not set (engine couldn't run rest source materialize)".into())
    })?;
    let output = Command::new(&binary)
        .arg(db.to_string_lossy().to_string())
        .arg("-c")
        .arg(sql)
        .output()
        .map_err(|e| EngineError::Query(format!("duckdb CLI for rest source: {}", e)))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(EngineError::Query(format!(
            "rest source materialize failed: {}",
            stderr.chars().take(500).collect::<String>()
        )));
    }
    Ok(())
}

/// Snowflake SQL API request - shared by run_snowflake_source and
/// its polling/partition helpers. method = "POST" or "GET"; for GET
/// body is None.
fn sf_request(
    url: &str,
    method: &str,
    auth_header: &str,
    is_jwt: bool,
    body: Option<&str>,
) -> Result<JsonValue, EngineError> {
    let mut req = ureq::request(method, url)
        .set("Authorization", auth_header)
        .set("Accept", "application/json");
    if body.is_some() {
        req = req.set("Content-Type", "application/json");
    }
    if is_jwt {
        req = req.set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
    }
    let resp = match body {
        Some(b) => req.send_string(b),
        None => req.call(),
    };
    match resp {
        Ok(r) => r
            .into_json()
            .map_err(|e| EngineError::Query(format!("Snowflake response not JSON: {}", e))),
        Err(ureq::Error::Status(code, r)) => {
            let body = r.into_string().unwrap_or_default();
            Err(EngineError::Query(format!(
                "Snowflake HTTP {} from {}: {}",
                code,
                url,
                body.chars().take(300).collect::<String>()
            )))
        }
        Err(e) => Err(EngineError::Query(format!(
            "Snowflake HTTP transport to {}: {}",
            url, e
        ))),
    }
}

/// Snowflake async polling: GET /api/v2/statements/<handle> until
/// the response carries `data`. Backoff is fixed 500ms; cap at 60
/// iterations (~30s total) before bailing.
fn poll_snowflake_until_done(
    base_url: &str,
    auth_header: &str,
    is_jwt: bool,
    handle: &str,
) -> Result<JsonValue, EngineError> {
    let poll_url = format!("{}/{}", base_url, handle);
    for _ in 0..60 {
        let resp = sf_request(&poll_url, "GET", auth_header, is_jwt, None)?;
        if resp.get("data").is_some() {
            return Ok(resp);
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(EngineError::Query(format!(
        "Snowflake statement {} did not complete within 30s of polling",
        handle
    )))
}

/// Databricks Statement API request - shared by source + chunk
/// follower. method = "POST" or "GET".
fn dbr_request(
    url: &str,
    method: &str,
    auth_header: &str,
    body: Option<&str>,
) -> Result<JsonValue, EngineError> {
    let mut req = ureq::request(method, url)
        .set("Authorization", auth_header)
        .set("Accept", "application/json");
    if body.is_some() {
        req = req.set("Content-Type", "application/json");
    }
    let resp = match body {
        Some(b) => req.send_string(b),
        None => req.call(),
    };
    match resp {
        Ok(r) => r
            .into_json()
            .map_err(|e| EngineError::Query(format!("Databricks response not JSON: {}", e))),
        Err(ureq::Error::Status(code, r)) => {
            let body = r.into_string().unwrap_or_default();
            Err(EngineError::Query(format!(
                "Databricks HTTP {} from {}: {}",
                code,
                url,
                body.chars().take(300).collect::<String>()
            )))
        }
        Err(e) => Err(EngineError::Query(format!(
            "Databricks HTTP transport to {}: {}",
            url, e
        ))),
    }
}

/// Databricks polling: GET .../statements/<id> until status.state
/// becomes SUCCEEDED. Bails on FAILED / CANCELED / CLOSED. Cap at
/// 60 iterations (~30s).
fn poll_databricks_until_done(
    poll_url: &str,
    auth_header: &str,
) -> Result<JsonValue, EngineError> {
    for _ in 0..60 {
        let resp = dbr_request(poll_url, "GET", auth_header, None)?;
        let state = resp
            .pointer("/status/state")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN")
            .to_string();
        match state.as_str() {
            "SUCCEEDED" => return Ok(resp),
            "PENDING" | "RUNNING" => {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            other => {
                let err = resp
                    .pointer("/status/error/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(no message)");
                return Err(EngineError::Query(format!(
                    "Databricks statement state {}: {}",
                    other, err
                )));
            }
        }
    }
    Err(EngineError::Query(format!(
        "Databricks statement at {} did not succeed within 30s of polling",
        poll_url
    )))
}

/// Snowflake identifier quoting: double quotes, internal quotes
/// doubled, and the identifier is treated case-sensitive.
fn sf_quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Build the Authorization header value for a Snowflake request.
/// PAT: just "Bearer <token>". JWT: read the PEM private key,
/// compute the public-key fingerprint Snowflake wants
/// (SHA256:<base64(SHA-256 of SubjectPublicKeyInfo DER)>), build the
/// claims (iss = "ACCOUNT.USER.SHA256:fp", sub = "ACCOUNT.USER",
/// iat = now, exp = now + 3600), sign RS256, and prefix with
/// "Bearer ". Snowflake also wants the X-Snowflake-Authorization-
/// Token-Type: KEYPAIR_JWT header for JWT requests, set at the
/// dispatch point.
fn build_snowflake_auth_header(
    account: &str,
    auth: &SnowflakeAuth,
) -> Result<String, EngineError> {
    match auth {
        SnowflakeAuth::Pat { token } => Ok(format!("Bearer {}", token)),
        SnowflakeAuth::Jwt { user, private_key_pem } => {
            use base64::Engine as _;
            use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
            use rsa::RsaPrivateKey;
            use sha2::{Digest, Sha256};
            let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem).map_err(|e| {
                EngineError::Config(format!("snowflake jwt: bad PEM: {}", e))
            })?;
            let public_key = private_key.to_public_key();
            let der = public_key
                .to_public_key_der()
                .map_err(|e| EngineError::Config(format!("snowflake jwt: DER encode: {}", e)))?;
            let fp = Sha256::digest(der.as_bytes());
            let fp_b64 = base64::engine::general_purpose::STANDARD.encode(fp);
            let account_upper = account.to_uppercase();
            let user_upper = user.to_uppercase();
            let qualified_user = format!("{}.{}", account_upper, user_upper);
            let iss = format!("{}.SHA256:{}", qualified_user, fp_b64);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let claims = serde_json::json!({
                "iss": iss,
                "sub": qualified_user,
                "iat": now,
                "exp": now + 3600,
            });
            let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
            let key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
                .map_err(|e| EngineError::Config(format!("snowflake jwt: key encode: {}", e)))?;
            let token = jsonwebtoken::encode(&header, &claims, &key)
                .map_err(|e| EngineError::Config(format!("snowflake jwt: sign: {}", e)))?;
            Ok(format!("Bearer {}", token))
        }
    }
}

/// Databricks SQL identifier quoting: backticks, internal backticks
/// doubled. Works in both Spark SQL and ANSI mode.
fn db_quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// SQL Server identifier quoting: square brackets, internal `]` doubled.
fn ss_quote_ident(s: &str) -> String {
    format!("[{}]", s.replace(']', "]]"))
}

/// Best-effort scylla::CqlValue -> JsonValue conversion. Covers the
/// common scalar types; falls back to JSON string for anything we
/// don't know about (lists/sets/maps stringify as Display).
fn cql_value_to_json(v: &scylla::frame::response::result::CqlValue) -> JsonValue {
    use scylla::frame::response::result::CqlValue;
    match v {
        CqlValue::Boolean(b) => JsonValue::Bool(*b),
        CqlValue::TinyInt(n) => JsonValue::from(*n as i64),
        CqlValue::SmallInt(n) => JsonValue::from(*n as i64),
        CqlValue::Int(n) => JsonValue::from(*n as i64),
        CqlValue::BigInt(n) => JsonValue::from(*n),
        CqlValue::Counter(c) => JsonValue::from(c.0),
        CqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        CqlValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        CqlValue::Text(s) | CqlValue::Ascii(s) => JsonValue::String(s.clone()),
        CqlValue::Uuid(u) => JsonValue::String(u.to_string()),
        CqlValue::Timeuuid(u) => JsonValue::String(u.to_string()),
        CqlValue::Empty => JsonValue::Null,
        other => JsonValue::String(format!("{:?}", other)),
    }
}

/// Render a serde_json::Value as a Snowflake SQL literal.
/// - NULL  -> NULL
/// - bool  -> TRUE / FALSE
/// - num   -> verbatim
/// - str   -> 'escaped' (single quotes doubled)
/// - obj/arr -> PARSE_JSON('escaped json') so it lands in a VARIANT column
fn json_to_sql_literal(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "NULL".into(),
        JsonValue::Bool(true) => "TRUE".into(),
        JsonValue::Bool(false) => "FALSE".into(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            let j = serde_json::to_string(v).unwrap_or_else(|_| "null".into());
            format!("PARSE_JSON('{}')", j.replace('\'', "''"))
        }
    }
}

/// True for a local filesystem path (not a cloud / http URI).
fn is_local_path(p: &str) -> bool {
    let lower = p.to_ascii_lowercase();
    !["s3://", "gs://", "gcs://", "az://", "azure://", "http://", "https://"]
        .iter()
        .any(|scheme| lower.starts_with(scheme))
}

/// Parse the (possibly multiple) top-level JSON arrays the DuckDB CLI
/// prints in `-json` mode.
fn parse_json_arrays(s: &str) -> Vec<Vec<JsonValue>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let stream = serde_json::Deserializer::from_str(trimmed).into_iter::<JsonValue>();
    for value in stream {
        match value {
            Ok(JsonValue::Array(a)) => out.push(a),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    out
}

/// Turn one DuckDB `DESCRIBE` row into a Column.
fn parse_describe_row(v: &JsonValue) -> Option<Column> {
    let name = v.get("column_name")?.as_str()?.to_string();
    let type_name = v
        .get("column_type")
        .and_then(JsonValue::as_str)
        .unwrap_or("VARCHAR");
    let nullable = v
        .get("null")
        .and_then(JsonValue::as_str)
        .map(|s| !s.eq_ignore_ascii_case("NO"))
        .unwrap_or(true);
    Some(Column {
        name,
        data_type: map_duckdb_type(type_name),
        nullable,
        primary_key: None,
    })
}

fn map_duckdb_type(t: &str) -> DataType {
    let upper = t.to_uppercase();
    let base = upper.split('(').next().unwrap_or(&upper).trim();
    match base {
        "BOOLEAN" | "BOOL" => DataType::Bool,
        "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "INT4" | "INT2" | "UTINYINT" | "USMALLINT"
        | "UINTEGER" => DataType::Int32,
        "BIGINT" | "INT8" | "HUGEINT" | "UBIGINT" => DataType::Int64,
        "REAL" | "FLOAT" | "FLOAT4" => DataType::Float32,
        "DOUBLE" | "FLOAT8" => DataType::Float64,
        "DECIMAL" | "NUMERIC" => DataType::Decimal,
        "DATE" => DataType::Date,
        "TIME" => DataType::Time,
        "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_NS" | "TIMESTAMP_US"
        | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => DataType::Timestamp,
        "JSON" | "MAP" | "STRUCT" | "LIST" | "ARRAY" => DataType::Json,
        "BLOB" | "VARBINARY" => DataType::Binary,
        _ => DataType::String,
    }
}

pub(crate) fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build a `CREATE OR REPLACE SECRET` statement for a cloud format if
/// the options carry credentials. `secret_name` keeps per-source
/// secrets distinct so connections don't trample each other.
/// Compose the upsert + cleanup SQL that runs natively on the target
/// DB (through postgres_execute / mysql_execute), reading from the
/// staging table we just populated via ATTACH. Identifiers are native
/// to each family: double-quoted for Postgres, backticks for MySQL.
fn build_native_upsert_sql(
    spec: &plan::UpsertSpec,
    set_cols: &[&String],
    target_native: &str,
    staging_native: &str,
) -> String {
    match spec.family {
        plan::UpsertFamily::Postgres => {
            let key_list = spec
                .conflict_cols
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ");
            let conflict = if set_cols.is_empty() {
                format!("ON CONFLICT ({}) DO NOTHING", key_list)
            } else {
                let set_clause = set_cols
                    .iter()
                    .map(|c| {
                        let q = format!("\"{}\"", c.replace('"', "\"\""));
                        format!("{q} = EXCLUDED.{q}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ON CONFLICT ({}) DO UPDATE SET {}", key_list, set_clause)
            };
            format!(
                "INSERT INTO {target} SELECT * FROM {staging} {conflict}; DROP TABLE {staging};",
                target = target_native,
                staging = staging_native,
                conflict = conflict
            )
        }
        plan::UpsertFamily::MySql => {
            // MySQL relies on the target's existing UNIQUE/PRIMARY KEY.
            // INSERT IGNORE is the fallback when there are no non-key
            // columns to update.
            if set_cols.is_empty() {
                format!(
                    "INSERT IGNORE INTO {target} SELECT * FROM {staging}; DROP TABLE {staging};",
                    target = target_native,
                    staging = staging_native
                )
            } else {
                let set_clause = set_cols
                    .iter()
                    .map(|c| format!("`{c}` = VALUES(`{c}`)"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "INSERT INTO {target} SELECT * FROM {staging} ON DUPLICATE KEY UPDATE {set}; DROP TABLE {staging};",
                    target = target_native,
                    staging = staging_native,
                    set = set_clause
                )
            }
        }
    }
}

pub(crate) fn secret_statement(
    format: &str,
    secret_name: &str,
    options: &JsonValue,
) -> Option<String> {
    let get = |k: &str| options.get(k).and_then(JsonValue::as_str);
    let sane = secret_name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    match format {
        "s3" => {
            let key = get("accessKey")?;
            let sec = get("secretKey")?;
            let region = get("region").unwrap_or("us-east-1");
            let session = get("sessionToken");
            // S3-compatible (MinIO / R2 / B2) sets endpoint + url_style +
            // use_ssl. Empty / missing values are skipped so plain AWS S3
            // keeps its defaults.
            let endpoint = get("endpoint").filter(|s| !s.is_empty());
            let url_style = get("urlStyle").filter(|s| !s.is_empty());
            let use_ssl = get("useSsl").filter(|s| !s.is_empty());
            let mut parts = vec![
                "TYPE S3".to_string(),
                format!("KEY_ID '{}'", sql_escape(key)),
                format!("SECRET '{}'", sql_escape(sec)),
                format!("REGION '{}'", sql_escape(region)),
            ];
            if let Some(s) = session {
                parts.push(format!("SESSION_TOKEN '{}'", sql_escape(s)));
            }
            if let Some(e) = endpoint {
                parts.push(format!("ENDPOINT '{}'", sql_escape(e)));
            }
            if let Some(u) = url_style {
                parts.push(format!("URL_STYLE '{}'", sql_escape(u)));
            }
            if let Some(s) = use_ssl {
                // DuckDB takes USE_SSL as a bool literal, not a string.
                parts.push(format!("USE_SSL {}", s));
            }
            Some(format!(
                "CREATE OR REPLACE SECRET secret_{} ({});",
                sane,
                parts.join(", ")
            ))
        }
        "gcs" => {
            let key = get("accessKey")?;
            let sec = get("secretKey")?;
            Some(format!(
                "CREATE OR REPLACE SECRET secret_{} (TYPE GCS, KEY_ID '{}', SECRET '{}');",
                sane,
                sql_escape(key),
                sql_escape(sec)
            ))
        }
        "azureblob" => {
            let account = get("accountName")?;
            let key = get("accountKey")?;
            Some(format!(
                "CREATE OR REPLACE SECRET secret_{} (TYPE AZURE, CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net');",
                sane,
                sql_escape(account),
                sql_escape(key)
            ))
        }
        _ => None,
    }
}

/// CREATE SECRET statements for every cloud source/sink with creds.
pub(crate) fn collect_pipeline_secrets(doc: &PipelineDoc) -> Vec<String> {
    let mut out = Vec::new();
    for node in &doc.nodes {
        let id = match node.data.component_id.as_deref() {
            Some(s) => s,
            None => continue,
        };
        let format = match id {
            // S3-compatible (plain S3 + MinIO / R2 / B2) all use the same
            // CREATE SECRET (TYPE S3) machinery; the MinIO / R2 / B2
            // variants add ENDPOINT + URL_STYLE in the form.
            "src.s3" | "snk.s3" | "src.minio" | "src.r2" | "src.b2" => "s3",
            "src.gcs" | "snk.gcs" => "gcs",
            "src.azureblob" | "snk.azureblob" => "azureblob",
            _ => continue,
        };
        if let Some(props) = node.data.properties.as_ref() {
            if let Some(stmt) = secret_statement(format, &node.id, props) {
                out.push(stmt);
            }
        }
    }
    out
}

// ---- Streaming events + run result -------------------------------------

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineEvent {
    Started {
        total_stages: u32,
    },
    StageStarted {
        node_id: String,
        label: String,
        kind: String,
    },
    StageFinished {
        node_id: String,
        kind: String,
        status: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        rows: Option<u64>,
        duration_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Cancelled,
    Finished {
        status: String,
        duration_ms: u64,
    },
}

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub status: String,
    pub duration_ms: u64,
    pub nodes: std::collections::BTreeMap<String, NodeRunStatus>,
    pub preview: Vec<NodePreview>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl RunResult {
    fn failed(start: Instant, error: String) -> Self {
        RunResult {
            status: "error".into(),
            duration_ms: start.elapsed().as_millis() as u64,
            nodes: Default::default(),
            preview: Vec::new(),
            error: Some(error),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct NodeRunStatus {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NodePreview {
    pub node_id: String,
    pub columns: Vec<Column>,
    pub rows: Vec<JsonValue>,
}

/// SQL for a single stage - returned by the `compile_pipeline` command
/// so the frontend can show / copy the generated SQL without running.
#[derive(Debug, Serialize)]
pub struct StageSql {
    pub node_id: String,
    pub label: String,
    pub kind: String,
    pub sql: String,
}

pub fn compile_pipeline_sql(doc: &PipelineDoc) -> Result<Vec<StageSql>, EngineError> {
    let compiled = plan::compile(doc)?;
    Ok(compiled
        .stages
        .into_iter()
        .map(|s| StageSql {
            node_id: s.node_id,
            label: s.label,
            kind: match s.kind {
                StageKind::Sink => "sink".into(),
                StageKind::View => "view".into(),
            },
            sql: s.sql,
        })
        .collect())
}

/// Finalize an XML element being popped from the stack: convert it
/// to a JSON value, push to rows if its path matches row_path, and
/// merge it into its parent (multiple same-named children collapse
/// to an array). Standalone (not a method) so the borrow checker
/// doesn't complain about &mut stack + &mut rows at the same time.
fn xml_close_element(
    stack: &mut Vec<(String, serde_json::Map<String, JsonValue>, String)>,
    rows: &mut Vec<JsonValue>,
    row_path: &[String],
    name: &str,
    mut builder: serde_json::Map<String, JsonValue>,
    text: String,
) {
    let text_trimmed = text.trim().to_string();
    let value: JsonValue = if builder.is_empty() && !text_trimmed.is_empty() {
        JsonValue::String(text_trimmed)
    } else if builder.is_empty() {
        JsonValue::Null
    } else {
        if !text_trimmed.is_empty() {
            builder.insert("_text".into(), JsonValue::String(text_trimmed));
        }
        JsonValue::Object(builder)
    };

    // Check if (stack path + name) ends with row_path. Empty row_path
    // matches every element - useful for "every immediate child" type
    // use cases when combined with a single-segment path.
    let mut current_path: Vec<&str> = stack.iter().map(|(n, _, _)| n.as_str()).collect();
    current_path.push(name);
    let matches = if row_path.is_empty() {
        // No filter - match every direct child of the root only, to
        // avoid emitting nested structures as separate rows.
        current_path.len() == 1
    } else {
        current_path.len() >= row_path.len()
            && current_path[current_path.len() - row_path.len()..]
                .iter()
                .zip(row_path.iter())
                .all(|(a, b)| *a == b.as_str())
    };

    if matches {
        rows.push(value.clone());
    }

    if let Some((_, parent_builder, _)) = stack.last_mut() {
        match parent_builder.get_mut(name) {
            Some(JsonValue::Array(arr)) => arr.push(value),
            Some(existing) => {
                let prev = std::mem::replace(existing, JsonValue::Null);
                *existing = JsonValue::Array(vec![prev, value]);
            }
            None => {
                parent_builder.insert(name.to_string(), value);
            }
        }
    }
}

/// Convert a JSON value into an apache-avro Value matching the
/// shapes the inferred schemas can hold. Objects + arrays JSON-
/// stringify into a String field since the inferred schema treats
/// them as strings.
fn json_to_avro_value(v: &JsonValue) -> apache_avro::types::Value {
    use apache_avro::types::Value as A;
    match v {
        JsonValue::Null => A::Null,
        JsonValue::Bool(b) => A::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                A::Long(i)
            } else if let Some(f) = n.as_f64() {
                A::Double(f)
            } else {
                A::String(n.to_string())
            }
        }
        JsonValue::String(s) => A::String(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            A::String(serde_json::to_string(v).unwrap_or_default())
        }
    }
}

/// Infer an Avro JSON-schema type for a single JSON value. Used by
/// snk.avro when schemaJson isn't supplied. Numeric values get the
/// most-permissive numeric type (double); strings stay string;
/// booleans stay boolean; nulls become "null"; everything else
/// (objects, arrays) falls back to string with the JSON encoding.
fn infer_avro_field_type(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Null => JsonValue::String("null".into()),
        JsonValue::Bool(_) => JsonValue::String("boolean".into()),
        JsonValue::Number(n) => {
            if n.is_i64() {
                JsonValue::String("long".into())
            } else {
                JsonValue::String("double".into())
            }
        }
        JsonValue::String(_) => JsonValue::String("string".into()),
        JsonValue::Array(_) | JsonValue::Object(_) => JsonValue::String("string".into()),
    }
}

/// Parse `git log -z --pretty=format:%H%x09%h%x09%an%x09%ae%x09%ad%x09%s`
/// output. Records are NUL-separated; fields are TAB-separated. Subjects
/// may contain anything except NUL.
fn parse_git_log(bytes: &[u8]) -> Vec<JsonValue> {
    let mut out: Vec<JsonValue> = Vec::new();
    for rec in bytes.split(|b| *b == 0) {
        if rec.is_empty() {
            continue;
        }
        let s = String::from_utf8_lossy(rec);
        let parts: Vec<&str> = s.splitn(6, '\t').collect();
        if parts.len() < 6 {
            continue;
        }
        let mut row = serde_json::Map::new();
        row.insert("hash".into(), JsonValue::String(parts[0].to_string()));
        row.insert("short_hash".into(), JsonValue::String(parts[1].to_string()));
        row.insert(
            "author_name".into(),
            JsonValue::String(parts[2].to_string()),
        );
        row.insert(
            "author_email".into(),
            JsonValue::String(parts[3].to_string()),
        );
        row.insert("date".into(), JsonValue::String(parts[4].to_string()));
        row.insert("subject".into(), JsonValue::String(parts[5].to_string()));
        out.push(JsonValue::Object(row));
    }
    out
}

/// Parse `git ls-tree -r -z --long <rev>` output. Records are NUL-
/// separated; each record is `<mode> <type> <hash> <size>\t<path>`.
fn parse_git_ls_tree(bytes: &[u8], max_rows: usize) -> Vec<JsonValue> {
    let mut out: Vec<JsonValue> = Vec::new();
    for rec in bytes.split(|b| *b == 0) {
        if rec.is_empty() {
            continue;
        }
        if out.len() >= max_rows {
            break;
        }
        let s = String::from_utf8_lossy(rec);
        let mut split = s.splitn(2, '\t');
        let meta = split.next().unwrap_or("");
        let path = split.next().unwrap_or("");
        let meta_parts: Vec<&str> = meta.split_whitespace().collect();
        if meta_parts.len() < 4 {
            continue;
        }
        let size: JsonValue = meta_parts[3]
            .parse::<i64>()
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null);
        let mut row = serde_json::Map::new();
        row.insert("mode".into(), JsonValue::String(meta_parts[0].to_string()));
        row.insert("type".into(), JsonValue::String(meta_parts[1].to_string()));
        row.insert("hash".into(), JsonValue::String(meta_parts[2].to_string()));
        row.insert("size".into(), size);
        row.insert("path".into(), JsonValue::String(path.to_string()));
        out.push(JsonValue::Object(row));
    }
    out
}
