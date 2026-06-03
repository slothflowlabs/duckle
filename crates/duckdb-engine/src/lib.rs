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
    quote_ident, AiChunkSpec, AiClassifySpec, AiDedupeSpec, AiEmbedSpec, AiLlmSpec, AiPiiSpec,
    AvroSinkSpec, AvroSourceSpec, CassandraSinkSpec, CassandraSourceSpec, ClickHouseSinkSpec,
    ClickHouseSourceSpec, ClipboardSourceSpec, DatabricksSinkSpec, DatabricksSourceSpec,
    DynamoDbSourceSpec, ElasticSourceSpec, EmailSinkSpec, EmailSourceSpec, FormatFileSinkSpec,
    FormatFileSourceSpec, FormatKind, FtpSourceSpec, GitSourceSpec, JavaScriptSpec, KafkaSinkSpec,
    KafkaSourceSpec, KinesisSourceSpec, MilvusSourceSpec, MongoSinkSpec, MongoSourceSpec,
    NatsSinkSpec, NatsSourceSpec, OracleSinkSpec, OracleSourceSpec, PubSubSinkSpec,
    PubSubSourceSpec, QdrantSourceSpec, RabbitSinkSpec, RabbitSourceSpec, RedisSinkSpec,
    RedisSourceSpec, RestPagination, RestResponseFormat, RestSourceSpec, ShellSpec, SnowflakeAuth,
    SnowflakeSinkSpec, SnowflakeSourceSpec, SqlServerSinkSpec, SqlServerSourceSpec, WasmSpec,
    WeaviateSourceSpec, WebhookSourceSpec, WebhookSpec, XmlSinkSpec, XmlSourceSpec,
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

        // Drain stdout AND stderr on dedicated threads so the child can
        // never deadlock against a full OS pipe buffer. The previous code
        // polled try_wait() to completion and only called wait_with_output()
        // *after* the process exited - but a Windows anonymous pipe holds
        // only ~64 KiB, so once DuckDB's result exceeds that it blocks
        // writing stdout while we block waiting for it to exit. A wide-table
        // preview (`SELECT * ... LIMIT 100` over ~36 columns is ~128 KiB)
        // hit this exactly, hanging the whole pipeline on the source node's
        // preview before it ever reached the sink (issue #4). Concurrent
        // readers keep the pipe drained regardless of result size.
        use std::io::Read;
        let mut stdout_pipe = child
            .stdout
            .take()
            .ok_or_else(|| EngineError::Other("duckdb stdout not captured".into()))?;
        let mut stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| EngineError::Other("duckdb stderr not captured".into()))?;
        let stdout_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut buf);
            buf
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf);
            buf
        });

        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) => {
                    if self.cancel.load(Ordering::Relaxed) {
                        let _ = child.kill();
                        let _ = child.wait();
                        // Killing closes the pipes, so the reader threads
                        // unblock; join them so their handles are released.
                        let _ = stdout_reader.join();
                        let _ = stderr_reader.join();
                        return Err(EngineError::Cancelled);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(40));
                }
                Err(e) => return Err(EngineError::Other(e.to_string())),
            }
        };

        let stdout_bytes = stdout_reader.join().unwrap_or_default();
        let stderr_bytes = stderr_reader.join().unwrap_or_default();
        if !status.success() {
            let mut msg = String::from_utf8_lossy(&stderr_bytes).trim().to_string();
            if msg.is_empty() {
                msg = String::from_utf8_lossy(&stdout_bytes).trim().to_string();
            }
            if msg.is_empty() {
                msg = "DuckDB CLI exited with an error".into();
            }
            return Err(EngineError::Query(msg));
        }
        Ok(String::from_utf8_lossy(&stdout_bytes).into_owned())
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

        // Fast path: if every stage is pure-SQL with no per-stage
        // hooks, pipe the whole pipeline as one SQL stream into a
        // single duckdb.exe invocation. Saves ~47 ms of fixed spawn
        // overhead per stage on Windows (4.4x speedup on a 5-stage
        // pipeline measured locally).
        //
        // Stage-boundary signalling is file-based, not stdout-based:
        // each stage's SQL is followed by a tiny COPY ... TO
        // 'marker_dir/<i>.csv' that records the node id + COUNT(*).
        // A poll loop in the main thread watches the marker dir
        // and emits StageFinished events in real time. We don't
        // read stdout at all so the CLI's stdin-piped-stdout-buffering
        // behaviour is irrelevant here.
        //
        // Anything driver-based (Oracle / SQL Server / Kafka / REST
        // / Mongo / ...) or with mid-pipeline Rust control flow
        // (ctl.iterate / ctl.foreach / ctl.try / per-stage retry /
        // ctl.wait) drops to the per-stage path below. Same for
        // partial runs (subgraph-up-to-target), one-stage pipelines
        // (no win), per-stage memory_limit_mb overrides (would leak
        // PRAGMA across stages in a single session), and any
        // sink_mode="error" with a pre-existing local file (the
        // Rust pre-check guards against silent overwrite).
        let batchable = target.is_none()
            && compiled.stages.len() >= 2
            && compiled.stages.iter().all(|s| {
                s.is_pure_sql()
                    && s.retry_attempts <= 1
                    && s.wait_ms.is_none()
                    && s.memory_limit_mb.is_none()
                    && !(s.sink_mode.as_deref() == Some("error")
                        && s.sink_path.as_deref().map(is_local_path).unwrap_or(false)
                        && s.sink_path
                            .as_deref()
                            .map(|p| std::path::Path::new(p).exists())
                            .unwrap_or(false))
            });

        if batchable {
            let r = self.execute_batched(
                &db_path,
                &secret_prefix,
                &compiled.stages,
                total_start,
                &mut on_event,
            );
            return r;
        }

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
            //
            // Three resource knobs, all with environment-variable defaults
            // so a workspace config can cap the engine globally without
            // touching every stage. Per-stage settings still override
            // the env defaults.
            //
            //   DUCKLE_MEMORY_LIMIT - e.g. "4GB", "2048MB" (DuckDB syntax)
            //   DUCKLE_THREADS      - integer; DuckDB defaults to N cores
            //   DUCKLE_TEMP_DIR     - spill directory (default: OS temp)
            //
            // Plus a fixed performance preset (always on) that flips a
            // handful of DuckDB defaults that hurt typical ETL throughput:
            //
            //   preserve_insertion_order = false
            //     Lets DuckDB use multi-threaded sink writes for COPY and
            //     CREATE TABLE AS. The default-on insertion-order guarantee
            //     forces a serial collector at the end of every parallel
            //     scan, which can halve wall time on Parquet writes.
            //
            //   enable_object_cache = true
            //     Caches Parquet file metadata between reads in the same
            //     process. Big win when the same source file appears in
            //     multiple stages, or when read_parquet hits a glob.
            //
            //   enable_progress_bar = false
            //     Saves a few percent CPU + avoids tearing the CLI output
            //     when we shell out non-interactively.
            //
            // Why env-vars instead of an EngineConfig struct: the engine
            // is constructed in many places (tests, scheduler, desktop)
            // and threading a config through every call site is invasive.
            // Env vars let the Tauri app's setup hook publish workspace
            // settings once, and tests can set them ad-hoc.
            let memory_pragma = {
                let mut prag = String::from(
                    "PRAGMA preserve_insertion_order=false; \
                     PRAGMA enable_object_cache=true; \
                     PRAGMA enable_progress_bar=false; ",
                );
                let env_mem = std::env::var("DUCKLE_MEMORY_LIMIT").ok().filter(|s| !s.is_empty());
                let mem = match stage.memory_limit_mb {
                    Some(mb) => Some(format!("{}MB", mb)),
                    None => env_mem,
                };
                if let Some(m) = mem {
                    prag.push_str(&format!("PRAGMA memory_limit='{}'; ", m.replace('\'', "''")));
                }
                if let Ok(t) = std::env::var("DUCKLE_THREADS") {
                    if let Ok(n) = t.trim().parse::<u32>() {
                        if n > 0 {
                            prag.push_str(&format!("PRAGMA threads={}; ", n));
                        }
                    }
                }
                if let Ok(d) = std::env::var("DUCKLE_TEMP_DIR") {
                    let d = d.trim();
                    if !d.is_empty() {
                        let escaped = d.replace('\'', "''").replace('\\', "/");
                        prag.push_str(&format!("PRAGMA temp_directory='{}'; ", escaped));
                    }
                }
                prag
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
                } else if let Some(spec) = stage.adbc_source.as_ref() {
                    self.run_adbc_source(&db_path, spec)
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
                } else if let Some(spec) = stage.ftp_source.as_ref() {
                    self.run_ftp_source(&db_path, spec)
                } else if let Some(spec) = stage.clipboard_source.as_ref() {
                    self.run_clipboard_source(&db_path, spec)
                } else if let Some(spec) = stage.ai_embed.as_ref() {
                    self.run_ai_embed(&db_path, spec)
                } else if let Some(spec) = stage.wasm.as_ref() {
                    self.run_wasm(&db_path, spec)
                } else if let Some(spec) = stage.javascript.as_ref() {
                    self.run_javascript(&db_path, spec)
                } else if let Some(spec) = stage.ai_chunk.as_ref() {
                    self.run_ai_chunk(&db_path, spec)
                } else if let Some(spec) = stage.ai_pii.as_ref() {
                    self.run_ai_pii(&db_path, spec)
                } else if let Some(spec) = stage.ai_llm.as_ref() {
                    self.run_ai_llm(&db_path, spec)
                } else if let Some(spec) = stage.ai_classify.as_ref() {
                    self.run_ai_classify(&db_path, spec)
                } else if let Some(spec) = stage.ai_dedupe.as_ref() {
                    self.run_ai_dedupe(&db_path, spec)
                } else if let Some(spec) = stage.email_source.as_ref() {
                    self.run_email_source(&db_path, spec)
                } else if let Some(spec) = stage.webhook_source.as_ref() {
                    self.run_webhook_source(&db_path, spec)
                } else if let Some(spec) = stage.email_sink.as_ref() {
                    self.run_email_sink(&db_path, spec)
                } else if let Some(spec) = stage.dynamodb_source.as_ref() {
                    self.run_dynamodb_source(&db_path, spec)
                } else if let Some(spec) = stage.kinesis_source.as_ref() {
                    self.run_kinesis_source(&db_path, spec)
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
                    // A View stage needs count + schema + preview rows; fetch
                    // all three in ONE duckdb spawn (count_and_preview)
                    // instead of three separate ones - saves ~2 process
                    // spawns/stage of fixed overhead on the per-stage path
                    // (audit B8). Sink stages only need a count of their
                    // upstream and have no preview.
                    let (rows_opt, view_preview) = match stage.kind {
                        StageKind::Sink => (
                            stage
                                .from
                                .as_ref()
                                .and_then(|f| self.count_rows(&db_path, f).ok()),
                            None,
                        ),
                        StageKind::View => self.count_and_preview(&db_path, &stage.node_id),
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
                    if let Some(p) = view_preview {
                        preview.push(p);
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

    /// Run an all-pure-SQL pipeline as a single `duckdb.exe` invocation
    /// fed by stdin. Each stage's SQL is followed by a COPY that drops
    /// a small NDJSON marker (node id + row count) plus, for view
    /// stages, a schema + preview COPY. The main thread polls the
    /// marker dir and emits StageStarted / StageFinished as each
    /// marker lands, so the UI still sees per-stage progress.
    ///
    /// Saves ~47 ms of fixed spawn overhead per stage on Windows
    /// (measured locally with 5 stages: 245 ms per-spawn vs 56 ms
    /// batched). The win shows up most on small-data pipelines with
    /// many stages - exactly the dev / debug loop where users feel
    /// slowness.
    ///
    /// We never read the CLI's stdout - we only wait for its exit
    /// code and read stderr on failure. The stdin-piped-stdout-buffers
    /// behaviour that blocked the persistent-session attempt doesn't
    /// matter here.
    fn execute_batched(
        &self,
        db_path: &Path,
        secret_prefix: &str,
        stages: &[plan::Stage],
        total_start: Instant,
        on_event: &mut dyn FnMut(PipelineEvent),
    ) -> RunResult {
        use std::io::Write;
        use std::process::Stdio;

        let mut nodes: std::collections::BTreeMap<String, NodeRunStatus> = Default::default();
        let mut overall_error: Option<String> = None;
        let mut preview: Vec<NodePreview> = Vec::new();
        let mut was_cancelled = false;

        let marker_dir = std::env::temp_dir().join(format!(
            "duckle_marks_{}_{}_{}",
            std::process::id(),
            now_nanos(),
            RUN_SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        if let Err(e) = std::fs::create_dir_all(&marker_dir) {
            return RunResult::failed(
                total_start,
                format!("could not create marker dir: {}", e),
            );
        }
        let _marker_guard = TempDirGuard(marker_dir.clone());

        let path_to_sql = |p: &Path| -> String {
            p.display()
                .to_string()
                .replace('\\', "/")
                .replace('\'', "''")
        };

        // Pre-compute the set of node ids whose stage SQL produces a
        // view backed by a DuckDB extension's ATTACH machinery
        // (postgres, mysql, sqlite, duckdb, motherduck, etc.). Their
        // views work for sinks downstream that do plain COPY, but
        // they break the marker's `SELECT COUNT(*) AS r FROM <v>`:
        // DuckDB's binder rejects the aliased-aggregate shape inside
        // a batched session with "Failed to bind column reference r".
        // Per-stage avoids it because each spawn is a fresh session.
        // For these stages and any sink that reads from them, emit a
        // count-less marker; we lose batched-mode row counts on
        // those, but the pipeline still runs and the perf win for
        // the rest of the pipeline is preserved.
        let extension_attach = |cid: &str| -> bool {
            matches!(
                cid,
                "src.postgres"
                    | "src.cockroach"
                    | "src.pgvector"
                    | "src.redshift"
                    | "src.mysql"
                    | "src.mariadb"
                    | "src.motherduck"
                    | "src.ducklake"
                    | "src.bigquery"
                    | "src.quack"
                    | "src.duckdb"
                    | "src.sqlite"
            )
        };
        let extension_node_ids: std::collections::HashSet<&str> = stages
            .iter()
            .filter(|s| extension_attach(&s.component_id))
            .map(|s| s.node_id.as_str())
            .collect();

        // Build the batched SQL: secret prefix, PRAGMA preset (once),
        // then per-stage SQL + per-stage markers + per-view previews.
        let mut batched_sql = String::new();
        if !secret_prefix.is_empty() {
            batched_sql.push_str(secret_prefix);
            batched_sql.push('\n');
        }
        batched_sql.push_str(
            "PRAGMA preserve_insertion_order=false;\n\
             PRAGMA enable_object_cache=true;\n\
             PRAGMA enable_progress_bar=false;\n",
        );
        if let Ok(m) = std::env::var("DUCKLE_MEMORY_LIMIT") {
            let m = m.trim();
            if !m.is_empty() {
                batched_sql.push_str(&format!(
                    "PRAGMA memory_limit='{}';\n",
                    m.replace('\'', "''")
                ));
            }
        }
        if let Ok(t) = std::env::var("DUCKLE_THREADS") {
            if let Ok(n) = t.trim().parse::<u32>() {
                if n > 0 {
                    batched_sql.push_str(&format!("PRAGMA threads={};\n", n));
                }
            }
        }
        if let Ok(d) = std::env::var("DUCKLE_TEMP_DIR") {
            let d = d.trim();
            if !d.is_empty() {
                batched_sql.push_str(&format!(
                    "PRAGMA temp_directory='{}';\n",
                    d.replace('\'', "''").replace('\\', "/"),
                ));
            }
        }

        for (i, stage) in stages.iter().enumerate() {
            batched_sql.push_str(&stage.sql);
            // Planner does not always terminate stage.sql with ';' -
            // the per-stage path tolerates it because each CLI invocation
            // gets exactly one logical statement and parses fine at EOF.
            // In batched mode the marker COPY follows immediately, so we
            // need an explicit ';' to keep the parser from gluing them.
            if !stage.sql.trim_end().ends_with(';') {
                batched_sql.push(';');
            }
            batched_sql.push('\n');
            // Two cases where the marker MUST NOT query <node>:
            //   - ctl.switch creates <node>__case_N + <node>__default
            //     tables instead of a <node> view, so querying <node>
            //     itself fails with "table does not exist".
            //   - xf.assert wraps an error() call in its view body;
            //     querying the view eagerly fires the error here even
            //     when the actual assertion failure should surface at
            //     the downstream sink (matches per-stage semantics).
            // Other view stages CAN be counted - querying them only
            // evaluates the same view body the downstream sink would
            // anyway, so it's free.
            let count_unsafe = matches!(
                stage.component_id.as_str(),
                "ctl.switch" | "xf.assert"
            );
            let marker = marker_dir.join(format!("{}.json", i));
            let count_target = match stage.kind {
                plan::StageKind::Sink => Some(stage.from.as_deref().unwrap_or(&stage.node_id)),
                plan::StageKind::View if !count_unsafe => Some(stage.node_id.as_str()),
                plan::StageKind::View => None,
            };
            // Skip the COUNT(*) entirely if the target is an extension
            // ATTACH view; the binder bug above otherwise aborts the
            // batch.
            let count_target = count_target.filter(|t| !extension_node_ids.contains(t));
            // Marker shape is just `SELECT COUNT(*) AS _duckle_r FROM <t>`
            // (or `SELECT NULL AS _duckle_r` when there's no countable
            // target). No string-literal projected alongside the
            // aggregate: DuckDB's binder repeatedly tripped on
            // `SELECT 'literal' AS x, COUNT(*) AS y FROM <foreign>`
            // when <foreign> was an extension-backed ATTACH view
            // (mysql + postgres), with internal "Failed to bind
            // column reference ..." errors. The stage's identity is
            // already in the marker FILE NAME (<i>.json), so we don't
            // need it inside the payload.
            match count_target {
                Some(t) => batched_sql.push_str(&format!(
                    "COPY (SELECT COUNT(*) AS _duckle_r FROM {}) TO '{}' (FORMAT 'json', ARRAY false);\n",
                    plan::quote_ident(t),
                    path_to_sql(&marker),
                )),
                None => batched_sql.push_str(&format!(
                    "COPY (SELECT NULL AS _duckle_r) TO '{}' (FORMAT 'json', ARRAY false);\n",
                    path_to_sql(&marker),
                )),
            }
            // Preview only for view stages, and only if querying the view
            // for preview rows wouldn't trigger the same eager-evaluation
            // problem the row-count subquery just dodged. We accept the
            // cost here because the preview is the user-visible payoff
            // for the batched mode; users would lose it otherwise. If
            // the preview query fails (assert / switch / etc.), -bail
            // aborts the batch and we attribute the failure to this
            // stage - same as per-stage would for the same reason. So:
            // skip preview for components that don't produce <node> and
            // for xf.assert (where the predicate check would fire here
            // rather than at the downstream sink).
            if matches!(stage.kind, plan::StageKind::View)
                && stage.component_id != "ctl.switch"
                && stage.component_id != "xf.assert"
                && !extension_node_ids.contains(stage.node_id.as_str())
            {
                let schema = marker_dir.join(format!("{}_schema.json", i));
                let rows = marker_dir.join(format!("{}_rows.json", i));
                batched_sql.push_str(&format!(
                    "COPY (SELECT * FROM (DESCRIBE {})) TO '{}' (FORMAT 'json', ARRAY false);\n",
                    plan::quote_ident(&stage.node_id),
                    path_to_sql(&schema),
                ));
                batched_sql.push_str(&format!(
                    "COPY (SELECT * FROM {} LIMIT {}) TO '{}' (FORMAT 'json', ARRAY false);\n",
                    plan::quote_ident(&stage.node_id),
                    PREVIEW_ROW_LIMIT,
                    path_to_sql(&rows),
                ));
            }
        }

        let mut cmd = std::process::Command::new(&self.bin);
        cmd.arg(db_path)
            .arg("-bail")
            .stdin(Stdio::piped())
            // stdout MUST be piped, not null. On Windows the DuckDB
            // CLI suppresses stderr output entirely when stdout is
            // redirected to NUL - verified empirically: same SQL,
            // stdout=null gives 0 bytes of stderr; stdout=piped or
            // inherit gives the full error text. We never look at
            // the bytes we read here, but a background thread has
            // to drain the pipe so its kernel buffer doesn't fill
            // and block the CLI.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return RunResult::failed(
                    total_start,
                    format!("could not start duckdb: {}", e),
                );
            }
        };

        // Write the SQL on a side thread so the main thread is free
        // to poll markers + cancel. Closing stdin (drop on thread
        // exit) signals EOF to the CLI = "no more statements coming."
        let stdin = match child.stdin.take() {
            Some(s) => s,
            None => {
                return RunResult::failed(total_start, "duckdb stdin not captured".into());
            }
        };
        let sql_for_writer = batched_sql;
        let writer_thread = std::thread::spawn(move || {
            let mut s = stdin;
            let _ = s.write_all(sql_for_writer.as_bytes());
        });

        // Drain stdout + stderr on side threads. The pipe kernel
        // buffer (~4 KB on Windows) fills if no one reads, and the
        // CLI blocks on write -> deadlock. We discard stdout (we
        // don't read result sets via stdout) but still have to
        // drain it. Stderr is what carries CLI errors; we keep it.
        let stdout_handle = match child.stdout.take() {
            Some(s) => s,
            None => {
                return RunResult::failed(total_start, "duckdb stdout not captured".into());
            }
        };
        let _stdout_drain = std::thread::spawn(move || {
            use std::io::Read;
            let mut s = stdout_handle;
            let mut sink = [0u8; 4096];
            while let Ok(n) = s.read(&mut sink) {
                if n == 0 {
                    break;
                }
            }
        });
        let stderr_handle = match child.stderr.take() {
            Some(s) => s,
            None => {
                return RunResult::failed(total_start, "duckdb stderr not captured".into());
            }
        };
        let stderr_thread = std::thread::spawn(move || {
            use std::io::Read;
            let mut s = stderr_handle;
            let mut buf = Vec::new();
            let _ = s.read_to_end(&mut buf);
            buf
        });

        // Emit StageStarted for stage 0 + record its wall-clock start.
        let mut stage_started_at: Vec<Instant> = vec![Instant::now(); stages.len()];
        if let Some(first) = stages.first() {
            on_event(PipelineEvent::StageStarted {
                node_id: first.node_id.clone(),
                label: first.label.clone(),
                kind: stage_kind_label(&first.kind).into(),
            });
        }

        let mut completed = 0usize;
        let mut failed_stage_idx: Option<usize> = None;
        let cli_stderr: Vec<u8>;

        loop {
            drain_batched_markers(
                &mut completed,
                stages,
                &marker_dir,
                &mut stage_started_at,
                &mut nodes,
                on_event,
                false,
            );
            match child.try_wait() {
                Ok(Some(status)) => {
                    drain_batched_markers(
                        &mut completed,
                        stages,
                        &marker_dir,
                        &mut stage_started_at,
                        &mut nodes,
                        on_event,
                        true,
                    );
                    if !status.success() {
                        failed_stage_idx = Some(completed);
                    }
                    break;
                }
                Ok(None) => {
                    if self.cancel.load(Ordering::Relaxed) {
                        let _ = child.kill();
                        let _ = child.wait();
                        was_cancelled = true;
                        on_event(PipelineEvent::Cancelled);
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Err(e) => {
                    overall_error = Some(format!("duckdb wait: {}", e));
                    break;
                }
            }
        }

        // Best-effort join: the writer should be done as soon as the
        // CLI EOF'd or errored (broken pipe wakes write_all). Doesn't
        // matter if it isn't - the thread is detached cleanly either
        // way.
        let _ = writer_thread.join();
        cli_stderr = stderr_thread.join().unwrap_or_default();

        if let Some(idx) = failed_stage_idx {
            if idx < stages.len() {
                let stage = &stages[idx];
                let kind = stage_kind_label(&stage.kind);
                let elapsed =
                    Instant::now().duration_since(stage_started_at[idx]).as_millis() as u64;
                let stderr_str = String::from_utf8_lossy(&cli_stderr).trim().to_string();
                let msg = if stderr_str.is_empty() {
                    "duckdb exited with error (no diagnostic on stderr)".to_string()
                } else {
                    stderr_str
                };
                nodes.insert(
                    stage.node_id.clone(),
                    NodeRunStatus {
                        status: "error".into(),
                        kind: Some(kind.into()),
                        rows: None,
                        duration_ms: Some(elapsed),
                        error: Some(msg.clone()),
                    },
                );
                on_event(PipelineEvent::StageFinished {
                    node_id: stage.node_id.clone(),
                    kind: kind.into(),
                    status: "error".into(),
                    rows: None,
                    duration_ms: elapsed,
                    error: Some(msg.clone()),
                });
                overall_error.get_or_insert(format!("{}: {}", stage.label, msg));
            } else {
                let stderr_str = String::from_utf8_lossy(&cli_stderr).trim().to_string();
                overall_error.get_or_insert(format!("duckdb pipeline error: {}", stderr_str));
            }
        }

        // Read previews for the view stages that actually completed.
        for (i, stage) in stages.iter().enumerate() {
            if !matches!(stage.kind, plan::StageKind::View) {
                continue;
            }
            if i >= completed {
                continue;
            }
            let schema_path = marker_dir.join(format!("{}_schema.json", i));
            let rows_path = marker_dir.join(format!("{}_rows.json", i));
            let schema: Vec<Column> = read_ndjson(&schema_path)
                .iter()
                .filter_map(parse_describe_row)
                .collect();
            let rows = read_ndjson(&rows_path);
            preview.push(NodePreview {
                node_id: stage.node_id.clone(),
                columns: schema,
                rows,
            });
        }

        let final_status = if was_cancelled {
            "cancelled"
        } else if overall_error.is_some() {
            "error"
        } else {
            "ok"
        };
        let duration_ms = total_start.elapsed().as_millis() as u64;
        on_event(PipelineEvent::Finished {
            status: final_status.into(),
            duration_ms,
        });

        RunResult {
            status: final_status.into(),
            duration_ms,
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
                            sql_literal(v, None, Dialect::JsonNative)
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
        // Column names + DuckDB types in view order, used to auto-create the
        // target, decide the fast bind path, and (fallback) render literals.
        let describe = describe_columns(self, db, &spec.from_view);
        if describe.is_empty() {
            return Ok(format!("oracle: 0 columns to insert into {}", spec.table));
        }
        let cols: Vec<String> = describe.iter().map(|(n, _)| n.clone()).collect();
        let col_types: std::collections::HashMap<String, String> =
            describe.iter().cloned().collect();
        // Oracle limits a table to 1000 columns; reject up front with a clear
        // message rather than failing deep in CREATE TABLE / INSERT.
        if cols.len() >= 1000 {
            return Err(EngineError::Query(format!(
                "oracle: {} columns exceeds Oracle's 1000-column table limit",
                cols.len()
            )));
        }
        let qualified = match &spec.schema {
            Some(s) => format!("\"{}\".\"{}\"", s, spec.table),
            None => format!("\"{}\"", spec.table),
        };
        let cols_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");

        // Decide whether every column can take the fast array-bind path. Bind
        // values are sent as strings and converted by Oracle: numbers / text
        // implicitly, DATE / TIMESTAMP via an explicit TO_DATE / TO_TIMESTAMP
        // fed a canonical strftime string. Time-zone, BLOB and nested types
        // are not handled this way, so any of them drops the whole sink to the
        // per-literal INSERT ALL fallback below (no behavior change for them).
        let mut bindable = true;
        let mut placeholders: Vec<String> = Vec::with_capacity(cols.len());
        let mut select_items: Vec<String> = Vec::with_capacity(cols.len());
        for (idx, (name, duck)) in describe.iter().enumerate() {
            let up = duck.trim().to_ascii_uppercase();
            let n = idx + 1;
            let qn = plan::quote_ident(name);
            if up.contains("TIME ZONE")
                || up.starts_with("BLOB")
                || up.starts_with("BYTEA")
                || up.starts_with("BINARY")
                || up.starts_with("VARBINARY")
                || up.ends_with("[]")
                || up.starts_with("STRUCT")
                || up.starts_with("MAP")
                || up.starts_with("LIST")
                || up.starts_with("UNION")
            {
                bindable = false;
                break;
            } else if up == "DATE" {
                placeholders.push(format!("TO_DATE(:{}, 'YYYY-MM-DD')", n));
                select_items.push(format!("strftime({}, '%Y-%m-%d') AS {}", qn, qn));
            } else if up.starts_with("TIMESTAMP") || up == "DATETIME" {
                placeholders.push(format!("TO_TIMESTAMP(:{}, 'YYYY-MM-DD HH24:MI:SS.FF6')", n));
                select_items.push(format!("strftime({}, '%Y-%m-%d %H:%M:%S.%f') AS {}", qn, qn));
            } else {
                placeholders.push(format!(":{}", n));
                select_items.push(qn);
            }
        }

        let conn = oracle::Connection::connect(&spec.user, &spec.password, &spec.connect)
            .map_err(|e| EngineError::Query(format!("oracle connect: {}", e)))?;
        // Pin the decimal separator so string-bound numbers parse with '.'
        // regardless of the server locale (NLS_NUMERIC_CHARACTERS).
        let _ = conn.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'", &[]);

        // Auto-create the target table if absent, inferring column types from
        // the upstream DuckDB view (issue #8). Oracle has no CREATE TABLE IF
        // NOT EXISTS, so swallow ORA-00955 (name already used) in PL/SQL.
        {
            let col_defs = cols
                .iter()
                .map(|c| {
                    let ty = duckdb_type_to_oracle(
                        col_types.get(c).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                    );
                    format!("\"{}\" {}", c.replace('"', "\"\""), ty)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let create_inner =
                format!("CREATE TABLE {} ({})", qualified, col_defs).replace('\'', "''");
            let create_plsql = format!(
                "BEGIN EXECUTE IMMEDIATE '{}'; EXCEPTION WHEN OTHERS THEN \
                 IF SQLCODE != -955 THEN RAISE; END IF; END;",
                create_inner
            );
            conn.execute(&create_plsql, &[])
                .map_err(|e| EngineError::Query(format!("oracle create table: {}", e)))?;
        }

        // Commit periodically, not after every statement: a commit forces a
        // redo-log flush, so per-batch commits dominated large-load wall-clock.
        const COMMIT_EVERY: usize = 200_000;

        // Fast path: one prepared INSERT, array-bound and array-executed
        // (dpiStmt_executeMany). Replaces the old per-99-row INSERT ALL, each
        // a unique literal statement Oracle had to hard-parse.
        if bindable {
            let select = format!(
                "SELECT {} FROM {}",
                select_items.join(", "),
                plan::quote_ident(&spec.from_view)
            );
            let rows = self.run_rows(Some(db), &select)?;
            if rows.is_empty() {
                return Ok(format!("oracle: 0 rows to insert into {}", spec.table));
            }
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                cols_list,
                placeholders.join(", ")
            );
            const BIND_BATCH: usize = 5000;
            let mut batch = conn
                .batch(&insert_sql, BIND_BATCH)
                .build()
                .map_err(|e| EngineError::Query(format!("oracle batch prepare: {}", e)))?;
            let mut total = 0_usize;
            let mut uncommitted = 0_usize;
            for row in &rows {
                if total % BIND_BATCH == 0 {
                    self.check_cancelled()?;
                }
                let obj = row.as_object();
                // Bind every value as a string; the SQL placeholders and
                // Oracle implicit conversion turn it back into the column type.
                let binds: Vec<Option<String>> = cols
                    .iter()
                    .map(|c| match obj.and_then(|o| o.get(c)) {
                        None | Some(JsonValue::Null) => None,
                        Some(JsonValue::String(s)) => Some(s.clone()),
                        Some(JsonValue::Bool(b)) => {
                            Some(if *b { "1".to_string() } else { "0".to_string() })
                        }
                        Some(JsonValue::Number(num)) => Some(num.to_string()),
                        Some(other) => Some(other.to_string()),
                    })
                    .collect();
                let refs: Vec<&dyn oracle::sql_type::ToSql> =
                    binds.iter().map(|b| b as &dyn oracle::sql_type::ToSql).collect();
                batch
                    .append_row(&refs)
                    .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
                total += 1;
                uncommitted += 1;
                if uncommitted >= COMMIT_EVERY {
                    batch
                        .execute()
                        .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
                    conn.commit()
                        .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
                    uncommitted = 0;
                }
            }
            batch
                .execute()
                .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
            conn.commit()
                .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
            return Ok(format!("oracle: inserted {} rows into {}", total, qualified));
        }

        // Fallback path (time-zone / BLOB / nested types): per-literal INSERT
        // ALL, capped under Oracle's 999 cumulative-value limit (issue #11).
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("oracle: 0 rows to insert into {}", spec.table));
        }
        let mut total = 0_usize;
        let mut uncommitted = 0_usize;
        let rows_per_stmt = oracle_insert_all_rows_per_stmt(cols.len(), spec.batch_size);
        for chunk in rows.chunks(rows_per_stmt) {
            self.check_cancelled()?;
            let mut sql = String::from("INSERT ALL");
            for row in chunk {
                let row_obj = row.as_object();
                let vals: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        let v = row_obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                        sql_literal(v, col_types.get(c).map(|s| s.as_str()), Dialect::Oracle)
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
            total += chunk.len();
            uncommitted += chunk.len();
            if uncommitted >= COMMIT_EVERY {
                conn.commit()
                    .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
                uncommitted = 0;
            }
        }
        if uncommitted > 0 {
            conn.commit()
                .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
        }
        Ok(format!("oracle: inserted {} rows into {}", total, qualified))
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
        // Liveness trace (issue #4): each phase plus periodic row progress
        // is timestamped to a temp file so a stuck pull can be located from
        // the log even when the desktop shows no console. Truncated per run.
        let trace_path = std::env::temp_dir().join("duckle-oracle-trace.log");
        let _ = std::fs::remove_file(&trace_path);
        let t0 = std::time::Instant::now();
        let mark = |msg: &str| {
            use std::io::Write;
            let line = format!(
                "[+{:>7}ms] [{}] {}",
                t0.elapsed().as_millis(),
                spec.node_id,
                msg
            );
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&trace_path)
            {
                let _ = writeln!(f, "{}", line);
            }
            eprintln!("[duckle:oracle] {}", line);
        };
        mark(&format!("connecting to {} as {}", spec.connect, spec.user));

        let conn = oracle::Connection::connect(&spec.user, &spec.password, &spec.connect)
            .map_err(|e| EngineError::Query(format!("oracle connect: {}", e)))?;
        mark("connected; normalizing NLS session formats");

        // Issue #4 robustness (not a confirmed fix): pin the session NLS
        // formats to a stable ISO-ish shape so serialized DATE/TIMESTAMP
        // strings do not vary with the server locale. A format that forces
        // read_json_auto to re-sniff every row is the leading remaining
        // hypothesis for the wide-table slowdown. Best-effort: a server
        // that rejects any of these still proceeds with its defaults.
        for nls in [
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
            "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'",
        ] {
            if let Err(e) = conn.execute(nls, &[]) {
                mark(&format!("NLS set skipped: {}", e));
            }
        }
        mark("preparing query");

        // Issue #4: the default Oracle prefetch is tiny (often 1-2 rows
        // per round trip). Two knobs matter for a bulk pull and BOTH must be
        // raised: prefetch_rows is OCI's server prefetch, and fetch_array_size
        // (ODPI default 100) is how many rows the client buffers per fetch.
        // Left at 100, a 2M-row pull is ~20 000 client fetches and the OCI
        // fetch dominated wall-clock (profiled at ~12s). Matching both at
        // 5 000 cuts that to ~400 fetches.
        let mut stmt = conn
            .statement(&spec.query)
            .prefetch_rows(5000)
            .fetch_array_size(5000)
            .build()
            .map_err(|e| EngineError::Query(format!("oracle prepare: {}", e)))?;
        let rs = stmt
            .query(&[])
            .map_err(|e| EngineError::Query(format!("oracle query: {}", e)))?;
        let cols: Vec<String> = rs
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        mark(&format!("query open; {} columns; streaming rows", cols.len()));

        // Stream rows straight to the NDJSON temp file. The previous
        // Vec<JsonValue> collector held the entire result set in RAM
        // before handing it to DuckDB - on a million-row x 37-col pull
        // that peaked at ~30 GB resident. Now the writer keeps a 64 KiB
        // buffer regardless of row count.
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut count = 0_usize;
        for row_res in rs {
            let row = row_res.map_err(|e| EngineError::Query(format!("oracle row: {}", e)))?;
            let mut obj = serde_json::Map::new();
            for (i, name) in cols.iter().enumerate() {
                obj.insert(name.clone(), Self::oracle_cell_to_json(&row, i));
            }
            writer.write_row(&JsonValue::Object(obj))?;
            count += 1;
            if count % 25_000 == 0 {
                mark(&format!("fetched {} rows", count));
            }
        }
        mark(&format!(
            "fetch complete: {} rows; materializing into DuckDB",
            count
        ));
        writer.finalize_into_table(db, &spec.node_id)?;
        mark(&format!(
            "materialize complete: {} into {}",
            count, spec.node_id
        ));
        Ok(format!(
            "oracle: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Convert one cell of an Oracle row to JSON without silently
    /// losing data. The old approach was a try-String-then-i64-then-
    /// f64 cascade, which fell through to NULL for DATE / TIMESTAMP /
    /// BLOB / RAW / NUMBER-that-overflows-i64 columns - whole
    /// columns vanished in downstream Parquet (issue #4).
    ///
    /// Strategy: dispatch by Oracle column type. NUMBER with a
    /// non-zero scale is parsed as f64 if it fits, otherwise kept as
    /// a string to avoid the precision trap with high-precision
    /// decimals. DATE / TIMESTAMP becomes an ISO-shaped string.
    /// BLOB / RAW gets base64-encoded. Unknown types fall through to
    /// the String accessor so the cell is at worst visible as text
    /// rather than NULL.
    #[cfg(feature = "oracle")]
    fn oracle_cell_to_json(row: &oracle::Row, i: usize) -> JsonValue {
        use oracle::sql_type::OracleType;
        let infos = row.column_info();
        let oty = infos
            .get(i)
            .map(|c| c.oracle_type().clone())
            .unwrap_or(OracleType::Varchar2(0));

        match oty {
            OracleType::Number(_, scale) if scale == 0 => {
                if let Ok(Some(n)) = row.get::<usize, Option<i64>>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    return JsonValue::String(s);
                }
                JsonValue::Null
            }
            // Decimal NUMBER / ANSI FLOAT carry up to 38 significant
            // digits, but f64 only round-trips ~15. Reading a
            // high-precision value through f64 silently drops the extra
            // digits (e.g. NUMBER(38,12) 123456.123456789012 -> ...789),
            // so keep the exact text when it would not survive f64.
            OracleType::Number(_, _) | OracleType::Float(_) => {
                // Significant digits = digits with the sign, decimal point
                // and leading/trailing zeros removed.
                fn significant_digits(s: &str) -> usize {
                    let d: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
                    d.trim_start_matches('0').trim_end_matches('0').len()
                }
                if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    if significant_digits(&s) <= 15 {
                        if let Ok(n) = s.parse::<f64>() {
                            if let Some(num) = serde_json::Number::from_f64(n) {
                                return JsonValue::Number(num);
                            }
                        }
                    }
                    return JsonValue::String(s);
                }
                JsonValue::Null
            }
            // BINARY_DOUBLE / BINARY_FLOAT are true IEEE floats; f64
            // represents them exactly, so emit a JSON number.
            OracleType::BinaryDouble | OracleType::BinaryFloat => {
                if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    if let Ok(n) = s.parse::<f64>() {
                        if let Some(num) = serde_json::Number::from_f64(n) {
                            return JsonValue::Number(num);
                        }
                    }
                    return JsonValue::String(s);
                }
                JsonValue::Null
            }
            OracleType::Date
            | OracleType::Timestamp(_)
            | OracleType::TimestampTZ(_)
            | OracleType::TimestampLTZ(_) => row
                .get::<usize, Option<String>>(i)
                .ok()
                .flatten()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            OracleType::BLOB | OracleType::Raw(_) | OracleType::LongRaw => {
                use base64::engine::general_purpose::STANDARD as B64;
                use base64::Engine as _;
                row.get::<usize, Option<Vec<u8>>>(i)
                    .ok()
                    .flatten()
                    .map(|b| JsonValue::String(B64.encode(&b)))
                    .unwrap_or(JsonValue::Null)
            }
            _ => row
                .get::<usize, Option<String>>(i)
                .ok()
                .flatten()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        }
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

    /// src.adbc: load a prebuilt ADBC driver at runtime, run the query, and
    /// stream the Arrow result to a Parquet temp file, then materialize it
    /// into the node's DuckDB table via read_parquet (no in-process DuckDB).
    /// Not feature-gated: adbc_core links unconditionally; a missing or
    /// incompatible driver surfaces as a clear engine error at load time.
    fn run_adbc_source(
        &self,
        db: &Path,
        spec: &plan::AdbcSourceSpec,
    ) -> Result<String, EngineError> {
        use adbc_core::{
            driver_manager::ManagedDriver,
            options::{AdbcVersion, OptionDatabase, OptionValue},
            Connection, Database, Driver, Statement,
        };
        use arrow_array::RecordBatchReader;
        use parquet::arrow::ArrowWriter;

        // Prepend the driver's own directory to PATH so a self-contained
        // bundled driver folder (driver lib + its dependent libs, e.g.
        // sqlite3.dll) loads without extra setup.
        let driver_path = Path::new(&spec.driver);
        if let Some(parent) = driver_path.parent() {
            if !parent.as_os_str().is_empty() {
                let cur = std::env::var("PATH").unwrap_or_default();
                let sep = if cfg!(windows) { ';' } else { ':' };
                std::env::set_var("PATH", format!("{}{}{}", parent.display(), sep, cur));
            }
        }

        let entry: Option<&[u8]> = spec.entrypoint.as_deref().map(|s| s.as_bytes());
        let looks_like_path = spec.driver.contains('/')
            || spec.driver.contains('\\')
            || spec.driver.ends_with(".dll")
            || spec.driver.ends_with(".so")
            || spec.driver.ends_with(".dylib");
        let mut driver = if looks_like_path {
            ManagedDriver::load_dynamic_from_filename(&spec.driver, entry, AdbcVersion::V110)
        } else {
            ManagedDriver::load_dynamic_from_name(&spec.driver, entry, AdbcVersion::V110)
        }
        .map_err(|e| EngineError::Query(format!("adbc: load driver '{}': {}", spec.driver, e)))?;

        let opts = spec
            .options
            .iter()
            .map(|(k, v)| (OptionDatabase::from(k.as_str()), OptionValue::String(v.clone())));
        let mut database = driver
            .new_database_with_opts(opts)
            .map_err(|e| EngineError::Query(format!("adbc: open database: {}", e)))?;
        let mut conn = database
            .new_connection()
            .map_err(|e| EngineError::Query(format!("adbc: connect: {}", e)))?;
        let mut stmt = conn
            .new_statement()
            .map_err(|e| EngineError::Query(format!("adbc: statement: {}", e)))?;
        stmt.set_sql_query(&spec.query)
            .map_err(|e| EngineError::Query(format!("adbc: set query: {}", e)))?;
        let reader = stmt
            .execute()
            .map_err(|e| EngineError::Query(format!("adbc: execute: {}", e)))?;

        let schema = reader.schema();
        let parquet_path =
            std::env::temp_dir().join(format!("duckle-adbc-{}.parquet", spec.node_id));
        let file = std::fs::File::create(&parquet_path)
            .map_err(|e| EngineError::Query(format!("adbc: temp parquet: {}", e)))?;

        // Encode the Arrow batches to the temp parquet on a dedicated thread
        // so the parquet encode overlaps the *next* ADBC driver fetch rather
        // than running strictly after it. The driver pull is the dominant cost
        // (measured ~2x the encode for a 2M-row source), so the encode hides
        // behind it almost entirely. Tuning: statistics are disabled (no
        // downstream stage reads parquet stats here) and the row group is
        // enlarged - one big group reads back faster than the default
        // many-small-groups layout. Compression stays the parquet-crate
        // default (uncompressed): a local temp file optimizes for round-trip
        // speed, not disk size.
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(1_000_000)
            .build();
        let writer_schema = schema.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel::<arrow_array::RecordBatch>(8);
        let writer = std::thread::spawn(move || -> Result<usize, String> {
            let mut w = ArrowWriter::try_new(file, writer_schema, Some(props))
                .map_err(|e| e.to_string())?;
            let mut n = 0usize;
            for batch in rx {
                n += batch.num_rows();
                w.write(&batch).map_err(|e| e.to_string())?;
            }
            w.close().map_err(|e| e.to_string())?;
            Ok(n)
        });

        // The main thread drives the ADBC reader (its FFI stream is not Send,
        // so it stays here) and ships each batch to the writer thread. A send
        // failure means the writer thread already errored; we stop pulling and
        // surface that error from the join below.
        for batch in reader {
            self.check_cancelled()?;
            let batch = batch.map_err(|e| EngineError::Query(format!("adbc: read batch: {}", e)))?;
            if tx.send(batch).is_err() {
                break;
            }
        }
        drop(tx); // close the channel so the writer loop terminates
        let count = writer
            .join()
            .map_err(|_| EngineError::Query("adbc: parquet writer thread panicked".into()))?
            .map_err(|e| EngineError::Query(format!("adbc: write parquet: {}", e)))?;

        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            ppath
        );
        self.run(Some(db), &create, false)?;
        let _ = std::fs::remove_file(&parquet_path);
        Ok(format!("adbc: materialized {} rows into {}", count, spec.node_id))
    }

    /// Convert one cell of a SQL Server row to JSON without silently
    /// losing data. Same issue as Oracle: the old cascade
    /// try-`&str`-then-`i64`-then-`i32`-then-`f64`-then-`bool` failed
    /// for the common Microsoft SQL Server types (DATETIME / DATE /
    /// DATETIMEOFFSET / DECIMAL / NUMERIC / UNIQUEIDENTIFIER /
    /// VARBINARY), silently emitting NULL and dropping whole columns
    /// from the downstream Parquet / DuckDB table.
    ///
    /// Tiberius exposes a `ColumnData` enum reachable via
    /// `Row::try_get_by_index`; we dispatch on it so every SQL Server
    /// scalar gets a faithful JSON representation.
    fn sqlserver_cell_to_json(
        row: &tiberius::Row,
        col: &tiberius::Column,
        i: usize,
    ) -> JsonValue {
        use tiberius::ColumnType;
        // First, the easy path: the most common scalar types map cleanly
        // through Tiberius' generic try_get<T>. We dispatch by the column
        // type the server reported so we don't blindly probe every type.
        match col.column_type() {
            ColumnType::Bit | ColumnType::Bitn => row
                .try_get::<bool, _>(i)
                .ok()
                .flatten()
                .map(JsonValue::Bool)
                .unwrap_or(JsonValue::Null),
            ColumnType::Int1
            | ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Intn => {
                // Try the widest signed int the server might have packed in.
                if let Ok(Some(n)) = row.try_get::<i64, _>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(n)) = row.try_get::<i32, _>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(n)) = row.try_get::<i16, _>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(n)) = row.try_get::<u8, _>(i) {
                    return JsonValue::from(n);
                }
                JsonValue::Null
            }
            // Float8 / FLOAT and MONEY / SMALLMONEY all decode to f64 in
            // tiberius (money is the scaled integer / 1e4); REAL /
            // FLOAT(24) decodes to f32, which try_get::<f64> rejects - so
            // fall back to f32 before giving up. The previous code read
            // floats as f64 only (REAL -> NULL) and routed MONEY through
            // the Numeric path (which money is NOT -> NULL).
            ColumnType::Float4
            | ColumnType::Float8
            | ColumnType::Floatn
            | ColumnType::Money
            | ColumnType::Money4 => {
                let v = row.try_get::<f64, _>(i).ok().flatten().or_else(|| {
                    row.try_get::<f32, _>(i).ok().flatten().map(|x| x as f64)
                });
                v.and_then(|x| serde_json::Number::from_f64(x).map(JsonValue::Number))
                    .unwrap_or(JsonValue::Null)
            }
            // DECIMAL / NUMERIC arrive as tiberius::numeric::Numeric.
            // Stringify (JSON has no fixed-point; f64 would lose the
            // precision that's the point of DECIMAL) - but format it
            // ourselves from the unscaled value + scale. Numeric's own
            // Display signs both the integer and fractional parts, so a
            // negative like -1.2500 renders as the malformed "-1.-2500".
            ColumnType::Decimaln | ColumnType::Numericn => row
                .try_get::<tiberius::numeric::Numeric, _>(i)
                .ok()
                .flatten()
                .map(|n| JsonValue::String(mssql_numeric_to_string(n.value(), n.scale())))
                .unwrap_or(JsonValue::Null),
            // Date / time / datetime / datetimeoffset all expose a
            // chrono::NaiveDate/NaiveDateTime/DateTime<Utc> via tiberius'
            // optional `time`/`chrono` features. The crate's default
            // path on try_get::<&str>` doesn't work for them, but
            // ToString does - drop to that and emit ISO-shaped strings.
            // DATETIMEOFFSET is offset-aware: tiberius decodes it to
            // chrono::DateTime<FixedOffset> (or Utc), NOT a Naive* type, so
            // the naive probes below would all miss and it became NULL.
            // Emit an RFC3339 string preserving the original offset.
            ColumnType::DatetimeOffsetn => {
                if let Ok(Some(dt)) = row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(i) {
                    return JsonValue::String(dt.to_rfc3339());
                }
                if let Ok(Some(dt)) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(i) {
                    return JsonValue::String(dt.to_rfc3339());
                }
                return row
                    .try_get::<&str, _>(i)
                    .ok()
                    .flatten()
                    .map(|s| JsonValue::String(s.to_string()))
                    .unwrap_or(JsonValue::Null);
            }
            ColumnType::Datetime
            | ColumnType::Datetime2
            | ColumnType::Datetime4
            | ColumnType::Datetimen
            | ColumnType::Daten
            | ColumnType::Timen => {
                // Tiberius with its `chrono` feature exposes try_get<T>
                // for NaiveDateTime / NaiveDate / NaiveTime / DateTime<Utc>.
                // Without these, DATETIME columns silently return None and
                // become NULL downstream - the cascade-style bug we're
                // hunting. ISO-formatted strings travel cleanly to
                // DuckDB's read_json_auto which re-parses them as
                // TIMESTAMP / DATE / TIME.
                if let Ok(Some(dt)) = row.try_get::<chrono::NaiveDateTime, _>(i) {
                    return JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string());
                }
                if let Ok(Some(d)) = row.try_get::<chrono::NaiveDate, _>(i) {
                    return JsonValue::String(d.format("%Y-%m-%d").to_string());
                }
                if let Ok(Some(t)) = row.try_get::<chrono::NaiveTime, _>(i) {
                    return JsonValue::String(t.format("%H:%M:%S%.f").to_string());
                }
                row.try_get::<&str, _>(i)
                    .ok()
                    .flatten()
                    .map(|s| JsonValue::String(s.to_string()))
                    .unwrap_or(JsonValue::Null)
            }
            // VARBINARY / BINARY / IMAGE: base64. JSON can't carry raw bytes.
            ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => {
                use base64::engine::general_purpose::STANDARD as B64;
                use base64::Engine as _;
                row.try_get::<&[u8], _>(i)
                    .ok()
                    .flatten()
                    .map(|b| JsonValue::String(B64.encode(b)))
                    .unwrap_or(JsonValue::Null)
            }
            // GUID -> tiberius re-exposes its own Uuid type. Convert to
            // standard 8-4-4-4-12 hex form via its Display impl. If the
            // re-export changes name across versions, fall through to
            // the &str path which Tiberius supports for Guid columns.
            // GUID: tiberius only provides FromSql for its re-exported
            // Uuid type (the &str accessor doesn't match a Guid column, so
            // the old code always returned NULL). Emit the standard
            // 8-4-4-4-12 hex form.
            ColumnType::Guid => row
                .try_get::<tiberius::Uuid, _>(i)
                .ok()
                .flatten()
                .map(|u| JsonValue::String(u.to_string()))
                .unwrap_or(JsonValue::Null),
            // Everything else (NVarchar / Char / NText / SsVariant / etc):
            // string path. Tiberius' &str accessor handles N* types via
            // UTF-16 -> UTF-8 internally.
            _ => row
                .try_get::<&str, _>(i)
                .ok()
                .flatten()
                .map(|s| JsonValue::String(s.to_string()))
                .unwrap_or(JsonValue::Null),
        }
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
                            sql_literal(v, None, Dialect::Cassandra)
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
                _ => {
                    next_offset = None;
                    break;
                }
            }
        }
        // A non-null cursor surviving the loop means we stopped on the
        // page cap, not because the scroll was exhausted: more points
        // remain. Fail loud rather than materialize a silent subset.
        if next_offset.is_some() {
            return Err(pagination_capped_err(
                "qdrant",
                all_points.len(),
                spec.max_pages,
            ));
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
        let mut more_pending = false;
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
                more_pending = false;
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
                more_pending = false;
                break;
            }
            match last_id {
                Some(id) => {
                    after = Some(id);
                    more_pending = true;
                }
                None => {
                    more_pending = false;
                    break;
                }
            }
        }
        if more_pending {
            return Err(pagination_capped_err(
                "weaviate",
                all_objects.len(),
                spec.max_pages,
            ));
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
        let mut more_pending = false;
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
                more_pending = false;
                break;
            };
            let page_len = arr.len();
            for v in arr {
                all_rows.push(v.clone());
            }
            if page_len < spec.page_size as usize {
                more_pending = false;
                break;
            }
            offset += spec.page_size;
            more_pending = true;
        }
        if more_pending {
            return Err(pagination_capped_err(
                "milvus",
                all_rows.len(),
                spec.max_pages,
            ));
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
        let content = std::fs::read_to_string(&spec.path)
            .map_err(|e| EngineError::Query(format!("xml: read {}: {}", spec.path, e)))?;
        let rows = walk_xml_to_rows(&content, &spec.row_path, &self.cancel)?;
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
        // Drain stdout AND stderr on dedicated threads, the same way run()
        // does, so the child can never deadlock against a full OS pipe
        // buffer (~64 KiB on Windows). The previous code polled try_wait()
        // to exit and only read via wait_with_output() afterwards - a
        // user command emitting more than the buffer (a verbose build log,
        // a recursive listing, `type`/`cat` of a file) blocked writing
        // stdout/stderr while we blocked waiting for exit. With no timeout
        // that hung forever; with one it was killed and misreported as a
        // timeout, discarding output. Concurrent readers keep both pipes
        // drained regardless of size.
        use std::io::Read;
        let mut stdout_pipe = child
            .stdout
            .take()
            .ok_or_else(|| EngineError::Query("shell: stdout not captured".into()))?;
        let mut stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| EngineError::Query("shell: stderr not captured".into()))?;
        let stdout_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut buf);
            buf
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf);
            buf
        });
        // Poll: cancel kills the child; timeout kills the child; else
        // wait for natural exit.
        //
        // On the abort paths (cancel / timeout / wait error) we DON'T join
        // the reader threads: a shell spawns the real command as a
        // grandchild that inherits the pipe write ends, and killing the
        // shell does not kill the grandchild. read_to_end would then block
        // until the grandchild exits on its own - which for a `sleep 30`
        // is exactly the hang the timeout is meant to escape. We discard
        // the output when aborting anyway, so the reader threads are left
        // to finish on their own (they exit once the grandchild releases
        // the pipe). Only the natural-exit path joins to collect output.
        let deadline = spec
            .timeout_ms
            .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
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
        };
        let stdout_bytes = stdout_reader.join().unwrap_or_default();
        let stderr_bytes = stderr_reader.join().unwrap_or_default();
        let duration_ms = started.elapsed().as_millis() as i64;
        let exit_code = status.code().unwrap_or(-1);
        let mut row = serde_json::Map::new();
        row.insert(
            "stdout".into(),
            JsonValue::String(String::from_utf8_lossy(&stdout_bytes).into_owned()),
        );
        row.insert(
            "stderr".into(),
            JsonValue::String(String::from_utf8_lossy(&stderr_bytes).into_owned()),
        );
        row.insert("exit_code".into(), JsonValue::from(exit_code));
        row.insert("duration_ms".into(), JsonValue::from(duration_ms));
        materialize_jsonobjects_as_table(db, &spec.node_id, &[JsonValue::Object(row)])?;
        Ok(format!(
            "shell: exit {} in {}ms -> {}",
            exit_code, duration_ms, spec.node_id
        ))
    }

    /// src.ftp: connect, login, list `directory`, filter by optional
    /// glob `pattern`, download up to `max_files`. Each file becomes a
    /// row {filename, size, content_b64, modified}. Content is base64-
    /// encoded so the row stays JSON-clean for downstream stages /
    /// CSV sinks; downstream can use `from_base64()` in DuckDB if it
    /// needs raw bytes back.
    fn run_ftp_source(&self, db: &Path, spec: &FtpSourceSpec) -> Result<String, EngineError> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        use suppaftp::FtpStream;
        self.check_cancelled()?;
        let addr = format!("{}:{}", spec.host, spec.port);
        let mut ftp = FtpStream::connect(&addr)
            .map_err(|e| EngineError::Query(format!("ftp connect {}: {}", addr, e)))?;
        if spec.secure {
            return Err(EngineError::Config(
                "src.ftp: secure=true (FTPS) requires the rustls TLS wrapper which isn't wired up yet. Use secure=false (plain FTP) or wait for the FTPS-explicit feature.".into(),
            ));
        }
        ftp.login(&spec.user, &spec.password)
            .map_err(|e| EngineError::Query(format!("ftp login: {}", e)))?;
        if !spec.directory.is_empty() && spec.directory != "/" {
            ftp.cwd(&spec.directory)
                .map_err(|e| EngineError::Query(format!("ftp cwd {}: {}", spec.directory, e)))?;
        }
        let names = ftp
            .nlst(None)
            .map_err(|e| EngineError::Query(format!("ftp nlst: {}", e)))?;
        let mut rows: Vec<JsonValue> = Vec::new();
        for name in names.iter() {
            self.check_cancelled()?;
            if rows.len() as u64 >= spec.max_files {
                break;
            }
            if let Some(p) = &spec.pattern {
                if !glob_match(p, name) {
                    continue;
                }
            }
            let size = ftp.size(name).ok().map(|n| n as i64);
            // mdtm returns NaiveDateTime in UTC by the FTP spec.
            let modified = ftp
                .mdtm(name)
                .ok()
                .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string());
            let bytes = match ftp.retr_as_buffer(name) {
                Ok(cur) => cur.into_inner(),
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "ftp retr {}: {}",
                        name, e
                    )))
                }
            };
            let mut row = serde_json::Map::new();
            row.insert("filename".into(), JsonValue::String(name.clone()));
            row.insert(
                "size".into(),
                size.map(JsonValue::from).unwrap_or(JsonValue::Null),
            );
            row.insert(
                "modified".into(),
                modified.map(JsonValue::String).unwrap_or(JsonValue::Null),
            );
            row.insert(
                "content_b64".into(),
                JsonValue::String(B64.encode(&bytes)),
            );
            rows.push(JsonValue::Object(row));
        }
        let _ = ftp.quit();
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "ftp: materialized {} file(s) from {}:{} into {}",
            count, spec.host, spec.port, spec.node_id
        ))
    }

    /// xf.ai.embed: per-row embedding via an OpenAI-compatible API.
    /// Reads the upstream view, batches rows into groups of
    /// batch_size, sends the input_column text array to /v1/embeddings,
    /// zips the returned vectors back into the rows under
    /// output_column. Works with OpenAI, Cohere (via baseUrl override),
    /// Voyage, llama.cpp's embedding server, or any other
    /// OpenAI-shaped endpoint.
    ///
    /// Establishes the AI credential pattern the other xf.ai.* tiles
    /// will follow: apiKey lives in stage props for now (revisable
    /// later if we add a secure keystore - just rewires this one read).
    fn run_ai_embed(&self, db: &Path, spec: &AiEmbedSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(db, &spec.node_id, &[])?;
            return Ok(format!(
                "ai.embed: 0 upstream rows -> {}",
                spec.node_id
            ));
        }
        let endpoint = format!("{}/v1/embeddings", spec.base_url.trim_end_matches('/'));
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            // Pull the text from each row; missing / non-string values
            // become empty strings so the API call doesn't fail on a
            // single bad row.
            let inputs: Vec<String> = chunk
                .iter()
                .map(|row| {
                    row.get(&spec.input_column)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            let body = serde_json::json!({
                "model": spec.model,
                "input": inputs,
            });
            let resp = ureq::post(&endpoint)
                .set("Authorization", &format!("Bearer {}", spec.api_key))
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("ai.embed parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "ai.embed HTTP {}: {}",
                        code, body
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "ai.embed transport: {}",
                        e
                    )))
                }
            };
            // OpenAI shape: response.data is an array of {index, embedding: [...]}.
            // Order is guaranteed to match the input order per the API contract.
            let data = response
                .get("data")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if data.len() != chunk.len() {
                return Err(EngineError::Query(format!(
                    "ai.embed: expected {} embeddings, got {}",
                    chunk.len(),
                    data.len()
                )));
            }
            for (row, item) in chunk.iter().zip(data.iter()) {
                let embedding = item.get("embedding").cloned().unwrap_or(JsonValue::Null);
                let mut obj = match row {
                    JsonValue::Object(m) => m.clone(),
                    _ => serde_json::Map::new(),
                };
                obj.insert(spec.output_column.clone(), embedding);
                out.push(JsonValue::Object(obj));
            }
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.embed ({}): embedded {} row(s) into {}",
            spec.model, count, spec.node_id
        ))
    }

    /// src.kinesis: single-shard read via direct HTTP + AWS SigV4
    /// (reuses the helper shipped with src.dynamodb). 3-step protocol
    /// per AWS Kinesis API:
    ///   1. ListShards -> get shard IDs
    ///   2. GetShardIterator -> get a starting iterator
    ///   3. GetRecords loop -> consume up to max_records
    /// Each record's Data field is base64-encoded; if the decoded
    /// payload is a JSON object the object is the row, otherwise we
    /// fall back to {partition_key, sequence_number, data}.
    fn run_kinesis_source(
        &self,
        db: &Path,
        spec: &KinesisSourceSpec,
    ) -> Result<String, EngineError> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        self.check_cancelled()?;
        let host = format!("kinesis.{}.amazonaws.com", spec.region);
        let endpoint = format!("https://{}/", host);
        // Helper: sign + post a Kinesis JSON request, return parsed response.
        let call = |target: &str, body: &serde_json::Value| -> Result<JsonValue, EngineError> {
            let body_str = body.to_string();
            let now = chrono::Utc::now();
            let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
            let date = now.format("%Y%m%d").to_string();
            let signed = aws_sigv4_sign(
                "POST",
                "/",
                "",
                &host,
                &datetime,
                &date,
                "kinesis",
                &spec.region,
                target,
                &body_str,
                &spec.access_key_id,
                &spec.secret_access_key,
                spec.session_token.as_deref(),
            );
            let mut req = ureq::post(&endpoint)
                .set("Host", &host)
                .set("Content-Type", "application/x-amz-json-1.0")
                .set("X-Amz-Date", &datetime)
                .set("X-Amz-Target", target)
                .set("Authorization", &signed.authorization);
            if let Some(tok) = &spec.session_token {
                req = req.set("X-Amz-Security-Token", tok);
            }
            match req.send_string(&body_str) {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("kinesis parse: {}", e))),
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "kinesis HTTP {} {}: {}",
                        code, target, b
                    )))
                }
                Err(e) => Err(EngineError::Query(format!("kinesis transport: {}", e))),
            }
        };
        // 1. ListShards
        let shards_resp = call(
            "Kinesis_20131202.ListShards",
            &serde_json::json!({"StreamName": spec.stream_name}),
        )?;
        let shards = shards_resp
            .get("Shards")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let shard_id = shards
            .get(spec.shard_index)
            .and_then(|s| s.get("ShardId"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                EngineError::Query(format!(
                    "kinesis: no shard at index {} (got {} shards)",
                    spec.shard_index,
                    shards.len()
                ))
            })?;
        // 2. GetShardIterator
        let iter_resp = call(
            "Kinesis_20131202.GetShardIterator",
            &serde_json::json!({
                "StreamName": spec.stream_name,
                "ShardId": shard_id,
                "ShardIteratorType": spec.iterator_type,
            }),
        )?;
        let mut shard_iter = iter_resp
            .get("ShardIterator")
            .and_then(|v| v.as_str())
            .ok_or_else(|| EngineError::Query("kinesis: no ShardIterator returned".into()))?
            .to_string();
        // 3. GetRecords loop.
        let mut out: Vec<JsonValue> = Vec::new();
        let mut polls = 0;
        while (out.len() as u64) < spec.max_records && polls < 100 {
            self.check_cancelled()?;
            let remaining = (spec.max_records - out.len() as u64).min(10000);
            let rec_resp = call(
                "Kinesis_20131202.GetRecords",
                &serde_json::json!({
                    "ShardIterator": shard_iter,
                    "Limit": remaining,
                }),
            )?;
            let records = rec_resp
                .get("Records")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let got = records.len();
            for r in records {
                if (out.len() as u64) >= spec.max_records {
                    break;
                }
                let data_b64 = r.get("Data").and_then(|v| v.as_str()).unwrap_or("");
                let partition_key = r
                    .get("PartitionKey")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let sequence_number = r
                    .get("SequenceNumber")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let decoded = B64.decode(data_b64).unwrap_or_default();
                let decoded_str = String::from_utf8_lossy(&decoded).into_owned();
                // If JSON object, that IS the row; otherwise fallback row.
                match serde_json::from_str::<JsonValue>(&decoded_str) {
                    Ok(JsonValue::Object(o)) => out.push(JsonValue::Object(o)),
                    _ => {
                        let mut row = serde_json::Map::new();
                        row.insert("partition_key".into(), JsonValue::String(partition_key));
                        row.insert(
                            "sequence_number".into(),
                            JsonValue::String(sequence_number),
                        );
                        row.insert("data".into(), JsonValue::String(decoded_str));
                        out.push(JsonValue::Object(row));
                    }
                }
            }
            // Advance iterator. If response gives a NextShardIterator,
            // we follow it; otherwise we're done.
            match rec_resp.get("NextShardIterator").and_then(|v| v.as_str()) {
                Some(next) => shard_iter = next.to_string(),
                None => break,
            }
            // If this poll returned nothing and we're at the tip,
            // stop - don't busy-loop on an empty stream.
            if got == 0 {
                break;
            }
            polls += 1;
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "kinesis: read {} record(s) from {}/shard[{}] -> {}",
            count, spec.stream_name, spec.shard_index, spec.node_id
        ))
    }

    /// src.dynamodb: scan a DynamoDB table via direct HTTP + AWS
    /// SigV4 signing. Pure-Rust dependency (avoids the 300-service
    /// aws-sdk-rust tree). DynamoDB's typed-attribute response shape
    /// ({"S": "x"}, {"N": "5"}, {"BOOL": true}, ...) gets unwrapped
    /// into plain JSON before each row is emitted. Pagination
    /// follows LastEvaluatedKey across up to max_pages requests.
    fn run_dynamodb_source(
        &self,
        db: &Path,
        spec: &DynamoDbSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let host = format!("dynamodb.{}.amazonaws.com", spec.region);
        let endpoint = format!("https://{}/", host);
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut last_key: Option<JsonValue> = None;
        let mut pages = 0u64;
        loop {
            self.check_cancelled()?;
            if pages >= spec.max_pages {
                break;
            }
            // Build request body.
            let mut body = serde_json::Map::new();
            body.insert(
                "TableName".into(),
                JsonValue::String(spec.table_name.clone()),
            );
            body.insert("Limit".into(), JsonValue::from(spec.limit_per_page as i64));
            if let Some(lk) = &last_key {
                body.insert("ExclusiveStartKey".into(), lk.clone());
            }
            let body_str = serde_json::Value::Object(body).to_string();
            // Sign with SigV4 + send.
            let now = chrono::Utc::now();
            let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
            let date = now.format("%Y%m%d").to_string();
            let signed_headers = aws_sigv4_sign(
                "POST",
                "/",
                "",
                &host,
                &datetime,
                &date,
                "dynamodb",
                &spec.region,
                "DynamoDB_20120810.Scan",
                &body_str,
                &spec.access_key_id,
                &spec.secret_access_key,
                spec.session_token.as_deref(),
            );
            let mut req = ureq::post(&endpoint)
                .set("Host", &host)
                .set("Content-Type", "application/x-amz-json-1.0")
                .set("X-Amz-Date", &datetime)
                .set("X-Amz-Target", "DynamoDB_20120810.Scan")
                .set("Authorization", &signed_headers.authorization);
            if let Some(tok) = &spec.session_token {
                req = req.set("X-Amz-Security-Token", tok);
            }
            let resp = req.send_string(&body_str);
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("dynamodb parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "dynamodb HTTP {}: {}",
                        code, b
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("dynamodb transport: {}", e)))
                }
            };
            // Items: array of {col: {S: "x"}, col2: {N: "5"}, ...}
            let items = response
                .get("Items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for item in items {
                all_rows.push(unwrap_dynamodb_attrs(&item));
            }
            // Pagination: stop when no LastEvaluatedKey returned.
            last_key = response.get("LastEvaluatedKey").cloned();
            pages += 1;
            if last_key.is_none() {
                break;
            }
        }
        // A surviving LastEvaluatedKey means the scan stopped on the page
        // cap with more rows still to read - fail loud, don't silently
        // materialize a partial scan.
        if last_key.is_some() {
            return Err(pagination_capped_err(
                "dynamodb",
                all_rows.len(),
                spec.max_pages,
            ));
        }
        let count = all_rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "dynamodb: scanned {} row(s) from {} ({} page(s)) -> {}",
            count, spec.table_name, pages, spec.node_id
        ))
    }

    /// snk.email: per-row SMTP send via lettre. For each upstream
    /// row, build an email from {to_column, subject_column,
    /// body_column}, send via SMTPS on `port` to `host`. Optional
    /// credentials (host doesn't always require auth for relay).
    fn run_email_sink(&self, db: &Path, spec: &EmailSinkSpec) -> Result<String, EngineError> {
        use lettre::message::{header, Message};
        use lettre::transport::smtp::authentication::Credentials;
        use lettre::{SmtpTransport, Transport};
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            return Ok(format!("email sink: 0 upstream rows"));
        }
        // Build the SMTP transport once per stage.
        let mut builder = SmtpTransport::relay(&spec.host)
            .map_err(|e| EngineError::Query(format!("smtp relay setup: {}", e)))?
            .port(spec.port);
        if !spec.user.is_empty() {
            builder = builder.credentials(Credentials::new(
                spec.user.clone(),
                spec.password.clone(),
            ));
        }
        let mailer = builder.build();
        let from_parsed: lettre::message::Mailbox = spec
            .from_address
            .parse()
            .map_err(|e| EngineError::Query(format!("from address: {}", e)))?;
        let mut sent = 0usize;
        for row in rows.iter() {
            self.check_cancelled()?;
            let to_str = row
                .get(&spec.to_column)
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    EngineError::Query(format!(
                        "snk.email: row missing `{}` column",
                        spec.to_column
                    ))
                })?;
            let subject_str = row
                .get(&spec.subject_column)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let body_str = row
                .get(&spec.body_column)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let to_parsed: lettre::message::Mailbox = to_str
                .parse()
                .map_err(|e| EngineError::Query(format!("to address `{}`: {}", to_str, e)))?;
            let msg = Message::builder()
                .from(from_parsed.clone())
                .to(to_parsed)
                .subject(subject_str)
                .header(header::ContentType::TEXT_PLAIN)
                .body(body_str.to_string())
                .map_err(|e| EngineError::Query(format!("snk.email build: {}", e)))?;
            mailer
                .send(&msg)
                .map_err(|e| EngineError::Query(format!("snk.email send: {}", e)))?;
            sent += 1;
        }
        Ok(format!(
            "email sink: sent {} message(s) via {}:{}",
            sent, spec.host, spec.port
        ))
    }

    /// src.webhook: bind 127.0.0.1:port, collect up to max_requests
    /// inbound HTTP requests with a global timeout deadline, close
    /// the listener. Each request body becomes a row: if the body
    /// parses as JSON object, the object is the row; if it parses
    /// as a JSON array, each element becomes a row; otherwise a
    /// fallback row {method, path, body} captures the raw request.
    fn run_webhook_source(
        &self,
        db: &Path,
        spec: &WebhookSourceSpec,
    ) -> Result<String, EngineError> {
        use std::io::Write;
        use std::net::TcpListener;
        use std::time::{Duration, Instant};
        self.check_cancelled()?;
        let addr = format!("127.0.0.1:{}", spec.port);
        let listener = TcpListener::bind(&addr)
            .map_err(|e| EngineError::Query(format!("webhook bind {}: {}", addr, e)))?;
        // Non-blocking so we can poll cancel + global deadline.
        listener
            .set_nonblocking(true)
            .map_err(|e| EngineError::Query(format!("webhook set_nonblocking: {}", e)))?;
        let deadline = Instant::now() + Duration::from_millis(spec.timeout_ms);
        let mut rows: Vec<JsonValue> = Vec::new();
        while (rows.len() as u64) < spec.max_requests {
            self.check_cancelled()?;
            if Instant::now() >= deadline {
                break;
            }
            let (mut stream, _addr) = match listener.accept() {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("webhook accept: {}", e)));
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_millis(1000)))
                .ok();
            // Read request bytes until headers parse + body fully consumed.
            let (method, path, headers, body) = match read_http_request(&mut stream) {
                Ok(req) => req,
                Err(e) => {
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = stream.flush();
                    eprintln!("webhook: skipping malformed request: {}", e);
                    continue;
                }
            };
            // Path filter: 404 anything that doesn't match.
            if let Some(prefix) = &spec.path_filter {
                if !path.starts_with(prefix) {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = stream.flush();
                    continue;
                }
            }
            // Always send 200 OK so the caller knows we got it.
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            let _ = stream.flush();
            // Parse the body: prefer JSON shape, fall back to raw.
            let body_str = String::from_utf8_lossy(&body).into_owned();
            match serde_json::from_str::<JsonValue>(&body_str) {
                Ok(JsonValue::Object(o)) => rows.push(JsonValue::Object(o)),
                Ok(JsonValue::Array(arr)) => {
                    for v in arr {
                        rows.push(v);
                    }
                }
                _ => {
                    let mut row = serde_json::Map::new();
                    row.insert("method".into(), JsonValue::String(method));
                    row.insert("path".into(), JsonValue::String(path));
                    row.insert("body".into(), JsonValue::String(body_str));
                    let mut hdrs = serde_json::Map::new();
                    for (k, v) in headers {
                        hdrs.insert(k, JsonValue::String(v));
                    }
                    row.insert("headers".into(), JsonValue::Object(hdrs));
                    rows.push(JsonValue::Object(row));
                }
            }
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "webhook: collected {} request(s) on :{} -> {}",
            count, spec.port, spec.node_id
        ))
    }

    /// src.email: connect to an IMAP server via rustls, select a
    /// mailbox, fetch up to max_messages most recent messages by
    /// reverse-UID order, parse with mail-parser, emit one row per
    /// message with {uid, from, to, subject, date, body_text}.
    ///
    /// Basic auth only - OAuth (gmail / o365) is a follow-up that
    /// needs the same model-API-credential pattern xf.ai.embed
    /// established, plus a token-refresh worker.
    fn run_email_source(
        &self,
        db: &Path,
        spec: &EmailSourceSpec,
    ) -> Result<String, EngineError> {
        use imap::ClientBuilder;
        use mail_parser::MessageParser;
        self.check_cancelled()?;
        let client = ClientBuilder::new(&spec.host, spec.port)
            .connect()
            .map_err(|e| EngineError::Query(format!("imap connect: {}", e)))?;
        let mut session = client
            .login(&spec.user, &spec.password)
            .map_err(|(e, _)| EngineError::Query(format!("imap login: {}", e)))?;
        let mailbox = session
            .select(&spec.mailbox)
            .map_err(|e| EngineError::Query(format!("imap select {}: {}", spec.mailbox, e)))?;
        let total = mailbox.exists as u64;
        if total == 0 {
            let _ = session.logout();
            materialize_jsonobjects_as_table(db, &spec.node_id, &[])?;
            return Ok(format!(
                "email: 0 messages in {} -> {}",
                spec.mailbox, spec.node_id
            ));
        }
        // Fetch the last N messages (by sequence). seqset is 1-based.
        let from = total.saturating_sub(spec.max_messages.saturating_sub(1)).max(1);
        let seqset = format!("{}:{}", from, total);
        let messages = session
            .fetch(&seqset, "(UID BODY[])")
            .map_err(|e| EngineError::Query(format!("imap fetch: {}", e)))?;
        let parser = MessageParser::default();
        let mut rows: Vec<JsonValue> = Vec::new();
        for fetch in messages.iter() {
            self.check_cancelled()?;
            let uid = fetch.uid.map(|u| u as i64).unwrap_or(0);
            let body = fetch.body().unwrap_or_default();
            let parsed = parser
                .parse(body)
                .ok_or_else(|| EngineError::Query("email parse failed".into()))?;
            let from = parsed
                .from()
                .map(|addrs| {
                    addrs
                        .iter()
                        .filter_map(|a| a.address())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let to = parsed
                .to()
                .map(|addrs| {
                    addrs
                        .iter()
                        .filter_map(|a| a.address())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let subject = parsed.subject().unwrap_or("").to_string();
            let date = parsed.date().map(|d| d.to_rfc3339()).unwrap_or_default();
            let body_text = parsed.body_text(0).map(|s| s.into_owned()).unwrap_or_default();
            let mut row = serde_json::Map::new();
            row.insert("uid".into(), JsonValue::from(uid));
            row.insert("from".into(), JsonValue::String(from));
            row.insert("to".into(), JsonValue::String(to));
            row.insert("subject".into(), JsonValue::String(subject));
            row.insert("date".into(), JsonValue::String(date));
            row.insert("body_text".into(), JsonValue::String(body_text));
            rows.push(JsonValue::Object(row));
        }
        let _ = session.logout();
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "email: materialized {} message(s) from {}@{}:{}/{} into {}",
            count, spec.user, spec.host, spec.port, spec.mailbox, spec.node_id
        ))
    }

    /// code.javascript: per-row JS transform via boa_engine. The
    /// user's script is evaluated once to define a `transform`
    /// function, then transform(row) runs per row. Row goes in as a
    /// JS object (marshalled from JSON), transformed row comes back
    /// as a JS object and is converted back. Boa is sandboxed - no
    /// fs, no fetch, no DOM, no setTimeout.
    fn run_javascript(
        &self,
        db: &Path,
        spec: &JavaScriptSpec,
    ) -> Result<String, EngineError> {
        use boa_engine::{js_string, Context, Source};
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(db, &spec.node_id, &[])?;
            return Ok(format!(
                "code.javascript: 0 upstream rows -> {}",
                spec.node_id
            ));
        }
        // One context per stage - state is intentionally not shared
        // across stages, but IS shared across rows within a stage so
        // the user can declare helpers once at the top of the script.
        let mut ctx = Context::default();
        ctx.eval(Source::from_bytes(spec.script.as_bytes()))
            .map_err(|e| EngineError::Query(format!("js: script eval: {}", e)))?;
        let transform = ctx
            .global_object()
            .get(js_string!("transform"), &mut ctx)
            .map_err(|e| EngineError::Query(format!("js: lookup transform: {}", e)))?;
        if !transform.is_callable() {
            return Err(EngineError::Query(
                "js: script must define a global `transform` function".into(),
            ));
        }
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            // JSON -> JsValue
            let js_in = boa_engine::JsValue::from_json(row, &mut ctx).map_err(|e| {
                EngineError::Query(format!("js: row -> JsValue: {}", e))
            })?;
            let result = transform
                .as_callable()
                .ok_or_else(|| EngineError::Query("js: transform not callable".into()))?
                .call(&boa_engine::JsValue::Undefined, &[js_in], &mut ctx)
                .map_err(|e| EngineError::Query(format!("js: transform call: {}", e)))?;
            // JsValue -> JSON (only objects make sense as rows). Guard the
            // value's shape BEFORE calling to_json: boa's to_json PANICS
            // (aborting the whole process) on Undefined, so a transform
            // that falls off the end with no return value would crash the
            // run instead of surfacing a clean error.
            if result.is_undefined() || result.is_null() {
                return Err(EngineError::Query(format!(
                    "js: transform must return an object, got {} (did the function return a value?)",
                    if result.is_undefined() { "undefined" } else { "null" }
                )));
            }
            let json_out = result.to_json(&mut ctx).map_err(|e| {
                EngineError::Query(format!("js: result -> JSON: {}", e))
            })?;
            if !json_out.is_object() {
                return Err(EngineError::Query(format!(
                    "js: transform must return an object, got: {}",
                    json_out
                )));
            }
            out.push(json_out);
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "code.javascript: transformed {} row(s) into {}",
            count, spec.node_id
        ))
    }

    /// xf.ai.dedupe: drop rows whose embedding is within `threshold`
    /// cosine similarity of a previously-kept row. Reads the
    /// embedding column as a list of floats from each row. No API
    /// call - pure local math. O(N^2) per stage which is fine for
    /// ETL-scale datasets (low thousands of rows).
    fn run_ai_dedupe(&self, db: &Path, spec: &AiDedupeSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        let mut kept: Vec<JsonValue> = Vec::new();
        let mut kept_embeddings: Vec<Vec<f64>> = Vec::new();
        for row in rows.iter() {
            self.check_cancelled()?;
            let raw = row.get(&spec.embedding_column);
            // Accept either a JSON array directly (when read via
            // read_json_auto) OR a stringified JSON array (when the
            // upstream came through a CSV round-trip - DuckDB keeps
            // list literals as strings in CSV).
            let emb: Option<Vec<f64>> = raw.and_then(|v| match v {
                JsonValue::Array(arr) => Some(
                    arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<_>>(),
                ),
                JsonValue::String(s) => serde_json::from_str::<JsonValue>(s)
                    .ok()
                    .and_then(|j| j.as_array().cloned())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<_>>()),
                _ => None,
            });
            let Some(e) = emb else {
                // Missing/invalid embedding - keep the row (don't
                // silently drop data the user might want).
                kept.push(row.clone());
                kept_embeddings.push(Vec::new());
                continue;
            };
            // Drop if any previously-kept embedding is within threshold.
            let is_dup = kept_embeddings
                .iter()
                .filter(|p| !p.is_empty())
                .any(|p| cosine_similarity(p, &e) >= spec.threshold);
            if !is_dup {
                kept.push(row.clone());
                kept_embeddings.push(e);
            }
        }
        let count = kept.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &kept)?;
        Ok(format!(
            "ai.dedupe: {} -> {} row(s) (threshold {}) into {}",
            rows.len(),
            count,
            spec.threshold,
            spec.node_id
        ))
    }

    /// xf.ai.classify: per-row LLM-backed classifier. Builds a
    /// constrained prompt asking the model to choose exactly one of
    /// the user-supplied categories. Result that's not in the list
    /// gets normalized to "UNKNOWN" so downstream filters don't break.
    fn run_ai_classify(
        &self,
        db: &Path,
        spec: &AiClassifySpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(db, &spec.node_id, &[])?;
            return Ok(format!("ai.classify: 0 upstream rows -> {}", spec.node_id));
        }
        let endpoint = format!("{}/v1/chat/completions", spec.base_url.trim_end_matches('/'));
        let cat_list = spec.categories.join(", ");
        let system_prompt = format!(
            "You are a strict classifier. Pick exactly one of these categories: {}. \
             Reply with only the category name and nothing else.",
            cat_list
        );
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let body = serde_json::json!({
                "model": spec.model,
                "temperature": 0.0,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
            });
            let resp = ureq::post(&endpoint)
                .set("Authorization", &format!("Bearer {}", spec.api_key))
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("ai.classify parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!("ai.classify HTTP {}: {}", code, b)));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("ai.classify transport: {}", e)))
                }
            };
            let raw = response
                .pointer("/choices/0/message/content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            // Constrain to the supplied category list; anything not
            // in it becomes UNKNOWN so downstream pipelines don't
            // see surprise values.
            let chosen = spec
                .categories
                .iter()
                .find(|c| c.eq_ignore_ascii_case(&raw))
                .cloned()
                .unwrap_or_else(|| "UNKNOWN".into());
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), JsonValue::String(chosen));
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.classify ({}): {} row(s) -> {}",
            spec.model, count, spec.node_id
        ))
    }

    /// xf.ai.llm: per-row LLM call via OpenAI-compatible chat
    /// completions API. Renders prompt_template with {col} subst
    /// from each row; if template is empty, sends the input column
    /// text as-is. Optional system prompt + temperature. Result text
    /// lands in output_column.
    ///
    /// Unlike xf.ai.embed which batches inputs in a single request,
    /// chat completions are one prompt per call - N rows = N HTTP
    /// requests. Users should keep dataset sizes manageable or chain
    /// with xf.rows.head to sample.
    fn run_ai_llm(&self, db: &Path, spec: &AiLlmSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(db, &spec.node_id, &[])?;
            return Ok(format!("ai.llm: 0 upstream rows -> {}", spec.node_id));
        }
        let endpoint = format!("{}/v1/chat/completions", spec.base_url.trim_end_matches('/'));
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let user_text = if spec.prompt_template.is_empty() {
                row.get(&spec.input_column)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            } else {
                render_prompt_template(&spec.prompt_template, row)
            };
            let mut messages: Vec<serde_json::Value> = Vec::new();
            if let Some(sys) = &spec.system_prompt {
                messages.push(serde_json::json!({"role": "system", "content": sys}));
            }
            messages.push(serde_json::json!({"role": "user", "content": user_text}));
            let body = serde_json::json!({
                "model": spec.model,
                "messages": messages,
                "temperature": spec.temperature,
            });
            let resp = ureq::post(&endpoint)
                .set("Authorization", &format!("Bearer {}", spec.api_key))
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("ai.llm parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!("ai.llm HTTP {}: {}", code, b)));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("ai.llm transport: {}", e)))
                }
            };
            let content = response
                .pointer("/choices/0/message/content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), JsonValue::String(content));
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.llm ({}): {} row(s) -> {}",
            spec.model, count, spec.node_id
        ))
    }

    /// xf.ai.pii: regex-based PII redaction. For each upstream row,
    /// detect emails / phones / SSNs / credit-card numbers in the
    /// input column and replace each match with `[REDACTED-TYPE]`.
    /// Pure local regex - no API call, no model. LLM-backed redaction
    /// is a follow-up that would share the xf.ai.embed pattern.
    ///
    /// The regex set is intentionally conservative (favor false-
    /// negatives over false-positives) - users with stricter PII
    /// needs should follow up with an LLM-backed pass or NER model.
    fn run_ai_pii(&self, db: &Path, spec: &AiPiiSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        // Compile regex set once per stage (not once per row).
        let patterns = pii_patterns(&spec.types);
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let redacted = patterns.iter().fold(text, |acc, (re, label)| {
                re.replace_all(&acc, *label).into_owned()
            });
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), JsonValue::String(redacted));
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.pii: redacted {} row(s) into {}",
            count, spec.node_id
        ))
    }

    /// xf.ai.chunk: text splitter for RAG / embedding pipelines.
    /// Splits the `input_column` of each upstream row into chunks of
    /// at most `chunk_size` characters with `chunk_overlap` between
    /// successive chunks. mode="explode" emits one row per chunk
    /// (with chunk_index + chunk_count + the rest of the source row);
    /// mode="array" emits one row per source row with the chunks as
    /// a JSON array in `output_column`.
    fn run_ai_chunk(&self, db: &Path, spec: &AiChunkSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        let mut out: Vec<JsonValue> = Vec::new();
        for row in rows.iter() {
            self.check_cancelled()?;
            let text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let chunks = chunk_text(text, spec.chunk_size, spec.chunk_overlap);
            let base = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            if spec.mode == "array" {
                let mut obj = base;
                obj.insert(
                    spec.output_column.clone(),
                    JsonValue::Array(
                        chunks.into_iter().map(JsonValue::String).collect(),
                    ),
                );
                out.push(JsonValue::Object(obj));
            } else {
                // explode (default)
                let count = chunks.len() as i64;
                for (idx, chunk) in chunks.into_iter().enumerate() {
                    let mut obj = base.clone();
                    obj.insert(
                        spec.output_column.clone(),
                        JsonValue::String(chunk),
                    );
                    obj.insert("chunk_index".into(), JsonValue::from(idx as i64));
                    obj.insert("chunk_count".into(), JsonValue::from(count));
                    out.push(JsonValue::Object(obj));
                }
            }
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.chunk: split {} upstream row(s) into {} chunk(s) -> {}",
            rows.len(),
            count,
            spec.node_id
        ))
    }

    /// code.wasm: per-row WebAssembly transform via wasmi (interpreter).
    /// For each upstream row, the engine writes the input column text
    /// into the module's linear memory, calls the exported transform
    /// function (i32, i32) -> i64, then reads the (out_ptr, out_len)
    /// pair back from the returned i64 to recover the result string.
    ///
    /// Each row gets a fresh module instance so state doesn't leak
    /// between rows - safer for user-supplied modules. wasmi is an
    /// interpreter so each call has interpretation overhead; for ETL
    /// (rows in the thousands, not millions per second) it's fine.
    ///
    /// Modules run sandboxed: no host imports, no fs, no network. If
    /// the module's exports don't match the contract we return a
    /// clear EngineError rather than panicking.
    fn run_wasm(&self, db: &Path, spec: &WasmSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(db, &spec.node_id, &[])?;
            return Ok(format!("wasm: 0 upstream rows -> {}", spec.node_id));
        }
        let engine = wasmi::Engine::default();
        let module = wasmi::Module::new(&engine, &spec.wasm_bytes[..])
            .map_err(|e| EngineError::Query(format!("wasm: parse module: {}", e)))?;
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let input_text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let result_text = self.invoke_wasm_transform(&engine, &module, &spec.function, &input_text)?;
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(
                spec.output_column.clone(),
                JsonValue::String(result_text),
            );
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &out)?;
        Ok(format!(
            "wasm ({}): processed {} row(s) into {}",
            spec.function, count, spec.node_id
        ))
    }

    /// Run a single transform invocation against a fresh module
    /// instance. Returns the output string read back from module
    /// memory. Pulled out so the per-row loop stays compact.
    fn invoke_wasm_transform(
        &self,
        engine: &wasmi::Engine,
        module: &wasmi::Module,
        function: &str,
        input: &str,
    ) -> Result<String, EngineError> {
        let mut store = wasmi::Store::new(engine, ());
        let linker = wasmi::Linker::new(engine);
        let instance = linker
            .instantiate(&mut store, module)
            .and_then(|p| p.start(&mut store))
            .map_err(|e| EngineError::Query(format!("wasm: instantiate: {}", e)))?;
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or_else(|| EngineError::Query("wasm: module has no exported `memory`".into()))?;
        let transform = instance
            .get_typed_func::<(i32, i32), i64>(&store, function)
            .map_err(|e| {
                EngineError::Query(format!(
                    "wasm: export `{}(i32, i32) -> i64` not found: {}",
                    function, e
                ))
            })?;
        // Write input at a fixed offset (1024). Modules that want
        // dynamic alloc can ignore this offset and use their own
        // allocator - we still pass our offset as in_ptr.
        let in_ptr: u32 = 1024;
        let in_len: u32 = input.len() as u32;
        memory
            .data_mut(&mut store)
            .get_mut(in_ptr as usize..(in_ptr + in_len) as usize)
            .ok_or_else(|| EngineError::Query("wasm: input doesn't fit in memory".into()))?
            .copy_from_slice(input.as_bytes());
        let packed = transform
            .call(&mut store, (in_ptr as i32, in_len as i32))
            .map_err(|e| EngineError::Query(format!("wasm: call {}: {}", function, e)))?;
        let out_ptr = ((packed >> 32) & 0xFFFFFFFF) as u32;
        let out_len = (packed & 0xFFFFFFFF) as u32;
        let mem_data = memory.data(&store);
        let out_slice = mem_data
            .get(out_ptr as usize..(out_ptr + out_len) as usize)
            .ok_or_else(|| {
                EngineError::Query(format!(
                    "wasm: out (ptr={}, len={}) out of memory bounds (mem_size={})",
                    out_ptr,
                    out_len,
                    mem_data.len()
                ))
            })?;
        String::from_utf8(out_slice.to_vec())
            .map_err(|e| EngineError::Query(format!("wasm: output not utf-8: {}", e)))
    }

    /// src.clipboard: read the system clipboard as text. If it parses
    /// as a JSON array-of-objects the array becomes rows directly; if
    /// it parses as a single JSON object that single object becomes
    /// one row; otherwise we emit one row {text, length}. Fails with
    /// a clear EngineError when the display server isn't reachable
    /// (e.g. headless Linux CI) - arboard's Clipboard::new returns
    /// the underlying platform error.
    fn run_clipboard_source(
        &self,
        db: &Path,
        spec: &ClipboardSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let mut cb = arboard::Clipboard::new()
            .map_err(|e| EngineError::Query(format!("clipboard unavailable: {}", e)))?;
        let text = cb
            .get_text()
            .map_err(|e| EngineError::Query(format!("clipboard get_text: {}", e)))?;
        let rows: Vec<JsonValue> = match serde_json::from_str::<JsonValue>(&text) {
            Ok(JsonValue::Array(arr)) if arr.iter().all(|v| v.is_object()) => arr,
            Ok(JsonValue::Object(o)) => vec![JsonValue::Object(o)],
            _ => {
                let mut row = serde_json::Map::new();
                row.insert("text".into(), JsonValue::String(text.clone()));
                row.insert("length".into(), JsonValue::from(text.chars().count() as i64));
                vec![JsonValue::Object(row)]
            }
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(db, &spec.node_id, &rows)?;
        Ok(format!(
            "clipboard: materialized {} row(s) into {}",
            count, spec.node_id
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
        // Auto-create the target table when it doesn't exist, inferring
        // column types from the upstream DuckDB view. The sink otherwise
        // only INSERTs, so loading into a not-yet-created table failed with
        // "Invalid object name" (issue #8: "newly created tables"). Wrapped
        // in IF OBJECT_ID(...) IS NULL so an existing table is untouched.
        let col_types: std::collections::HashMap<String, String> =
            describe_columns(self, db, &spec.from_view).into_iter().collect();
        let col_defs = cols
            .iter()
            .map(|c| {
                let ty = duckdb_type_to_sqlserver(
                    col_types.get(c).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                );
                format!("{} {}", ss_quote_ident(c), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let create_sql = format!(
            "IF OBJECT_ID('{}', 'U') IS NULL CREATE TABLE {} ({})",
            qualified.replace('\'', "''"),
            qualified,
            col_defs
        );
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
                // Create the table if it isn't there yet (no-op otherwise).
                client
                    .execute(create_sql.as_str(), &[])
                    .await
                    .map_err(|e| format!("create table: {}", e))?;
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
                                    sql_literal(
                                        v,
                                        col_types.get(c).map(|s| s.as_str()),
                                        Dialect::SqlServer,
                                    )
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
        // Open the NDJSON file BEFORE the async block so we own the
        // writer on the executor thread; pass it in by move so the
        // streaming row loop can write each row as it arrives.
        // tiberius's old into_first_result() collected the full row
        // set into a Vec<tiberius::Row> in driver memory, doubled
        // again when we converted to Vec<JsonValue>. For a 1 M-row
        // pull that's two large allocations alive at once; now neither
        // exists - rows pass through tiberius -> writer immediately.
        let writer = JsonLinesWriter::open(&spec.node_id)?;
        let count: usize = rt
            .block_on(async move {
                use futures_util::TryStreamExt;
                use tiberius::QueryItem;
                use tokio_util::compat::TokioAsyncWriteCompatExt;
                let mut writer = writer;
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
                let mut stream = client
                    .query(&spec.query, &[])
                    .await
                    .map_err(|e| format!("query: {}", e))?;
                let mut count = 0_usize;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|e| format!("row stream: {}", e))?
                {
                    let row = match item {
                        QueryItem::Row(r) => r,
                        QueryItem::Metadata(_) => continue,
                    };
                    let mut obj = serde_json::Map::new();
                    for (i, col) in row.columns().iter().enumerate() {
                        let name = col.name().to_string();
                        obj.insert(name, Self::sqlserver_cell_to_json(&row, col, i));
                    }
                    writer
                        .write_row(&JsonValue::Object(obj))
                        .map_err(|e| format!("write row: {}", e))?;
                    count += 1;
                }
                writer
                    .finalize_into_table(db, &spec.node_id)
                    .map_err(|e| format!("finalize: {}", e))?;
                Ok::<usize, String>(count)
            })
            .map_err(|e| EngineError::Query(format!("sqlserver source: {}", e)))?;
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
        let mut truncated = false;
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
                    if (hit_count as u64) < spec.size {
                        break;
                    }
                    if pages >= spec.max_pages {
                        truncated = true;
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
                    if hit_count == 0 {
                        break;
                    }
                    if (hit_count as u64) < spec.size {
                        // Last page didn't fill - we're done even with
                        // search_after.
                        break;
                    }
                    if pages >= spec.max_pages {
                        truncated = true;
                        break;
                    }
                    last_sort = match next_after {
                        Some(s) => Some(s),
                        None => break, // server returned no sort; can't continue.
                    };
                }
            }
        }
        if truncated {
            return Err(pagination_capped_err(
                "elastic",
                all_rows.len(),
                spec.max_pages,
            ));
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
        let mut truncated = false;
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
            // For XML, parse as text + walk row_path; pagination is
            // not meaningful (SOAP has no cross-envelope convention)
            // so we treat the JSON-pointer/cursor variants as no-ops
            // by returning a Null response from this branch.
            let (rows, response): (Vec<JsonValue>, JsonValue) = match spec.response_format {
                RestResponseFormat::Json => {
                    let response: JsonValue = response_raw.into_json().map_err(|e| {
                        EngineError::Query(format!("REST response not JSON: {}", e))
                    })?;
                    let rows = if spec.response_path.is_empty() {
                        response.as_array().cloned().unwrap_or_default()
                    } else {
                        response
                            .pointer(&spec.response_path)
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default()
                    };
                    (rows, response)
                }
                RestResponseFormat::Xml => {
                    let body = response_raw.into_string().map_err(|e| {
                        EngineError::Query(format!("REST XML response read: {}", e))
                    })?;
                    let rows = walk_xml_to_rows(&body, &spec.response_path, &self.cancel)?;
                    (rows, JsonValue::Null)
                }
            };
            let row_count = rows.len();
            all_rows.extend(rows);
            pages += 1;
            // Determine whether another page exists (and set up the next
            // request URL as a side effect). Done BEFORE the page-cap
            // check so we can tell "genuinely exhausted" (advanced=false)
            // from "stopped at the cap with more to fetch" (advanced=true
            // while pages >= max_pages).
            let advanced = match &spec.pagination {
                RestPagination::None => false,
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
                            true
                        }
                        None => false,
                    }
                }
                RestPagination::Offset { offset_param, page_size } => {
                    // A short page means we have reached the end.
                    if (row_count as u64) < *page_size {
                        false
                    } else {
                        offset = offset.saturating_add(*page_size);
                        let sep = if spec.url.contains('?') { '&' } else { '?' };
                        url = format!("{}{}{}={}", spec.url, sep, offset_param, offset);
                        true
                    }
                }
                RestPagination::Page { page_param, .. } => {
                    if row_count == 0 {
                        false
                    } else {
                        page_no = page_no.saturating_add(1);
                        let sep = if spec.url.contains('?') { '&' } else { '?' };
                        url = format!("{}{}{}={}", spec.url, sep, page_param, page_no);
                        true
                    }
                }
                RestPagination::Link => {
                    match link_header.as_deref().and_then(parse_link_next) {
                        Some(next_url) => {
                            url = next_url;
                            true
                        }
                        None => false,
                    }
                }
                RestPagination::NextUrl { next_path } => {
                    let next = response
                        .pointer(next_path)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(String::from);
                    match next {
                        Some(next_url) => {
                            url = next_url;
                            true
                        }
                        None => false,
                    }
                }
            };
            if !advanced {
                break;
            }
            if pages >= spec.max_pages {
                truncated = true;
                break;
            }
        }
        if truncated {
            return Err(pagination_capped_err(
                "rest",
                all_rows.len(),
                spec.max_pages,
            ));
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
                            sql_literal(v, None, Dialect::JsonNative)
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

    /// Count + schema + preview for a view stage's relation in ONE duckdb
    /// spawn instead of three (count_rows + a DESCRIBE + a SELECT). Runs the
    /// three statements together; -json mode prints one result array per
    /// statement, which parse_json_arrays splits. Returns (count, preview);
    /// both None when the relation can't be read (e.g. ctl.switch /
    /// xf.assert nodes that never create a plain `<node>` relation) -
    /// matching the old behavior where count_rows().ok() was None and the
    /// preview was skipped.
    fn count_and_preview(&self, db: &Path, name: &str) -> (Option<u64>, Option<NodePreview>) {
        let q = plan::quote_ident(name);
        let sql = format!(
            "SELECT COUNT(*) AS n FROM {q}; SELECT * FROM (DESCRIBE {q}); SELECT * FROM {q} LIMIT {lim};",
            q = q,
            lim = PREVIEW_ROW_LIMIT
        );
        // -bail makes a missing relation fail the whole invocation; treat
        // that as "nothing to report".
        let out = match self.run(Some(db), &sql, true) {
            Ok(o) => o,
            Err(_) => return (None, None),
        };
        let arrays = parse_json_arrays(&out);
        let count = arrays
            .first()
            .and_then(|a| a.first())
            .and_then(|r| r.get("n"))
            .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|x| x.max(0) as u64)));
        let preview = arrays.get(1).map(|schema_rows| {
            let schema: Vec<Column> = schema_rows.iter().filter_map(parse_describe_row).collect();
            NodePreview {
                node_id: name.to_string(),
                columns: schema,
                rows: arrays.get(2).cloned().unwrap_or_default(),
            }
        });
        (count, preview)
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

/// Removes the per-run marker / preview directory when the batched
/// executor returns. Failures here are ignored - leftover temp files
/// would only matter for disk pressure, and the OS temp dir gets
/// reaped on its own schedule.
struct TempDirGuard(PathBuf);
impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn stage_kind_label(k: &plan::StageKind) -> &'static str {
    match k {
        plan::StageKind::Sink => "sink",
        plan::StageKind::View => "view",
    }
}

/// Whether a batched marker file has finished being written.
///
/// DuckDB's `COPY ... TO file` creates the output file EMPTY at the
/// start of the statement and writes the row only once the (possibly
/// slow) source query finishes - verified: counting a 2 M-row CSV view
/// leaves the marker at 0 bytes for hundreds of ms. The poll loop must
/// not consume a marker in that window, or it reads an empty file, gets
/// no count, and advances past the stage forever (the "0 rows written
/// despite RUN SUCCEEDED" bug). So we distinguish "not finished writing"
/// from "finished, and the count is legitimately null".
enum MarkerState {
    /// Missing, empty, partially written, or momentarily unreadable
    /// (DuckDB still holds the handle open). Caller must wait + retry.
    Pending,
    /// Fully written. Inner is the row count, or None for the count-less
    /// markers (ctl.switch / xf.assert; see execute_batched).
    Ready(Option<u64>),
}

/// Read the single-row NDJSON marker the batched executor emits at each
/// stage boundary. Returns [`MarkerState::Pending`] until the file is a
/// complete, parseable JSON object carrying `_duckle_r`.
fn read_marker(path: &Path) -> MarkerState {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return MarkerState::Pending,
    };
    let line = match content.lines().next() {
        Some(l) => l.trim(),
        None => return MarkerState::Pending,
    };
    if line.is_empty() {
        return MarkerState::Pending;
    }
    let v: JsonValue = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return MarkerState::Pending,
    };
    match v.get("_duckle_r") {
        None => MarkerState::Pending,
        Some(JsonValue::Null) => MarkerState::Ready(None),
        Some(x) => {
            MarkerState::Ready(x.as_u64().or_else(|| x.as_i64().map(|i| i.max(0) as u64)))
        }
    }
}

/// Post-exit wait: once the CLI has exited every marker is final, but
/// the last one may still be flushing to disk. Give it a bounded moment
/// to become readable rather than recording a spurious None.
fn wait_for_marker(path: &Path) -> MarkerState {
    for _ in 0..50 {
        if let MarkerState::Ready(r) = read_marker(path) {
            return MarkerState::Ready(r);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    read_marker(path)
}

/// Promote every marker file `<i>.json` present on disk into a
/// StageFinished event + `NodeRunStatus` entry, advancing `completed`
/// past each one. Also emits StageStarted for the next stage after
/// each completion so the UI sees a continuous progress stream.
///
/// Free function (not a method or closure) so the borrow checker can
/// see we only touch the arguments we declare - on_event is `&mut dyn`
/// so the caller can pass a different closure each invocation.
fn drain_batched_markers(
    completed: &mut usize,
    stages: &[plan::Stage],
    marker_dir: &Path,
    stage_started_at: &mut [Instant],
    nodes: &mut std::collections::BTreeMap<String, NodeRunStatus>,
    on_event: &mut dyn FnMut(PipelineEvent),
    final_pass: bool,
) {
    while *completed < stages.len() {
        let marker = marker_dir.join(format!("{}.json", *completed));
        if !marker.exists() {
            break;
        }
        // Only consume a marker once DuckDB has finished writing it. An
        // existing-but-empty file means the COPY is still running: break
        // and let the caller poll again. After the CLI has exited
        // (final_pass) the file is final, so wait briefly for the last
        // flush instead of recording a spurious None.
        let rows = match read_marker(&marker) {
            MarkerState::Ready(r) => r,
            MarkerState::Pending if final_pass => match wait_for_marker(&marker) {
                MarkerState::Ready(r) => r,
                MarkerState::Pending => break,
            },
            MarkerState::Pending => break,
        };
        let finish = Instant::now();
        let elapsed = finish
            .duration_since(stage_started_at[*completed])
            .as_millis() as u64;
        let stage = &stages[*completed];
        let kind = stage_kind_label(&stage.kind);
        nodes.insert(
            stage.node_id.clone(),
            NodeRunStatus {
                status: "ok".into(),
                kind: Some(kind.into()),
                rows,
                duration_ms: Some(elapsed),
                error: None,
            },
        );
        on_event(PipelineEvent::StageFinished {
            node_id: stage.node_id.clone(),
            kind: kind.into(),
            status: "ok".into(),
            rows,
            duration_ms: elapsed,
            error: None,
        });
        *completed += 1;
        if *completed < stages.len() {
            stage_started_at[*completed] = finish;
            let next = &stages[*completed];
            on_event(PipelineEvent::StageStarted {
                node_id: next.node_id.clone(),
                label: next.label.clone(),
                kind: stage_kind_label(&next.kind).into(),
            });
        }
    }
}

/// Error returned when a paginated source stops at its `maxPages` cap
/// while more data is still available upstream. `maxPages` is a runaway
/// safety net, not a hard maximum, so hitting it means the result was
/// truncated - surface that loudly instead of reporting a partial pull
/// as success (silent data loss).
fn pagination_capped_err(component: &str, fetched: usize, max_pages: u64) -> EngineError {
    EngineError::Query(format!(
        "{}: reached the maxPages={} page limit after {} row(s); more data may remain upstream. \
         Raise the 'maxPages' property to pull the complete result set (maxPages is a runaway \
         safety cap, not a hard maximum).",
        component, max_pages, fetched
    ))
}

/// Read an NDJSON file (one JSON object per line) emitted by DuckDB's
/// `COPY ... TO 'x.json' (FORMAT 'json', ARRAY false)`. Used by the
/// batched executor to read back per-stage previews + schema.
fn read_ndjson(path: &Path) -> Vec<JsonValue> {
    let content = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    content
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            if l.is_empty() {
                None
            } else {
                serde_json::from_str(l).ok()
            }
        })
        .collect()
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
        // Each parameter is `name=value`, separated by ';'. RFC 8288 allows
        // whitespace around '=' (`rel = "next"`), an optional quoted value,
        // and multiple space-separated rel values (`rel="prefetch next"`),
        // so match a whitespace-delimited "next" token rather than a raw
        // substring - the old check missed those forms and could also have
        // false-matched an unquoted `rel=nextpage`.
        for param in rest.split(';') {
            let Some((name, value)) = param.split_once('=') else {
                continue;
            };
            if !name.trim().eq_ignore_ascii_case("rel") {
                continue;
            }
            let value = value.trim().trim_matches('"');
            if value.split_whitespace().any(|tok| tok.eq_ignore_ascii_case("next")) {
                return Some(url.to_string());
            }
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
    // Bulk-Vec path. Most REST connectors collect a bounded response
    // (a single API call's worth of rows) and hand it in here. For
    // sources that pull millions of rows from a database, use
    // JsonLinesWriter directly so the rows never collect in RAM.
    let mut writer = JsonLinesWriter::open(node_id)?;
    for row in rows {
        writer.write_row(row)?;
    }
    writer.finalize_into_table(db, node_id)
}

/// Streaming NDJSON writer used by source loops that don't want to
/// hold every fetched row in memory at once. A 1 M-row x 37-col
/// Oracle pull through the Vec path peaks at ~30 GB resident set;
/// through this writer it's O(64 KiB) regardless of row count.
///
/// Usage:
///   let mut w = JsonLinesWriter::open(&spec.node_id)?;
///   for row in cursor { w.write_row(&row_as_json)?; }
///   w.finalize_into_table(db, &spec.node_id)?;
pub(crate) struct JsonLinesWriter {
    writer: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
}

impl JsonLinesWriter {
    pub(crate) fn open(node_id: &str) -> Result<Self, EngineError> {
        let path = unique_rest_tmp_path(node_id);
        let file = std::fs::File::create(&path)
            .map_err(|e| EngineError::Query(format!("rest source: create tmp file: {}", e)))?;
        Ok(Self {
            writer: std::io::BufWriter::with_capacity(64 * 1024, file),
            path,
        })
    }

    pub(crate) fn write_row(&mut self, row: &JsonValue) -> Result<(), EngineError> {
        use std::io::Write;
        serde_json::to_writer(&mut self.writer, row)
            .map_err(|e| EngineError::Query(format!("rest source: JSON encode: {}", e)))?;
        self.writer
            .write_all(b"\n")
            .map_err(|e| EngineError::Query(format!("rest source: write tmp file: {}", e)))
    }

    pub(crate) fn finalize_into_table(
        mut self,
        db: &Path,
        node_id: &str,
    ) -> Result<(), EngineError> {
        use std::io::Write;
        self.writer
            .flush()
            .map_err(|e| EngineError::Query(format!("rest source: flush tmp file: {}", e)))?;
        // Drop the buffer (closes file handle) before DuckDB reads it.
        drop(self.writer);
        let sql = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_json_auto('{}', format='newline_delimited')",
            plan::quote_ident(node_id),
            self.path
                .display()
                .to_string()
                .replace('\\', "/")
                .replace('\'', "''")
        );
        rest_source_apply(db, &sql)
    }
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
    // Stream each column-zipped row object straight to the NDJSON temp
    // file via JsonLinesWriter instead of building a second re-keyed Vec
    // plus one giant serialized String (peak ~3x the dataset). The writer
    // holds O(64 KiB) regardless of row count - same as the sibling
    // materialize_jsonobjects_as_table. Column order is preserved by the
    // cols iteration order. Output is identical: finalize reads the file
    // with format='newline_delimited' (verified equivalent to 'array').
    let mut writer = JsonLinesWriter::open(node_id)?;
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
        writer.write_row(&JsonValue::Object(obj))?;
    }
    writer.finalize_into_table(db, node_id)
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

/// Convert a two's-complement big-endian byte string (CQL varint encoding)
/// to an exact base-10 string. Arbitrary precision - varints and the
/// unscaled part of decimals can exceed i64/i128, so we do the base-256
/// to base-10 conversion by hand rather than going through a fixed int.
fn cql_be_twos_complement_to_decimal(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0".to_string();
    }
    let negative = bytes[0] & 0x80 != 0;
    // Work on the magnitude: for negatives, take the two's complement.
    let mut mag: Vec<u8> = bytes.to_vec();
    if negative {
        for b in mag.iter_mut() {
            *b = !*b;
        }
        let mut carry = 1u16;
        for b in mag.iter_mut().rev() {
            let v = *b as u16 + carry;
            *b = (v & 0xff) as u8;
            carry = v >> 8;
            if carry == 0 {
                break;
            }
        }
    }
    // Base-256 (big-endian) -> base-10, accumulating little-endian digits.
    let mut decimal: Vec<u8> = vec![0];
    for &byte in &mag {
        let mut carry = byte as u32;
        for d in decimal.iter_mut() {
            let v = (*d as u32) * 256 + carry;
            *d = (v % 10) as u8;
            carry = v / 10;
        }
        while carry > 0 {
            decimal.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    while decimal.len() > 1 && *decimal.last().unwrap() == 0 {
        decimal.pop();
    }
    let mut s = String::new();
    if negative && !(decimal.len() == 1 && decimal[0] == 0) {
        s.push('-');
    }
    for d in decimal.iter().rev() {
        s.push((b'0' + d) as char);
    }
    s
}

/// Render a CQL decimal (unscaled two's-complement BE bytes + scale) as an
/// exact decimal string. value = unscaled * 10^(-scale).
fn cql_decimal_to_string(bytes: &[u8], scale: i32) -> String {
    let unscaled = cql_be_twos_complement_to_decimal(bytes);
    if scale == 0 {
        return unscaled;
    }
    let (sign, digits) = match unscaled.strip_prefix('-') {
        Some(rest) => ("-", rest.to_string()),
        None => ("", unscaled),
    };
    if scale < 0 {
        // Multiply by 10^(-scale): append zeros.
        return format!("{}{}{}", sign, digits, "0".repeat((-scale) as usize));
    }
    let scale = scale as usize;
    if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{}{}.{}", sign, &digits[..point], &digits[point..])
    } else {
        format!("{}0.{}{}", sign, "0".repeat(scale - digits.len()), digits)
    }
}

/// scylla::CqlValue -> JsonValue. Scalars map to their natural JSON type;
/// temporal types render as ISO-8601 strings; decimal/varint keep full
/// precision (decimal as a string, varint as a number when it fits i64 else
/// a string); blob as a `0x...` hex string; inet as its textual address;
/// and list/set/map/tuple/UDT recurse into JSON arrays/objects. Earlier this
/// fell through to Rust `{:?}` Debug for every non-scalar, corrupting
/// timestamps, decimals, collections, etc.
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
        // Milliseconds since the unix epoch -> RFC-3339 UTC.
        CqlValue::Timestamp(ts) => chrono::DateTime::from_timestamp_millis(ts.0)
            .map(|dt| JsonValue::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)))
            .unwrap_or_else(|| JsonValue::from(ts.0)),
        // Days offset by 2^31 from the unix epoch -> YYYY-MM-DD.
        CqlValue::Date(d) => {
            let days = d.0 as i64 - (1i64 << 31);
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|e| chrono::Duration::try_days(days).and_then(|dur| e.checked_add_signed(dur)))
                .map(|nd| JsonValue::String(nd.format("%Y-%m-%d").to_string()))
                .unwrap_or_else(|| JsonValue::from(d.0))
        }
        // Nanoseconds since midnight -> HH:MM:SS[.fffffffff].
        CqlValue::Time(t) => {
            let secs = (t.0 / 1_000_000_000) as u32;
            let nanos = (t.0 % 1_000_000_000) as u32;
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                .map(|nt| JsonValue::String(nt.to_string()))
                .unwrap_or_else(|| JsonValue::from(t.0))
        }
        CqlValue::Decimal(dec) => {
            let (bytes, scale) = dec.as_signed_be_bytes_slice_and_exponent();
            JsonValue::String(cql_decimal_to_string(bytes, scale))
        }
        CqlValue::Varint(vi) => {
            let s = cql_be_twos_complement_to_decimal(vi.as_signed_bytes_be_slice());
            match s.parse::<i64>() {
                Ok(n) => JsonValue::from(n),
                Err(_) => JsonValue::String(s),
            }
        }
        CqlValue::Blob(bytes) => {
            let mut s = String::with_capacity(2 + bytes.len() * 2);
            s.push_str("0x");
            for b in bytes {
                s.push_str(&format!("{:02x}", b));
            }
            JsonValue::String(s)
        }
        CqlValue::Inet(addr) => JsonValue::String(addr.to_string()),
        CqlValue::Duration(d) => {
            let mut obj = serde_json::Map::new();
            obj.insert("months".into(), JsonValue::from(d.months));
            obj.insert("days".into(), JsonValue::from(d.days));
            obj.insert("nanoseconds".into(), JsonValue::from(d.nanoseconds));
            JsonValue::Object(obj)
        }
        CqlValue::List(items) | CqlValue::Set(items) => {
            JsonValue::Array(items.iter().map(cql_value_to_json).collect())
        }
        CqlValue::Map(pairs) => {
            let mut obj = serde_json::Map::new();
            for (k, val) in pairs {
                let key = match cql_value_to_json(k) {
                    JsonValue::String(s) => s,
                    other => other.to_string(),
                };
                obj.insert(key, cql_value_to_json(val));
            }
            JsonValue::Object(obj)
        }
        CqlValue::Tuple(items) => JsonValue::Array(
            items
                .iter()
                .map(|o| o.as_ref().map(cql_value_to_json).unwrap_or(JsonValue::Null))
                .collect(),
        ),
        CqlValue::UserDefinedType { fields, .. } => {
            let mut obj = serde_json::Map::new();
            for (name, val) in fields {
                obj.insert(
                    name.clone(),
                    val.as_ref().map(cql_value_to_json).unwrap_or(JsonValue::Null),
                );
            }
            JsonValue::Object(obj)
        }
    }
}

#[cfg(test)]
mod cql_value_tests {
    use super::{cql_be_twos_complement_to_decimal, cql_decimal_to_string, cql_value_to_json};
    use scylla::frame::response::result::CqlValue;
    use scylla::frame::value::{
        CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlVarint,
    };
    use serde_json::json;

    #[test]
    fn varint_be_to_decimal() {
        assert_eq!(cql_be_twos_complement_to_decimal(&[]), "0");
        assert_eq!(cql_be_twos_complement_to_decimal(&[0x00]), "0");
        assert_eq!(cql_be_twos_complement_to_decimal(&[0x04, 0xD2]), "1234");
        assert_eq!(cql_be_twos_complement_to_decimal(&[0xFF]), "-1");
        assert_eq!(cql_be_twos_complement_to_decimal(&[0x80]), "-128");
        assert_eq!(cql_be_twos_complement_to_decimal(&[0xFF, 0x00]), "-256");
    }

    #[test]
    fn decimal_with_scale() {
        assert_eq!(cql_decimal_to_string(&[0x04, 0xD2], 2), "12.34");
        assert_eq!(cql_decimal_to_string(&[0x04, 0xD2], 0), "1234");
        assert_eq!(cql_decimal_to_string(&[0x01], 4), "0.0001");
        // 0xFF9C = -100, scale 1 -> -10.0
        assert_eq!(cql_decimal_to_string(&[0xFF, 0x9C], 1), "-10.0");
        // negative scale multiplies by 10^(-scale)
        assert_eq!(cql_decimal_to_string(&[0x01], -3), "1000");
    }

    #[test]
    fn temporal_types_render_iso() {
        assert_eq!(
            cql_value_to_json(&CqlValue::Timestamp(CqlTimestamp(1_700_000_000_000))),
            json!("2023-11-14T22:13:20.000Z")
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Date(CqlDate(1u32 << 31))),
            json!("1970-01-01")
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Time(CqlTime(3_661_000_000_000))),
            json!("01:01:01")
        );
    }

    #[test]
    fn numeric_and_binary_types() {
        assert_eq!(
            cql_value_to_json(&CqlValue::Varint(CqlVarint::from_signed_bytes_be_slice(&[0x04, 0xD2]))),
            json!(1234)
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Decimal(
                CqlDecimal::from_signed_be_bytes_slice_and_exponent(&[0x04, 0xD2], 2)
            )),
            json!("12.34")
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            json!("0xdeadbeef")
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Inet("10.0.0.1".parse().unwrap())),
            json!("10.0.0.1")
        );
    }

    #[test]
    fn collections_recurse() {
        assert_eq!(
            cql_value_to_json(&CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)])),
            json!([1, 2])
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Map(vec![(
                CqlValue::Text("a".into()),
                CqlValue::Int(1)
            )])),
            json!({ "a": 1 })
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Tuple(vec![Some(CqlValue::Int(1)), None])),
            json!([1, null])
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::UserDefinedType {
                keyspace: "ks".into(),
                type_name: "t".into(),
                fields: vec![("x".into(), Some(CqlValue::Int(5)))],
            }),
            json!({ "x": 5 })
        );
        assert_eq!(
            cql_value_to_json(&CqlValue::Duration(CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3,
            })),
            json!({ "months": 1, "days": 2, "nanoseconds": 3 })
        );
    }
}

/// Render a serde_json::Value as a Snowflake SQL literal.
/// - NULL  -> NULL
/// - bool  -> TRUE / FALSE
/// - num   -> verbatim
/// - str   -> 'escaped' (single quotes doubled)
/// - obj/arr -> PARSE_JSON('escaped json') so it lands in a VARIANT column
/// Render a SQL Server NUMERIC/DECIMAL as an exact decimal string from
/// tiberius' unscaled i128 value + scale. tiberius' own Display formats
/// the integer and fractional parts independently and signs both, so a
/// negative value like -1.2500 comes out as "-1.-2500". This formats it
/// correctly with a single leading sign and zero-padded fraction, with no
/// precision loss.
fn mssql_numeric_to_string(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let neg = value < 0;
    let abs = value.unsigned_abs();
    let pow = 10u128.pow(scale as u32);
    let int_part = abs / pow;
    let frac_part = abs % pow;
    let body = format!("{}.{:0>width$}", int_part, frac_part, width = scale as usize);
    if neg {
        format!("-{}", body)
    } else {
        body
    }
}

/// Map a DuckDB column type (from DESCRIBE) to the closest SQL Server
/// type, for auto-creating a sink table that doesn't exist yet.
fn duckdb_type_to_sqlserver(t: &str) -> String {
    let up = t.trim().to_ascii_uppercase();
    if up.starts_with("DECIMAL") || up.starts_with("NUMERIC") {
        // DECIMAL(p,s) carries straight over; NUMERIC is the same thing.
        return up.replacen("NUMERIC", "DECIMAL", 1);
    }
    match up.as_str() {
        "BOOLEAN" | "BOOL" => "BIT",
        "TINYINT" | "UTINYINT" => "SMALLINT",
        "SMALLINT" | "INT2" | "USMALLINT" => "SMALLINT",
        "INTEGER" | "INT" | "INT4" | "UINTEGER" => "INT",
        "BIGINT" | "INT8" => "BIGINT",
        "UBIGINT" => "DECIMAL(20,0)",
        "HUGEINT" | "UHUGEINT" => "DECIMAL(38,0)",
        "REAL" | "FLOAT" | "FLOAT4" => "REAL",
        "DOUBLE" | "FLOAT8" => "FLOAT",
        "DATE" => "DATE",
        "TIME" => "TIME",
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" => {
            "DATETIME2"
        }
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "DATETIMEOFFSET",
        "UUID" => "UNIQUEIDENTIFIER",
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => "VARBINARY(MAX)",
        _ => "NVARCHAR(MAX)",
    }
    .to_string()
}

/// Map a DuckDB column type to the closest Oracle type, for auto-creating
/// a sink table that doesn't exist yet.
#[cfg(feature = "oracle")]
fn duckdb_type_to_oracle(t: &str) -> String {
    let up = t.trim().to_ascii_uppercase();
    if up.starts_with("DECIMAL") || up.starts_with("NUMERIC") {
        // DECIMAL(p,s) -> NUMBER(p,s).
        return up.replacen("DECIMAL", "NUMBER", 1).replacen("NUMERIC", "NUMBER", 1);
    }
    match up.as_str() {
        "BOOLEAN" | "BOOL" => "NUMBER(1)",
        "TINYINT" | "UTINYINT" | "SMALLINT" | "USMALLINT" | "INT2" => "NUMBER(5)",
        "INTEGER" | "INT" | "INT4" | "UINTEGER" => "NUMBER(10)",
        "BIGINT" | "INT8" => "NUMBER(19)",
        "UBIGINT" => "NUMBER(20)",
        "HUGEINT" | "UHUGEINT" => "NUMBER(38)",
        "REAL" | "FLOAT" | "FLOAT4" => "BINARY_FLOAT",
        "DOUBLE" | "FLOAT8" => "BINARY_DOUBLE",
        "DATE" => "DATE",
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" => "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "TIMESTAMP WITH TIME ZONE",
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => "BLOB",
        _ => "VARCHAR2(4000)",
    }
    .to_string()
}

/// (name, DuckDB type) pairs for a view/table, via DuckDB DESCRIBE. Used
/// by driver sinks to auto-create a target table with sensible types.
fn describe_columns(engine: &DuckdbEngine, db: &Path, view: &str) -> Vec<(String, String)> {
    engine
        .run_rows(Some(db), &format!("DESCRIBE {}", plan::quote_ident(view)))
        .unwrap_or_default()
        .iter()
        .filter_map(|d| {
            let n = d.get("column_name").and_then(|v| v.as_str())?;
            let t = d.get("column_type").and_then(|v| v.as_str()).unwrap_or("VARCHAR");
            Some((n.to_string(), t.to_string()))
        })
        .collect()
}

/// SQL dialect for literal rendering in driver sinks. JsonNative covers
/// Snowflake + Databricks (both accept `TRUE`/`FALSE` and `PARSE_JSON(...)`);
/// the others need dialect-specific forms (live-confirmed on Oracle 21c,
/// SQL Server 2022, Cassandra 5).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    JsonNative,
    Oracle,
    SqlServer,
    Cassandra,
}

/// Render a JSON cell value as a SQL literal for `dialect`, using the
/// target column's DuckDB type (`target_type`, when the sink knows it) to
/// pick a correct form. The universal-DuckDB rendering broke non-DuckDB
/// sinks: `TRUE`/`FALSE` and `PARSE_JSON(...)` are rejected by Oracle
/// (ORA-00984 / ORA-00904) and SQL Server (Msg 207 / 195), and Oracle does
/// not implicitly parse ISO date/timestamp strings (ORA-01861/01843).
/// Snowflake + Databricks (JsonNative) keep the original behavior exactly.
fn sql_literal(v: &JsonValue, target_type: Option<&str>, dialect: Dialect) -> String {
    // Snowflake + Databricks treat backslash as a string-literal escape
    // char by default, so a value like 'C:\path' silently loses its
    // backslashes (verified: stored as 'C:path'). Double backslashes first
    // for those dialects. Oracle / SQL Server / Cassandra take backslashes
    // literally, so they only need single-quote doubling.
    let quote = |s: &str| {
        let body = match dialect {
            Dialect::JsonNative => s.replace('\\', "\\\\").replace('\'', "''"),
            _ => s.replace('\'', "''"),
        };
        format!("'{}'", body)
    };
    match v {
        JsonValue::Null => "NULL".into(),
        JsonValue::Bool(b) => match dialect {
            // Oracle has no boolean literal in INSERT before 23c; SQL Server
            // BIT takes 1/0. Cassandra CQL and Snowflake/Databricks accept
            // real booleans (CQL is lowercase).
            Dialect::Oracle | Dialect::SqlServer => if *b { "1" } else { "0" }.into(),
            Dialect::Cassandra => if *b { "true" } else { "false" }.into(),
            Dialect::JsonNative => if *b { "TRUE" } else { "FALSE" }.into(),
        },
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            // Oracle is the only dialect that needs explicit date/timestamp
            // construction; the rest accept a quoted ISO string. Key off the
            // DuckDB column type, never the value shape, so a VARCHAR column
            // holding a date-like string stays a plain string.
            if dialect == Dialect::Oracle {
                if let Some(t) = target_type {
                    let norm = t.trim().to_ascii_uppercase();
                    if norm == "DATE" {
                        return format!("TO_DATE({}, 'YYYY-MM-DD')", quote(s));
                    }
                    // TIMESTAMP WITH TIME ZONE / TIMESTAMPTZ carry an offset
                    // suffix (e.g. +00 / +05:30); TO_TIMESTAMP_TZ with TZR
                    // consumes it. .FF6 accepts both fractional and
                    // non-fractional values (verified on Oracle 21c).
                    if norm.contains("TIME ZONE") || norm.contains("TIMESTAMPTZ") {
                        return format!(
                            "TO_TIMESTAMP_TZ({}, 'YYYY-MM-DD HH24:MI:SS.FF6 TZR')",
                            quote(s)
                        );
                    }
                    if norm.starts_with("TIMESTAMP") || norm == "DATETIME" {
                        return format!(
                            "TO_TIMESTAMP({}, 'YYYY-MM-DD HH24:MI:SS.FF6')",
                            quote(s)
                        );
                    }
                }
            }
            quote(s)
        }
        JsonValue::Array(_) | JsonValue::Object(_) => {
            let j = serde_json::to_string(v).unwrap_or_else(|_| "null".into());
            match dialect {
                // PARSE_JSON is Snowflake/Databricks-only; elsewhere store
                // the JSON as a plain quoted string (lands in VARCHAR/text).
                // The JSON text can contain backslashes (escaped chars in
                // string values), so escape it the same way as a string
                // literal for the dialect - quote() handles backslash +
                // single-quote doubling.
                Dialect::JsonNative => format!("PARSE_JSON({})", quote(&j)),
                _ => quote(&j),
            }
        }
    }
}

/// Back-compat shim: the original universal renderer is exactly
/// `sql_literal(v, None, Dialect::JsonNative)`.
#[allow(dead_code)]
fn json_to_sql_literal(v: &JsonValue) -> String {
    sql_literal(v, None, Dialect::JsonNative)
}

/// Oracle's multitable INSERT ALL caps the cumulative number of inserted
/// column-values across every INTO branch at 999. With `num_cols` columns,
/// each appended row contributes `num_cols` values, so at most
/// floor(999 / num_cols) rows fit in one statement. Clamp to the user's
/// `batch_size`, and never go below 1 so a wide table still makes progress
/// one row at a time. (A table with 1000+ columns cannot be represented by
/// INSERT ALL at all and is rejected up front by the caller.)
fn oracle_insert_all_rows_per_stmt(num_cols: usize, batch_size: usize) -> usize {
    let per_stmt = 999 / num_cols.max(1);
    batch_size.min(per_stmt.max(1))
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
        format: None,
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
    // By default the exported / displayed SQL has its secret values
    // replaced with named placeholders. Setting DUCKLE_EXPORT_INCLUDE_SECRETS
    // to a truthy value (1/true/yes/on) opts in to emitting the real
    // credentials so the script runs unchanged against the source (issue
    // #9). The value is then live and the output must be handled with care.
    let include_secrets = std::env::var("DUCKLE_EXPORT_INCLUDE_SECRETS")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    compile_pipeline_sql_opts(doc, include_secrets)
}

/// Compile a pipeline to display-only SQL, choosing explicitly whether to
/// keep real secret values (`include_secrets = true`) or replace them with
/// named placeholders. `compile_pipeline_sql` is the env-driven wrapper;
/// this variant lets callers (and tests) decide deterministically.
pub fn compile_pipeline_sql_opts(
    doc: &PipelineDoc,
    include_secrets: bool,
) -> Result<Vec<StageSql>, EngineError> {
    let compiled = plan::compile(doc)?;
    // This SQL is for DISPLAY only (Plan tab, copy-to-clipboard, export) -
    // it is never executed. The execution path uses the real credentials.
    // Some stages (relational ATTACH, secrets prelude) interpolate a
    // plaintext password / token / key into the SQL, which would otherwise
    // leak into the exported script. Replace those secret VALUES with named
    // placeholders unless the caller explicitly opted in to raw secrets.
    let secrets = if include_secrets {
        Vec::new()
    } else {
        collect_secrets(doc)
    };
    Ok(compiled
        .stages
        .into_iter()
        .map(|s| {
            // Driver-backed and control-flow stages carry no DuckDB SQL
            // (they run in the Duckle runtime via Rust connectors / hooks).
            // For the SQL export we annotate them so the exported script
            // reflects the WHOLE pipeline order, not just the parts that
            // lower to SQL - issue #7.
            let sql = if s.sql.trim().is_empty() {
                procedural_note(&s)
            } else {
                redact_secret_values(&s.sql, &secrets)
            };
            StageSql {
                node_id: s.node_id,
                label: s.label,
                kind: match s.kind {
                    StageKind::Sink => "sink".into(),
                    StageKind::View => "view".into(),
                },
                sql,
            }
        })
        .collect())
}

/// True for a property key that holds a credential (case-insensitive
/// substring match), so its value should never appear in exported SQL.
fn is_secret_prop_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    [
        "password", "passwd", "secret", "token", "apikey", "api_key",
        "privatekey", "private_key", "accesskey", "access_key", "pat",
        "clientsecret", "client_secret", "connectionstring", "connection_string",
        "sas", "credential",
    ]
    .iter()
    .any(|needle| k.contains(needle))
}

/// A secret found in the pipeline: its plaintext VALUE and the named
/// placeholder that stands in for it in exported SQL (e.g. value
/// "sup3r" under prop key "password" -> placeholder "${DUCKLE_PASSWORD}").
struct Secret {
    value: String,
    placeholder: String,
}

/// Turn a secret prop key into an env-style placeholder name, e.g.
/// "password" -> "${DUCKLE_PASSWORD}", "client_secret" ->
/// "${DUCKLE_CLIENT_SECRET}", "apiKey" -> "${DUCKLE_API_KEY}". Non
/// alphanumeric characters become underscores; camelCase boundaries are
/// split so the result reads as a conventional env var.
fn secret_placeholder(key: &str) -> String {
    let mut out = String::from("DUCKLE_");
    let mut prev_lower = false;
    for ch in key.chars() {
        if ch.is_ascii_uppercase() && prev_lower {
            out.push('_');
        }
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
        prev_lower = ch.is_ascii_lowercase() || ch.is_ascii_digit();
    }
    format!("${{{}}}", out.trim_end_matches('_'))
}

/// Collect the plaintext secrets configured anywhere in the pipeline, so
/// they can be replaced in display-only SQL. Only strings of a few chars
/// or more are taken, to avoid redacting incidental short values that
/// collide with SQL tokens. Sorted longest-value-first so a value that
/// contains another is replaced first.
fn collect_secrets(doc: &PipelineDoc) -> Vec<Secret> {
    let mut out: Vec<Secret> = Vec::new();
    for node in &doc.nodes {
        if let Some(JsonValue::Object(props)) = node.data.properties.as_ref() {
            for (key, val) in props {
                if is_secret_prop_key(key) {
                    if let Some(s) = val.as_str() {
                        if s.len() >= 4 {
                            out.push(Secret {
                                value: s.to_string(),
                                placeholder: secret_placeholder(key),
                            });
                        }
                    }
                }
            }
        }
    }
    out.sort_by(|a, b| b.value.len().cmp(&a.value.len()));
    out.dedup_by(|a, b| a.value == b.value);
    out
}

/// Replace each known secret value in `sql` with its named placeholder
/// (e.g. ${DUCKLE_PASSWORD}), so the exported script stays structurally
/// valid and is safe to share - the user substitutes the real value at
/// run time. The export path can opt out of this entirely to emit raw
/// credentials (DUCKLE_EXPORT_INCLUDE_SECRETS=1).
fn redact_secret_values(sql: &str, secrets: &[Secret]) -> String {
    let mut out = sql.to_string();
    for secret in secrets {
        if out.contains(secret.value.as_str()) {
            out = out.replace(secret.value.as_str(), &secret.placeholder);
        }
    }
    out
}

/// A human-readable comment describing a stage that has no DuckDB SQL
/// (a driver source/sink or a ctl.* control step). Keeps the SQL export
/// complete + self-documenting instead of emitting a bare empty stage.
fn procedural_note(s: &plan::Stage) -> String {
    let cid = s.component_id.as_str();
    let body = if let Some(p) = s.run_pipeline_path.as_deref() {
        format!("control step: runs sub-pipeline '{}' as a side effect", p)
    } else if let Some(p) = s.iterate_pipeline_path.as_deref() {
        format!(
            "control step: runs sub-pipeline '{}' x{} (ctl.iterate)",
            p,
            s.iterate_count.unwrap_or(0)
        )
    } else if let Some(p) = s.foreach_pipeline_path.as_deref() {
        format!("control step: runs sub-pipeline '{}' once per upstream row (ctl.foreach)", p)
    } else if let Some(p) = s.install_fallback_path.as_deref() {
        format!("control step: installs fallback pipeline '{}' (ctl.try)", p)
    } else if cid.starts_with("snk.") {
        match s.from.as_deref() {
            Some(from) => format!(
                "sink: '{}' connector writes rows from \"{}\" (runs in the Duckle runtime, no DuckDB SQL)",
                cid, from
            ),
            None => format!(
                "sink: '{}' connector (runs in the Duckle runtime, no DuckDB SQL)",
                cid
            ),
        }
    } else if cid.starts_with("src.") {
        format!(
            "source: '{}' connector fetches rows and materializes them as \"{}\" (runs in the Duckle runtime, no DuckDB SQL)",
            cid, s.node_id
        )
    } else if cid.starts_with("code.") {
        format!(
            "code step: '{}' transforms rows in the Duckle runtime (no DuckDB SQL)",
            cid
        )
    } else if cid.starts_with("xf.ai.") {
        format!(
            "AI step: '{}' processes rows in the Duckle runtime (no DuckDB SQL)",
            cid
        )
    } else {
        format!(
            "'{}' runs in the Duckle runtime (no DuckDB SQL)",
            cid
        )
    };
    format!("/* {} */", body)
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
    // Compare element names ignoring namespace prefix on both sides
    // (`soap:Envelope` matches user's `Envelope` as well as their
    // `soap:Envelope`). The user can still preserve namespaces in
    // their row_path if they want exact-match against a single ns.
    fn local(name: &str) -> &str {
        match name.rfind(':') {
            Some(i) => &name[i + 1..],
            None => name,
        }
    }
    let matches = if row_path.is_empty() {
        // No filter - match every direct child of the root only, to
        // avoid emitting nested structures as separate rows.
        current_path.len() == 1
    } else {
        current_path.len() >= row_path.len()
            && current_path[current_path.len() - row_path.len()..]
                .iter()
                .zip(row_path.iter())
                .all(|(a, b)| local(a) == local(b.as_str()))
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

/// Parse `content` as XML and walk slash-separated `row_path` (e.g.
/// `library/books/book`). Each match becomes one row, with attributes
/// keyed `@name`, text content under `_text`, and nested children
/// nested as sub-objects. Shared between src.xml (file input) and the
/// XML response branch of src.rest / src.soap (in-memory string input).
fn walk_xml_to_rows(
    content: &str,
    row_path: &str,
    cancel: &Arc<AtomicBool>,
) -> Result<Vec<JsonValue>, EngineError> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(content);
    reader.config_mut().trim_text(true);
    let row_path_parts: Vec<String> = row_path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut stack: Vec<(String, serde_json::Map<String, JsonValue>, String)> = Vec::new();
    let mut rows: Vec<JsonValue> = Vec::new();
    let mut buf = Vec::new();
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(EngineError::Cancelled);
        }
        let event = reader
            .read_event_into(&mut buf)
            .map_err(|e| EngineError::Query(format!("xml: parse: {}", e)))?;
        match event {
            Event::Eof => break,
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let v = String::from_utf8_lossy(&attr.value).to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                stack.push((name, builder, String::new()));
            }
            Event::Empty(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let mut builder = serde_json::Map::new();
                for attr in e.attributes().flatten() {
                    let k = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let v = String::from_utf8_lossy(&attr.value).to_string();
                    builder.insert(k, JsonValue::String(v));
                }
                xml_close_element(
                    &mut stack,
                    &mut rows,
                    &row_path_parts,
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
                    xml_close_element(&mut stack, &mut rows, &row_path_parts, &name, builder, text);
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(rows)
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

/// Tiny shell-style glob matcher for src.ftp's pattern filter.
/// Supports `*` (zero or more chars) and `?` (one char). No bracket
/// expressions, no escape - matches the common ETL `orders_*.csv`
/// shape without pulling in a glob crate.
fn glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    fn go(p: &[char], n: &[char]) -> bool {
        if p.is_empty() {
            return n.is_empty();
        }
        match p[0] {
            '*' => {
                // Skip consecutive stars, then try every split.
                let mut i = 1;
                while i < p.len() && p[i] == '*' {
                    i += 1;
                }
                if i == p.len() {
                    return true;
                }
                for j in 0..=n.len() {
                    if go(&p[i..], &n[j..]) {
                        return true;
                    }
                }
                false
            }
            '?' => !n.is_empty() && go(&p[1..], &n[1..]),
            c => !n.is_empty() && n[0] == c && go(&p[1..], &n[1..]),
        }
    }
    go(&p, &n)
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

/// AWS SigV4 signed-headers bundle. We only need the Authorization
/// value; X-Amz-Date / X-Amz-Security-Token / Host are set on the
/// request separately so they show up in the canonical headers.
pub(crate) struct SigV4Signed {
    pub authorization: String,
}

/// Compute an AWS SigV4 v4 signature for a JSON-API style request
/// (DynamoDB, Kinesis, etc - the "x-amz-target" header is part of
/// the signed headers list). Returns the Authorization header value
/// to set on the request.
///
/// Steps mirror the AWS Signing Process exactly:
/// 1. Canonical request (method + path + query + canonical headers
///    + signed headers + hashed payload)
/// 2. String to sign (algorithm + datetime + scope + hashed canonical)
/// 3. Derive signing key (HMAC chain: date, region, service, "aws4_request")
/// 4. Sign string-to-sign with derived key
/// 5. Build authorization header
#[allow(clippy::too_many_arguments)]
pub(crate) fn aws_sigv4_sign(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    host: &str,
    amz_date: &str,
    short_date: &str,
    service: &str,
    region: &str,
    amz_target: &str,
    payload: &str,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
) -> SigV4Signed {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;
    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }
    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(key).expect("hmac");
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let sha256_hex = |s: &str| -> String { hex(&Sha256::digest(s.as_bytes())) };
    // 1. Canonical request. Headers must be sorted lexically.
    let mut canonical_headers: Vec<(String, String)> = vec![
        ("content-type".into(), "application/x-amz-json-1.0".into()),
        ("host".into(), host.to_string()),
        ("x-amz-date".into(), amz_date.to_string()),
        ("x-amz-target".into(), amz_target.to_string()),
    ];
    if let Some(tok) = session_token {
        canonical_headers.push(("x-amz-security-token".into(), tok.to_string()));
    }
    canonical_headers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_header_block: String = canonical_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();
    let signed_headers_list: String = canonical_headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let payload_hash = sha256_hex(payload);
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        canonical_uri,
        canonical_query,
        canonical_header_block,
        signed_headers_list,
        payload_hash
    );
    // 2. String to sign.
    let scope = format!("{}/{}/{}/aws4_request", short_date, region, service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        sha256_hex(&canonical_request)
    );
    // 3. Derive signing key.
    let k_secret = format!("AWS4{}", secret_access_key);
    let k_date = mac(k_secret.as_bytes(), short_date.as_bytes());
    let k_region = mac(&k_date, region.as_bytes());
    let k_service = mac(&k_region, service.as_bytes());
    let k_signing = mac(&k_service, b"aws4_request");
    // 4. Sign string-to-sign.
    let signature = hex(&mac(&k_signing, string_to_sign.as_bytes()));
    // 5. Authorization header.
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key_id, scope, signed_headers_list, signature
    );
    SigV4Signed { authorization }
}

/// Unwrap DynamoDB's typed-attribute representation into plain JSON.
/// {"S": "x"} -> "x"
/// {"N": "5"} -> 5 (number; falls back to string if not parseable)
/// {"BOOL": true} -> true
/// {"NULL": true} -> null
/// {"L": [...]} -> array (recursive)
/// {"M": {...}} -> object (recursive, attribute names as keys)
/// {"SS": ["a","b"]} -> ["a","b"]
/// {"NS": ["1","2"]} -> [1, 2]
/// Unknown shapes pass through unchanged.
pub(crate) fn unwrap_dynamodb_attrs(v: &JsonValue) -> JsonValue {
    let JsonValue::Object(obj) = v else {
        return v.clone();
    };
    // Top-level Items rows look like {col: {S: "x"}, col2: {N: "5"}}
    // - unwrap each value but keep the keys.
    let mut out = serde_json::Map::new();
    for (k, attr) in obj {
        out.insert(k.clone(), unwrap_dynamodb_value(attr));
    }
    JsonValue::Object(out)
}

fn unwrap_dynamodb_value(v: &JsonValue) -> JsonValue {
    let JsonValue::Object(o) = v else {
        return v.clone();
    };
    if o.len() != 1 {
        return v.clone();
    }
    let (tag, inner) = o.iter().next().unwrap();
    match tag.as_str() {
        "S" => inner.clone(),
        "N" => {
            if let JsonValue::String(s) = inner {
                if let Ok(i) = s.parse::<i64>() {
                    return JsonValue::from(i);
                }
                if let Ok(f) = s.parse::<f64>() {
                    return JsonValue::from(f);
                }
                inner.clone()
            } else {
                inner.clone()
            }
        }
        "BOOL" => inner.clone(),
        "NULL" => JsonValue::Null,
        "L" => {
            if let JsonValue::Array(arr) = inner {
                JsonValue::Array(arr.iter().map(unwrap_dynamodb_value).collect())
            } else {
                inner.clone()
            }
        }
        "M" => {
            if let JsonValue::Object(m) = inner {
                let mut out = serde_json::Map::new();
                for (k, attr) in m {
                    out.insert(k.clone(), unwrap_dynamodb_value(attr));
                }
                JsonValue::Object(out)
            } else {
                inner.clone()
            }
        }
        "SS" => inner.clone(),
        "NS" => {
            if let JsonValue::Array(arr) = inner {
                JsonValue::Array(
                    arr.iter()
                        .map(|x| match x {
                            JsonValue::String(s) => s
                                .parse::<i64>()
                                .map(JsonValue::from)
                                .or_else(|_| s.parse::<f64>().map(JsonValue::from))
                                .unwrap_or_else(|_| x.clone()),
                            other => other.clone(),
                        })
                        .collect(),
                )
            } else {
                inner.clone()
            }
        }
        _ => v.clone(),
    }
}

/// Read one HTTP/1.x request off `stream` and return (method, path,
/// headers, body). Tiny ad-hoc parser - good enough for webhook
/// receivers from well-behaved clients. Reads until Content-Length
/// bytes of body have arrived; rejects requests with no
/// Content-Length when there's a non-empty body indication.
fn read_http_request(
    stream: &mut std::net::TcpStream,
) -> Result<(String, String, Vec<(String, String)>, Vec<u8>), String> {
    use std::io::Read;
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    // Read until we see end-of-headers (\r\n\r\n).
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        if buf.len() > 1_048_576 {
            return Err("request too large".into());
        }
        match stream.read(&mut chunk) {
            Ok(0) => return Err("connection closed before headers".into()),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => return Err(format!("read: {}", e)),
        }
    }
    let split_at = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "no header/body split".to_string())?;
    let head = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or_else(|| "empty request".to_string())?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    // Body: any bytes we've already read past the header split + more
    // until we have content_length bytes total.
    let mut body: Vec<u8> = buf[split_at + 4..].to_vec();
    while body.len() < content_length {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    body.truncate(content_length);
    Ok((method, path, headers, body))
}

/// Cosine similarity between two equal-length float vectors. Used by
/// xf.ai.dedupe. Returns 0.0 if either vector is empty / lengths
/// mismatch / either has zero magnitude (all-zero vector).
fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Render a prompt template by substituting `{column_name}` tokens
/// with the row's value for that column. Missing columns or non-
/// scalar values become empty strings. Used by xf.ai.llm and
/// xf.ai.classify.
fn render_prompt_template(template: &str, row: &JsonValue) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    let obj = row.as_object();
    while let Some(c) = chars.next() {
        if c != '{' {
            out.push(c);
            continue;
        }
        let mut key = String::new();
        let mut closed = false;
        for k in chars.by_ref() {
            if k == '}' {
                closed = true;
                break;
            }
            key.push(k);
        }
        if !closed {
            // Unclosed `{...` -> emit literally so user sees mistake.
            out.push('{');
            out.push_str(&key);
            continue;
        }
        let val = obj
            .and_then(|m| m.get(&key))
            .map(|v| match v {
                JsonValue::String(s) => s.clone(),
                JsonValue::Null => String::new(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        out.push_str(&val);
    }
    out
}

/// Compile the regex set for xf.ai.pii based on the user's `types`
/// selection (empty = all). Each regex is paired with the replacement
/// label that gets substituted in for each match. Conservative
/// patterns - favor false-negatives over false-positives. Users with
/// stricter needs should follow up with an LLM-backed pass.
fn pii_patterns(types: &[String]) -> Vec<(regex::Regex, &'static str)> {
    let want = |t: &str| -> bool { types.is_empty() || types.iter().any(|s| s == t) };
    let mut out: Vec<(regex::Regex, &'static str)> = Vec::new();
    if want("email") {
        // RFC 5322 lite - good enough for production-ish ETL use.
        out.push((
            regex::Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").unwrap(),
            "[REDACTED-EMAIL]",
        ));
    }
    if want("credit_card") {
        // Run BEFORE phone so a 16-digit number isn't half-eaten by
        // the phone matcher.
        out.push((
            regex::Regex::new(r"\b(?:\d[ -]*?){13,19}\b").unwrap(),
            "[REDACTED-CREDIT-CARD]",
        ));
    }
    if want("ssn") {
        out.push((
            regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap(),
            "[REDACTED-SSN]",
        ));
    }
    if want("phone") {
        // US-ish plus E.164. REQUIRES a separator (space/dash) or
        // parentheses between groups, so a bare run of digits is NOT
        // treated as a phone. The previous pattern had no separator
        // requirement and no word boundaries, so it destructively
        // redacted any 10-digit token (order ids, account numbers,
        // epoch timestamps) as [REDACTED-PHONE], and partially ate the
        // digits of long/letter-glued card numbers the credit_card
        // pattern missed - both contradict the module's documented
        // "favor false-negatives" design. Won't catch every
        // international format (intentionally conservative).
        // No leading \b: a literal "(" has no word boundary before it, so
        // anchoring there would break the "(415) 555-0100" form. The
        // separator requirement inside the pattern is what rejects bare
        // digit runs; the trailing \b keeps it from eating glued suffixes.
        out.push((
            regex::Regex::new(
                r"(?:\+?\d{1,3}[ -])?(?:\(\d{3}\)[ -]?|\d{3}[ -])\d{3}[ -]\d{4}\b",
            )
            .unwrap(),
            "[REDACTED-PHONE]",
        ));
    }
    out
}

/// Split `text` into chunks of at most `size` chars with `overlap`
/// chars between successive chunks. Walks in char (not byte) windows
/// to avoid splitting UTF-8 sequences. Returns at least one chunk
/// even for empty input - callers usually want a row to exist.
fn chunk_text(text: &str, size: usize, overlap: usize) -> Vec<String> {
    if size == 0 {
        return vec![text.to_string()];
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= size {
        return vec![text.to_string()];
    }
    let step = size.saturating_sub(overlap).max(1);
    let mut out: Vec<String> = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let end = (start + size).min(chars.len());
        out.push(chars[start..end].iter().collect());
        if end == chars.len() {
            break;
        }
        start += step;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        aws_sigv4_sign, chunk_text, cosine_similarity, glob_match, mssql_numeric_to_string,
        parse_link_next, pii_patterns, read_marker, render_prompt_template, secret_placeholder,
        unwrap_dynamodb_attrs, MarkerState,
    };

    #[test]
    fn secret_placeholder_derives_env_style_name() {
        // Issue #9: secret values are replaced with a named placeholder so
        // the exported SQL stays valid; the name comes from the prop key.
        assert_eq!(secret_placeholder("password"), "${DUCKLE_PASSWORD}");
        assert_eq!(secret_placeholder("client_secret"), "${DUCKLE_CLIENT_SECRET}");
        assert_eq!(secret_placeholder("apiKey"), "${DUCKLE_API_KEY}");
        assert_eq!(secret_placeholder("connectionString"), "${DUCKLE_CONNECTION_STRING}");
    }

    #[test]
    fn link_next_basic_and_quoted() {
        let h = "<https://api.example.com/items?page=2>; rel=\"next\", \
                 <https://api.example.com/items?page=1>; rel=\"prev\"";
        assert_eq!(
            parse_link_next(h).as_deref(),
            Some("https://api.example.com/items?page=2")
        );
    }

    #[test]
    fn link_next_multi_value_rel() {
        // RFC 8288 allows several space-separated rel values; the old
        // substring check missed "next" when it was not exactly rel="next".
        let h = "<https://api.example.com/p2>; rel=\"prefetch next\"";
        assert_eq!(parse_link_next(h).as_deref(), Some("https://api.example.com/p2"));
        let h2 = "<https://api.example.com/p2>; rel=\"next prev\"";
        assert_eq!(parse_link_next(h2).as_deref(), Some("https://api.example.com/p2"));
    }

    #[test]
    fn link_next_whitespace_around_equals() {
        let h = "<https://api.example.com/p2>; rel = next";
        assert_eq!(parse_link_next(h).as_deref(), Some("https://api.example.com/p2"));
    }

    #[test]
    fn link_next_ignores_lookalikes_and_missing() {
        // A different rel must not match.
        assert_eq!(parse_link_next("<https://x/p2>; rel=\"prev\"").as_deref(), None);
        // "nextpage" is a distinct token and must not match "next".
        assert_eq!(parse_link_next("<https://x/p2>; rel=\"nextpage\"").as_deref(), None);
        // No params at all -> no next, and must not panic.
        assert_eq!(parse_link_next("<https://x/p2>").as_deref(), None);
        assert_eq!(parse_link_next("").as_deref(), None);
    }

    #[test]
    fn mssql_numeric_to_string_signs_once() {
        // tiberius Numeric stores an unscaled i128 value + scale; its own
        // Display signs both parts (-1.2500 -> "-1.-2500"). Ours must sign
        // once and zero-pad the fraction.
        assert_eq!(mssql_numeric_to_string(-12500, 4), "-1.2500");
        assert_eq!(mssql_numeric_to_string(99999999999, 4), "9999999.9999");
        assert_eq!(mssql_numeric_to_string(500, 4), "0.0500"); // zero-pad
        assert_eq!(mssql_numeric_to_string(-5, 2), "-0.05");
        assert_eq!(mssql_numeric_to_string(42, 0), "42"); // scale 0
        assert_eq!(mssql_numeric_to_string(0, 4), "0.0000");
    }
    use serde_json::json;

    /// Flatten MarkerState for assertions: None = Pending, Some(r) = Ready(r).
    fn marker_state(s: MarkerState) -> Option<Option<u64>> {
        match s {
            MarkerState::Pending => None,
            MarkerState::Ready(r) => Some(r),
        }
    }

    #[test]
    fn read_marker_waits_for_complete_file() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, body: &str| {
            let p = dir.path().join(name);
            let mut f = std::fs::File::create(&p).unwrap();
            f.write_all(body.as_bytes()).unwrap();
            f.flush().unwrap();
            p
        };
        // Missing file -> Pending (caller keeps polling).
        assert_eq!(marker_state(read_marker(&dir.path().join("nope.json"))), None);
        // Empty file -> Pending. This is the bug: DuckDB's COPY creates
        // the marker empty while a slow COUNT runs; consuming it here
        // produced "0 rows written despite RUN SUCCEEDED".
        assert_eq!(marker_state(read_marker(&write("empty.json", ""))), None);
        // Partially written JSON -> Pending.
        assert_eq!(marker_state(read_marker(&write("partial.json", "{\"_duckle_"))), None);
        // Object without the key -> Pending.
        assert_eq!(marker_state(read_marker(&write("nokey.json", "{\"x\":1}"))), None);
        // Complete count -> Ready(Some(n)), including large counts.
        assert_eq!(
            marker_state(read_marker(&write("full.json", "{\"_duckle_r\":2000000}"))),
            Some(Some(2_000_000))
        );
        // Legitimate count-less marker (ctl.switch / xf.assert) -> Ready(None).
        assert_eq!(
            marker_state(read_marker(&write("null.json", "{\"_duckle_r\":null}"))),
            Some(None)
        );
    }

    fn redact_all(text: &str) -> String {
        let patterns = pii_patterns(&[]);
        patterns
            .iter()
            .fold(text.to_string(), |acc, (re, lbl)| re.replace_all(&acc, *lbl).into_owned())
    }

    #[test]
    fn unwrap_dynamodb_attrs_handles_known_types() {
        // Simple row with S/N/BOOL
        let row = json!({
            "name": {"S": "alice"},
            "age": {"N": "30"},
            "active": {"BOOL": true},
        });
        let out = unwrap_dynamodb_attrs(&row);
        assert_eq!(out["name"], json!("alice"));
        assert_eq!(out["age"], json!(30));
        assert_eq!(out["active"], json!(true));
        // NULL
        let row = json!({"x": {"NULL": true}});
        assert_eq!(unwrap_dynamodb_attrs(&row), json!({"x": null}));
        // List (L) with nested types
        let row = json!({"tags": {"L": [{"S": "a"}, {"S": "b"}, {"N": "3"}]}});
        assert_eq!(
            unwrap_dynamodb_attrs(&row),
            json!({"tags": ["a", "b", 3]})
        );
        // Map (M)
        let row = json!({"addr": {"M": {"city": {"S": "Tokyo"}, "zip": {"N": "100"}}}});
        assert_eq!(
            unwrap_dynamodb_attrs(&row),
            json!({"addr": {"city": "Tokyo", "zip": 100}})
        );
        // Numeric strings that aren't valid numbers fall back to string
        let row = json!({"weird": {"N": "not-a-num"}});
        assert_eq!(
            unwrap_dynamodb_attrs(&row),
            json!({"weird": "not-a-num"})
        );
        // Float
        let row = json!({"pi": {"N": "3.14159"}});
        let out = unwrap_dynamodb_attrs(&row);
        let pi = out["pi"].as_f64().unwrap();
        assert!((pi - 3.14159).abs() < 1e-6);
    }

    #[test]
    fn aws_sigv4_sign_matches_known_canonical_example() {
        // Use the AWS documentation's well-known SigV4 test vector to
        // sanity-check our derivation chain. (Not a strict roundtrip
        // of the full doc example - we use DynamoDB headers - but if
        // the signature differs for the same inputs across runs we
        // have a determinism bug.)
        let sig1 = aws_sigv4_sign(
            "POST",
            "/",
            "",
            "dynamodb.us-east-1.amazonaws.com",
            "20250525T000000Z",
            "20250525",
            "dynamodb",
            "us-east-1",
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"Music"}"#,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
        );
        let sig2 = aws_sigv4_sign(
            "POST",
            "/",
            "",
            "dynamodb.us-east-1.amazonaws.com",
            "20250525T000000Z",
            "20250525",
            "dynamodb",
            "us-east-1",
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"Music"}"#,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
        );
        // Same inputs -> same signature (determinism)
        assert_eq!(sig1.authorization, sig2.authorization);
        // Authorization should contain the standard fields
        assert!(sig1.authorization.starts_with("AWS4-HMAC-SHA256 Credential="));
        assert!(sig1.authorization.contains("AKIAIOSFODNN7EXAMPLE/20250525/us-east-1/dynamodb/aws4_request"));
        assert!(sig1.authorization.contains("SignedHeaders="));
        assert!(sig1.authorization.contains("Signature="));
        // Session token should appear in SignedHeaders if supplied
        let with_tok = aws_sigv4_sign(
            "POST",
            "/",
            "",
            "dynamodb.us-east-1.amazonaws.com",
            "20250525T000000Z",
            "20250525",
            "dynamodb",
            "us-east-1",
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"Music"}"#,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Some("AQoDYXdzEJ..."),
        );
        assert!(with_tok.authorization.contains("x-amz-security-token"));
        assert_ne!(sig1.authorization, with_tok.authorization);
    }

    #[test]
    fn cosine_similarity_basic_cases() {
        // Identical vectors -> 1.0
        let sim = cosine_similarity(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]);
        assert!((sim - 1.0).abs() < 1e-9, "expected 1.0, got {}", sim);
        // Orthogonal -> 0.0
        let sim = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]);
        assert!(sim.abs() < 1e-9, "expected 0.0, got {}", sim);
        // Opposite -> -1.0
        let sim = cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]);
        assert!((sim - (-1.0)).abs() < 1e-9, "expected -1.0, got {}", sim);
        // Mismatched length -> 0.0 (degrade gracefully)
        assert_eq!(cosine_similarity(&[1.0, 2.0], &[1.0]), 0.0);
        // Zero vector -> 0.0 (no division by zero)
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 2.0]), 0.0);
        // Almost-identical -> close to 1.0
        let sim = cosine_similarity(&[1.0, 1.0, 1.0], &[1.0, 1.0, 0.99]);
        assert!(sim > 0.99, "expected > 0.99, got {}", sim);
    }

    #[test]
    fn render_prompt_template_substitutes_row_values() {
        let row = json!({"name": "alice", "city": "Tokyo", "age": 30});
        assert_eq!(
            render_prompt_template("Hello {name} from {city}!", &row),
            "Hello alice from Tokyo!"
        );
        // numbers get stringified
        assert_eq!(
            render_prompt_template("{name} is {age} years old", &row),
            "alice is 30 years old"
        );
        // missing columns become empty
        assert_eq!(
            render_prompt_template("Hi {name}, your {nonexistent} is here", &row),
            "Hi alice, your  is here"
        );
        // unclosed brace stays literal so user notices
        assert_eq!(
            render_prompt_template("hi {name", &row),
            "hi {name"
        );
        // no placeholders = passthrough
        assert_eq!(
            render_prompt_template("just plain text", &row),
            "just plain text"
        );
    }

    #[test]
    fn pii_patterns_redact_known_shapes() {
        // emails
        assert_eq!(
            redact_all("contact alice@example.com please"),
            "contact [REDACTED-EMAIL] please"
        );
        // SSN
        assert_eq!(
            redact_all("SSN: 123-45-6789"),
            "SSN: [REDACTED-SSN]"
        );
        // phone, parenthesized
        assert_eq!(
            redact_all("call (415) 555-0100 today"),
            "call [REDACTED-PHONE] today"
        );
        // credit card (Luhn-ish 16-digit)
        assert_eq!(
            redact_all("Card: 4242 4242 4242 4242"),
            "Card: [REDACTED-CREDIT-CARD]"
        );
        // no false positive on a normal sentence
        assert_eq!(
            redact_all("hello world, this is fine"),
            "hello world, this is fine"
        );
    }

    #[test]
    fn pii_phone_does_not_eat_bare_digit_runs() {
        // Regression: the phone pattern used to match any 10-digit run,
        // destroying order ids / account numbers / timestamps as
        // [REDACTED-PHONE]. It now requires a separator, so bare digit
        // runs pass through untouched (favor false-negatives).
        // 10-digit runs (below the credit_card pattern's 13-19 range) used
        // to be eaten by phone; they now pass through.
        assert_eq!(redact_all("order id 1234567890 shipped"), "order id 1234567890 shipped");
        assert_eq!(redact_all("account 0001234567"), "account 0001234567");
        // Real, separator-formatted phones are still redacted.
        assert_eq!(redact_all("ring 415-555-0100 now"), "ring [REDACTED-PHONE] now");
        assert_eq!(redact_all("intl +1 415 555 0100 ok"), "intl [REDACTED-PHONE] ok");
    }

    #[test]
    fn chunk_text_splits_with_overlap_and_preserves_utf8() {
        // shorter than size = one chunk untouched
        assert_eq!(chunk_text("abc", 10, 2), vec!["abc"]);
        // exact-size = one chunk
        assert_eq!(chunk_text("abcde", 5, 1), vec!["abcde"]);
        // size 3, overlap 1, step 2: abc, cde, efg, ghi (last is short)
        let chunks = chunk_text("abcdefghi", 3, 1);
        assert_eq!(chunks, vec!["abc", "cde", "efg", "ghi"]);
        // UTF-8 chars treated as single units (not bytes)
        let chunks = chunk_text("hello", 2, 0);
        assert_eq!(chunks, vec!["he", "ll", "o"]);
        // empty input = single empty chunk
        assert_eq!(chunk_text("", 5, 1), vec![""]);
        // overlap >= size collapses to step=1 (no infinite loop)
        let chunks = chunk_text("abcde", 2, 10);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn glob_match_handles_star_and_question() {
        // exact matches
        assert!(glob_match("orders.csv", "orders.csv"));
        assert!(!glob_match("orders.csv", "orders.json"));
        // leading and trailing star
        assert!(glob_match("*.csv", "orders.csv"));
        assert!(glob_match("orders_*.csv", "orders_2025-05.csv"));
        assert!(!glob_match("orders_*.csv", "shipments_2025.csv"));
        // star matches empty
        assert!(glob_match("orders*.csv", "orders.csv"));
        // ? is exactly one char
        assert!(glob_match("v?.txt", "v1.txt"));
        assert!(!glob_match("v?.txt", "v.txt"));
        assert!(!glob_match("v?.txt", "v10.txt"));
        // multiple stars collapse
        assert!(glob_match("**.csv", "orders.csv"));
        // bare star matches anything (including empty)
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
    }
}

#[cfg(test)]
mod sql_literal_tests {
    use super::{sql_literal, Dialect};
    use serde_json::json;

    #[test]
    fn json_native_unchanged() {
        // Snowflake / Databricks must keep the original behavior exactly.
        let d = Dialect::JsonNative;
        assert_eq!(sql_literal(&json!(null), None, d), "NULL");
        assert_eq!(sql_literal(&json!(true), None, d), "TRUE");
        assert_eq!(sql_literal(&json!(false), None, d), "FALSE");
        assert_eq!(sql_literal(&json!(42), None, d), "42");
        assert_eq!(sql_literal(&json!("hi"), None, d), "'hi'");
        assert_eq!(sql_literal(&json!([1, 2]), None, d), "PARSE_JSON('[1,2]')");
        assert_eq!(sql_literal(&json!({"k":1}), None, d), "PARSE_JSON('{\"k\":1}')");
    }

    #[test]
    fn json_native_doubles_backslash() {
        // Snowflake / Databricks treat backslash as a string escape, so a
        // Windows path or regex would silently lose its backslashes
        // (verified on a Snowflake-API emulator: 'C:\path' stored as
        // 'C:path'). Double them. Oracle / SQL Server / Cassandra take
        // backslash literally, so they must NOT be doubled there.
        let v = json!("C:\\path\\file");
        assert_eq!(sql_literal(&v, None, Dialect::JsonNative), "'C:\\\\path\\\\file'");
        assert_eq!(sql_literal(&v, None, Dialect::Oracle), "'C:\\path\\file'");
        assert_eq!(sql_literal(&v, None, Dialect::SqlServer), "'C:\\path\\file'");
        assert_eq!(sql_literal(&v, None, Dialect::Cassandra), "'C:\\path\\file'");
        // A JSON value with a backslash inside a string also gets it
        // doubled in the PARSE_JSON payload for JsonNative.
        let arr = json!(["a\\b"]);
        assert_eq!(
            sql_literal(&arr, None, Dialect::JsonNative),
            "PARSE_JSON('[\"a\\\\\\\\b\"]')"
        );
    }

    #[test]
    fn booleans_per_dialect() {
        assert_eq!(sql_literal(&json!(true), None, Dialect::Oracle), "1");
        assert_eq!(sql_literal(&json!(false), None, Dialect::Oracle), "0");
        assert_eq!(sql_literal(&json!(true), None, Dialect::SqlServer), "1");
        assert_eq!(sql_literal(&json!(false), None, Dialect::SqlServer), "0");
        // CQL has real boolean literals (lowercase).
        assert_eq!(sql_literal(&json!(true), None, Dialect::Cassandra), "true");
        assert_eq!(sql_literal(&json!(false), None, Dialect::Cassandra), "false");
    }

    #[test]
    fn arrays_objects_per_dialect() {
        // Non-JsonNative dialects get a plain quoted JSON string, not PARSE_JSON.
        for d in [Dialect::Oracle, Dialect::SqlServer, Dialect::Cassandra] {
            assert_eq!(sql_literal(&json!([1, 2]), None, d), "'[1,2]'");
            assert_eq!(sql_literal(&json!({"k":1}), None, d), "'{\"k\":1}'");
        }
    }

    #[test]
    fn oracle_temporal_wrapping() {
        let d = Dialect::Oracle;
        assert_eq!(
            sql_literal(&json!("2024-12-31"), Some("DATE"), d),
            "TO_DATE('2024-12-31', 'YYYY-MM-DD')"
        );
        assert_eq!(
            sql_literal(&json!("2024-12-31 14:30:00"), Some("TIMESTAMP"), d),
            "TO_TIMESTAMP('2024-12-31 14:30:00', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        );
        // Microsecond form takes the same .FF6 mask (verified on Oracle 21c).
        assert_eq!(
            sql_literal(&json!("2024-12-31 14:30:00.123456"), Some("TIMESTAMP_NS"), d),
            "TO_TIMESTAMP('2024-12-31 14:30:00.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        );
        // TIMESTAMP WITH TIME ZONE carries an offset suffix -> TO_TIMESTAMP_TZ.
        assert_eq!(
            sql_literal(&json!("2024-12-31 09:00:00+00"), Some("TIMESTAMP WITH TIME ZONE"), d),
            "TO_TIMESTAMP_TZ('2024-12-31 09:00:00+00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZR')"
        );
    }

    #[test]
    fn oracle_no_wrap_without_temporal_type() {
        // The deciding input is the column TYPE, never the value shape:
        // a date-looking string in a VARCHAR (or unknown) column stays plain.
        let d = Dialect::Oracle;
        assert_eq!(sql_literal(&json!("2024-12-31"), None, d), "'2024-12-31'");
        assert_eq!(
            sql_literal(&json!("2024-12-31"), Some("VARCHAR2(4000)"), d),
            "'2024-12-31'"
        );
        // A non-string value into a DATE column is not wrapped.
        assert_eq!(sql_literal(&json!(42), Some("DATE"), d), "42");
    }

    #[test]
    fn sqlserver_leaves_dates_as_strings() {
        // SQL Server implicitly casts ISO date/timestamp strings (live-OK),
        // so it must NOT get TO_DATE wrapping - only bool + JSON change.
        let d = Dialect::SqlServer;
        assert_eq!(sql_literal(&json!("2024-12-31"), Some("DATE"), d), "'2024-12-31'");
        assert_eq!(
            sql_literal(&json!("2024-12-31 14:30:00"), Some("DATETIME2"), d),
            "'2024-12-31 14:30:00'"
        );
    }

    #[test]
    fn quote_escaping_preserved() {
        // Single quotes double in every quoted form, across dialects.
        for d in [Dialect::Oracle, Dialect::SqlServer, Dialect::Cassandra, Dialect::JsonNative] {
            assert_eq!(sql_literal(&json!("O'Brien"), None, d), "'O''Brien'");
        }
    }
}

#[cfg(test)]
mod oracle_insert_all_tests {
    use super::oracle_insert_all_rows_per_stmt as f;

    #[test]
    fn never_exceeds_999_cumulative() {
        // Every statement must keep rows * cols <= 999 (Oracle's INSERT ALL
        // cumulative-value cap). cols >= 1000 is rejected by the caller, so
        // it is not exercised here.
        for &c in &[1usize, 2, 36, 999] {
            assert!(f(c, 1000) * c <= 999, "cols={} overflowed", c);
        }
    }

    #[test]
    fn fixes_reported_off_by_one() {
        // The bug: batch_size 1000 * 1 col = 1000 > 999 -> ORA-00913.
        assert_eq!(f(1, 1000), 999);
        assert_eq!(f(2, 1000), 499); // 998 <= 999
        assert_eq!(f(36, 1000), 27); // 972 <= 999
    }

    #[test]
    fn respects_smaller_batch_size() {
        // A user batch smaller than the 999 cap is honored, not raised.
        assert_eq!(f(1, 10), 10);
    }

    #[test]
    fn always_at_least_one() {
        assert_eq!(f(999, 1000), 1);
        assert_eq!(f(500, 1000), 1);
    }

    #[test]
    fn zero_cols_defensive() {
        // Defensive divisor max(1): 999 / 1 = 999, then .min(1000) = 999.
        assert_eq!(f(0, 1000), 999);
    }
}
