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

pub mod context;
pub mod error_category;
pub mod history;
pub mod lineage;
pub mod plan;
pub mod tls;
pub mod watermark;
mod connectors;
mod run_log;
mod util;
pub(crate) use util::*;
pub use util::is_secret_prop_key;
pub use history::{append_run_record, load_run_history, RunRecord};
pub use plan::{CompiledPipeline, PipelineDoc, Stage, StageKind};
use plan::{
    quote_ident, AiChunkSpec, AiClassifySpec, AiDedupeSpec, AiEmbedSpec, AiLlmSpec, AiPiiSpec,
    AvroSinkSpec, AvroSourceSpec, CassandraSinkSpec, CassandraSourceSpec, ClickHouseSinkSpec,
    ClickHouseSourceSpec, ClipboardSourceSpec, DatabricksSinkSpec, DatabricksSourceSpec,
    DbtSpec, DynamoDbSourceSpec, ElasticSourceSpec, EmailSinkSpec, EmailSourceSpec,
    FormatFileSinkSpec,
    FormatFileSourceSpec, FormatKind, FtpSinkSpec, FtpSourceSpec, GitSourceSpec, JavaScriptSpec,
    KafkaSinkSpec, KafkaSourceSpec, KinesisSourceSpec, MilvusSourceSpec, MongoSinkSpec,
    MongoSourceSpec,
    NatsSinkSpec, NatsSourceSpec, OracleSinkSpec, OracleSourceSpec, PubSubSinkSpec,
    PubSubSourceSpec, QdrantSourceSpec, RabbitSinkSpec, RabbitSourceSpec, RedisSinkSpec,
    RedisSourceSpec, RestPagination, RestResponseFormat, RestSourceSpec, RuntimeSpec, ShellSpec,
    SftpSinkSpec, SftpSourceSpec, SnowflakeAuth, SnowflakeSinkSpec, SnowflakeSourceSpec,
    SqlServerSinkSpec,
    SqlServerSourceSpec, WasmSpec, WeaviateSourceSpec, WebhookSourceSpec, WebhookSpec, XmlSinkSpec,
    XmlSourceSpec,
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

/// Upper bound on input rows for xf.ai.dedupe. The stage compares every row
/// against all previously-kept rows (O(N^2) cosine), so an unbounded input can
/// hang the pipeline for minutes. Above this we fail loud with guidance rather
/// than silently grinding.
const AI_DEDUPE_MAX_ROWS: usize = 25_000;

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

    /// A clone of this engine carrying a FRESH, independent cancel flag, for a
    /// new top-level run. Each run owns its own cancellation scope: cancelling
    /// (or a stale cancel from) one run must not stop another concurrent run,
    /// and a nested sub-pipeline shares THIS run's flag (it clones this engine)
    /// rather than resetting it. Use this at every top-level entry point
    /// (interactive run, per-schedule run) so runs don't share one flag.
    pub fn for_new_run(&self) -> DuckdbEngine {
        DuckdbEngine {
            bin: self.bin.clone(),
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

    /// Row count of an already-materialized upstream view/table, for the
    /// ctl.log / ctl.warn `{rows}` token and ctl.die row-based conditions.
    /// Returns 0 when there's no input or the count can't be read - the
    /// caller treats "couldn't count" the same as empty rather than failing.
    fn count_view(&self, db: &Path, view: Option<&str>) -> u64 {
        let Some(view) = view else { return 0 };
        let sql = format!("SELECT count(*) AS c FROM {}", plan::quote_ident(view));
        self.run_rows(Some(db), &sql)
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.get("c").cloned())
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
            })
            .unwrap_or(0)
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

    /// Column-level lineage for a single SQL query: which source columns feed
    /// each projected column. Asks DuckDB to serialize the SQL to its AST
    /// (json_serialize_sql, a core function - no extension) and resolves the
    /// lineage from it. Foundation for impact analysis / breaking-change diff /
    /// data contracts.
    pub fn column_lineage(&self, sql: &str) -> Result<Vec<lineage::OutputColumn>, EngineError> {
        let q = format!("SELECT json_serialize_sql('{}') AS ast", sql_escape(sql));
        let rows = self.run_rows(None, &q)?;
        let ast = rows
            .into_iter()
            .next()
            .and_then(|r| r.get("ast").cloned())
            .ok_or_else(|| EngineError::Query("lineage: no AST returned".into()))?;
        // A JSON-typed column may come back as a nested object or as a string.
        let ast = match ast {
            JsonValue::String(s) => serde_json::from_str(&s)
                .map_err(|e| EngineError::Query(format!("lineage: parse AST: {}", e)))?,
            other => other,
        };
        if ast.get("error").and_then(|e| e.as_bool()) == Some(true) {
            return Err(EngineError::Query(
                "lineage: the SQL could not be parsed".into(),
            ));
        }
        Ok(lineage::lineage_from_serialized_sql(&ast))
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
        // DuckLake autodetect + snapshot inspector: ATTACH the catalog read-only
        // as duckle_src so the inspect SELECT (build_relational_source) or the
        // ducklake_snapshots() listing resolves (issue #18; Data Diff feature).
        if format == "ducklake" || format == "ducklake_snapshots" {
            p.push_str(&plan::ducklake_attach(options, true));
        }
        p
    }

    // ---- Execution -----------------------------------------------------

    pub fn execute_pipeline(&self, doc: &PipelineDoc) -> RunResult {
        self.execute_pipeline_with_events(doc, None::<&str>, None, |_| {})
    }

    /// Like [`execute_pipeline`], naming the per-pipeline run-log folder
    /// (`<DUCKLE_LOG_DIR>/<pipeline_name>/runtime.log`). Used by headless
    /// runners (the scheduler) that have no event sink but still want the
    /// run logged under the pipeline's name rather than the fallback folder.
    pub fn execute_pipeline_named(&self, doc: &PipelineDoc, pipeline_name: &str) -> RunResult {
        self.execute_pipeline_with_events(doc, None::<&str>, Some(pipeline_name), |_| {})
    }

    /// Execute a pipeline, optionally only the subgraph upstream of
    /// `target`, streaming [`PipelineEvent`]s through `on_event`.
    pub fn execute_pipeline_with_events<F>(
        &self,
        doc: &PipelineDoc,
        target: Option<&str>,
        pipeline_name: Option<&str>,
        mut user_on_event: F,
    ) -> RunResult
    where
        F: FnMut(PipelineEvent),
    {
        let total_start = Instant::now();
        // NOTE: do NOT clear the cancel flag here. Each top-level run is given a
        // fresh flag via for_new_run(); clearing on every entry would let a
        // nested sub-pipeline run (ctl.iterate/foreach/runjob/parallelize) wipe
        // a cancel the user requested mid-loop.

        if !self.bin.exists() {
            return RunResult::failed(
                total_start,
                "DuckDB engine isn't installed yet. Open Setup to install it.".into(),
            );
        }

        // Create the parent folder of every local file sink before running, so a
        // timestamped path like `exports/${date}/out.csv` (already resolved by
        // apply_time_builtins) doesn't fail because today's folder doesn't exist
        // yet - DuckDB's COPY does not create intermediate directories.
        ensure_local_sink_dirs(doc);

        // Secret values to scrub from any error string before it is surfaced or
        // persisted. DuckDB's postgres/mysql ATTACH echoes the full connection
        // string (password included) in connect errors, which would otherwise
        // land verbatim in the UI, run-history JSON, and NDJSON run logs. The
        // export path is already redacted (compile_pipeline_sql_opts); this
        // covers the execution path. (Named distinctly from the SECRET-prelude
        // `secrets` bound later in this fn.)
        let redact_secrets = collect_secrets(doc);

        let compiled = match target {
            Some(t) => plan::compile_partial(doc, t),
            None => plan::compile(doc),
        };
        let compiled = match compiled {
            Ok(c) => c,
            Err(e) => return RunResult::failed(total_start, e.to_string()),
        };

        // Component-level run log (Splunk / Dynatrace), gated on
        // DUCKLE_LOG_DIR. We tee every event through it so BOTH the fast
        // batched path and the per-stage path log uniformly, for every run
        // mode (interactive, scheduled, sub-pipeline). The map gives each
        // line its component id + label.
        let node_meta: std::collections::HashMap<String, run_log::NodeMeta> = doc
            .nodes
            .iter()
            .map(|n| {
                (
                    n.id.clone(),
                    run_log::NodeMeta {
                        component: n.data.component_id.clone().unwrap_or_default(),
                        label: n.data.label.clone(),
                    },
                )
            })
            .collect();
        let run_id = format!("run-{}-{}", std::process::id(), now_nanos());
        let mut runlog = run_log::RunLog::open(pipeline_name, run_id, node_meta);
        let mut on_event = |evt: PipelineEvent| {
            if runlog.enabled() {
                runlog.record(&evt);
            }
            user_on_event(evt);
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
        // xf.incremental high-water marks to persist - but only if the WHOLE
        // run succeeds, so a later-stage failure never advances the mark past
        // rows that were never actually delivered. (state file path, json).
        let mut pending_watermarks: Vec<(std::path::PathBuf, JsonValue)> = Vec::new();

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
                &redact_secrets,
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
                if let Some(RuntimeSpec::RunJob { path, vars }) = stage.runtime.as_ref() {
                    let res = if vars.is_empty() {
                        self.run_subpipeline(path)
                    } else {
                        let subs: std::collections::HashMap<String, String> =
                            vars.iter().cloned().collect();
                        self.run_subpipeline_with_subs(path, &subs)
                    };
                    if let Err(e) = res {
                        result = Err(EngineError::Query(format!("ctl.runjob({}): {}", path, e)));
                        continue;
                    }
                }
                // ctl.iterate: run the sub-pipeline N times, substituting
                // ${ITER_INDEX} into the pipeline JSON before each call.
                if let Some(RuntimeSpec::Iterate { path: iter_path, count }) = stage.runtime.as_ref()
                {
                    let mut iter_err: Option<String> = None;
                    for i in 0..*count {
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
                if let Some(RuntimeSpec::Foreach { path: each_path, concurrency }) =
                    stage.runtime.as_ref()
                {
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
                    // Build each row's ${ITER_*} substitution map up front.
                    let per_row: Vec<std::collections::HashMap<String, String>> = rows
                        .iter()
                        .enumerate()
                        .map(|(i, row)| {
                            let mut subs = std::collections::HashMap::new();
                            subs.insert("ITER_INDEX".to_string(), i.to_string());
                            if let Some(obj) = row.as_object() {
                                for (k, v) in obj {
                                    let val_str = v
                                        .as_str()
                                        .map(String::from)
                                        .unwrap_or_else(|| v.to_string());
                                    subs.insert(format!("ITER_ITEM_{}", k.to_uppercase()), val_str);
                                }
                            }
                            subs
                        })
                        .collect();

                    let mut each_err: Option<String> = None;
                    if *concurrency <= 1 {
                        // Sequential: stop at the first failing row.
                        for (i, subs) in per_row.iter().enumerate() {
                            if let Err(e) = self.run_subpipeline_with_subs(each_path, subs) {
                                each_err =
                                    Some(format!("ctl.foreach({})[row {}]: {}", each_path, i, e));
                                break;
                            }
                        }
                    } else {
                        // Concurrent: run the per-row children in bounded waves,
                        // each on its own thread (and its own temp DB via
                        // run_subpipeline_with_subs -> execute_pipeline). Rows
                        // write to independent targets, so there is no shared
                        // write state. Report the first error by row index.
                        let wave = (*concurrency).min(per_row.len().max(1));
                        'waves: for chunk in per_row.chunks(wave) {
                            let mut handles = Vec::with_capacity(chunk.len());
                            for subs in chunk {
                                let engine = self.clone();
                                let path = each_path.clone();
                                let subs = subs.clone();
                                let idx = subs
                                    .get("ITER_INDEX")
                                    .cloned()
                                    .unwrap_or_default();
                                handles.push(std::thread::spawn(move || {
                                    engine
                                        .run_subpipeline_with_subs(&path, &subs)
                                        .map_err(|e| (idx, e))
                                }));
                            }
                            for h in handles {
                                match h.join() {
                                    Ok(Ok(())) => {}
                                    Ok(Err((idx, e))) => {
                                        each_err = Some(format!(
                                            "ctl.foreach({})[row {}]: {}",
                                            each_path, idx, e
                                        ));
                                        break 'waves;
                                    }
                                    Err(_) => {
                                        each_err = Some(format!(
                                            "ctl.foreach({}): a child thread panicked",
                                            each_path
                                        ));
                                        break 'waves;
                                    }
                                }
                            }
                        }
                    }
                    if let Some(e) = each_err {
                        result = Err(EngineError::Query(e));
                        continue;
                    }
                }
                // ctl.parallelize: snapshot the upstream once, then run each
                // independent branch sub-pipeline concurrently (each in its own
                // temp DB). Side-effect before the pass-through SQL.
                if let Some(RuntimeSpec::Parallelize(spec)) = stage.runtime.as_ref() {
                    let from = stage.from.clone().unwrap_or_else(|| stage.node_id.clone());
                    let snap = unique_rest_tmp_path(&stage.node_id).with_extension("parquet");
                    let snap_sql = snap.display().to_string().replace('\\', "/").replace('\'', "''");
                    let copy = format!(
                        "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET)",
                        plan::quote_ident(&from),
                        snap_sql
                    );
                    let outcome = self.run(Some(&db_path), &copy, false).and_then(|_| {
                        self.run_parallel_branches(&spec.branches, &snap, spec.max_concurrency)
                    });
                    let _ = std::fs::remove_file(&snap);
                    match outcome {
                        Err(e) => {
                            result = Err(EngineError::Query(format!(
                                "ctl.parallelize({}): {}",
                                stage.node_id, e
                            )));
                            continue;
                        }
                        Ok(branch_results) => {
                            // Fold each branch's node statuses into this run so the
                            // Run report + "rows written" include the branch sinks
                            // (they execute as isolated sub-pipelines). Skip the
                            // injected snapshot source, whose id == the parallelize
                            // node id, so it doesn't shadow the parent's own entry.
                            for br in &branch_results {
                                for (nid, st) in &br.nodes {
                                    if *nid == stage.node_id {
                                        continue;
                                    }
                                    on_event(PipelineEvent::StageFinished {
                                        node_id: nid.clone(),
                                        kind: st.kind.clone().unwrap_or_default(),
                                        status: st.status.clone(),
                                        rows: st.rows,
                                        duration_ms: st.duration_ms.unwrap_or(0),
                                        error: st.error.clone(),
                                    });
                                    nodes.insert(
                                        nid.clone(),
                                        NodeRunStatus {
                                            status: st.status.clone(),
                                            kind: st.kind.clone(),
                                            rows: st.rows,
                                            duration_ms: st.duration_ms,
                                            error: st.error.clone(),
                                            category: st.category.clone(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
                // ctl.log / ctl.warn: emit a diagnostic line ({rows} -> the
                // upstream count), then fall through to the pass-through SQL.
                if let Some(RuntimeSpec::Log { level, message }) = stage.runtime.as_ref() {
                    let rows = self.count_view(&db_path, stage.from.as_deref());
                    let msg = message.replace("{rows}", &rows.to_string());
                    on_event(PipelineEvent::Log {
                        node_id: stage.node_id.clone(),
                        level: level.clone(),
                        message: msg,
                    });
                }
                // ctl.die: fail the run when the condition holds against the
                // upstream row count.
                if let Some(RuntimeSpec::Die { message, condition }) = stage.runtime.as_ref() {
                    let rows = self.count_view(&db_path, stage.from.as_deref());
                    let fire = match condition.as_str() {
                        "has-rows" => rows > 0,
                        "no-rows" => rows == 0,
                        _ => true, // "always"
                    };
                    if fire {
                        let msg = message.replace("{rows}", &rows.to_string());
                        on_event(PipelineEvent::Log {
                            node_id: stage.node_id.clone(),
                            level: "error".into(),
                            message: msg.clone(),
                        });
                        result = Err(EngineError::Query(format!("ctl.die: {}", msg)));
                        continue;
                    }
                }
                result = match stage.runtime.as_ref() {
                    // HTTP sink (snk.webhook / snk.rest): materialize the
                    // upstream as JSON via DuckDB, then dispatch one request
                    // per row or one batched request via ureq.
                    Some(RuntimeSpec::Webhook(spec)) => {
                        self.run_webhook(&db_path, &secret_prefix, spec)
                    }
                    // Snowflake / Databricks SQL API sinks: multi-row INSERTs
                    // batched and POSTed with Bearer / token auth.
                    Some(RuntimeSpec::SnowflakeSink(spec)) => {
                        self.run_snowflake_sink(&db_path, &secret_prefix, spec)
                    }
                    Some(RuntimeSpec::DatabricksSink(spec)) => {
                        self.run_databricks_sink(&db_path, &secret_prefix, spec)
                    }
                    // Snowflake / Databricks sources: POST SELECT, parse the
                    // response, materialize as node_id via read_json_auto.
                    Some(RuntimeSpec::SnowflakeSource(spec)) => {
                        self.run_snowflake_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::DatabricksSource(spec)) => {
                        self.run_databricks_source(&db_path, spec)
                    }
                    // Generic HTTP source: fetch URL, walk response_path,
                    // follow cursor pagination, materialize as table.
                    Some(RuntimeSpec::RestSource(spec)) => self.run_rest_source(&db_path, spec),
                    Some(RuntimeSpec::ElasticSource(spec)) => {
                        self.run_elastic_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::MongoSink(spec)) => self.run_mongo_sink(&db_path, spec),
                    Some(RuntimeSpec::MongoSource(spec)) => self.run_mongo_source(&db_path, spec),
                    Some(RuntimeSpec::ClickhouseSink(spec)) => {
                        self.run_clickhouse_sink(&db_path, spec)
                    }
                    Some(RuntimeSpec::ClickhouseSource(spec)) => {
                        self.run_clickhouse_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::SqlserverSink(spec)) => {
                        self.run_sqlserver_sink(&db_path, spec)
                    }
                    Some(RuntimeSpec::SqlserverSource(spec)) => {
                        self.run_sqlserver_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::CassandraSink(spec)) => {
                        self.run_cassandra_sink(&db_path, spec)
                    }
                    Some(RuntimeSpec::CassandraSource(spec)) => {
                        self.run_cassandra_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::OracleSink(spec)) => self.run_oracle_sink(&db_path, spec),
                    Some(RuntimeSpec::OracleSource(spec)) => self.run_oracle_source(&db_path, spec),
                    Some(RuntimeSpec::AdbcSource(spec)) => self.run_adbc_source(&db_path, spec),
                    Some(RuntimeSpec::AttachParquetSource(spec)) => {
                        self.run_attach_parquet_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::MaterializeDuckDb(spec)) => {
                        self.run_materialize_duckdb(&db_path, spec)
                    }
                    Some(RuntimeSpec::RedisSink(spec)) => self.run_redis_sink(&db_path, spec),
                    Some(RuntimeSpec::RedisSource(spec)) => self.run_redis_source(&db_path, spec),
                    Some(RuntimeSpec::QdrantSource(spec)) => self.run_qdrant_source(&db_path, spec),
                    Some(RuntimeSpec::WeaviateSource(spec)) => {
                        self.run_weaviate_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::MilvusSource(spec)) => self.run_milvus_source(&db_path, spec),
                    Some(RuntimeSpec::FormatSource(spec)) => self.run_format_source(&db_path, spec),
                    Some(RuntimeSpec::FormatSink(spec)) => self.run_format_sink(&db_path, spec),
                    Some(RuntimeSpec::KafkaSink(spec)) => self.run_kafka_sink(&db_path, spec),
                    Some(RuntimeSpec::KafkaSource(spec)) => self.run_kafka_source(&db_path, spec),
                    Some(RuntimeSpec::AvroSource(spec)) => self.run_avro_source(&db_path, spec),
                    Some(RuntimeSpec::NatsSink(spec)) => self.run_nats_sink(&db_path, spec),
                    Some(RuntimeSpec::NatsSource(spec)) => self.run_nats_source(&db_path, spec),
                    Some(RuntimeSpec::PubsubSink(spec)) => self.run_pubsub_sink(&db_path, spec),
                    Some(RuntimeSpec::PubsubSource(spec)) => self.run_pubsub_source(&db_path, spec),
                    Some(RuntimeSpec::XmlSource(spec)) => self.run_xml_source(&db_path, spec),
                    Some(RuntimeSpec::XmlSink(spec)) => self.run_xml_sink(&db_path, spec),
                    Some(RuntimeSpec::AvroSink(spec)) => self.run_avro_sink(&db_path, spec),
                    Some(RuntimeSpec::RabbitSink(spec)) => self.run_rabbit_sink(&db_path, spec),
                    Some(RuntimeSpec::RabbitSource(spec)) => self.run_rabbit_source(&db_path, spec),
                    Some(RuntimeSpec::GitSource(spec)) => self.run_git_source(&db_path, spec),
                    Some(RuntimeSpec::Shell(spec)) => self.run_shell(&db_path, spec),
                    Some(RuntimeSpec::Dbt(spec)) => self.run_dbt(&db_path, spec),
                    Some(RuntimeSpec::FtpSource(spec)) => self.run_ftp_source(&db_path, spec),
                    Some(RuntimeSpec::SftpSource(spec)) => self.run_sftp_source(&db_path, spec),
                    Some(RuntimeSpec::FtpSink(spec)) => self.run_ftp_sink(&db_path, spec),
                    Some(RuntimeSpec::SftpSink(spec)) => self.run_sftp_sink(&db_path, spec),
                    Some(RuntimeSpec::ClipboardSource(spec)) => {
                        self.run_clipboard_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::AiEmbed(spec)) => self.run_ai_embed(&db_path, spec),
                    Some(RuntimeSpec::Wasm(spec)) => self.run_wasm(&db_path, spec),
                    Some(RuntimeSpec::Javascript(spec)) => self.run_javascript(&db_path, spec),
                    Some(RuntimeSpec::AiChunk(spec)) => self.run_ai_chunk(&db_path, spec),
                    Some(RuntimeSpec::AiPii(spec)) => self.run_ai_pii(&db_path, spec),
                    Some(RuntimeSpec::AiLlm(spec)) => self.run_ai_llm(&db_path, spec),
                    Some(RuntimeSpec::AiClassify(spec)) => self.run_ai_classify(&db_path, spec),
                    Some(RuntimeSpec::AiDedupe(spec)) => self.run_ai_dedupe(&db_path, spec),
                    Some(RuntimeSpec::EmailSource(spec)) => self.run_email_source(&db_path, spec),
                    Some(RuntimeSpec::WebhookSource(spec)) => {
                        self.run_webhook_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::EmailSink(spec)) => self.run_email_sink(&db_path, spec),
                    Some(RuntimeSpec::DynamodbSource(spec)) => {
                        self.run_dynamodb_source(&db_path, spec)
                    }
                    Some(RuntimeSpec::KinesisSource(spec)) => {
                        self.run_kinesis_source(&db_path, spec)
                    }
                    // Relational-DB upsert: DESCRIBE the upstream first to get
                    // the column list, then assemble INSERT ... ON CONFLICT
                    // (Postgres) or ON DUPLICATE KEY UPDATE (MySQL).
                    Some(RuntimeSpec::Upsert(spec)) => {
                        self.run_upsert(&db_path, &secret_prefix, spec)
                    }
                    // FTS in DuckDB v1.5+ can't see tables created in the same
                    // -c invocation, so we stage in one CLI call then index +
                    // query in a second.
                    Some(RuntimeSpec::TextSearch(spec)) => {
                        self.run_text_search(&db_path, &secret_prefix, &stage.node_id, spec)
                    }
                    // Watermark incremental load: materialize only rows past
                    // the saved mark; queue the new mark for persist-on-success.
                    Some(RuntimeSpec::Incremental(spec)) => self.run_incremental(
                        &db_path,
                        spec,
                        pipeline_name,
                        &mut pending_watermarks,
                    ),
                    // DuckLake change-data-feed source: materialize table_changes
                    // since the saved snapshot; persist the new snapshot on success.
                    Some(RuntimeSpec::DuckLakeCdc(spec)) => self.run_ducklake_cdc(
                        &db_path,
                        spec,
                        pipeline_name,
                        &mut pending_watermarks,
                    ),
                    // Control-flow variants (RunJob / InstallFallback /
                    // Iterate / Foreach / Log / Warn / non-firing Die) already
                    // ran their side effect above, so they fall through here to
                    // the stage's pass-through SQL - as does a plain SQL stage
                    // (None).
                    _ => {
                        if stage.sink_mode.as_deref() == Some("error")
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
                        }
                    }
                };
                // Stop retrying on success OR cancellation - a cancel must
                // exit immediately, not burn through the remaining attempts
                // (the contract is "retry on engine errors, not cancellation").
                if result.is_ok() || matches!(result, Err(EngineError::Cancelled)) {
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
                            category: None,
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
                    let msg = redact_secret_values(&err.to_string(), &redact_secrets);
                    let category = error_category::categorize_error(&msg);
                    nodes.insert(
                        stage.node_id.clone(),
                        NodeRunStatus {
                            status: "error".into(),
                            kind: Some(kind_label.into()),
                            rows: None,
                            duration_ms: Some(elapsed_ms),
                            error: Some(msg.clone()),
                            category: Some(category.into()),
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
            // ctl.try sets the InstallFallback runtime spec on the stage;
            // after a successful run, install it for subsequent stages.
            if let Some(RuntimeSpec::InstallFallback(p)) = stage.runtime.as_ref() {
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

        // Persist xf.incremental / DuckLake-CDC high-water marks ONLY on a
        // fully successful, FULL run. If anything failed or was cancelled we
        // drop them, so the next run re-reads the same window rather than
        // skipping undelivered rows. We also skip a partial "Run from here"
        // run (target.is_some()): it loads rows into a throwaway temp DB and
        // may stop before the sink, so advancing the watermark there would make
        // the next full run skip rows that were never written to any sink.
        if final_status == "ok" && target.is_none() {
            for (path, value) in &pending_watermarks {
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if let Ok(text) = serde_json::to_string_pretty(value) {
                    let _ = std::fs::write(path, text);
                }
            }
        }

        on_event(PipelineEvent::Finished {
            status: final_status.into(),
            duration_ms: total_start.elapsed().as_millis() as u64,
        });

        let category = overall_error
            .as_deref()
            .map(|e| error_category::categorize_error(e).to_string());
        RunResult {
            status: final_status.into(),
            duration_ms: total_start.elapsed().as_millis() as u64,
            nodes,
            preview,
            error: overall_error,
            category,
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
        redact_secrets: &[Secret],
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
            // Preview is emitted BEFORE the count marker (below) for view
            // stages, and only if querying the view for preview rows wouldn't
            // trigger an eager-evaluation problem. We accept the cost here
            // because the preview is the user-visible payoff for batched mode;
            // users would lose it otherwise. Skip preview for components that
            // don't produce <node> and for xf.assert (where the predicate check
            // would fire here rather than at the downstream sink).
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
            // The count marker (<i>.json) is the per-stage "done" signal that
            // drain_batched_markers uses to mark stage i "ok" and advance
            // `completed`, so it MUST be the LAST statement for the stage -
            // emitted only after the row-evaluating preview SELECT above has
            // succeeded. Otherwise, because COUNT(*) lets DuckDB prune the
            // projection, the marker would land even when the view body errors
            // on full-row evaluation (e.g. a divide-by-zero / failed CAST on
            // some row): `completed` would advance past stage i, the preview
            // SELECT would then fail and -bail would abort the batch, and the
            // failure would be mis-attributed to the next (downstream) stage
            // while the real culprit showed as "ok" (audit).
            //
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
                    redact_secret_values(&stderr_str, &redact_secrets)
                };
                nodes.insert(
                    stage.node_id.clone(),
                    NodeRunStatus {
                        status: "error".into(),
                        kind: Some(kind.into()),
                        rows: None,
                        duration_ms: Some(elapsed),
                        error: Some(msg.clone()),
                        category: Some(error_category::categorize_error(&msg).into()),
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
                let stderr_str = redact_secret_values(
                    &String::from_utf8_lossy(&cli_stderr).trim().to_string(),
                    &redact_secrets,
                );
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

        let category = overall_error
            .as_deref()
            .map(|e| error_category::categorize_error(e).to_string());
        RunResult {
            status: final_status.into(),
            duration_ms,
            nodes,
            preview,
            error: overall_error,
            category,
        }
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

/// Removes the temp run database (and its WAL) when dropped, plus any
/// `<db>.*.parquet` temp files that single-consumer sources kept alive as lazy
/// read_parquet VIEWs for the duration of the run (src.adbc -> `<db>.adbc-*`,
/// network-DB sources -> `<db>.attsrc-*`).
struct TempDbGuard(PathBuf);
impl Drop for TempDbGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let mut wal = self.0.clone().into_os_string();
        wal.push(".wal");
        let _ = std::fs::remove_file(PathBuf::from(wal));
        // Sweep this run's view parquets, all named "<db_file>.*.parquet" as a
        // sibling of the run db (keyed off the unique run db name so concurrent
        // runs never sweep each other's files).
        if let (Some(dir), Some(db_name)) = (
            self.0.parent(),
            self.0.file_name().map(|s| s.to_string_lossy().into_owned()),
        ) {
            let prefix = format!("{}.", db_name);
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with(&prefix) && name.ends_with(".parquet") {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
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
                category: None,
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

/// Best-effort: create the parent directory of every local file sink's output
/// path before the run. DuckDB's `COPY ... TO` does not create intermediate
/// directories, so a timestamped path like `exports/${date}/out.csv` (already
/// resolved by [`context::apply_time_builtins`]) would otherwise fail the first
/// time today's folder is needed. Considers only `snk.*` nodes with a string
/// `path` property; skips cloud URIs (anything containing `://`) and driver
/// sinks (which carry no `path`). Errors are ignored - the COPY surfaces the
/// real one if the directory still can't be made.
fn ensure_local_sink_dirs(doc: &PipelineDoc) {
    for node in &doc.nodes {
        let is_sink = node
            .data
            .component_id
            .as_deref()
            .map(|c| c.starts_with("snk."))
            .unwrap_or(false);
        if !is_sink {
            continue;
        }
        let path = match node
            .data
            .properties
            .as_ref()
            .and_then(|p| p.get("path"))
            .and_then(|v| v.as_str())
            .map(str::trim)
        {
            Some(p) if !p.is_empty() && !p.contains("://") => p,
            _ => continue,
        };
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
    }
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
    bin: &Path,
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
    writer.finalize_into_table(bin, db, node_id)
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
        bin: &Path,
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
        let r = apply_duckdb_sql(bin, db, &sql);
        // Clean up the temp NDJSON file whether the load succeeded or failed
        // (DuckDB has already read it by now); otherwise duckle-rest-*.json
        // accumulate in the temp dir forever.
        let _ = std::fs::remove_file(&self.path);
        r
    }

    /// Like finalize_into_table, but reads every JSON field as VARCHAR and
    /// projects through a caller-supplied SELECT list, so the caller can apply
    /// exact per-column casts. Used by the Snowflake source, whose cells are
    /// all JSON strings that must be cast to their real types (TIMESTAMP /
    /// DATE / DECIMAL ...) rather than left to read_json_auto's inference.
    /// `columns_spec` is the body of read_json's `columns={...}` map (no
    /// braces); `select_list` is the projection (e.g. `expr AS "name", ...`).
    pub(crate) fn finalize_typed(
        mut self,
        bin: &Path,
        db: &Path,
        node_id: &str,
        columns_spec: &str,
        select_list: &str,
    ) -> Result<(), EngineError> {
        use std::io::Write;
        self.writer
            .flush()
            .map_err(|e| EngineError::Query(format!("rest source: flush tmp file: {}", e)))?;
        drop(self.writer);
        let path = self
            .path
            .display()
            .to_string()
            .replace('\\', "/")
            .replace('\'', "''");
        let sql = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT {} FROM read_json('{}', format='newline_delimited', columns={{{}}})",
            plan::quote_ident(node_id),
            select_list,
            path,
            columns_spec,
        );
        apply_duckdb_sql(bin, db, &sql)
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
    bin: &Path,
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
    writer.finalize_into_table(bin, db, node_id)
}

/// Snowflake variant of materialize_arrayrows_as_table: writes the raw
/// (string) cells as NDJSON keyed by column name, then reads every column as
/// VARCHAR and projects through `select_list` so each column lands at its real
/// Snowflake type. `columns_spec` (the read_json `columns={...}` body) and
/// `select_list` are both built from resultSetMetaData.rowType by the caller.
fn materialize_typed_arrayrows(
    bin: &Path,
    db: &Path,
    node_id: &str,
    cols: &[String],
    columns_spec: &str,
    select_list: &str,
    rows: &[JsonValue],
) -> Result<(), EngineError> {
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
    writer.finalize_typed(bin, db, node_id, columns_spec, select_list)
}

/// Run a single SQL statement against `db` using the engine's own DuckDB
/// binary. `bin` is threaded down from `&self.bin` so this never depends on
/// the DUCKLE_DUCKDB_BIN environment variable being set - the engine can be
/// constructed with a valid binary and still materialize results even when
/// the process env is empty (tests, embedded hosts).
fn apply_duckdb_sql(bin: &Path, db: &Path, sql: &str) -> Result<(), EngineError> {
    use std::process::Command;
    let mut cmd = Command::new(bin);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let output = cmd
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
    let mut req = crate::tls::http_agent()
        .request(method, url)
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
    let mut req = crate::tls::http_agent()
        .request(method, url)
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

/// The account identifier Snowflake expects inside a key-pair JWT's `iss` /
/// `sub` claims. The REST URL uses the full account (with region / cloud /
/// `privatelink`), but the JWT must use ONLY the account locator: strip
/// everything from the first `.` onward, or for `.global` replication accounts
/// from the first `-` onward. Mirrors Snowflake's official
/// sql-api-generate-jwt generator. Result is uppercased.
///
/// Without this, a regional / PrivateLink account like `xy12345.us-east-1`
/// (or `xy12345.us-east-1.privatelink`) yields `iss = "XY12345.US-EAST-1.USER..."`,
/// which Snowflake rejects with 390144 "JWT token is invalid" (GitHub #22).
fn snowflake_jwt_account(account: &str) -> String {
    let acct = if account.contains(".global") {
        match account.find('-') {
            Some(i) if i > 0 => &account[..i],
            _ => account,
        }
    } else {
        match account.find('.') {
            Some(i) if i > 0 => &account[..i],
            _ => account,
        }
    };
    acct.to_uppercase()
}

/// Build the Authorization header value for a Snowflake request.
/// PAT: just "Bearer <token>". JWT: read the PEM private key,
/// compute the public-key fingerprint Snowflake wants
/// (SHA256:<base64(SHA-256 of SubjectPublicKeyInfo DER)>), build the
/// claims (iss = "ACCOUNT.USER.SHA256:fp", sub = "ACCOUNT.USER",
/// iat = now, exp = now + 3600), sign RS256, and prefix with
/// "Bearer ". ACCOUNT here is the locator-only form (see
/// snowflake_jwt_account). Snowflake also wants the X-Snowflake-Authorization-
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
            let account_upper = snowflake_jwt_account(account);
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
mod snowflake_jwt_tests {
    use super::snowflake_jwt_account;

    #[test]
    fn strips_region_and_privatelink_for_jwt() {
        // Plain locator stays as-is (uppercased).
        assert_eq!(snowflake_jwt_account("xy12345"), "XY12345");
        // Region is dropped (Snowflake JWT wants the locator only).
        assert_eq!(snowflake_jwt_account("xy12345.us-east-1"), "XY12345");
        // Cloud platform suffix is dropped too.
        assert_eq!(snowflake_jwt_account("xy12345.us-east-1.aws"), "XY12345");
        // PrivateLink (GitHub #22): everything after the first '.' is dropped.
        assert_eq!(snowflake_jwt_account("xy12345.us-east-1.privatelink"), "XY12345");
        // org-account form (no dot) is kept whole.
        assert_eq!(snowflake_jwt_account("myorg-acct1"), "MYORG-ACCT1");
        // `.global` replication accounts strip at the first '-' instead.
        assert_eq!(snowflake_jwt_account("xy12345-9999.global"), "XY12345");
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

/// Map a DuckDB column type to a Snowflake type for auto-creating a sink
/// table. Uses Snowflake type SYNONYMS (BIGINT/DECIMAL/DOUBLE/VARCHAR/...)
/// that Snowflake accepts as aliases for NUMBER/FLOAT/etc. - these are also
/// valid DuckDB type names, so the same DDL works against real Snowflake and
/// the DuckDB-backed local emulator used for tests.
fn duckdb_type_to_snowflake(t: &str) -> String {
    let up = t.trim().to_ascii_uppercase();
    if up.starts_with("DECIMAL") || up.starts_with("NUMERIC") {
        // Snowflake accepts DECIMAL(p,s) as a synonym for NUMBER(p,s).
        return up.replacen("NUMERIC", "DECIMAL", 1);
    }
    match up.as_str() {
        "BOOLEAN" | "BOOL" => "BOOLEAN",
        "TINYINT" | "UTINYINT" | "SMALLINT" | "USMALLINT" | "INT2" | "INTEGER" | "INT"
        | "INT4" | "UINTEGER" | "BIGINT" | "INT8" | "UBIGINT" | "HUGEINT" | "UHUGEINT" => {
            "BIGINT"
        }
        "REAL" | "FLOAT" | "FLOAT4" | "DOUBLE" | "FLOAT8" => "DOUBLE",
        "DATE" => "DATE",
        "TIME" => "TIME",
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" => "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "TIMESTAMP_TZ",
        _ => "VARCHAR",
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
/// Build the native upsert statements to run on the target DB. Returns one
/// or more statements: Postgres tolerates a single multi-statement string
/// (run via postgres_execute), but MySQL's extension can't (it raises
/// "Commands out of sync"), so the MySQL path returns each statement
/// separately and the caller issues them one at a time.
fn build_native_upsert_sql(
    spec: &plan::UpsertSpec,
    set_cols: &[&String],
    data_cols: &[&String],
    target_native: &str,
    staging_native: &str,
) -> Vec<String> {
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
            match spec.delete_column.as_deref() {
                // No delete propagation: insert every staged row verbatim.
                None => vec![format!(
                    "INSERT INTO {target} SELECT * FROM {staging} {conflict}; DROP TABLE {staging};",
                    target = target_native,
                    staging = staging_native,
                    conflict = conflict
                )],
                // Delete propagation: the flag column is staged but is not a
                // target column, so the INSERT lists explicit data columns and
                // skips flagged rows; a prior DELETE removes flagged keys.
                Some(dc) => {
                    let flag = format!("\"{}\"", dc.replace('"', "\"\""));
                    let v = spec.delete_value.replace('\'', "''");
                    let col_list = data_cols
                        .iter()
                        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                        .collect::<Vec<_>>()
                        .join(", ");
                    vec![format!(
                        "DELETE FROM {target} WHERE ({keys}) IN (SELECT {keys} FROM {staging} WHERE {flag} = '{v}'); \
                         INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging} WHERE ({flag} IS NULL OR {flag} <> '{v}') {conflict}; \
                         DROP TABLE {staging};",
                        target = target_native,
                        staging = staging_native,
                        keys = key_list,
                        flag = flag,
                        v = v,
                        cols = col_list,
                        conflict = conflict
                    )]
                }
            }
        }
        plan::UpsertFamily::MySql => {
            // MySQL relies on the target's existing UNIQUE/PRIMARY KEY.
            // INSERT IGNORE is the fallback when there are no non-key
            // columns to update. The target is re-quoted with backticks
            // from the raw name (DuckDB's `target` uses double quotes, which
            // MySQL rejects unless ANSI_QUOTES is set).
            let target_native = match &spec.raw_schema {
                Some(s) => format!("`{}`.`{}`", s.replace('`', "``"), spec.raw_table.replace('`', "``")),
                None => format!("`{}`", spec.raw_table.replace('`', "``")),
            };
            let target_native = target_native.as_str();
            let on_dup = if set_cols.is_empty() {
                None
            } else {
                Some(
                    set_cols
                        .iter()
                        .map(|c| format!("`{c}` = VALUES(`{c}`)"))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            };
            match spec.delete_column.as_deref() {
                None => {
                    let insert = if let Some(set_clause) = on_dup {
                        format!(
                            "INSERT INTO {target} SELECT * FROM {staging} ON DUPLICATE KEY UPDATE {set}",
                            target = target_native,
                            staging = staging_native,
                            set = set_clause
                        )
                    } else {
                        format!(
                            "INSERT IGNORE INTO {target} SELECT * FROM {staging}",
                            target = target_native,
                            staging = staging_native
                        )
                    };
                    vec![insert, format!("DROP TABLE {}", staging_native)]
                }
                Some(dc) => {
                    let flag = format!("`{}`", dc.replace('`', "``"));
                    let v = spec.delete_value.replace('\'', "''");
                    let key_list = spec
                        .conflict_cols
                        .iter()
                        .map(|c| format!("`{}`", c.replace('`', "``")))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let col_list = data_cols
                        .iter()
                        .map(|c| format!("`{}`", c.replace('`', "``")))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let insert_head = match &on_dup {
                        Some(_) => format!("INSERT INTO {}", target_native),
                        None => format!("INSERT IGNORE INTO {}", target_native),
                    };
                    let on_dup_tail = match on_dup {
                        Some(set_clause) => format!(" ON DUPLICATE KEY UPDATE {}", set_clause),
                        None => String::new(),
                    };
                    vec![
                        format!(
                            "DELETE FROM {target} WHERE ({keys}) IN (SELECT {keys} FROM {staging} WHERE {flag} = '{v}')",
                            target = target_native,
                            staging = staging_native,
                            keys = key_list,
                            flag = flag,
                            v = v
                        ),
                        format!(
                            "{head} ({cols}) SELECT {cols} FROM {staging} WHERE ({flag} IS NULL OR {flag} <> '{v}'){tail}",
                            staging = staging_native,
                            flag = flag,
                            v = v,
                            cols = col_list,
                            head = insert_head,
                            tail = on_dup_tail
                        ),
                        format!("DROP TABLE {}", staging_native),
                    ]
                }
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
    /// A diagnostic line from a ctl.log / ctl.warn node (and, on failure,
    /// the ctl.die message). `level` is "info" / "warn" / "error".
    Log {
        node_id: String,
        level: String,
        message: String,
    },
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
    /// Coarse bucket of `error` (see error_category) - present only on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
}

impl RunResult {
    fn failed(start: Instant, error: String) -> Self {
        let category = error_category::categorize_error(&error);
        RunResult {
            status: "error".into(),
            duration_ms: start.elapsed().as_millis() as u64,
            nodes: Default::default(),
            preview: Vec::new(),
            error: Some(error),
            category: Some(category.into()),
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
    /// Coarse error bucket (auth/network/timeout/oom/disk/schema/syntax/
    /// cancelled/other) - present only when `error` is. See error_category.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
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
            // Control-flow stages (ctl.runjob/iterate/foreach/parallelize/try)
            // carry a non-empty PASS-THROUGH view so downstream wiring works,
            // which would otherwise hide their orchestration side effect from
            // the export. Prepend the procedural note to (not replace) their SQL
            // so the export documents which sub-pipeline runs / fans out (#7).
            let sql = if s.sql.trim().is_empty() {
                procedural_note(&s)
            } else if matches!(
                s.runtime.as_ref(),
                Some(
                    RuntimeSpec::RunJob { .. }
                        | RuntimeSpec::Iterate { .. }
                        | RuntimeSpec::Foreach { .. }
                        | RuntimeSpec::Parallelize(_)
                        | RuntimeSpec::InstallFallback(_)
                )
            ) {
                format!("{}\n{}", procedural_note(&s), redact_secret_values(&s.sql, &secrets))
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

#[cfg(test)]
mod tests {
    use super::{
        aws_sigv4_sign, chunk_text, cosine_similarity, glob_match, mssql_numeric_to_string,
        parse_link_next, pii_patterns, read_marker, render_prompt_template, secret_placeholder,
        unwrap_dynamodb_attrs, DuckdbEngine, MarkerState,
    };
    use std::path::PathBuf;

    #[test]
    fn for_new_run_isolates_cancel_but_clones_share_within_a_run() {
        let base = DuckdbEngine::new(PathBuf::from("duckdb"));
        // A fresh run scope: cancelling it must NOT affect a separate run.
        let run_a = base.for_new_run();
        let run_b = base.for_new_run();
        run_a.request_cancel();
        assert!(run_a.check_cancelled().is_err(), "run A should be cancelled");
        assert!(run_b.check_cancelled().is_ok(), "run B must be independent of run A");
        assert!(base.check_cancelled().is_ok(), "the base engine is unaffected");
        // A plain clone (how sub-pipelines / parallelize branches get an engine)
        // shares the SAME run's flag, so a cancel propagates to children.
        let child = run_b.clone();
        run_b.request_cancel();
        assert!(child.check_cancelled().is_err(), "a clone shares the run's flag");
    }

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
