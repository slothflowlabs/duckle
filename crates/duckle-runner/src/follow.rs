//! `duckle-runner follow` - run one pipeline continuously instead of once.
//!
//! A scheduled pipeline already streams in the sense that matters: a source
//! that tracks its position (src.kafka's `trackOffset`, xf.incremental's
//! watermark) resumes where the last successful run stopped, so consecutive
//! runs consume a topic without gaps or replays. What a schedule cannot give
//! is latency: the scheduler wakes every 15 seconds, and each run pays process
//! start, DuckDB resolution and document parsing again.
//!
//! Follow keeps that same execution model and removes the per-batch overhead.
//! The document is read, resolved and validated once; the engine is built
//! once; then the pipeline is executed in a loop. Each pass is one micro-batch.
//!
//! ## Why the commit ordering is correct here for free
//!
//! Position state (Kafka resume offsets, incremental watermarks) is not written
//! when the source reads it. It is queued and flushed only if the run reaches
//! `status == "ok"`, which is after every sink has written. So a batch that
//! fails anywhere - transform, quality gate, sink - leaves the saved offset
//! where it was, and the next pass re-reads exactly the records that did not
//! land.
//!
//! That is the property this mode is built on, and it is worth stating because
//! the obvious alternative is what most streaming tools do: commit the source
//! position on read, then process. That loses a batch on any downstream
//! failure. Here it cannot happen, in either direction - `--on-error continue`
//! keeps the loop alive but still does not advance the position past a batch
//! that failed.
//!
//! ## What it deliberately does not do
//!
//! No windowing, no cross-batch state beyond what the pipeline itself persists,
//! and no interruption of a batch in flight. Ctrl-C sets a flag that is checked
//! BETWEEN passes, so a shutdown never lands in the middle of a sink write with
//! the position half-advanced.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};

/// What a batch that fails should do to the loop.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OnError {
    /// Report and exit non-zero. The default: an unattended follower that
    /// keeps failing silently is worse than one that stops.
    Stop,
    /// Report and keep going. The saved position does not advance for the
    /// failed batch either way, so the next pass retries the same records -
    /// which is what makes this safe for a transient sink outage.
    Continue,
}

pub struct FollowOptions {
    pub pipeline: PathBuf,
    pub workspace: Option<PathBuf>,
    pub duckdb: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
    pub name: Option<String>,
    /// How long to wait after a pass that read nothing. A busy topic never
    /// waits; a quiet one does not spin.
    pub idle_ms: u64,
    /// Stop after this many passes. Bounds a test or a one-shot drain.
    pub max_batches: Option<u64>,
    pub on_error: OnError,
}

impl Default for FollowOptions {
    fn default() -> Self {
        Self {
            pipeline: PathBuf::new(),
            workspace: None,
            duckdb: None,
            log_dir: None,
            name: None,
            idle_ms: 1000,
            max_batches: None,
            on_error: OnError::Stop,
        }
    }
}

/// One pass's outcome, kept separate from the printing so the loop can be
/// tested without reading stdout.
pub struct BatchOutcome {
    pub ok: bool,
    pub rows: u64,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Rows this pass actually WROTE, which is the signal for whether it did
/// anything worth doing again immediately.
///
/// Summing the whole graph looks equivalent and is not. A file source re-reads
/// the same rows every pass, so with an `xf.incremental` downstream filtering
/// all of them out, the graph total stays non-zero forever and the follower
/// spins at full speed on a pipeline that is producing nothing. Counting what
/// the sinks wrote gets that right, and gets Kafka right too - an empty topic
/// yields no rows anywhere.
///
/// A graph with no sink at all (preview or a self-contained SQL stage) has
/// nothing to count, so it falls back to the total rather than declaring
/// itself permanently idle.
pub fn rows_of(result: &duckle_duckdb_engine::RunResult) -> u64 {
    let mut sink_rows = 0u64;
    let mut saw_sink = false;
    let mut total = 0u64;
    for n in result.nodes.values() {
        let rows = n.rows.unwrap_or(0);
        total += rows;
        if n.kind.as_deref() == Some("sink") {
            saw_sink = true;
            sink_rows += rows;
        }
    }
    if saw_sink {
        sink_rows
    } else {
        total
    }
}

/// Decide what the loop does next. Split out from `run` so the policy is
/// testable without a Kafka broker: given an outcome and the options, should
/// the loop continue, and should it wait first?
///
/// Returns `(keep_going, wait)`.
pub fn next_step(outcome: &BatchOutcome, opts: &FollowOptions, passes: u64) -> (bool, Duration) {
    if !outcome.ok && opts.on_error == OnError::Stop {
        return (false, Duration::ZERO);
    }
    if let Some(max) = opts.max_batches {
        if passes >= max {
            return (false, Duration::ZERO);
        }
    }
    // A failed pass waits too. Retrying a dead sink as fast as the CPU allows
    // just turns one outage into a hot loop against it.
    let idle = outcome.rows == 0 || !outcome.ok;
    (
        true,
        if idle { Duration::from_millis(opts.idle_ms) } else { Duration::ZERO },
    )
}

/// Read the document and apply every resolution pass that does not change
/// between batches: saved connection refs, `${ENV:...}`, and workspace context.
///
/// Time builtins are deliberately NOT applied here. `${date}` must be stamped
/// per batch, or a follower started before midnight would keep writing
/// yesterday's partition until it was restarted.
fn load_base_doc(pipeline: &Path, workspace: &Path) -> Result<PipelineDoc, String> {
    let text = std::fs::read_to_string(pipeline)
        .map_err(|e| format!("read {}: {}", pipeline.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text)
        .map_err(|e| format!("parse {}: {}", pipeline.display(), e))?;
    duckle_secrets::resolve_connection_refs(workspace, &mut doc.nodes)?;
    let env_file = workspace.join("secrets.env");
    crate::apply_env_pass(&mut doc, workspace, &env_file)?;
    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, workspace);
    Ok(doc)
}

/// Run the pipeline until stopped. Returns the number of passes completed.
pub fn run(opts: FollowOptions) -> Result<u64, String> {
    if !opts.pipeline.exists() {
        return Err(format!("pipeline file not found: {}", opts.pipeline.display()));
    }
    let workspace = opts
        .workspace
        .clone()
        .or_else(|| opts.pipeline.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));

    // Everything below happens ONCE. That is the whole point of the mode: a
    // scheduled run pays all of it per batch.
    let base_doc = load_base_doc(&opts.pipeline, &workspace)?;
    let log_dir = opts.log_dir.clone().unwrap_or_else(|| workspace.join("logs"));
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", &log_dir);

    let duckdb = crate::resolve_duckdb(opts.duckdb.clone())?;
    let name = opts.name.clone().unwrap_or_else(|| {
        opts.pipeline
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "pipeline".into())
    });
    let engine = DuckdbEngine::new(duckdb).without_previews();

    // Ctrl-C is checked between passes, never during one. Stopping mid-batch
    // is what would leave a sink half-written with the position already moved.
    let stop = Arc::new(AtomicBool::new(false));
    install_stop_handler(stop.clone());

    eprintln!(
        "duckle-runner follow: {} (workspace {}), idle {}ms, on error: {}",
        opts.pipeline.display(),
        workspace.display(),
        opts.idle_ms,
        match opts.on_error {
            OnError::Stop => "stop",
            OnError::Continue => "continue",
        }
    );
    eprintln!("  Ctrl-C stops after the batch in flight finishes.");

    let started = Instant::now();
    let mut passes = 0u64;
    let mut total_rows = 0u64;
    let mut failures = 0u64;

    while !stop.load(Ordering::Relaxed) {
        // A fresh stamp per batch, from the pristine document.
        let mut doc = base_doc.clone();
        duckle_duckdb_engine::context::apply_time_builtins(&mut doc);

        let result = engine.execute_pipeline_named(&doc, &name);
        passes += 1;
        let outcome = BatchOutcome {
            ok: result.status == "ok",
            rows: rows_of(&result),
            duration_ms: result.duration_ms,
            error: result.error.clone(),
        };
        total_rows += outcome.rows;
        if !outcome.ok {
            failures += 1;
        }
        report(passes, &outcome);

        let (keep_going, wait) = next_step(&outcome, &opts, passes);
        if !keep_going {
            if !outcome.ok && opts.on_error == OnError::Stop {
                eprintln!(
                    "follow: stopping on a failed batch. The saved position did not advance, \
                     so restarting re-reads the same records."
                );
                return Err(outcome.error.unwrap_or_else(|| "batch failed".into()));
            }
            break;
        }
        // Sleep in slices so Ctrl-C during a quiet spell is still responsive.
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50).min(deadline - Instant::now()));
        }
    }

    eprintln!(
        "follow: {} batch(es), {} row(s), {} failed, {:.1}s elapsed",
        passes,
        total_rows,
        failures,
        started.elapsed().as_secs_f64()
    );
    Ok(passes)
}

/// One line per batch, on stdout so it can be piped, with the fields a person
/// watching a stream actually wants: which pass, how many rows, how long.
fn report(pass: u64, o: &BatchOutcome) {
    if o.ok {
        // A pass that read nothing is the normal quiet case and would drown
        // the interesting lines, so it is not printed.
        if o.rows > 0 {
            println!("batch {:<6} {:>8} rows  {:>6} ms", pass, o.rows, o.duration_ms);
            let _ = std::io::stdout().flush();
        }
    } else {
        println!(
            "batch {:<6} FAILED       {:>6} ms  {}",
            pass,
            o.duration_ms,
            o.error.as_deref().unwrap_or("(no message)")
        );
        let _ = std::io::stdout().flush();
    }
}

/// Ctrl-C sets a flag the loop checks BETWEEN passes.
///
/// Worth being clear about what this is and is not for. It is NOT what makes
/// shutdown safe: a follower killed outright mid-batch is already safe,
/// because the saved position only advances when a run reaches "ok", so the
/// records in flight are simply re-read next start. What it buys is finishing
/// the batch in hand rather than leaving a half-written sink file behind.
fn install_stop_handler(stop: Arc<AtomicBool>) {
    if let Err(e) = ctrlc::set_handler(move || stop.store(true, Ordering::Relaxed)) {
        // Not fatal. Without it Ctrl-C kills the process, which costs an
        // unfinished batch and no correctness.
        eprintln!(
            "follow: could not install a Ctrl-C handler ({e}); Ctrl-C will stop the process immediately"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(ok: bool, rows: u64) -> BatchOutcome {
        BatchOutcome { ok, rows, duration_ms: 1, error: if ok { None } else { Some("boom".into()) } }
    }

    #[test]
    fn a_pass_that_read_rows_asks_again_immediately() {
        let opts = FollowOptions { idle_ms: 500, ..Default::default() };
        let (go, wait) = next_step(&outcome(true, 100), &opts, 1);
        assert!(go);
        assert_eq!(wait, Duration::ZERO, "a busy topic must not be made to wait");
    }

    #[test]
    fn a_pass_that_read_nothing_backs_off() {
        let opts = FollowOptions { idle_ms: 500, ..Default::default() };
        let (go, wait) = next_step(&outcome(true, 0), &opts, 1);
        assert!(go);
        assert_eq!(wait, Duration::from_millis(500), "a quiet topic must not be spun on");
    }

    #[test]
    fn a_failed_batch_stops_by_default() {
        let opts = FollowOptions::default();
        let (go, _) = next_step(&outcome(false, 0), &opts, 1);
        assert!(!go, "an unattended follower that fails silently is worse than one that stops");
    }

    #[test]
    fn on_error_continue_keeps_going_but_still_waits() {
        let opts = FollowOptions { on_error: OnError::Continue, idle_ms: 250, ..Default::default() };
        let (go, wait) = next_step(&outcome(false, 0), &opts, 1);
        assert!(go);
        assert_eq!(
            wait,
            Duration::from_millis(250),
            "retrying a dead sink as fast as the CPU allows turns an outage into a hot loop"
        );
    }

    #[test]
    fn a_failed_batch_that_read_rows_still_waits_under_continue() {
        // Rows were read but the run failed, so the position did not advance.
        // Asking again instantly would re-read the same rows at full speed.
        let opts = FollowOptions { on_error: OnError::Continue, idle_ms: 300, ..Default::default() };
        let (go, wait) = next_step(&outcome(false, 5_000), &opts, 1);
        assert!(go);
        assert_eq!(wait, Duration::from_millis(300));
    }

    #[test]
    fn max_batches_bounds_the_loop() {
        let opts = FollowOptions { max_batches: Some(3), ..Default::default() };
        assert!(next_step(&outcome(true, 10), &opts, 2).0, "under the cap, keep going");
        assert!(!next_step(&outcome(true, 10), &opts, 3).0, "at the cap, stop");
        assert!(!next_step(&outcome(true, 10), &opts, 4).0, "past the cap, stop");
    }

    #[test]
    fn idleness_is_judged_by_what_the_sinks_wrote_not_what_the_source_read() {
        use duckle_duckdb_engine::{NodeRunStatus, RunResult};
        let mut nodes = std::collections::BTreeMap::new();
        let mk = |rows: Option<u64>, kind: &str| NodeRunStatus {
            status: "ok".into(),
                    note: None,
            kind: Some(kind.to_string()),
            rows,
            duration_ms: None,
            error: None,
            category: None,
            sql: None,
        };
        nodes.insert("src".to_string(), mk(Some(12), "view"));
        nodes.insert("xf".to_string(), mk(Some(0), "view"));
        nodes.insert("snk".to_string(), mk(Some(0), "sink"));
        let r = RunResult {
            status: "ok".into(),
            unchanged: false,
            artifacts: Vec::new(),
            artifacts_truncated: false,
            duration_ms: 1,
            nodes,
            preview: vec![],
            error: None,
            category: None,
        };
        assert_eq!(
            rows_of(&r),
            0,
            "a source that re-read 12 rows an incremental filter then dropped is an IDLE pass - counting the graph total would spin the follower at full speed"
        );
    }
}
