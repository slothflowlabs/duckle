//! Duckle scheduler.
//!
//! Cron- and interval-based triggers for pipelines. Schedules are
//! persisted to `<workspace>/schedules.json` so they survive restarts.
//! A single tokio task wakes every 15 seconds, decides which schedules
//! are due, and fires each as a non-blocking spawn that calls into the
//! shared `DuckdbEngine`.

use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use duckle_duckdb_engine::{
    append_run_record, plans, runlock, schedules, DuckdbEngine, RunRecord, RunResult,
};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, Debouncer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tracing::warn;

/// Default poll cadence for checking due schedules. Overridable via the
/// DUCKLE_TICK_INTERVAL env var (whole seconds, must be > 0) so sub-15s
/// real-time schedules can fire closer to their configured rate (issue #135).
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(15);
const WATCH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Resolve the scheduler poll cadence: DUCKLE_TICK_INTERVAL (whole seconds)
/// if set and greater than 0, otherwise the 15s default.
fn tick_interval() -> Duration {
    std::env::var("DUCKLE_TICK_INTERVAL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TICK_INTERVAL)
}

/// The schedule record and its trigger kinds live in the engine crate, because
/// `duckle-runner serve` writes the same store and a second definition of the
/// same file format is a drift waiting to happen. Re-exported so callers keep
/// naming them here.
pub use duckle_duckdb_engine::schedules::{Schedule, ScheduleKind};

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerInner>>,
    engine: DuckdbEngine,
    fire_tx: UnboundedSender<String>,
}

struct SchedulerInner {
    schedules: Vec<Schedule>,
    workspace_path: Option<PathBuf>,
    /// Why the store could not be read, when it could not be.
    ///
    /// Kept so the answer to "what are my schedules?" can be the truth rather
    /// than an empty list. See [`Scheduler::list`].
    load_error: Option<String>,
    /// Active file-watchers, keyed by schedule id. Holding the
    /// `Debouncer` keeps the watch alive; dropping it stops watching.
    watchers: HashMap<String, Debouncer<RecommendedWatcher>>,
    /// Receiver for file-watch fires; taken by `spawn_ticker`.
    fire_rx: Option<UnboundedReceiver<String>>,
}

/// What a schedule locks when it fires, if any one thing.
///
/// The pipeline, not the schedule record. The pipeline owns the sink and the
/// `xf.incremental` watermark, so it is the thing that must not run twice: two
/// schedules pointed at one pipeline and coinciding at midnight collide every
/// bit as much as two processes do. It also has to be the pipeline for the
/// lock to work across products, because the web console identifies a schedule
/// by its pipeline while this crate mints a uuid, so a record-keyed lock would
/// have the two naming different files and guarding nothing.
///
/// A schedule that fires a plan locks nothing here, and each pipeline in the plan takes its
/// own lock as it comes up instead. Locking `pipeline_id` for a plan would be worse than
/// useless: that field is a label on a plan schedule, so the lock would guard a file the
/// plan never opens and leave every file it does open unguarded.
fn lock_key(s: &Schedule) -> Option<&str> {
    match s.plan_id {
        Some(_) => None,
        None => Some(&s.pipeline_id),
    }
}

/// What a schedule actually runs, as a plan.
///
/// A schedule naming one pipeline is a plan of one step with one pipeline in it. Saying so
/// here rather than branching at the fire site means there is one execution path instead of
/// two, and the two cannot drift the way the console's and this one's already did.
fn work_of(workspace: &Path, s: &Schedule) -> Result<plans::Plan, String> {
    let Some(plan_id) = s.plan_id.as_deref() else {
        return Ok(plans::Plan {
            id: s.id.clone(),
            name: s.name.clone(),
            stop_on_failure: true,
            steps: vec![plans::Step {
                name: s.name.clone(),
                pipelines: vec![s.pipeline_id.clone()],
                // A single-pipeline schedule has nothing after it to carry on to.
                continue_on_failure: None,
            }],
        });
    };
    plans::load(workspace)?
        .into_iter()
        .find(|p| p.id == plan_id)
        // Named, not silent. A schedule pointing at a plan somebody deleted is the same
        // failure as one pointing at a deleted pipeline, and gets the same treatment: it
        // reports a failed run rather than doing nothing every night.
        .ok_or_else(|| format!("This schedule runs the plan '{plan_id}', which no longer exists"))
}

/// The answer to "may this process run that schedule right now?".
enum Claim {
    /// Yes. Dropping the payload gives the claim back.
    Ours(Option<runlock::RunLock>),
    /// No - another Duckle process is already running it. The next tick will
    /// come round and may well succeed.
    Taken,
    /// No, and waiting will not help: this workspace cannot be locked at all.
    /// Kept apart from `Taken` because the two call for opposite responses, and
    /// blaming an imaginary other process sends somebody hunting for it.
    Unusable(String),
}

/// Ask for the exclusive right to run `pipeline_id`.
///
/// Both of this crate's fire paths and the runner's own scheduler go through a
/// lock like this, because the in-process guards each of them keeps - a
/// semaphore here, a last-fired map there - say nothing about the other
/// process. Two schedulers on one workspace is not a misconfiguration: it is
/// what a workspace looks like mid-way through moving from a laptop to a
/// server. Firing twice means two runs into the same sink and two runs
/// advancing the same `xf.incremental` watermark, and the second is how a load
/// silently skips rows.
///
/// See [`lock_key`] for why the key is the pipeline rather than the schedule
/// record that fired.
///
/// A scheduler with no workspace is handed an unheld claim rather than a
/// refusal: there is nothing on disk for two processes to race over, and
/// `run_now` declines such a run on its own terms with a clearer message than
/// a lock could give.
fn claim_run(workspace: Option<&Path>, pipeline_id: &str) -> Claim {
    match workspace {
        None => Claim::Ours(None),
        Some(ws) => match runlock::try_acquire_reason(ws, pipeline_id) {
            runlock::AcquireOutcome::Claimed(lock) => Claim::Ours(Some(lock)),
            runlock::AcquireOutcome::HeldByOther => Claim::Taken,
            runlock::AcquireOutcome::Unusable(e) => Claim::Unusable(e.to_string()),
        },
    }
}

/// Resolve and run one pipeline the way a schedule does, and block until it is done.
///
/// The same preparation `run_now` does for a single pipeline: workspace context, the
/// date/time builtins, saved connection refs, then the environment. A plan runs its
/// pipelines through this so a pipeline behaves identically whether a plan ran it or a
/// schedule of its own did.
fn run_one_blocking(
    engine: &DuckdbEngine,
    workspace: &Path,
    pipeline_id: &str,
) -> Result<RunResult, String> {
    // Normalised because a plan step may name a pipeline the console's way, as a
    // workspace-relative file. `resolve_workspace` takes a bare id and builds the path
    // itself, so an un-normalised step asked it for `pipelines/pipelines/orders.json.json`.
    // A bare id normalises to itself, so an ordinary schedule is unaffected.
    let mut pipeline = duckle_duckdb_engine::context::resolve_workspace(
        workspace,
        plans::step_pipeline_id(pipeline_id),
        None,
    )?
    .doc;
    duckle_duckdb_engine::context::apply_time_builtins(&mut pipeline);
    duckle_secrets::resolve_connection_refs(workspace, &mut pipeline.nodes)?;
    duckle_duckdb_engine::context::apply_env(&mut pipeline);
    duckle_duckdb_engine::context::apply_vault(&mut pipeline);
    // A fresh cancel scope per pipeline, so one step of a plan cannot cancel the next.
    Ok(run_recorded(
        engine,
        workspace,
        &pipeline,
        pipeline_id,
        "plan",
        &workspace.join("pipelines").join(format!("{pipeline_id}.json")).display().to_string(),
    ))
}

/// #259: run a pipeline and record its identity, before and after.
///
/// Both scheduler paths go through here, so a scheduled run and a plan step are
/// addressable the same way as a `duckle-runner` run. Before this, neither
/// recorded a run id at all - `execute_one` hard-codes `None` - so "which run
/// was that?" had no answer for anything the scheduler started.
fn run_recorded(
    engine: &DuckdbEngine,
    workspace: &Path,
    pipeline: &duckle_duckdb_engine::PipelineDoc,
    pipeline_id: &str,
    trigger: &str,
    pipeline_path: &str,
) -> RunResult {
    let hash = duckle_duckdb_engine::retry::pipeline_hash(pipeline);
    let run_id = duckle_duckdb_engine::retry::new_run_id(pipeline_id, trigger);
    let receipt = duckle_duckdb_engine::retry::begin(
        workspace,
        &run_id,
        trigger,
        pipeline_id,
        pipeline_path,
        &hash,
        None,
    );
    let result = engine.for_new_run().execute_pipeline_named(pipeline, pipeline_id);
    duckle_duckdb_engine::retry::finish(
        workspace,
        receipt,
        &result.status,
        result
            .nodes
            .iter()
            .map(|(id, st)| {
                (
                    id.clone(),
                    duckle_duckdb_engine::retry::ReceiptNode {
                        status: st.status.clone(),
                        kind: st.kind.clone(),
                        output_cache_key: result.cache_keys.get(id).cloned(),
                    },
                )
            })
            .collect(),
    );
    result
}

/// A run that never started, as a result.
///
/// A pipeline whose file has been renamed fails before the engine sees it, and that is
/// still a failed run: it took time and it did not work. Shaping it like one keeps it out
/// of the gap where a broken schedule reads as a schedule that never fired.
fn failed_run(started: DateTime<Utc>, error: &str) -> RunResult {
    RunResult {
        cache_keys: Default::default(),
        status: "error".into(),
        duration_ms: Utc::now().signed_duration_since(started).num_milliseconds().max(0) as u64,
        nodes: Default::default(),
        preview: Vec::new(),
        category: Some(duckle_duckdb_engine::error_category::categorize_error(error).to_string()),
        error: Some(error.to_string()),
    unchanged: false,
    incomplete: false,
    incomplete_reason: None,
    artifacts: Vec::new(),
    artifacts_truncated: false,
    }
}

impl Scheduler {
    pub fn new(engine: DuckdbEngine) -> Self {
        let (fire_tx, fire_rx) = unbounded_channel();
        Self {
            inner: Arc::new(Mutex::new(SchedulerInner {
                schedules: Vec::new(),
                workspace_path: None,
                load_error: None,
                watchers: HashMap::new(),
                fire_rx: Some(fire_rx),
            })),
            engine,
            fire_tx,
        }
    }

    /// Switch to a different workspace path. Loads schedules from the
    /// new path; computes next-run times for each; rebuilds watchers.
    pub fn set_workspace(&self, path: Option<PathBuf>) {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        g.workspace_path = path;
        self.reload(&mut g);
        self.rebuild_watchers(&mut g);
    }

    /// Re-read the store, and remember it if it will not be read.
    fn reload(&self, inner: &mut SchedulerInner) {
        let Some(path) = inner.workspace_path.clone() else {
            inner.schedules = Vec::new();
            inner.load_error = None;
            return;
        };
        match schedules::load(&path) {
            Ok(mut list) => {
                for s in list.iter_mut() {
                    compute_next_run(s);
                }
                inner.schedules = list;
                inner.load_error = None;
            }
            Err(e) => {
                warn!("Failed to load schedules: {}", e);
                // Emptied rather than left as it was. This may be a different
                // workspace than the one those schedules came from, and a
                // ticker firing another workspace's schedules would be far
                // worse than firing none. Nothing is lost by it: the file is
                // never written back over, because every write re-reads under
                // a lock and fails the same way.
                inner.schedules = Vec::new();
                inner.load_error = Some(e);
            }
        }
    }

    /// Recreate file-watchers for the current schedule set. Drops all
    /// existing watchers and rebuilds from enabled FileWatch
    /// schedules.
    fn rebuild_watchers(&self, inner: &mut SchedulerInner) {
        inner.watchers.clear();
        let specs: Vec<(String, String, bool)> = inner
            .schedules
            .iter()
            .filter(|s| s.enabled)
            .filter_map(|s| match &s.kind {
                ScheduleKind::FileWatch { path, recursive } => {
                    Some((s.id.clone(), path.clone(), *recursive))
                }
                _ => None,
            })
            .collect();
        for (id, path, recursive) in specs {
            match self.make_watcher(&id, &path, recursive) {
                Ok(w) => {
                    inner.watchers.insert(id, w);
                }
                Err(e) => warn!("File-watch setup failed for {}: {}", id, e),
            }
        }
    }

    fn make_watcher(
        &self,
        schedule_id: &str,
        path: &str,
        recursive: bool,
    ) -> notify::Result<Debouncer<RecommendedWatcher>> {
        let tx = self.fire_tx.clone();
        let sid = schedule_id.to_string();
        let mut debouncer = new_debouncer(WATCH_DEBOUNCE, move |res: DebounceEventResult| {
            if let Ok(events) = res {
                if !events.is_empty() {
                    let _ = tx.send(sid.clone());
                }
            }
        })?;
        let mode = if recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        debouncer.watcher().watch(Path::new(path), mode)?;
        Ok(debouncer)
    }

    /// The schedules, or why the store could not be read.
    ///
    /// A store that will not parse used to come back as an empty list, which
    /// reads as "you have no schedules" - the most alarming way possible to
    /// say "I could not open the file", and one that invites re-creating
    /// schedules that are still sitting on disk.
    ///
    /// While in the failed state this retries the read, so repairing the file
    /// is enough to recover without restarting the app. A healthy store is
    /// served from memory and costs nothing.
    pub fn list(&self) -> Result<Vec<Schedule>, String> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        if g.load_error.is_some() {
            self.reload(&mut g);
        }
        match &g.load_error {
            Some(e) => Err(e.clone()),
            None => Ok(g.schedules.clone()),
        }
    }

    pub fn upsert(&self, mut schedule: Schedule) -> Result<Schedule, String> {
        validate_schedule(&schedule)?;
        match &schedule.kind {
            ScheduleKind::Cron { .. } => {}
            ScheduleKind::Interval { seconds } => {
                if *seconds < 1 {
                    return Err("Interval must be at least 1 second".into());
                }
            }
            ScheduleKind::FileWatch { path, .. } => {
                if path.trim().is_empty() {
                    return Err("Watch path is required".into());
                }
            }
        }
        if schedule.id.is_empty() {
            schedule.id = uuid::Uuid::new_v4().to_string();
        }
        compute_next_run(&mut schedule);
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let saved = schedule.clone();
        self.commit(&mut g, move |list| {
            match list.iter().position(|s| s.id == saved.id) {
                Some(idx) => {
                    // Upsert carries config only; preserve the existing
                    // run-history fields so a partial payload doesn't wipe
                    // last_run_* to null.
                    let prev = &list[idx];
                    let mut next = saved;
                    next.last_run_at = prev.last_run_at;
                    next.last_run_status = prev.last_run_status.clone();
                    next.last_run_duration_ms = prev.last_run_duration_ms;
                    next.last_run_error = prev.last_run_error.clone();
                    // The plan a schedule runs is kept for the same reason, and it matters
                    // more: an editor with no plan field sends "no plan" for a schedule that
                    // has one, and believing it would leave the schedule pointed at the
                    // label in its pipeline_id, which is not a file. Clearing a plan is done
                    // by naming a pipeline instead, not by saying nothing.
                    next.plan_id = next.plan_id.or_else(|| prev.plan_id.clone());
                    list[idx] = next;
                }
                None => list.push(saved),
            }
        })?;
        self.rebuild_watchers(&mut g);
        Ok(schedule)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        g.watchers.remove(id);
        let id = id.to_string();
        self.commit(&mut g, move |list| list.retain(|s| s.id != id))
    }

    /// Apply a change to the shared store and adopt the result.
    ///
    /// The change runs against the list as it is on disk, not the copy this
    /// process is holding, because `duckle-runner serve` may be editing the
    /// same file. Whatever comes back becomes the in-memory state, so a
    /// schedule added by the other process shows up here rather than being
    /// overwritten on the next save.
    ///
    /// A scheduler with no workspace keeps its list in memory only; that is
    /// the pre-workspace state at startup, not an error worth surfacing.
    ///
    /// The write failing IS worth surfacing, and the caller decides how. This
    /// used to log and return nothing, so `upsert` and `delete` reported
    /// success to the UI for a schedule that never reached the disk: a full
    /// disk, a read-only workspace or a store that will not parse all looked
    /// like a save that worked, and the schedule was gone at the next restart.
    fn commit<F>(&self, inner: &mut SchedulerInner, change: F) -> Result<(), String>
    where
        F: FnOnce(&mut Vec<Schedule>),
    {
        let Some(path) = inner.workspace_path.clone() else {
            change(&mut inner.schedules);
            return Ok(());
        };
        let mut list = schedules::update(&path, change)?;
        // Next-run times are this process's own bookkeeping and are not what
        // the other process wrote, so recompute rather than trust.
        for s in list.iter_mut() {
            if s.next_run_at.is_none() {
                compute_next_run(s);
            }
        }
        inner.schedules = list;
        // The write re-read the store to apply the change, so it parses: any
        // remembered failure is stale.
        inner.load_error = None;
        Ok(())
    }

    /// Execute what a schedule runs right now, regardless of its timing.
    /// Updates last-run bookkeeping on completion.
    ///
    /// That is one pipeline for most schedules and a whole plan for one that names one.
    pub async fn run_now(&self, id: &str) -> Result<RunResult, String> {
        let (workspace, sched) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            let s = g
                .schedules
                .iter()
                .find(|s| s.id == id)
                .ok_or_else(|| "Schedule not found".to_string())?;
            (g.workspace_path.clone(), s.clone())
        };
        let workspace =
            workspace.ok_or_else(|| "No workspace set for the scheduler".to_string())?;
        if sched.plan_id.is_some() {
            let started = Utc::now();
            let result = self.run_plan(&workspace, &sched).await?;
            self.record_run(id, started, &result);
            return Ok(result);
        }
        let pipeline_id = sched.pipeline_id.clone();
        // Resolve workspace context exactly like the canvas and the runner do:
        // substitute ${var} / ${context.var} (e.g. a context-based DB password),
        // inline SQL routines, and rewrite child-pipeline refs. Without this a
        // scheduled run sent the raw ${context.X} placeholder to the driver, so
        // a pipeline that ran fine from the canvas failed under a schedule with
        // auth errors like ORA-01017 (issue #32).
        let mut pipeline = duckle_duckdb_engine::context::resolve_workspace(
            &workspace,
            &pipeline_id,
            None,
        )?
        .doc;
        // Stamp the dynamic date/time builtins (${date}/${datetime}/...) at fire
        // time, so a recurring schedule writes a fresh-dated path on every run.
        duckle_duckdb_engine::context::apply_time_builtins(&mut pipeline);
        // Expand saved Salesforce connection refs into node auth props (#166
        // stage 2) BEFORE the env pass, so a connection field stored as
        // ${ENV:...} still resolves below.
        duckle_secrets::resolve_connection_refs(&workspace, &mut pipeline.nodes)?;
        // Resolve ${ENV:NAME} from the process environment so scheduled runs see
        // OS env vars just like the headless runner does (issue #137).
        duckle_duckdb_engine::context::apply_env(&mut pipeline);
        // Fetch anything held in a vault (CyberArk and the like) for this run.
        duckle_duckdb_engine::context::apply_vault(&mut pipeline);
        // A fresh per-run cancel scope so concurrent scheduled runs (and the
        // interactive run) don't share or reset each other's cancellation.
        let engine = self.engine.for_new_run();
        let started = Utc::now();
        // Log scheduled runs under the pipeline id (the scheduler has no
        // friendly name handy) so they still land in the per-pipeline log.
        let log_name = pipeline_id.clone();
        // #259: identity before work, on the scheduler too. A scheduled run
        // that dies with the server used to leave nothing addressable at all.
        let hash = duckle_duckdb_engine::retry::pipeline_hash(&pipeline);
        let run_id = duckle_duckdb_engine::retry::new_run_id(&pipeline_id, "scheduled");
        let receipt = duckle_duckdb_engine::retry::begin(
            &workspace,
            &run_id,
            "scheduled",
            &pipeline_id,
            &workspace
                .join("pipelines")
                .join(format!("{pipeline_id}.json"))
                .display()
                .to_string(),
            &hash,
            None,
        );
        let result =
            tokio::task::spawn_blocking(move || engine.execute_pipeline_named(&pipeline, &log_name))
                .await
                .map_err(|e| e.to_string())?;
        duckle_duckdb_engine::retry::finish(
            &workspace,
            receipt,
            &result.status,
            result
                .nodes
                .iter()
                .map(|(nid, st)| {
                    (
                        nid.clone(),
                        duckle_duckdb_engine::retry::ReceiptNode {
                            status: st.status.clone(),
                            kind: st.kind.clone(),
                            output_cache_key: result.cache_keys.get(nid).cloned(),
                        },
                    )
                })
                .collect(),
        );
        self.record_run(id, started, &result);
        Ok(result)
    }

    /// Run the plan a schedule names, one pipeline at a time.
    ///
    /// Each pipeline goes through the ordinary single-pipeline path, so run history names
    /// the pipeline that failed rather than the plan around it: at three in the morning the
    /// question is which step broke, and a plan-shaped record cannot answer it.
    ///
    /// Locking is per pipeline and taken here, as each one comes up, because that is the
    /// thing another process might also be running. A plan holding every one of its
    /// pipelines for its whole duration would block a colleague's unrelated run for as long
    /// as the slowest step.
    async fn run_plan(&self, workspace: &Path, sched: &Schedule) -> Result<RunResult, String> {
        let plan = work_of(workspace, sched)?;
        let started = Utc::now();
        let run = self.execute_plan(workspace, plan.clone(), "scheduled").await?;

        // One aggregate result for the schedule itself, so the Schedules view shows a plan
        // going green or red like anything else. The detail is in the per-pipeline history
        // written above; what belongs here is which pipelines did not work.
        let broken: Vec<String> = run
            .steps
            .iter()
            .flat_map(|s| s.pipelines.iter())
            .filter(|p| p.status != "ok")
            .map(|p| match &p.error {
                Some(e) => format!("{}: {e}", p.pipeline),
                None => format!("{} was skipped after an earlier failure", p.pipeline),
            })
            .collect();
        let elapsed = Utc::now().signed_duration_since(started).num_milliseconds().max(0) as u64;
        Ok(RunResult {
            cache_keys: Default::default(),
            status: if run.failed() { "error".into() } else { "success".into() },
            // A plan rollup is not a source poll; it always did work or failed.
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            artifacts: Vec::new(),
            artifacts_truncated: false,
            duration_ms: elapsed,
            nodes: Default::default(),
            preview: Vec::new(),
            category: None,
            error: (!broken.is_empty())
                .then(|| format!("Plan '{}' - {}", plan.id, broken.join("; "))),
        })
    }

    /// Run a plan by name, right now, and answer with what became of each pipeline.
    ///
    /// This is what an editor calls. It answers with the whole `PlanRun` rather than the
    /// one aggregate a schedule records, because somebody watching a plan they just started
    /// wants to see which step is which, including the ones an earlier failure meant nobody
    /// attempted.
    pub async fn run_plan_now(
        &self,
        workspace: &Path,
        plan_id: &str,
    ) -> Result<plans::PlanRun, String> {
        let plan = plans::load(workspace)?
            .into_iter()
            .find(|p| p.id == plan_id)
            .ok_or_else(|| format!("There is no plan called '{plan_id}'"))?;
        // Refused before anything runs rather than half way through. A plan with an empty
        // step would otherwise report two pipelines fine and then stop for a reason that
        // has nothing to do with either of them.
        let problems = plan.problems();
        if !problems.is_empty() {
            return Err(problems.join("; "));
        }
        self.execute_plan(workspace, plan, "manual").await
    }

    /// Run a plan's pipelines, with the locking, run history and alerting each one would
    /// get under a schedule of its own.
    ///
    /// The work is blocking - it waits on a DuckDB child process per pipeline - so it goes
    /// to a blocking thread. Running it inline on the async runtime, as this briefly did,
    /// parks a tokio worker for the whole length of the plan, and a plan is the longest
    /// thing this crate runs.
    async fn execute_plan(
        &self,
        workspace: &Path,
        plan: plans::Plan,
        trigger: &str,
    ) -> Result<plans::PlanRun, String> {
        let engine = self.engine.clone();
        let ws = workspace.to_path_buf();
        let trigger = trigger.to_string();
        tokio::task::spawn_blocking(move || {
            plans::execute(&plan, |step| {
                // Normalised ONCE, here, and everything below uses it. The run lock, the
                // run history and the alert are all keyed by a pipeline's bare id, because
                // that is what a schedule of its own uses and what the Runs views read.
                // Passing the raw step filed a plan's runs at `runs/pipelines/x.json.json`,
                // where they were recorded and invisible.
                let pipeline = plans::step_pipeline_id(step);
                let started = Utc::now();
                // A lock this process cannot take means somebody else is running that
                // pipeline now. Treated as a failure of the step rather than a skip,
                // because carrying on would run the next step against data this one did
                // not produce.
                let _claim = match claim_run(Some(&ws), pipeline) {
                    Claim::Ours(lock) => lock,
                    Claim::Taken => {
                        return Err(format!("{pipeline} is already running in another process"))
                    }
                    Claim::Unusable(why) => {
                        return Err(format!("Cannot take a run lock for {pipeline}: {why}"))
                    }
                };
                let result = run_one_blocking(&engine, &ws, pipeline);
                let answer = match &result {
                    Ok(r) if r.status == "error" => {
                        Err(r.error.clone().unwrap_or_else(|| "the run failed".into()))
                    }
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.clone()),
                };
                // Every pipeline gets its own history entry and its own alert, exactly as
                // it would under a schedule of its own. Whoever watches a pipeline does not
                // have to know it was a plan that ran it.
                let record = match result {
                    Ok(r) => r,
                    Err(e) => failed_run(started, &e),
                };
                duckle_duckdb_engine::alerts::notify(&ws, pipeline, &record);
                let _ = append_run_record(
                    &ws,
                    pipeline,
                    RunRecord::from_result_in(&ws, pipeline, &record, &trigger),
                );
                answer
            })
        })
        .await
        .map_err(|e| format!("the plan could not be run: {e}"))
    }

    /// Fire a schedule and make sure the outcome is recorded whichever way it
    /// goes.
    ///
    /// `run_now` only records after the pipeline has actually executed, so
    /// every `?` before that point - a pipeline file that has been renamed or
    /// deleted, a context that will not resolve, no workspace - produced a log
    /// line and nothing else. No alert, no `last_run_at`, and a schedule that
    /// reads as though it never fired at all. That is the same silence the
    /// runner's scheduler had, and it is worse here because the desktop is
    /// where someone would go looking for the reason.
    async fn fire_and_record(&self, id: &str, why: &str) {
        let started = Utc::now();
        let Err(e) = self.run_now(id).await else {
            return;
        };
        warn!("{} run {} failed: {}", why, id, e);
        // A run that never started still took time and still failed, which is
        // exactly what an operator needs to see against the schedule.
        let elapsed = Utc::now().signed_duration_since(started).num_milliseconds().max(0) as u64;
        let result = RunResult {
            cache_keys: Default::default(),
            status: "error".into(),
            duration_ms: elapsed,
            nodes: Default::default(),
            preview: Vec::new(),
            category: Some(
                duckle_duckdb_engine::error_category::categorize_error(&e).to_string(),
            ),
            error: Some(e),
            unchanged: false,
            incomplete: false,
            incomplete_reason: None,
            artifacts: Vec::new(),
            artifacts_truncated: false,
        };
        self.record_run(id, started, &result);
    }

    fn record_run(&self, id: &str, started: DateTime<Utc>, result: &RunResult) {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        // A plan schedule has no run history of its own: `run_plan` already wrote one entry
        // per pipeline it actually ran, and adding another under the schedule's label would
        // put a run in the history of a pipeline that never executed.
        let pipeline_id = g
            .schedules
            .iter()
            .find(|s| s.id == id)
            .filter(|s| s.plan_id.is_none())
            .map(|s| s.pipeline_id.clone());
        let (sid, status, duration, error) =
            (id.to_string(), result.status.clone(), result.duration_ms, result.error.clone());
        let saved = self.commit(&mut g, move |list| {
            if let Some(s) = list.iter_mut().find(|s| s.id == sid) {
                s.last_run_at = Some(started);
                s.last_run_status = Some(status);
                s.last_run_duration_ms = Some(duration);
                s.last_run_error = error;
                compute_next_run(s);
            }
        });
        // Nobody is waiting on this one - the run already happened - so the
        // outcome goes to the log. Run history below is written either way, so
        // the run is not lost with it.
        if let Err(e) = saved {
            warn!("Could not record the run against schedule {}: {}", id, e);
        }
        let workspace = g.workspace_path.clone();
        // Everything below talks to the disk and the network, so the lock goes
        // back first. Alert delivery waits up to ten seconds per channel, and
        // holding the scheduler's mutex across that stalls every other thing
        // that needs it - the next tick, the schedule list, an edit from the
        // UI - for as long as an unreachable webhook takes to time out.
        drop(g);

        // Append to the pipeline's run history too, and tell whoever asked to
        // be told. Alerting comes after the record so a channel that is down
        // cannot cost a run its history entry, and it never raises: see
        // duckle_duckdb_engine::alerts::notify.
        if let (Some(path), Some(pid)) = (workspace, pipeline_id) {
            let record = RunRecord::from_result_in(&path, &pid, result, "scheduled");
            let _ = append_run_record(&path, &pid, record);
            duckle_duckdb_engine::alerts::notify(&path, &pid, result);
        }
    }

    /// Start the polling task and the file-watch fire listener.
    /// Returns immediately.
    pub fn spawn_ticker(&self) {
        // Cron / interval poller.
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = time::interval(tick_interval());
            tick.tick().await; // Skip the immediate tick.
            loop {
                tick.tick().await;
                me.fire_due().await;
            }
        });

        // File-watch fire listener - drains the channel watchers post to.
        let rx = {
            let mut g = self.inner.lock().expect("scheduler poisoned");
            g.fire_rx.take()
        };
        if let Some(mut rx) = rx {
            let me = self.clone();
            tokio::spawn(async move {
                while let Some(id) = rx.recv().await {
                    let me2 = me.clone();
                    tokio::spawn(async move {
                        // Watching is per process, so two Duckle processes
                        // watching one folder both see the same file land and
                        // both fire. Same clash as a cron tick, same guard.
                        let (workspace, pipeline_id) = {
                            let g = me2.inner.lock().expect("scheduler poisoned");
                            let pipeline_id = g
                                .schedules
                                .iter()
                                .find(|s| s.id == id)
                                .and_then(|s| lock_key(s).map(str::to_string));
                            (g.workspace_path.clone(), pipeline_id)
                        };
                        // Nothing to lock in two cases, and neither wants one. A plan locks
                        // each of its pipelines as it reaches them; a schedule that vanished
                        // between the file event and here has no pipeline at all, and
                        // run_now reports it missing.
                        let _claim = match &pipeline_id {
                            None => None,
                            Some(key) => match claim_run(workspace.as_deref(), key) {
                                Claim::Ours(lock) => lock,
                                Claim::Taken => {
                                    warn!(
                                        "Pipeline {} is already running in another process; \
                                         skipping the file-watch fire of {}",
                                        key, id
                                    );
                                    return;
                                }
                                Claim::Unusable(why) => {
                                    warn!(
                                        "Cannot take a run lock for {} in this workspace, so the \
                                         file-watch fire of {} was skipped: {}. This will not \
                                         clear on its own.",
                                        key, id, why
                                    );
                                    return;
                                }
                            },
                        };
                        me2.fire_and_record(&id, "File-watch").await;
                    });
                }
            });
        }
    }

    /// Take every schedule that is due, and claim it so nothing else takes it.
    ///
    /// Split out from `fire_due` so a test can drive the claim itself rather
    /// than a copy of it: the bug this guards against was invisible to a test
    /// that re-implemented the claiming step.
    ///
    /// Returns each due schedule's id alongside its pipeline id, because the
    /// schedule is what came due but the pipeline is what gets locked.
    fn claim_due(&self, now: DateTime<Utc>) -> Vec<(String, Option<String>)> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let due: Vec<(String, Option<String>)> = g
            .schedules
            .iter()
            .filter(|s| s.enabled && matches!(s.next_run_at, Some(t) if t <= now))
            .map(|s| (s.id.clone(), lock_key(s).map(str::to_string)))
            .collect();
            // Claim the occurrence immediately, under the lock, by advancing
            // next_run_at to the next FUTURE time. The tick wakes every 15s and
            // run_now only recomputes next_run_at on completion (record_run);
            // without this claim a run slower than 15s gets re-fired every
            // tick. Advancing (vs clearing to None) keeps the schedule firing
            // on cadence even if this run errors before record_run.
            //
            // The claim goes to the STORE, not just to this process's copy.
            // Held in memory it was undone by the next commit for any reason
            // at all - a schedule edited in the UI, another schedule's run
            // finishing - because commit adopts the list from disk, where
            // next_run_at was still the time already claimed and therefore
            // still in the past. The run in flight was then due again on the
            // very next tick, and only the run lock stopped it, logging a
            // refusal that blamed "another process" for this one. Writing it
            // also makes the claim visible to the other process, so the lock
            // goes back to being the backstop it was meant to be.
        if !due.is_empty() {
            let claimed: Vec<String> = due.iter().map(|(id, _)| id.clone()).collect();
            if let Err(e) = self.commit(&mut g, move |list| {
                for s in list.iter_mut() {
                    if claimed.iter().any(|id| id == &s.id) {
                        claim_next_run(s, now);
                    }
                }
            }) {
                // The runs still go ahead: the lock keeps them from doubling,
                // and refusing to fire because the bookkeeping could not be
                // written would turn a full disk into a silent outage of every
                // schedule.
                warn!("Could not record the fire claim: {}", e);
                for s in g.schedules.iter_mut() {
                    if due.iter().any(|(id, _)| id == &s.id) {
                        claim_next_run(s, now);
                    }
                }
            }
        }
        due
    }

    async fn fire_due(&self) {
        let now = Utc::now();
        // Read the workspace under the same lock as the due list, so the path
        // used for the run lock is the one this tick actually decided against.
        let workspace = { self.inner.lock().expect("scheduler poisoned").workspace_path.clone() };
        let due = self.claim_due(now);
        for (id, pipeline_id) in due {
            let me = self.clone();
            let workspace = workspace.clone();
            let permit = run_permits().clone();
            tokio::spawn(async move {
                // Hold a permit for the whole run. Every schedule that comes due
                // in the same tick used to fire at once, so ten due at midnight
                // meant ten pipelines each sized for the whole machine. The
                // permit bounds that; the run still happens, it just queues.
                let _slot = permit.acquire_owned().await;
                // The semaphore above bounds this process only. Skipping on a
                // clash rather than queueing is deliberate: the next tick comes
                // round anyway, and a backlog of identical overdue runs helps
                // nobody.
                //
                // No key means a plan, which locks each of its pipelines itself as it
                // reaches them. See `lock_key`.
                let _claim = match &pipeline_id {
                    None => None,
                    Some(pipeline_id) => match claim_run(workspace.as_deref(), pipeline_id) {
                        Claim::Ours(lock) => lock,
                        Claim::Taken => {
                            warn!(
                                "Pipeline {} is already running in another process; \
                                 skipping schedule {} this tick",
                                pipeline_id, id
                            );
                            return;
                        }
                        Claim::Unusable(why) => {
                            warn!(
                                "Cannot take a run lock for {} in this workspace, so schedule {} \
                                 was skipped: {}. Every tick will skip it until this is fixed.",
                                pipeline_id, id, why
                            );
                            return;
                        }
                    },
                };
                me.fire_and_record(&id, "Scheduled").await;
            });
        }
    }
}

/// How many scheduled pipelines may execute at once.
///
/// Set by power mode via DUCKLE_MAX_CONCURRENT_RUNS. Read once, because the
/// bound has to be a single shared semaphore for it to mean anything.
///
/// The default is deliberately generous rather than 1: firing due schedules
/// concurrently is long-standing behaviour here and some workspaces rely on
/// it. What it was missing was any ceiling at all. Each concurrent run gets
/// its own memory limit and its own DuckDB child, so the honest ceiling is a
/// function of RAM, which is why power mode asks rather than assumes.
fn run_permits() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    static PERMITS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    PERMITS.get_or_init(|| {
        let n = std::env::var("DUCKLE_MAX_CONCURRENT_RUNS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(8);
        std::sync::Arc::new(tokio::sync::Semaphore::new(n))
    })
}

/// Advance next_run_at to the next occurrence strictly after `now`.
/// Used to "claim" a due schedule at dispatch so the 15s ticker can't
/// re-fire a still-running schedule. Unlike compute_next_run (which for
/// intervals is anchored on last_run_at and can still be in the past for
/// an overdue run), this is always anchored on `now`, guaranteeing a
/// future time.
fn claim_next_run(s: &mut Schedule, now: DateTime<Utc>) {
    s.next_run_at = match &s.kind {
        // #318: read in the schedule's own zone when it names one, otherwise
        // the machine's, and store the resulting absolute instant as UTC. Both
        // schedulers call the same evaluator so they cannot drift apart again.
        ScheduleKind::Cron { expr } => cron_next(expr, s.timezone.as_deref(), &s.exclude, now),
        ScheduleKind::Interval { seconds } => {
            Some(now + chrono::Duration::seconds(*seconds as i64))
        }
        ScheduleKind::FileWatch { .. } => None,
    };
}

fn compute_next_run(s: &mut Schedule) {
    if !s.enabled {
        s.next_run_at = None;
        return;
    }
    s.next_run_at = match &s.kind {
        ScheduleKind::Cron { expr } => cron_next(expr, s.timezone.as_deref(), &s.exclude, Utc::now()),
        ScheduleKind::Interval { seconds } => {
            let base = s.last_run_at.unwrap_or_else(Utc::now);
            Some(base + chrono::Duration::seconds(*seconds as i64))
        }
        // Event-driven - no scheduled next-run time.
        ScheduleKind::FileWatch { .. } => None,
    };
}

/// What makes a schedule saveable.
///
/// Split out so saving and evaluating cannot disagree, which they did: this
/// validated with a bare `CronSchedule::from_str`, so a five-field expression
/// was REFUSED on save while `compute_next_run` normalised it and scheduled it
/// happily. A schedule you cannot save but which would have worked is the same
/// class of bug as one you can save that never fires.
fn validate_schedule(schedule: &Schedule) -> Result<(), String> {
    // #318: an unknown zone is refused here rather than at fire time, so a typo
    // is a save error in front of the person who made it, not a job that
    // quietly runs on UTC in a container.
    duckle_duckdb_engine::cronzone::resolve_zone(schedule.timezone.as_deref())?;
    schedule.exclude.validate()?;
    if let ScheduleKind::Cron { expr } = &schedule.kind {
        let normalized = duckle_duckdb_engine::cronzone::normalize_cron(expr).ok_or_else(|| {
            format!("Invalid cron expression: {expr:?} does not have 5, 6 or 7 fields")
        })?;
        CronSchedule::from_str(&normalized)
            .map_err(|e| format!("Invalid cron expression: {}", e))?;
    }
    Ok(())
}

/// The next firing of a cron expression, in the schedule's zone (#318).
///
/// A bad expression or an unknown zone yields None - the same "this schedule
/// has no next run" the old code produced for an unparseable expression - but
/// the reason is said out loud, because a schedule that silently never fires is
/// the failure mode this area already had once.
fn cron_next(
    expr: &str,
    timezone: Option<&str>,
    exclude: &duckle_duckdb_engine::cronzone::Exclusions,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let zone = match duckle_duckdb_engine::cronzone::resolve_zone(timezone) {
        Ok(z) => z,
        Err(e) => {
            eprintln!("duckle: schedule has an unusable time zone: {e}");
            return None;
        }
    };
    match duckle_duckdb_engine::cronzone::next_after_excluding(expr, &zone, exclude, now) {
        Ok((occ, skipped)) => {
            for s in skipped {
                // A civil time that does not exist has not been missed by the
                // scheduler - the day was short. Said once, where an operator
                // looking for "why did 02:30 not run" will find it.
                eprintln!("duckle: schedule skipped an occurrence: {s:?}");
            }
            occ.map(|o| o.at)
        }
        Err(e) => {
            eprintln!("duckle: schedule cron is unusable: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Local;
    use super::*;

    #[test]
    fn cron_parses_and_computes_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "every minute".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 * * * * *".into(),
            },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        assert!(s.next_run_at.is_some());
        assert!(s.next_run_at.unwrap() > Utc::now());
    }

    /// Issue #194: cron must be evaluated in the machine's local time zone,
    /// not UTC. Asserting on the LOCAL hour (rather than a hardcoded UTC hour)
    /// keeps this correct on any developer machine and in CI.
    #[test]
    fn cron_fires_at_the_local_wall_clock_hour() {
        use chrono::Timelike;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "daily 3am".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 0 3 * * *".into(),
            },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("next_run_at set").with_timezone(&Local);
        assert_eq!(next.hour(), 3, "3am cron must land on 3am local, got {}", next);
        assert_eq!(next.minute(), 0);
    }

    /// The claim path (used at dispatch to stop a re-fire) must agree with
    /// compute_next_run, or a schedule fires correctly once and then re-arms
    /// in the wrong zone.
    #[test]
    fn claim_next_run_also_uses_local_time() {
        use chrono::Timelike;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "daily 3am".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 0 3 * * *".into(),
            },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        claim_next_run(&mut s, Utc::now());
        let next = s.next_run_at.expect("next_run_at set").with_timezone(&Local);
        assert_eq!(next.hour(), 3, "claim must also be local, got {}", next);
    }

    /// A hand-written 5-field cron used to parse to None, leaving next_run_at
    /// unset so the schedule silently never fired.
    #[test]
    fn five_field_cron_is_accepted_and_scheduled() {
        use chrono::Timelike;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "daily 3am, 5-field".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 3 * * *".into(),
            },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("5-field cron must schedule").with_timezone(&Local);
        assert_eq!(next.hour(), 3);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn a_five_field_cron_can_be_saved_as_well_as_scheduled() {
        // Saving used to validate with a bare CronSchedule::from_str, which
        // refuses a five-field expression, while compute_next_run normalised it
        // and scheduled it. So an expression that worked could not be saved.
        let s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "daily".into(),
            enabled: true,
            kind: ScheduleKind::Cron { expr: "0 3 * * *".into() },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        assert!(validate_schedule(&s).is_ok(), "{:?}", validate_schedule(&s));
    }

    #[test]
    fn an_unknown_time_zone_is_refused_on_save() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "daily".into(),
            enabled: true,
            kind: ScheduleKind::Cron { expr: "0 0 3 * * *".into() },
            timezone: Some("Europe/Brussel".into()),
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        let e = validate_schedule(&s).unwrap_err();
        assert!(e.contains("Europe/Brussel"), "must name the typo: {e}");
        s.timezone = Some("Europe/Brussels".into());
        assert!(validate_schedule(&s).is_ok(), "the real zone must be accepted");
    }

    /// The point of #318: the instant follows the named zone, not the host.
    #[test]
    fn a_zoned_cron_fires_on_that_zones_clock() {
        use chrono::TimeZone;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "brussels 3am".into(),
            enabled: true,
            kind: ScheduleKind::Cron { expr: "0 0 3 * * *".into() },
            timezone: Some("Europe/Brussels".into()),
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        claim_next_run(&mut s, chrono::Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap());
        assert_eq!(
            s.next_run_at.expect("scheduled"),
            chrono::Utc.with_ymd_and_hms(2026, 1, 11, 2, 0, 0).unwrap(),
            "03:00 Brussels in January is 02:00 UTC, wherever this runs"
        );
    }

    #[test]
    fn normalize_cron_rejects_bad_field_counts() {
        use duckle_duckdb_engine::cronzone::normalize_cron;
        assert_eq!(normalize_cron("0 3 * * *").as_deref(), Some("0 0 3 * * *"));
        assert_eq!(normalize_cron("0 0 3 * * *").as_deref(), Some("0 0 3 * * *"));
        assert!(normalize_cron("* * *").is_none());
        assert!(normalize_cron("* * * * * * * *").is_none());
        assert!(normalize_cron("").is_none());
    }

    #[test]
    fn interval_computes_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "every 5".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 300 },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("next_run_at set");
        let now = Utc::now();
        let delta = next - now;
        assert!(delta.num_seconds() <= 301 && delta.num_seconds() >= 299);
    }

    #[test]
    fn disabled_clears_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "off".into(),
            enabled: false,
            kind: ScheduleKind::Interval { seconds: 60 },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: Some(Utc::now()),
        };
        compute_next_run(&mut s);
        assert!(s.next_run_at.is_none());
    }

    /// The condition the run lock exists for. `fire_due` claims an occurrence
    /// by advancing `next_run_at` under the in-process mutex, which is enough
    /// for one process and does nothing for two: the claim never reaches disk
    /// at fire time, so a desktop app and a `duckle-runner serve` daemon
    /// pointed at one workspace independently decide the same schedule is due
    /// in the same second. This asserts that decision is genuinely made twice,
    /// so the guard in `fire_due` is load-bearing rather than defensive.
    #[test]
    fn two_schedulers_on_one_workspace_both_decide_the_same_run_is_due() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let engine = || DuckdbEngine::new(PathBuf::from("duckdb"));

        // The desktop app, which writes the schedule to the workspace.
        let desktop = Scheduler::new(engine());
        desktop.set_workspace(Some(ws.clone()));
        desktop
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "nightly-load".into(),
                plan_id: None,
                name: "every second".into(),
                enabled: true,
                // Six fields, so the leading one is seconds: due almost at once.
                kind: ScheduleKind::Cron { expr: "* * * * * *".into() },
                timezone: None,
                exclude: Default::default(),
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .expect("schedule rejected");

        // A runner daemon started afterwards against the same workspace, which
        // is how a workspace gets promoted from a laptop to a server.
        let daemon = Scheduler::new(engine());
        daemon.set_workspace(Some(ws.clone()));

        // Let the next-run time arrive for both.
        std::thread::sleep(Duration::from_millis(1500));
        let now = Utc::now();
        let due = |s: &Scheduler| -> Vec<String> {
            s.list().expect("schedules unreadable")
                .into_iter()
                .filter(|x| x.enabled && matches!(x.next_run_at, Some(t) if t <= now))
                .map(|x| x.id)
                .collect()
        };
        let a = due(&desktop);
        let b = due(&daemon);
        assert_eq!(a.len(), 1, "the desktop scheduler did not consider it due");
        assert_eq!(
            a, b,
            "both processes must reach the same fire decision for the lock to matter"
        );

        // And that shared decision is exactly what the lock arbitrates: the
        // first process to ask gets to run it, the second is turned away.
        // Keys come from lock_key, the same function both fire paths use, so a
        // change of mind about what gets locked fails here rather than shipping.
        let key = |s: &Scheduler, id: &str| -> String {
            lock_key(
                s.list()
                    .expect("schedules unreadable")
                    .iter()
                    .find(|x| x.id == id)
                    .expect("schedule vanished"),
            )
            .expect("a single-pipeline schedule locks that pipeline")
            .to_string()
        };
        let held = key(&desktop, &a[0]);
        let first = match claim_run(Some(&ws), &held) {
            Claim::Ours(lock) => lock.expect("a workspace was set, so a lock was due"),
            Claim::Taken => panic!("the first process could not take the run lock"),
            Claim::Unusable(why) => panic!("this workspace cannot be locked at all: {why}"),
        };
        assert!(
            matches!(claim_run(Some(&ws), &key(&daemon, &b[0])), Claim::Taken),
            "the second process was allowed to run the same pipeline"
        );

        // What is locked is the pipeline, and that is what makes the guard hold
        // across products: the web console names a schedule by its pipeline
        // while this crate mints a uuid, so a record-keyed lock would have the
        // two picking different files and guarding nothing. The ids genuinely
        // differ here, so this would catch that.
        assert_eq!(held, "nightly-load", "the lock was not keyed on the pipeline");
        assert_ne!(a[0], held, "the schedule id and pipeline id must differ here");
        drop(first);
    }

    /// A save that did not reach the disk is not a save.
    ///
    /// `commit` logged the write failure and returned nothing, so `upsert` and
    /// `delete` handed back Ok for a schedule that never reached the store. A
    /// read-only workspace, a full disk or a store that will not parse all
    /// looked exactly like success, and the schedule was simply absent at the
    /// next restart.
    #[test]
    fn a_schedule_that_could_not_be_written_is_not_reported_as_saved() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        // A store that will not parse. Every write is a read-modify-write, so
        // this fails the read and must not be silently overwritten either.
        std::fs::write(ws.join("schedules.json"), "{ this is not json").unwrap();

        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));

        let err = sched
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "nightly-load".into(),
                plan_id: None,
                name: "nightly".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                timezone: None,
                exclude: Default::default(),
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .expect_err("a schedule that was never written was reported as saved");
        assert!(!err.is_empty(), "the failure has to say something");

        // And the unreadable store is left exactly as it was, rather than
        // being replaced by a list built from a failed read.
        let after = std::fs::read_to_string(ws.join("schedules.json")).unwrap();
        assert_eq!(after, "{ this is not json", "a corrupt store was overwritten");

        assert!(sched.delete("anything").is_err(), "delete reported success too");
    }

    /// "I could not read the file" must never be shown as "you have none".
    ///
    /// An unreadable store came back as an empty list, so the UI said there
    /// were no schedules - the most alarming possible way to report a parse
    /// error, and one that invites re-creating schedules that are still on
    /// disk. It also has to recover on its own once the file is repaired,
    /// because the alternative is telling someone to restart the app.
    #[test]
    fn an_unreadable_store_says_so_and_recovers_when_it_is_repaired() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));

        // A workspace with a real schedule in it.
        sched.set_workspace(Some(ws.clone()));
        let saved = sched
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "nightly-load".into(),
                plan_id: None,
                name: "nightly".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                timezone: None,
                exclude: Default::default(),
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .unwrap();
        assert_eq!(sched.list().unwrap().len(), 1);

        // Now the file is damaged - a half-written save, a bad merge.
        let good = std::fs::read_to_string(ws.join("schedules.json")).unwrap();
        std::fs::write(ws.join("schedules.json"), "[{\"id\": \"nightly").unwrap();
        sched.set_workspace(Some(ws.clone()));

        let err = sched.list().expect_err("an unreadable store was reported as no schedules");
        assert!(!err.is_empty(), "the failure has to say something");

        // And nothing fires while it cannot be read, rather than the previous
        // workspace's schedules firing against this one.
        assert!(sched.claim_due(Utc::now()).is_empty(), "a schedule fired from an unreadable store");

        // The file is left exactly as it was, so the schedules are recoverable.
        assert_eq!(
            std::fs::read_to_string(ws.join("schedules.json")).unwrap(),
            "[{\"id\": \"nightly",
            "the damaged store was overwritten"
        );

        // Repair it, and the next question gets the right answer without a
        // restart or a workspace switch.
        std::fs::write(ws.join("schedules.json"), good).unwrap();
        let back = sched.list().expect("a repaired store still reported as broken");
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].id, saved.id, "the schedule came back changed");
    }

    /// A fire claim has to survive the next save, whatever caused it.
    ///
    /// `fire_due` advanced `next_run_at` to claim an occurrence, but only in
    /// this process's copy. `commit` adopts the list from disk, where the time
    /// was still the one already claimed and therefore still in the past, so
    /// any unrelated save - a schedule edited in the UI, another schedule's run
    /// finishing - put the in-flight schedule straight back into the due set.
    #[test]
    fn an_unrelated_save_does_not_make_a_running_schedule_due_again() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));

        let mut due_now = Schedule {
            id: String::new(),
            pipeline_id: "nightly-load".into(),
            plan_id: None,
            name: "nightly".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 3600 },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        let running = sched.upsert(due_now.clone()).unwrap().id;
        // Make it due, the way an hour passing would.
        let now = Utc::now();
        {
            let mut g = sched.inner.lock().unwrap();
            sched
                .commit(&mut g, |list| {
                    for s in list.iter_mut() {
                        s.next_run_at = Some(now - chrono::Duration::seconds(1));
                    }
                })
                .unwrap();
        }

        // Claim it through the code a tick actually runs, not a copy of it.
        // An earlier version of this test re-implemented the claim and so
        // passed with the defect still in place.
        let claimed = sched.claim_due(now);
        assert_eq!(claimed.len(), 1, "the schedule was not due when it should have been");

        // Now something else saves, which is the step that used to undo it.
        due_now.id = String::new();
        due_now.pipeline_id = "unrelated".into();
        due_now.name = "unrelated".into();
        sched.upsert(due_now).unwrap();

        let after = sched.list().unwrap().into_iter().find(|s| s.id == running).unwrap();
        assert!(
            matches!(after.next_run_at, Some(t) if t > now),
            "the running schedule is due again after an unrelated save: {:?}",
            after.next_run_at
        );
    }

    /// A run that never gets as far as starting still has to be reported.
    ///
    /// `run_now` records only after the pipeline has executed, so every early
    /// return - a pipeline file renamed or deleted out from under a schedule,
    /// a context that will not resolve - used to leave a `warn!` in the log and
    /// nothing anywhere a person looks: the schedule kept its old green status,
    /// `last_run_at` stayed where it was, run history gained no entry and no
    /// alert went out. A schedule that stopped working looked like one that was
    /// working. This drives the failure through the fire path both triggers now
    /// use and asserts every one of those surfaces sees it.
    #[tokio::test]
    async fn a_schedule_whose_pipeline_is_gone_reports_a_failed_run() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));
        let id = sched
            .upsert(Schedule {
                id: String::new(),
                // No such file in the workspace: this is the pipeline someone
                // renamed without touching the schedule that points at it.
                pipeline_id: "nightly-load".into(),
                plan_id: None,
                name: "nightly".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                timezone: None,
                exclude: Default::default(),
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .expect("schedule rejected")
            .id;

        sched.fire_and_record(&id, "Test").await;

        let after = sched.list().unwrap().into_iter().find(|s| s.id == id).expect("schedule vanished");
        assert_eq!(after.last_run_status.as_deref(), Some("error"));
        assert!(after.last_run_at.is_some(), "the fire left no last_run_at");
        assert!(
            after.last_run_error.is_some(),
            "the failure was not kept against the schedule"
        );

        // The same failure has to survive a restart, because the console reads
        // the store rather than this process's memory.
        let reread = schedules::load(&ws).expect("schedules did not persist");
        let stored = reread.iter().find(|s| s.id == id).expect("schedule not on disk");
        assert_eq!(stored.last_run_status.as_deref(), Some("error"));

        // And it has to reach run history, which is what the Runs view reads
        // and what the metrics textfile is derived from.
        let history = ws.join("runs").join("nightly-load.json");
        let text = std::fs::read_to_string(&history)
            .unwrap_or_else(|e| panic!("no run history at {}: {e}", history.display()));
        let records: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
        let last = records.last().expect("run history is empty");
        assert_eq!(last["status"], "error");
        assert_eq!(last["trigger"], "scheduled");
        assert!(
            last["error"].as_str().unwrap_or("").contains("nightly-load"),
            "the record does not say which pipeline could not be loaded: {last}"
        );
    }

    fn plan_schedule(plan: &str) -> Schedule {
        Schedule {
            id: "s".into(),
            // Deliberately a real-looking pipeline id. A schedule that fires a plan still
            // carries one, and the bug this guards against is firing it.
            pipeline_id: "not-the-plan".into(),
            plan_id: Some(plan.into()),
            name: "nightly".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 3600 },
            timezone: None,
            exclude: Default::default(),
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        }
    }

    fn write_plan(ws: &Path) {
        let plan = plans::Plan {
            id: "nightly".into(),
            name: "Nightly load".into(),
            stop_on_failure: true,
            steps: vec![
                plans::Step {
                    name: "Extract".into(),
                    pipelines: vec!["orders.json".into(), "customers.json".into()],
                    continue_on_failure: None,
                },
                plans::Step { name: "Publish".into(), pipelines: vec!["export.json".into()], continue_on_failure: None },
            ],
        };
        plans::update(ws, |list| list.push(plan)).unwrap();
    }

    /// The whole point of a plan schedule, and the thing the desktop scheduler did not do.
    ///
    /// The console learned to fire a plan; this crate is the scheduler the desktop app runs,
    /// against the same `schedules.json`. Until it agreed, one workspace opened in both
    /// products meant the same record fired a plan in one and a single pipeline in the
    /// other, which is the shape of the timezone disagreement in issue #194 all over again.
    #[test]
    fn a_schedule_that_names_a_plan_runs_the_plan_and_not_its_pipeline_id() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_plan(ws);

        let work = work_of(ws, &plan_schedule("nightly")).expect("the plan should be found");
        let order: Vec<&str> =
            work.steps.iter().flat_map(|s| s.pipelines.iter()).map(String::as_str).collect();

        assert_eq!(order, ["orders.json", "customers.json", "export.json"]);
        assert!(
            !order.contains(&"not-the-plan"),
            "the schedule's own pipeline_id must not run: it is a label, not the work"
        );
        assert_eq!(work.steps.len(), 2, "the steps are what makes a plan a plan, not a list");
    }

    /// Every schedule written before plans existed still means one pipeline.
    #[test]
    fn a_schedule_without_a_plan_is_that_one_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let mut s = plan_schedule("nightly");
        s.plan_id = None;

        let work = work_of(ws, &s).expect("a plain schedule needs no store");
        assert_eq!(work.steps.len(), 1);
        assert_eq!(work.steps[0].pipelines, ["not-the-plan"]);
    }

    /// A schedule pointing at a plan somebody deleted has to say so, the same way one
    /// pointing at a deleted pipeline does. Doing nothing quietly is how a nightly load
    /// stops running for a week before anyone notices.
    #[test]
    fn a_schedule_naming_a_plan_that_is_gone_fails_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_plan(ws);

        let e = work_of(ws, &plan_schedule("weekly")).expect_err("a missing plan is not a no-op");
        assert!(e.contains("weekly"), "the error does not name the plan: {e}");
    }

    /// The desktop schedule editor has no plan field, so everything it saves says "no plan".
    /// Taking that at face value turns a plan schedule into a schedule for the label in its
    /// `pipeline_id`, which is not a pipeline: changing the interval of a nightly plan from
    /// the desktop app would quietly stop the plan from ever running again.
    #[test]
    fn editing_a_plan_schedule_from_an_editor_that_has_no_plan_field_keeps_the_plan() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));

        let mut saved = sched.upsert(plan_schedule("nightly")).expect("schedule rejected");
        assert_eq!(saved.plan_id.as_deref(), Some("nightly"));

        // What the desktop editor sends back after somebody changes the interval.
        saved.plan_id = None;
        saved.kind = ScheduleKind::Interval { seconds: 900 };
        sched.upsert(saved.clone()).expect("the edit was refused");

        let after = schedules::load(&ws).unwrap();
        let stored = after.iter().find(|s| s.id == saved.id).expect("schedule vanished");
        assert_eq!(
            stored.plan_id.as_deref(),
            Some("nightly"),
            "the edit dropped the plan this schedule runs"
        );
        assert!(matches!(stored.kind, ScheduleKind::Interval { seconds: 900 }));
    }

    /// One plans.json, two products, and they spelled a pipeline differently.
    ///
    /// The console writes a step as a workspace-relative file (`pipelines/orders.json`),
    /// because its run API takes a path. This crate hands the step straight to
    /// `context::resolve_workspace`, which builds `<workspace>/pipelines/<id>.json` from a
    /// BARE id. So a plan authored in the console asked the desktop app for
    /// `pipelines/pipelines/orders.json.json` and failed on every step.
    ///
    /// The plan tests above never caught it because they inject the runner as a closure -
    /// deliberately, so ordering is testable without DuckDB - and so nothing ever resolved
    /// a real name. This one goes through the real resolution path.
    #[test]
    fn a_plan_step_written_as_a_file_path_still_finds_its_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines").join("orders.json"),
            r#"{"name":"orders","nodes":[],"edges":[]}"#,
        )
        .unwrap();

        let engine = DuckdbEngine::new(PathBuf::from("duckdb"));
        for spelling in ["orders", "pipelines/orders.json"] {
            // Whether DuckDB is installed is not this test's business: resolving the name
            // is. A failure to RESOLVE comes back as Err before the engine is ever asked.
            if let Err(e) = run_one_blocking(&engine, ws, spelling) {
                panic!("a plan step spelled '{spelling}' could not be resolved: {e}");
            }
        }
    }

    /// A plan's runs have to land where everything else looks for them.
    ///
    /// Run history, alerts and the run lock are all keyed by the pipeline's bare id, because
    /// that is what a schedule of its own would use and what the Runs views read. Handing
    /// them the raw step instead put the history at `runs/pipelines/j1.json.json` - a real
    /// file, holding real runs, that nothing in either product ever looks at. The run was
    /// recorded and invisible, which is worse than not recorded at all.
    ///
    /// Caught by running a plan in the actual desktop app and looking at the folder, not by
    /// any test: every earlier test either injected the runner or never got as far as
    /// writing history.
    #[test]
    fn a_plans_runs_land_in_the_same_history_a_schedule_would_write() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines").join("orders.json"),
            r#"{"name":"orders","nodes":[],"edges":[]}"#,
        )
        .unwrap();

        let plan = plans::Plan {
            id: "nightly".into(),
            name: String::new(),
            stop_on_failure: true,
            // The console's spelling, which is what a plan written there contains.
            steps: vec![plans::Step {
                name: "Extract".into(),
                pipelines: vec!["pipelines/orders.json".into()],
                continue_on_failure: None,
            }],
        };
        plans::update(&ws, |list| list.push(plan)).unwrap();

        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            // Whether DuckDB is installed decides the run's STATUS, not where it is filed.
            let _ = sched.run_plan_now(&ws, "nightly").await.expect("the plan should run");
        });

        assert!(
            ws.join("runs").join("orders.json").exists(),
            "no history at runs/orders.json; what was written: {:?}",
            walk(&ws.join("runs"))
        );
        assert!(
            !ws.join("runs").join("pipelines").exists(),
            "the raw step was used as the history key, so these runs are invisible"
        );
    }

    /// Every file under a directory, for an assertion that needs to say what it found.
    fn walk(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&d) else { continue };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(rel) = p.strip_prefix(dir) {
                    out.push(rel.to_string_lossy().into_owned());
                }
            }
        }
        out
    }

    /// A plan schedule locks nothing up front, because it is not one pipeline. Each pipeline
    /// takes its own lock as it comes up. Locking `pipeline_id` here would guard a file the
    /// plan never touches while leaving the ones it does touch open to a second process.
    #[test]
    fn a_plan_schedule_does_not_lock_a_pipeline_it_will_not_run() {
        assert_eq!(lock_key(&plan_schedule("nightly")), None);
        let mut plain = plan_schedule("nightly");
        plain.plan_id = None;
        assert_eq!(lock_key(&plain), Some("not-the-plan"));
    }
}
