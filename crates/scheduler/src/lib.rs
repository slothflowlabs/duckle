//! Duckle scheduler.
//!
//! Cron- and interval-based triggers for pipelines. Schedules are
//! persisted to `<workspace>/schedules.json` so they survive restarts.
//! A single tokio task wakes every 15 seconds, decides which schedules
//! are due, and fires each as a non-blocking spawn that calls into the
//! shared `DuckdbEngine`.

use chrono::{DateTime, Utc};
use duckle_duckdb_engine::{
    plans, runlock, schedules, DuckdbEngine, RunRecord, RunResult,
};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, Debouncer};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
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
    /// #296: the schedules known to have no occurrence waiting for a run in their
    /// way. A waiting occurrence is kept in the workspace's ledger, so it outlives
    /// the app; this spares reading that ledger for each schedule on every tick.
    idle: HashSet<String>,
    /// #296: the cancel handle of each run a schedule started that is still going,
    /// by schedule id, with its pipeline, for `overlap: replace`.
    scheduled_runs: HashMap<String, (String, DuckdbEngine)>,
}

/// Holds a scheduled run's cancel handle in the scheduler while it runs, and
/// removes it however the run ends.
struct ScheduledRun {
    inner: Arc<Mutex<SchedulerInner>>,
    id: String,
}

impl ScheduledRun {
    fn hold(inner: &Arc<Mutex<SchedulerInner>>, id: &str, pipeline_id: &str, engine: &DuckdbEngine) -> Self {
        if let Ok(mut g) = inner.lock() {
            g.scheduled_runs.insert(id.to_string(), (pipeline_id.to_string(), engine.clone()));
        }
        Self { inner: Arc::clone(inner), id: id.to_string() }
    }
}

impl Drop for ScheduledRun {
    fn drop(&mut self) {
        if let Ok(mut g) = self.inner.lock() {
            g.scheduled_runs.remove(&self.id);
        }
    }
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
                // #317: what the schedule binds is what its one pipeline is given.
                params: s
                    .params
                    .clone()
                    .map(|values| [(s.pipeline_id.clone(), values)].into_iter().collect()),
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
    values: &std::collections::BTreeMap<String, String>,
) -> Result<RunResult, String> {
    // Normalised because a plan step may name a pipeline the console's way, as a
    // workspace-relative file. `resolve_workspace` takes a bare id and builds the path
    // itself, so an un-normalised step asked it for `pipelines/pipelines/orders.json.json`.
    // A bare id normalises to itself, so an ordinary schedule is unaffected.
    // #317: held to its declared parameter contract like any other run, with the
    // values its plan step binds for it - named as the step's, so the run's
    // provenance record says where they came from.
    let supplied: Vec<duckle_duckdb_engine::params::Supplied> = values
        .iter()
        .map(|(name, value)| duckle_duckdb_engine::params::Supplied {
            name: name.clone(),
            value: value.clone(),
            source: "plan step".to_string(),
        })
        .collect();
    let mut pipeline = duckle_duckdb_engine::context::resolve_workspace_with_params(
        workspace,
        plans::step_pipeline_id(pipeline_id),
        None,
        &supplied,
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
    // #259: same id in the log as in the receipt.
    let result = engine
        .for_new_run()
        .with_run_id(&receipt.run_id)
        .execute_pipeline_named(pipeline, pipeline_id);
    duckle_duckdb_engine::retry::finish(
        workspace,
        receipt,
        &result.status,
        duckle_duckdb_engine::retry::nodes_of(&result),
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
        link_previews: Vec::new(),
        stopped_at: None,
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
                idle: HashSet::new(),
                scheduled_runs: HashMap::new(),
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
        // Another workspace, another ledger.
        g.idle.clear();
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
        schedules::validate(&schedule)?;
        if schedule.id.is_empty() {
            schedule.id = uuid::Uuid::new_v4().to_string();
        }
        compute_next_run(&mut schedule);
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let saved = schedule.clone();
        self.commit(&mut g, move |list| {
            schedules::merge_saved(list, saved)
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
    /// Run a schedule now, taking the run lock first.
    ///
    /// The public entry point, which is what "Run now" in the desktop app calls.
    /// It claims because a manual fire is a run of the pipeline like any other:
    /// pressing the button while this scheduler's own tick, `duckle-runner
    /// serve`'s scheduler, or a follower is running the same pipeline used to
    /// start a second one beside it, and two runs of one pipeline write the same
    /// sink and advance the same `xf.incremental` watermark.
    ///
    /// The claim lives HERE rather than inside the body because the scheduled
    /// paths take it before they call through and hold it for the duration - the
    /// lock is the operating system's and is not reentrant, so claiming it again
    /// underneath them would refuse every scheduled run. Which way round it goes
    /// is therefore load-bearing: the unlocked body is private, so a new caller
    /// gets the safe one by default.
    pub async fn run_now(&self, id: &str) -> Result<RunResult, String> {
        let (workspace, key) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            let s = g
                .schedules
                .iter()
                .find(|s| s.id == id)
                .ok_or_else(|| "Schedule not found".to_string())?;
            (g.workspace_path.clone(), lock_key(s).map(str::to_string))
        };
        // No key means a plan, which locks each of its pipelines itself as it
        // reaches them. See `lock_key`.
        let _claim = match &key {
            None => None,
            Some(pipeline_id) => match claim_run(workspace.as_deref(), pipeline_id) {
                Claim::Ours(lock) => lock,
                Claim::Taken => {
                    return Err(format!(
                        "{pipeline_id} is already running in this workspace, so this run was \
                         refused rather than started beside it. Two runs of one pipeline write \
                         the same sink and advance the same saved state. Wait for it to finish, \
                         or run it somewhere else."
                    ))
                }
                Claim::Unusable(why) => {
                    return Err(format!("cannot take a run lock for {pipeline_id}: {why}"))
                }
            },
        };
        self.run_now_claimed(id, false).await
    }

    /// The body of [`run_now`](Self::run_now), for a caller that already holds
    /// the run lock. Private: see the note there on why the order matters.
    async fn run_now_claimed(&self, id: &str, scheduled: bool) -> Result<RunResult, String> {
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
        //
        // #317: the schedule's parameter values, checked against the pipeline's
        // declared contract before any context, as the server's scheduler and
        // every other surface do. This scheduler applied none of it, so a declared
        // default stayed a literal placeholder and a required one went unasked.
        let supplied: Vec<duckle_duckdb_engine::params::Supplied> = sched
            .params
            .iter()
            .flatten()
            .map(|(name, value)| duckle_duckdb_engine::params::Supplied {
                name: name.clone(),
                value: value.clone(),
                source: "schedule".to_string(),
            })
            .collect();
        let mut pipeline = duckle_duckdb_engine::context::resolve_workspace_with_params(
            &workspace,
            &pipeline_id,
            None,
            &supplied,
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
        // #296: a scheduled run's cancel handle, for `overlap: replace`. Never a run
        // a person started from the dialog: a schedule coming due does not end one.
        let _registered = scheduled.then(|| ScheduledRun::hold(&self.inner, id, &pipeline_id, &engine));
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
            duckle_duckdb_engine::retry::nodes_of(&result),
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
            link_previews: Vec::new(),
            stopped_at: None,
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
            plans::execute(&plan, |step, values| {
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
                let result = run_one_blocking(&engine, &ws, pipeline, values);
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
                duckle_duckdb_engine::record_run(
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
        // The caller already holds the run lock for this pipeline and the
        // lock is not reentrant, so this takes the body rather than the public
        // entry point.
        let Err(e) = self.run_now_claimed(id, true).await else {
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
            link_previews: Vec::new(),
            stopped_at: None,
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
            duckle_duckdb_engine::record_run(&path, &pid, record);
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
                                // #296: the same overlap policy as a timed fire.
                                Claim::Taken if me2.overlapped(&id, key, Utc::now()) => None,
                                Claim::Taken => return,
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

    /// #296: an occurrence came due while its pipeline is running. Do what the
    /// schedule's overlap policy says, decided by the function `duckle serve`
    /// decides with; true when it should run anyway.
    fn overlapped(&self, id: &str, pipeline_id: &str, due: DateTime<Utc>) -> bool {
        use duckle_duckdb_engine::occurrences::{self, Decision, OnOverlap};
        let (policy, ws) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            let Some(policy) = g.schedules.iter().find(|s| s.id == id).map(|s| s.overlap) else {
                return false;
            };
            (policy, g.workspace_path.clone())
        };
        // A run is only ever in the way on a workspace: without one nothing is locked.
        let Some(ws) = ws else { return false };
        match occurrences::on_overlap(policy, &occurrences::waiting(&ws, id)) {
            OnOverlap::Run => true,
            OnOverlap::Skip(reason) => {
                warn!("Schedule {} ({}): {}", id, pipeline_id, reason);
                false
            }
            OnOverlap::Wait { reason, stop_running, supersede } => {
                warn!("Schedule {} ({}): {}", id, pipeline_id, reason);
                let decided_at = Utc::now().to_rfc3339();
                for older in &supersede {
                    let replaced = Decision::Skipped { reason: "replaced by a newer occurrence".into() };
                    if let Err(e) = occurrences::record(&ws, &occurrences::decided(older, replaced, &decided_at)) {
                        warn!("Schedule {}: the occurrence it replaced was not recorded: {}", id, e);
                    }
                }
                let at = duckle_duckdb_engine::cronzone::Occurrence {
                    at: due,
                    local: String::new(),
                    offset_seconds: 0,
                    adjustment: None,
                };
                let queued = occurrences::entry(id, &at, None, Decision::Queued { reason }, None, &decided_at);
                // No longer idle from before the entry is written. A tick that
                // sees it waiting while it is still marked idle passes it over,
                // and a newer occurrence takes the run it was waiting for. Cleared
                // again below, for a tick that read the ledger empty and marked
                // it idle while the entry was being written.
                self.inner.lock().expect("scheduler poisoned").idle.remove(id);
                // In the ledger rather than in memory, so it outlives the app.
                if let Err(e) = occurrences::record(&ws, &queued) {
                    warn!("Schedule {}: the occurrence left waiting was not recorded: {}", id, e);
                }
                let mut g = self.inner.lock().expect("scheduler poisoned");
                g.idle.remove(id);
                if stop_running {
                    for (pipeline, run) in g.scheduled_runs.values() {
                        if pipeline == pipeline_id {
                            run.request_cancel();
                        }
                    }
                }
                false
            }
        }
    }

    /// #296: start the occurrence a schedule has waiting for a run that was in its
    /// way, once that run has ended, as `duckle serve` starts it.
    ///
    /// Read from the workspace's ledger, so an occurrence left waiting when the
    /// app closed still runs when it starts again. `idle` holds the schedules known
    /// to have none, so the ledger is read for each once after a start, and after
    /// that only when one is queued.
    ///
    /// The pipeline is taken before the occurrence is recorded as fired, and held
    /// until its run ends. An occurrence due in the same tick then finds this run
    /// in its way and waits behind it, rather than taking the pipeline from one
    /// already recorded as fired.
    fn start_waiting(&self, now: DateTime<Utc>) {
        use duckle_duckdb_engine::occurrences::{self, Decision};
        let (ws, candidates) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            let Some(ws) = g.workspace_path.clone() else { return };
            let candidates: Vec<(String, String)> = g
                .schedules
                .iter()
                .filter(|s| s.enabled && !g.idle.contains(&s.id))
                .filter_map(|s| Some((s.id.clone(), lock_key(s)?.to_string())))
                .collect();
            (ws, candidates)
        };
        for (id, pipeline_id) in candidates {
            if occurrences::waiting(&ws, &id).is_empty() {
                self.inner.lock().expect("scheduler poisoned").idle.insert(id);
                continue;
            }
            let lock = match claim_run(Some(&ws), &pipeline_id) {
                Claim::Ours(lock) => lock,
                // Still going: the next tick asks again, and nothing is recorded meanwhile.
                Claim::Taken => continue,
                Claim::Unusable(why) => {
                    warn!(
                        "Cannot take a run lock for {} in this workspace, so the occurrence \
                         schedule {} has waiting cannot start: {}",
                        pipeline_id, id, why
                    );
                    continue;
                }
            };
            // Again, now the pipeline is held: another scheduler on the workspace may
            // have started it meanwhile.
            let Some(next) = occurrences::waiting(&ws, &id).into_iter().next() else {
                self.inner.lock().expect("scheduler poisoned").idle.insert(id);
                continue;
            };
            let fired = occurrences::decided(&next, Decision::Fired, &now.to_rfc3339());
            if let Err(e) = occurrences::record(&ws, &fired) {
                warn!("Schedule {}: the occurrence that waited was not recorded: {}", id, e);
            }
            let (me, permit) = (self.clone(), pool_permits(&ws, &pool_of(&ws, &pipeline_id)));
            tokio::spawn(async move {
                let _lock = lock;
                let _slot = permit.acquire_owned().await;
                me.fire_and_record(&id, "Scheduled").await;
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
    /// schedule is what came due but the pipeline is what gets locked, and the
    /// instant it came due.
    fn claim_due(&self, now: DateTime<Utc>) -> Vec<(String, Option<String>, DateTime<Utc>)> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let due: Vec<(String, Option<String>, DateTime<Utc>)> = g
            .schedules
            .iter()
            .filter(|s| s.enabled)
            .filter_map(|s| {
                let at = s.next_run_at.filter(|t| *t <= now)?;
                Some((s.id.clone(), lock_key(s).map(str::to_string), at))
            })
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
            let claimed: Vec<String> = due.iter().map(|(id, ..)| id.clone()).collect();
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
                    if due.iter().any(|(id, ..)| id == &s.id) {
                        claim_next_run(s, now);
                    }
                }
            }
        }
        // Advanced whoever takes it, so a process that loses does not ask again next tick.
        // What is left is what this process runs. Another scheduler on the workspace, a
        // second desktop or a server, decides from its own copy, so the decision above
        // cannot settle it, and this does. With no workspace there is nothing to share.
        let workspace = g.workspace_path.clone();
        due.into_iter()
            .filter(|(id, _, at)| {
                workspace.as_deref().is_none_or(|ws| {
                    duckle_duckdb_engine::occurrences::claim(ws, id, *at).unwrap_or_else(|e| {
                        warn!("Could not claim the occurrence of {}, firing anyway: {}", id, e);
                        true
                    })
                })
            })
            .map(|(id, key, at)| (id, key, at))
            .collect()
    }

    async fn fire_due(&self) {
        let now = Utc::now();
        // Read the workspace under the same lock as the due list, so the path
        // used for the run lock is the one this tick actually decided against.
        let workspace = { self.inner.lock().expect("scheduler poisoned").workspace_path.clone() };
        // #296: what came due while nothing was ticking, decided before this tick
        // moves on the occurrences it has armed.
        self.catch_up(now);
        // #296: what waited for a run in its way goes first: it is older than
        // anything due now.
        self.start_waiting(now);
        for (id, pipeline_id, at) in self.claim_due(now) {
            let me = self.clone();
            let workspace = workspace.clone();
            // #289: the pool this pipeline asks for, resolved against what the
            // workspace (and any server ceiling) actually defines.
            // A scheduler with no workspace path cannot read a pipeline to
            // find its pool, so it stays on the default - which is exactly the
            // behaviour it had before pools existed.
            // #289: a Plan takes NO workload slot.
            //
            // Plan orchestration is a supervisor: it does almost nothing itself
            // and spends its time waiting for children, each of which acquires
            // the pool its own pipeline asks for. Holding a slot while waiting
            // for a child that needs the same pool is a deadlock the size of
            // the pool - with `heavy: 1`, a Plan whose first step is a heavy
            // pipeline would wait forever for the permit it is itself holding.
            //
            // A scheduler with no workspace path cannot read a pipeline to find
            // its pool, so it stays on the default - exactly the behaviour it
            // had before pools existed.
            let permit = match (workspace.as_deref(), pipeline_id.as_deref()) {
                (Some(ws), Some(id)) => Some(pool_permits(ws, &pool_of(ws, id))),
                // No pipeline id means a Plan.
                (_, None) => None,
                _ => Some(run_permits().clone()),
            };
            tokio::spawn(async move {
                // Hold a permit for the whole run. Every schedule that comes due
                // in the same tick used to fire at once, so ten due at midnight
                // meant ten pipelines each sized for the whole machine. The
                // permit bounds that; the run still happens, it just queues.
                let _slot = match permit {
                    Some(p) => Some(p.acquire_owned().await),
                    None => None,
                };
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
                        // #296: what the schedule's overlap policy makes of it.
                        Claim::Taken if me.overlapped(&id, pipeline_id, at) => None,
                        Claim::Taken => return,
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

    /// #296: what the cron schedules missed while nothing was ticking, run or
    /// recorded as each schedule's misfire policy says: the app closed, or the
    /// machine asleep past more than the one occurrence it was waiting for.
    ///
    /// Decided by the functions `duckle serve` decides with, and recorded in the
    /// same ledger. The desktop used to run at most the occurrence it had armed
    /// and nothing else, whatever the policy said, and on a fresh start not even
    /// that.
    ///
    /// Measured from the newest occurrence any scheduler on the workspace took,
    /// rather than from the ledger as serve measures: the desktop records only
    /// what it decides about missed occurrences, because a record per run is a
    /// line a minute for a minutely schedule, in a file a catch-up reads whole.
    fn catch_up(&self, now: DateTime<Utc>) {
        use duckle_duckdb_engine::{cronzone, occurrences};
        let (workspace, list) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            (g.workspace_path.clone(), g.schedules.clone())
        };
        // What was taken is kept in the workspace: with none there is nothing to measure from.
        let Some(ws) = workspace else { return };
        for s in list.into_iter().filter(|s| s.enabled) {
            let ScheduleKind::Cron { expr } = &s.kind else { continue };
            // Never taken: a schedule seen for the first time arms, it does not
            // catch up, or a new schedule would run its whole history.
            let Some(after) = occurrences::claimed_through(&ws, &s.id) else { continue };
            // An unusable zone is reported where its next run is worked out.
            let Ok(zone) = cronzone::resolve_zone(s.timezone.as_deref()) else { continue };
            // The occurrence armed now is this tick's to fire, however late it is.
            let horizon = occurrences::catch_up_horizon(now, tick_interval(), s.next_run_at);
            let missed = match occurrences::missed(expr, &zone, &s.exclude, after, horizon, &s.catchup) {
                Ok(missed) => missed,
                Err(e) => {
                    warn!("Schedule {}: could not work out what it missed: {}", s.id, e);
                    continue;
                }
            };
            let decided_at = now.to_rfc3339();
            let mut to_run = Vec::new();
            for (occ, decision) in occurrences::decide(&missed, s.misfire) {
                if decision == occurrences::Decision::Fired {
                    // Recorded by the catch-up run, immediately before its own run.
                    to_run.push(occ);
                    continue;
                }
                if !take_occurrence(&ws, &s.id, occ.at) {
                    continue;
                }
                let entry = occurrences::entry(&s.id, &occ, s.timezone.as_deref(), decision, None, &decided_at);
                if let Err(e) = occurrences::record(&ws, &entry) {
                    warn!("Schedule {}: a missed occurrence was not recorded: {}", s.id, e);
                }
            }
            if !to_run.is_empty() {
                self.run_missed(&ws, s, to_run);
            }
        }
    }

    /// #296: run the missed occurrences a catch-up decided, one after another on
    /// one task, oldest first. That is what `all` means: each of them, in order,
    /// rather than whichever won a race for the pipeline.
    fn run_missed(&self, ws: &Path, s: Schedule, to_run: Vec<duckle_duckdb_engine::cronzone::Occurrence>) {
        use duckle_duckdb_engine::occurrences;
        // One scheduler catches a schedule up at a time, across processes, under
        // the name `duckle serve` takes the same turn under. Held, it is the one
        // already doing it, here or elsewhere, and the next tick asks again.
        let Some(turn) = runlock::try_acquire_nested(ws, "catch-up", &s.id) else { return };
        let (me, ws) = (self.clone(), ws.to_path_buf());
        tokio::spawn(async move {
            let _turn = turn;
            let key = lock_key(&s).map(str::to_string);
            for occ in to_run {
                // The permit a run of this pipeline takes, as `fire_due` takes it.
                let _slot = match &key {
                    Some(pipeline_id) => Some(pool_permits(&ws, &pool_of(&ws, pipeline_id)).acquire_owned().await),
                    None => None,
                };
                // The pipeline first, the occurrence second. A claimed occurrence can
                // never be taken again, so one claimed while its pipeline was busy
                // would be lost rather than left to the next tick, which finds
                // these still missed and starts again from the oldest.
                let _claim = match &key {
                    None => None,
                    Some(pipeline_id) => match claim_run(Some(&ws), pipeline_id) {
                        Claim::Ours(lock) => lock,
                        Claim::Taken => return,
                        Claim::Unusable(why) => {
                            warn!(
                                "Cannot take a run lock for {} in this workspace, so schedule {} \
                                 cannot catch up: {}",
                                pipeline_id, s.id, why
                            );
                            return;
                        }
                    },
                };
                if !take_occurrence(&ws, &s.id, occ.at) {
                    continue;
                }
                let decided_at = Utc::now().to_rfc3339();
                let entry = occurrences::entry(
                    &s.id,
                    &occ,
                    s.timezone.as_deref(),
                    occurrences::Decision::Fired,
                    None,
                    &decided_at,
                );
                if let Err(e) = occurrences::record(&ws, &entry) {
                    warn!("Schedule {}: the occurrence it caught up was not recorded: {}", s.id, e);
                }
                me.fire_and_record(&s.id, "Catch-up").await;
            }
        });
    }
}

/// Take one occurrence for this process, or leave it to whichever got there
/// first. A claim that cannot be made at all goes ahead, as `claim_due` does:
/// bookkeeping that failed must not quietly stop a schedule.
fn take_occurrence(workspace: &Path, id: &str, at: DateTime<Utc>) -> bool {
    duckle_duckdb_engine::occurrences::claim(workspace, id, at).unwrap_or_else(|e| {
        warn!("Could not claim the occurrence of {}, deciding it anyway: {}", id, e);
        true
    })
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

/// #289: the semaphore for one named pool.
///
/// The numbers come from [`duckle_duckdb_engine::pools`], the same definition
/// the runner's gate reads. Two limiters that each parsed their own config is
/// how the two schedulers came to disagree about time zones, and a pool that
/// means 8 here and 1 there is worth less than no pool at all.
///
/// The default pool keeps [`run_permits`] - a generous 8 rather than 1 -
/// because firing due schedules concurrently is long-standing behaviour here
/// and changing it silently would be a regression dressed as a feature.
fn pool_permits(workspace: &std::path::Path, pool: &str) -> std::sync::Arc<tokio::sync::Semaphore> {
    use std::collections::HashMap;
    if pool == duckle_duckdb_engine::pools::DEFAULT {
        return run_permits().clone();
    }
    static POOLS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<String, std::sync::Arc<tokio::sync::Semaphore>>>,
    > = std::sync::OnceLock::new();
    let map = POOLS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut map = map.lock().unwrap_or_else(|p| p.into_inner());
    map.entry(pool.to_string())
        .or_insert_with(|| {
            let n = duckle_duckdb_engine::pools::Pools::load(workspace).limit(pool);
            std::sync::Arc::new(tokio::sync::Semaphore::new(n))
        })
        .clone()
}

/// Which pool a scheduled pipeline belongs to.
///
/// Reads the document, because the pool is a property of the pipeline rather
/// than of the schedule - a pipeline moved between pools should not need every
/// schedule that fires it edited too.
fn pool_of(workspace: &std::path::Path, pipeline_id: &str) -> String {
    let pools = duckle_duckdb_engine::pools::Pools::load(workspace);
    let path = workspace.join("pipelines").join(format!("{pipeline_id}.json"));
    let asked = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .map(|v| duckle_duckdb_engine::pools::requested(&v))
        .unwrap_or_default();
    pools.resolve(&asked)
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
            Some(duckle_duckdb_engine::occurrences::next_interval_slot(now, *seconds))
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
            use duckle_duckdb_engine::occurrences::{interval_slot, next_interval_slot};
            let now = Utc::now();
            let next = next_interval_slot(s.last_run_at.unwrap_or(now), *seconds);
            // Overdue: the newest instant that has passed, so it fires once now rather
            // than once per instant it slept through. Still on the grid, because that
            // instant is what every scheduler has to agree on.
            Some(if next <= now { interval_slot(now, *seconds) } else { next })
        }
        // Event-driven - no scheduled next-run time.
        ScheduleKind::FileWatch { .. } => None,
    };
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        let validate = schedules::validate;
        assert!(validate(&s).is_ok(), "{:?}", validate(&s));
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        let validate = schedules::validate;
        let e = validate(&s).unwrap_err();
        assert!(e.contains("Europe/Brussel"), "must name the typo: {e}");
        s.timezone = Some("Europe/Brussels".into());
        assert!(validate(&s).is_ok(), "the real zone must be accepted");
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("next_run_at set");
        // On the grid, so every scheduler on a workspace names the same instant.
        assert_eq!(next.timestamp() % 300, 0, "{next} is off the grid");
        let delta = next - Utc::now();
        assert!(delta.num_seconds() <= 300 && delta.num_seconds() >= -1, "{delta}");
    }

    #[test]
    fn an_overdue_interval_fires_once_for_all_the_instants_it_slept_through() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            plan_id: None,
            name: "every 5".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 300 },
            timezone: None,
            exclude: Default::default(),
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
            last_run_at: Some(Utc::now() - chrono::Duration::hours(3)),
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("next_run_at set");
        assert!(next <= Utc::now(), "an overdue schedule must be due now: {next}");
        assert_eq!(next.timestamp() % 300, 0, "{next} is off the grid");
        assert!(Utc::now() - next < chrono::Duration::seconds(301), "it is due for the newest instant");
    }

    /// The bug this stops: each process decides from its own copy of the store, so two of
    /// them on one workspace both found the schedule due and both ran it.
    #[test]
    fn two_schedulers_on_one_workspace_run_an_occurrence_once() {
        let dir = tempfile::tempdir().unwrap();
        let first = Scheduler::new(DuckdbEngine::new("duckdb".into()));
        first.set_workspace(Some(dir.path().to_path_buf()));
        let id = first
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "p1".into(),
                plan_id: None,
                name: "every 5".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 300 },
                timezone: None,
                exclude: Default::default(),
                misfire: Default::default(),
                catchup: Default::default(),
                overlap: Default::default(),
                params: None,
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .unwrap()
            .id;
        let second = Scheduler::new(DuckdbEngine::new("duckdb".into()));
        second.set_workspace(Some(dir.path().to_path_buf()));
        // Due in each process's OWN copy, the way five minutes passing makes it. That copy
        // is the race: neither hears what the other has decided until it reads the store.
        let due = Utc::now() - chrono::Duration::seconds(1);
        for sched in [&first, &second] {
            for s in sched.inner.lock().unwrap().schedules.iter_mut() {
                s.next_run_at = Some(due);
            }
        }
        let now = Utc::now();
        let won = first.claim_due(now).len() + second.claim_due(now).len();
        assert_eq!(won, 1, "{id} ran on both schedulers");
        // The loser moved on too, or it would ask again on every tick.
        let after = second.list().unwrap().into_iter().find(|s| s.id == id).unwrap();
        assert!(matches!(after.next_run_at, Some(t) if t > now), "{:?}", after.next_run_at);
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
                misfire: Default::default(),
                catchup: Default::default(),
                overlap: Default::default(),
                params: None,
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
                misfire: Default::default(),
                catchup: Default::default(),
                overlap: Default::default(),
                params: None,
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
                misfire: Default::default(),
                catchup: Default::default(),
                overlap: Default::default(),
                params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
                misfire: Default::default(),
                catchup: Default::default(),
                overlap: Default::default(),
                params: None,
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
            misfire: Default::default(),
            catchup: Default::default(),
            overlap: Default::default(),
            params: None,
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
                    params: None,
                },
                plans::Step { name: "Publish".into(), pipelines: vec!["export.json".into()], continue_on_failure: None, params: None },
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
            if let Err(e) = run_one_blocking(&engine, ws, spelling, &Default::default()) {
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
    const REGIONAL_PIPELINE: &str = r#"{"parameters":{"region":{"type":"string","enum":["eu","us"],"default":"eu"}},
 "nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"s","componentId":"src.csv","properties":{"path":"${workspace}/in.csv","hasHeader":true}}},
          {"id":"k","position":{"x":0,"y":0},"data":{"label":"k","componentId":"snk.csv","properties":{"path":"${workspace}/out/${region}.csv"}}}],
 "edges":[{"id":"e","source":"s","target":"k"}]}"#;

    /// #317: a plan step hands each pipeline its own values - the step's, not the
    /// pipeline's, since one pipeline can sit in two steps with different ones.
    #[test]
    fn a_plan_step_hands_each_pipeline_its_own_values() {
        let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists()) else {
            eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::create_dir_all(ws.join("out")).unwrap();
        std::fs::write(ws.join("in.csv"), "id\n1\n").unwrap();
        std::fs::write(ws.join("pipelines").join("regional.json"), REGIONAL_PIPELINE).unwrap();
        // The same pipeline in two steps: bound in the first, left to its default
        // in the second, so the values are the STEP's and not the pipeline's.
        plans::update(&ws, |list| {
            list.push(plans::Plan {
                id: "nightly".into(),
                name: String::new(),
                stop_on_failure: true,
                steps: vec![
                    plans::Step {
                        name: "Bound".into(),
                        pipelines: vec!["pipelines/regional.json".into()],
                        continue_on_failure: None,
                        params: Some(
                            [(
                                "pipelines/regional.json".to_string(),
                                [("region".to_string(), "us".to_string())].into_iter().collect(),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                    },
                    plans::Step {
                        name: "Default".into(),
                        pipelines: vec!["regional".into()],
                        continue_on_failure: None,
                        params: None,
                    },
                ],
            })
        })
        .unwrap();

        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from(bin)));
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let run = rt.block_on(sched.run_plan_now(&ws, "nightly")).expect("the plan runs");
        assert!(!run.failed(), "{run:?}");
        let out: Vec<String> = std::fs::read_dir(ws.join("out"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(out.contains(&"us.csv".to_string()), "the step's value did not reach the run: {out:?}");
        assert!(out.contains(&"eu.csv".to_string()), "a step without values still gets the default: {out:?}");
    }

    /// #317: a desktop-scheduled run is held to the pipeline's parameter contract,
    /// as the server's is: the schedule's values reach the run, and with none the
    /// declared default does. The desktop scheduler applied neither, so a default
    /// stayed a literal `${region}` in the file it wrote.
    #[test]
    fn a_scheduled_run_gets_its_parameter_values_and_the_declared_defaults() {
        let Some(bin) = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists()) else {
            eprintln!("skipping: set DUCKLE_DUCKDB_BIN");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::create_dir_all(ws.join("out")).unwrap();
        std::fs::write(ws.join("in.csv"), "id\n1\n").unwrap();
        std::fs::write(ws.join("pipelines").join("regional.json"), r#"{"parameters":{"region":{"type":"string","enum":["eu","us"],"default":"eu"}},
 "nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"s","componentId":"src.csv","properties":{"path":"${workspace}/in.csv","hasHeader":true}}},
          {"id":"k","position":{"x":0,"y":0},"data":{"label":"k","componentId":"snk.csv","properties":{"path":"${workspace}/out/${region}.csv"}}}],
 "edges":[{"id":"e","source":"s","target":"k"}]}"#).unwrap();

        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from(bin)));
        sched.set_workspace(Some(ws.clone()));
        let add = |params: Option<std::collections::BTreeMap<String, String>>| {
            sched
                .upsert(Schedule {
                    id: String::new(),
                    pipeline_id: "regional".into(),
                    plan_id: None,
                    name: "regional".into(),
                    enabled: true,
                    kind: ScheduleKind::Interval { seconds: 3600 },
                    timezone: None,
                    exclude: Default::default(),
                    misfire: Default::default(),
                    catchup: Default::default(),
                    overlap: Default::default(),
                    params,
                    last_run_at: None,
                    last_run_status: None,
                    last_run_duration_ms: None,
                    last_run_error: None,
                    next_run_at: None,
                })
                .expect("schedule rejected")
                .id
        };
        let bound = add(Some([("region".to_string(), "us".to_string())].into_iter().collect()));
        let unbound = add(None);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            sched.fire_and_record(&bound, "Test").await;
            sched.fire_and_record(&unbound, "Test").await;
        });
        let out: Vec<String> = std::fs::read_dir(ws.join("out"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(out.contains(&"us.csv".to_string()), "the schedule's value did not reach the run: {out:?}");
        assert!(out.contains(&"eu.csv".to_string()), "the declared default was not applied: {out:?}");
        assert!(!out.iter().any(|f| f.contains("${")), "a placeholder reached the file name: {out:?}");
    }

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
                params: None,
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

    // ---- #296: overlap, decided as `duckle serve` decides it ---------------------

    /// A scheduler on a workspace whose pipeline `p` takes `wait_ms`, with one
    /// interval schedule for it under `policy`. None without a DuckDB to run.
    fn overlapping(policy: &str, wait_ms: u64) -> Option<(tempfile::TempDir, PathBuf, Scheduler, String)> {
        let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok().filter(|b| Path::new(b).exists())?;
        let tmp = tempfile::tempdir().unwrap();
        // Not canonicalized: on Windows that adds a verbatim prefix, which this
        // scheduler hands to DuckDB as it is.
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(ws.join("in.csv"), "id\n1\n").unwrap();
        let path = |p: &Path| p.to_string_lossy().replace('\\', "/");
        let pipeline = serde_json::json!({
            "name": "p",
            "nodes": [
                { "id": "s", "position": {"x":0,"y":0}, "data": { "label": "in", "componentId": "src.csv",
                  "properties": { "path": path(&ws.join("in.csv")), "hasHeader": true } } },
                { "id": "w", "position": {"x":0,"y":0}, "data": { "label": "wait", "componentId": "ctl.wait",
                  "properties": { "duration": wait_ms, "unit": "ms" } } },
                { "id": "k", "position": {"x":0,"y":0}, "data": { "label": "out", "componentId": "snk.csv",
                  "properties": { "path": path(&ws.join("out.csv")), "hasHeader": true } } }
            ],
            "edges": [
                { "id": "e1", "source": "s", "target": "w", "data": { "connectionType": "main" } },
                { "id": "e2", "source": "w", "target": "k", "data": { "connectionType": "main" } }
            ]
        });
        std::fs::write(ws.join("pipelines").join("p.json"), pipeline.to_string()).unwrap();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from(bin)));
        sched.set_workspace(Some(ws.clone()));
        let id = sched
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "p".into(),
                plan_id: None,
                name: "p".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                timezone: None,
                exclude: Default::default(),
                misfire: Default::default(),
                catchup: Default::default(),
                overlap: serde_json::from_value(serde_json::json!(policy)).unwrap(),
                params: None,
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .unwrap()
            .id;
        Some((tmp, ws, sched, id))
    }

    /// Make the schedule due `ago` seconds back, the way time passing would.
    fn due_since(sched: &Scheduler, ago: i64) {
        let at = Utc::now() - chrono::Duration::seconds(ago);
        let mut g = sched.inner.lock().unwrap();
        sched.commit(&mut g, |list| list.iter_mut().for_each(|s| s.next_run_at = Some(at))).unwrap();
    }

    async fn eventually(what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what} never happened");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Each run's status, sorted. A failed run carries its error, so a test that
    /// expected `ok` says why it was not.
    fn statuses(ws: &Path) -> Vec<String> {
        let mut s: Vec<String> = duckle_duckdb_engine::history::load_run_history(ws, "p")
            .into_iter()
            .map(|r| match (r.status.as_str(), r.error) {
                ("error", Some(e)) => format!("error: {e}"),
                (status, _) => status.to_string(),
            })
            .collect();
        s.sort();
        s
    }

    /// Whether the schedule has an occurrence waiting, as the workspace's ledger
    /// says: that is where one is kept, so it outlives the app.
    fn waiting(sched: &Scheduler, id: &str) -> bool {
        let ws = sched.inner.lock().unwrap().workspace_path.clone().expect("a workspace");
        !duckle_duckdb_engine::occurrences::waiting(&ws, id).is_empty()
    }

    /// `queue`: the occurrence waits while another process runs the pipeline, then
    /// runs on the first tick after that run has ended.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_queued_occurrence_waits_for_the_run_in_its_way_then_runs() {
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the occurrence waiting", || waiting(&sched, &id)).await;
        sched.fire_due().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(statuses(&ws).is_empty(), "it ran beside the run in its way");
        assert!(waiting(&sched, &id), "a tick while the pipeline was busy dropped it");

        drop(other);
        sched.fire_due().await;
        eventually("the run that waited", || statuses(&ws).len() == 1).await;
        assert_eq!(statuses(&ws), vec!["ok"]);
        assert!(!waiting(&sched, &id));
    }

    /// `skip` keeps nothing back: what was turned away stays turned away.
    #[tokio::test(flavor = "multi_thread")]
    async fn skip_keeps_nothing_waiting() {
        let Some((_tmp, ws, sched, id)) = overlapping("skip", 0) else { return };
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 2);
        sched.fire_due().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(other);
        sched.fire_due().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!waiting(&sched, &id));
        assert!(statuses(&ws).is_empty(), "a skipped occurrence ran after all");
    }

    /// `allow` runs beside the run another process holds the pipeline for.
    #[tokio::test(flavor = "multi_thread")]
    async fn allow_runs_beside_the_run_in_its_way() {
        let Some((_tmp, ws, sched, _id)) = overlapping("allow", 0) else { return };
        let _other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the run alongside", || statuses(&ws).len() == 1).await;
        assert_eq!(statuses(&ws), vec!["ok"]);
    }

    /// `replace`: this scheduler's run is stopped and the new occurrence runs once
    /// it has.
    #[tokio::test(flavor = "multi_thread")]
    async fn replace_stops_the_scheduled_run_and_runs_the_new_occurrence() {
        let Some((_tmp, ws, sched, id)) = overlapping("replace", 3_000) else { return };
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the first run starting", || sched.inner.lock().unwrap().scheduled_runs.contains_key(&id)).await;

        let asked = std::time::Instant::now();
        due_since(&sched, 1);
        sched.fire_due().await;
        eventually("the first run ending", || statuses(&ws).len() == 1).await;
        assert!(asked.elapsed() < Duration::from_millis(2_500), "it ran out its wait: not stopped");

        eventually("the pipeline coming free", || !sched.inner.lock().unwrap().scheduled_runs.contains_key(&id)).await;
        // The stopped run gives its lock back a moment after it leaves that list,
        // and a tick that comes in between leaves the occurrence to the next one.
        tick_until(&sched, "the replacing run", || statuses(&ws).len() == 2).await;
        assert_eq!(statuses(&ws), vec!["cancelled", "ok"]);
    }

    /// `replace` does not end a run a person started from the dialog: the
    /// occurrence waits for it instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn replace_leaves_a_run_started_from_the_dialog_alone() {
        let Some((_tmp, ws, sched, id)) = overlapping("replace", 1_500) else { return };
        let person = {
            let (sched, id) = (sched.clone(), id.clone());
            tokio::spawn(async move { sched.run_now(&id).await })
        };
        // run_now takes the lock before the run starts, and the run writes its
        // receipt as it starts, so a receipt means the lock is held. Asking the
        // lock itself took it whenever it was free, and a person's run that came
        // for it in that moment was refused as already running: the test then
        // waited out its 90 s for a run that never was (Windows CI, 2da33b87).
        eventually("the person's run holding the pipeline", || {
            std::fs::read_dir(duckle_duckdb_engine::retry::dir(&ws))
                .map(|d| d.flatten().any(|e| e.path().extension().is_some_and(|x| x == "json")))
                .unwrap_or(false)
        })
        .await;
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the occurrence waiting", || waiting(&sched, &id)).await;
        let ran = person.await.unwrap().expect("the person's run");
        assert_eq!(ran.status, "ok", "a schedule stopped a run a person started");
    }

    /// A file-watch fire is decided by the same policy as a timed one. Sent the
    /// way a watcher sends it, without waiting on a real file event.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_watch_fire_follows_the_overlap_policy_too() {
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        let _other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        sched.spawn_ticker();
        sched.fire_tx.send(id.clone()).unwrap();
        eventually("the file-watch fire waiting", || waiting(&sched, &id)).await;
        assert!(statuses(&ws).is_empty(), "it ran beside the run in its way");
    }

    /// The occurrence that waits is recorded once, however many ticks it waits
    /// through, and again as fired when it runs. A record a tick would grow a
    /// file every scheduler on the workspace reads whole.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_waiting_occurrence_is_recorded_once_then_as_fired() {
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        let from = Utc::now() - chrono::Duration::minutes(10);
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the occurrence waiting", || waiting(&sched, &id)).await;
        for _ in 0..3 {
            sched.fire_due().await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let queued = decided(&ws, &id, from);
        assert_eq!(queued.iter().map(|(_, d)| d.as_str()).collect::<Vec<_>>(), ["queued"]);

        drop(other);
        tick_until(&sched, "the run that waited", || statuses(&ws).len() == 1).await;
        assert_eq!(decided(&ws, &id, from), vec![queued[0].clone(), (queued[0].0, "fired".to_string())]);
    }

    /// An occurrence left waiting when the app closed runs when it starts again.
    /// The scheduler that queued it is gone, and with it anything it held in memory.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_occurrence_left_waiting_when_the_app_closed_runs_when_it_starts_again() {
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the occurrence waiting", || waiting(&sched, &id)).await;
        drop(sched);
        drop(other);

        let bin = std::env::var("DUCKLE_DUCKDB_BIN").unwrap();
        let again = Scheduler::new(DuckdbEngine::new(PathBuf::from(bin)));
        again.set_workspace(Some(ws.clone()));
        tick_until(&again, "the occurrence that waited", || statuses(&ws).len() == 1).await;
        assert_eq!(statuses(&ws), vec!["ok"]);
        assert!(!waiting(&again, &id));
    }

    /// `replace`, with an occurrence already waiting: the newer one takes its
    /// place, and the one it replaced is recorded skipped rather than left to run.
    #[tokio::test(flavor = "multi_thread")]
    async fn replace_records_the_waiting_occurrence_it_takes_the_place_of_as_skipped() {
        let Some((_tmp, ws, sched, id)) = overlapping("replace", 0) else { return };
        let from = Utc::now() - chrono::Duration::minutes(10);
        // Another process's run, which `replace` cannot stop, so both have to wait.
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 120);
        sched.fire_due().await;
        eventually("the first occurrence waiting", || waiting(&sched, &id)).await;
        due_since(&sched, 60);
        sched.fire_due().await;
        eventually("the second taking its place", || decided(&ws, &id, from).len() == 3).await;
        let d = decided(&ws, &id, from);
        let (first, second) = (d[0].0, d[2].0);
        assert!(first < second);
        assert_eq!(d, vec![(first, "queued".into()), (first, "skipped".into()), (second, "queued".into())]);

        drop(other);
        tick_until(&sched, "the occurrence that took its place", || statuses(&ws).len() == 1).await;
        assert_eq!(decided(&ws, &id, from).last(), Some(&(second, "fired".to_string())));
        assert!(!waiting(&sched, &id), "the replaced occurrence is still waiting to run");
    }

    /// A disabled schedule starts nothing, an occurrence it left waiting included.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_disabled_schedule_does_not_start_the_occurrence_it_left_waiting() {
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 2);
        sched.fire_due().await;
        eventually("the occurrence waiting", || waiting(&sched, &id)).await;
        {
            let mut g = sched.inner.lock().unwrap();
            sched.commit(&mut g, |list| list.iter_mut().for_each(|s| s.enabled = false)).unwrap();
        }
        drop(other);
        for _ in 0..3 {
            sched.fire_due().await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(waiting(&sched, &id), "a disabled schedule started the occurrence that waited");
        assert!(statuses(&ws).is_empty(), "a disabled schedule ran");
    }

    /// An occurrence due in the tick that starts the one that waited finds that
    /// run in its way and waits behind it, rather than running beside a run the
    /// ledger already says was fired.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_occurrence_due_as_a_waiting_one_starts_waits_behind_it() {
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        let from = Utc::now() - chrono::Duration::minutes(10);
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        due_since(&sched, 120);
        sched.fire_due().await;
        eventually("the occurrence waiting", || waiting(&sched, &id)).await;
        drop(other);
        due_since(&sched, 60);
        sched.fire_due().await;
        eventually("the newer occurrence waiting", || decided(&ws, &id, from).len() == 3).await;
        let d = decided(&ws, &id, from);
        let (first, second) = (d[0].0, d[2].0);
        assert_eq!(d, vec![(first, "queued".into()), (first, "fired".into()), (second, "queued".into())]);

        tick_until(&sched, "both runs", || statuses(&ws).len() == 2).await;
        assert_eq!(statuses(&ws), vec!["ok", "ok"]);
    }

    /// Opening a workspace reads what waits in its ledger afresh: an occurrence
    /// another scheduler left there runs, though this one had found none before.
    #[tokio::test(flavor = "multi_thread")]
    async fn opening_a_workspace_reads_what_waits_in_its_ledger_afresh() {
        use duckle_duckdb_engine::occurrences::{self, Decision};
        let Some((_tmp, ws, sched, id)) = overlapping("queue", 0) else { return };
        sched.fire_due().await;
        let at = duckle_duckdb_engine::cronzone::Occurrence {
            at: Utc::now() - chrono::Duration::seconds(30),
            local: String::new(),
            offset_seconds: 0,
            adjustment: None,
        };
        let left = Decision::Queued { reason: "waiting for the previous run to end".into() };
        occurrences::record(&ws, &occurrences::entry(&id, &at, None, left, None, &Utc::now().to_rfc3339())).unwrap();

        sched.set_workspace(Some(ws.clone()));
        tick_until(&sched, "the occurrence left waiting", || statuses(&ws).len() == 1).await;
        assert!(!waiting(&sched, &id));
    }

    // ---- #296: misfire, decided as `duckle serve` decides it ---------------------

    /// A scheduler whose pipeline `p` missed three daily occurrences, as if the app
    /// last ran three days before the newest of them: that one is claimed, which is
    /// what a catch-up measures from. `overlapping`'s workspace, with its schedule
    /// made a daily cron at a UTC time two hours back, so the three are the same
    /// three whatever the clock says. Armed as the app arms it on start, at the next
    /// one. Returns the three, oldest first.
    fn missed_three(
        misfire: &str,
    ) -> Option<(tempfile::TempDir, PathBuf, Scheduler, String, Vec<DateTime<Utc>>)> {
        use chrono::Timelike;
        use duckle_duckdb_engine::occurrences;
        let (tmp, ws, sched, id) = overlapping("skip", 0)?;
        let newest = (Utc::now() - chrono::Duration::hours(2))
            .with_nanosecond(0)
            .and_then(|t| t.with_second(0))
            .unwrap();
        let expr = format!("{} {} * * *", newest.minute(), newest.hour());
        let policy: occurrences::Misfire = serde_json::from_value(serde_json::json!(misfire)).unwrap();
        {
            let mut g = sched.inner.lock().unwrap();
            sched
                .commit(&mut g, move |list| {
                    for s in list.iter_mut() {
                        s.kind = ScheduleKind::Cron { expr: expr.clone() };
                        s.timezone = Some("UTC".into());
                        s.misfire = policy;
                        // Recomputed by the commit, from now.
                        s.next_run_at = None;
                    }
                })
                .unwrap();
        }
        assert!(occurrences::claim(&ws, &id, newest - chrono::Duration::days(3)).unwrap());
        let missed = (0..3).rev().map(|d| newest - chrono::Duration::days(d)).collect();
        Some((tmp, ws, sched, id, missed))
    }

    /// What the ledger says of this schedule's occurrences from `from` on, in the
    /// order it was written.
    fn decided(ws: &Path, id: &str, from: DateTime<Utc>) -> Vec<(DateTime<Utc>, String)> {
        duckle_duckdb_engine::occurrences::read(ws)
            .into_iter()
            .filter(|o| o.schedule_id == id)
            .filter_map(|o| {
                let at = DateTime::parse_from_rfc3339(&o.scheduled_for).ok()?.with_timezone(&Utc);
                (at >= from).then(|| (at, o.decision.as_str().to_string()))
            })
            .collect()
    }

    /// Tick until `done`, the way the ticker would: a tick may leave work to the next.
    async fn tick_until(sched: &Scheduler, what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what} never happened");
            sched.fire_due().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn each(at: &[DateTime<Utc>], decision: &str) -> Vec<(DateTime<Utc>, String)> {
        at.iter().map(|t| (*t, decision.to_string())).collect()
    }

    /// `all`: each missed occurrence runs, oldest first, and is recorded as fired
    /// before its run. The desktop ran none of them: it armed from "now" on start,
    /// and an occurrence it had missed was never looked at again.
    #[tokio::test(flavor = "multi_thread")]
    async fn misfire_all_runs_each_missed_occurrence_oldest_first() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("all") else { return };
        tick_until(&sched, "every missed occurrence run", || statuses(&ws).len() == 3).await;
        assert_eq!(statuses(&ws), vec!["ok", "ok", "ok"]);
        assert_eq!(decided(&ws, &id, missed[0]), each(&missed, "fired"));
    }

    /// `latest`: only the newest runs, and the two before it are recorded as skipped.
    #[tokio::test(flavor = "multi_thread")]
    async fn misfire_latest_runs_only_the_newest_missed_occurrence() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("latest") else { return };
        tick_until(&sched, "the newest missed occurrence run", || statuses(&ws).len() == 1).await;
        sched.fire_due().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(statuses(&ws), vec!["ok"], "more than the newest ran");
        let mut want = each(&missed[..2], "skipped");
        want.push((missed[2], "fired".into()));
        assert_eq!(decided(&ws, &id, missed[0]), want);
    }

    /// `skip`, the default: none of them runs, and each is recorded as skipped, so
    /// "it was skipped" is not the same silence as "nothing happened".
    #[tokio::test(flavor = "multi_thread")]
    async fn misfire_skip_records_each_missed_occurrence_and_runs_none() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("skip") else { return };
        // Twice: the next tick must not decide them again.
        for _ in 0..2 {
            sched.fire_due().await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(statuses(&ws).is_empty(), "a missed occurrence ran under skip: {:?}", statuses(&ws));
        assert_eq!(decided(&ws, &id, missed[0]), each(&missed, "skipped"));
    }

    /// The occurrence the app was waiting for when the machine went to sleep is still
    /// the tick's to fire when it wakes, as it is for `duckle serve`, and misfire
    /// decides the ones after it. Decided as missed, it would be claimed as skipped
    /// and the run it was armed for refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_occurrence_armed_before_a_sleep_runs_and_misfire_decides_the_rest() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("skip") else { return };
        let armed = missed[0];
        {
            let mut g = sched.inner.lock().unwrap();
            sched.commit(&mut g, move |list| list.iter_mut().for_each(|s| s.next_run_at = Some(armed))).unwrap();
        }
        tick_until(&sched, "the armed occurrence run and the rest decided", || {
            statuses(&ws).len() == 1 && decided(&ws, &id, missed[0]).len() == 2
        })
        .await;
        assert_eq!(statuses(&ws), vec!["ok"]);
        // The armed one ran as any occurrence runs, unrecorded; the two after it were missed.
        assert_eq!(decided(&ws, &id, missed[0]), each(&missed[1..], "skipped"));
    }

    /// A catch-up that finds its pipeline running leaves the missed occurrences to a
    /// later tick, claiming none: an occurrence once claimed can never be taken
    /// again, so one claimed while the pipeline was busy would be lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_catch_up_leaves_the_missed_occurrences_while_the_pipeline_is_running() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("all") else { return };
        let other = duckle_duckdb_engine::runlock::try_acquire(&ws, "p").expect("free to take");
        for _ in 0..2 {
            sched.fire_due().await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(statuses(&ws).is_empty(), "it ran beside the run in its way");
        assert_eq!(decided(&ws, &id, missed[0]), vec![], "decided while it could run none");
        drop(other);
        tick_until(&sched, "every missed occurrence run", || statuses(&ws).len() == 3).await;
        assert_eq!(decided(&ws, &id, missed[0]), each(&missed, "fired"));
    }

    /// A schedule no scheduler has taken an occurrence of is seen for the first time:
    /// it arms and catches nothing up, or a new schedule would run its whole history.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_schedule_never_taken_catches_nothing_up() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("all") else { return };
        std::fs::remove_dir_all(ws.join(".duckle").join("claims")).unwrap();
        sched.fire_due().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(decided(&ws, &id, missed[0] - chrono::Duration::days(45)), vec![]);
        assert!(statuses(&ws).is_empty());
    }

    /// A disabled schedule catches nothing up, as it fires nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_disabled_schedule_catches_nothing_up() {
        let Some((_tmp, ws, sched, id, missed)) = missed_three("all") else { return };
        {
            let mut g = sched.inner.lock().unwrap();
            sched.commit(&mut g, |list| list.iter_mut().for_each(|s| s.enabled = false)).unwrap();
        }
        sched.fire_due().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(decided(&ws, &id, missed[0]), vec![], "a disabled schedule caught up");
        assert!(statuses(&ws).is_empty());
    }
}
