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
    append_run_record, DuckdbEngine, RunRecord, RunResult,
};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, Debouncer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tracing::warn;

const SCHEDULES_FILE: &str = "schedules.json";
const TICK_INTERVAL: Duration = Duration::from_secs(15);
const WATCH_DEBOUNCE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Standard 5-field cron (minute hour day month weekday) or
    /// 6-field with seconds. Whatever the `cron` crate accepts.
    Cron { expr: String },
    /// Fire every N seconds since last run (or app start).
    Interval { seconds: u64 },
    /// Fire when a file or folder changes (debounced ~2s).
    FileWatch {
        path: String,
        #[serde(default)]
        recursive: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub pipeline_id: String,
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub kind: ScheduleKind,
    #[serde(default)]
    pub last_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_run_status: Option<String>,
    #[serde(default)]
    pub last_run_duration_ms: Option<u64>,
    #[serde(default)]
    pub last_run_error: Option<String>,
    #[serde(default)]
    pub next_run_at: Option<DateTime<Utc>>,
}

fn default_true() -> bool {
    true
}

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerInner>>,
    engine: DuckdbEngine,
    fire_tx: UnboundedSender<String>,
}

struct SchedulerInner {
    schedules: Vec<Schedule>,
    workspace_path: Option<PathBuf>,
    /// Active file-watchers, keyed by schedule id. Holding the
    /// `Debouncer` keeps the watch alive; dropping it stops watching.
    watchers: HashMap<String, Debouncer<RecommendedWatcher>>,
    /// Receiver for file-watch fires; taken by `spawn_ticker`.
    fire_rx: Option<UnboundedReceiver<String>>,
}

impl Scheduler {
    pub fn new(engine: DuckdbEngine) -> Self {
        let (fire_tx, fire_rx) = unbounded_channel();
        Self {
            inner: Arc::new(Mutex::new(SchedulerInner {
                schedules: Vec::new(),
                workspace_path: None,
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
        g.workspace_path = path.clone();
        g.schedules = match path {
            Some(p) => load_schedules(&p).unwrap_or_else(|e| {
                warn!("Failed to load schedules: {}", e);
                Vec::new()
            }),
            None => Vec::new(),
        };
        for s in g.schedules.iter_mut() {
            compute_next_run(s);
        }
        self.rebuild_watchers(&mut g);
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

    pub fn list(&self) -> Vec<Schedule> {
        self.inner
            .lock()
            .expect("scheduler poisoned")
            .schedules
            .clone()
    }

    pub fn upsert(&self, mut schedule: Schedule) -> Result<Schedule, String> {
        match &schedule.kind {
            ScheduleKind::Cron { expr } => {
                CronSchedule::from_str(expr)
                    .map_err(|e| format!("Invalid cron expression: {}", e))?;
            }
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
        if let Some(idx) = g.schedules.iter().position(|s| s.id == schedule.id) {
            // Upsert carries config only; preserve the existing run-history
            // fields so a partial payload doesn't wipe last_run_* to null.
            let prev = g.schedules[idx].clone();
            schedule.last_run_at = prev.last_run_at;
            schedule.last_run_status = prev.last_run_status;
            schedule.last_run_duration_ms = prev.last_run_duration_ms;
            schedule.last_run_error = prev.last_run_error;
            g.schedules[idx] = schedule.clone();
        } else {
            g.schedules.push(schedule.clone());
        }
        if let Some(path) = g.workspace_path.clone() {
            let _ = save_schedules(&path, &g.schedules);
        }
        self.rebuild_watchers(&mut g);
        Ok(schedule)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        g.schedules.retain(|s| s.id != id);
        g.watchers.remove(id);
        if let Some(path) = g.workspace_path.clone() {
            let _ = save_schedules(&path, &g.schedules);
        }
        Ok(())
    }

    /// Execute a schedule's pipeline right now, regardless of its
    /// timing. Updates last-run bookkeeping on completion.
    pub async fn run_now(&self, id: &str) -> Result<RunResult, String> {
        let (workspace, pipeline_id) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            let s = g
                .schedules
                .iter()
                .find(|s| s.id == id)
                .ok_or_else(|| "Schedule not found".to_string())?;
            (g.workspace_path.clone(), s.pipeline_id.clone())
        };
        let workspace =
            workspace.ok_or_else(|| "No workspace set for the scheduler".to_string())?;
        // Resolve workspace context exactly like the canvas and the runner do:
        // substitute ${var} / ${context.var} (e.g. a context-based DB password),
        // inline SQL routines, and rewrite child-pipeline refs. Without this a
        // scheduled run sent the raw ${context.X} placeholder to the driver, so
        // a pipeline that ran fine from the canvas failed under a schedule with
        // auth errors like ORA-01017 (issue #32).
        let pipeline = duckle_duckdb_engine::context::resolve_workspace(
            &workspace,
            &pipeline_id,
            None,
        )?
        .doc;
        // A fresh per-run cancel scope so concurrent scheduled runs (and the
        // interactive run) don't share or reset each other's cancellation.
        let engine = self.engine.for_new_run();
        let started = Utc::now();
        // Log scheduled runs under the pipeline id (the scheduler has no
        // friendly name handy) so they still land in the per-pipeline log.
        let log_name = pipeline_id.clone();
        let result =
            tokio::task::spawn_blocking(move || engine.execute_pipeline_named(&pipeline, &log_name))
                .await
                .map_err(|e| e.to_string())?;
        self.record_run(id, started, &result);
        Ok(result)
    }

    fn record_run(&self, id: &str, started: DateTime<Utc>, result: &RunResult) {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let mut pipeline_id = None;
        if let Some(s) = g.schedules.iter_mut().find(|s| s.id == id) {
            s.last_run_at = Some(started);
            s.last_run_status = Some(result.status.clone());
            s.last_run_duration_ms = Some(result.duration_ms);
            s.last_run_error = result.error.clone();
            pipeline_id = Some(s.pipeline_id.clone());
            compute_next_run(s);
        }
        if let Some(path) = g.workspace_path.clone() {
            let _ = save_schedules(&path, &g.schedules);
            // Append to the pipeline's run history too.
            if let Some(pid) = pipeline_id {
                let record = RunRecord::from_result(result, "scheduled");
                let _ = append_run_record(&path, &pid, record);
            }
        }
    }

    /// Start the polling task and the file-watch fire listener.
    /// Returns immediately.
    pub fn spawn_ticker(&self) {
        // Cron / interval poller.
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = time::interval(TICK_INTERVAL);
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
                        if let Err(e) = me2.run_now(&id).await {
                            warn!("File-watch run {} failed: {}", id, e);
                        }
                    });
                }
            });
        }
    }

    async fn fire_due(&self) {
        let now = Utc::now();
        let due: Vec<String> = {
            let mut g = self.inner.lock().expect("scheduler poisoned");
            let mut due = Vec::new();
            for s in g.schedules.iter_mut() {
                if s.enabled && matches!(s.next_run_at, Some(t) if t <= now) {
                    due.push(s.id.clone());
                    // Claim the occurrence immediately, under the lock, by
                    // advancing next_run_at to the next FUTURE time. The
                    // tick wakes every 15s and run_now only recomputes
                    // next_run_at on completion (record_run); without this
                    // claim a run slower than 15s gets re-fired every tick.
                    // Advancing (vs clearing to None) keeps the schedule
                    // firing on cadence even if this run errors before
                    // record_run.
                    claim_next_run(s, now);
                }
            }
            due
        };
        for id in due {
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.run_now(&id).await {
                    warn!("Scheduled run {} failed: {}", id, e);
                }
            });
        }
    }
}

/// Advance next_run_at to the next occurrence strictly after `now`.
/// Used to "claim" a due schedule at dispatch so the 15s ticker can't
/// re-fire a still-running schedule. Unlike compute_next_run (which for
/// intervals is anchored on last_run_at and can still be in the past for
/// an overdue run), this is always anchored on `now`, guaranteeing a
/// future time.
fn claim_next_run(s: &mut Schedule, now: DateTime<Utc>) {
    s.next_run_at = match &s.kind {
        ScheduleKind::Cron { expr } => CronSchedule::from_str(expr)
            .ok()
            .and_then(|sched| sched.after(&now).next()),
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
        ScheduleKind::Cron { expr } => CronSchedule::from_str(expr)
            .ok()
            .and_then(|sched| sched.upcoming(Utc).next()),
        ScheduleKind::Interval { seconds } => {
            let base = s.last_run_at.unwrap_or_else(Utc::now);
            Some(base + chrono::Duration::seconds(*seconds as i64))
        }
        // Event-driven - no scheduled next-run time.
        ScheduleKind::FileWatch { .. } => None,
    };
}

fn schedules_path(workspace: &PathBuf) -> PathBuf {
    workspace.join(SCHEDULES_FILE)
}

fn load_schedules(workspace: &PathBuf) -> Result<Vec<Schedule>, String> {
    let p = schedules_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    let parsed: Vec<Schedule> =
        serde_json::from_str(&content).map_err(|e| format!("Parse schedules.json: {}", e))?;
    Ok(parsed)
}

fn save_schedules(workspace: &PathBuf, schedules: &[Schedule]) -> Result<(), String> {
    let p = schedules_path(workspace);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let s = serde_json::to_string_pretty(schedules).map_err(|e| e.to_string())?;
    std::fs::write(&p, s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_parses_and_computes_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "every minute".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 * * * * *".into(),
            },
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

    #[test]
    fn interval_computes_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "every 5".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 300 },
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
            name: "off".into(),
            enabled: false,
            kind: ScheduleKind::Interval { seconds: 60 },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: Some(Utc::now()),
        };
        compute_next_run(&mut s);
        assert!(s.next_run_at.is_none());
    }
}
