//! #295: a backfill that survives a restart, and knows which day failed.
//!
//! The plan is written before anything runs and updated as each slice
//! finishes, so a server that dies halfway through comes back knowing what it
//! had done. That is the whole difference between a backfill and a long run:
//! one can be resumed and retried per slice, the other can only be started
//! again.
//!
//! ## States mean what they say
//!
//! `running` is a claim about a process that exists. On the next start,
//! anything still marked running is turned into `interrupted` - the same
//! reconciliation a run receipt gets - because a slice that was killed and one
//! that is quietly still going call for opposite responses, and telling them
//! apart afterwards is impossible if both read `running` forever.
//!
//! ## Retry means the failures, not the day's work
//!
//! Retrying moves `failed` back to `requested` and leaves `succeeded` alone.
//! A backfill of a thousand days that fails on four should cost four runs to
//! finish, not a thousand.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// Wanted, not yet started.
    Requested,
    /// A process claimed it and said so on disk before starting.
    Running,
    Succeeded,
    Failed,
    /// The process holding it went away.
    Interrupted,
    Cancelled,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Requested => "requested",
            State::Running => "running",
            State::Succeeded => "succeeded",
            State::Failed => "failed",
            State::Interrupted => "interrupted",
            State::Cancelled => "cancelled",
        }
    }

    /// Whether this slice still needs attention - it has not succeeded and was
    /// not cancelled. Used to decide whether a backfill is finished, and what
    /// a retry should pick up.
    pub fn is_open(self) -> bool {
        matches!(self, State::Requested | State::Failed | State::Interrupted)
    }

    /// Whether a worker may claim it in THIS pass.
    ///
    /// Only `requested`. A failed slice still needs attention and must not be
    /// re-claimed by the same run: an executor that claimed anything `is_open`
    /// picked its own failure straight back up and retried it forever, which
    /// is how a five-day backfill with one missing file never terminated.
    /// Retrying is a deliberate act that moves failures back to `requested`.
    pub fn is_claimable(self) -> bool {
        self == State::Requested
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRun {
    pub key: String,
    pub state: State,
    /// The durable run this slice produced, so the receipt, the log and the
    /// lineage for one day are all reachable from the backfill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// What this slice binds, carried so a retry uses the same values the
    /// original attempt did rather than regenerating them from a definition
    /// that may since have been edited.
    pub params: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Backfill {
    pub id: String,
    pub pipeline: String,
    pub pipeline_path: String,
    pub created_at: String,
    /// The release active when the backfill was created (#297), so every slice
    /// is traceable to the code it was meant to run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_id: Option<String>,
    pub max_concurrent: usize,
    /// The process that is executing it, when one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub partitions: Vec<PartitionRun>,
}

impl Backfill {
    pub fn counts(&self) -> std::collections::BTreeMap<&'static str, usize> {
        let mut out = std::collections::BTreeMap::new();
        for p in &self.partitions {
            *out.entry(p.state.as_str()).or_insert(0) += 1;
        }
        out
    }

    pub fn is_done(&self) -> bool {
        !self.partitions.iter().any(|p| p.state.is_open())
    }

    /// Move every failed or interrupted slice back to requested.
    ///
    /// Succeeded slices are untouched: a backfill of a thousand days that
    /// failed on four should cost four runs to finish, not a thousand.
    pub fn retry_open(&mut self, only: Option<&[String]>) -> usize {
        let mut n = 0;
        for p in self.partitions.iter_mut() {
            let wanted = only.is_none_or(|keys| keys.iter().any(|k| k == &p.key));
            if wanted && matches!(p.state, State::Failed | State::Interrupted) {
                p.state = State::Requested;
                p.error = None;
                n += 1;
            }
        }
        n
    }

    pub fn cancel(&mut self) -> usize {
        let mut n = 0;
        for p in self.partitions.iter_mut() {
            if p.state.is_open() {
                p.state = State::Cancelled;
                n += 1;
            }
        }
        n
    }
}

pub fn dir(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("backfills")
}

pub fn path_for(workspace: &Path, id: &str) -> PathBuf {
    dir(workspace).join(format!("{id}.json"))
}

pub fn new_id(pipeline: &str) -> String {
    let safe: String = pipeline
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("bf-{safe}-{stamp}")
}

/// Write the plan, atomically.
///
/// Temp then rename, never unlink first: a reader must see the previous
/// complete plan or the new one, and a backfill whose file is briefly absent
/// is one a concurrent `status` reports as missing entirely.
pub fn save(workspace: &Path, backfill: &Backfill) -> Result<(), String> {
    let dir = dir(workspace);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let body = serde_json::to_string_pretty(backfill).map_err(|e| e.to_string())?;
    let tmp = dir.join(format!(".{}.tmp", backfill.id));
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path_for(workspace, &backfill.id)).map_err(|e| e.to_string())
}

pub fn load(workspace: &Path, id: &str) -> Result<Backfill, String> {
    let path = path_for(workspace, id);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

pub fn list(workspace: &Path) -> Vec<Backfill> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir(workspace)) else { return out };
    for e in entries.flatten() {
        if e.path().extension().is_none_or(|x| x != "json") {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(e.path()) {
            if let Ok(b) = serde_json::from_str::<Backfill>(&text) {
                out.push(b);
            }
        }
    }
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    out
}

/// Turn abandoned `running` slices into `interrupted`.
///
/// The same treatment a run receipt gets, for the same reason: a slice whose
/// process was killed and one still going look identical from outside, and
/// they call for opposite responses.
pub fn reconcile(workspace: &Path, live_pids: &dyn Fn(u32) -> bool) -> Vec<String> {
    let mut changed = Vec::new();
    for mut b in list(workspace) {
        if b.pid.is_some_and(|pid| live_pids(pid)) {
            continue;
        }
        let mut touched = false;
        for p in b.partitions.iter_mut() {
            if p.state == State::Running {
                p.state = State::Interrupted;
                touched = true;
            }
        }
        if touched {
            b.pid = None;
            if save(workspace, &b).is_ok() {
                changed.push(b.id.clone());
            }
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{Cadence, PartitionDef};

    fn plan(days: (&str, &str)) -> Backfill {
        let def = PartitionDef::Time {
            cadence: Cadence::Day,
            timezone: "UTC".into(),
            parameter_start: "window_start".into(),
            parameter_end: "window_end".into(),
        };
        let parts = crate::partition::generate(&def, days.0, days.1).unwrap();
        Backfill {
            id: "bf-test-1".into(),
            pipeline: "accounts".into(),
            pipeline_path: "pipelines/accounts.json".into(),
            created_at: "2026-09-01T00:00:00Z".into(),
            release_id: Some("rel-1".into()),
            max_concurrent: 4,
            pid: Some(std::process::id()),
            partitions: parts
                .into_iter()
                .map(|p| PartitionRun {
                    key: p.key,
                    state: State::Requested,
                    run_id: None,
                    attempts: 0,
                    error: None,
                    finished_at: None,
                    params: p.params,
                })
                .collect(),
        }
    }

    #[test]
    fn a_plan_survives_being_written_and_read_back() {
        // Criterion 3: restarting the server must not lose the plan.
        let tmp = tempfile::tempdir().unwrap();
        let b = plan(("2020-01-01", "2020-01-05"));
        save(tmp.path(), &b).unwrap();
        let back = load(tmp.path(), &b.id).unwrap();
        assert_eq!(back, b);
        assert_eq!(back.partitions.len(), 5);
        assert_eq!(back.partitions[0].params.get("window_start").is_some(), true);
    }

    #[test]
    fn retrying_touches_only_the_failures() {
        // Criterion 4: a thousand days failing on four should cost four runs.
        let tmp = tempfile::tempdir().unwrap();
        let mut b = plan(("2020-01-01", "2020-01-05"));
        b.partitions[0].state = State::Succeeded;
        b.partitions[1].state = State::Failed;
        b.partitions[2].state = State::Succeeded;
        b.partitions[3].state = State::Interrupted;
        b.partitions[4].state = State::Requested;
        assert_eq!(b.retry_open(None), 2, "only the failed and interrupted ones");
        assert_eq!(b.partitions[0].state, State::Succeeded, "a success must not be redone");
        assert_eq!(b.partitions[1].state, State::Requested);
        assert_eq!(b.partitions[3].state, State::Requested);
        save(tmp.path(), &b).unwrap();
    }

    #[test]
    fn retrying_can_name_one_partition() {
        let mut b = plan(("2020-01-01", "2020-01-03"));
        for p in b.partitions.iter_mut() {
            p.state = State::Failed;
        }
        assert_eq!(b.retry_open(Some(&["2020-01-02".to_string()])), 1);
        assert_eq!(b.partitions[0].state, State::Failed);
        assert_eq!(b.partitions[1].state, State::Requested);
    }

    #[test]
    fn a_killed_backfill_is_interrupted_and_a_live_one_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let mut dead = plan(("2020-01-01", "2020-01-02"));
        dead.id = "bf-dead".into();
        dead.pid = Some(4242);
        dead.partitions[0].state = State::Running;
        save(tmp.path(), &dead).unwrap();

        let mut alive = plan(("2020-01-01", "2020-01-02"));
        alive.id = "bf-alive".into();
        alive.pid = Some(7);
        alive.partitions[0].state = State::Running;
        save(tmp.path(), &alive).unwrap();

        let changed = reconcile(tmp.path(), &|pid| pid == 7);
        assert_eq!(changed, vec!["bf-dead".to_string()]);
        assert_eq!(load(tmp.path(), "bf-dead").unwrap().partitions[0].state, State::Interrupted);
        assert_eq!(
            load(tmp.path(), "bf-alive").unwrap().partitions[0].state,
            State::Running,
            "a live backfill must not be reaped"
        );
    }

    #[test]
    fn cancelling_leaves_finished_work_alone() {
        let mut b = plan(("2020-01-01", "2020-01-04"));
        b.partitions[0].state = State::Succeeded;
        b.partitions[1].state = State::Failed;
        assert_eq!(b.cancel(), 3, "the failed one and the two still wanted");
        assert_eq!(b.partitions[0].state, State::Succeeded);
        assert!(b.is_done());
    }

    #[test]
    fn a_failed_slice_is_not_claimable_again_in_the_same_pass() {
        // An executor that claimed anything `is_open` picked its own failure
        // straight back up and retried it forever - a five-day backfill with
        // one missing file never terminated.
        assert!(State::Requested.is_claimable());
        for s in [State::Failed, State::Interrupted, State::Running, State::Succeeded, State::Cancelled] {
            assert!(!s.is_claimable(), "{s:?} must not be re-claimed mid-pass");
        }
        // But a failure is still open, so the backfill is not done and a retry
        // will pick it up.
        assert!(State::Failed.is_open());
        assert!(State::Interrupted.is_open());
    }

    #[test]
    fn a_backfill_is_done_only_when_nothing_is_still_open() {
        let mut b = plan(("2020-01-01", "2020-01-02"));
        assert!(!b.is_done());
        b.partitions[0].state = State::Succeeded;
        b.partitions[1].state = State::Failed;
        assert!(!b.is_done(), "a failure is still open until it is retried or cancelled");
        b.partitions[1].state = State::Succeeded;
        assert!(b.is_done());
        assert_eq!(b.counts().get("succeeded"), Some(&2));
    }
}
