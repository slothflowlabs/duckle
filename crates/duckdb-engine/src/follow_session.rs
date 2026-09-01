//! #259: a watcher is not a run.
//!
//! `follow` polls a source continuously. Most polls find nothing: a source
//! checked every ten seconds is unchanged thousands of times between real
//! arrivals. Minting a run receipt for each would bury the handful that moved
//! data under a flood of receipts describing nothing, and make run history
//! useless for the pipeline it is supposed to describe.
//!
//! So there are two identities, and they answer different questions:
//!
//! ```text
//! session_id  the watcher      "is it up, and when did it last look?"
//! run_id      one execution    "what did that batch do, and can I retry it?"
//! ```
//!
//! A poll that finds nothing updates the session and nothing else. A poll that
//! actually executes work gets a normal run id from the same primitive every
//! other surface uses, naming the session as its parent - so it is retryable,
//! comparable and addressable exactly like a scheduled or manual run.
//!
//! ## The session is durable for one reason
//!
//! A watcher that was killed must be distinguishable from one still watching.
//! Written before the loop starts and reconciled on the next start, the same
//! way [`crate::retry`] treats a run - because "the box rebooted" and "it is
//! quietly still polling" call for opposite responses.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const RUNNING: &str = "running";
pub const STOPPED: &str = "stopped";
pub const INTERRUPTED: &str = "interrupted";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FollowSession {
    pub session_id: String,
    pub pipeline_name: String,
    pub pipeline_path: String,
    pub started_at: String,
    /// `running`, `stopped`, or `interrupted` once something else notices the
    /// process is gone.
    pub state: String,
    /// Only meaningful while `running`, and only on the host that wrote it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// When the watcher last looked, whether or not anything was there. This is
    /// the liveness signal: a session whose last poll was an hour ago is stuck,
    /// even though its state still says running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_poll_at: Option<String>,
    /// When it last found something. Separate from `last_poll_at` because
    /// "healthy and idle" and "healthy and ingesting" are different states and
    /// one field cannot say which.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<String>,
    pub poll_count: u64,
    /// Polls that became real runs. `poll_count - run_count` is how much of the
    /// watching was quiet.
    pub run_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

pub fn dir(workspace: &Path) -> PathBuf {
    workspace.join("runs").join("follow")
}

fn path_for(workspace: &Path, session_id: &str) -> PathBuf {
    dir(workspace).join(format!("{session_id}.json"))
}

pub fn new_session_id(pipeline_name: &str) -> String {
    let safe: String = pipeline_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("follow-{safe}-{stamp}")
}

pub fn write(workspace: &Path, session: &FollowSession) -> std::io::Result<()> {
    let dir = dir(workspace);
    std::fs::create_dir_all(&dir)?;
    let body = serde_json::to_vec_pretty(session).unwrap_or_default();
    // Temp then rename, so a reader never sees half a session.
    let tmp = dir.join(format!(".{}.tmp", session.session_id));
    std::fs::write(&tmp, &body)?;
    std::fs::rename(&tmp, path_for(workspace, &session.session_id))
}

pub fn load(workspace: &Path, session_id: &str) -> Option<FollowSession> {
    let text = std::fs::read_to_string(path_for(workspace, session_id)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Every session this workspace knows about, newest first.
pub fn list(workspace: &Path) -> Vec<FollowSession> {
    let mut out: Vec<FollowSession> = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir(workspace)) else { return out };
    for e in entries.flatten() {
        if e.path().extension().is_none_or(|x| x != "json") {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(e.path()) {
            if let Ok(s) = serde_json::from_str::<FollowSession>(&text) {
                out.push(s);
            }
        }
    }
    out.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    out
}

/// Record that a watcher has started, before it looks at anything.
pub fn begin(
    workspace: &Path,
    session_id: &str,
    pipeline_name: &str,
    pipeline_path: &str,
) -> FollowSession {
    let session = FollowSession {
        session_id: session_id.to_string(),
        pipeline_name: pipeline_name.to_string(),
        pipeline_path: pipeline_path.to_string(),
        started_at: chrono::Utc::now().to_rfc3339(),
        state: RUNNING.to_string(),
        pid: Some(std::process::id()),
        last_poll_at: None,
        last_event_at: None,
        poll_count: 0,
        run_count: 0,
        last_run_id: None,
        last_error: None,
    };
    // Best effort, like a receipt: a watcher that cannot record itself is
    // still a watcher that is running.
    let _ = write(workspace, &session);
    session
}

/// One poll happened. `run_id` is `Some` only when it became a real run.
pub fn record_poll(
    workspace: &Path,
    session: &mut FollowSession,
    run_id: Option<&str>,
    error: Option<&str>,
) {
    let now = chrono::Utc::now().to_rfc3339();
    session.poll_count += 1;
    session.last_poll_at = Some(now.clone());
    if let Some(id) = run_id {
        session.run_count += 1;
        session.last_run_id = Some(id.to_string());
        session.last_event_at = Some(now);
    }
    // Cleared on a poll that did not fail, so `last_error` describes the
    // current state rather than the worst thing that ever happened.
    session.last_error = error.map(str::to_string);
    let _ = write(workspace, session);
}

/// The watcher stopped on purpose.
pub fn finish(workspace: &Path, session: &mut FollowSession) {
    session.state = STOPPED.to_string();
    session.pid = None;
    let _ = write(workspace, session);
}

/// Turn abandoned `running` sessions into an honest `interrupted`.
///
/// The same treatment [`crate::retry::reconcile`] gives a run, for the same
/// reason: a watcher that was killed and one that is quietly still polling look
/// identical from the outside, and they call for opposite responses.
pub fn reconcile(workspace: &Path, live_pids: &dyn Fn(u32) -> bool) -> Vec<String> {
    let mut changed = Vec::new();
    for mut session in list(workspace) {
        if session.state != RUNNING {
            continue;
        }
        if session.pid.is_some_and(|pid| live_pids(pid)) {
            continue;
        }
        session.state = INTERRUPTED.to_string();
        session.pid = None;
        if write(workspace, &session).is_ok() {
            changed.push(session.session_id.clone());
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn a_quiet_poll_updates_the_session_and_mints_no_run() {
        // The whole point: a source checked every ten seconds is unchanged
        // thousands of times between real arrivals, and a receipt for each
        // would bury the ones that moved data.
        let tmp = ws();
        let mut s = begin(tmp.path(), "follow-x-1", "orders", "pipelines/orders.json");
        for _ in 0..500 {
            record_poll(tmp.path(), &mut s, None, None);
        }
        assert_eq!(s.poll_count, 500);
        assert_eq!(s.run_count, 0);
        assert!(s.last_poll_at.is_some(), "it is demonstrably alive");
        assert!(s.last_event_at.is_none(), "and demonstrably idle");
        assert!(s.last_run_id.is_none());
        // 500 polls, one file.
        assert_eq!(std::fs::read_dir(dir(tmp.path())).unwrap().count(), 1);
    }

    #[test]
    fn a_poll_that_did_work_names_the_run_it_caused() {
        let tmp = ws();
        let mut s = begin(tmp.path(), "follow-x-2", "orders", "pipelines/orders.json");
        record_poll(tmp.path(), &mut s, None, None);
        record_poll(tmp.path(), &mut s, Some("run-follow-orders-1"), None);
        assert_eq!(s.poll_count, 2);
        assert_eq!(s.run_count, 1, "one poll became a run");
        assert_eq!(s.last_run_id.as_deref(), Some("run-follow-orders-1"));
        assert!(s.last_event_at.is_some());
        // Healthy-and-idle versus healthy-and-ingesting: one field cannot say.
        assert_ne!(s.last_poll_at, None);
    }

    #[test]
    fn the_last_error_describes_now_and_not_the_worst_thing_that_ever_happened() {
        let tmp = ws();
        let mut s = begin(tmp.path(), "follow-x-3", "orders", "p.json");
        record_poll(tmp.path(), &mut s, Some("r1"), Some("connection refused"));
        assert_eq!(s.last_error.as_deref(), Some("connection refused"));
        record_poll(tmp.path(), &mut s, Some("r2"), None);
        assert_eq!(s.last_error, None, "it recovered, and the record should say so");
    }

    #[test]
    fn a_killed_watcher_is_interrupted_and_a_live_one_is_left_alone() {
        let tmp = ws();
        let mut alive = begin(tmp.path(), "follow-alive", "a", "a.json");
        let dead = begin(tmp.path(), "follow-dead", "b", "b.json");
        alive.pid = Some(4242);
        write(tmp.path(), &alive).unwrap();

        let changed = reconcile(tmp.path(), &|pid| pid == 4242);
        assert_eq!(changed, vec![dead.session_id.clone()]);
        assert_eq!(load(tmp.path(), &dead.session_id).unwrap().state, INTERRUPTED);
        assert_eq!(load(tmp.path(), &alive.session_id).unwrap().state, RUNNING);
    }

    #[test]
    fn a_session_that_stopped_on_purpose_is_not_interrupted_later() {
        let tmp = ws();
        let mut s = begin(tmp.path(), "follow-x-4", "orders", "p.json");
        finish(tmp.path(), &mut s);
        assert_eq!(s.state, STOPPED);
        assert!(reconcile(tmp.path(), &|_| false).is_empty(), "stopping is not being killed");
        assert_eq!(load(tmp.path(), "follow-x-4").unwrap().state, STOPPED);
    }

    #[test]
    fn a_session_survives_being_written_and_read_back() {
        let tmp = ws();
        let mut s = begin(tmp.path(), "follow-x-5", "orders", "p.json");
        record_poll(tmp.path(), &mut s, Some("r1"), None);
        let loaded = load(tmp.path(), "follow-x-5").expect("durable");
        assert_eq!(loaded, s);
    }
}
